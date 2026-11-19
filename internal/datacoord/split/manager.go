// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package split

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Results of one attempt of the trigger, the label of
// metrics.DataCoordShardSplitTriggered.
const (
	resultIssued      = "issued"
	resultFailed      = "failed"
	resultNoPChannel  = "no_pchannel"
	resultFlushFailed = "flush_failed"
)

// pendingRecordRetention is how long a pending record of a collection that no
// longer exists is kept before the trigger loop removes it.
const pendingRecordRetention = 10 * time.Minute

// Collection is what the trigger reads of one collection.
type Collection struct {
	ID int64
	// VChannels are the shards of the collection that accept DML.
	VChannels  []string
	Properties []*commonpb.KeyValuePair
	// SchemaProperties are the properties of the schema of the collection.
	SchemaProperties []*commonpb.KeyValuePair
	// EnableNamespace is set for a collection whose schema enables namespaces.
	EnableNamespace bool
	// External is set for a collection that takes no writes of its own.
	External bool
}

// Request is one split as the trigger plans it. The residues of the targets
// are planned by the issuer against the collection metadata, under the
// resource key of the collection.
type Request struct {
	CollectionID int64
	SplitTaskID  int64
	Source       string
	Targets      [routing.SplitTargetCount]string
}

// Dependencies is everything the trigger reads and calls. Each field is a
// function so that the trigger runs without a coordinator in tests.
type Dependencies struct {
	Store *Store
	// CheckParentRetired consults current CDC members under the collection broadcast lock.
	CheckParentRetired func(context.Context, *datapb.SplitShardTask) error
	// Collections lists the live collections.
	Collections func() []Collection
	// Collection returns one live collection, false if it does not exist.
	Collection func(collectionID int64) (Collection, bool)
	// ShardStats returns the bytes and the rows of the healthy segments of a
	// vchannel.
	ShardStats func(vchannel string) (size int64, rows int64)
	// CheckRewrite checks local atomic-adoption capacity before the WAL message.
	CheckRewrite func(context.Context, int64, string) error
	// DataViewShards returns the shards of the collection in its DataView. A
	// split source stays one until its adoption.
	DataViewShards func(ctx context.Context, collectionID int64) ([]string, error)
	// PChannels returns the pchannels a new vchannel can be placed on, the
	// least loaded first.
	PChannels func(ctx context.Context) ([]string, error)
	// AllocID allocates the id of a split task.
	AllocID func(ctx context.Context) (int64, error)
	// IsPrimary reports whether this cluster may issue a replicated message.
	IsPrimary func(ctx context.Context) (bool, error)
	// IsReplicating reports whether this cluster is part of a replicate
	// topology, as a primary or as a secondary.
	IsReplicating func(ctx context.Context) (bool, error)
	// NodeVersions lists the version of every registered session of a role
	// that takes part in a split, see MinimalNodeVersion.
	NodeVersions func(ctx context.Context) ([]NodeVersion, error)
	// HasPendingImport reports whether an import job of the collection is not
	// finished.
	HasPendingImport func(ctx context.Context, collectionID int64) bool
	// HasPendingRestore reports whether a snapshot restore into the
	// collection (a copy-segment job) is not finished. Its segments are added
	// to the vchannels of the collection outside the WAL.
	HasPendingRestore func(ctx context.Context, collectionID int64) bool
	// FlushSource flushes the source shard on this cluster and returns when
	// the flush has completed.
	FlushSource func(ctx context.Context, collectionID int64, vchannel string) error
	// Issue broadcasts the SplitShard message of the request and returns when
	// its ack callback has returned. It writes nothing if it fails before the
	// message is appended.
	//
	// It calls admit first while it holds the exclusive resource key of the
	// collection, and appends nothing if admit fails. Every DDL of the
	// collection whose ack callback ran before, a split, an import or a
	// restore, is then visible to admit, and none can start before the
	// callback of this split has recorded it.
	Issue func(ctx context.Context, request Request, admit func(ctx context.Context) error) error
}

// Manager is the trigger of shard splits. It decides which shard to split,
// names the targets and issues the SplitShard message. It keeps no state of
// its own about a split: the record of a split is written by the consumers of
// the message (Callback), on this cluster and on every other.
type Manager struct {
	deps Dependencies

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// issuing holds the collections whose split is being issued. The record
	// of a split appears while its issue is in flight, so a collection is
	// excluded from the trigger from the decision to the end of its window.
	issuingMu sync.Mutex
	issuing   map[int64]struct{}
	// spawn runs one issue. A goroutine, replaced in tests.
	spawn func(issue func())
}

// NewManager creates the trigger. Start runs its loop.
func NewManager(ctx context.Context, deps Dependencies) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	m := &Manager{
		deps:    deps,
		ctx:     ctx,
		cancel:  cancel,
		issuing: make(map[int64]struct{}),
	}
	m.spawn = func(issue func()) { go issue() }
	return m
}

func (m *Manager) issuingCount() int {
	m.issuingMu.Lock()
	defer m.issuingMu.Unlock()
	return len(m.issuing)
}

// activeCount returns the number of collections that have a split in its
// window or being issued. A split whose record appears while its issue is
// still in flight is counted once.
func (m *Manager) activeCount() int {
	collections := make(map[int64]struct{})
	for _, task := range m.deps.Store.List() {
		if InWindow(task.GetState()) {
			collections[task.GetCollectionId()] = struct{}{}
		}
	}
	m.issuingMu.Lock()
	defer m.issuingMu.Unlock()
	for collectionID := range m.issuing {
		collections[collectionID] = struct{}{}
	}
	return len(collections)
}

func (m *Manager) isIssuing(collectionID int64) bool {
	m.issuingMu.Lock()
	defer m.issuingMu.Unlock()
	_, ok := m.issuing[collectionID]
	return ok
}

// beginIssue marks the collection as having a split being issued. It returns
// false if it has one already.
func (m *Manager) beginIssue(collectionID int64) bool {
	m.issuingMu.Lock()
	defer m.issuingMu.Unlock()
	if _, ok := m.issuing[collectionID]; ok {
		return false
	}
	m.issuing[collectionID] = struct{}{}
	return true
}

func (m *Manager) endIssue(collectionID int64) {
	m.issuingMu.Lock()
	defer m.issuingMu.Unlock()
	delete(m.issuing, collectionID)
}

// Start runs the trigger loop. With dataCoord.shardSplit.enable off, which is
// the default, a round does nothing but remove stale pending records.
func (m *Manager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			interval := paramtable.Get().DataCoordCfg.ShardSplitCheckInterval.GetAsDuration(time.Second)
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(interval):
				m.sweep()
				m.TriggerOnce()
			}
		}
	}()
}

// Stop stops the loop and waits for it. It does not wait for an issue in
// flight: an issue may be waiting for the resource key of its collection,
// which takes no context, or for the residual of its source. Its context is
// canceled here, so it ends as soon as the broadcaster returns to it. A split
// whose message is appended is carried on by the ack callback, on this
// coordinator or on the next one.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
}

// sweep removes the pending records whose collection no longer exists.
func (m *Manager) sweep() {
	err := m.deps.Store.DropPendingBefore(m.ctx, time.Now().Add(-pendingRecordRetention), func(collectionID int64) bool {
		_, ok := m.deps.Collection(collectionID)
		return !ok
	})
	if err != nil {
		mlog.Warn(m.ctx, "failed to remove stale shard split records", mlog.Err(err))
	}
}

// TriggerOnce inspects every collection once and issues a split for each
// shard over the thresholds, within the limits. It returns the splits it
// started to issue.
func (m *Manager) TriggerOnce() []Request {
	params := &paramtable.Get().DataCoordCfg
	if !params.ShardSplitEnable.GetAsBool() {
		return nil
	}
	if primary, err := m.deps.IsPrimary(m.ctx); err != nil || !primary {
		// A secondary receives its splits through replication. A role that
		// cannot be read is treated as secondary; the next round asks again.
		if err != nil {
			mlog.Warn(m.ctx, "shard split trigger cannot read the replication role of this cluster, skip this round", mlog.Err(err))
		}
		return nil
	}
	if err := m.checkGates(m.ctx); err != nil {
		mlog.RatedWarn(m.ctx, 600, "shard split trigger is enabled but a gate refuses to plan any split", mlog.Err(err))
		return nil
	}
	limit := params.ShardSplitMaxConcurrent.GetAsInt()
	var started []Request
	for _, collection := range m.deps.Collections() {
		if m.activeCount() >= limit {
			break
		}
		if m.CheckAdmission(m.ctx, collection) != nil {
			continue
		}
		for _, vchannel := range collection.VChannels {
			size, rows := m.deps.ShardStats(vchannel)
			if !overThreshold(size, rows) {
				continue
			}
			request, err := m.Split(m.ctx, collection.ID, vchannel)
			if err != nil {
				mlog.RatedWarn(m.ctx, 60, "cannot split the shard over the shard split thresholds",
					mlog.FieldCollectionID(collection.ID), mlog.String("vchannel", vchannel),
					mlog.Int64("size", size), mlog.Int64("rows", rows), mlog.Err(err))
				continue
			}
			mlog.Info(m.ctx, "shard split triggered",
				mlog.FieldCollectionID(collection.ID), mlog.String("vchannel", vchannel),
				mlog.Int64("size", size), mlog.Int64("rows", rows),
				mlog.Int64("splitTaskID", request.SplitTaskID), mlog.Strings("targets", request.Targets[:]))
			started = append(started, request)
			// One split of a collection at a time.
			break
		}
	}
	return started
}

// overThreshold reports whether a shard is to be split.
func overThreshold(size int64, rows int64) bool {
	params := &paramtable.Get().DataCoordCfg
	return size >= params.ShardSplitMaxShardSize.GetAsInt64()*1024*1024 || rows >= params.ShardSplitMaxShardRows.GetAsInt64()
}

// CheckAdmission refuses a collection that cannot be split now:
//
//   - an external collection, which takes no writes to place;
//   - a collection that does not place its rows by the hash of the primary
//     key (routing.CheckSplitAdmission), namespace collections included;
//   - a collection with a split being issued, or in its window on this
//     cluster. Its targets are shards of the collection already, so this also
//     keeps a target from being split before its own adoption;
//   - a collection with an import job or a snapshot restore that is not
//     finished.
//
// The trigger checks it before it flushes the source, to skip a collection
// early. The check that decides is admit, the same rules without the issue in
// flight, under the resource key of the collection.
//
// A refusal is a System error: the request comes from a coordinator, and the
// state that refuses it passes.
func (m *Manager) CheckAdmission(ctx context.Context, collection Collection) error {
	if err := checkPlacement(collection); err != nil {
		return err
	}
	if m.isIssuing(collection.ID) {
		return merr.WrapErrServiceUnavailableMsg("a shard split of collection %d is being issued", collection.ID)
	}
	return m.checkState(ctx, collection.ID)
}

// checkPlacement refuses a collection whose rows a split cannot place.
//
// No namespace collection is split, whatever its namespace mode: a schema
// that enables namespaces, a namespace.mode property (partition or
// partition_key), or namespace sharding (routing.CheckSplitAdmission).
func checkPlacement(collection Collection) error {
	if collection.External {
		return merr.WrapErrOperationNotSupportedMsg("collection %d is an external collection, its shards cannot be split", collection.ID)
	}
	if collection.EnableNamespace || hasProperty(collection.Properties, common.NamespaceModeKey) ||
		hasProperty(collection.SchemaProperties, common.NamespaceModeKey) {
		return merr.WrapErrOperationNotSupportedMsg("collection %d is a namespace collection, its shards cannot be split", collection.ID)
	}
	if err := routing.CheckSplitAdmission(collection.Properties); err != nil {
		return err
	}
	return routing.CheckSplitAdmission(collection.SchemaProperties)
}

func hasProperty(properties []*commonpb.KeyValuePair, key string) bool {
	return slices.ContainsFunc(properties, func(property *commonpb.KeyValuePair) bool { return property.GetKey() == key })
}

// checkState refuses a collection with a split in its window, or an import or
// a restore that is not finished.
func (m *Manager) checkState(ctx context.Context, collectionID int64) error {
	if task := m.deps.Store.WindowOf(collectionID); task != nil {
		return merr.WrapErrServiceUnavailableMsg("shard split task %d of collection %d is in its window, in state %s",
			task.GetTaskId(), collectionID, task.GetState().String())
	}
	if m.deps.HasPendingImport(ctx, collectionID) {
		return merr.WrapErrServiceUnavailableMsg("collection %d has an import job that is not finished", collectionID)
	}
	if m.deps.HasPendingRestore(ctx, collectionID) {
		return merr.WrapErrServiceUnavailableMsg("collection %d has a snapshot restore that is not finished", collectionID)
	}
	return nil
}

// admit is the admission of a split that decides. The issuer runs it under
// the exclusive resource key of the collection, right before it plans and
// appends the message (Dependencies.Issue).
//
// Under the key, every DDL of the collection that was appended before has run
// its ack callback: a split has recorded itself (its callback keeps the key
// until the residual of its source has flushed), and an import or a restore
// has created its job. An import, a snapshot or a restore that starts after
// this split takes the key after its callback, which has recorded the split,
// and is refused by the split window (Server.checkSplitWindow). The flush of
// the source before the split stays outside the key.
func (m *Manager) admit(ctx context.Context, request Request) error {
	if err := m.checkGates(ctx); err != nil {
		return err
	}
	collection, ok := m.deps.Collection(request.CollectionID)
	if !ok {
		return merr.WrapErrCollectionNotFound(request.CollectionID)
	}
	if err := checkPlacement(collection); err != nil {
		return err
	}
	// Planning can precede a pre-flush or a long resource-key wait. Recheck
	// readiness under the key before the irreversible source fence. A newly
	// discovered channel is not a usable target until its WAL has opened.
	pchannels, err := m.deps.PChannels(ctx)
	if err != nil {
		return merr.Wrap(err, "recheck shard split target readiness")
	}
	for _, target := range request.Targets {
		if !slices.Contains(pchannels, funcutil.ToPhysicalChannel(target)) {
			return merr.WrapErrServiceNotReadyMsg("shard split target %s has no ready read-write WAL", target)
		}
	}
	if m.deps.CheckRewrite != nil {
		if err := m.deps.CheckRewrite(ctx, request.CollectionID, request.Source); err != nil {
			return err
		}
	}
	if err := m.checkState(ctx, request.CollectionID); err != nil {
		return err
	}
	for _, parent := range m.deps.Store.List() {
		if parent.GetCollectionId() == request.CollectionID && slices.Contains(targetVChannels(parent), request.Source) {
			if parent.GetState() != datapb.SplitShardTaskState_SplitShardTaskRetired {
				return merr.WrapErrServiceNotReadyMsg("parent split %d has not retired its source", parent.GetTaskId())
			}
			return m.deps.CheckParentRetired(ctx, parent)
		}
	}
	return nil
}

// Split issues the split of one shard of a collection, whatever its size and
// whether the trigger is enabled or not. The trigger calls it for a shard
// over the thresholds; a test or an operator tool may call it directly. The
// gates (checkGates) apply to it as to the trigger.
//
// It checks the admission, names the two targets and returns the request. The
// message is issued in the background: until its ack callback has run,
// nothing is recorded, and if the plan is refused or the append never
// happens, nothing is left behind.
func (m *Manager) Split(ctx context.Context, collectionID int64, source string) (Request, error) {
	if primary, err := m.deps.IsPrimary(ctx); err != nil {
		return Request{}, merr.Wrap(err, "read the replication role of this cluster")
	} else if !primary {
		return Request{}, merr.WrapErrOperationNotSupportedMsg("a shard split is issued on the primary cluster only")
	}
	if err := m.checkGates(ctx); err != nil {
		return Request{}, err
	}
	collection, ok := m.deps.Collection(collectionID)
	if !ok {
		return Request{}, merr.WrapErrCollectionNotFound(collectionID)
	}
	if !slices.Contains(collection.VChannels, source) {
		return Request{}, merr.WrapErrChannelNotFound(source, "it is not a shard of the collection that accepts DML")
	}
	if err := m.CheckAdmission(ctx, collection); err != nil {
		return Request{}, err
	}
	if m.deps.CheckRewrite != nil {
		if err := m.deps.CheckRewrite(ctx, collectionID, source); err != nil {
			return Request{}, err
		}
	}
	targets, err := m.placeTargets(ctx, collection)
	if err != nil {
		metrics.DataCoordShardSplitTriggered.WithLabelValues(resultNoPChannel).Inc()
		return Request{}, err
	}
	taskID, err := m.deps.AllocID(ctx)
	if err != nil {
		return Request{}, merr.Wrap(err, "allocate the id of a shard split task")
	}
	request := Request{CollectionID: collectionID, SplitTaskID: taskID, Source: source, Targets: targets}
	if !m.beginIssue(collectionID) {
		return Request{}, merr.WrapErrServiceUnavailableMsg("a shard split of collection %d is being issued", collectionID)
	}
	m.spawn(func() {
		defer m.endIssue(collectionID)
		m.issue(request)
	})
	return request, nil
}

// placeTargets names the two target vchannels of a split of the collection.
//
// Each target goes to a pchannel that never held a vchannel of the
// collection: not a live shard, not a shard of its DataView (a split source
// stays one until its adoption), not a vchannel of any split this cluster
// records. RootCoord checks the same rule against its lineage records before
// the message is built; this is the best DataCoord knows, and a plan RootCoord
// refuses leaves nothing behind.
func (m *Manager) placeTargets(ctx context.Context, collection Collection) ([routing.SplitTargetCount]string, error) {
	var targets [routing.SplitTargetCount]string
	shards, err := m.deps.DataViewShards(ctx, collection.ID)
	if err != nil {
		return targets, merr.Wrap(err, "read the shards of the collection from its DataView")
	}
	known := typeutil.NewSet(collection.VChannels...)
	known.Insert(shards...)
	known.Insert(m.deps.Store.VChannels(collection.ID)...)
	occupied := typeutil.NewSet[string]()
	for vchannel := range known {
		occupied.Insert(funcutil.ToPhysicalChannel(vchannel))
	}
	pchannels, err := m.deps.PChannels(ctx)
	if err != nil {
		return targets, merr.Wrap(err, "list the pchannels for the targets of a shard split")
	}
	// A vchannel name carries an index. Take indexes above every one the
	// collection has used, so that no two of its vchannels share one.
	index := known.Len()
	for vchannel := range known {
		if _, _, used, err := funcutil.ParseVChannel(vchannel); err == nil && used >= index {
			index = used + 1
		}
	}
	placed := 0
	for _, pchannel := range pchannels {
		if placed == len(targets) {
			break
		}
		if occupied.Contain(pchannel) {
			continue
		}
		occupied.Insert(pchannel)
		targets[placed] = funcutil.GetVirtualChannel(pchannel, collection.ID, index)
		index++
		placed++
	}
	if placed < len(targets) {
		return targets, merr.WrapErrServiceUnavailableMsg(
			"a shard split of collection %d needs %d pchannels that hold no vchannel of the collection, %d of %d available pchannels qualify",
			collection.ID, len(targets), placed, len(pchannels))
	}
	return targets, nil
}

// issue flushes the source, if configured, and broadcasts the split.
func (m *Manager) issue(request Request) {
	logger := mlog.With(
		mlog.FieldCollectionID(request.CollectionID),
		mlog.Int64("splitTaskID", request.SplitTaskID),
		mlog.String("source", request.Source),
		mlog.Strings("targets", request.Targets[:]))
	if paramtable.Get().DataCoordCfg.ShardSplitFlushBeforeSplit.GetAsBool() {
		// Less data is unflushed at the fence, so the residual flushes sooner
		// and the split keeps the resource key of the collection for a
		// shorter time. Local to this cluster, and not needed for correctness:
		// a failure only gives up this attempt, before anything is appended.
		if err := m.deps.FlushSource(m.ctx, request.CollectionID, request.Source); err != nil {
			metrics.DataCoordShardSplitTriggered.WithLabelValues(resultFlushFailed).Inc()
			logger.Warn(m.ctx, "failed to flush the source before its shard split, the split is not issued", mlog.Err(err))
			return
		}
	}
	start := time.Now()
	admit := func(ctx context.Context) error { return m.admit(ctx, request) }
	if err := m.deps.Issue(m.ctx, request, admit); err != nil {
		metrics.DataCoordShardSplitTriggered.WithLabelValues(resultFailed).Inc()
		if m.deps.Store.Get(request.SplitTaskID) != nil {
			// The message is appended and its callback has recorded the
			// split: the split goes on whatever this call returned.
			logger.Warn(m.ctx, "the issue of a shard split returned an error after the split was recorded, the split goes on", mlog.Err(err))
			return
		}
		logger.Warn(m.ctx, "failed to issue a shard split", mlog.Err(err))
		return
	}
	metrics.DataCoordShardSplitTriggered.WithLabelValues(resultIssued).Inc()
	logger.Info(m.ctx, "shard split issued, the residual of its source has flushed", mlog.Duration("elapsed", time.Since(start)))
}
