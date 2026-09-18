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

package datacoord

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The shard split manager (design doc §5, §6.1 step 1).
//
// It detects the shards that need a split and drives every split task this
// cluster records through its lifecycle:
//
//	Preparing -> Fencing -> Redistributing -> Adopting -> Done
//	    \-> Aborted (only before the fence)
//
// Only a collection placed by primary key is split, and only by rewriting its
// data (design doc §1.3): a split fences one source shard and creates two
// targets whose residues halve the source's.
//
// The trigger and the write switch are the primary's: a secondary's tasks are
// created by the SplitShard ack callback (Server.CommitShardSplit), already in
// Redistributing, and the manager drives them from there like any other.

// splitVChannelAllocator allocates the target vchannels of a split next to the
// collection's existing ones.
type splitVChannelAllocator interface {
	AllocVirtualChannels(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error)
}

// staticVChannelAllocator is the streaming node manager's allocator, resolved
// at call time.
type staticVChannelAllocator struct{}

func (staticVChannelAllocator) AllocVirtualChannels(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error) {
	return snmanager.StaticStreamingNodeManager.AllocVirtualChannels(ctx, param)
}

// splitCoordinator is the part of the datacoord server the manager acts
// through. Implemented by *Server; a seam so the task machine can be driven
// without a live rootcoord or streamingcoord.
type splitCoordinator interface {
	// describeSplitCollection reads rootcoord's current record of the
	// collection; merr.ErrCollectionNotFound once it is dropped.
	describeSplitCollection(ctx context.Context, collectionID int64) (*splitCollection, error)
	// issueShardSplit broadcasts a planned task's write switch under the
	// collection's resource keys (Server.issueShardSplit).
	issueShardSplit(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error
	// splitDrainBlockReason names the drain conjunct a task still waits on, ""
	// once it is drained: the predicate CheckShardSplitDrained answers the
	// adoption gate with (splitSourcesDrained).
	splitDrainBlockReason(ctx context.Context, task *datapb.SplitShardTask) string
	// fenceFlushBlockReason names the source whose fence is not recorded or
	// whose checkpoint is short of it, "" once the redistribution may start.
	fenceFlushBlockReason(task *datapb.SplitShardTask) string
	// issueShardSplitAdoption broadcasts a drained task's adoption under the
	// collection's resource keys (Server.issueShardSplitAdoption).
	issueShardSplitAdoption(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error
	// splitSourceServed reports whether this cluster's QueryCoord still serves
	// the source.
	splitSourceServed(ctx context.Context, collectionID int64, source string) (bool, error)
}

// splitRedistributor moves a fenced split's source data into its targets. The
// rewrite implements it; the manager calls it once per tick while the task is
// Redistributing and its source is past its T_switch, and moves the task on
// once the drain predicate holds.
type splitRedistributor interface {
	redistribute(ctx context.Context, task *datapb.SplitShardTask)
}

// shardSplitManager detects the shards that need a split and drives the split
// tasks. Every threshold it reads is a refreshable configuration under
// dataCoord.shardSplit.
type shardSplitManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	meta      *meta
	catalog   metastore.DataCoordCatalog
	allocator allocator.Allocator
	// store is the one record of split tasks datacoord keeps, shared with the
	// SplitShard ack callback (Server.CommitShardSplit). The manager writes a
	// record only through store.create and store.modify, which take the
	// per-task lock the callback holds.
	store             *shardSplitTasks
	vchannelAllocator splitVChannelAllocator
	coordinator       splitCoordinator
	// redistributor is wired by the rewrite; nil leaves a task in its
	// redistribution window.
	redistributor splitRedistributor
	// preempter is the compaction inspector, wired once it is built (it is
	// built after the manager, because it consumes the manager's freeze).
	preempter compactionPreempter

	// controlChannel names the control channel the write switch reaches, and
	// replicationRole reads this cluster's replication role. Functions so the
	// manager can be tested without a streaming client or a balancer.
	controlChannel  func() string
	replicationRole func(ctx context.Context) (replicateutil.Role, error)

	lastDetect time.Time
}

func newShardSplitManager(
	ctx context.Context,
	meta *meta,
	allocator allocator.Allocator,
	store *shardSplitTasks,
	coordinator splitCoordinator,
) *shardSplitManager {
	ctx, cancel := context.WithCancel(ctx)
	return &shardSplitManager{
		ctx:               ctx,
		cancel:            cancel,
		meta:              meta,
		catalog:           meta.catalog,
		allocator:         allocator,
		store:             store,
		vchannelAllocator: staticVChannelAllocator{},
		coordinator:       coordinator,
		controlChannel:    func() string { return streaming.WAL().ControlChannel() },
		replicationRole:   balancerReplicationRole,
	}
}

// setRedistributor wires the redistribution in.
func (m *shardSplitManager) setRedistributor(redistributor splitRedistributor) {
	m.redistributor = redistributor
}

// balancerReplicationRole reads the replication role off the streamingcoord
// balancer.
func balancerReplicationRole(ctx context.Context) (replicateutil.Role, error) {
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return replicateutil.RolePrimary, err
	}
	return b.ReplicateRole(), nil
}

// Start starts the manager's loop: every task interval it advances every task
// by one step, and every check interval it runs the trigger.
func (m *shardSplitManager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		logger := mlog.With(mlog.FieldComponent("shard-split-manager"))
		logger.Info(m.ctx, "shard split manager started")
		for {
			interval := paramtable.Get().DataCoordCfg.ShardSplitTaskInterval.GetAsDuration(time.Second)
			select {
			case <-m.ctx.Done():
				logger.Info(m.ctx, "shard split manager stopped")
				return
			case <-time.After(interval):
				m.tick(time.Now())
			}
		}
	}()
}

// Stop stops the loop and waits for it.
func (m *shardSplitManager) Stop() {
	m.cancel()
	m.wg.Wait()
}

// tick runs one round of the loop at now.
func (m *shardSplitManager) tick(now time.Time) {
	checkInterval := paramtable.Get().DataCoordCfg.ShardSplitCheckInterval.GetAsDuration(time.Second)
	if now.Sub(m.lastDetect) >= checkInterval {
		m.lastDetect = now
		m.detectOnce()
	}
	m.advanceTasks()
	m.refreshMetrics()
}

// clusterIsReplicationSecondary reports whether this cluster is the secondary
// of a replication pair. A failed lookup answers yes: guessing "primary" is the
// only way a secondary would plan a split, and the next round asks again.
func (m *shardSplitManager) clusterIsReplicationSecondary() bool {
	ctx, cancel := context.WithTimeout(m.ctx, time.Second)
	defer cancel()
	role, err := m.replicationRole(ctx)
	if err != nil {
		mlog.RatedWarn(m.ctx, 60, "cannot read this cluster's replication role, treating it as a secondary this round",
			mlog.FieldComponent("shard-split-manager"), mlog.Err(err))
		return true
	}
	return role == replicateutil.RoleSecondary
}

// shardStats is the aggregated statistics of one shard.
type shardStats struct {
	vchannel string
	size     int64 // bytes of every healthy segment.
	rows     int64
}

// collectShardStats aggregates the healthy segments of one vchannel.
func (m *shardSplitManager) collectShardStats(vchannel string) *shardStats {
	stats := &shardStats{vchannel: vchannel}
	for _, segment := range m.meta.GetSegmentsByChannel(vchannel) {
		stats.size += segment.getSegmentSize()
		stats.rows += segment.GetNumOfRows()
	}
	return stats
}

// shouldSplit reports whether a shard is at or over the size or row threshold.
func shouldSplit(stats *shardStats) bool {
	params := &paramtable.Get().DataCoordCfg
	maxSize := params.ShardSplitMaxShardSize.GetAsInt64() * 1024 * 1024 * 1024
	maxRows := params.ShardSplitMaxShardRows.GetAsInt64()
	return stats.size >= maxSize || stats.rows >= maxRows
}

// isSplitShardTaskActive reports whether the task is not in a terminal state.
func isSplitShardTaskActive(task *datapb.SplitShardTask) bool {
	return task.GetState() != datapb.SplitShardTaskState_SplitShardTaskDone &&
		task.GetState() != datapb.SplitShardTaskState_SplitShardTaskAborted
}

// activeTaskCount is the number of tasks that are not Done or Aborted.
func (m *shardSplitManager) activeTaskCount() int {
	count := 0
	for _, task := range m.store.list() {
		if isSplitShardTaskActive(task) {
			count++
		}
	}
	return count
}

// hasActiveTaskOnVChannel reports whether a task that is not Done or Aborted
// names the vchannel as its source or one of its targets. The trigger skips
// such a shard: it would otherwise fire on it again every round.
func (m *shardSplitManager) hasActiveTaskOnVChannel(vchannel string) bool {
	for _, task := range m.store.list() {
		if !isSplitShardTaskActive(task) {
			continue
		}
		if slices.Contains(splitSourceVChannels(task), vchannel) || slices.Contains(splitTaskTargetVChannels(task), vchannel) {
			return true
		}
	}
	return false
}

// hasActiveTaskOnCollection reports whether a split of the collection is not
// Done or Aborted. The trigger splits one shard of a collection at a time: a
// split is planned against the collection's routing as it stands, and every
// split changes it -- fencing a source, creating targets, possibly doubling
// the modulus -- so a second plan made meanwhile would no longer fit the
// record its write switch is checked against, and would be refused before its
// fence on every tick without end.
func (m *shardSplitManager) hasActiveTaskOnCollection(collectionID int64) bool {
	for _, task := range m.store.list() {
		if task.GetCollectionId() == collectionID && isSplitShardTaskActive(task) {
			return true
		}
	}
	return false
}

// splittableCollection reports whether the trigger may split a collection at
// all: one with a schema, placed by primary key. A namespace collection is
// never selected (design doc §1.3), whatever its placement, and neither is an
// external collection, which takes no writes to place.
func splittableCollection(collection *collectionInfo) bool {
	if collection == nil || collection.Schema == nil {
		return false
	}
	if collection.Schema.GetEnableNamespace() {
		return false
	}
	return !typeutil.IsExternalCollection(collection.Schema) && splitRefusalReason(collection.Schema) == ""
}

// splitRefusalReason names why a collection that is otherwise splittable
// cannot be split by rewrite, "" when it can.
//
// A TEXT field is stored through LOB references the rewrite writer does not
// carry, so every rewrite plan of such a collection would fail and the split,
// which cannot abort past its fence, would never finish. Rootcoord refuses to
// add a TEXT field while a split is in flight (Splitting or Creating shards),
// so a collection refused here is refused on every check from planning to the
// write switch.
func splitRefusalReason(schema *schemapb.CollectionSchema) string {
	if typeutil.HasTextField(schema) {
		return "the collection has a TEXT field, which a shard split rewrite cannot carry yet"
	}
	return ""
}

// detectOnce inspects every splittable collection and creates a split task for
// each shard at or over the thresholds, within the concurrency cap. It runs on
// the primary only, and only while splits may be issued and compaction, which
// carries the rewrite, is enabled.
func (m *shardSplitManager) detectOnce() {
	params := &paramtable.Get().DataCoordCfg
	logger := mlog.With(mlog.FieldComponent("shard-split-manager"))
	if !params.ShardSplitEnable.GetAsBool() {
		return
	}
	if !params.EnableCompaction.GetAsBool() {
		// A split redistributes by rewriting, dispatched as compaction plans.
		// Fenced with compaction off it could never move and never abort.
		logger.RatedInfo(m.ctx, 60, "shard split trigger suppressed while dataCoord.enableCompaction is off")
		return
	}
	if m.clusterIsReplicationSecondary() {
		// A secondary's splits come from the replicated broadcast.
		logger.RatedInfo(m.ctx, 60, "shard split trigger suppressed on a replication secondary")
		return
	}
	maxConcurrent := params.ShardSplitMaxConcurrentTasks.GetAsInt()
	active := m.activeTaskCount()
	for _, collection := range m.meta.GetCollections() {
		if reason := splitRefusalReason(collection.Schema); reason != "" {
			logger.RatedInfo(m.ctx, 600, "shard split trigger skips a collection it cannot split",
				mlog.FieldCollectionID(collection.ID), mlog.String("reason", reason))
			continue
		}
		if !splittableCollection(collection) || m.hasActiveTaskOnCollection(collection.ID) {
			continue
		}
		for _, vchannel := range collection.VChannelNames {
			if active >= maxConcurrent {
				return
			}
			if m.hasActiveTaskOnVChannel(vchannel) {
				continue
			}
			stats := m.collectShardStats(vchannel)
			if !shouldSplit(stats) {
				continue
			}
			task, err := m.planSplit(collection.ID, stats)
			if err != nil {
				logger.RatedWarn(m.ctx, 60, "cannot plan a split of the shard over the thresholds",
					mlog.FieldCollectionID(collection.ID), mlog.String("vchannel", vchannel), mlog.Err(err))
				continue
			}
			if task == nil {
				continue
			}
			logger.Info(m.ctx, "shard split task created",
				mlog.Int64("taskID", task.GetTaskId()),
				mlog.FieldCollectionID(collection.ID),
				mlog.String("vchannel", vchannel),
				mlog.Int64("size", stats.size),
				mlog.Int64("rows", stats.rows),
				mlog.Uint64("routingModulus", task.GetRoutingModulus()))
			active++
			// One split per collection at a time.
			break
		}
	}
}

// planSplit plans the split of one shard against rootcoord's current record of
// its collection and persists it as a Preparing task. It returns nil, and no
// error, for a shard that is not to be split after all.
//
// The plan is the two targets' residues and the modulus after the split
// (planSplitResidues). The target vchannels are allocated later, in Preparing,
// so a failed allocation aborts this task instead of the trigger's scan.
func (m *shardSplitManager) planSplit(collectionID int64, stats *shardStats) (*datapb.SplitShardTask, error) {
	coll, err := m.coordinator.describeSplitCollection(m.ctx, collectionID)
	if err != nil {
		return nil, err
	}
	if coll.EnableNamespace || coll.schema.GetEnableNamespace() {
		// datacoord's cached copy said otherwise; the record wins.
		return nil, nil
	}
	if reason := splitRefusalReason(coll.schema); reason != "" {
		mlog.RatedInfo(m.ctx, 600, "not planning a shard split of the collection",
			mlog.FieldComponent("shard-split-manager"), mlog.FieldCollectionID(collectionID), mlog.String("reason", reason))
		return nil, nil
	}
	info, ok := coll.ShardInfos[stats.vchannel]
	if !slices.Contains(coll.VirtualChannelNames, stats.vchannel) || (ok && info.State != schemapb.ShardState_ShardNormal) {
		// Retired, fenced or not adopted yet: only a Normal shard is split.
		return nil, nil
	}
	residues, err := residuesOf(coll.Collection)
	if err != nil {
		return nil, err
	}
	own, err := residues.of(stats.vchannel)
	if err != nil {
		return nil, err
	}
	if m.doublingRelievedNothing(coll, residues, own, stats.size) {
		mlog.RatedWarn(m.ctx, 300,
			"refusing to double a shard its last doubling did not relieve; its sibling half is nearly empty, "+
				"which a unique primary key cannot produce -- look for one key inserted many times",
			mlog.FieldComponent("shard-split-manager"),
			mlog.FieldCollectionID(collectionID),
			mlog.String("vchannel", stats.vchannel),
			mlog.Int64("size", stats.size))
		return nil, nil
	}
	left, right, after, err := planSplitResidues(residues.modulus, own)
	if err != nil {
		return nil, err
	}
	taskID, err := m.allocator.AllocID(m.ctx)
	if err != nil {
		return nil, err
	}
	task := &datapb.SplitShardTask{
		TaskId:         taskID,
		CollectionId:   collectionID,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: stats.vchannel}},
		Targets:        []*datapb.SplitShardTaskTarget{{Buckets: left}, {Buckets: right}},
		RoutingModulus: after,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
		StartTime:      uint64(time.Now().Unix()),
	}
	if err := m.store.create(m.ctx, m.catalog, task); err != nil {
		return nil, err
	}
	return task, nil
}
