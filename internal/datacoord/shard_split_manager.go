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

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitVChannelAllocator allocates additional vchannels for an existing
// collection; implemented by snmanager.StaticStreamingNodeManager.
type splitVChannelAllocator interface {
	AllocVirtualChannels(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error)
}

// compactionPreempter kills the queued and executing compaction tasks of a
// channel; implemented by the compaction inspector. Together with the
// enqueue-time freeze it guarantees a shard split preempts compaction
// instead of waiting behind it.
type compactionPreempter interface {
	preemptTasksByChannel(channel string)
}

// routingCommitter commits a shard-split routing change into the collection
// meta via rootcoord (the write-switch routing commit and the adoption flip);
// implemented by the datacoord broker. The call is idempotent by shard state.
type routingCommitter interface {
	CommitShardSplitRouting(ctx context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error
}

// shardSplitManager detects the shards that need a split and drives the
// persisted split task FSM. Every threshold it relies on is a refreshable
// configuration under `dataCoord.shardSplit`.
type shardSplitManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	meta              *meta
	catalog           metastore.DataCoordCatalog
	allocator         allocator.Allocator
	wal               streaming.WALAccesser
	vchannelAllocator splitVChannelAllocator
	planner           splitPlanner
	preempter         compactionPreempter // set after the compaction inspector is built.
	importMeta        ImportMeta          // set during the server initialization; the redistribution drain check consults it.
	router            routingCommitter    // set during the server initialization; commits the routing change at fence and adoption.
	// collectionLocker takes the collection-level exclusion a write switch runs
	// under, so no DDL can change the collection between the fence and the
	// routing commit (shard_split_lock.go).
	collectionLocker collectionLocker

	// tasks holds every split task, of both kinds: they share one lifecycle
	// and differ only in how the redistribution phase moves the data.
	tasks *typeutil.ConcurrentMap[int64, *datapb.SplitShardTask] // task id -> task
	// rewriteDispatcher submits the hash-split rewrite plans; set after the
	// compaction inspector is built (like preempter above).
	rewriteDispatcher *inspectorRewriteDispatcher
}

// setRoutingCommitter wires the routing committer (the datacoord broker) in; it
// is called once during the server initialization.
func (m *shardSplitManager) setRoutingCommitter(router routingCommitter) {
	m.router = router
}

// setCompactionPreempter wires the compaction inspector in; it is called
// once during the server initialization (the inspector is built after the
// shard split manager because it also consumes the freeze predicate).
func (m *shardSplitManager) setCompactionPreempter(preempter compactionPreempter) {
	m.preempter = preempter
}

// setImportMeta wires the import meta in; it is called once during the server
// initialization. The redistribution drain check uses it to wait out the
// import jobs that target the source vchannel, including the ones still in
// Pending/PreImporting that have not registered any segment in meta yet.
func (m *shardSplitManager) setImportMeta(importMeta ImportMeta) {
	m.importMeta = importMeta
}

// hasActiveImportOnVChannel reports whether an unfinished import job targets
// the vchannel. A job's target vchannels are fixed at creation, so this is a
// purely datacoord-local check that needs no import/split mutual exclusion.
func (m *shardSplitManager) hasActiveImportOnVChannel(vchannel string) bool {
	return m.hasActiveImportOnAnyVChannel([]string{vchannel})
}

// hasActiveImportOnAnyVChannel reports whether an unfinished import job targets
// any of the given vchannels.
//
// The job list is fetched once and matched against the whole set, rather than
// re-scanning per vchannel: a hash split may hold many sources at once, and each
// scan walks every unfinished job in the cluster.
func (m *shardSplitManager) hasActiveImportOnAnyVChannel(vchannels []string) bool {
	if m.importMeta == nil || len(vchannels) == 0 {
		return false
	}
	wanted := typeutil.NewSet(vchannels...)
	jobs := m.importMeta.GetJobBy(m.ctx, WithoutJobStates(
		internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed))
	for _, job := range jobs {
		for _, vc := range job.GetVchannels() {
			if wanted.Contain(vc) {
				return true
			}
		}
	}
	return false
}

// clusterReplicating reports whether replication/CDC is currently enabled on the
// cluster. A shard split must not start while replicating: the split's control
// messages are not yet part of the replication stream, so a replica would miss
// the topology change (D6). The check is best-effort with a short timeout — the
// streamingnode fence is the backstop — so a transient balancer lookup failure
// never wedges the detection loop (it errs toward allowing the trigger).
func (m *shardSplitManager) clusterReplicating() bool {
	ctx, cancel := context.WithTimeout(m.ctx, time.Second)
	defer cancel()
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return false
	}
	assignment, err := b.GetLatestChannelAssignment()
	if err != nil || assignment == nil {
		return false
	}
	return isReplicatingCluster(assignment.ReplicateConfiguration)
}

// fenceFlushed reports whether the streamingnode flusher has flushed and reported
// to datacoord every segment the SplitShard fence sealed. The fence sealed all data
// at or before T_switch, and the source channel checkpoint only advances past a
// position once the segments holding that data are durably synced and reported (the
// write buffer holds the checkpoint at the earliest un-synced position). So the
// checkpoint reaching T_switch proves the fence-sealed set is fully in datacoord
// meta and relabelable.
//
// A task without a recorded T_switch (switch_time_tick == 0, e.g. a crash before it
// was persisted) does not gate on the checkpoint: the fence already holds, the
// segment scan is the backstop, and T_switch is restored on a later fence retry.
func (m *shardSplitManager) fenceFlushed(task *datapb.SplitShardTask) bool {
	if firstSwitchTimeTick(task) == 0 {
		return true
	}
	cp := m.meta.GetChannelCheckpoint(firstSourceVChannel(task))
	return cp != nil && cp.GetTimestamp() >= firstSwitchTimeTick(task)
}

// shardStats is the aggregated statistics of one shard (vchannel).
type shardStats struct {
	vchannel       string
	size           int64 // bytes of all healthy segments.
	rows           int64
	namespaceCount int // distinct partitions; a namespace collection maps one namespace to one partition.
}

// newShardSplitManager recovers the split tasks from the catalog so an
// in-flight task resumes after a datacoord restart.
func newShardSplitManager(
	ctx context.Context,
	meta *meta,
	catalog metastore.DataCoordCatalog,
	allocator allocator.Allocator,
	wal streaming.WALAccesser,
	vchannelAllocator splitVChannelAllocator,
	planner splitPlanner,
) (*shardSplitManager, error) {
	tasks, err := catalog.ListSplitShardTask(ctx)
	if err != nil {
		return nil, err
	}
	if planner == nil {
		planner = unimplementedSplitPlanner{}
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &shardSplitManager{
		ctx:               ctx,
		cancel:            cancel,
		meta:              meta,
		catalog:           catalog,
		allocator:         allocator,
		wal:               wal,
		vchannelAllocator: vchannelAllocator,
		planner:           planner,
		collectionLocker:  broadcastCollectionLocker(ctx),
		tasks:             typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask](),
	}
	for _, task := range tasks {
		m.tasks.Insert(task.GetTaskId(), task)
	}
	return m, nil
}

// Start starts the background detection loop.
func (m *shardSplitManager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		logger := mlog.With(mlog.FieldComponent("shard-split-manager"))
		logger.Info(m.ctx, "shard split manager started")
		for {
			interval := paramtable.Get().DataCoordCfg.ShardSplitCheckInterval.GetAsDuration(time.Second)
			select {
			case <-m.ctx.Done():
				logger.Info(m.ctx, "shard split manager stopped")
				return
			case <-time.After(interval):
				m.detectOnce()
				m.detectHashSplitOnce()
				// Reconcile any user-requested shard count. Separate from the
				// size trigger above: that one decides on its own whether to
				// split, this one only carries out a request already made.
				m.reconcileDesiredShardNum()
				m.advanceTasks()
				// After advanceTasks, so a task reaped this tick is already gone
				// from the cache and the source it retired becomes reclaimable
				// here rather than a whole interval later.
				m.reclaimRetiredVChannelsOnce()
				m.refreshMetrics()
			}
		}
	}()
}

// Stop stops the background detection loop.
func (m *shardSplitManager) Stop() {
	m.cancel()
	m.wg.Wait()
}

// detectOnce inspects the per-shard statistics of every namespace-enabled
// collection and creates a split task for the shards over the thresholds.
func (m *shardSplitManager) detectOnce() {
	params := &paramtable.Get().DataCoordCfg
	logger := mlog.With(mlog.FieldComponent("shard-split-manager"))
	if !params.ShardSplitEnable.GetAsBool() || !params.ShardSplitAutoTriggerEnable.GetAsBool() {
		// autoTriggerEnable selects the mode; with it off the cluster sizes its
		// shards only on user request, and nothing here may fire.
		return
	}
	if m.clusterReplicating() {
		// D6: a shard split's control messages (SplitShard/CreateVChannel) are not
		// yet part of the replication stream, so a replica would miss the topology
		// change. Suppress the trigger while the cluster is replicating; the
		// streamingnode fence is the backstop if a split somehow starts anyway.
		logger.RatedWarn(m.ctx, 60, "shard split trigger suppressed while replication/CDC is enabled")
		return
	}
	maxConcurrent := params.ShardSplitMaxConcurrentTasks.GetAsInt()
	active := m.activeTaskCount()
	if active >= maxConcurrent {
		return
	}

	for _, collection := range m.meta.GetCollections() {
		if !collection.Schema.GetEnableNamespace() {
			// only the namespace (multi-tenant) collections are subject to
			// the metadata-only relabel split.
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
			if !m.shouldSplit(stats) {
				continue
			}
			if stats.namespaceCount <= 1 {
				// A single-namespace shard satisfies the thresholds forever
				// but cannot be split further: the split point must fall on
				// a namespace boundary, and its growth is bounded by the
				// namespace hard limit instead.
				logger.RatedWarn(m.ctx, 60, "the shard over the split thresholds holds a single namespace, skip it",
					mlog.String("vchannel", stats.vchannel),
					mlog.Int64("size", stats.size),
					mlog.Int64("rows", stats.rows))
				continue
			}
			if residues, err := residuesOf(collection); err != nil {
				// Checked before the task exists rather than in the planner
				// alone: a task created here would fail in Preparing on every
				// tick forever, and hold a concurrency slot while doing it.
				logger.RatedWarn(m.ctx, 60, "the shard over the split thresholds cannot be split, skip it",
					mlog.String("vchannel", vchannel), mlog.Err(err))
				continue
			} else if _, err := residues.of(vchannel); err != nil {
				logger.RatedWarn(m.ctx, 60, "the shard over the split thresholds is not a routable shard, skip it",
					mlog.String("vchannel", vchannel), mlog.Err(err))
				continue
			}
			task, err := m.createTask(collection.ID, stats)
			if err != nil {
				logger.Warn(m.ctx, "create shard split task failed",
					mlog.String("vchannel", stats.vchannel), mlog.Err(err))
				continue
			}
			logger.Info(m.ctx, "shard split task created",
				mlog.Int64("taskID", task.GetTaskId()),
				mlog.Int64("collectionID", collection.ID),
				mlog.String("vchannel", stats.vchannel),
				mlog.Int64("size", stats.size),
				mlog.Int64("rows", stats.rows),
				mlog.Int("namespaceCount", stats.namespaceCount))
			active++
		}
	}
}

// shouldSplit returns true if any of the configured thresholds is reached.
func (m *shardSplitManager) shouldSplit(stats *shardStats) bool {
	params := &paramtable.Get().DataCoordCfg
	maxSize := params.ShardSplitMaxShardSize.GetAsInt64() * 1024 * 1024 * 1024
	maxRows := params.ShardSplitMaxShardRows.GetAsInt64()
	maxNamespaces := params.ShardSplitMaxNamespaceCount.GetAsInt()
	return stats.size >= maxSize || stats.rows >= maxRows || stats.namespaceCount >= maxNamespaces
}

// collectShardStats aggregates the statistics of one shard from the healthy
// segments of its vchannel.
func (m *shardSplitManager) collectShardStats(vchannel string) *shardStats {
	stats := &shardStats{vchannel: vchannel}
	namespaces := typeutil.NewSet[int64]()
	for _, segment := range m.meta.GetSegmentsByChannel(vchannel) {
		stats.size += segment.getSegmentSize()
		stats.rows += segment.GetNumOfRows()
		namespaces.Insert(segment.GetPartitionID())
	}
	stats.namespaceCount = namespaces.Len()
	return stats
}

// activeTaskCount returns the number of split tasks that are not finished.
func (m *shardSplitManager) activeTaskCount() int {
	count := 0
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if isSplitShardTaskActive(task) {
			count++
		}
		return true
	})
	return count
}

// IsVChannelSplitting returns true if an unfinished split task references
// the vchannel as the source or as a target. Such a vchannel is excluded
// from compaction, clustering and GC during the split window: compaction
// would churn the redistribution work list and replace the segment IDs the
// in-place delegator handoff relies on.
func (m *shardSplitManager) IsVChannelSplitting(vchannel string) bool {
	// Both split kinds freeze their shards: a hash split's rewrite reads the
	// source segments and writes new ones, so a concurrent compaction would
	// churn the work list and replace the very segments being rewritten.
	return m.hasAnyActiveTaskOnVChannel(vchannel)
}

// SplitTargetsOfSource returns the target vchannels of the unfinished RELABEL
// split task whose source is the given vchannel, or nil. It powers the merged
// recovery view: a relabel MOVES a segment from the source channel to a target
// channel, so the source's recovery info would lose it mid-window and a target
// refresh could produce a release task against a segment the source delegator
// is still serving. Reporting the union keeps the set stable.
//
// Hash split tasks are deliberately NOT consulted, and adding them would be a
// bug rather than a completion. A rewrite moves nothing: the source segments
// stay on the source channel until adoption drops them, so that view is already
// stable — and the rewrite outputs are new segments holding COPIES of the same
// rows. Merging them into the source delegator's view would make it serve every
// rewritten row twice. Each child loads its own outputs through its own target
// channel's recovery info instead.
func (m *shardSplitManager) SplitTargetsOfSource(vchannel string) []string {
	var targets []string
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(task) || rewrites(task) {
			// A rewriting task is skipped for the reason above: its outputs are
			// copies, and merging them in would serve every rewritten row twice.
			return true
		}
		if !slices.Contains(splitSourceVChannels(task), vchannel) {
			return true
		}
		targets = splitTargetVChannels(task.GetTargets())
		return false
	})
	return targets
}

// hasActiveTaskOnVChannel returns true if an unfinished task already works
// on the vchannel, either as the source or as a target.
func (m *shardSplitManager) hasActiveTaskOnVChannel(vchannel string) bool {
	found := false
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(task) {
			return true
		}
		if slices.Contains(splitSourceVChannels(task), vchannel) {
			found = true
			return false
		}
		for _, target := range task.GetTargets() {
			if target.GetVchannel() == vchannel {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// createTask persists a new split task in the Preparing state. The target
// shards (vchannel allocation and split point selection) are filled by the
// task executor, which can still abort the task before the write fence.
func (m *shardSplitManager) createTask(collectionID int64, stats *shardStats) (*datapb.SplitShardTask, error) {
	taskID, err := m.allocator.AllocID(m.ctx)
	if err != nil {
		return nil, err
	}
	task := &datapb.SplitShardTask{
		TaskId:       taskID,
		CollectionId: collectionID,
		Sources: []*datapb.SplitShardTaskSource{
			{Vchannel: stats.vchannel},
		},
		Redistribution: datapb.SplitShardRedistribution_SplitShardRelabel,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
		StartTime:      uint64(time.Now().Unix()),
	}
	if err := m.catalog.SaveSplitShardTask(m.ctx, task); err != nil {
		return nil, err
	}
	m.tasks.Insert(taskID, task)
	return task, nil
}

// isSplitShardTaskActive returns true if the task is not in a terminal state.
func isSplitShardTaskActive(task *datapb.SplitShardTask) bool {
	return task.GetState() != datapb.SplitShardTaskState_SplitShardTaskDone &&
		task.GetState() != datapb.SplitShardTaskState_SplitShardTaskAborted
}
