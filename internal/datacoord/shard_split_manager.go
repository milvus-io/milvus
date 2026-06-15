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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

	tasks *typeutil.ConcurrentMap[int64, *datapb.SplitShardTask] // task id -> task
}

// setCompactionPreempter wires the compaction inspector in; it is called
// once during the server initialization (the inspector is built after the
// shard split manager because it also consumes the freeze predicate).
func (m *shardSplitManager) setCompactionPreempter(preempter compactionPreempter) {
	m.preempter = preempter
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
		logger := log.Ctx(m.ctx).With(log.FieldComponent("shard-split-manager"))
		logger.Info("shard split manager started")
		for {
			interval := paramtable.Get().DataCoordCfg.ShardSplitCheckInterval.GetAsDuration(time.Second)
			select {
			case <-m.ctx.Done():
				logger.Info("shard split manager stopped")
				return
			case <-time.After(interval):
				m.detectOnce()
				m.advanceTasks()
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
	logger := log.Ctx(m.ctx).With(log.FieldComponent("shard-split-manager"))
	if !params.ShardSplitEnable.GetAsBool() {
		return
	}
	// TODO: reject the trigger when replication/CDC is enabled on the
	// cluster (D6); the fence path on the streamingnode is the backstop.
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
				logger.RatedWarn(60, "the shard over the split thresholds holds a single namespace, skip it",
					zap.String("vchannel", stats.vchannel),
					zap.Int64("size", stats.size),
					zap.Int64("rows", stats.rows))
				continue
			}
			task, err := m.createTask(collection.ID, stats)
			if err != nil {
				logger.Warn("create shard split task failed",
					zap.String("vchannel", stats.vchannel), zap.Error(err))
				continue
			}
			logger.Info("shard split task created",
				zap.Int64("taskID", task.GetTaskId()),
				zap.Int64("collectionID", collection.ID),
				zap.String("vchannel", stats.vchannel),
				zap.Int64("size", stats.size),
				zap.Int64("rows", stats.rows),
				zap.Int("namespaceCount", stats.namespaceCount))
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
	return m.hasActiveTaskOnVChannel(vchannel)
}

// SplitTargetsOfSource returns the target vchannels of the unfinished split
// task whose source is the given vchannel, or nil. It powers the merged
// recovery view: while the source shard is splitting, its recovery info
// reports the union of its remaining segments and the segments already
// relabeled to the targets, so a target refresh never sees a segment
// disappear mid-window.
func (m *shardSplitManager) SplitTargetsOfSource(vchannel string) []string {
	var targets []string
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(task) || task.GetSourceVchannel() != vchannel {
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
		if task.GetSourceVchannel() == vchannel {
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
		TaskId:         taskID,
		CollectionId:   collectionID,
		SourceVchannel: stats.vchannel,
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
