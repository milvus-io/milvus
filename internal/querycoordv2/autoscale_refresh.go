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

package querycoordv2

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/autoscale"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type refreshSegmentReplica struct {
	segmentID int64
	replicaID int64
}

type refreshLoadDemand struct {
	gate     *syncutil.Semaphore
	version  qviews.DataVersion
	reported typeutil.Set[refreshSegmentReplica]
}

// recordRefreshLoadDemand runs after publication, while QueryNodes can still be
// preparing the new DataView. Estimation is advisory and never rejects a refresh.
func (s *Server) recordRefreshLoadDemand(ctx context.Context, cfg *loadmgr.LoadConfig, version *viewpb.DataVersion) {
	params := &paramtable.Get().QueryCoordCfg
	if !params.AutoscalePrecheckEnabled.GetAsBool() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, params.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	runtime := s.qviewsRuntime
	collectionID := cfg.CollectionID
	if runtime.loadConfigStore.GetConfigVersion(collectionID) != cfg.Version {
		return
	}
	value, ok := runtime.refreshDemand.Load(collectionID)
	if !ok {
		value, _ = runtime.refreshDemand.LoadOrStore(collectionID, &refreshLoadDemand{gate: syncutil.NewSemaphore(1)})
	}
	state := value.(*refreshLoadDemand)
	if err := state.gate.Acquire(ctx); err != nil {
		return
	}
	defer state.gate.Release()
	currentVersion := runtime.loadConfigStore.GetConfigVersion(collectionID)
	if currentVersion == 0 {
		runtime.refreshDemand.CompareAndDelete(collectionID, state)
		return
	}
	dataVersion := qviews.FromProtoDataVersion(version)
	if currentVersion != cfg.Version || (state.reported != nil && state.version.GTE(dataVersion)) {
		return
	}

	ref, err := runtime.resources.DataViews.Get(ctx, collectionID, version)
	if err != nil {
		mlog.Warn(ctx, "skip refresh load demand because DataView lookup failed", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return
	}
	if ref == nil {
		// A newer publication can supersede this version before we acquire it.
		return
	}
	defer ref.Deref()

	stats := runtime.shardViewRegistry.SnapshotForCollection(collectionID).StatsMap()
	candidates, reported, ready := refreshMissingSegments(ref.DataView(), dataVersion, cfg, stats, state.reported)
	if !ready {
		return
	}
	estimates, err := s.estimateRefreshLoadDemand(ctx, cfg, slices.Collect(maps.Keys(candidates)))
	if err != nil {
		mlog.Warn(ctx, "skip refresh load demand because estimation failed", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return
	}
	requiredByRG := make(map[string]autoscaleResourceUsage)
	for segmentID, estimate := range estimates {
		for _, replica := range candidates[segmentID] {
			rg := replica.ResourceGroup
			total := requiredByRG[rg]
			total.add(estimate)
			requiredByRG[rg] = total
			reported.Insert(refreshSegmentReplica{segmentID: segmentID, replicaID: replica.ReplicaID})
		}
	}
	currentState, _ := runtime.refreshDemand.Load(collectionID)
	if ctx.Err() != nil || currentState != state || runtime.loadConfigStore.GetConfigVersion(collectionID) != cfg.Version {
		return
	}
	recordLoadResourceDemand(requiredByRG)
	state.version, state.reported = dataVersion, reported
}

// refreshMissingSegments counts each segment once per replica even if it is
// assigned to several nodes. Keep reported entries only while they are pending
// in the current target so the next refresh cannot charge them again.
func refreshMissingSegments(
	view *viewpb.DataViewOfCollection,
	version qviews.DataVersion,
	cfg *loadmgr.LoadConfig,
	stats map[qviews.ShardID]*coordview.ShardStats,
	previouslyReported typeutil.Set[refreshSegmentReplica],
) (map[int64][]*loadmgr.ReplicaAssignment, typeutil.Set[refreshSegmentReplica], bool) {
	partitions := typeutil.NewSet(cfg.PartitionIDs...)
	candidates := make(map[int64][]*loadmgr.ReplicaAssignment)
	reported := typeutil.NewSet[refreshSegmentReplica]()
	for _, replica := range cfg.Replicas {
		for _, shard := range view.GetShards() {
			stat := stats[qviews.ShardID{VChannel: shard.GetVchannel(), ReplicaID: replica.ReplicaID}]
			if stat == nil || stat.UpVersion == nil || stat.UpLoadInfoVersion != cfg.Version {
				return nil, nil, false
			}
			if stat.UpVersion.DataVersion.GTE(version) {
				continue
			}
			for _, partition := range shard.GetPartitions() {
				if !partitions.Contain(partition.GetPartitionId()) {
					continue
				}
				for _, segmentID := range partition.GetSegmentIds() {
					if refreshSegmentResident(stat.Segments[segmentID]) {
						continue
					}
					key := refreshSegmentReplica{segmentID: segmentID, replicaID: replica.ReplicaID}
					if previouslyReported.Contain(key) {
						reported.Insert(key)
						continue
					}
					candidates[segmentID] = append(candidates[segmentID], replica)
				}
			}
		}
	}
	return candidates, reported, true
}

func refreshSegmentResident(segment *coordview.SegmentStats) bool {
	if segment != nil {
		for _, state := range segment.Nodes {
			if state >= coordview.SegmentStateReady {
				return true
			}
		}
	}
	return false
}

func (s *Server) estimateRefreshLoadDemand(ctx context.Context, cfg *loadmgr.LoadConfig, segmentIDs []int64) (map[int64]autoscaleResourceUsage, error) {
	if len(segmentIDs) == 0 {
		return nil, nil
	}
	collectionID := cfg.CollectionID
	collection, err := s.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	segments, err := s.broker.GetSegmentInfo(ctx, segmentIDs...)
	if err != nil {
		return nil, err
	}
	indexes, err := getIndexInfoBySegments(ctx, s.broker, collectionID, segmentIDs)
	if err != nil {
		return nil, err
	}
	schema := task.ApplyCollectionMmapSetting(typeutil.Clone(collection.GetSchema()), collection.GetProperties())
	// An empty QueryView preload set means system fields only. The shared
	// estimator otherwise interprets an empty list as all fields.
	loadFields := append(qviewsLoadFieldIDs(cfg), common.RowIDField)
	segments = autoscale.FilterSegmentsByLoadFields(segments, loadFields)
	indexes = autoscale.FilterIndexesByLoadFields(indexes, loadFields)
	options := autoscale.DefaultEstimateOptions()
	estimates := make(map[int64]autoscaleResourceUsage, len(segments))
	for _, segment := range segments {
		usage, err := autoscale.EstimateSegmentsLoadResource(ctx, schema, []*datapb.SegmentInfo{segment}, indexes, options)
		if err != nil {
			return nil, err
		}
		estimate := autoscaleResourceUsage{memoryBytes: usage.MemoryBytes, diskBytes: usage.DiskBytes}
		if !estimate.isZero() {
			estimates[segment.GetID()] = estimate
		}
	}
	return estimates, nil
}
