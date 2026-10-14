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
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (t *clusteringCompactionTask) snapshotLayout() bool {
	return typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().GetDataType())
}

func (t *clusteringCompactionTask) committedSnapshot() *datapb.PartitionStatsInfo {
	task := t.GetTaskProto()
	info := t.meta.GetPartitionStatsMeta().GetPartitionStats(task.GetCollectionID(), task.GetPartitionID(), task.GetChannel(), task.GetPlanID())
	if info.GetClusteringFormatVersion() == 0 {
		return nil
	}
	return info
}

func (t *clusteringCompactionTask) dispatchSnapshotTask(node int64, cluster session.Cluster) error {
	// NodeID is written before the RPC. Recovery only polls it, including the
	// crash window before the RPC; no second writer may reuse this generation.
	if t.GetTaskProto().GetNodeID() > 0 {
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	}
	if _, err := t.validateClusteringBase(context.Background()); err != nil {
		return err
	}
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		return err
	}
	t.plan = plan
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(node)); err != nil {
		return err
	}
	if err := cluster.CreateCompaction(node, plan, t.GetTaskProto().GetCollectionID()); err != nil {
		mlog.Warn(context.TODO(), "snapshot dispatch outcome is unknown; poll the recorded worker", mlog.Int64("planID", plan.GetPlanID()), mlog.Int64("nodeID", node), mlog.Err(err))
	}
	return nil
}

func (t *clusteringCompactionTask) completeSnapshotTask() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	task := t.GetTaskProto()
	m := t.meta.(*meta)
	receipt := t.committedSnapshot()
	if receipt == nil {
		base, err := t.validateClusteringBase(ctx)
		if err != nil {
			return err
		}
		key := path.Join(m.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(task.GetCollectionID(), task.GetPartitionID()), task.GetChannel(), strconv.FormatInt(task.GetPlanID(), 10))
		blob, err := m.chunkManager.Read(ctx, key)
		if err != nil {
			return err
		}
		stats, err := storage.DeserializePartitionsStatsSnapshot(blob)
		if err != nil {
			return err
		}
		c := stats.ClusteringStats
		dim, dimErr := storage.GetDimFromParams(task.GetClusteringKeyField().GetTypeParams())
		if dimErr != nil {
			return dimErr
		}
		if c.GetDim() != int64(dim) || stats.Version != task.GetPlanID() || c == nil || c.GetFieldId() != task.GetClusteringKeyField().GetFieldID() || c.GetDataType() != task.GetClusteringKeyField().GetDataType() {
			return merr.WrapErrDataIntegrityMsg("clustering snapshot receipt differs from task")
		}
		expected := make(map[int64]int64)
		if base != nil && base.ClusteringStats != nil {
			for id, segment := range base.SegmentStats {
				expected[id] = int64(segment.NumRows)
			}
		}
		for _, id := range task.GetInputSegments() {
			delete(expected, id)
		}
		for _, id := range task.GetResultSegments() {
			segment := m.GetSegment(ctx, id)
			if segment == nil || segment.GetPartitionStatsVersion() != task.GetPlanID() || segment.GetCollectionID() != task.GetCollectionID() || segment.GetPartitionID() != task.GetPartitionID() || segment.GetInsertChannel() != task.GetChannel() {
				return merr.WrapErrDataIntegrityMsg("snapshot output scope differs")
			}
			if _, duplicate := expected[id]; duplicate {
				return merr.WrapErrDataIntegrityMsg("snapshot output reuses retained segment")
			}
			expected[id] = segment.GetNumOfRows()
		}
		if len(expected) != len(stats.SegmentStats) {
			return merr.WrapErrDataIntegrityMsg("snapshot coverage differs from retained and output segments")
		}
		ids := make([]int64, 0, len(expected))
		for id, rows := range expected {
			segment, exists := stats.SegmentStats[id]
			if !exists || int64(segment.NumRows) != rows {
				return merr.WrapErrDataIntegrityMsg("snapshot row count differs for segment %d", id)
			}
			ids = append(ids, id)
		}
		slices.Sort(ids)
		if c.GetNumClusters() > 0 {
			if c.GetClustersFile() != key+".clusters" {
				return merr.WrapErrDataIntegrityMsg("snapshot cluster file has a different owner")
			}
			keys := append([]string{c.GetClustersFile()}, c.GetClusteringIndex().GetIndexFilePaths()...)
			for i, object := range keys {
				if i > 0 && !strings.HasPrefix(object, key+".clustering_index/") {
					return merr.WrapErrDataIntegrityMsg("snapshot index file has a different owner")
				}
				size, err := m.chunkManager.Size(ctx, object)
				if err != nil {
					return err
				}
				if size <= 0 {
					return merr.WrapErrDataIntegrityMsg("snapshot artifact is empty")
				}
			}
		}
		receipt = &datapb.PartitionStatsInfo{
			CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(), VChannel: task.GetChannel(), Version: task.GetPlanID(), SegmentIDs: ids,
			InputSegmentIds: append([]int64(nil), task.GetInputSegments()...), ClusteringFormatVersion: c.GetFormatVersion(), CommitTime: time.Now().Unix(),
		}
	}
	// Reader and publisher take the same lock order: segment view, then stats.
	// The two-key catalog publication is atomic. Once it succeeds it is the
	// authority even if the following visibility cleanup fails or the process dies.
	m.segMu.Lock()
	if t.committedSnapshot() == nil {
		for _, id := range task.GetInputSegments() {
			s := m.segments.GetSegment(id)
			if s == nil || !isFlushed(s) || s.GetIsInvisible() || s.GetCollectionID() != task.GetCollectionID() || s.GetPartitionID() != task.GetPartitionID() || s.GetInsertChannel() != task.GetChannel() {
				m.segMu.Unlock()
				return merr.WrapErrCompactionPlanConflict("snapshot input changed before publication")
			}
		}
	}
	err := m.partitionStatsMeta.SavePartitionStatsAndVersion(receipt, task.GetPlanID(), task.GetBasePartitionStatsVersion())
	if t.committedSnapshot() != nil {
		err = nil
	} // already adopted, possibly followed by another generation
	if err == nil {
		err = t.applySnapshotVisibilityLocked(ctx, m)
	}
	m.segMu.Unlock()
	if err != nil {
		return err
	}
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		return err
	}
	m.recomputeDataView(ctx, task.GetCollectionID())
	return nil
}

func (t *clusteringCompactionTask) applySnapshotVisibilityLocked(ctx context.Context, m *meta) error {
	task := t.GetTaskProto()
	operators := make([]UpdateOperator, 0, len(task.GetInputSegments())+len(task.GetResultSegments()))
	for _, id := range task.GetResultSegments() {
		operators = append(operators, SetSegmentIsInvisible(id, false))
	}
	for _, id := range task.GetInputSegments() {
		operators = append(operators, UpdateStatusOperator(id, commonpb.SegmentState_Dropped))
	}
	pack, err := m.buildUpdateSegmentPack(ctx, operators)
	if err != nil || pack == nil {
		return err
	}
	segments := make([]*datapb.SegmentInfo, 0, len(pack.segments))
	for _, s := range pack.segments {
		segments = append(segments, s.SegmentInfo)
	}
	if err := m.catalog.AlterSegments(ctx, segments); err != nil {
		return err
	}
	pack.metricMutation.commit()
	for id, s := range pack.segments {
		m.segments.SetSegment(id, s)
	}
	return nil
}

func (t *clusteringCompactionTask) cleanSnapshotTask() error {
	if t.committedSnapshot() != nil {
		if err := t.completeSnapshotTask(); err != nil {
			return err
		}
	} else {
		var ops []UpdateOperator
		for _, id := range append(append([]int64(nil), t.GetTaskProto().GetTmpSegments()...), t.GetTaskProto().GetResultSegments()...) {
			ops = append(ops, UpdateStatusOperator(id, commonpb.SegmentState_Dropped))
		}
		if err := t.meta.UpdateSegmentsInfo(context.Background(), ops...); err != nil {
			return err
		}
		// Worker termination and distributed snapshot references are not proven.
		// Keep this generation's objects; later work can add acknowledged reclamation.
	}
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned)); err != nil {
		return err
	}
	t.resetSegmentCompacting()
	return nil
}

// Called with segMu held. The committed receipt defines the segment projection
// during interrupted visibility cleanup. Never infer the binding from each
// segment's original PartitionStatsVersion: retained segments keep that value.
func (m *meta) projectClusteringSegmentsLocked(segments []*SegmentInfo) []*SegmentInfo {
	if m.partitionStatsMeta == nil {
		return segments
	}
	m.partitionStatsMeta.RLock()
	defer m.partitionStatsMeta.RUnlock()
	type projection struct{ current, retired map[int64]bool }
	scopes := make(map[*partitionStatsInfo]projection)
	result := make([]*SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		channelScopes := m.partitionStatsMeta.partitionStatsInfos[segment.GetInsertChannel()]
		scope := channelScopes[segment.GetPartitionID()]
		if scope == nil {
			result = append(result, segment)
			continue
		}
		current := scope.infos[scope.currentVersion]
		if current.GetClusteringFormatVersion() == 0 {
			result = append(result, segment)
			continue
		}
		p, exists := scopes[scope]
		if !exists {
			p = projection{map[int64]bool{}, map[int64]bool{}}
			for _, id := range current.GetSegmentIDs() {
				p.current[id] = true
			}
			for _, receipt := range scope.infos {
				if receipt.GetClusteringFormatVersion() > 0 {
					for _, id := range receipt.GetInputSegmentIds() {
						p.retired[id] = true
					}
				}
			}
			scopes[scope] = p
		}
		if p.current[segment.GetID()] {
			clone := segment.Clone()
			clone.IsInvisible = false
			clone.State = commonpb.SegmentState_Flushed
			result = append(result, clone)
			continue
		}
		if p.retired[segment.GetID()] {
			clone := segment.Clone()
			clone.State = commonpb.SegmentState_Dropped
			result = append(result, clone)
			continue
		}
		// Unpublished outputs remain invisible; unrelated fresh segments retain
		// their ordinary query path and cannot be pruned by the snapshot.
		result = append(result, segment)
	}
	return result
}

func (m *meta) clusteringRecoveryView(collection int64, channel string) ([]*SegmentInfo, map[int64]int64) {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	segments := m.projectClusteringSegmentsLocked(m.segments.GetRealSegmentsForChannel(channel))
	versions := m.partitionStatsMeta.GetChannelPartitionsStatsVersion(collection, channel)
	return segments, versions
}

// All entry points converge here, including manual, storage/schema rewrite and
// target-based compaction. A physical rewrite of a covered segment must replace
// the complete clustering group and publish a fresh snapshot.
func (c *compactionInspector) routeClusteringRewrite(task *datapb.CompactionTask) (*datapb.CompactionTask, error) {
	if task.GetType() == datapb.CompactionType_ClusteringCompaction || task.GetType() == datapb.CompactionType_Level0DeleteCompaction {
		return task, nil
	}
	ctx := context.Background()
	var covered bool
	for _, id := range task.GetInputSegments() {
		s := c.meta.GetSegment(ctx, id)
		if s != nil && s.GetLevel() == datapb.SegmentLevel_L2 {
			covered = true
			break
		}
	}
	if !covered {
		return task, nil
	}
	collection, err := c.handler.GetCollection(ctx, task.GetCollectionID())
	if err != nil {
		return nil, err
	}
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(task.GetCollectionID())
	}
	field := clustering.GetClusteringKeyField(collection.Schema)
	if field == nil {
		return nil, merr.WrapErrCompactionPlanConflict("L2 rewrite requires an enabled clustering field")
	}
	m := c.meta.(*meta)
	view := &ClusteringSegmentsView{
		label:    &CompactionGroupLabel{CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(), Channel: task.GetChannel()},
		segments: GetViewsByInfo(c.meta.GetSegmentInfos(task.GetInputSegments())...), clusteringKeyField: field, collectionTTL: time.Duration(task.GetCollectionTtl()), triggerID: task.GetTriggerID(),
	}
	if typeutil.IsVectorType(field.GetDataType()) {
		view, err = prepareClusteringView(ctx, m, view, collection.Schema.GetEnableNamespace())
		if err != nil {
			return nil, err
		}
	}
	size := getExpectedSegmentSize(m, collection.ID, collection.Schema)
	total, maxRows, preferRows, err := calculateClusteringCompactionConfig(collection, view, size)
	if err != nil {
		return nil, err
	}
	_, analyzeID, outputs, err := allocClusteringCompactionPlanIDs(c.allocator, view.GetTotalSize(), float64(size)*paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat())
	if err != nil {
		return nil, err
	}
	next := proto.Clone(task).(*datapb.CompactionTask)
	next.Type = datapb.CompactionType_ClusteringCompaction
	next.Schema = proto.Clone(collection.Schema).(*schemapb.CollectionSchema)
	next.ClusteringKeyField = proto.Clone(field).(*schemapb.FieldSchema)
	next.InputSegments = nil
	for _, segment := range view.segments {
		next.InputSegments = append(next.InputSegments, segment.ID)
	}
	next.MaxSize, next.TotalRows, next.MaxSegmentRows, next.PreferSegmentRows = size, total, maxRows, preferRows
	next.PreAllocatedSegmentIDs, next.AnalyzeTaskID, next.BasePartitionStatsVersion = outputs, analyzeID, view.basePartitionStatsVersion
	next.ClusteringPlanParams = maps.Clone(paramtable.Get().KnowhereConfig.GetCompactionPlanParams())
	next.ClusteringIndexParams = maps.Clone(paramtable.Get().KnowhereConfig.GetClusterIndexParams())
	if typeutil.IsVectorType(field.GetDataType()) {
		next.ClusteringIndexEngineVersion = c.ievm.ResolveVecIndexVersion()
	}
	return next, nil
}

// Without distributed leases, neither a committed reader nor an uncertain
// worker can be proven to have released these physical files.
func (m *meta) clusteringSegmentReferenced(segment *SegmentInfo) bool {
	if version := segment.GetPartitionStatsVersion(); version > 0 {
		if m.partitionStatsMeta == nil {
			return true
		}
		info := m.partitionStatsMeta.GetPartitionStats(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetInsertChannel(), version)
		if info == nil || info.GetClusteringFormatVersion() > 0 {
			return true
		}
	}
	if m.partitionStatsMeta == nil {
		return false
	}
	m.partitionStatsMeta.RLock()
	defer m.partitionStatsMeta.RUnlock()
	scope := m.partitionStatsMeta.partitionStatsInfos[segment.GetInsertChannel()][segment.GetPartitionID()]
	if scope == nil {
		return false
	}
	for _, receipt := range scope.infos {
		if receipt.GetClusteringFormatVersion() > 0 && (slices.Contains(receipt.GetSegmentIDs(), segment.GetID()) || slices.Contains(receipt.GetInputSegmentIds(), segment.GetID())) {
			return true
		}
	}
	return false
}
