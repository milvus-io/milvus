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

package compactor

import (
	"context"
	"math"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func (t *clusteringCompactionTask) getVectorAnalyzeResult(ctx context.Context) error {
	params := t.compactionParams.ClusteringPlan
	if params == nil || params.ClusterType == "" || len(params.SegmentRows) != len(t.plan.GetSegmentBinlogs()) {
		return merr.WrapErrServiceInternalMsg("vector compaction requires frozen planner parameters and input row counts")
	}
	inputs := make(map[int64]struct{}, len(params.SegmentRows))
	var total int64
	for _, segment := range t.plan.GetSegmentBinlogs() {
		id := segment.GetSegmentID()
		rows, exists := params.SegmentRows[id]
		_, duplicate := inputs[id]
		if !exists || duplicate || rows < 0 || rows > math.MaxInt64-total || segment.GetCollectionID() != t.collectionID || segment.GetPartitionID() != t.partitionID {
			return merr.WrapErrServiceInternalMsg("invalid planner input segment %d", id)
		}
		inputs[id] = struct{}{}
		total += rows
	}
	if total != t.plan.GetTotalRows() || len(t.plan.GetAnalyzeSegmentIds()) != len(inputs) {
		return merr.WrapErrServiceInternalMsg("planner inputs differ from Analyze inputs")
	}
	t.segmentIDOffsetMapping = make(map[int64]string, len(inputs))
	prefix := path.Join(t.plan.GetAnalyzeResultPath(), metautil.JoinIDPath(t.collectionID, t.partitionID, t.clusteringKeyField.GetFieldID()))
	for _, id := range t.plan.GetAnalyzeSegmentIds() {
		if _, exists := inputs[id]; !exists {
			return merr.WrapErrServiceInternalMsg("duplicate or unknown Analyze segment %d", id)
		}
		delete(inputs, id)
		t.segmentIDOffsetMapping[id] = path.Join(prefix, metautil.JoinIDPath(id), common.OffsetMapping)
	}
	payloads, err := t.binlogIO.Download(ctx, []string{path.Join(prefix, common.Centroids)})
	if err != nil {
		return err
	}
	if len(payloads) != 1 {
		return merr.WrapErrDataIntegrityMsg("missing clustering centroid payload")
	}
	centroids := &clusteringpb.ClusteringCentroidsStats{}
	if err := proto.Unmarshal(payloads[0], centroids); err != nil {
		return merr.WrapErrDataIntegrity(err, "decode clustering centroids")
	}
	dim, err := storage.GetDimFromParams(t.clusteringKeyField.GetTypeParams())
	if err != nil {
		return err
	}
	if err := clustercompaction.ValidateCentroids(centroids, t.clusteringKeyField.GetDataType(), int64(dim)); err != nil {
		return err
	}
	counts := make([]int64, len(centroids.GetCentroids()))
	// Retain only one segment's assignments at a time. No second count directory
	// or all-segment row mapping cache is needed for planning.
	for _, id := range t.plan.GetAnalyzeSegmentIds() {
		if err := ctx.Err(); err != nil {
			return err
		}
		payloads, err := t.binlogIO.Download(ctx, []string{t.segmentIDOffsetMapping[id]})
		if err != nil {
			return err
		}
		if len(payloads) != 1 {
			return merr.WrapErrDataIntegrityMsg("missing clustering assignment payload")
		}
		assignment := &clusteringpb.ClusteringCentroidIdMappingStats{}
		if err := proto.Unmarshal(payloads[0], assignment); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode clustering assignment")
		}
		if err := clustercompaction.ValidateAssignment(assignment, params.SegmentRows[id], int64(len(counts))); err != nil {
			return err
		}
		for offset, count := range assignment.GetNumInCentroid() {
			counts[offset] += count
		} // Bounded by total.
	}
	return t.switchPolicyForVectorPlan(ctx, centroids, counts)
}

func (t *clusteringCompactionTask) switchPolicyForVectorPlan(ctx context.Context, centroids *clusteringpb.ClusteringCentroidsStats, counts []int64) error {
	options := t.compactionParams.ClusteringPlan
	if options == nil {
		return merr.WrapErrServiceInternalMsg("missing frozen clustering planner parameters")
	}
	layout, err := analyzecgowrapper.BuildCompactionPlan(ctx, centroids, t.clusteringKeyField.GetDataType(), counts,
		options.ClusterType, options.Params, t.plan.GetMaxSegmentRows())
	if err != nil {
		return err
	}
	return t.generatedVectorPlan(ctx, centroids, layout)
}

func (t *clusteringCompactionTask) generatedVectorPlan(ctx context.Context, centroids *clusteringpb.ClusteringCentroidsStats, layout *clustercompaction.LayoutPlan) error {
	if err := layout.Validate(); err != nil {
		return err
	}
	if len(layout.Counts) != len(centroids.GetCentroids()) {
		return merr.WrapErrDataIntegrityMsg("layout centroid count mismatch")
	}
	t.vectorLayout = layout
	// Grouping is independent of memory capacity. The current mapper needs one
	// resident writer per group; reject excessive fan-out until PR4 supplies
	// bounded spill/merge. Never combine groups to fit memory.
	capacity := t.memoryLimit / max(int64(expectedBinlogSize), int64(t.compactionParams.BinLogMaxSize))
	if int64(len(layout.Groups)) > capacity {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "clustering plan has %d groups, resident mapper supports %d; bounded spill mapping is required", len(layout.Groups), capacity)
	}
	groupIndex := make([]int, len(layout.Counts))
	for i := range groupIndex {
		groupIndex[i] = -1
	}
	for ordinal, group := range layout.Groups {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.GetFieldID(), t.clusteringKeyField.GetDataType(), 0)
		if err != nil {
			return err
		}
		values := make([]storage.VectorFieldValue, len(group.Offsets))
		for i, offset := range group.Offsets {
			groupIndex[offset] = ordinal
			values[i] = storage.NewVectorFieldValue(t.clusteringKeyField.GetDataType(), centroids.GetCentroids()[offset])
		}
		fieldStats.SetVectorCentroids(values...)
		writer, err := NewMultiSegmentWriter(ctx, t.binlogIO, NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc),
			t.plan.GetMaxSize(), t.plan.GetSchema(), t.compactionParams, t.plan.GetMaxSegmentRows(),
			t.partitionID, t.collectionID, t.plan.GetChannel(), 100, t.getWriterOpts()...)
		if err != nil {
			return err
		}
		t.clusterBuffers = append(t.clusterBuffers, newClusterBuffer(ordinal, writer, fieldStats))
	}
	t.offsetToBufferFunc = func(row int64, assignment []uint32) *ClusterBuffer {
		// Assignments were validated before mapping; empty offsets never occur.
		return t.clusterBuffers[groupIndex[assignment[row]]]
	}

	var rows, estimatedCapacity float64
	if target := t.plan.GetMaxSegmentRows(); target > 0 {
		for _, group := range layout.Groups {
			rows += float64(group.Rows)
			estimatedCapacity += math.Ceil(float64(group.Rows)/float64(target)) * float64(target)
		}
	}
	mlog.Info(ctx, "clustering layout planned", mlog.Int("clusters", len(layout.Counts)), mlog.Int("groups", len(layout.Groups)), mlog.Float64("estimatedSegmentFillRatio", rows/max(1, estimatedCapacity)))
	return nil
}
