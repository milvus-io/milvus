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
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

type clusteringPlanReader struct {
	binlogio.BinlogIO
	files map[string][]byte
	reads []string
}

func (r *clusteringPlanReader) Download(ctx context.Context, paths []string) ([][]byte, error) {
	if len(paths) == 1 {
		if blob, exists := r.files[paths[0]]; exists {
			r.reads = append(r.reads, paths[0])
			return [][]byte{blob}, nil
		}
	}
	return r.BinlogIO.Download(ctx, paths)
}

func (s *ClusteringCompactionTaskSuite) TestVectorPlannerFeedsMapping() {
	s.preparScalarCompactionNormalTask()
	s.plan.ClusteringKeyField = 103
	s.plan.TotalRows = 10240
	s.plan.AnalyzeResultPath = "analyze_stats/80/3"
	segment := s.plan.SegmentBinlogs[0]
	segment.PartitionID = PartitionID
	s.plan.AnalyzeSegmentIds = []int64{segment.SegmentID}
	s.task.compactionParams.ClusteringPlan = &compaction.ClusteringPlanParameters{
		ClusterType: "CARDINAL_KMEANS",
		Params:      map[string]string{"random_state": "42", "search_quant_type": "NONE", "refine_quant_type": "NONE"}, SegmentRows: map[int64]int64{segment.SegmentID: 10240},
	}
	s.Require().NoError(s.task.init())
	defer s.task.cleanUp(context.Background())
	s.task.memoryLimit = 1 << 30
	centroids := &clusteringpb.ClusteringCentroidsStats{}
	for i := 0; i < 4096; i++ {
		centroids.Centroids = append(centroids.Centroids, &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{float32(i%2*10000 + i/2), float32(i % 31), float32(i % 11), 1}}}})
	}
	assignment := &clusteringpb.ClusteringCentroidIdMappingStats{NumInCentroid: make([]int64, 4096), CentroidIdMapping: make([]uint32, 10240), DistanceToCentroid: make([]float32, 10240)}
	for i := range assignment.CentroidIdMapping {
		assignment.CentroidIdMapping[i] = uint32(i % 4096)
		assignment.NumInCentroid[i%4096]++
	}
	centroidBytes, err := proto.Marshal(centroids)
	s.Require().NoError(err)
	assignmentBytes, err := proto.Marshal(assignment)
	s.Require().NoError(err)
	prefix := path.Join(s.plan.AnalyzeResultPath, metautil.JoinIDPath(CollectionID, PartitionID, 103))
	reader := &clusteringPlanReader{BinlogIO: s.mockBinlogIO, files: map[string][]byte{
		path.Join(prefix, common.Centroids):                                             centroidBytes,
		path.Join(prefix, metautil.JoinIDPath(segment.SegmentID), common.OffsetMapping): assignmentBytes,
	}}
	s.task.binlogIO = reader
	s.Require().NoError(s.task.getVectorAnalyzeResult(context.Background()))
	s.Require().NoError(s.task.vectorLayout.Validate())
	s.Len(reader.reads, 2, "planner reads one assignment file at a time")
	s.Len(s.task.clusterBuffers, len(s.task.vectorLayout.Groups))
	for ordinal, group := range s.task.vectorLayout.Groups {
		for _, offset := range group.Offsets {
			s.Same(s.task.clusterBuffers[ordinal], s.task.offsetToBufferFunc(0, []uint32{offset}))
		}
	}
	segments, stats, err := s.task.mapping(context.Background())
	s.Require().NoError(err)
	s.Nil(stats.ClusteringStats, "PR3 must not publish the new snapshot format")
	owners := map[int64]int{}
	var total int64
	for ordinal, buffer := range s.task.clusterBuffers {
		var rows int64
		for _, segment := range buffer.GetCompactionSegments() {
			_, exists := owners[segment.SegmentID]
			s.False(exists, "different groups must never share output segments")
			owners[segment.SegmentID] = ordinal
			rows += segment.NumOfRows
		}
		expected := s.task.vectorLayout.Groups[ordinal].Rows
		if buffer == s.task.offsetToBufferFunc(100, assignment.CentroidIdMapping) {
			expected--
		} // Deleted source row consumes its assignment.
		s.Equal(expected, rows)
		total += rows
	}
	s.Equal(int64(10239), total)
	s.Len(segments, len(owners))
}

func TestClusteringPlannerRejectsExcessiveResidentGroups(t *testing.T) {
	layout := &clustercompaction.LayoutPlan{Counts: []int64{1, 1}, Groups: []clustercompaction.ClusterGroup{{Offsets: []uint32{0}, Rows: 1}, {Offsets: []uint32{1}, Rows: 1}}}
	task := &clusteringCompactionTask{memoryLimit: expectedBinlogSize}
	err := task.generatedVectorPlan(context.Background(), &clusteringpb.ClusteringCentroidsStats{Centroids: make([]*schemapb.VectorField, 2)}, layout)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.Empty(t, task.clusterBuffers, "reject before allocating any group writer")
	require.Same(t, layout, task.vectorLayout, "retain logical grouping independently of buffer capacity")
}
