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
	"encoding/binary"
	"io"
	"math"
	"path"
	"slices"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/internal/util/clusteringindex"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (r *clusteringPlanReader) Upload(ctx context.Context, files map[string][]byte) error {
	for key, value := range files {
		r.files[key] = append([]byte(nil), value...)
	}
	return nil
}

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
	s.plan.Channel = "clustering-test"
	s.plan.TotalRows = 10240
	s.plan.AnalyzeResultPath = "analyze_stats/80/3"
	segment := s.plan.SegmentBinlogs[0]
	segment.PartitionID = PartitionID
	s.plan.AnalyzeSegmentIds = []int64{segment.SegmentID}
	s.task.compactionParams.ClusteringPlan = &compaction.ClusteringPlanParameters{
		ClusterType: "CARDINAL_KMEANS", IndexParams: map[string]string{"index_type": "FLAT", "metric_type": "L2"}, IndexEngineVersion: 10,
		Params: map[string]string{"random_state": "42", "search_quant_type": "NONE", "refine_quant_type": "NONE"}, SegmentRows: map[int64]int64{segment.SegmentID: 10240},
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
		assignment.DistanceToCentroid[i] = float32(10240 - i)
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
	s.Empty(s.task.clusterBuffers, "planning does not allocate resident group writers")
	segments, stats, err := s.task.mapping(context.Background())
	s.Require().NoError(err)
	s.NotNil(stats.ClusteringStats, "mapping collects final group segment IDs")
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
		if slices.Contains(s.task.vectorLayout.Groups[ordinal].Offsets, assignment.CentroidIdMapping[100]) {
			expected--
		} // Deleted source row consumes its assignment.
		s.Equal(expected, rows)
		total += rows
	}
	s.Equal(int64(10239), total)
	s.Len(segments, len(owners))

	// Read the actual output files: ranges address physical rows, and distance
	// order must survive record and segment boundaries.
	for _, output := range segments {
		rr, err := storage.NewBinlogRecordReader(context.Background(), output.InsertLogs, s.plan.Schema,
			storage.WithVersion(output.StorageVersion), storage.WithStorageConfig(s.task.compactionParams.StorageConfig), storage.WithDownloader(reader.Download))
		s.Require().NoError(err)
		physical := int64(0)
		previousCluster := uint32(math.MaxUint32)
		previousDistance := float32(-1)
		for {
			rec, err := rr.Next()
			if err == io.EOF {
				break
			}
			s.Require().NoError(err)
			for row := 0; row < rec.Len(); row++ {
				pk := rec.Column(100).(*array.Int64).Value(row)
				s.NotEqual(int64(100), pk)
				cluster := assignment.CentroidIdMapping[pk]
				distance := assignment.DistanceToCentroid[pk]
				if cluster == previousCluster {
					s.GreaterOrEqual(distance, previousDistance)
				}
				previousCluster, previousDistance = cluster, distance
				covered := false
				for _, interval := range stats.SegmentStats[output.SegmentID].ClusteringRanges {
					if physical >= interval.RowOffset && physical < interval.RowOffset+interval.RowCount {
						s.Equal(cluster, interval.ClusterOffset)
						covered = true
					}
				}
				s.True(covered)
				physical++
			}
		}
		rr.Close()
		s.Equal(output.NumOfRows, physical)
	}
	params := paramtable.Get()
	s.Require().NoError(initcore.InitLocalChunkManager(params.LocalStorageCfg.Path.GetValue()))
	s.Require().NoError(initcore.InitRemoteChunkManager(params))
	s.Require().NoError(initcore.InitRemoteArrowFileSystem(params))
	initcore.InitTieredStorage(params)
	full, err := s.task.buildClusteringSnapshot(context.Background(), stats)
	s.Require().NoError(err)
	s.Require().NoError(full.ValidateClusteringStats())
	s.Require().NoError(s.task.uploadPartitionStats(context.Background(), CollectionID, PartitionID, full))
	native, err := clusteringindex.Load(context.Background(), CollectionID, PartitionID, s.plan.Channel, s.plan.PlanID, s.task.clusteringKeyField, full.ClusteringStats.ClusteringIndex, int64(full.ClusteringStats.NumClusters))
	s.Require().NoError(err)
	defer native.Close()
	query := make([]byte, 16)
	for i, v := range centroids.Centroids[0].GetFloatVector().Data {
		binary.LittleEndian.PutUint32(query[4*i:], math.Float32bits(v))
	}
	labels, _, err := native.Search(context.Background(), query, 1, 1)
	s.Require().NoError(err)
	s.Equal([]int64{0}, labels)
	// Entire selected group disappears after deletes; retain all other groups.
	removed := full.ClusteringStats.Groups[0].SegmentIds
	s.task.plan.BasePartitionStatsVersion = s.plan.PlanID
	s.task.plan.PlanID++
	s.task.plan.SegmentBinlogs = nil
	for _, id := range removed {
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{SegmentID: id})
	}
	next, err := s.task.buildClusteringSnapshot(context.Background(), &storage.PartitionStatsSnapshot{SegmentStats: map[int64]storage.SegmentStats{}, ClusteringStats: &clusteringpb.PartitionClusteringStats{}})
	s.Require().NoError(err)
	s.Require().NoError(next.ValidateClusteringStats())
	s.Len(next.SegmentStats, len(full.SegmentStats)-len(removed))
	for _, id := range removed {
		s.Contains(full.SegmentStats, id)
		s.NotContains(next.SegmentStats, id)
	}
}

func TestClusteringPlannerDoesNotAllocateResidentGroups(t *testing.T) {
	layout := &clustercompaction.LayoutPlan{Counts: []int64{1, 1}, Groups: []clustercompaction.ClusterGroup{{Offsets: []uint32{0}, Rows: 1}, {Offsets: []uint32{1}, Rows: 1}}}
	task := &clusteringCompactionTask{memoryLimit: expectedBinlogSize}
	err := task.generatedVectorPlan(context.Background(), &clusteringpb.ClusteringCentroidsStats{Centroids: make([]*schemapb.VectorField, 2)}, layout)
	require.NoError(t, err)
	require.Empty(t, task.clusterBuffers, "group count does not allocate writers before mapping")
	require.Same(t, layout, task.vectorLayout, "retain logical grouping independently of buffer capacity")
}
