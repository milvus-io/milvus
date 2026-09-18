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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCollectCentroidCounts(t *testing.T) {
	binlogIO := mock_util.NewMockBinlogIO(t)
	first := marshalCentroidMapping(t, &clusteringpb.ClusteringCentroidIdMappingStats{
		CentroidIdMapping:  []uint32{0, 1, 0},
		NumInCentroid:      []int64{2, 1, 0},
		DistanceToCentroid: []float32{1, 2, 3},
	})
	second := marshalCentroidMapping(t, &clusteringpb.ClusteringCentroidIdMappingStats{
		CentroidIdMapping:  []uint32{2, 2, 1},
		NumInCentroid:      []int64{0, 1, 2},
		DistanceToCentroid: []float32{4, 5, 6},
	})
	binlogIO.EXPECT().Download(mock.Anything, []string{"mapping-10"}).Return([][]byte{first}, nil).Once()
	binlogIO.EXPECT().Download(mock.Anything, []string{"mapping-20"}).Return([][]byte{second}, nil).Once()

	task := &clusteringCompactionTask{
		binlogIO: binlogIO,
		plan: &datapb.CompactionPlan{
			AnalyzeSegmentIds: []int64{10, 20},
		},
		segmentIDOffsetMapping: map[int64]string{10: "mapping-10", 20: "mapping-20"},
	}
	counts, err := task.collectCentroidCounts(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 2, 2}, counts)
}

func TestCollectCentroidCountsValidatesAssignmentArtifact(t *testing.T) {
	binlogIO := mock_util.NewMockBinlogIO(t)
	invalid := marshalCentroidMapping(t, &clusteringpb.ClusteringCentroidIdMappingStats{
		CentroidIdMapping: []uint32{0},
		NumInCentroid:     []int64{1, 0},
	})
	binlogIO.EXPECT().Download(mock.Anything, []string{"mapping-10"}).Return([][]byte{invalid}, nil).Once()

	task := &clusteringCompactionTask{
		binlogIO: binlogIO,
		plan: &datapb.CompactionPlan{
			AnalyzeSegmentIds: []int64{10},
		},
		segmentIDOffsetMapping: map[int64]string{10: "mapping-10"},
	}
	_, err := task.collectCentroidCounts(context.Background(), 2)
	require.ErrorContains(t, err, "distance length mismatch")
}

func TestGetCompactionPlanParams(t *testing.T) {
	configured := map[string]string{
		"planner": "metis",
		"opaque":  "value",
	}
	params := getCompactionPlanParams(configured, 2048)
	require.Equal(t, map[string]string{
		"planner":             "metis",
		"opaque":              "value",
		"compaction_max_rows": "2048",
	}, params)
	require.NotContains(t, configured, "compaction_max_rows")

	configured["compaction_max_rows"] = "4096"
	params = getCompactionPlanParams(configured, 2048)
	require.Equal(t, "4096", params["compaction_max_rows"])
}

func marshalCentroidMapping(t *testing.T, stats *clusteringpb.ClusteringCentroidIdMappingStats) []byte {
	payload, err := proto.Marshal(stats)
	require.NoError(t, err)
	return payload
}
