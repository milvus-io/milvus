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

package delegator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

var benchmarkClusterHintsSink map[int64]*querypb.SegmentClusterSearchHints

func BenchmarkClusteringSearchHints(b *testing.B) {
	for _, nq := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprint(nq), func(b *testing.B) {
			bindings := map[int64]*partitionStatsBinding{}
			rows := map[int64]int64{}
			scope := []SnapshotItem{{NodeID: 1}}
			labels := make([]int64, 2*nq)
			for q := 0; q < nq; q++ {
				labels[2*q] = 0
				labels[2*q+1] = 2
			}
			for p := int64(1); p <= 8; p++ {
				binding := testStatsBinding(100, false)
				stats := map[int64]storage.SegmentStats{}
				for id, v := range binding.stats.SegmentStats {
					stats[p*100+id] = v
					rows[p*100+id] = int64(v.NumRows)
					scope[0].Segments = append(scope[0].Segments, SegmentEntry{NodeID: 1, PartitionID: p, SegmentID: p*100 + id})
				}
				binding.stats.SegmentStats = stats
				binding.locations = clusteringLocations(binding.stats)
				binding.index = testClusterSearcher{labels: labels}
				bindings[p] = binding
			}
			values := make([][]byte, nq)
			for q := range values {
				values[q] = make([]byte, 8)
			}
			ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values}}})
			require.NoError(b, err)
			plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}})
			require.NoError(b, err)
			req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: int64(nq), PlaceholderGroup: ph, SerializedExprPlan: plan}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkClusterHintsSink = clusteringSearchHints(context.Background(), bindings, req, scope, rows, 0.5)
			}
		})
	}
}
