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

package grpcclient

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// makeSearchResults creates a SearchResults message with a sliced_blob of the given size.
func makeSearchResults(blobSize int) *internalpb.SearchResults {
	blob := make([]byte, blobSize)
	rand.Read(blob)
	return &internalpb.SearchResults{
		NumQueries:               10,
		TopK:                     100,
		MetricType:               "L2",
		SlicedBlob:               blob,
		SealedSegmentIDsSearched: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		ChannelIDsSearched:       []string{"ch-0", "ch-1", "ch-2", "ch-3"},
	}
}

// makePlanNodeWithTermExpr creates a PlanNode with a TermExpr containing numPKs int64 values.
func makePlanNodeWithTermExpr(numPKs int) *planpb.PlanNode {
	values := make([]*planpb.GenericValue, numPKs)
	for i := range values {
		values[i] = &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{Int64Val: int64(i)},
		}
	}
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId: 100,
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:  0,
								DataType: 5, // Int64
							},
							Values: values,
						},
					},
				},
				QueryInfo: &planpb.QueryInfo{
					Topk:         100,
					MetricType:   "L2",
					SearchParams: `{"nprobe": 16}`,
				},
			},
		},
		OutputFieldIds: []int64{0, 1, 2, 3},
	}
}

// makeSearchResultData creates a schemapb.SearchResultData (external API type) with scores.
func makeSearchResultData(numResults int) *schemapb.SearchResultData {
	scores := make([]float32, numResults)
	ids := make([]int64, numResults)
	for i := range scores {
		scores[i] = float32(i) * 0.1
		ids[i] = int64(i)
	}
	return &schemapb.SearchResultData{
		NumQueries: 10,
		TopK:       int64(numResults / 10),
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		Topks: []int64{int64(numResults / 10)},
	}
}

// --- SearchResults benchmarks (internal type) ---

func BenchmarkSearchResults_Marshal_Proto_1KB(b *testing.B) {
	msg := makeSearchResults(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkSearchResults_Marshal_VT_1KB(b *testing.B) {
	msg := makeSearchResults(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.MarshalVT()
	}
}

func BenchmarkSearchResults_Marshal_Proto_1MB(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkSearchResults_Marshal_VT_1MB(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.MarshalVT()
	}
}

func BenchmarkSearchResults_Unmarshal_Proto_1KB(b *testing.B) {
	msg := makeSearchResults(1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = proto.Unmarshal(data, out)
	}
}

func BenchmarkSearchResults_Unmarshal_VT_1KB(b *testing.B) {
	msg := makeSearchResults(1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = out.UnmarshalVT(data)
	}
}

func BenchmarkSearchResults_Unmarshal_Proto_1MB(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = proto.Unmarshal(data, out)
	}
}

func BenchmarkSearchResults_Unmarshal_VT_1MB(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = out.UnmarshalVT(data)
	}
}

// --- PlanNode benchmarks (2000 PK TermExpr) ---

func BenchmarkPlanNode_Marshal_Proto(b *testing.B) {
	msg := makePlanNodeWithTermExpr(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkPlanNode_Marshal_VT(b *testing.B) {
	msg := makePlanNodeWithTermExpr(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.MarshalVT()
	}
}

func BenchmarkPlanNode_Unmarshal_Proto(b *testing.B) {
	msg := makePlanNodeWithTermExpr(2000)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &planpb.PlanNode{}
		_ = proto.Unmarshal(data, out)
	}
}

func BenchmarkPlanNode_Unmarshal_VT(b *testing.B) {
	msg := makePlanNodeWithTermExpr(2000)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &planpb.PlanNode{}
		_ = out.UnmarshalVT(data)
	}
}

// --- SearchResultData benchmarks (external API type, proto only - no vtproto for go-api types yet) ---

func BenchmarkSearchResultData_Marshal_Proto(b *testing.B) {
	msg := makeSearchResultData(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkSearchResultData_Unmarshal_Proto(b *testing.B) {
	msg := makeSearchResultData(10000)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &schemapb.SearchResultData{}
		_ = proto.Unmarshal(data, out)
	}
}

// --- Codec layer benchmarks (simulating gRPC path) ---

func BenchmarkCodec_Marshal_SearchResults_1MB(b *testing.B) {
	codec := vtprotoCodec{}
	msg := makeSearchResults(1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Marshal(msg)
	}
}

func BenchmarkCodec_Unmarshal_SearchResults_1MB(b *testing.B) {
	codec := vtprotoCodec{}
	msg := makeSearchResults(1024 * 1024)
	data, _ := codec.Marshal(msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = codec.Unmarshal(data, out)
	}
}

// --- Correctness test ---

func TestVTProtoCodec_Roundtrip(t *testing.T) {
	codec := vtprotoCodec{}

	// Test with SearchResults (internal type)
	msg := makeSearchResults(1024)
	data, err := codec.Marshal(msg)
	assert.NoError(t, err)

	out := &internalpb.SearchResults{}
	err = codec.Unmarshal(data, out)
	assert.NoError(t, err)
	assert.Equal(t, msg.NumQueries, out.NumQueries)
	assert.Equal(t, msg.TopK, out.TopK)
	assert.Equal(t, msg.SlicedBlob, out.SlicedBlob)

	// Test with PlanNode (internal type)
	plan := makePlanNodeWithTermExpr(100)
	planData, err := codec.Marshal(plan)
	assert.NoError(t, err)

	planOut := &planpb.PlanNode{}
	err = codec.Unmarshal(planData, planOut)
	assert.NoError(t, err)
	assert.Equal(t, plan.OutputFieldIds, planOut.OutputFieldIds)

	// Test with SearchResultData (external API type, now has VT)
	srd := makeSearchResultData(100)
	srdData, err := codec.Marshal(srd)
	assert.NoError(t, err)

	srdOut := &schemapb.SearchResultData{}
	err = codec.Unmarshal(srdData, srdOut)
	assert.NoError(t, err)
	assert.Equal(t, srd.NumQueries, srdOut.NumQueries)
	assert.Equal(t, srd.Scores, srdOut.Scores)

	// Verify codec name
	assert.Equal(t, "proto", codec.Name())

	// Verify non-VT types return error
	_, err = codec.Marshal("not a proto")
	assert.Error(t, err)

	err = codec.Unmarshal([]byte{}, "not a proto")
	assert.Error(t, err)
}
