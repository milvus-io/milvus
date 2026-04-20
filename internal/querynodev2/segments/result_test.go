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

package segments

import (
	"context"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ResultSuite struct {
	suite.Suite
}

func (suite *ResultSuite) TestResult_SelectSearchResultData_int() {
	type args struct {
		dataArray     []*schemapb.SearchResultData
		resultOffsets [][]int64
		offsets       []int64
		topk          int64
		nq            int64
		qi            int64
	}
	suite.Run("Integer ID", func() {
		tests := []struct {
			name string
			args args
			want int
		}{
			{
				args: args{
					dataArray: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{11, 9, 7, 5, 3, 1},
									},
								},
							},
							Scores: []float32{1.1, 0.9, 0.7, 0.5, 0.3, 0.1},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{12, 10, 8, 6, 4, 2},
									},
								},
							},
							Scores: []float32{1.2, 1.0, 0.8, 0.6, 0.4, 0.2},
							Topks:  []int64{2, 2, 2},
						},
					},
					resultOffsets: [][]int64{{0, 2, 4}, {0, 2, 4}},
					offsets:       []int64{0, 1},
					topk:          2,
					nq:            3,
					qi:            0,
				},
				want: 0,
			},
		}
		for _, tt := range tests {
			suite.Run(tt.name, func() {
				if got := SelectSearchResultData(tt.args.dataArray, tt.args.resultOffsets, tt.args.offsets, tt.args.qi); got != tt.want {
					suite.T().Errorf("SelectSearchResultData() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	suite.Run("Integer ID with bad score", func() {
		tests := []struct {
			name string
			args args
			want int
		}{
			{
				args: args{
					dataArray: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{11, 9, 7, 5, 3, 1},
									},
								},
							},
							Scores: []float32{-math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{12, 10, 8, 6, 4, 2},
									},
								},
							},
							Scores: []float32{-math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32},
							Topks:  []int64{2, 2, 2},
						},
					},
					resultOffsets: [][]int64{{0, 2, 4}, {0, 2, 4}},
					offsets:       []int64{0, 1},
					topk:          2,
					nq:            3,
					qi:            0,
				},
				want: -1,
			},
		}
		for _, tt := range tests {
			suite.Run(tt.name, func() {
				if got := SelectSearchResultData(tt.args.dataArray, tt.args.resultOffsets, tt.args.offsets, tt.args.qi); got != tt.want {
					suite.T().Errorf("SelectSearchResultData() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}

func (suite *ResultSuite) TestSort() {
	result := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 4, 3, 2, 9, 8, 7, 6},
				},
			},
		},
		Offset: []int64{5, 4, 3, 2, 9, 8, 7, 6},
		FieldsData: []*schemapb.FieldData{
			mock_segcore.GenFieldData("int64 field", 100, schemapb.DataType_Int64,
				[]int64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			mock_segcore.GenFieldData("double field", 101, schemapb.DataType_Double,
				[]float64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			mock_segcore.GenFieldData("string field", 102, schemapb.DataType_VarChar,
				[]string{"5", "4", "3", "2", "9", "8", "7", "6"}, 1),
			mock_segcore.GenFieldData("bool field", 103, schemapb.DataType_Bool,
				[]bool{false, true, false, true, false, true, false, true}, 1),
			mock_segcore.GenFieldData("float field", 104, schemapb.DataType_Float,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			mock_segcore.GenFieldData("int field", 105, schemapb.DataType_Int32,
				[]int32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			mock_segcore.GenFieldData("float vector field", 106, schemapb.DataType_FloatVector,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			mock_segcore.GenFieldData("binary vector field", 107, schemapb.DataType_BinaryVector,
				[]byte{5, 4, 3, 2, 9, 8, 7, 6}, 8),
			mock_segcore.GenFieldData("json field", 108, schemapb.DataType_JSON,
				[][]byte{
					[]byte("{\"5\": 5}"), []byte("{\"4\": 4}"), []byte("{\"3\": 3}"), []byte("{\"2\": 2}"),
					[]byte("{\"9\": 9}"), []byte("{\"8\": 8}"), []byte("{\"7\": 7}"), []byte("{\"6\": 6}"),
				}, 1),
			mock_segcore.GenFieldData("json field", 108, schemapb.DataType_Array,
				[]*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6, 7}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{4, 5, 6}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4, 5}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2, 3, 4}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{9, 10, 11}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{8, 9, 10}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 8, 9}}}},
					{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{6, 7, 8}}}},
				}, 1),
		},
	}

	sort.Sort(&byPK{result})

	suite.Equal([]int64{2, 3, 4, 5, 6, 7, 8, 9}, result.GetIds().GetIntId().GetData())
	suite.Equal([]int64{2, 3, 4, 5, 6, 7, 8, 9}, result.GetOffset())
	suite.Equal([]int64{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[0].GetScalars().GetLongData().Data)
	suite.InDeltaSlice([]float64{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[1].GetScalars().GetDoubleData().Data, 10e-10)
	suite.Equal([]string{"2", "3", "4", "5", "6", "7", "8", "9"}, result.FieldsData[2].GetScalars().GetStringData().Data)
	suite.Equal([]bool{true, false, true, false, true, false, true, false}, result.FieldsData[3].GetScalars().GetBoolData().Data)
	suite.InDeltaSlice([]float32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[4].GetScalars().GetFloatData().Data, 10e-10)
	suite.Equal([]int32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[5].GetScalars().GetIntData().Data)
	suite.InDeltaSlice([]float32{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[6].GetVectors().GetFloatVector().GetData(), 10e-10)
	suite.Equal([]byte{2, 3, 4, 5, 6, 7, 8, 9}, result.FieldsData[7].GetVectors().GetBinaryVector())
	suite.Equal([][]byte{
		[]byte("{\"2\": 2}"), []byte("{\"3\": 3}"), []byte("{\"4\": 4}"), []byte("{\"5\": 5}"),
		[]byte("{\"6\": 6}"), []byte("{\"7\": 7}"), []byte("{\"8\": 8}"), []byte("{\"9\": 9}"),
	}, result.FieldsData[8].GetScalars().GetJsonData().GetData())
	suite.Equal([]*schemapb.ScalarField{
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2, 3, 4}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4, 5}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{4, 5, 6}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6, 7}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{6, 7, 8}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 8, 9}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{8, 9, 10}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{9, 10, 11}}}},
	}, result.FieldsData[9].GetScalars().GetArrayData().GetData())
}

func (suite *ResultSuite) TestReduceSearchOnQueryNode() {
	results := make([]*internalpb.SearchResults, 0)
	metricType := metric.IP
	nq := int64(1)
	topK := int64(1)
	mockBlob := []byte{65, 66, 67, 65, 66, 67}
	{
		subRes1 := &internalpb.SearchResults{
			MetricType:         metricType,
			NumQueries:         nq,
			TopK:               topK,
			SlicedBlob:         mockBlob,
			ScannedRemoteBytes: 100,
			ScannedTotalBytes:  200,
		}
		results = append(results, subRes1)
	}
	{
		subRes2 := &internalpb.SearchResults{
			MetricType:         metricType,
			NumQueries:         nq,
			TopK:               topK,
			SlicedBlob:         mockBlob,
			ScannedRemoteBytes: 100,
			ScannedTotalBytes:  200,
		}
		results = append(results, subRes2)
	}
	reducedRes, err := ReduceSearchOnQueryNode(context.Background(), results, reduce.NewReduceSearchResultInfo(nq, topK).
		WithMetricType(metricType).WithPkType(schemapb.DataType_Int8).WithAdvance(true))
	suite.NoError(err)
	suite.Equal(2, len(reducedRes.GetSubResults()))

	subRes1 := reducedRes.GetSubResults()[0]
	suite.Equal(metricType, subRes1.GetMetricType())
	suite.Equal(nq, subRes1.GetNumQueries())
	suite.Equal(topK, subRes1.GetTopK())
	suite.Equal(mockBlob, subRes1.GetSlicedBlob())
	suite.Equal(int64(200), reducedRes.GetScannedRemoteBytes())
	suite.Equal(int64(400), reducedRes.GetScannedTotalBytes())
}

func (suite *ResultSuite) TestReduceSearchOnQueryNode_NonAdvanced() {
	ctx := context.Background()
	metricType := metric.IP
	nq := int64(1)
	topK := int64(1)
	// build minimal valid blobs via encoder
	srd1 := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}
	srd2 := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
		Scores:     []float32{0.8},
		Topks:      []int64{1},
	}
	rEnc1, err := EncodeSearchResultData(ctx, srd1, nq, topK, metricType)
	suite.NoError(err)
	rEnc1.ScannedRemoteBytes = 111
	rEnc1.ScannedTotalBytes = 222
	rEnc2, err := EncodeSearchResultData(ctx, srd2, nq, topK, metricType)
	suite.NoError(err)
	rEnc2.ScannedRemoteBytes = 333
	rEnc2.ScannedTotalBytes = 444

	out, err := ReduceSearchOnQueryNode(ctx, []*internalpb.SearchResults{rEnc1, rEnc2}, reduce.NewReduceSearchResultInfo(nq, topK).WithMetricType(metricType).WithPkType(schemapb.DataType_Int64))
	suite.NoError(err)
	// costs should aggregate across both included results
	suite.Equal(int64(111+333), out.GetScannedRemoteBytes())
	suite.Equal(int64(222+444), out.GetScannedTotalBytes())
}

func (suite *ResultSuite) TestEncodeSearchResultData_ZeroCopySwitch() {
	ctx := context.Background()
	key := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.Key
	original := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.GetValue()
	defer paramtable.Get().Save(key, original)

	srd := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}

	paramtable.Get().Save(key, "false")
	encodedBlob, err := EncodeSearchResultData(ctx, srd, 1, 1, metric.IP)
	suite.NoError(err)
	suite.Nil(encodedBlob.GetResultData())
	suite.NotNil(encodedBlob.GetSlicedBlob())

	paramtable.Get().Save(key, "true")
	encodedZeroCopy, err := EncodeSearchResultData(ctx, srd, 1, 1, metric.IP)
	suite.NoError(err)
	suite.Nil(encodedZeroCopy.GetSlicedBlob())
	suite.Same(srd, encodedZeroCopy.GetResultData())
}

func (suite *ResultSuite) TestReduceSearchOnQueryNode_AdvancedPreservesResultData() {
	nq := int64(1)
	topK := int64(1)
	resultData := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}

	reducedRes, err := ReduceSearchOnQueryNode(context.Background(), []*internalpb.SearchResults{
		{
			MetricType:         metric.IP,
			NumQueries:         nq,
			TopK:               topK,
			ResultData:         resultData,
			ScannedRemoteBytes: 10,
			ScannedTotalBytes:  20,
		},
	}, reduce.NewReduceSearchResultInfo(nq, topK).
		WithMetricType(metric.IP).WithPkType(schemapb.DataType_Int64).WithAdvance(true))
	suite.NoError(err)
	suite.Len(reducedRes.GetSubResults(), 1)
	suite.Same(resultData, reducedRes.GetSubResults()[0].GetResultData())
	suite.Nil(reducedRes.GetSubResults()[0].GetSlicedBlob())
}

func (suite *ResultSuite) TestDecodeSearchResults_ResultDataAndBlob() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(1)

	// ResultData path (pre-decoded, zero-copy)
	resultData := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}

	// SlicedBlob path (legacy marshaled)
	blobData := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{20}}}},
		Scores:     []float32{0.8},
		Topks:      []int64{1},
	}
	blob, err := proto.Marshal(blobData)
	suite.NoError(err)

	decoded, err := DecodeSearchResults(ctx, []*internalpb.SearchResults{
		{ResultData: resultData}, // zero-copy path
		{SlicedBlob: blob},       // legacy path
		{},                       // empty — should be skipped
		{SlicedBlob: nil},        // nil blob — should be skipped
	})
	suite.NoError(err)
	suite.Len(decoded, 2)
	suite.Same(resultData, decoded[0])            // zero-copy: same pointer
	suite.True(proto.Equal(blobData, decoded[1])) // blob: equal content
}

func (suite *ResultSuite) TestReduceSearchResults_FilterIncludesResultData() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(1)

	resultData := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}

	// Single result with ResultData only (no SlicedBlob) — should pass filter and use shortcut return
	out, err := ReduceSearchResults(ctx, []*internalpb.SearchResults{
		{
			MetricType: metric.IP,
			NumQueries: nq,
			TopK:       topK,
			ResultData: resultData,
		},
		nil, // should be filtered out
		{},  // no data — should be filtered out
	}, reduce.NewReduceSearchResultInfo(nq, topK).WithMetricType(metric.IP).WithPkType(schemapb.DataType_Int64))
	suite.NoError(err)
	suite.Same(resultData, out.GetResultData())
}

func (suite *ResultSuite) TestEncodeSearchResultData_EmptyResult() {
	ctx := context.Background()

	// nil searchResultData — should produce neither SlicedBlob nor ResultData
	key := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.Key
	original := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.GetValue()
	defer paramtable.Get().Save(key, original)

	paramtable.Get().Save(key, "true")
	encoded, err := EncodeSearchResultData(ctx, nil, 1, 1, metric.IP)
	suite.NoError(err)
	suite.Nil(encoded.GetResultData())
	suite.Nil(encoded.GetSlicedBlob())

	// empty IDs — should produce neither
	srd := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Ids:        &schemapb.IDs{},
		Scores:     []float32{},
		Topks:      []int64{0},
	}
	encoded, err = EncodeSearchResultData(ctx, srd, 1, 1, metric.IP)
	suite.NoError(err)
	suite.Nil(encoded.GetResultData())
	suite.Nil(encoded.GetSlicedBlob())
}

func TestResult_MergeRequestCost(t *testing.T) {
	costs := []*internalpb.CostAggregation{
		{
			ResponseTime: 11,
			ServiceTime:  12,
			TotalNQ:      13,
		},

		{
			ResponseTime: 21,
			ServiceTime:  22,
			TotalNQ:      23,
		},

		{
			ResponseTime: 31,
			ServiceTime:  32,
			TotalNQ:      33,
		},

		{
			ResponseTime: 41,
			ServiceTime:  42,
			TotalNQ:      43,
		},
	}

	channelCost := mergeRequestCost(costs)
	assert.Equal(t, int64(41), channelCost.ResponseTime)
	assert.Equal(t, int64(42), channelCost.ServiceTime)
	assert.Equal(t, int64(43), channelCost.TotalNQ)
}

func TestResult(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(ResultSuite))
}
