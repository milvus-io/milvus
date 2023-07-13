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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ResultSuite struct {
	suite.Suite
}

func (suite *ResultSuite) TestResult_MergeSegcoreRetrieveResults() {
	const (
		Dim                  = 8
		Int64FieldName       = "Int64Field"
		FloatVectorFieldName = "FloatVectorField"
		Int64FieldID         = common.StartOfUserFieldID + 1
		FloatVectorFieldID   = common.StartOfUserFieldID + 2
	)
	Int64Array := []int64{11, 22}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	suite.Run("test skip dupPK 2", func() {
		result1 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
			Offset:     []int64{0, 1},
			FieldsData: fieldDataArray1,
		}
		result2 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
			Offset:     []int64{0, 1},
			FieldsData: fieldDataArray2,
		}

		result, err := MergeSegcoreRetrieveResults(context.Background(), []*segcorepb.RetrieveResults{result1, result2}, typeutil.Unlimited)
		suite.NoError(err)
		suite.Equal(2, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 1}, result.GetIds().GetIntId().GetData())
		suite.Equal(Int64Array, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
		suite.InDeltaSlice(FloatVector, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
	})

	suite.Run("test nil results", func() {
		ret, err := MergeSegcoreRetrieveResults(context.Background(), nil, typeutil.Unlimited)
		suite.NoError(err)
		suite.Empty(ret.GetIds())
		suite.Empty(ret.GetFieldsData())
	})

	suite.Run("test no offset", func() {
		r := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
			FieldsData: fieldDataArray1,
		}

		ret, err := MergeSegcoreRetrieveResults(context.Background(), []*segcorepb.RetrieveResults{r}, typeutil.Unlimited)
		suite.NoError(err)
		suite.Empty(ret.GetIds())
		suite.Empty(ret.GetFieldsData())
	})

	suite.Run("test merge", func() {
		r1 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{1, 3},
					},
				},
			},
			Offset:     []int64{0, 1},
			FieldsData: fieldDataArray1,
		}
		r2 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{2, 4},
					},
				},
			},
			Offset:     []int64{0, 1},
			FieldsData: fieldDataArray2,
		}

		resultFloat := []float32{
			1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
			1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0,
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

		suite.Run("test limited", func() {
			tests := []struct {
				description string
				limit       int64
			}{
				{"limit 1", 1},
				{"limit 2", 2},
				{"limit 3", 3},
				{"limit 4", 4},
			}
			resultIDs := []int64{1, 2, 3, 4}
			resultField0 := []int64{11, 11, 22, 22}
			for _, test := range tests {
				suite.Run(test.description, func() {
					result, err := MergeSegcoreRetrieveResults(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, test.limit)
					suite.Equal(2, len(result.GetFieldsData()))
					suite.Equal(int(test.limit), len(result.GetIds().GetIntId().GetData()))
					suite.Equal(resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())
					suite.Equal(resultField0[0:test.limit], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
					suite.InDeltaSlice(resultFloat[0:test.limit*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
					suite.NoError(err)
				})
			}
		})

		suite.Run("test int ID", func() {
			result, err := MergeSegcoreRetrieveResults(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, typeutil.Unlimited)
			suite.Equal(2, len(result.GetFieldsData()))
			suite.Equal([]int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			suite.Equal([]int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			suite.InDeltaSlice(resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			suite.NoError(err)
		})

		suite.Run("test string ID", func() {
			r1.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"a", "c"},
					}}}

			r2.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"b", "d"},
					}}}

			result, err := MergeSegcoreRetrieveResults(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, typeutil.Unlimited)
			suite.NoError(err)
			suite.Equal(2, len(result.GetFieldsData()))
			suite.Equal([]string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			suite.Equal([]int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			suite.InDeltaSlice(resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			suite.NoError(err)
		})

	})
}

func (suite *ResultSuite) TestResult_MergeInternalRetrieveResults() {
	const (
		Dim                  = 8
		Int64FieldName       = "Int64Field"
		FloatVectorFieldName = "FloatVectorField"
		Int64FieldID         = common.StartOfUserFieldID + 1
		FloatVectorFieldID   = common.StartOfUserFieldID + 2
	)
	Int64Array := []int64{11, 22}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	suite.Run("test skip dupPK 2", func() {
		result1 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
			FieldsData: fieldDataArray1,
		}
		result2 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
			FieldsData: fieldDataArray2,
		}

		result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result1, result2}, typeutil.Unlimited)
		suite.NoError(err)
		suite.Equal(2, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 1}, result.GetIds().GetIntId().GetData())
		suite.Equal(Int64Array, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
		suite.InDeltaSlice(FloatVector, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
	})

	suite.Run("test nil results", func() {
		ret, err := MergeInternalRetrieveResult(context.Background(), nil, typeutil.Unlimited)
		suite.NoError(err)
		suite.Empty(ret.GetIds())
		suite.Empty(ret.GetFieldsData())
	})

	suite.Run("test timestamp decided", func() {
		ret1 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					}},
			},
			FieldsData: []*schemapb.FieldData{
				genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64,
					[]int64{1, 2}, 1),
				genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64,
					[]int64{3, 4}, 1),
			},
		}
		ret2 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1},
					}},
			},
			FieldsData: []*schemapb.FieldData{
				genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64,
					[]int64{5, 6}, 1),
				genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64,
					[]int64{7, 8}, 1),
			},
		}
		result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{ret1, ret2}, typeutil.Unlimited)
		suite.NoError(err)
		suite.Equal(2, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 1}, result.GetIds().GetIntId().GetData())
		suite.Equal([]int64{7, 8}, result.GetFieldsData()[1].GetScalars().GetLongData().Data)
	})

	suite.Run("test merge", func() {
		r1 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{1, 3},
					},
				},
			},
			FieldsData: fieldDataArray1,
		}
		r2 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{2, 4},
					},
				},
			},
			FieldsData: fieldDataArray2,
		}

		resultFloat := []float32{
			1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
			1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0,
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

		suite.Run("test limited", func() {
			tests := []struct {
				description string
				limit       int64
			}{
				{"limit 1", 1},
				{"limit 2", 2},
				{"limit 3", 3},
				{"limit 4", 4},
			}
			resultIDs := []int64{1, 2, 3, 4}
			resultField0 := []int64{11, 11, 22, 22}
			for _, test := range tests {
				suite.Run(test.description, func() {
					result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2}, test.limit)
					suite.Equal(2, len(result.GetFieldsData()))
					suite.Equal(int(test.limit), len(result.GetIds().GetIntId().GetData()))
					suite.Equal(resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())
					suite.Equal(resultField0[0:test.limit], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
					suite.InDeltaSlice(resultFloat[0:test.limit*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
					suite.NoError(err)
				})
			}
		})

		suite.Run("test int ID", func() {
			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2}, typeutil.Unlimited)
			suite.Equal(2, len(result.GetFieldsData()))
			suite.Equal([]int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			suite.Equal([]int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			suite.InDeltaSlice(resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			suite.NoError(err)
		})

		suite.Run("test string ID", func() {
			r1.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"a", "c"},
					},
				},
			}

			r2.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"b", "d"},
					},
				},
			}

			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2}, typeutil.Unlimited)
			suite.NoError(err)
			suite.Equal(2, len(result.GetFieldsData()))
			suite.Equal([]string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			suite.Equal([]int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			suite.InDeltaSlice(resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			suite.NoError(err)
		})

	})
}

func (suite *ResultSuite) TestResult_ReduceSearchResultData() {
	const (
		nq         = 1
		topk       = 4
		metricType = "L2"
	)
	suite.Run("case1", func() {
		ids := []int64{1, 2, 3, 4}
		scores := []float32{-1.0, -2.0, -3.0, -4.0}
		topks := []int64{int64(len(ids))}
		data1 := genSearchResultData(nq, topk, ids, scores, topks)
		data2 := genSearchResultData(nq, topk, ids, scores, topks)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := ReduceSearchResultData(context.TODO(), dataArray, nq, topk)
		suite.Nil(err)
		suite.Equal(ids, res.Ids.GetIntId().Data)
		suite.Equal(scores, res.Scores)
	})
	suite.Run("case2", func() {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := genSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := genSearchResultData(nq, topk, ids2, scores2, topks2)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := ReduceSearchResultData(context.TODO(), dataArray, nq, topk)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 5, 2, 3}, res.Ids.GetIntId().Data)
	})
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
				}},
		},
		Offset: []int64{5, 4, 3, 2, 9, 8, 7, 6},
		FieldsData: []*schemapb.FieldData{
			genFieldData("int64 field", 100, schemapb.DataType_Int64,
				[]int64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("double field", 101, schemapb.DataType_Double,
				[]float64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("string field", 102, schemapb.DataType_VarChar,
				[]string{"5", "4", "3", "2", "9", "8", "7", "6"}, 1),
			genFieldData("bool field", 103, schemapb.DataType_Bool,
				[]bool{false, true, false, true, false, true, false, true}, 1),
			genFieldData("float field", 104, schemapb.DataType_Float,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("int field", 105, schemapb.DataType_Int32,
				[]int32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("float vector field", 106, schemapb.DataType_FloatVector,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("binary vector field", 107, schemapb.DataType_BinaryVector,
				[]byte{5, 4, 3, 2, 9, 8, 7, 6}, 8),
			genFieldData("json field", 108, schemapb.DataType_JSON,
				[][]byte{[]byte("{\"5\": 5}"), []byte("{\"4\": 4}"), []byte("{\"3\": 3}"), []byte("{\"2\": 2}"),
					[]byte("{\"9\": 9}"), []byte("{\"8\": 8}"), []byte("{\"7\": 7}"), []byte("{\"6\": 6}")}, 1),
			genFieldData("json field", 108, schemapb.DataType_Array,
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
	suite.Equal([][]byte{[]byte("{\"2\": 2}"), []byte("{\"3\": 3}"), []byte("{\"4\": 4}"), []byte("{\"5\": 5}"),
		[]byte("{\"6\": 6}"), []byte("{\"7\": 7}"), []byte("{\"8\": 8}"), []byte("{\"9\": 9}")}, result.FieldsData[8].GetScalars().GetJsonData().GetData())
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
	suite.Run(t, new(ResultSuite))
}
