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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

func TestResult_mergeSegcoreRetrieveResults(t *testing.T) {
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

	t.Run("test skip dupPK 2", func(t *testing.T) {
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

		result, err := mergeSegcoreRetrieveResultsV2(context.Background(), []*segcorepb.RetrieveResults{result1, result2}, unlimited)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))
		assert.Equal(t, []int64{0, 1}, result.GetIds().GetIntId().GetData())
		assert.Equal(t, Int64Array, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
		assert.InDeltaSlice(t, FloatVector, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
	})

	t.Run("test nil results", func(t *testing.T) {
		ret, err := mergeSegcoreRetrieveResultsV2(context.Background(), nil, unlimited)
		assert.NoError(t, err)
		assert.Empty(t, ret.GetIds())
		assert.Empty(t, ret.GetFieldsData())
	})

	t.Run("test no offset", func(t *testing.T) {
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

		ret, err := mergeSegcoreRetrieveResultsV2(context.Background(), []*segcorepb.RetrieveResults{r}, unlimited)
		assert.NoError(t, err)
		assert.Empty(t, ret.GetIds())
		assert.Empty(t, ret.GetFieldsData())
	})

	t.Run("test merge", func(t *testing.T) {
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

		t.Run("test limited", func(t *testing.T) {
			tests := []struct {
				description string
				limit       int
			}{
				{"limit 1", 1},
				{"limit 2", 2},
				{"limit 3", 3},
				{"limit 4", 4},
			}
			resultIDs := []int64{1, 2, 3, 4}
			resultField0 := []int64{11, 11, 22, 22}
			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					result, err := mergeSegcoreRetrieveResultsV2(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, test.limit)
					assert.Equal(t, 2, len(result.GetFieldsData()))
					assert.Equal(t, test.limit, len(result.GetIds().GetIntId().GetData()))
					assert.Equal(t, resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())
					assert.Equal(t, resultField0[0:test.limit], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
					assert.InDeltaSlice(t, resultFloat[0:test.limit*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
					assert.NoError(t, err)
				})
			}
		})

		t.Run("test int ID", func(t *testing.T) {
			result, err := mergeSegcoreRetrieveResultsV2(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, unlimited)
			assert.Equal(t, 2, len(result.GetFieldsData()))
			assert.Equal(t, []int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			assert.Equal(t, []int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			assert.InDeltaSlice(t, resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			assert.NoError(t, err)
		})

		t.Run("test string ID", func(t *testing.T) {
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

			result, err := mergeSegcoreRetrieveResultsV2(context.Background(), []*segcorepb.RetrieveResults{r1, r2}, unlimited)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(result.GetFieldsData()))
			assert.Equal(t, []string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			assert.Equal(t, []int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			assert.InDeltaSlice(t, resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			assert.NoError(t, err)
		})

	})
}

func TestResult_mergeInternalRetrieveResults(t *testing.T) {
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

	t.Run("test skip dupPK 2", func(t *testing.T) {
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

		result, err := mergeInternalRetrieveResultsV2(context.Background(), []*internalpb.RetrieveResults{result1, result2}, unlimited)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))
		assert.Equal(t, []int64{0, 1}, result.GetIds().GetIntId().GetData())
		assert.Equal(t, Int64Array, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
		assert.InDeltaSlice(t, FloatVector, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
	})

	t.Run("test nil results", func(t *testing.T) {
		ret, err := mergeInternalRetrieveResultsV2(context.Background(), nil, unlimited)
		assert.NoError(t, err)
		assert.Empty(t, ret.GetIds())
		assert.Empty(t, ret.GetFieldsData())
	})

	t.Run("test merge", func(t *testing.T) {
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

		t.Run("test limited", func(t *testing.T) {
			tests := []struct {
				description string
				limit       int
			}{
				{"limit 1", 1},
				{"limit 2", 2},
				{"limit 3", 3},
				{"limit 4", 4},
			}
			resultIDs := []int64{1, 2, 3, 4}
			resultField0 := []int64{11, 11, 22, 22}
			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					result, err := mergeInternalRetrieveResultsV2(context.Background(), []*internalpb.RetrieveResults{r1, r2}, test.limit)
					assert.Equal(t, 2, len(result.GetFieldsData()))
					assert.Equal(t, test.limit, len(result.GetIds().GetIntId().GetData()))
					assert.Equal(t, resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())
					assert.Equal(t, resultField0[0:test.limit], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
					assert.InDeltaSlice(t, resultFloat[0:test.limit*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
					assert.NoError(t, err)
				})
			}
		})

		t.Run("test int ID", func(t *testing.T) {
			result, err := mergeInternalRetrieveResultsV2(context.Background(), []*internalpb.RetrieveResults{r1, r2}, unlimited)
			assert.Equal(t, 2, len(result.GetFieldsData()))
			assert.Equal(t, []int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			assert.Equal(t, []int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			assert.InDeltaSlice(t, resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			assert.NoError(t, err)
		})

		t.Run("test string ID", func(t *testing.T) {
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

			result, err := mergeInternalRetrieveResultsV2(context.Background(), []*internalpb.RetrieveResults{r1, r2}, unlimited)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(result.GetFieldsData()))
			assert.Equal(t, []string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			assert.Equal(t, []int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			assert.InDeltaSlice(t, resultFloat, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			assert.NoError(t, err)
		})

	})
}

func TestResult_reduceSearchResultData(t *testing.T) {
	const (
		nq         = 1
		topk       = 4
		metricType = "L2"
	)
	t.Run("case1", func(t *testing.T) {
		ids := []int64{1, 2, 3, 4}
		scores := []float32{-1.0, -2.0, -3.0, -4.0}
		topks := []int64{int64(len(ids))}
		data1 := genSearchResultData(nq, topk, ids, scores, topks)
		data2 := genSearchResultData(nq, topk, ids, scores, topks)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := reduceSearchResultData(context.TODO(), dataArray, nq, topk)
		assert.Nil(t, err)
		assert.Equal(t, ids, res.Ids.GetIntId().Data)
		assert.Equal(t, scores, res.Scores)
	})
	t.Run("case2", func(t *testing.T) {
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
		res, err := reduceSearchResultData(context.TODO(), dataArray, nq, topk)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int64{1, 5, 2, 3}, res.Ids.GetIntId().Data)
	})
}

func TestResult_selectSearchResultData_int(t *testing.T) {
	type args struct {
		dataArray     []*schemapb.SearchResultData
		resultOffsets [][]int64
		offsets       []int64
		topk          int64
		nq            int64
		qi            int64
	}
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
		t.Run(tt.name, func(t *testing.T) {
			if got := selectSearchResultData(tt.args.dataArray, tt.args.resultOffsets, tt.args.offsets, tt.args.qi); got != tt.want {
				t.Errorf("selectSearchResultData() = %v, want %v", got, tt.want)
			}
		})
	}
}
