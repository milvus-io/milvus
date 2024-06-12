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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func getFieldData[T interface {
	GetFieldsData() []*schemapb.FieldData
}](rs T, fieldID int64) (*schemapb.FieldData, bool) {
	fd, has := lo.Find(rs.GetFieldsData(), func(fd *schemapb.FieldData) bool {
		return fd.GetFieldId() == fieldID
	})
	return fd, has
}

type ResultSuite struct {
	suite.Suite
}

func MergeSegcoreRetrieveResultsV1(ctx context.Context, retrieveResults []*segcorepb.RetrieveResults, param *mergeParam) (*segcorepb.RetrieveResults, error) {
	plan := &RetrievePlan{ignoreNonPk: false}
	return MergeSegcoreRetrieveResults(ctx, retrieveResults, param, nil, plan, nil)
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
	fieldDataArray1 = append(fieldDataArray1, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{1000, 2000}, 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{2000, 3000}, 1))
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

		result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{result1, result2},
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
		suite.NoError(err)
		suite.Equal(3, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 1}, result.GetIds().GetIntId().GetData())
		intFieldData, has := getFieldData(result, Int64FieldID)
		suite.Require().True(has)
		suite.Equal(Int64Array, intFieldData.GetScalars().GetLongData().Data)
		vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
		suite.Require().True(has)
		suite.InDeltaSlice(FloatVector, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
	})

	suite.Run("test nil results", func() {
		ret, err := MergeSegcoreRetrieveResultsV1(context.Background(), nil,
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
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

		ret, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{r},
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
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
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0,
		}

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
					result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{r1, r2},
						NewMergeParam(test.limit, make([]int64, 0), nil, false))
					suite.Equal(3, len(result.GetFieldsData()))
					suite.Equal(int(test.limit), len(result.GetIds().GetIntId().GetData()))
					suite.Equal(resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())
					intFieldData, has := getFieldData(result, Int64FieldID)
					suite.Require().True(has)
					suite.Equal(resultField0[0:test.limit], intFieldData.GetScalars().GetLongData().Data)
					vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
					suite.Require().True(has)
					suite.InDeltaSlice(resultFloat[0:test.limit*Dim], vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
					suite.NoError(err)
				})
			}
		})

		suite.Run("test unLimited and maxOutputSize", func() {
			reqLimit := typeutil.Unlimited
			paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1")

			ids := make([]int64, 100)
			offsets := make([]int64, 100)
			for i := range ids {
				ids[i] = int64(i)
				offsets[i] = int64(i)
			}
			fieldData := genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, ids, 1)

			result := &segcorepb.RetrieveResults{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: ids,
						},
					},
				},
				Offset:     offsets,
				FieldsData: []*schemapb.FieldData{fieldData},
			}

			_, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{result},
				NewMergeParam(reqLimit, make([]int64, 0), nil, false))
			suite.Error(err)
			paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1104857600")
		})

		suite.Run("test int ID", func() {
			result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{r1, r2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 11, 22, 22}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice(resultFloat, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
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

			result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{r1, r2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 11, 22, 22}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice(resultFloat, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
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
	fieldDataArray1 = append(fieldDataArray1, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{1000, 2000}, 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{2000, 3000}, 1))
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

		result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result1, result2},
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
		suite.NoError(err)
		suite.Equal(3, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 1}, result.GetIds().GetIntId().GetData())
		intFieldData, has := getFieldData(result, Int64FieldID)
		suite.Require().True(has)
		suite.Equal(Int64Array, intFieldData.GetScalars().GetLongData().GetData())
		vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
		suite.Require().True(has)
		suite.InDeltaSlice(FloatVector, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
	})

	suite.Run("test nil results", func() {
		ret, err := MergeInternalRetrieveResult(context.Background(), nil,
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
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
					},
				},
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
					},
				},
			},
			FieldsData: []*schemapb.FieldData{
				genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64,
					[]int64{5, 6}, 1),
				genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64,
					[]int64{7, 8}, 1),
			},
		}
		result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{ret1, ret2},
			NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
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
			11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0,
		}

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
					result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2},
						NewMergeParam(test.limit, make([]int64, 0), nil, false))
					suite.Equal(3, len(result.GetFieldsData()))
					suite.Equal(int(test.limit), len(result.GetIds().GetIntId().GetData()))
					suite.Equal(resultIDs[0:test.limit], result.GetIds().GetIntId().GetData())

					intFieldData, has := getFieldData(result, Int64FieldID)
					suite.Require().True(has)
					suite.Equal(resultField0[0:test.limit], intFieldData.GetScalars().GetLongData().Data)
					vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
					suite.Require().True(has)
					suite.InDeltaSlice(resultFloat[0:test.limit*Dim], vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
					suite.NoError(err)
				})
			}
		})

		suite.Run("test unLimited and maxOutputSize", func() {
			paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1")

			ids := make([]int64, 100)
			offsets := make([]int64, 100)
			for i := range ids {
				ids[i] = int64(i)
				offsets[i] = int64(i)
			}
			fieldData := genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, ids, 1)

			result := &internalpb.RetrieveResults{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: ids,
						},
					},
				},
				FieldsData: []*schemapb.FieldData{fieldData},
			}

			_, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result, result},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
			suite.Error(err)
			paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1104857600")
		})

		suite.Run("test int ID", func() {
			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())

			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 11, 22, 22}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice(resultFloat, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
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

			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{r1, r2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, false))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]string{"a", "b", "c", "d"}, result.GetIds().GetStrId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 11, 22, 22}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice(resultFloat, vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
			suite.NoError(err)
		})
	})
}

func (suite *ResultSuite) TestResult_MergeStopForBestResult() {
	const (
		Dim                  = 4
		Int64FieldName       = "Int64Field"
		FloatVectorFieldName = "FloatVectorField"
		Int64FieldID         = common.StartOfUserFieldID + 1
		FloatVectorFieldID   = common.StartOfUserFieldID + 2
	)
	Int64Array := []int64{11, 22, 33}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0}

	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{1000, 2000, 3000}, 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID,
		schemapb.DataType_Int64, Int64Array[0:3], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID,
		schemapb.DataType_FloatVector, FloatVector[0:12], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{2000, 3000, 4000}, 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID,
		schemapb.DataType_Int64, Int64Array[0:3], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID,
		schemapb.DataType_FloatVector, FloatVector[0:12], Dim))

	suite.Run("test stop seg core merge for best", func() {
		result1 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 1, 4},
					},
				},
			},
			Offset:     []int64{0, 1, 2},
			FieldsData: fieldDataArray1,
		}
		result2 := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{2, 3, 6},
					},
				},
			},
			Offset:     []int64{0, 1, 2},
			FieldsData: fieldDataArray2,
		}
		suite.Run("merge stop finite limited", func() {
			result1.HasMoreResult = true
			result2.HasMoreResult = true
			result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{result1, result2},
				NewMergeParam(3, make([]int64, 0), nil, true))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			// has more result both, stop reduce when draining one result
			// here, we can only get best result from 0 to 4 without 6, because result1 has more results
			suite.Equal([]int64{0, 1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 22, 11, 22, 33}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice([]float32{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 11, 22, 33, 44},
				vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
		})
		suite.Run("merge stop unlimited", func() {
			result1.HasMoreResult = false
			result2.HasMoreResult = false
			result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{result1, result2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, true))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			// as result1 and result2 don't have better results neither
			// we can reduce all available result into the reduced result
			suite.Equal([]int64{0, 1, 2, 3, 4, 6}, result.GetIds().GetIntId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 22, 11, 22, 33, 33}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice([]float32{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 11, 22, 33, 44, 11, 22, 33, 44},
				vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
		})
		suite.Run("merge stop one limited", func() {
			result1.HasMoreResult = true
			result2.HasMoreResult = false
			result, err := MergeSegcoreRetrieveResultsV1(context.Background(), []*segcorepb.RetrieveResults{result1, result2},
				NewMergeParam(typeutil.Unlimited, make([]int64, 0), nil, true))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			// as result1 may have better results, stop reducing when draining it
			suite.Equal([]int64{0, 1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
			intFieldData, has := getFieldData(result, Int64FieldID)
			suite.Require().True(has)
			suite.Equal([]int64{11, 22, 11, 22, 33}, intFieldData.GetScalars().GetLongData().Data)
			vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
			suite.Require().True(has)
			suite.InDeltaSlice([]float32{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 11, 22, 33, 44},
				vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
		})
	})

	suite.Run("test stop internal merge for best", func() {
		result1 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 4, 7},
					},
				},
			},
			FieldsData: fieldDataArray1,
		}
		result2 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{2, 6, 9},
					},
				},
			},
			FieldsData: fieldDataArray2,
		}
		result1.HasMoreResult = true
		result2.HasMoreResult = false
		result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result1, result2},
			NewMergeParam(3, make([]int64, 0), nil, true))
		suite.NoError(err)
		suite.Equal(3, len(result.GetFieldsData()))
		suite.Equal([]int64{0, 2, 4, 6, 7}, result.GetIds().GetIntId().GetData())
		intFieldData, has := getFieldData(result, Int64FieldID)
		suite.Require().True(has)
		suite.Equal([]int64{11, 11, 22, 22, 33}, intFieldData.GetScalars().GetLongData().Data)
		vectorFieldData, has := getFieldData(result, FloatVectorFieldID)
		suite.Require().True(has)
		suite.InDeltaSlice([]float32{1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 8, 5, 6, 7, 8, 11, 22, 33, 44},
			vectorFieldData.GetVectors().GetFloatVector().Data, 10e-10)
	})

	suite.Run("test stop internal merge for best with early termination", func() {
		result1 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{0, 4, 7},
					},
				},
			},
			FieldsData: fieldDataArray1,
		}
		var drainDataArray2 []*schemapb.FieldData
		drainDataArray2 = append(drainDataArray2, genFieldData(common.TimeStampFieldName, common.TimeStampField, schemapb.DataType_Int64, []int64{2000}, 1))
		drainDataArray2 = append(drainDataArray2, genFieldData(Int64FieldName, Int64FieldID,
			schemapb.DataType_Int64, Int64Array[0:1], 1))
		drainDataArray2 = append(drainDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID,
			schemapb.DataType_FloatVector, FloatVector[0:4], Dim))
		result2 := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{2},
					},
				},
			},
			FieldsData: drainDataArray2,
		}
		suite.Run("test drain one result without more results", func() {
			result1.HasMoreResult = false
			result2.HasMoreResult = false
			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result1, result2},
				NewMergeParam(3, make([]int64, 0), nil, true))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]int64{0, 2, 4, 7}, result.GetIds().GetIntId().GetData())
		})
		suite.Run("test drain one result with more results", func() {
			result1.HasMoreResult = false
			result2.HasMoreResult = true
			result, err := MergeInternalRetrieveResult(context.Background(), []*internalpb.RetrieveResults{result1, result2},
				NewMergeParam(3, make([]int64, 0), nil, true))
			suite.NoError(err)
			suite.Equal(3, len(result.GetFieldsData()))
			suite.Equal([]int64{0, 2}, result.GetIds().GetIntId().GetData())
		})
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
				},
			},
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
				[][]byte{
					[]byte("{\"5\": 5}"), []byte("{\"4\": 4}"), []byte("{\"3\": 3}"), []byte("{\"2\": 2}"),
					[]byte("{\"9\": 9}"), []byte("{\"8\": 8}"), []byte("{\"7\": 7}"), []byte("{\"6\": 6}"),
				}, 1),
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
