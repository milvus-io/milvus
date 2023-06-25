// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestQueryTask_all(t *testing.T) {
	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = types.NewMockQueryCoord(t)
		qn = getQueryNode()

		shardsNum      = common.DefaultShardsNum
		collectionName = t.Name() + funcutil.GenRandomStr()

		expr   = fmt.Sprintf("%s > 0", testInt64Field)
		hitNum = 10
	)

	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()

	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc.EXPECT().Start().Return(nil)
	qc.EXPECT().Stop().Return(nil)
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&successStatus, nil)
	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil).Maybe()

	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	lb := NewLBPolicyImpl(mgr)

	rc.Start()
	defer rc.Stop()
	qc.Start()
	defer qc.Stop()

	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
	}

	require.NoError(t, createColT.OnEnqueue())
	require.NoError(t, createColT.PreExecute(ctx))
	require.NoError(t, createColT.Execute(ctx))
	require.NoError(t, createColT.PostExecute(ctx))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: paramtable.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	// test begins
	task := &queryTask{
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionID:   collectionID,
			OutputFieldsId: make([]int64, len(fieldName2Types)),
		},
		ctx: ctx,
		result: &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			FieldsData: []*schemapb.FieldData{},
		},
		request: &milvuspb.QueryRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionName: collectionName,
			Expr:           expr,
			QueryParams: []*commonpb.KeyValuePair{
				{
					Key:   IgnoreGrowingKey,
					Value: "false",
				},
				{
					Key:   IterationExtensionReduceRateKey,
					Value: "10",
				},
			},
		},
		qc: qc,
		lb: lb,
	}

	assert.NoError(t, task.OnEnqueue())

	// test query task with timeout
	ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	// before preExecute
	assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	assert.NoError(t, task.PreExecute(ctx))
	// after preExecute
	assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

	// check iteration extension reduce
	assert.Equal(t, int64(10), task.RetrieveRequest.IterationExtensionReduceRate)
	task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{
		Key:   IterationExtensionReduceRateKey,
		Value: "10XXX",
	})
	assert.Error(t, task.PreExecute(ctx))

	result1 := &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_RetrieveResult},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: generateInt64Array(hitNum)},
			},
		},
	}

	outputFieldIDs := make([]UniqueID, 0, len(fieldName2Types))
	for i := 0; i < len(fieldName2Types); i++ {
		outputFieldIDs = append(outputFieldIDs, int64(common.StartOfUserFieldID+i))
	}
	task.RetrieveRequest.OutputFieldsId = outputFieldIDs
	for fieldName, dataType := range fieldName2Types {
		result1.FieldsData = append(result1.FieldsData, generateFieldData(dataType, fieldName, hitNum))
	}
	result1.FieldsData = append(result1.FieldsData, generateFieldData(schemapb.DataType_Int64, common.TimeStampFieldName, hitNum))
	task.RetrieveRequest.OutputFieldsId = append(task.RetrieveRequest.OutputFieldsId, common.TimeStampField)
	task.ctx = ctx
	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
	assert.Error(t, task.Execute(ctx))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
		},
	}, nil)
	err = task.Execute(ctx)
	assert.True(t, strings.Contains(err.Error(), errInvalidShardLeaders.Error()))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}, nil)
	assert.Error(t, task.Execute(ctx))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Query(mock.Anything, mock.Anything).Return(result1, nil)
	assert.NoError(t, task.Execute(ctx))

	task.queryParams = &queryParams{
		limit:  100,
		offset: 100,
	}
	assert.NoError(t, task.PostExecute(ctx))

	for i := 0; i < len(task.result.FieldsData); i++ {
		assert.NotEqual(t, task.result.FieldsData[i].FieldId, common.TimeStampField)
	}
}

func Test_translateToOutputFieldIDs(t *testing.T) {
	type testCases struct {
		name          string
		outputFields  []string
		schema        *schemapb.CollectionSchema
		expectedError bool
		expectedIDs   []int64
	}

	cases := []testCases{
		{
			name:         "empty output fields",
			outputFields: []string{},
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField,
						Name:    common.RowIDFieldName,
					},
					{
						FieldID:      100,
						Name:         "ID",
						IsPrimaryKey: true,
					},
					{
						FieldID: 101,
						Name:    "Vector",
					},
				},
			},
			expectedError: false,
			expectedIDs:   []int64{100, 101},
		},
		{
			name:         "nil output fields",
			outputFields: nil,
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField,
						Name:    common.RowIDFieldName,
					},
					{
						FieldID:      100,
						Name:         "ID",
						IsPrimaryKey: true,
					},
					{
						FieldID: 101,
						Name:    "Vector",
					},
				},
			},
			expectedError: false,
			expectedIDs:   []int64{100, 101},
		},
		{
			name:         "full list",
			outputFields: []string{"ID", "Vector"},
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField,
						Name:    common.RowIDFieldName,
					},
					{
						FieldID:      100,
						Name:         "ID",
						IsPrimaryKey: true,
					},
					{
						FieldID: 101,
						Name:    "Vector",
					},
				},
			},
			expectedError: false,
			expectedIDs:   []int64{100, 101},
		},
		{
			name:         "vector only",
			outputFields: []string{"Vector"},
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField,
						Name:    common.RowIDFieldName,
					},
					{
						FieldID:      100,
						Name:         "ID",
						IsPrimaryKey: true,
					},
					{
						FieldID: 101,
						Name:    "Vector",
					},
				},
			},
			expectedError: false,
			expectedIDs:   []int64{101, 100},
		},
		{
			name:         "with field not exist",
			outputFields: []string{"ID", "Vector", "Extra"},
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField,
						Name:    common.RowIDFieldName,
					},
					{
						FieldID:      100,
						Name:         "ID",
						IsPrimaryKey: true,
					},
					{
						FieldID: 101,
						Name:    "Vector",
					},
				},
			},
			expectedError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := translateToOutputFieldIDs(tc.outputFields, tc.schema)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, len(tc.expectedIDs), len(ids))
				for idx, expectedID := range tc.expectedIDs {
					assert.Equal(t, expectedID, ids[idx])
				}
			}
		})
	}
}

func TestTaskQuery_functions(t *testing.T) {
	t.Run("test parseQueryParams", func(t *testing.T) {
		tests := []struct {
			description string

			inKey   []string
			inValue []string

			expectErr bool
			outLimit  int64
			outOffset int64
		}{
			{"empty input", []string{}, []string{}, false, typeutil.Unlimited, 0},
			{"valid limit=1", []string{LimitKey}, []string{"1"}, false, 1, 0},
			{"valid limit=1, offset=2", []string{LimitKey, OffsetKey}, []string{"1", "2"}, false, 1, 2},
			{"valid no limit, offset=2", []string{OffsetKey}, []string{"2"}, false, typeutil.Unlimited, 0},
			{"invalid limit str", []string{LimitKey}, []string{"a"}, true, 0, 0},
			{"invalid limit zero", []string{LimitKey}, []string{"0"}, true, 0, 0},
			{"invalid limit negative", []string{LimitKey}, []string{"-1"}, true, 0, 0},
			{"invalid limit 16385", []string{LimitKey}, []string{"16385"}, true, 0, 0},
			{"invalid offset negative", []string{LimitKey, OffsetKey}, []string{"1", "-1"}, true, 0, 0},
			{"invalid offset 16385", []string{LimitKey, OffsetKey}, []string{"1", "16385"}, true, 0, 0},
			{"invalid limit=16384 offset=16384", []string{LimitKey, OffsetKey}, []string{"16384", "16384"}, true, 0, 0},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				var inParams []*commonpb.KeyValuePair
				for i := range test.inKey {
					inParams = append(inParams, &commonpb.KeyValuePair{
						Key:   test.inKey[i],
						Value: test.inValue[i],
					})

				}
				ret, err := parseQueryParams(inParams)
				if test.expectErr {
					assert.Error(t, err)
					assert.Empty(t, ret)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, test.outLimit, ret.limit)
					assert.Equal(t, test.outOffset, ret.offset)
				}
			})
		}
	})

	t.Run("test reduceRetrieveResults", func(t *testing.T) {
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
		fieldDataArray1 = append(fieldDataArray1, getFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
		fieldDataArray1 = append(fieldDataArray1, getFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

		var fieldDataArray2 []*schemapb.FieldData
		fieldDataArray2 = append(fieldDataArray2, getFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
		fieldDataArray2 = append(fieldDataArray2, getFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

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

			result, err := reduceRetrieveResults(context.Background(), []*internalpb.RetrieveResults{result1, result2}, nil)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(result.GetFieldsData()))
			assert.Equal(t, Int64Array, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			assert.InDeltaSlice(t, FloatVector, result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
		})

		t.Run("test nil results", func(t *testing.T) {
			ret, err := reduceRetrieveResults(context.Background(), nil, nil)
			assert.NoError(t, err)
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
					limit       int64
				}{
					{"limit 1", 1},
					{"limit 2", 2},
					{"limit 3", 3},
					{"limit 4", 4},
				}
				resultField0 := []int64{11, 11, 22, 22}
				for _, test := range tests {
					t.Run(test.description, func(t *testing.T) {
						result, err := reduceRetrieveResults(context.Background(), []*internalpb.RetrieveResults{r1, r2}, &queryParams{limit: test.limit})
						assert.Equal(t, 2, len(result.GetFieldsData()))
						assert.Equal(t, resultField0[0:test.limit], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
						assert.InDeltaSlice(t, resultFloat[0:test.limit*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
						assert.NoError(t, err)
					})
				}

			})

			t.Run("test offset", func(t *testing.T) {
				tests := []struct {
					description string
					offset      int64
				}{
					{"offset 0", 0},
					{"offset 1", 1},
					{"offset 2", 2},
					{"offset 3", 3},
				}
				resultField0 := []int64{11, 11, 22, 22}
				for _, test := range tests {
					t.Run(test.description, func(t *testing.T) {
						result, err := reduceRetrieveResults(context.Background(), []*internalpb.RetrieveResults{r1, r2}, &queryParams{limit: 1, offset: test.offset})
						assert.NoError(t, err)
						assert.Equal(t, 2, len(result.GetFieldsData()))
						assert.Equal(t, resultField0[test.offset:test.offset+1], result.GetFieldsData()[0].GetScalars().GetLongData().Data)
						assert.InDeltaSlice(t, resultFloat[test.offset*Dim:(test.offset+1)*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
					})
				}
			})
		})
	})
}

func getFieldData(fieldName string, fieldID int64, fieldType schemapb.DataType, fieldValue interface{}, dim int64) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch fieldType {
	case schemapb.DataType_Bool:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Bool,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: fieldValue.([]bool),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int32:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: fieldValue.([]int32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int64:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: fieldValue.([]int64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Float:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Double:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: fieldValue.([]float64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_VarChar:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
						},
					},
				},
			},
			FieldId: fieldID,
		}

	case schemapb.DataType_BinaryVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_FloatVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	default:
		log.Warn("not supported field type", zap.String("fieldType", fieldType.String()))
	}

	return fieldData
}

func Test_filterSystemFields(t *testing.T) {
	outputFieldIDs := []UniqueID{common.RowIDField, common.TimeStampField, common.StartOfUserFieldID}
	filtered := filterSystemFields(outputFieldIDs)
	assert.ElementsMatch(t, []UniqueID{common.StartOfUserFieldID}, filtered)
}

func Test_matchCountRule(t *testing.T) {
	type args struct {
		outputs []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				outputs: nil,
			},
			want: false,
		},
		{
			args: args{
				outputs: []string{"count(*)", "count(*)"},
			},
			want: false,
		},
		{
			args: args{
				outputs: []string{"not count(*)"},
			},
			want: false,
		},
		{
			args: args{
				outputs: []string{"count(*)"},
			},
			want: true,
		},
		{
			args: args{
				outputs: []string{"COUNT(*)"},
			},
			want: true,
		},
		{
			args: args{
				outputs: []string{"      count(*)"},
			},
			want: true,
		},
		{
			args: args{
				outputs: []string{"      COUNT(*)"},
			},
			want: true,
		},
		{
			args: args{
				outputs: []string{"count(*)     "},
			},
			want: true,
		},
		{
			args: args{
				outputs: []string{"COUNT(*)     "},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchCountRule(tt.args.outputs), "matchCountRule(%v)", tt.args.outputs)
		})
	}
}

func Test_createCntPlan(t *testing.T) {
	t.Run("plan without filter", func(t *testing.T) {
		plan, err := createCntPlan("", nil)
		assert.NoError(t, err)
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.Nil(t, plan.GetQuery().GetPredicates())
	})

	t.Run("invalid schema", func(t *testing.T) {
		_, err := createCntPlan("a > b", nil)
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "a",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}
		plan, err := createCntPlan("a > 4", schema)
		assert.NoError(t, err)
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.NotNil(t, plan.GetQuery().GetPredicates())
	})
}

func Test_queryTask_createPlan(t *testing.T) {
	t.Run("match count rule", func(t *testing.T) {
		tsk := &queryTask{
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"count(*)"},
			},
		}
		err := tsk.createPlan(context.TODO())
		assert.NoError(t, err)
		plan := tsk.plan
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.Nil(t, plan.GetQuery().GetPredicates())
	})

	t.Run("query without expression", func(t *testing.T) {

		tsk := &queryTask{
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"a"},
			},
		}
		err := tsk.createPlan(context.TODO())
		assert.Error(t, err)
	})

	t.Run("invalid expression", func(t *testing.T) {

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "a",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}

		tsk := &queryTask{
			schema: schema,
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"a"},
				Expr:         "b > 2",
			},
		}
		err := tsk.createPlan(context.TODO())
		assert.Error(t, err)
	})

	t.Run("invalid output fields", func(t *testing.T) {

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "a",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}

		tsk := &queryTask{
			schema: schema,
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"b"},
				Expr:         "a > 2",
			},
		}
		err := tsk.createPlan(context.TODO())
		assert.Error(t, err)
	})
}

func TestQueryTask_IDs2Expr(t *testing.T) {
	fieldName := "pk"
	intIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2, 3, 4, 5},
			},
		},
	}
	stringIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"a", "b", "c"},
			},
		},
	}
	idExpr := IDs2Expr(fieldName, intIDs)
	expectIDExpr := "pk in [ 1, 2, 3, 4, 5 ]"
	assert.Equal(t, expectIDExpr, idExpr)

	strExpr := IDs2Expr(fieldName, stringIDs)
	expectStrExpr := "pk in [ \"a\", \"b\", \"c\" ]"
	assert.Equal(t, expectStrExpr, strExpr)
}
