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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestQueryTask_all(t *testing.T) {
	var (
		err error
		ctx = context.TODO()

		qc = NewMixCoordMock()
		qn = getQueryNodeClient()

		shardsNum      = common.DefaultShardsNum
		collectionName = t.Name() + funcutil.GenRandomStr()

		expr   = fmt.Sprintf("%s > 0", testInt64Field)
		hitNum = 10
	)

	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	lb := NewLBPolicyImpl(mgr)

	err = InitMetaCache(ctx, qc, mgr)
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
		ctx:      ctx,
		mixCoord: qc,
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

	t.Run("test query task parameters", func(t *testing.T) {
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
				Status:     merr.Success(),
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
				},
			},
			mixCoord: qc,
			lb:       lb,
		}

		assert.NoError(t, task.OnEnqueue())

		// test query task with timeout
		ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
		defer cancel1()
		// before preExecute
		assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
		task.ctx = ctx1
		assert.NoError(t, task.PreExecute(ctx))

		{
			task.mustUsePartitionKey = true
			err := task.PreExecute(ctx)
			assert.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			task.mustUsePartitionKey = false
		}

		// after preExecute
		assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

		// check reduce_stop_for_best
		assert.Equal(t, false, task.RetrieveRequest.GetReduceStopForBest())
		task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{
			Key:   ReduceStopForBestKey,
			Value: "trxxxx",
		})
		assert.Error(t, task.PreExecute(ctx))
		task.request.QueryParams = task.request.QueryParams[0 : len(task.request.QueryParams)-1]

		// check parse collection id
		task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{
			Key:   CollectionID,
			Value: "trxxxx",
		})
		err := task.PreExecute(ctx)
		assert.Error(t, err)
		task.request.QueryParams = task.request.QueryParams[0 : len(task.request.QueryParams)-1]

		// check collection id consistency
		task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{
			Key:   LimitKey,
			Value: "11",
		})
		task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{
			Key:   CollectionID,
			Value: "8080",
		})
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.request.QueryParams = make([]*commonpb.KeyValuePair, 0)

		result1 := &internalpb.RetrieveResults{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_RetrieveResult},
			Status: merr.Success(),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: testutils.GenerateInt64Array(hitNum)},
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
		qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		assert.Error(t, task.Execute(ctx))

		qn.ExpectedCalls = nil
		qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: merr.Status(merr.ErrChannelNotAvailable),
		}, nil)
		err = task.Execute(ctx)
		assert.ErrorIs(t, err, merr.ErrChannelNotAvailable)

		qn.ExpectedCalls = nil
		qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil)
		assert.Error(t, task.Execute(ctx))

		qn.ExpectedCalls = nil
		qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
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
	})

	t.Run("test query for iterator", func(t *testing.T) {
		qt := &queryTask{
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
				Status:     merr.Success(),
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
						Key:   IteratorField,
						Value: "True",
					},
				},
			},
			mixCoord:  qc,
			lb:        lb,
			resultBuf: &typeutil.ConcurrentSet[*internalpb.RetrieveResults]{},
		}
		// simulate scheduler enqueue task
		enqueTs := uint64(10000)
		qt.SetTs(enqueTs)
		qtErr := qt.PreExecute(context.TODO())
		assert.Nil(t, qtErr)
		assert.True(t, qt.queryParams.isIterator)
		qt.resultBuf.Insert(&internalpb.RetrieveResults{})
		qtErr = qt.PostExecute(context.TODO())
		assert.Nil(t, qtErr)
		// after first page, sessionTs is set
		assert.True(t, qt.result.GetSessionTs() > 0)

		// next page query task
		qt = &queryTask{
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
				Status:     merr.Success(),
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
						Key:   IteratorField,
						Value: "True",
					},
				},
				GuaranteeTimestamp: enqueTs,
			},
			mixCoord:  qc,
			lb:        lb,
			resultBuf: &typeutil.ConcurrentSet[*internalpb.RetrieveResults]{},
		}
		qtErr = qt.PreExecute(context.TODO())
		assert.Nil(t, qtErr)
		assert.True(t, qt.queryParams.isIterator)
		// from the second page, the mvccTs is set to the sessionTs init in the first page
		assert.Equal(t, enqueTs, qt.GetMvccTimestamp())
	})
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
			expectedIDs:   []int64{100},
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
			expectedIDs:   []int64{100},
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

	t.Run("test parseQueryParams for reduce type", func(t *testing.T) {
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "True",
			})
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "True",
			})
			ret, err := parseQueryParams(inParams)
			assert.NoError(t, err)
			assert.Equal(t, reduce.IReduceInOrderForBest, ret.reduceType)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "True",
			})
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "TrueXXXX",
			})
			ret, err := parseQueryParams(inParams)
			assert.Error(t, err)
			assert.Nil(t, ret)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "TrueXXXXX",
			})
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "True",
			})
			ret, err := parseQueryParams(inParams)
			assert.Error(t, err)
			assert.Nil(t, ret)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "True",
			})
			// when not setting iterator tag, ignore reduce_stop_for_best
			ret, err := parseQueryParams(inParams)
			assert.NoError(t, err)
			assert.Equal(t, reduce.IReduceNoOrder, ret.reduceType)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "True",
			})
			// when not setting reduce_stop_for_best tag, reduce by keep results in order
			ret, err := parseQueryParams(inParams)
			assert.NoError(t, err)
			assert.Equal(t, reduce.IReduceInOrder, ret.reduceType)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "False",
			})
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "True",
			})
			ret, err := parseQueryParams(inParams)
			assert.NoError(t, err)
			assert.Equal(t, reduce.IReduceInOrder, ret.reduceType)
		}
		{
			var inParams []*commonpb.KeyValuePair
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   ReduceStopForBestKey,
				Value: "False",
			})
			inParams = append(inParams, &commonpb.KeyValuePair{
				Key:   IteratorField,
				Value: "False",
			})
			ret, err := parseQueryParams(inParams)
			assert.NoError(t, err)
			assert.Equal(t, reduce.IReduceNoOrder, ret.reduceType)
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

		t.Run("test nil results", func(t *testing.T) {
			ret, err := reduceRetrieveResults(context.Background(), nil, &queryParams{})
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
				11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0,
			}

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

			t.Run("test unLimited and maxOutputSize", func(t *testing.T) {
				paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1")

				ids := make([]int64, 100)
				offsets := make([]int64, 100)
				for i := range ids {
					ids[i] = int64(i)
					offsets[i] = int64(i)
				}
				fieldData := getFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, ids, 1)

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

				_, err := reduceRetrieveResults(context.Background(), []*internalpb.RetrieveResults{result}, &queryParams{limit: typeutil.Unlimited})
				assert.Error(t, err)
				paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxOutputSize.Key, "1104857600")
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

			t.Run("test stop reduce for best for limit", func(t *testing.T) {
				r1.HasMoreResult = true
				r2.HasMoreResult = false
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: 2, reduceType: reduce.IReduceInOrderForBest})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))
				assert.Equal(t, []int64{11, 11, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
				len := len(result.GetFieldsData()[0].GetScalars().GetLongData().Data)
				assert.InDeltaSlice(t, resultFloat[0:(len)*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			})

			t.Run("test stop reduce for best for limit and offset", func(t *testing.T) {
				r1.HasMoreResult = true
				r2.HasMoreResult = true
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: 1, offset: 1, reduceType: reduce.IReduceInOrderForBest})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))
				assert.Equal(t, []int64{11, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			})

			t.Run("test stop reduce for best for limit and offset", func(t *testing.T) {
				r1.HasMoreResult = false
				r2.HasMoreResult = true
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: 2, offset: 1, reduceType: reduce.IReduceInOrderForBest})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))

				// we should get 6 result back in total, but only get 4, which means all the result should actually be part of result
				assert.Equal(t, []int64{11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			})

			t.Run("test stop reduce for best for unlimited set", func(t *testing.T) {
				r1.HasMoreResult = false
				r2.HasMoreResult = false
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: typeutil.Unlimited, reduceType: reduce.IReduceInOrderForBest})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))
				assert.Equal(t, []int64{11, 11, 22, 22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
				len := len(result.GetFieldsData()[0].GetScalars().GetLongData().Data)
				assert.InDeltaSlice(t, resultFloat[0:(len)*Dim], result.FieldsData[1].GetVectors().GetFloatVector().Data, 10e-10)
			})

			t.Run("test stop reduce for best for unlimited set amd offset", func(t *testing.T) {
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: typeutil.Unlimited, offset: 3, reduceType: reduce.IReduceInOrderForBest})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))
				assert.Equal(t, []int64{22}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
			})
			t.Run("test iterator without setting reduce stop for best", func(t *testing.T) {
				r1.HasMoreResult = true
				r2.HasMoreResult = true
				result, err := reduceRetrieveResults(context.Background(),
					[]*internalpb.RetrieveResults{r1, r2},
					&queryParams{limit: 1, reduceType: reduce.IReduceInOrder})
				assert.NoError(t, err)
				assert.Equal(t, 2, len(result.GetFieldsData()))
				assert.Equal(t, []int64{11}, result.GetFieldsData()[0].GetScalars().GetLongData().Data)
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
		plan, err := createCntPlan("", nil, nil)
		assert.NoError(t, err)
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.Nil(t, plan.GetQuery().GetPredicates())
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
		schemaHelper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		plan, err := createCntPlan("a > 4", schemaHelper, nil)
		assert.NoError(t, err)
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.NotNil(t, plan.GetQuery().GetPredicates())
	})
}

func Test_queryTask_createPlan(t *testing.T) {
	collSchema := newTestSchema()
	t.Run("match count rule", func(t *testing.T) {
		schema := newSchemaInfo(collSchema)
		tsk := &queryTask{
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"count(*)"},
			},
			schema: schema,
		}
		err := tsk.createPlan(context.TODO())
		assert.NoError(t, err)
		plan := tsk.plan
		assert.True(t, plan.GetQuery().GetIsCount())
		assert.Nil(t, plan.GetQuery().GetPredicates())
	})

	t.Run("query without expression", func(t *testing.T) {
		schema := newSchemaInfo(collSchema)
		tsk := &queryTask{
			request: &milvuspb.QueryRequest{
				OutputFields: []string{"Int64"},
			},
			schema: schema,
		}
		err := tsk.createPlan(context.TODO())
		assert.Error(t, err)
	})

	t.Run("invalid expression", func(t *testing.T) {
		schema := newSchemaInfo(collSchema)

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
		schema := newSchemaInfo(collSchema)

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

func TestQueryTask_CanSkipAllocTimestamp(t *testing.T) {
	dbName := "test_query"
	collName := "test_skip_alloc_timestamp"
	collID := UniqueID(111)
	mockMetaCache := NewMockCache(t)
	globalMetaCache = mockMetaCache

	t.Run("default consistency level", func(t *testing.T) {
		qt := &queryTask{
			request: &milvuspb.QueryRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: true,
			},
		}
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, nil)
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil).Once()

		skip := qt.CanSkipAllocTimestamp()
		assert.True(t, skip)

		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Bounded,
			}, nil).Once()
		skip = qt.CanSkipAllocTimestamp()
		assert.True(t, skip)

		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Strong,
			}, nil).Once()
		skip = qt.CanSkipAllocTimestamp()
		assert.False(t, skip)
	})

	t.Run("request consistency level", func(t *testing.T) {
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil).Times(3)

		qt := &queryTask{
			request: &milvuspb.QueryRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip := qt.CanSkipAllocTimestamp()
		assert.True(t, skip)

		qt.request.ConsistencyLevel = commonpb.ConsistencyLevel_Bounded
		skip = qt.CanSkipAllocTimestamp()
		assert.True(t, skip)

		qt.request.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
		skip = qt.CanSkipAllocTimestamp()
		assert.False(t, skip)
	})

	t.Run("legacy_guarantee_ts", func(t *testing.T) {
		qt := &queryTask{
			request: &milvuspb.QueryRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
			},
		}

		skip := qt.CanSkipAllocTimestamp()
		assert.False(t, skip)

		qt.request.GuaranteeTimestamp = 1 // eventually
		skip = qt.CanSkipAllocTimestamp()
		assert.True(t, skip)

		qt.request.GuaranteeTimestamp = 2 // bounded
		skip = qt.CanSkipAllocTimestamp()
		assert.True(t, skip)
	})

	t.Run("failed", func(t *testing.T) {
		mockMetaCache.ExpectedCalls = nil
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, nil)
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("mock error")).Once()

		qt := &queryTask{
			request: &milvuspb.QueryRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: true,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip := qt.CanSkipAllocTimestamp()
		assert.False(t, skip)

		mockMetaCache.ExpectedCalls = nil
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, errors.New("mock error"))
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil)

		skip = qt.CanSkipAllocTimestamp()
		assert.False(t, skip)

		qt2 := &queryTask{
			request: &milvuspb.QueryRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip = qt2.CanSkipAllocTimestamp()
		assert.True(t, skip)
	})
}

func TestQueryTask_DistanceQuery(t *testing.T) {
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		panic(err)
	}
	schemaInfo := &schemaInfo{
		CollectionSchema: schema,
		schemaHelper:     schemaHelper,
	}

	tests := []struct {
		name                  string
		expr                  string
		outputFields          []string
		queryParams           []*commonpb.KeyValuePair
		expectedDistanceQuery bool
	}{
		{
			name:         "distance function in expr",
			expr:         "distance(a.vector, b.vector, 'L2') as _distance",
			outputFields: []string{"a.id as id1", "b.id as id2", "_distance"},
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"alias": "a", "source": "collection", "filter": "id IN [1,2,3]"},
						{"alias": "b", "source": "collection", "filter": "id IN [4,5,6]"}
					]`,
				},
			},
			expectedDistanceQuery: true,
		},
		{
			name:         "distance function in output fields",
			expr:         "",
			outputFields: []string{"distance(a.vector, b.vector, 'L2') as _distance"},
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"alias": "a", "source": "collection", "filter": "id IN [1,2,3]"},
						{"alias": "b", "source": "collection", "filter": "id IN [4,5,6]"}
					]`,
				},
			},
			expectedDistanceQuery: true,
		},
		{
			name:                  "regular query without distance",
			expr:                  "id > 0",
			outputFields:          []string{"id"},
			queryParams:           []*commonpb.KeyValuePair{},
			expectedDistanceQuery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &queryTask{
				ctx:    ctx,
				schema: schemaInfo,
				request: &milvuspb.QueryRequest{
					CollectionName: "test_collection",
					Expr:           tt.expr,
					OutputFields:   tt.outputFields,
					QueryParams:    tt.queryParams,
				},
				RetrieveRequest: &internalpb.RetrieveRequest{
					CollectionID: 1,
				},
			}

			err := task.processDistanceQuery(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedDistanceQuery, task.isDistanceQuery)
		})
	}
}

func TestQueryTask_containsDistanceFunction(t *testing.T) {
	tests := []struct {
		name         string
		expr         string
		outputFields []string
		expected     bool
	}{
		{
			name:     "distance in expr",
			expr:     "distance(a.vector, b.vector, 'L2') > 0.5",
			expected: true,
		},
		{
			name:         "distance in output fields",
			expr:         "",
			outputFields: []string{"distance(a.vector, b.vector, 'L2') as _distance"},
			expected:     true,
		},
		{
			name:         "uppercase DISTANCE",
			expr:         "",
			outputFields: []string{"DISTANCE(a.vector, b.vector, 'L2') as _distance"},
			expected:     true,
		},
		{
			name:         "no distance function",
			expr:         "id > 0",
			outputFields: []string{"id", "vector"},
			expected:     false,
		},
		{
			name:         "distance in field name but not function",
			expr:         "distance_field > 0",
			outputFields: []string{"distance_field"},
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &queryTask{
				request: &milvuspb.QueryRequest{
					Expr:         tt.expr,
					OutputFields: tt.outputFields,
				},
			}

			result := task.containsDistanceFunction()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryTask_hasDistanceInString(t *testing.T) {
	task := &queryTask{
		ctx: context.Background(),
	}

	tests := []struct {
		text     string
		expected bool
	}{
		{"distance(a.vector, b.vector, 'L2')", true},
		{"DISTANCE(a.vector, b.vector, 'L2')", true},
		{"no distance function here", false},
		{"distance_field", false},
		{"", false},
		{"some distance( function", true},
	}

	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			result := task.hasDistanceInString(tt.text)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryTask_parseFromSources(t *testing.T) {
	tests := []struct {
		name        string
		queryParams []*commonpb.KeyValuePair
		expectError bool
		expectedLen int
	}{
		{
			name:        "no query params",
			queryParams: nil,
			expectError: false,
			expectedLen: 0,
		},
		{
			name: "no from parameter",
			queryParams: []*commonpb.KeyValuePair{
				{Key: "limit", Value: "100"},
			},
			expectError: false,
			expectedLen: 0,
		},
		{
			name: "valid from parameter",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"alias": "a", "source": "collection", "filter": "id IN [1,2,3]"},
						{"alias": "b", "source": "collection", "filter": "id IN [4,5,6]"}
					]`,
				},
			},
			expectError: false,
			expectedLen: 2,
		},
		{
			name: "invalid JSON in from parameter",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key:   "from",
					Value: `[invalid json`,
				},
			},
			expectError: true,
			expectedLen: 0,
		},
		{
			name: "empty from array",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key:   "from",
					Value: `[]`,
				},
			},
			expectError: true,
			expectedLen: 0,
		},
		{
			name: "missing alias",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"source": "collection", "filter": "id IN [1,2,3]"}
					]`,
				},
			},
			expectError: true,
			expectedLen: 0,
		},
		{
			name: "duplicate alias",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"alias": "a", "source": "collection", "filter": "id IN [1,2,3]"},
						{"alias": "a", "source": "collection", "filter": "id IN [4,5,6]"}
					]`,
				},
			},
			expectError: true,
			expectedLen: 0,
		},
		{
			name: "external vector source",
			queryParams: []*commonpb.KeyValuePair{
				{
					Key: "from",
					Value: `[
						{"alias": "a", "source": "collection", "filter": "id IN [1,2,3]"},
						{"alias": "b", "source": "external", "vectors": [[1.0, 2.0], [3.0, 4.0]]}
					]`,
				},
			},
			expectError: false,
			expectedLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &queryTask{
				request: &milvuspb.QueryRequest{
					QueryParams: tt.queryParams,
				},
				aliasMap:        make(map[string]*schemaInfo),
				externalVectors: make(map[string]*planpb.ExternalVectorData),
			}

			err := task.parseFromSources()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedLen, len(task.fromSources))
			}
		})
	}
}

func TestIsAliasField(t *testing.T) {
	tests := []struct {
		fieldName string
		expected  bool
	}{
		{"_distance", true},
		{"distance(a.vector, b.vector, 'L2') as _distance", true},
		{"a.id as id1", true},
		{"b.vector as vec", true},
		{"a.id", true},
		{"b.vector", true},
		{"_score", true},
		{"_rank", true},
		{"regular_field", false},
		{"id", false},
		{"vector", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.fieldName, func(t *testing.T) {
			result := isAliasField(tt.fieldName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTranslateToOutputFieldIDsForDistanceQuery(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}

	tests := []struct {
		name            string
		outputFields    []string
		isDistanceQuery bool
		expectError     bool
		expectedLen     int
	}{
		{
			name:            "regular query with existing fields",
			outputFields:    []string{"id", "vector"},
			isDistanceQuery: false,
			expectError:     false,
			expectedLen:     2, // Both fields + primary key already included
		},
		{
			name:            "distance query with alias fields",
			outputFields:    []string{"a.id as id1", "_distance"},
			isDistanceQuery: true,
			expectError:     false,
			expectedLen:     2,
		},
		{
			name:            "regular query with non-existent field",
			outputFields:    []string{"non_existent_field"},
			isDistanceQuery: false,
			expectError:     true,
			expectedLen:     0,
		},
		{
			name:            "distance query with distance expression",
			outputFields:    []string{"distance(a.vector, b.vector, 'L2') as _distance"},
			isDistanceQuery: true,
			expectError:     false,
			expectedLen:     1,
		},
		{
			name:            "empty output fields",
			outputFields:    []string{},
			isDistanceQuery: false,
			expectError:     false,
			expectedLen:     1, // Primary key only
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := translateToOutputFieldIDsForDistanceQuery(tt.outputFields, schema, tt.isDistanceQuery)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedLen, len(result))
			}
		})
	}
}

func TestValidateFromSource(t *testing.T) {
	task := &queryTask{}

	tests := []struct {
		name        string
		source      FromSourceDefinition
		expectError bool
	}{
		{
			name: "valid collection source",
			source: FromSourceDefinition{
				Alias:  "a",
				Source: "collection",
				Filter: "id IN [1,2,3]",
			},
			expectError: false,
		},
		{
			name: "valid external source",
			source: FromSourceDefinition{
				Alias:   "b",
				Source:  "external",
				Vectors: [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			expectError: false,
		},
		{
			name: "missing alias",
			source: FromSourceDefinition{
				Source: "collection",
				Filter: "id IN [1,2,3]",
			},
			expectError: true,
		},
		{
			name: "invalid alias format",
			source: FromSourceDefinition{
				Alias:  "123invalid",
				Source: "collection",
				Filter: "id IN [1,2,3]",
			},
			expectError: true,
		},
		{
			name: "missing source type",
			source: FromSourceDefinition{
				Alias:  "a",
				Filter: "id IN [1,2,3]",
			},
			expectError: true,
		},
		{
			name: "unsupported source type",
			source: FromSourceDefinition{
				Alias:  "a",
				Source: "unsupported",
			},
			expectError: true,
		},
		{
			name: "collection source missing filter",
			source: FromSourceDefinition{
				Alias:  "a",
				Source: "collection",
			},
			expectError: true,
		},
		{
			name: "external source missing vectors",
			source: FromSourceDefinition{
				Alias:  "b",
				Source: "external",
			},
			expectError: true,
		},
		{
			name: "external source with inconsistent vector dimensions",
			source: FromSourceDefinition{
				Alias:   "b",
				Source:  "external",
				Vectors: [][]float32{{1.0, 2.0}, {3.0, 4.0, 5.0}},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := task.validateFromSource(&tt.source, 0)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsValidAlias(t *testing.T) {
	tests := []struct {
		alias    string
		expected bool
	}{
		{"a", true},
		{"source_a", true},
		{"Source1", true},
		{"_alias", true},
		{"123invalid", false},
		{"invalid-alias", false},
		{"", false},
		{"very_long_alias_name_that_exceeds_the_maximum_allowed_length_limit", false},
		{"valid_alias_123", true},
		{"ALIAS", true},
	}

	for _, tt := range tests {
		t.Run(tt.alias, func(t *testing.T) {
			result := isValidAlias(tt.alias)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsValidFloat32(t *testing.T) {
	tests := []struct {
		name     string
		value    float32
		expected bool
	}{
		{"normal value", 1.5, true},
		{"zero", 0.0, true},
		{"negative", -1.5, true},
		{"NaN", float32(math.NaN()), false},
		{"positive infinity", float32(math.Inf(1)), false},
		{"negative infinity", float32(math.Inf(-1)), false},
		{"very small value", 1e-10, true},
		{"very large value", 1e10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidFloat32(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_reconstructStructFieldData(t *testing.T) {
	t.Run("count(*) query - should return early", func(t *testing.T) {
		results := &milvuspb.QueryResults{
			OutputFields: []string{"count(*)"},
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "count(*)",
					FieldId:   0,
					Type:      schemapb.DataType_Int64,
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "sub_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(results.FieldsData))
		copy(originalFieldsData, results.FieldsData)
		originalOutputFields := make([]string, len(results.OutputFields))
		copy(originalOutputFields, results.OutputFields)

		reconstructStructFieldData(results, schema)

		// Should not modify anything for count(*) query
		assert.Equal(t, originalFieldsData, results.FieldsData)
		assert.Equal(t, originalOutputFields, results.OutputFields)
	})

	t.Run("no struct array fields - should return early", func(t *testing.T) {
		results := &milvuspb.QueryResults{
			OutputFields: []string{"field1", "field2"},
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "field1",
					FieldId:   100,
					Type:      schemapb.DataType_Int64,
				},
				{
					FieldName: "field2",
					FieldId:   101,
					Type:      schemapb.DataType_VarChar,
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(results.FieldsData))
		copy(originalFieldsData, results.FieldsData)
		originalOutputFields := make([]string, len(results.OutputFields))
		copy(originalOutputFields, results.OutputFields)

		reconstructStructFieldData(results, schema)

		// Should not modify anything when no struct array fields
		assert.Equal(t, originalFieldsData, results.FieldsData)
		assert.Equal(t, originalOutputFields, results.OutputFields)
	})

	t.Run("reconstruct single struct field", func(t *testing.T) {
		// Create mock data
		subField1Data := &schemapb.FieldData{
			FieldName: "sub_int_array",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_IntData{
										IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
									},
								},
							},
						},
					},
				},
			},
		}

		subField2Data := &schemapb.FieldData{
			FieldName: "sub_text_array",
			FieldId:   1022,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_VarChar,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{Data: []string{"hello", "world"}},
									},
								},
							},
						},
					},
				},
			},
		}

		results := &milvuspb.QueryResults{
			OutputFields: []string{"sub_int_array", "sub_text_array"},
			FieldsData:   []*schemapb.FieldData{subField1Data, subField2Data},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "sub_int_array",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							FieldID:     1022,
							Name:        "sub_text_array",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
						},
					},
				},
			},
		}

		reconstructStructFieldData(results, schema)

		// Check result
		assert.Len(t, results.FieldsData, 1, "Should only have one reconstructed struct field")
		assert.Len(t, results.OutputFields, 1, "Output fields should only have one")

		structField := results.FieldsData[0]
		assert.Equal(t, "test_struct", structField.FieldName)
		assert.Equal(t, int64(102), structField.FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, structField.Type)
		assert.Equal(t, "test_struct", results.OutputFields[0])

		// Check fields inside struct
		structArrays := structField.GetStructArrays()
		assert.NotNil(t, structArrays)
		assert.Len(t, structArrays.Fields, 2, "Struct should contain 2 sub fields")

		// Check sub fields
		var foundIntField, foundTextField bool
		for _, field := range structArrays.Fields {
			switch field.FieldId {
			case 1021:
				assert.Equal(t, "sub_int_array", field.FieldName)
				assert.Equal(t, schemapb.DataType_Array, field.Type)
				foundIntField = true
			case 1022:
				assert.Equal(t, "sub_text_array", field.FieldName)
				assert.Equal(t, schemapb.DataType_Array, field.Type)
				foundTextField = true
			}
		}
		assert.True(t, foundIntField, "Should find int array field")
		assert.True(t, foundTextField, "Should find text array field")
	})

	t.Run("mixed regular and struct fields", func(t *testing.T) {
		// Create regular field data
		regularField := &schemapb.FieldData{
			FieldName: "regular_field",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		}

		// Create struct sub field data
		subFieldData := &schemapb.FieldData{
			FieldName: "sub_field",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_IntData{
										IntData: &schemapb.IntArray{Data: []int32{10, 20}},
									},
								},
							},
						},
					},
				},
			},
		}

		results := &milvuspb.QueryResults{
			OutputFields: []string{"regular_field", "sub_field"},
			FieldsData:   []*schemapb.FieldData{regularField, subFieldData},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "regular_field",
					DataType: schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "sub_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		reconstructStructFieldData(results, schema)

		// Check result: should have 2 fields (1 regular + 1 reconstructed struct)
		assert.Len(t, results.FieldsData, 2)
		assert.Len(t, results.OutputFields, 2)

		// Check regular and struct fields both exist
		var foundRegularField, foundStructField bool
		for i, field := range results.FieldsData {
			switch field.FieldId {
			case 100:
				assert.Equal(t, "regular_field", field.FieldName)
				assert.Equal(t, schemapb.DataType_Int64, field.Type)
				assert.Equal(t, "regular_field", results.OutputFields[i])
				foundRegularField = true
			case 102:
				assert.Equal(t, "test_struct", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				assert.Equal(t, "test_struct", results.OutputFields[i])
				foundStructField = true
			}
		}
		assert.True(t, foundRegularField, "Should find regular field")
		assert.True(t, foundStructField, "Should find reconstructed struct field")
	})

	t.Run("multiple struct fields", func(t *testing.T) {
		// Create sub field for first struct
		struct1SubField := &schemapb.FieldData{
			FieldName: "struct1_sub",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
		}

		// Create sub fields for second struct
		struct2SubField1 := &schemapb.FieldData{
			FieldName: "struct2_sub1",
			FieldId:   1031,
			Type:      schemapb.DataType_Array,
		}

		struct2SubField2 := &schemapb.FieldData{
			FieldName: "struct2_sub2",
			FieldId:   1032,
			Type:      schemapb.DataType_Array,
		}

		results := &milvuspb.QueryResults{
			OutputFields: []string{"struct1_sub", "struct2_sub1", "struct2_sub2"},
			FieldsData:   []*schemapb.FieldData{struct1SubField, struct2SubField1, struct2SubField2},
		}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "struct1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "struct1_sub",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
				{
					FieldID: 103,
					Name:    "struct2",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1031,
							Name:        "struct2_sub1",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
						},
						{
							FieldID:     1032,
							Name:        "struct2_sub2",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Float,
						},
					},
				},
			},
		}

		reconstructStructFieldData(results, schema)

		// Check result: should have 2 reconstructed struct fields
		assert.Len(t, results.FieldsData, 2)
		assert.Len(t, results.OutputFields, 2)

		// Check both struct fields are reconstructed correctly
		foundStruct1, foundStruct2 := false, false
		for i, field := range results.FieldsData {
			switch field.FieldId {
			case 102:
				assert.Equal(t, "struct1", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				assert.Equal(t, "struct1", results.OutputFields[i])

				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 1)
				assert.Equal(t, int64(1021), structArrays.Fields[0].FieldId)
				foundStruct1 = true

			case 103:
				assert.Equal(t, "struct2", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				assert.Equal(t, "struct2", results.OutputFields[i])

				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 2)

				// Check struct2 contains two sub fields
				subFieldIds := make([]int64, 0, 2)
				for _, subField := range structArrays.Fields {
					subFieldIds = append(subFieldIds, subField.FieldId)
				}
				assert.Contains(t, subFieldIds, int64(1031))
				assert.Contains(t, subFieldIds, int64(1032))
				foundStruct2 = true
			}
		}
		assert.True(t, foundStruct1, "Should find struct1")
		assert.True(t, foundStruct2, "Should find struct2")
	})

	t.Run("empty fields data", func(t *testing.T) {
		results := &milvuspb.QueryResults{
			OutputFields: []string{},
			FieldsData:   []*schemapb.FieldData{},
		}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "sub_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		reconstructStructFieldData(results, schema)

		// Empty data should remain unchanged
		assert.Len(t, results.FieldsData, 0)
		assert.Len(t, results.OutputFields, 0)
	})

	t.Run("no matching sub fields", func(t *testing.T) {
		// Field data does not match any struct definition
		regularField := &schemapb.FieldData{
			FieldName: "regular_field",
			FieldId:   200, // Not in any struct
			Type:      schemapb.DataType_Int64,
		}

		results := &milvuspb.QueryResults{
			OutputFields: []string{"regular_field"},
			FieldsData:   []*schemapb.FieldData{regularField},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  200,
					Name:     "regular_field",
					DataType: schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "sub_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		reconstructStructFieldData(results, schema)

		// Should only keep the regular field, no struct field
		assert.Len(t, results.FieldsData, 1)
		assert.Len(t, results.OutputFields, 1)
		assert.Equal(t, int64(200), results.FieldsData[0].FieldId)
		assert.Equal(t, "regular_field", results.OutputFields[0])
	})
}
