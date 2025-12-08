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

package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type DDLCallbacksCollectionFunctionTestSuite struct {
	suite.Suite
	core            *Core
	mockBroadcaster *mock_broadcaster.MockBroadcastAPI
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) SetupTest() {
	suite.core = initStreamingSystemAndCore(suite.T())
	suite.mockBroadcaster = mock_broadcaster.NewMockBroadcastAPI(suite.T())
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TearDownTest() {
	suite.mockBroadcaster = nil
	suite.core.Stop()
	suite.core = nil
}

// Helper function to create a test collection
func (suite *DDLCallbacksCollectionFunctionTestSuite) createTestCollection() *model.Collection {
	return &model.Collection{
		DBID:         1,
		CollectionID: 100,
		Name:         "test_collection",
		Description:  "test collection",
		AutoID:       true,
		Fields: []*model.Field{
			{
				FieldID:          101,
				Name:             "id",
				IsPrimaryKey:     true,
				AutoID:           true,
				DataType:         schemapb.DataType_Int64,
				IsFunctionOutput: false,
			},
			{
				FieldID:          102,
				Name:             "vector",
				DataType:         schemapb.DataType_FloatVector,
				IsFunctionOutput: false,
			},
			{
				FieldID:          103,
				Name:             "text_field",
				DataType:         schemapb.DataType_VarChar,
				IsFunctionOutput: false,
			},
			{
				FieldID:          104,
				Name:             "output_field",
				DataType:         schemapb.DataType_FloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*model.Function{
			{
				ID:               1001,
				Name:             "test_function",
				Type:             schemapb.FunctionType_TextEmbedding,
				Description:      "test function",
				InputFieldIDs:    []int64{103},
				InputFieldNames:  []string{"text_field"},
				OutputFieldIDs:   []int64{104},
				OutputFieldNames: []string{"output_field"},
				Params: []*commonpb.KeyValuePair{
					{Key: "param1", Value: "value1"},
				},
			},
		},
		VirtualChannelNames: []string{"test_channel"},
		EnableDynamicField:  false,
		Properties:          []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
		SchemaVersion:       1,
	}
}

// Helper function to create a test function schema
func (suite *DDLCallbacksCollectionFunctionTestSuite) createTestFunctionSchema() *schemapb.FunctionSchema {
	return &schemapb.FunctionSchema{
		Name:             "new_function",
		Type:             schemapb.FunctionType_TextEmbedding,
		Description:      "new test function",
		InputFieldNames:  []string{"text_field"},
		OutputFieldNames: []string{"vector"},
		Params: []*commonpb.KeyValuePair{
			{Key: "param1", Value: "value1"},
		},
	}
}

func TestDDLCallbacksCollectionFunctionTestSuite(t *testing.T) {
	suite.Run(t, new(DDLCallbacksCollectionFunctionTestSuite))
}

// Test callAlterCollection function
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestCallAlterCollection_Success() {
	ctx := context.Background()
	coll := suite.createTestCollection()
	dbName := "test_db"
	collectionName := "test_collection"

	// Create a test core with proper meta setup
	core := newTestCore(withHealthyCode())
	mockMeta := mockrootcoord.NewIMetaTable(suite.T())
	core.meta = mockMeta

	// Mock meta calls for getCacheExpireForCollection
	mockMeta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(coll, nil)
	mockMeta.EXPECT().ListAliases(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return([]string{}, nil)

	// Mock broadcaster
	suite.mockBroadcaster.EXPECT().Broadcast(mock.Anything, mock.MatchedBy(func(msg message.BroadcastMutableMessage) bool {
		// Verify the message structure
		return msg != nil
	})).Return(&types.BroadcastAppendResult{}, nil)

	err := callAlterCollection(ctx, core, suite.mockBroadcaster, coll, dbName, collectionName)
	suite.NoError(err)
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestCallAlterCollection_GetCacheExpireFailed() {
	ctx := context.Background()
	coll := suite.createTestCollection()
	dbName := "test_db"
	collectionName := "test_collection"

	// Create a test core with proper meta setup
	core := newTestCore(withHealthyCode())
	mockMeta := mockrootcoord.NewIMetaTable(suite.T())
	core.meta = mockMeta

	// Mock meta calls to return error
	mockMeta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(nil, errors.New("cache expire error"))

	err := callAlterCollection(ctx, core, suite.mockBroadcaster, coll, dbName, collectionName)
	suite.Error(err)
	suite.Contains(err.Error(), "cache expire error")
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestCallAlterCollection_BroadcastFailed() {
	ctx := context.Background()
	coll := suite.createTestCollection()
	dbName := "test_db"
	collectionName := "test_collection"

	// Create a test core with proper meta setup
	core := newTestCore(withHealthyCode())
	mockMeta := mockrootcoord.NewIMetaTable(suite.T())
	core.meta = mockMeta

	// Mock meta calls for getCacheExpireForCollection
	mockMeta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(coll, nil)
	mockMeta.EXPECT().ListAliases(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return([]string{}, nil)

	// Mock broadcaster to return error
	suite.mockBroadcaster.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, errors.New("broadcast error"))

	err := callAlterCollection(ctx, core, suite.mockBroadcaster, coll, dbName, collectionName)
	suite.Error(err)
	suite.Contains(err.Error(), "broadcast error")
}

// Test alterFunctionGenNewCollection function
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_Success() {
	ctx := context.Background()
	coll := suite.createTestCollection()

	// Create a function schema to alter existing function
	fSchema := &schemapb.FunctionSchema{
		Name:             "test_function", // Same name as existing function
		Type:             schemapb.FunctionType_TextEmbedding,
		Description:      "updated test function",
		InputFieldNames:  []string{"text_field"},
		OutputFieldNames: []string{"vector"}, // Different output field
	}

	err := alterFunctionGenNewCollection(ctx, fSchema, coll)
	suite.NoError(err)

	// Verify function ID is preserved
	suite.Equal(int64(1001), fSchema.Id)

	// Verify input field IDs are set
	suite.Equal([]int64{103}, fSchema.InputFieldIds)

	// Verify output field IDs are set
	suite.Equal([]int64{102}, fSchema.OutputFieldIds)

	// Verify old output field is no longer marked as function output
	oldOutputField := coll.Fields[3] // output_field
	suite.False(oldOutputField.IsFunctionOutput)

	// Verify new output field is marked as function output
	newOutputField := coll.Fields[1] // vector
	suite.True(newOutputField.IsFunctionOutput)

	// Verify function is added to collection
	suite.Len(coll.Functions, 1) // Original + new
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_FunctionNotExists() {
	ctx := context.Background()
	coll := suite.createTestCollection()

	fSchema := &schemapb.FunctionSchema{
		Name: "non_existent_function",
	}

	err := alterFunctionGenNewCollection(ctx, fSchema, coll)
	suite.Error(err)
	suite.Contains(err.Error(), "Function non_existent_function not exists")
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_InputFieldNotExists() {
	ctx := context.Background()
	coll := suite.createTestCollection()

	fSchema := &schemapb.FunctionSchema{
		Name:            "test_function",
		InputFieldNames: []string{"non_existent_field"},
	}

	err := alterFunctionGenNewCollection(ctx, fSchema, coll)
	suite.Error(err)
	suite.Contains(err.Error(), "function's input field non_existent_field not exists")
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_OutputFieldNotExists() {
	ctx := context.Background()
	coll := suite.createTestCollection()

	fSchema := &schemapb.FunctionSchema{
		Name:             "test_function",
		InputFieldNames:  []string{"text_field"},
		OutputFieldNames: []string{"non_existent_field"},
	}

	err := alterFunctionGenNewCollection(ctx, fSchema, coll)
	suite.Error(err)
	suite.Contains(err.Error(), "function's output field non_existent_field not exists")
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForAddFunction_FunctionNameExists() {
	coll := suite.createTestCollection()

	// Create request with existing function name
	req := &milvuspb.AddCollectionFunctionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		FunctionSchema: &schemapb.FunctionSchema{
			Name: "test_function", // Same as existing function
		},
	}

	newColl := coll.Clone()
	fSchema := req.FunctionSchema

	// Check for function name conflict
	for _, f := range newColl.Functions {
		if f.Name == fSchema.Name {
			err := errors.New("function name already exists")
			suite.Error(err)
			return
		}
	}
}

// Test broadcastAlterCollectionForDropFunction
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForDropFunction_Success() {
	coll := suite.createTestCollection()
	req := &milvuspb.DropCollectionFunctionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		FunctionName:   "test_function",
	}

	// Find function to delete
	var needDelFunc *model.Function
	for _, f := range coll.Functions {
		if f.Name == req.FunctionName {
			needDelFunc = f
			break
		}
	}
	suite.NotNil(needDelFunc, "Function should exist")

	newColl := coll.Clone()

	// Remove function from collection
	newFuncs := []*model.Function{}
	for _, f := range newColl.Functions {
		if f.Name != needDelFunc.Name {
			newFuncs = append(newFuncs, f)
		}
	}
	newColl.Functions = newFuncs

	// Reset output field flags
	fieldMapping := map[int64]*model.Field{}
	for _, field := range newColl.Fields {
		fieldMapping[field.FieldID] = field
	}
	for _, id := range needDelFunc.OutputFieldIDs {
		field, exists := fieldMapping[id]
		suite.True(exists, "Output field should exist")
		field.IsFunctionOutput = false
	}

	// Verify function was removed
	suite.Len(newColl.Functions, 0)

	// Verify output field is no longer marked as function output
	outputField := fieldMapping[104] // output_field
	suite.False(outputField.IsFunctionOutput)
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForDropFunction_FunctionNotExists() {
	coll := suite.createTestCollection()
	req := &milvuspb.DropCollectionFunctionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		FunctionName:   "non_existent_function",
	}

	// Find function to delete
	var needDelFunc *model.Function
	for _, f := range coll.Functions {
		if f.Name == req.FunctionName {
			needDelFunc = f
			break
		}
	}
	suite.Nil(needDelFunc, "Function should not exist")
	// This should return nil (no error) as per the original implementation
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForDropFunction_OutputFieldNotExists() {
	coll := suite.createTestCollection()

	// Create a function with non-existent output field ID
	needDelFunc := &model.Function{
		ID:             1001,
		Name:           "test_function",
		OutputFieldIDs: []int64{999}, // Non-existent field ID
	}

	newColl := coll.Clone()
	fieldMapping := map[int64]*model.Field{}
	for _, field := range newColl.Fields {
		fieldMapping[field.FieldID] = field
	}

	for _, id := range needDelFunc.OutputFieldIDs {
		_, exists := fieldMapping[id]
		if !exists {
			err := errors.New("function's output field not exists")
			suite.Error(err)
			return
		}
	}
}

// Test edge cases and error conditions
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_OldOutputFieldNotExists() {
	coll := suite.createTestCollection()

	// Modify the existing function to have a non-existent output field
	coll.Functions[0].OutputFieldNames = []string{"non_existent_output"}

	fSchema := &schemapb.FunctionSchema{
		Name:             "test_function",
		InputFieldNames:  []string{"text_field"},
		OutputFieldNames: []string{"vector"},
	}

	err := alterFunctionGenNewCollection(context.Background(), fSchema, coll)
	suite.Error(err)
	suite.Contains(err.Error(), "Old version function's output field non_existent_output not exists")
}

// Test with empty collections and functions
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAlterFunctionGenNewCollection_EmptyCollection() {
	// Create collection with no functions
	coll := &model.Collection{
		Fields:    []*model.Field{},
		Functions: []*model.Function{},
	}

	fSchema := &schemapb.FunctionSchema{
		Name: "test_function",
	}

	err := alterFunctionGenNewCollection(context.Background(), fSchema, coll)
	suite.Error(err)
	suite.Contains(err.Error(), "Function test_function not exists")
}

// Test max function ID calculation
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestAddFunction_NextFunctionIDCalculation() {
	coll := suite.createTestCollection()

	// Add more functions to test max ID calculation
	coll.Functions = append(coll.Functions, &model.Function{
		ID:   2000,
		Name: "function2",
	})
	coll.Functions = append(coll.Functions, &model.Function{
		ID:   1500,
		Name: "function3",
	})

	nextFunctionID := int64(StartOfUserFunctionID)
	for _, f := range coll.Functions {
		nextFunctionID = max(nextFunctionID, f.ID+1)
	}

	suite.Equal(int64(2001), nextFunctionID)
}

// Test broadcastAlterCollectionForAlterFunction
func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForAlterFunction() {
	suite.Run("success", func() {
		coll := suite.createTestCollection()

		mockMeta := mockrootcoord.NewIMetaTable(suite.T())
		mockMeta.EXPECT().GetCollectionByName(mock.Anything, "test_db", "test_collection", typeutil.MaxTimestamp).Return(coll, nil)
		suite.core.meta = mockMeta

		req := &milvuspb.AlterCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionSchema: &schemapb.FunctionSchema{
				Name:             "test_function",
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{"text_field"},
				OutputFieldNames: []string{"vector"},
			},
		}
		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForAlterFunction(context.Background(), req)
		suite.NoError(err)
	})
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForDropFunction() {
	suite.Run("success", func() {
		coll := suite.createTestCollection()

		mockMeta := mockrootcoord.NewIMetaTable(suite.T())
		mockMeta.EXPECT().GetCollectionByName(mock.Anything, "test_db", "test_collection", typeutil.MaxTimestamp).Return(coll, nil)
		suite.core.meta = mockMeta

		req := &milvuspb.DropCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionName:   "test_function",
		}

		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForDropFunction(context.Background(), req)
		suite.NoError(err)
	})

	suite.Run("function not exists", func() {
		req := &milvuspb.DropCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionName:   "non_existent_function",
		}

		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForDropFunction(context.Background(), req)
		suite.NoError(err)
	})
}

func (suite *DDLCallbacksCollectionFunctionTestSuite) TestBroadcastAlterCollectionForAddFunction() {
	suite.Run("function name already exists", func() {
		coll := suite.createTestCollection()

		mockMeta := mockrootcoord.NewIMetaTable(suite.T())
		mockMeta.EXPECT().GetCollectionByName(mock.Anything, "test_db", "test_collection", typeutil.MaxTimestamp).Return(coll, nil)
		suite.core.meta = mockMeta

		req := &milvuspb.AddCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionSchema: &schemapb.FunctionSchema{
				Name:             "test_function",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldNames:  []string{"text_field"},
				OutputFieldNames: []string{"vector"},
			},
		}

		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForAddFunction(context.Background(), req)
		suite.Error(err)
	})

	suite.Run("function input field not exists", func() {
		coll := suite.createTestCollection()
		mockMeta := mockrootcoord.NewIMetaTable(suite.T())
		mockMeta.EXPECT().GetCollectionByName(mock.Anything, "test_db", "test_collection", typeutil.MaxTimestamp).Return(coll, nil)
		suite.core.meta = mockMeta

		req := &milvuspb.AddCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionSchema: &schemapb.FunctionSchema{
				Name:             "new_function",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldNames:  []string{"non_existent_field"},
				OutputFieldNames: []string{"vector"},
			},
		}

		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForAddFunction(context.Background(), req)
		suite.Error(err)
		suite.Contains(err.Error(), "function's input field non_existent_field not exists")
	})

	suite.Run("function output field not exists", func() {
		coll := suite.createTestCollection()

		mockMeta := mockrootcoord.NewIMetaTable(suite.T())
		mockMeta.EXPECT().GetCollectionByName(mock.Anything, "test_db", "test_collection", typeutil.MaxTimestamp).Return(coll, nil)
		suite.core.meta = mockMeta

		req := &milvuspb.AddCollectionFunctionRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			FunctionSchema: &schemapb.FunctionSchema{
				Name:             "new_function2",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldNames:  []string{"text_field"},
				OutputFieldNames: []string{"non_existent_field"},
			},
		}

		mocker := mockey.Mock(callAlterCollection).Return(nil).Build()
		defer mocker.UnPatch()

		err := suite.core.broadcastAlterCollectionForAddFunction(context.Background(), req)
		suite.Error(err)
		suite.Contains(err.Error(), "function's output field non_existent_field not exists")
	})
}
