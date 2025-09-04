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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

type FunctionTaskSuite struct {
	suite.Suite
}

func TestFunctionTask(t *testing.T) {
	suite.Run(t, new(FunctionTaskSuite))
}

func (f *FunctionTaskSuite) TestAddCollectionFunctionPrepare() {
	{
		t := &addCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.AddCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionField,
				},
			},
		}
		err := t.Prepare(context.Background())
		f.Error(err)
	}

	{
		t := &addCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.AddCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionFunction,
				},
				FunctionSchema: &schemapb.FunctionSchema{},
			},
		}
		err := t.Prepare(context.Background())
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestAlterCollectionFunctionPrepare() {
	{
		t := &alterCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.AlterCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionFunction,
				},
			},
		}
		err := t.Prepare(context.Background())
		f.Error(err)
	}

	{
		t := &alterCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.AlterCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AlterCollectionFunction,
				},
				FunctionSchema: &schemapb.FunctionSchema{},
			},
		}
		err := t.Prepare(context.Background())
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestGenNewCollection() {
	ctx := context.Background()

	// Test case 1: Function name already exists
	{
		fSchema := &schemapb.FunctionSchema{
			Name: "existing_function",
		}
		collection := &model.Collection{
			Functions: []*model.Function{
				{
					Name: "existing_function",
					ID:   1001,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.Error(err)
		f.Contains(err.Error(), "function name already exists")
	}

	// Test case 2: Input field does not exist
	{
		fSchema := &schemapb.FunctionSchema{
			Name:            "test_function",
			InputFieldNames: []string{"non_existent_field"},
		}
		collection := &model.Collection{
			Functions: []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "existing_field",
					FieldID: 100,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.Error(err)
		f.Contains(err.Error(), "function's input field non_existent_field not exists")
	}

	// Test case 3: Output field does not exist
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			InputFieldNames:  []string{"input_field"},
			OutputFieldNames: []string{"non_existent_output"},
		}
		collection := &model.Collection{
			Functions: []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 100,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.Error(err)
		f.Contains(err.Error(), "function's output field non_existent_output not exists")
	}

	// Test case 4: Output field is already used by another function
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			InputFieldNames:  []string{"input_field"},
			OutputFieldNames: []string{"output_field"},
		}
		collection := &model.Collection{
			Functions: []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 100,
				},
				{
					Name:             "output_field",
					FieldID:          101,
					IsFunctionOutput: true,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.Error(err)
		f.Contains(err.Error(), "function's output field output_field is already of other functions")
	}

	// Test case 5: Successful function creation
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			InputFieldNames:  []string{"input_field1", "input_field2"},
			OutputFieldNames: []string{"output_field1", "output_field2"},
		}
		collection := &model.Collection{
			Functions: []*model.Function{
				{
					Name: "existing_function",
					ID:   1001,
				},
			},
			Fields: []*model.Field{
				{
					Name:    "input_field1",
					FieldID: 100,
				},
				{
					Name:    "input_field2",
					FieldID: 101,
				},
				{
					Name:             "output_field1",
					FieldID:          102,
					IsFunctionOutput: false,
				},
				{
					Name:             "output_field2",
					FieldID:          103,
					IsFunctionOutput: false,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.NoError(err)

		// Verify function ID is assigned correctly
		f.Equal(int64(1002), fSchema.Id)

		// Verify input field IDs are mapped correctly
		f.Equal([]int64{100, 101}, fSchema.InputFieldIds)

		// Verify output field IDs are mapped correctly
		f.Equal([]int64{102, 103}, fSchema.OutputFieldIds)

		// Verify output fields are marked as function outputs
		f.True(collection.Fields[2].IsFunctionOutput)
		f.True(collection.Fields[3].IsFunctionOutput)
	}

	// Test case 6: Function ID assignment with no existing functions
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "first_function",
			InputFieldNames:  []string{"input_field"},
			OutputFieldNames: []string{"output_field"},
		}
		collection := &model.Collection{
			Functions: []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 100,
				},
				{
					Name:             "output_field",
					FieldID:          101,
					IsFunctionOutput: false,
				},
			},
		}

		t := &addCollectionFunctionTask{}
		err := t.genNewCollection(ctx, fSchema, collection)
		f.NoError(err)

		// Verify function ID starts from StartOfUserFunctionID
		f.Equal(int64(StartOfUserFunctionID), fSchema.Id)
	}
}

func (f *FunctionTaskSuite) TestAddCollectionFunctionExecute() {
	ctx := context.Background()

	// Test case 1: Collection not found
	{
		fSchema := &schemapb.FunctionSchema{
			Name: "test_function",
		}
		t := &addCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AddCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionFunction,
				},
				CollectionName: "non_existent_collection",
				CollectionID:   999,
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock core with no collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "non_existent_collection", t.ts).Return(nil, fmt.Errorf("Collection not exists"))
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.Error(err)
	}

	// Test case 2: Successful execution
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			InputFieldNames:  []string{"input_field"},
			OutputFieldNames: []string{"output_field"},
		}
		t := &addCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AddCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionFunction,
				},
				CollectionName: "test_collection",
				CollectionID:   100,
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection with fields
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions:    []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 200,
				},
				{
					Name:             "output_field",
					FieldID:          201,
					IsFunctionOutput: false,
				},
			},
		}

		// Mock core with collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		mockMeta.EXPECT().ListAliasesByID(ctx, collection.CollectionID).Return([]string{})
		mockMeta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}
		packChan := make(chan *msgstream.ConsumeMsgPack, 10)
		ticker := newChanTimeTickSync(packChan)
		ticker.addDmlChannels("by-dev-rootcoord-dml_1")
		core := newTestCore(withValidProxyManager(), withMeta(mockMeta), withBroker(broker), withTtSynchronizer(ticker), withInvalidTsoAllocator())
		t.core = core
		err = t.Execute(ctx)
		f.NoError(err)
	}

	// Test case 3: genNewCollection fails
	{
		fSchema := &schemapb.FunctionSchema{
			Name:            "test_function",
			InputFieldNames: []string{"non_existent_field"},
		}
		t := &addCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AddCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AddCollectionFunction,
				},
				CollectionName: "test_collection",
				CollectionID:   100,
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection without the required field
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions:    []*model.Function{},
			Fields: []*model.Field{
				{
					Name:    "other_field",
					FieldID: 200,
				},
			},
		}

		// Mock core with collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.Error(err)
		f.Contains(err.Error(), "function's input field non_existent_field not exists")
	}
}

func (f *FunctionTaskSuite) TestAlterCollectionFunctionExecute() {
	ctx := context.Background()

	// Test case 1: Collection not found
	{
		fSchema := &schemapb.FunctionSchema{
			Name: "test_function",
		}
		t := &alterCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AlterCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AlterCollectionFunction,
				},
				CollectionName: "non_existent_collection",
				CollectionID:   999,
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock core with no collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "non_existent_collection", t.ts).Return(nil, fmt.Errorf("Collection not exists"))
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.Error(err)
	}

	// Test case 2: Successful execution
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			InputFieldNames:  []string{"input_field"},
			OutputFieldNames: []string{"output_field"},
		}
		t := &alterCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AlterCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AlterCollectionFunction,
				},
				CollectionName: "test_collection",
				CollectionID:   100,
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection with fields
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					ID:               100,
					Name:             "test_function",
					InputFieldIDs:    []int64{200},
					InputFieldNames:  []string{"input_field"},
					OutputFieldIDs:   []int64{201},
					OutputFieldNames: []string{"output_field"},
				},
			},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 200,
				},
				{
					Name:             "output_field",
					FieldID:          201,
					IsFunctionOutput: true,
				},
			},
		}

		// Mock core with collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		mockMeta.EXPECT().ListAliasesByID(ctx, collection.CollectionID).Return([]string{})
		mockMeta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}
		packChan := make(chan *msgstream.ConsumeMsgPack, 10)
		ticker := newChanTimeTickSync(packChan)
		ticker.addDmlChannels("by-dev-rootcoord-dml_1")
		core := newTestCore(withValidProxyManager(), withMeta(mockMeta), withBroker(broker), withTtSynchronizer(ticker), withInvalidTsoAllocator())
		t.core = core
		err = t.Execute(ctx)
		f.NoError(err)
	}

	// Test case 3: genNewCollection fails
	{
		fSchema := &schemapb.FunctionSchema{
			Name:            "test_function",
			InputFieldNames: []string{"non_existent_field"},
		}
		t := &alterCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.AlterCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_AlterCollectionFunction,
				},
				CollectionName: "test_collection",
				CollectionID:   100,
				FunctionName:   "test_function",
				FunctionSchema: fSchema,
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection without the required field
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					ID:               100,
					Name:             "test_function",
					InputFieldIDs:    []int64{200},
					InputFieldNames:  []string{"input_field"},
					OutputFieldIDs:   []int64{201},
					OutputFieldNames: []string{"output_field"},
				},
			},
			Fields: []*model.Field{
				{
					Name:    "input_field",
					FieldID: 200,
				},
				{
					Name:             "output_field",
					FieldID:          201,
					IsFunctionOutput: true,
				},
			},
		}

		// Mock core with collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.Error(err)
		f.Contains(err.Error(), "function's input field non_existent_field not exists")
	}
}

func (f *FunctionTaskSuite) TestDropCollectionFunctionPrepare() {
	{
		t := &dropCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.DropCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := t.Prepare(context.Background())
		f.Error(err)
	}
	{
		t := &dropCollectionFunctionTask{
			baseTask: baseTask{},
			Req: &milvuspb.DropCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollectionFunction,
				},
			},
		}
		err := t.Prepare(context.Background())
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestDropCollectionFunctionTask() {
	ctx := context.Background()

	// Test case 1: successful drop
	{
		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollectionFunction,
				},
				CollectionName: "test_collection",
				FunctionName:   "test_function",
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection with function
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					Name: "test_function",
					ID:   1,
				},
			},
		}

		// Mock core with collection and successful operations
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		mockMeta.EXPECT().ListAliasesByID(ctx, collection.CollectionID).Return([]string{})
		mockMeta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}
		packChan := make(chan *msgstream.ConsumeMsgPack, 10)
		ticker := newChanTimeTickSync(packChan)
		ticker.addDmlChannels("by-dev-rootcoord-dml_1")
		core := newTestCore(withValidProxyManager(), withMeta(mockMeta), withBroker(broker), withTtSynchronizer(ticker), withInvalidTsoAllocator())
		t.core = core

		err = t.Execute(ctx)
		f.NoError(err)
	}

	// Test case 2: collection not found
	{
		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollectionFunction,
				},
				CollectionName: "non_existent_collection",
				FunctionName:   "test_function",
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock core with collection not found error
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "non_existent_collection", t.ts).Return(nil, fmt.Errorf("collection not found"))
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.Error(err)
		f.Contains(err.Error(), "collection not found")
	}

	// Test case 3: function not found in collection
	{
		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollectionFunction,
				},
				CollectionName: "test_collection",
				FunctionName:   "non_existent_function",
			},
		}
		err := t.Prepare(ctx)
		f.NoError(err)

		// Mock collection without the function
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					Name: "other_function",
					ID:   1,
				},
			},
		}

		// Mock core with collection
		mockMeta := mockrootcoord.NewIMetaTable(f.T())
		mockMeta.EXPECT().GetCollectionByName(ctx, "", "test_collection", t.ts).Return(collection, nil)
		core := &Core{
			meta: mockMeta,
		}
		t.core = core

		err = t.Execute(ctx)
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestDropCollectionFunctionGenNewCollection() {
	ctx := context.Background()

	// Test case 1: Successful function removal
	{
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					Name:           "function1",
					ID:             1,
					OutputFieldIDs: []int64{201, 202},
				},
				{
					Name:           "function2",
					ID:             2,
					OutputFieldIDs: []int64{203},
				},
			},
			Fields: []*model.Field{
				{
					Name:             "input_field",
					FieldID:          200,
					IsFunctionOutput: false,
				},
				{
					Name:             "output_field1",
					FieldID:          201,
					IsFunctionOutput: true,
				},
				{
					Name:             "output_field2",
					FieldID:          202,
					IsFunctionOutput: true,
				},
				{
					Name:             "output_field3",
					FieldID:          203,
					IsFunctionOutput: true,
				},
			},
		}

		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				FunctionName: "function1",
			},
		}

		err := t.genNewCollection(ctx, collection.Functions[0], collection)
		f.NoError(err)

		// Verify function is removed
		f.Len(collection.Functions, 1)
		f.Equal("function2", collection.Functions[0].Name)

		// Verify output fields are reset to non-function-output
		fieldMap := make(map[int64]*model.Field)
		for _, field := range collection.Fields {
			fieldMap[field.FieldID] = field
		}
		f.False(fieldMap[201].IsFunctionOutput) // output_field1 should be reset
		f.False(fieldMap[202].IsFunctionOutput) // output_field2 should be reset
		f.True(fieldMap[203].IsFunctionOutput)  // output_field3 should remain true (used by function2)
		f.False(fieldMap[200].IsFunctionOutput) // input_field should remain false
	}

	// Test case 2: Remove function with no output fields
	{
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					Name:           "function_no_output",
					ID:             1,
					OutputFieldIDs: []int64{},
				},
			},
			Fields: []*model.Field{
				{
					Name:             "input_field",
					FieldID:          200,
					IsFunctionOutput: false,
				},
			},
		}

		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				FunctionName: "function_no_output",
			},
		}

		err := t.genNewCollection(ctx, collection.Functions[0], collection)
		f.NoError(err)

		// Verify function is removed
		f.Len(collection.Functions, 0)

		// Verify fields remain unchanged
		f.False(collection.Fields[0].IsFunctionOutput)
	}
	// Test case 3: Remove function with error output fields
	{
		collection := &model.Collection{
			CollectionID: 100,
			Name:         "test_collection",
			Functions: []*model.Function{
				{
					Name:           "function_no_output",
					ID:             1,
					OutputFieldIDs: []int64{201},
				},
			},
			Fields: []*model.Field{
				{
					Name:             "input_field",
					FieldID:          200,
					IsFunctionOutput: false,
				},
			},
		}

		t := &dropCollectionFunctionTask{
			baseTask: baseTask{
				ts: 100,
			},
			Req: &milvuspb.DropCollectionFunctionRequest{
				FunctionName: "function_no_output",
			},
		}

		err := t.genNewCollection(ctx, collection.Functions[0], collection)
		f.Error(err)
	}
}
