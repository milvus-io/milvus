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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksAlterCollectionAddField(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	// database not found
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	// collection not found
	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: util.DefaultDBName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         util.DefaultDBName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCollectionNotFound)

	// atler collection field already exists
	createCollectionForTest(t, ctx, core, dbName, collectionName)
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field1"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// add illegal field schema
	illegalFieldSchema := &schemapb.FieldSchema{
		Name:         "field2",
		DataType:     schemapb.DataType_String,
		IsPrimaryKey: true,
		Nullable:     true,
	}
	illegalFieldSchemaBytes, _ := proto.Marshal(illegalFieldSchema)
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         illegalFieldSchemaBytes,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// add new field successfully
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, "field1", "key1", "value1")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field2", 101)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	// add new field successfully
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field3"),
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, "field1", "key1", "value1")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field3", 102)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)
}

func TestDDLCallbacksAlterCollectionAddFieldAnalyzerFileResourceRefs(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	fieldName := "text_with_dict"

	createCollectionForTest(t, ctx, core, dbName, collectionName)

	meta := core.meta.(*MetaTable)
	resourceID := int64(10001)
	meta.fileResourceName2Meta = map[string]*internalpb.FileResourceInfo{
		"dict": {Id: resourceID, Name: "dict", Path: "dict.txt"},
	}
	meta.fileResourceID2Meta = map[int64]*internalpb.FileResourceInfo{
		resourceID: {Id: resourceID, Name: "dict", Path: "dict.txt"},
	}
	meta.fileResourceRefCnt = map[int64]int{}

	mixCoord := core.mixCoord.(*mocks.MixCoord)
	mixCoord.EXPECT().ValidateAnalyzer(mock.Anything, mock.MatchedBy(func(req *querypb.ValidateAnalyzerRequest) bool {
		infos := req.GetAnalyzerInfos()
		return len(infos) == 1 &&
			infos[0].GetField() == fieldName &&
			infos[0].GetParams() == `{"tokenizer":"standard"}`
	})).Return(&querypb.ValidateAnalyzerResponse{
		Status:      merr.Success(),
		ResourceIds: []int64{resourceID},
	}, nil).Once()

	fieldSchema := &schemapb.FieldSchema{
		Name:     fieldName,
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "128"},
			{Key: common.EnableAnalyzerKey, Value: "true"},
			{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"standard"}`},
		},
	}
	schemaBytes, err := proto.Marshal(fieldSchema)
	require.NoError(t, err)

	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{resourceID}, coll.FileResourceIds)
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])
	assertFieldExists(t, ctx, core, dbName, collectionName, fieldName, 101)
}

func TestDDLCallbacksAlterCollectionAddTextField(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	})

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getTextFieldSchema("text_storage_v3_disabled", true, nil),
	})
	addErr := merr.CheckRPCCall(resp, err)
	require.ErrorIs(t, addErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, addErr, "TEXT field requires StorageV3")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)

	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getTextFieldSchema("text_field", true, nil),
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertTextFieldExists(t, ctx, core, dbName, collectionName, "text_field", 101, true, false)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	defaultValue := &schemapb.ValueField{
		Data: &schemapb.ValueField_StringData{StringData: "default text"},
	}
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getTextFieldSchema("text_default", true, defaultValue),
	})
	addErr = merr.CheckRPCCall(resp, err)
	require.ErrorIs(t, addErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, addErr, "default value is not supported when adding TEXT field")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)
}

func getFieldSchema(fieldName string) []byte {
	fieldSchema := &schemapb.FieldSchema{
		Name:     fieldName,
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	}
	schemaBytes, _ := proto.Marshal(fieldSchema)
	return schemaBytes
}

func getTextFieldSchema(fieldName string, nullable bool, defaultValue *schemapb.ValueField) []byte {
	fieldSchema := &schemapb.FieldSchema{
		Name:         fieldName,
		DataType:     schemapb.DataType_Text,
		Nullable:     nullable,
		DefaultValue: defaultValue,
	}
	schemaBytes, _ := proto.Marshal(fieldSchema)
	return schemaBytes
}

func assertFieldExists(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string, fieldID int64) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			require.Equal(t, field.FieldID, fieldID)
			return
		}
	}
	require.Fail(t, "field not found")
}

func assertTextFieldExists(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string, fieldID int64, nullable bool, hasDefault bool) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			require.Equal(t, fieldID, field.FieldID)
			require.Equal(t, schemapb.DataType_Text, field.DataType)
			require.Equal(t, nullable, field.Nullable)
			require.Equal(t, hasDefault, field.DefaultValue != nil)
			return
		}
	}
	require.Fail(t, "field not found")
}

// A shard split rewrites the collection's data with a writer that does not
// carry TEXT fields, so a TEXT field may not be added while one is in flight.
func TestShardSplitInFlightRefusesATextField(t *testing.T) {
	shard := func(state schemapb.ShardState) *model.ShardInfo { return &model.ShardInfo{State: state} }
	newColl := func(states ...schemapb.ShardState) *model.Collection {
		coll := &model.Collection{CollectionID: 7, ShardInfos: map[string]*model.ShardInfo{}}
		for i, state := range states {
			coll.ShardInfos[fmt.Sprintf("v%d", i)] = shard(state)
		}
		return coll
	}
	text := &schemapb.FieldSchema{Name: "doc", DataType: schemapb.DataType_Text}
	scalar := &schemapb.FieldSchema{Name: "n", DataType: schemapb.DataType_Int64}

	assert.NoError(t, refuseTextFieldDuringShardSplit(newColl(), text), "never split")
	assert.NoError(t, refuseTextFieldDuringShardSplit(newColl(schemapb.ShardState_ShardNormal), text))
	for _, state := range []schemapb.ShardState{schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating} {
		err := refuseTextFieldDuringShardSplit(newColl(schemapb.ShardState_ShardNormal, state), text)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable, state.String())
		assert.True(t, merr.IsRetryableErr(err), "a split in flight is transient")
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.NoError(t, refuseTextFieldDuringShardSplit(newColl(state), scalar), "only TEXT is refused")
	}
	nilShard := newColl()
	nilShard.ShardInfos["v"] = nil
	assert.NoError(t, refuseTextFieldDuringShardSplit(nilShard, text))
}

func TestDDLCallbacksAlterCollectionAddTextFieldDuringShardSplit(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key) })
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	splitting := mockey.Mock(collectionHasShardSplitInFlight).Return(true).Build()
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getTextFieldSchema("text_field", true, nil),
	})
	splitting.UnPatch()
	addErr := merr.CheckRPCCall(resp, err)
	require.ErrorIs(t, addErr, merr.ErrServiceUnavailable)
	require.ErrorContains(t, addErr, "shard split")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
}
