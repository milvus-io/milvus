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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksAlterCollectionField(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	fieldName := "field1"

	// database not found
	resp, err := core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties:     []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	// collection not found
	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: util.DefaultDBName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         util.DefaultDBName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties:     []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCollectionNotFound)

	// atler collection field field not found
	createCollectionForTest(t, ctx, core, dbName, collectionName)
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName + "2",
		Properties:     []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties:     []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, "field1", "key1", "value1")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, "key2", "value2")
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, "key1", "value1")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	// delete key and add new key
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: "key1", Value: "value1"},
			{Key: "key3", Value: "value3"},
		},
		DeleteKeys: []string{"key2"},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, "key1", "value1")
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, "key3", "value3")
	assertFieldPropertiesNotFound(t, ctx, core, dbName, collectionName, fieldName, "key2")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)

	// idempotency check
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties:     []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
		DeleteKeys:     []string{"key2"},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, "key1", "value1")
	assertFieldPropertiesNotFound(t, ctx, core, dbName, collectionName, fieldName, "key2")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)
}

func TestDDLCallbacksAlterCollectionFieldAnalyzerValidation(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	fieldName := "text"
	intFieldName := "count"

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	testSchema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				Name:       fieldName,
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}},
			},
			{
				Name:     intFieldName,
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

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

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EnableAnalyzerKey, Value: "true"},
			{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"standard"}`},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{resourceID}, coll.FileResourceIds)
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, common.AnalyzerParamKey, `{"tokenizer":"standard"}`)

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EnableAnalyzerKey, Value: "not-bool"},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      intFieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EnableAnalyzerKey, Value: "true"},
			{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"standard"}`},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])

	mixCoord.EXPECT().ValidateAnalyzer(mock.Anything, mock.Anything).Return(&querypb.ValidateAnalyzerResponse{
		Status: merr.Status(merr.WrapErrParameterInvalidMsg("bad analyzer")),
	}, nil).Once()
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"bad"}`},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp, err))
	assertFieldProperties(t, ctx, core, dbName, collectionName, fieldName, common.AnalyzerParamKey, `{"tokenizer":"standard"}`)
	require.Equal(t, 1, meta.fileResourceRefCnt[resourceID])

	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      fieldName,
		DeleteKeys:     []string{common.AnalyzerParamKey},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Empty(t, coll.FileResourceIds)
	require.Equal(t, 0, meta.fileResourceRefCnt[resourceID])
	assertFieldPropertiesNotFound(t, ctx, core, dbName, collectionName, fieldName, common.AnalyzerParamKey)
}

func assertFieldPropertiesNotFound(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string, key string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			for _, property := range field.TypeParams {
				if property.Key == key {
					require.Fail(t, "property found", "property %s found in field %s", key, fieldName)
				}
			}
		}
	}
}

func assertFieldProperties(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string, key string, val string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			for _, property := range field.TypeParams {
				if property.Key == key {
					require.Equal(t, val, property.Value)
					return
				}
			}
		}
	}
}
