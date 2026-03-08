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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

func assertFieldPropertiesNotFound(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string, key string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
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
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
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
