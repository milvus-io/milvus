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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksAlterCollectionAddStructField(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	resp, err := core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		DbName:                 dbName,
		CollectionName:         collectionName,
		StructArrayFieldSchema: newRootAddStructFieldSchema("field1"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		DbName:                 dbName,
		CollectionName:         collectionName,
		StructArrayFieldSchema: newRootAddStructFieldSchema("profile"),
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Len(t, getUserFields(coll.Fields), 1)
	require.Len(t, coll.StructArrayFields, 1)

	structField := coll.StructArrayFields[0]
	require.Equal(t, int64(101), structField.FieldID)
	require.Equal(t, "profile", structField.Name)
	require.True(t, structField.Nullable)
	require.Len(t, structField.Fields, 2)

	require.Equal(t, int64(102), structField.Fields[0].FieldID)
	require.Equal(t, "profile[ints]", structField.Fields[0].Name)
	require.Equal(t, schemapb.DataType_Array, structField.Fields[0].DataType)
	require.True(t, structField.Fields[0].Nullable)

	require.Equal(t, int64(103), structField.Fields[1].FieldID)
	require.Equal(t, "profile[vectors]", structField.Fields[1].Name)
	require.Equal(t, schemapb.DataType_ArrayOfVector, structField.Fields[1].DataType)
	require.True(t, structField.Fields[1].Nullable)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 103)

	resp, err = core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		DbName:                 dbName,
		CollectionName:         collectionName,
		StructArrayFieldSchema: newRootAddStructFieldSchema("profile"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("profile"),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
}

func getUserFields(fields []*model.Field) []*model.Field {
	ret := make([]*model.Field, 0, len(fields))
	for _, field := range fields {
		if field.Name == RowIDFieldName || field.Name == TimeStampFieldName {
			continue
		}
		ret = append(ret, field)
	}
	return ret
}

func newRootAddStructFieldSchema(name string) *schemapb.StructArrayFieldSchema {
	return &schemapb.StructArrayFieldSchema{
		Name:     name,
		Nullable: true,
		Fields: []*schemapb.FieldSchema{
			{
				Name:        typeutil.ConcatStructFieldName(name, "ints"),
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxCapacityKey, Value: "16"},
				},
			},
			{
				Name:        typeutil.ConcatStructFieldName(name, "vectors"),
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "8"},
					{Key: common.MaxCapacityKey, Value: "16"},
				},
			},
		},
	}
}
