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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAlignMilvusTableAddedStructFields(t *testing.T) {
	for _, addChild := range []bool{false, true} {
		t.Run(fmt.Sprintf("add_child=%t", addChild), func(t *testing.T) {
			old := &schemapb.CollectionSchema{
				Name: "target", ExternalSource: "s3://bucket/snapshot",
				ExternalSpec: `{"format":"milvus-table","extfs":{"cloud_provider":"aws","region":"us-east-1","anonymous":"true"}}`,
				Fields:       []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", ExternalField: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
			}
			parent := &schemapb.StructArrayFieldSchema{
				FieldID: 110, Name: "clips", Nullable: true,
				Fields: []*schemapb.FieldSchema{{FieldID: 111, Name: "clips[base]", ExternalField: "clips[base]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true}},
			}
			if addChild {
				old.StructArrayFields = []*schemapb.StructArrayFieldSchema{proto.Clone(parent).(*schemapb.StructArrayFieldSchema)}
			}
			candidate := proto.Clone(old).(*schemapb.CollectionSchema)
			if addChild {
				candidate.StructArrayFields[0].Fields = append(candidate.StructArrayFields[0].Fields,
					&schemapb.FieldSchema{FieldID: 112, Name: "clips[note]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true})
			} else {
				parent.FieldID, parent.Fields[0].FieldID, parent.Fields[0].ExternalField = 101, 102, ""
				candidate.StructArrayFields = []*schemapb.StructArrayFieldSchema{parent}
			}
			source := proto.Clone(candidate).(*schemapb.CollectionSchema)
			source.ExternalSource, source.ExternalSpec, source.Fields[0].ExternalField = "", "", ""
			for _, child := range source.StructArrayFields[0].Fields {
				child.ExternalField = ""
			}
			if !addChild {
				source.StructArrayFields[0].FieldID = 120
			}
			added := source.StructArrayFields[0].Fields[len(source.StructArrayFields[0].Fields)-1]
			added.FieldID = 121
			read := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).Return(
				&datapb.SnapshotMetadata{Collection: &datapb.CollectionDescription{Schema: source}}, nil).Build()
			defer read.UnPatch()
			coll := model.UnmarshalCollectionModel(&etcdpb.CollectionInfo{Schema: old})
			before := proto.Clone(old)
			require.NoError(t, alignMilvusTableAddedStructFields(context.Background(), coll, candidate))
			bound := candidate.StructArrayFields[0].Fields[len(candidate.StructArrayFields[0].Fields)-1]
			require.Equal(t, int64(121), bound.FieldID)
			require.Equal(t, bound.Name, bound.ExternalField)
			require.Equal(t, source.StructArrayFields[0].FieldID, candidate.StructArrayFields[0].FieldID)
			require.Equal(t, int64(100), candidate.Fields[0].FieldID)
			if addChild {
				require.Equal(t, int64(111), candidate.StructArrayFields[0].Fields[0].FieldID)
			}
			require.True(t, proto.Equal(before, old))

			// Alignment cannot silently remap an already published parent or leaf.
			wrongID := proto.Clone(candidate).(*schemapb.CollectionSchema)
			wrongID.Fields[0].FieldID++
			require.ErrorIs(t, alignMilvusTableAddedStructFields(context.Background(), coll, wrongID), merr.ErrParameterInvalid)
			wrongType := proto.Clone(candidate).(*schemapb.CollectionSchema)
			wrongType.StructArrayFields[0].Fields[len(wrongType.StructArrayFields[0].Fields)-1].ElementType = schemapb.DataType_Double
			require.ErrorIs(t, alignMilvusTableAddedStructFields(context.Background(), coll, wrongType), merr.ErrParameterInvalid)
			missingChild := proto.Clone(candidate).(*schemapb.CollectionSchema)
			missingChild.StructArrayFields[0].Fields[len(missingChild.StructArrayFields[0].Fields)-1].Name = "clips[missing]"
			require.ErrorContains(t, alignMilvusTableAddedStructFields(context.Background(), coll, missingChild), "absent from the selected source snapshot")
			missingParent := proto.Clone(candidate).(*schemapb.CollectionSchema)
			missingParent.StructArrayFields[0].Name = "missing"
			require.ErrorContains(t, alignMilvusTableAddedStructFields(context.Background(), coll, missingParent), "absent from the selected source snapshot")
		})
	}
}

func TestValidateAddedStructFieldNameErrorClassification(t *testing.T) {
	for _, name := range []string{"", strings.Repeat("a", Params.ProxyCfg.MaxNameLength.GetAsInt()+1), "1field", "bad name", "AND"} {
		t.Run(name, func(t *testing.T) {
			err := validateAddedStructFieldName(name)
			require.ErrorIs(t, err, merr.ErrFieldInvalidName)
			require.Equal(t, merr.InputError, merr.GetErrorType(err))
			status := merr.Status(err)
			require.Equal(t, int32(1701), status.GetCode())
			require.Equal(t, err.Error(), status.GetReason())
			require.Equal(t, "true", status.GetExtraInfo()[merr.InputErrorFlagKey])
			require.False(t, status.GetRetriable())
			roundTrip := merr.Error(status)
			require.ErrorIs(t, roundTrip, merr.ErrFieldInvalidName)
			require.Equal(t, merr.InputError, merr.GetErrorType(roundTrip))
		})
	}
}

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

func TestDDLCallbacksAlterCollectionAddStructFieldRejectsTextMatch(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	for _, key := range []string{common.EnableMatchKey, common.EnableAnalyzerKey} {
		structField := newRootAddStructFieldSchema("profile")
		structField.Fields[0].ElementType = schemapb.DataType_VarChar
		structField.Fields[0].TypeParams = append(structField.Fields[0].TypeParams,
			&commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "16"},
			&commonpb.KeyValuePair{Key: key, Value: "true"})
		resp, err := core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
			DbName: dbName, CollectionName: collectionName, StructArrayFieldSchema: structField,
		})
		err = merr.CheckRPCCall(resp, err)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, key+" is not supported on struct sub-field profile[ints]")
	}
}

func TestDDLCallbacksExternalCollectionRejectsAddStructField(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testExternal" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	schema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{{
			Name: "field1", DataType: schemapb.DataType_Int64, ExternalField: "field1",
		}},
		ExternalSource: "s3://bucket/path/",
		ExternalSpec:   `{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1","cloud_provider":"aws"}}`,
	}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: dbName, CollectionName: collectionName, Schema: schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	structField := newRootAddStructFieldSchema("profile")
	structField.Fields[0].ElementNullable = true
	resp, err = core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		DbName: dbName, CollectionName: collectionName, StructArrayFieldSchema: structField,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetReason(), "does not support adding struct fields")

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Empty(t, coll.StructArrayFields)
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

func TestAlignMilvusTableAddedStructFieldsFailures(t *testing.T) {
	storageErr := merr.WrapErrServiceUnavailableMsg("snapshot unavailable")
	for _, tc := range []struct {
		name     string
		metadata *datapb.SnapshotMetadata
		readErr  error
		want     error
	}{
		{name: "storage unavailable", readErr: storageErr, want: storageErr},
		{name: "missing collection", metadata: &datapb.SnapshotMetadata{}, want: merr.ErrDataIntegrity},
		{name: "missing schema", metadata: &datapb.SnapshotMetadata{Collection: &datapb.CollectionDescription{}}, want: merr.ErrDataIntegrity},
		{name: "external source", metadata: &datapb.SnapshotMetadata{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "id", ExternalField: "id"}}}}}, want: merr.ErrParameterInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{ExternalSource: "s3://bucket/snapshot", ExternalSpec: `{"format":"milvus-table","extfs":{"cloud_provider":"aws","region":"us-east-1","anonymous":"true"}}`}
			read := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).Return(tc.metadata, tc.readErr).Build()
			defer read.UnPatch()
			require.ErrorIs(t, alignMilvusTableAddedStructFields(context.Background(), &model.Collection{}, schema), tc.want)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, alignMilvusTableAddedStructFields(ctx, &model.Collection{}, schema), context.Canceled)
			require.Equal(t, 1, read.Times(), "cancellation must not issue source I/O")
			schema.ExternalSource = ""
			require.Error(t, alignMilvusTableAddedStructFields(context.Background(), &model.Collection{}, schema))
			require.Equal(t, 1, read.Times(), "invalid source must fail before I/O")
			schema.ExternalSpec = `{"format":"parquet"}`
			require.NoError(t, alignMilvusTableAddedStructFields(context.Background(), &model.Collection{}, schema))
			require.Equal(t, 1, read.Times(), "other external formats must not read Milvus snapshots")
		})
	}
}
