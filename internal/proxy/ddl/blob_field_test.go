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

package ddl

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func enableBlobType(t *testing.T) {
	key := paramtable.Get().ProxyCfg.EnableBlobType.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func newBlobField(maxLength string) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "blob",
		DataType: schemapb.DataType_Blob,
		Nullable: true,
	}
	if maxLength != "" {
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: maxLength}}
	}
	return field
}

func TestValidateFieldBlob(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test_collection"}

	t.Run("rejected while the gate is off", func(t *testing.T) {
		err := ValidateField(newBlobField("16"), schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.ErrorContains(t, err, paramtable.Get().ProxyCfg.EnableBlobType.Key)
	})

	t.Run("accepted with the gate on", func(t *testing.T) {
		enableBlobType(t)
		assert.NoError(t, ValidateField(newBlobField("16"), schema))

		notNullable := newBlobField("16")
		notNullable.Nullable = false
		assert.NoError(t, ValidateField(notNullable, schema))
	})

	t.Run("max_length rules", func(t *testing.T) {
		enableBlobType(t)
		maxBlobLength := paramtable.Get().ProxyCfg.MaxBlobLength.GetAsInt64()

		assert.ErrorIs(t, ValidateField(newBlobField(""), schema), merr.ErrParameterMissing)
		assert.ErrorIs(t, ValidateField(newBlobField("0"), schema), merr.ErrParameterInvalid)
		assert.ErrorIs(t, ValidateField(newBlobField("abc"), schema), merr.ErrParameterInvalid)
		assert.NoError(t, ValidateField(newBlobField(strconv.FormatInt(maxBlobLength, 10)), schema))
		assert.ErrorIs(t, ValidateField(newBlobField(strconv.FormatInt(maxBlobLength+1, 10)), schema), merr.ErrParameterInvalid)
	})

	t.Run("default value rejected", func(t *testing.T) {
		enableBlobType(t)
		field := newBlobField("16")
		field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_BytesData{BytesData: []byte{0x01}}}
		assert.ErrorIs(t, ValidateField(field, schema), merr.ErrParameterInvalid)
	})
}

func TestBlobFieldKeyRoles(t *testing.T) {
	enableBlobType(t)
	ctx := context.Background()

	newSchema := func(blob *schemapb.FieldSchema) *schemapb.CollectionSchema {
		blob.Nullable = false
		return &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: !blob.GetIsPrimaryKey()},
				blob,
			},
		}
	}
	newTask := func(schema *schemapb.CollectionSchema) *CreateCollectionTask {
		return &CreateCollectionTask{
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{CollectionName: schema.GetName()},
			schema:                  schema,
		}
	}

	t.Run("primary key", func(t *testing.T) {
		blob := newBlobField("16")
		blob.IsPrimaryKey = true
		assert.ErrorIs(t, validatePrimaryKey(newSchema(blob)), merr.ErrParameterInvalid)
	})

	t.Run("partition key", func(t *testing.T) {
		blob := newBlobField("16")
		blob.IsPartitionKey = true
		assert.ErrorIs(t, newTask(newSchema(blob)).validatePartitionKey(ctx), merr.ErrParameterInvalid)
	})

	t.Run("clustering key", func(t *testing.T) {
		blob := newBlobField("16")
		blob.IsClusteringKey = true
		assert.ErrorIs(t, newTask(newSchema(blob)).validateClusteringKey(ctx), merr.ErrCollectionIllegalSchema)
	})

	t.Run("array element", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{{
				FieldID:     101,
				Name:        "blob_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Blob,
			}},
		}
		assert.ErrorIs(t, validateFieldType(schema), merr.ErrParameterInvalid)
	})

	t.Run("struct sub-field", func(t *testing.T) {
		blob := newBlobField("16")
		blob.Nullable = false
		err := ValidateFieldsInStruct(blob, &schemapb.CollectionSchema{Name: "test_collection"})
		assert.ErrorContains(t, err, "can only be array")
	})
}

func TestBlobFieldIndexRejected(t *testing.T) {
	blob := newBlobField("16")

	t.Run("autoindex", func(t *testing.T) {
		task := &CreateIndexTask{
			fieldSchema: blob,
			req:         &milvuspb.CreateIndexRequest{},
		}
		assert.ErrorIs(t, task.parseIndexParams(context.Background()), merr.ErrParameterInvalid)
	})

	for _, indexType := range []indexparamcheck.IndexType{
		indexparamcheck.IndexINVERTED,
		indexparamcheck.IndexSTLSORT,
		indexparamcheck.IndexTRIE,
		indexparamcheck.IndexBitmap,
		indexparamcheck.IndexHybrid,
		indexparamcheck.IndexRTREE,
		indexparamcheck.IndexNGRAM,
		indexparamcheck.IndexFMINDEX,
	} {
		t.Run(indexType, func(t *testing.T) {
			checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(indexType)
			require.NoError(t, err)
			assert.Error(t, checker.CheckValidDataType(indexType, blob))
		})
	}
}

func TestAddCollectionFieldBlob(t *testing.T) {
	ctx := context.Background()
	oldSchema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	newTask := func(t *testing.T) *AddCollectionFieldTask {
		bytes, err := proto.Marshal(newBlobField("16"))
		require.NoError(t, err)
		return &AddCollectionFieldTask{
			AddCollectionFieldRequest: &milvuspb.AddCollectionFieldRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: oldSchema.GetName(),
				Schema:         bytes,
			},
			oldSchema: oldSchema,
		}
	}

	t.Run("rejected while the gate is off", func(t *testing.T) {
		assert.ErrorIs(t, newTask(t).PreExecute(ctx), merr.ErrParameterInvalid)
	})

	t.Run("accepted with the gate on", func(t *testing.T) {
		enableBlobType(t)
		assert.NoError(t, newTask(t).PreExecute(ctx))
	})
}
