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

package dql

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const testVecDim = 128

const (
	testInt64Field     = "int64"
	testVarCharField   = "varChar"
	testFloatVecField  = "fvec"
	int64Field         = "int64"
	floatVecField      = "fVec"
	testBoolField      = "bool"
	testInt32Field     = "int32"
	testFloatField     = "float"
	testDoubleField    = "double"
	testBinaryVecField = "bvec"
)

// newTextSchemaForStorageV3Test builds a schema with a TEXT field for
// StorageV3-gating tests.
func newTextSchemaForStorageV3Test(collectionName string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: testInt64Field, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{
				FieldID:  102,
				Name:     testFloatVecField,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: strconv.Itoa(testVecDim)},
				},
			},
		},
	}
}

// newScalarFieldData builds scalar field data for tests.
func newScalarFieldData(fieldSchema *schemapb.FieldSchema, fieldName string, numRows int) *schemapb.FieldData {
	return testutils.GenerateScalarFieldData(fieldSchema.GetDataType(), fieldName, numRows)
}

// newFloatVectorFieldData builds float-vector field data for tests.
func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloatVectorFieldData(fieldName, numRows, dim)
}

// generateFieldData builds field data for a data type.
func generateFieldData(dataType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	if dataType < 100 {
		return testutils.GenerateScalarFieldData(dataType, fieldName, numRows)
	}
	return testutils.GenerateVectorFieldData(dataType, fieldName, numRows, testVecDim)
}

// newTestCache returns an empty metacache whose methods are expected to be
// patched by mockey in each test.
func newTestCache() *metacache.MetaCache {
	cache, err := metacache.NewMetaCache(nil)
	if err != nil {
		panic(err)
	}
	return cache
}

// mockTsoAllocator is a simple taskmodel.TsoAllocator for tests.
type mockTsoAllocator struct {
	mu        sync.Mutex
	logicPart uint32
}

func (tso *mockTsoAllocator) AllocOne(ctx context.Context) (Timestamp, error) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.logicPart++
	physical := uint64(time.Now().UnixMilli())
	return (physical << 18) + uint64(tso.logicPart), nil
}

var _ taskmodel.TsoAllocator = (*mockTsoAllocator)(nil)

const testMaxVarCharLength = 512

// constructCollectionSchemaByDataType builds a collection schema for tests.
func constructCollectionSchemaByDataType(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, autoID bool) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)

	idx := int64(100)
	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			FieldID:  idx,
			Name:     fieldName,
			DataType: dataType,
		}
		idx++
		if typeutil.IsVectorType(dataType) {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

// createTestSchema builds a test schemaInfo for query/search tests.
func createTestSchema() *schemaInfo {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
			{
				FieldID:  102,
				Name:     "name",
				DataType: schemapb.DataType_VarChar,
			},
		},
	}
	return mustNewSchemaInfo(schema)
}

// newTestSchema builds a schema covering every scalar/vector data type.
func newTestSchema() *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		if !typeutil.IsIntegerType(dataType) && !typeutil.IsFloatingType(dataType) && !typeutil.IsVectorType(dataType) && !typeutil.IsStringType(dataType) {
			continue
		}
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		fields = append(fields, newField)
	}

	return &schemapb.CollectionSchema{
		Name:               "test",
		Description:        "schema for test used",
		AutoID:             true,
		Fields:             fields,
		EnableDynamicField: true,
	}
}

// constructCollectionSchema builds a minimal int64-pk + float-vector schema.
func constructCollectionSchema(int64Field, floatVecField string, dim int, collectionName string) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "15",
			},
		},
	}
}

// constructPlaceholderGroup builds a float-vector placeholder group for tests.
func constructPlaceholderGroup(nq, dim int) *commonpb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	for i := 0; i < nq; i++ {
		bs := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			var buffer bytes.Buffer
			f := rand.Float32()
			err := binary.Write(&buffer, common.Endian, f)
			if err != nil {
				panic(err)
			}
			bs = append(bs, buffer.Bytes()...)
		}
		values = append(values, bs)
	}

	return &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
}

// ConstructCollectionSchemaWithPartitionKey builds a schema with a partition key.
func ConstructCollectionSchemaWithPartitionKey(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, partitionKeyFieldName string, autoID bool) *schemapb.CollectionSchema {
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2DataType, primaryFieldName, autoID)
	for _, field := range schema.Fields {
		if field.Name == partitionKeyFieldName {
			field.IsPartitionKey = true
		}
	}

	return schema
}

// fillFieldPropertiesOnly fills FieldId/Type from the schema for each column.
func fillFieldPropertiesOnly(columns []*schemapb.FieldData, schema *schemaInfo) error {
	for _, fieldData := range columns {
		fieldSchema, err := schema.SchemaHelper.GetFieldFromNameDefaultJSON(fieldData.FieldName)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("fieldName %v not exist in collection schema", fieldData.FieldName)
		}

		fieldData.FieldId = fieldSchema.FieldID
		fieldData.Type = fieldSchema.DataType

		switch fieldData.Type {
		case schemapb.DataType_Array:
			fd, ok := fieldData.Field.(*schemapb.FieldData_Scalars)
			if !ok || fd.Scalars.GetArrayData() == nil {
				return merr.WrapErrParameterInvalidMsg("field convert FieldData_Scalars fail in fieldData, fieldName: %s, collectionName: %s",
					fieldData.FieldName, schema.Name)
			}
			fd.Scalars.GetArrayData().ElementType = fieldSchema.ElementType
		case schemapb.DataType_ArrayOfVector:
			fd, ok := fieldData.Field.(*schemapb.FieldData_Vectors)
			if !ok || fd.Vectors.GetVectorArray() == nil {
				return merr.WrapErrParameterInvalidMsg("field convert FieldData_Vectors fail in fieldData, fieldName: %s, collectionName: %s",
					fieldData.FieldName, schema.Name)
			}
			fd.Vectors.GetVectorArray().ElementType = fieldSchema.ElementType
		}
	}

	return nil
}

// mockTest registers a mockey patch that is automatically unpatched when the
// test finishes, so global mockey patches never leak between tests.
func mockTest(t *testing.T, target any, rets ...any) *mockey.Mocker {
	m := mockey.Mock(target).Return(rets...).Build()
	t.Cleanup(func() { m.UnPatch() })
	return m
}

// mockTestTo registers a mockey patch with a custom implementation that is
// automatically unpatched when the test finishes.
func mockTestTo(t *testing.T, target any, fn any) *mockey.Mocker {
	m := mockey.Mock(target).To(fn).Build()
	t.Cleanup(func() { m.UnPatch() })
	return m
}
