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

package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestValidateCollectionName(t *testing.T) {
	assert.Nil(t, validateCollectionName("abc"))
	assert.Nil(t, validateCollectionName("_123abc"))
	assert.Nil(t, validateCollectionName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
		"abc ",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateCollectionName(name))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "name"))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "alias"))
	}
}

func TestValidateResourceGroupName(t *testing.T) {
	assert.Nil(t, ValidateResourceGroupName("abc"))
	assert.Nil(t, ValidateResourceGroupName("_123abc"))
	assert.Nil(t, ValidateResourceGroupName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, ValidateResourceGroupName(name))
	}
}

func TestValidateDatabaseName(t *testing.T) {
	assert.Nil(t, ValidateDatabaseName("dbname"))
	assert.Nil(t, ValidateDatabaseName("_123abc"))
	assert.Nil(t, ValidateDatabaseName("abc123_"))

	longName := make([]byte, 512)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.Error(t, ValidateDatabaseName(name))
	}
}

func TestValidatePartitionTag(t *testing.T) {
	assert.Nil(t, validatePartitionTag("abc", true))
	assert.Nil(t, validatePartitionTag("123abc", true))
	assert.Nil(t, validatePartitionTag("_123abc", true))
	assert.Nil(t, validatePartitionTag("abc123_", true))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validatePartitionTag(name, true))
	}

	assert.Nil(t, validatePartitionTag("ab cd", false))
	assert.Nil(t, validatePartitionTag("ab*", false))
}

func TestValidateFieldName(t *testing.T) {
	assert.Nil(t, validateFieldName("abc"))
	assert.Nil(t, validateFieldName("_123abc"))
	assert.Nil(t, validateFieldName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
		"True",
		"array_contains",
		"json_contains_any",
		"ARRAY_LENGTH",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateFieldName(name))
	}
}

func TestValidateDimension(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "1",
			},
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema = &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "2",
			},
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: Params.ProxyCfg.MaxDimension.GetValue(),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	// invalid dim
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "-1",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_BinaryVector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "8",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt()),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "9",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "262145",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_Int8Vector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "200",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "201",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
}

func TestValidateVectorFieldMetricType(t *testing.T) {
	field1 := &schemapb.FieldSchema{
		Name:         "",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	assert.Nil(t, validateVectorFieldMetricType(field1))
	field1.DataType = schemapb.DataType_FloatVector
	assert.NotNil(t, validateVectorFieldMetricType(field1))
	field1.IndexParams = []*commonpb.KeyValuePair{
		{
			Key:   "abcdefg",
			Value: "",
		},
	}
	assert.NotNil(t, validateVectorFieldMetricType(field1))
	field1.IndexParams = append(field1.IndexParams, &commonpb.KeyValuePair{
		Key:   common.MetricTypeKey,
		Value: "",
	})
	assert.Nil(t, validateVectorFieldMetricType(field1))
}

func TestValidateDuplicatedFieldName(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "abc"},
			{Name: "def"},
		},
	}
	assert.Nil(t, validateDuplicatedFieldName(schema))
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		Name: "abc",
	})
	assert.NotNil(t, validateDuplicatedFieldName(schema))
}

func TestValidateDuplicatedFieldNameWithStructArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "abc"},
			{Name: "def"},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "struct1",
				Fields: []*schemapb.FieldSchema{
					{Name: "abc2"},
					{Name: "def2"},
				},
			},
		},
	}
	assert.Nil(t, validateDuplicatedFieldName(schema))
	schema.StructArrayFields[0].Fields = append(schema.StructArrayFields[0].Fields, &schemapb.FieldSchema{
		Name: "abc",
	})
	assert.NotNil(t, validateDuplicatedFieldName(schema))
}

func TestValidatePrimaryKey(t *testing.T) {
	boolField := &schemapb.FieldSchema{
		Name:         "boolField",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_Bool,
	}

	int64Field := &schemapb.FieldSchema{
		Name:         "int64Field",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_Int64,
	}

	VarCharField := &schemapb.FieldSchema{
		Name:         "VarCharField",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "100",
			},
		},
	}

	// test collection without pk field
	assert.Error(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField},
	}))

	// test collection with int64 field ad pk
	int64Field.IsPrimaryKey = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, int64Field},
	}))

	// test collection with varChar field as pk
	VarCharField.IsPrimaryKey = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, VarCharField},
	}))

	// test collection with multi pk field
	assert.Error(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, int64Field, VarCharField},
	}))

	// test collection with varChar field as primary and autoID = true
	VarCharField.AutoID = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, VarCharField},
	}))
}

func TestValidateFieldType(t *testing.T) {
	type testCase struct {
		dt       schemapb.DataType
		et       schemapb.DataType
		validate bool
	}
	cases := []testCase{
		{
			dt:       schemapb.DataType_Bool,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int8,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int16,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int32,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int64,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Float,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Double,
			validate: true,
		},
		{
			dt:       schemapb.DataType_FloatVector,
			validate: true,
		},
		{
			dt:       schemapb.DataType_BinaryVector,
			validate: true,
		},
		{
			dt:       schemapb.DataType_None,
			validate: false,
		},
		{
			dt:       schemapb.DataType_VarChar,
			validate: true,
		},
		{
			dt:       schemapb.DataType_String,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Bool,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int8,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int16,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int32,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int64,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Float,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Double,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_VarChar,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_String,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_None,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_JSON,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Array,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_FloatVector,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_BinaryVector,
			validate: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.dt.String(), func(t *testing.T) {
			sch := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						DataType:    tc.dt,
						ElementType: tc.et,
					},
				},
			}
			err := validateFieldType(sch)
			if tc.validate {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateMultipleVectorFields(t *testing.T) {
	// case1, no vector field
	schema1 := &schemapb.CollectionSchema{}
	assert.NoError(t, validateMultipleVectorFields(schema1))

	// case2, only one vector field
	schema2 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "case2",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}
	assert.NoError(t, validateMultipleVectorFields(schema2))

	// case3, multiple vectors
	schema3 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "case3_f",
				DataType: schemapb.DataType_FloatVector,
			},
			{
				Name:     "case3_b",
				DataType: schemapb.DataType_BinaryVector,
			},
		},
	}
	if enableMultipleVectorFields {
		assert.NoError(t, validateMultipleVectorFields(schema3))
	} else {
		assert.Error(t, validateMultipleVectorFields(schema3))
	}
}

func TestFillFieldIDBySchema(t *testing.T) {
	t.Run("column count mismatch", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "TestFillFieldIDBySchema",
			},
		}
		// Validation should fail due to column count mismatch
		assert.Error(t, validateFieldDataColumns(columns, schema))
	})

	t.Run("successful validation and fill", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "TestFillFieldIDBySchema",
					DataType: schemapb.DataType_Int64,
					FieldID:  1,
				},
			},
		}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "TestFillFieldIDBySchema",
			},
		}
		// Validation should succeed
		assert.NoError(t, validateFieldDataColumns(columns, schema))
		// Fill properties should succeed
		assert.NoError(t, fillFieldPropertiesOnly(columns, schema))
		assert.Equal(t, "TestFillFieldIDBySchema", columns[0].FieldName)
		assert.Equal(t, schemapb.DataType_Int64, columns[0].Type)
		assert.Equal(t, int64(1), columns[0].FieldId)
	})

	t.Run("field not in schema", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "FieldA",
					DataType: schemapb.DataType_Int64,
					FieldID:  1,
				},
			},
		}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "FieldB",
			},
		}
		// Validation should fail because FieldB is not in schema
		err := validateFieldDataColumns(columns, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not exist in collection schema")
	})
}

func TestValidateUsername(t *testing.T) {
	// only spaces
	res := ValidateUsername(" ")
	assert.Error(t, res)
	// starts with non-alphabet
	res = ValidateUsername("1abc")
	assert.Error(t, res)
	// length gt 32
	res = ValidateUsername("aaaaaaaaaabbbbbbbbbbccccccccccddddd")
	assert.Error(t, res)
	// illegal character which not alphabet, _, ., ., or number
	res = ValidateUsername("a1^7*),")
	assert.Error(t, res)
	// normal username that only contains alphabet, _, ., -, and number
	res = ValidateUsername("a.17_good-")
	assert.Nil(t, res)
}

func TestValidatePassword(t *testing.T) {
	// only spaces
	res := ValidatePassword("")
	assert.NotNil(t, res)
	//
	res = ValidatePassword("1abc")
	assert.NotNil(t, res)
	//
	res = ValidatePassword("a1^7*).,")
	assert.Nil(t, res)
	//
	res = ValidatePassword("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmnnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrsssssssssstttttttttttuuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwwxxxxxxxxxxyyyyyyyyyzzzzzzzzzzz")
	assert.Error(t, res)
}

func TestReplaceID2Name(t *testing.T) {
	srcStr := "collection 432682805904801793 has not been loaded to memory or load failed"
	dstStr := "collection default_collection has not been loaded to memory or load failed"
	assert.Equal(t, dstStr, ReplaceID2Name(srcStr, int64(432682805904801793), "default_collection"))
}

func TestValidateName(t *testing.T) {
	nameType := "Test"
	validNames := []string{
		"abc",
		"_123abc",
	}
	for _, name := range validNames {
		assert.Nil(t, validateName(name, nameType))
		assert.Nil(t, ValidateRoleName(name))
		assert.Nil(t, ValidateObjectName(name))
		assert.Nil(t, ValidateObjectType(name))
		assert.Nil(t, ValidatePrivilege(name))
	}

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		" ",
		"123abc",
		"$abc",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateName(name, nameType))
		assert.NotNil(t, ValidateRoleName(name))
		assert.NotNil(t, ValidateObjectType(name))
		assert.NotNil(t, ValidatePrivilege(name))
	}
	assert.NotNil(t, ValidateObjectName(" "))
	assert.NotNil(t, ValidateObjectName(string(longName)))
	assert.Nil(t, ValidateObjectName("*"))
}

func TestValidateRoleName_HyphenToggle(t *testing.T) {
	pt := paramtable.Get()

	pt.ProxyCfg.RoleNameValidationAllowedChars.SwapTempValue("$-")
	assert.Nil(t, ValidateRoleName("Admin-1"))
	assert.Nil(t, ValidateRoleName("_a-bc$1"))
	assert.NotNil(t, ValidateRoleName("-bad"))
	assert.NotNil(t, ValidateRoleName("1leading"))
	assert.NotNil(t, ValidateRoleName(""))
	assert.NotNil(t, ValidateRoleName("*"))

	pt.ProxyCfg.RoleNameValidationAllowedChars.SwapTempValue("$")
	assert.Nil(t, ValidateRoleName("Admin_1"))
	assert.Nil(t, ValidateRoleName("Admin$1"))
	assert.NotNil(t, ValidateRoleName("Admin-1"))
}

func TestIsDefaultRole(t *testing.T) {
	assert.Equal(t, true, IsDefaultRole(util.RoleAdmin))
	assert.Equal(t, true, IsDefaultRole(util.RolePublic))
	assert.Equal(t, false, IsDefaultRole("manager"))
}

func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

func GetContextWithDB(ctx context.Context, originValue string, dbName string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	dbKey := strings.ToLower(util.HeaderDBName)
	contextMap := map[string]string{
		authKey: authValue,
		dbKey:   dbName,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

func TestGetCurUserFromContext(t *testing.T) {
	_, err := GetCurUserFromContext(context.Background())
	assert.Error(t, err)

	_, err = GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.Error(t, err)

	_, err = GetCurUserFromContext(GetContext(context.Background(), "123456"))
	assert.Error(t, err)

	root := "root"
	password := "123456"
	username, err := GetCurUserFromContext(GetContext(context.Background(), fmt.Sprintf("%s%s%s", root, util.CredentialSeperator, password)))
	assert.NoError(t, err)
	assert.Equal(t, "root", username)
}

func TestGetCurDBNameFromContext(t *testing.T) {
	dbName := GetCurDBNameFromContextOrDefault(context.Background())
	assert.Equal(t, util.DefaultDBName, dbName)

	dbName = GetCurDBNameFromContextOrDefault(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.Equal(t, util.DefaultDBName, dbName)

	dbNameKey := strings.ToLower(util.HeaderDBName)
	dbNameValue := "foodb"
	contextMap := map[string]string{
		dbNameKey: dbNameValue,
	}
	md := metadata.New(contextMap)

	dbName = GetCurDBNameFromContextOrDefault(metadata.NewIncomingContext(context.Background(), md))
	assert.Equal(t, dbNameValue, dbName)
}

func TestGetRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	privilege.ResetPrivilegeCacheForTest()
	_, err := GetRole("foo")
	assert.Error(t, err)

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status:    merr.Success(),
		UserRoles: []string{"root/role1", "root/admin", "root/role2", "foo/role1"},
	}, nil).Times(1)

	privilege.InitPrivilegeCache(ctx, mixcoord)

	roles, err := GetRole("root")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(roles))

	roles, err = GetRole("foo")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(roles))
}

func TestPasswordVerify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	username := "user-test00"
	password := "PasswordVerify"

	invokedCount := 0

	mockedRootCoord := NewMixCoordMock()
	mockedRootCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
		invokedCount++
		return nil, errors.New("get cred not found credential")
	}

	privilege.InitPrivilegeCache(ctx, mockedRootCoord)
	privilegeCache := privilege.GetPrivilegeCache()
	assert.False(t, passwordVerify(ctx, username, password, privilegeCache))
	assert.Equal(t, 1, invokedCount)

	// Sha256Password has not been filled into cache during establish connection firstly
	encryptedPwd, err := crypto.PasswordEncrypt(password)
	assert.NoError(t, err)
	privilegeCache.RemoveCredential(username)
	mockedRootCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
		invokedCount++
		return &rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: username,
			Password: encryptedPwd,
		}, nil
	}

	assert.True(t, passwordVerify(ctx, username, password, privilegeCache))

	ret, err := privilegeCache.GetCredentialInfo(ctx, username)
	assert.NoError(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, username, ret.Username)
	assert.NotNil(t, ret.Sha256Password)
	assert.Equal(t, 2, invokedCount)

	// Sha256Password already exists within cache
	assert.True(t, passwordVerify(ctx, username, password, privilegeCache))
	assert.Equal(t, 2, invokedCount)
}

func Test_isCollectionIsLoaded(t *testing.T) {
	ctx := context.Background()
	t.Run("normal", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        successStatus,
			CollectionIDs: []int64{collID, 10, 100},
		}, nil)
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("error", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        successStatus,
			CollectionIDs: []int64{collID},
		}, errors.New("error"))
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})

	t.Run("fail", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "fail reason",
			},
			CollectionIDs: []int64{collID},
		}, nil)
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})
}

func Test_isPartitionIsLoaded(t *testing.T) {
	ctx := context.Background()
	t.Run("normal", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status:       merr.Success(),
			PartitionIDs: []int64{partID},
		}, nil)
		loaded, err := isPartitionLoaded(ctx, mixc, collID, partID)
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("error", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixCoord := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixCoord.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status:       merr.Success(),
			PartitionIDs: []int64{partID},
		}, errors.New("error"))
		loaded, err := isPartitionLoaded(ctx, mixCoord, collID, partID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})

	t.Run("fail", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixCoord := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixCoord.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "fail reason",
			},
			PartitionIDs: []int64{partID},
		}, nil)
		loaded, err := isPartitionLoaded(ctx, mixCoord, collID, partID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})
}

func Test_InsertTaskcheckFieldsDataBySchema(t *testing.T) {
	paramtable.Init()
	log.Info("InsertTaskcheckFieldsDataBySchema", zap.Bool("enable", Params.ProxyCfg.SkipAutoIDCheck.GetAsBool()))
	var err error

	t.Run("schema is empty, though won't happen in system", func(t *testing.T) {
		// won't happen in system
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields:      []*schemapb.FieldSchema{},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					DbName:         "TestInsertTask_checkFieldsDataBySchema",
					CollectionName: "TestInsertTask_checkFieldsDataBySchema",
					PartitionName:  "TestInsertTask_checkFieldsDataBySchema",
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("miss field", func(t *testing.T) {
		// schema has field, msg has no field.
		// schema is not Nullable or has set default_value
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("miss field is nullable or set default_value", func(t *testing.T) {
		// schema has fields, msg has no field.
		// schema is Nullable or set default_value
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,

				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						Nullable: true,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
	})

	t.Run("schema has autoid pk", func(t *testing.T) {
		// schema has autoid pk
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("schema pk is not autoid, but not pass pk", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pass more data field", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "c",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("duplicate field datas", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("not pk field, but autoid == true", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("has more than one pk", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pk can not set default value", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, false)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})
	t.Run("normal when upsert", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, false)
		assert.NoError(t, err)

		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}
		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, false)
		assert.NoError(t, err)
	})

	t.Run("skip the auto id", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)

		paramtable.Get().Save(Params.ProxyCfg.SkipAutoIDCheck.Key, "true")
		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema.Fields, task.schema, task.insertMsg, true)
		assert.NoError(t, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
		paramtable.Get().Reset(Params.ProxyCfg.SkipAutoIDCheck.Key)
	})
}

func Test_InsertTaskCheckPrimaryFieldData(t *testing.T) {
	// schema is empty, though won't happen in system
	// num_rows(0) should be greater than 0
	case1 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "TestInsertTask_checkPrimaryFieldData",
				CollectionName: "TestInsertTask_checkPrimaryFieldData",
				PartitionName:  "TestInsertTask_checkPrimaryFieldData",
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}

	_, err := checkPrimaryFieldData(case1.schema.Fields, case1.schema, case1.insertMsg)
	assert.NotEqual(t, nil, err)

	// the num of passed fields is less than needed
	case2 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type: schemapb.DataType_Int64,
					},
				},
				Version: msgpb.InsertDataVersion_RowBased,
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(case2.schema.Fields, case2.schema, case2.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == false, no primary field schema
	// primary field is not found
	case3 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{},
					{},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(case3.schema.Fields, case3.schema, case3.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but primary field data exist
	// can not assign primary field data when auto id enabled int64Field
	case4 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					FieldID:  2,
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_Int64,
						FieldName: "int64Field",
					},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	case4.schema.Fields[0].IsPrimaryKey = true
	case4.schema.Fields[0].AutoID = true
	case4.insertMsg.FieldsData[0] = newScalarFieldData(case4.schema.Fields[0], case4.schema.Fields[0].Name, 10)
	_, err = checkPrimaryFieldData(case4.schema.Fields, case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but DataType don't match
	// the data type of the data not matches the schema
	case4.schema.Fields[0].IsPrimaryKey = false
	case4.schema.Fields[1].IsPrimaryKey = true
	case4.schema.Fields[1].AutoID = true
	_, err = checkPrimaryFieldData(case4.schema.Fields, case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)
}

func Test_UpsertTaskCheckPrimaryFieldData(t *testing.T) {
	// num_rows(0) should be greater than 0
	t.Run("schema is empty, though won't happen in system", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields:      []*schemapb.FieldSchema{},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					DbName:         "TestUpsertTask_checkPrimaryFieldData",
					CollectionName: "TestUpsertTask_checkPrimaryFieldData",
					PartitionName:  "TestUpsertTask_checkPrimaryFieldData",
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	t.Run("the num of passed fields is less than needed", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "int64Field",
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "int64Field",
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// autoID == false, no primary field schema
	t.Run("primary field is not found", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "int64Field",
						DataType: schemapb.DataType_Int64,
					},
					{
						Name:     "floatField",
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{},
						{},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// primary field data is nil, GetPrimaryFieldData fail
	t.Run("primary field data is nil", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       false,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{},
						{},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// only support DataType Int64 or VarChar as PrimaryField
	t.Run("primary field type wrong", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "floatVectorField",
						FieldID:      1,
						DataType:     schemapb.DataType_FloatVector,
						AutoID:       true,
						IsPrimaryKey: true,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_FloatVector,
							FieldName: "floatVectorField",
						},
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "floatField",
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	t.Run("upsert must assign pk", func(t *testing.T) {
		// autoid==true
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       true,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NoError(t, nil, err)

		// autoid==false
		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       false,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err = checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		assert.NoError(t, nil, err)
	})

	t.Run("will generate new pk when autoid == true", func(t *testing.T) {
		// autoid==true
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       true,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{2},
										},
									},
								},
							},
						},
					},
					RowIDs: []int64{1},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema.Fields, task.schema, task.insertMsg)
		newPK := task.insertMsg.FieldsData[0].GetScalars().GetLongData().GetData()
		assert.Equal(t, newPK, task.insertMsg.RowIDs)
		assert.NoError(t, nil, err)
	})
}

func Test_ParseGuaranteeTs(t *testing.T) {
	strongTs := typeutil.Timestamp(0)
	boundedTs := typeutil.Timestamp(2)
	tsNow := tsoutil.GetCurrentTime()
	tsMax := tsoutil.GetCurrentTime()

	assert.Equal(t, tsMax, parseGuaranteeTs(strongTs, tsMax))
	ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
	assert.Equal(t, tsoutil.AddPhysicalDurationOnTs(tsMax, -ratio), parseGuaranteeTs(boundedTs, tsMax))
	assert.Equal(t, tsNow, parseGuaranteeTs(tsNow, tsMax))
}

func Test_ParseGuaranteeTsFromConsistency(t *testing.T) {
	strong := commonpb.ConsistencyLevel_Strong
	bounded := commonpb.ConsistencyLevel_Bounded
	eventually := commonpb.ConsistencyLevel_Eventually
	session := commonpb.ConsistencyLevel_Session
	customized := commonpb.ConsistencyLevel_Customized

	tsDefault := typeutil.Timestamp(0)
	tsEventually := typeutil.Timestamp(1)
	tsNow := tsoutil.GetCurrentTime()
	tsMax := tsoutil.GetCurrentTime()

	assert.Equal(t, tsMax, parseGuaranteeTsFromConsistency(tsDefault, tsMax, strong))
	ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
	assert.Equal(t, tsoutil.AddPhysicalDurationOnTs(tsMax, -ratio), parseGuaranteeTsFromConsistency(tsDefault, tsMax, bounded))
	assert.Equal(t, tsNow, parseGuaranteeTsFromConsistency(tsNow, tsMax, session))
	assert.Equal(t, tsNow, parseGuaranteeTsFromConsistency(tsNow, tsMax, customized))
	assert.Equal(t, tsEventually, parseGuaranteeTsFromConsistency(tsDefault, tsMax, eventually))
}

func Test_NQLimit(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateNQLimit(16384))
	assert.Nil(t, validateNQLimit(1))
	assert.Error(t, validateNQLimit(16385))
	assert.Error(t, validateNQLimit(0))
}

func Test_TopKLimit(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateLimit(16384))
	assert.Nil(t, validateLimit(1))
	assert.Error(t, validateLimit(16385))
	assert.Error(t, validateLimit(0))
}

func Test_MaxQueryResultWindow(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateMaxQueryResultWindow(0, 16384))
	assert.Nil(t, validateMaxQueryResultWindow(0, 1))
	assert.Error(t, validateMaxQueryResultWindow(0, 16385))
	assert.Error(t, validateMaxQueryResultWindow(0, 0))
	assert.Error(t, validateMaxQueryResultWindow(1, 0))
}

func Test_GetPartitionProgressFailed(t *testing.T) {
	qc := mocks.NewMockQueryCoordClient(t)
	qc.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Unexpected error",
		},
	}, nil)
	_, _, err := getPartitionProgress(context.TODO(), qc, &commonpb.MsgBase{}, []string{}, "", 1, "")
	assert.Error(t, err)
}

func TestErrWithLog(t *testing.T) {
	err := errors.New("test")
	assert.ErrorIs(t, ErrWithLog(nil, "foo", err), err)
	assert.ErrorIs(t, ErrWithLog(log.Ctx(context.Background()), "foo", err), err)
}

func Test_CheckDynamicFieldData(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		jsonFieldData := autoGenDynamicFieldData(jsonData)
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.NoError(t, err)
	})
	t.Run("key has $meta", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
			"$meta": "error key",
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		jsonFieldData := autoGenDynamicFieldData(jsonData)
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("key has static field name", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
			"Int64Field": "error key",
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		jsonFieldData := autoGenDynamicFieldData(jsonData)
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("disable dynamic schema", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		jsonFieldData := autoGenDynamicFieldData(jsonData)
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		schema.EnableDynamicField = false
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("json data is string", func(t *testing.T) {
		data := "abcdefg"
		jsonFieldData := autoGenDynamicFieldData([][]byte{[]byte(data)})
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err := checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("no json data", func(t *testing.T) {
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err := checkDynamicFieldData(schema, insertMsg)
		assert.NoError(t, err)
	})
}

func Test_validateMaxCapacityPerRow(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
				{
					Key:   common.MaxCapacityKey,
					Value: "10",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.NoError(t, err)
	})

	t.Run("no max capacity", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity not int", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "six",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity exceed max", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "4097",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})
}

func TestAppendUserInfoForRPC(t *testing.T) {
	ctx := GetContext(context.Background(), "root:123456")
	ctx = AppendUserInfoForRPC(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	authorization, ok := md[strings.ToLower(util.HeaderAuthorize)]
	assert.True(t, ok)
	expectAuth := crypto.Base64Encode("root:root")
	assert.Equal(t, expectAuth, authorization[0])
}

func TestGetCostValue(t *testing.T) {
	t.Run("empty status", func(t *testing.T) {
		{
			cost := GetCostValue(&commonpb.Status{})
			assert.Equal(t, 0, cost)
		}

		{
			cost := GetCostValue(nil)
			assert.Equal(t, 0, cost)
		}
	})

	t.Run("wrong cost value style", func(t *testing.T) {
		cost := GetCostValue(&commonpb.Status{
			ExtraInfo: map[string]string{
				"report_value": "abc",
			},
		})
		assert.Equal(t, 0, cost)
	})

	t.Run("success", func(t *testing.T) {
		cost := GetCostValue(&commonpb.Status{
			ExtraInfo: map[string]string{
				"report_value": "100",
			},
		})
		assert.Equal(t, 100, cost)
	})
}

func TestValidateLoadFieldsList(t *testing.T) {
	type testCase struct {
		tag       string
		schema    *schemapb.CollectionSchema
		expectErr bool
	}

	rowIDField := &schemapb.FieldSchema{
		FieldID:  common.RowIDField,
		Name:     common.RowIDFieldName,
		DataType: schemapb.DataType_Int64,
	}
	timestampField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	pkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	scalarField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
	}
	partitionKeyField := &schemapb.FieldSchema{
		FieldID:        common.StartOfUserFieldID + 2,
		Name:           "part_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	vectorField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 3,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	dynamicField := &schemapb.FieldSchema{
		FieldID:   common.StartOfUserFieldID + 4,
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}
	clusteringKeyField := &schemapb.FieldSchema{
		FieldID:         common.StartOfUserFieldID + 5,
		Name:            common.MetaFieldName,
		DataType:        schemapb.DataType_Int32,
		IsClusteringKey: true,
	}

	addSkipLoadAttr := func(f *schemapb.FieldSchema, flag bool) *schemapb.FieldSchema {
		result := typeutil.Clone(f)
		result.TypeParams = append(f.TypeParams, &commonpb.KeyValuePair{
			Key:   common.FieldSkipLoadKey,
			Value: strconv.FormatBool(flag),
		})
		return result
	}

	testCases := []testCase{
		{
			tag: "default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
					clusteringKeyField,
				},
			},
			expectErr: false,
		},
		{
			tag: "pk_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					addSkipLoadAttr(pkField, true),
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "part_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					addSkipLoadAttr(pkField, true),
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "vector_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					addSkipLoadAttr(vectorField, true),
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "clustering_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
					addSkipLoadAttr(clusteringKeyField, true),
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			err := validateLoadFieldsList(tc.schema)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateFunction(t *testing.T) {
	t.Run("Valid function schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.NoError(t, err)
	})

	t.Run("Invalid function schema - duplicate function names", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate function name")
	})

	t.Run("Invalid function schema - input field not found", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"non_existent_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input field not found")
	})

	t.Run("Invalid function schema - output field not found", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"non_existent_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "output field not found")
	})

	t.Run("Valid function schema - nullable input field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}, Nullable: true},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.NoError(t, err)
	})

	t.Run("Invalid function schema - output field is primary key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsPrimaryKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be primary key")
	})

	t.Run("Invalid function schema - output field is partition key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsPartitionKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be partition key or clustering key")
	})

	t.Run("Invalid function schema - output field is clustering key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsClusteringKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be partition key or clustering key")
	})

	t.Run("Invalid function schema - nullable output field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be nullable")
	})
}

func TestValidateModelFunction(t *testing.T) {
	t.Run("Valid model function schema", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
			return map[string]string{
				"mock.apikey": "mock",
			}
		}
		ts := embedding.CreateOpenAIEmbeddingServer()
		defer ts.Close()
		paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
			return map[string]string{
				"openai.url": ts.URL,
			}
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
				{
					Name: "output_dense_field", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					},
				},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "text_embedding_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_dense_field"},
					Params: []*commonpb.KeyValuePair{
						{Key: "provider", Value: "openai"},
						{Key: "model_name", Value: "text-embedding-ada-002"},
						{Key: "credential", Value: "mock"},
						{Key: "dim", Value: "4"},
					},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.NoError(t, err)
	})

	t.Run("Invalid function schema - Invalid function info ", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
				{Name: "output_dense_field", DataType: schemapb.DataType_FloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "text_embedding_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_dense_field"},
					Params: []*commonpb.KeyValuePair{
						{Key: "provider", Value: "UnkownProvider"},
						{Key: "model_name", Value: "text-embedding-ada-002"},
						{Key: "api_key", Value: "mock"},
						{Key: "url", Value: "mock_url"},
						{Key: "dim", Value: "4"},
					},
				},
			},
		}
		err := validateFunction(schema, false)
		assert.Error(t, err)
	})
}

func TestValidateFunctionInputField(t *testing.T) {
	t.Run("Valid BM25 function input", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.NoError(t, err)
	})

	t.Run("Invalid BM25 function input - wrong data type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function input - analyzer not enabled", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "false"}},
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Unknown function type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Unknown,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - wrong type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := checkFunctionInputField(function, fields)
		assert.Error(t, err)
	})
}

func TestValidateFunctionOutputField(t *testing.T) {
	t.Run("Valid BM25 function output", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_SparseFloatVector,
			},
		}
		err := checkFunctionOutputField(function, fields)
		assert.NoError(t, err)
	})

	t.Run("Invalid BM25 function output - wrong data type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Float,
			},
		}
		err := checkFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function output - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_SparseFloatVector,
			},
			{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		err := checkFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Unknown function type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Unknown,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		err := checkFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{}
		err := checkFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - wrong type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := checkFunctionOutputField(function, fields)
		assert.Error(t, err)
	})
}

func TestValidateFunctionBasicParams(t *testing.T) {
	t.Run("Valid function", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "validFunction",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", "input2"},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.NoError(t, err)
	})

	t.Run("Empty function name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty input field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyInputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty output field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyOutputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty input field name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyInputName",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", ""},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Duplicate input field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "duplicateInputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", "input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty output field name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyOutputName",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1", ""},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Input field used as output", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "inputAsOutput",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"field1", "field2"},
			OutputFieldNames: []string{"field1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Duplicate output field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "duplicateOutputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1", "output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty text embedding params", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "textEmbeddingParam",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := checkFunctionBasicParams(function)
		assert.Error(t, err)
	})
}

func TestComputeRecall(t *testing.T) {
	t.Run("normal case1", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 3,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "9", "8", "5", "3", "1"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
			Topks:  []int64{2, 2, 2},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 3,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1"},
					},
				},
			},
			Scores: []float32{1.1, 0.98, 0.8, 0.5, 0.3, 0.1},
			Topks:  []int64{2, 2, 2},
		}

		err := computeRecall(result1, gt)
		assert.NoError(t, err)
		assert.Equal(t, result1.Recalls[0], float32(0.5))
		assert.Equal(t, result1.Recalls[1], float32(1.0))
		assert.Equal(t, result1.Recalls[2], float32(1.0))
	})

	t.Run("normal case2", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 6, 5, 4, 1, 34, 23, 22, 20},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.NoError(t, err)
		assert.Equal(t, result1.Recalls[0], float32(0.6))
		assert.Equal(t, result1.Recalls[1], float32(0.8))
	})

	t.Run("not match size", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 1,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 6, 5, 4},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3},
			Topks:  []int64{5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})

	t.Run("not match type1", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1", "23", "22", "21", "20"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})

	t.Run("not match type2", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1", "23", "22", "21", "20"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})
}

func TestCheckVarcharFormat(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
				FieldID:  100,
				TypeParams: []*commonpb.KeyValuePair{{
					Key:   common.EnableAnalyzerKey,
					Value: "true",
				}},
			},
			// skip field
			{
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	data := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"valid string"},
							},
						},
					},
				},
			}},
		},
	}

	err := checkInputUtf8Compatiable(schema.Fields, data)
	assert.NoError(t, err)

	// invalid data
	invalidUTF8 := []byte{0xC0, 0xAF}
	data = &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{string(invalidUTF8)},
							},
						},
					},
				},
			}},
		},
	}
	err = checkInputUtf8Compatiable(schema.Fields, data)
	assert.Error(t, err)
}

func BenchmarkCheckVarcharFormat(b *testing.B) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
				FieldID:  100,
				TypeParams: []*commonpb.KeyValuePair{{
					Key:   common.EnableAnalyzerKey,
					Value: "true",
				}},
			},
			// skip field
			{
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	data := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{strings.Repeat("a", 1024*1024)},
							},
						},
					},
				},
			}},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		checkInputUtf8Compatiable(schema.Fields, data)
	}
}

func TestCheckAndFlattenStructFieldData(t *testing.T) {
	createTestSchema := func(name string, structFields []*schemapb.StructArrayFieldSchema, normalFields []*schemapb.FieldSchema) *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Name:              name,
			Description:       "test collection with struct array fields",
			StructArrayFields: structFields,
			Fields:            normalFields,
		}
	}

	createTestInsertMsg := func(collectionName string, fieldsData []*schemapb.FieldData) *msgstream.InsertMsg {
		return &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{},
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				CollectionName: collectionName,
				FieldsData:     fieldsData,
			},
		}
	}

	createScalarArrayFieldData := func(fieldName string, data []*schemapb.ScalarField) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: fieldName,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data: data,
						},
					},
				},
			},
		}
	}

	createVectorArrayFieldData := func(fieldName string, data []*schemapb.VectorField) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: fieldName,
			Type:      schemapb.DataType_ArrayOfVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: data,
						},
					},
				},
			},
		}
	}

	createStructArrayFieldData := func(fieldName string, subFields []*schemapb.FieldData) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: fieldName,
			Type:      schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{
					Fields: subFields,
				},
			},
		}
	}

	createNormalFieldData := func(fieldName string, dataType schemapb.DataType) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: fieldName,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		}
	}

	t.Run("success - valid single struct array field", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "user_info",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "age_array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					Name:        "score_array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Float,
				},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		ageArrayData := createScalarArrayFieldData("age_array", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{20, 25}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{30, 35}}}},
		})
		scoreArrayData := createScalarArrayFieldData("score_array", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{85.5, 90.0}}}},
			{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{88.5, 92.0}}}},
		})

		structFieldData := createStructArrayFieldData("user_info", []*schemapb.FieldData{
			ageArrayData, scoreArrayData,
		})

		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 2)
		assert.Equal(t, "user_info[age_array]", insertMsg.FieldsData[0].FieldName)
		assert.Equal(t, "user_info[score_array]", insertMsg.FieldsData[1].FieldName)
	})

	t.Run("success - valid struct array field with vector arrays", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "embedding_info",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "embeddings",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
				},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		vectorArrayData := createVectorArrayFieldData("embeddings", []*schemapb.VectorField{
			{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1.0, 2.0}}}},
			{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3.0, 4.0}}}},
		})

		structFieldData := createStructArrayFieldData("embedding_info", []*schemapb.FieldData{vectorArrayData})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 1)
		assert.Equal(t, "embedding_info[embeddings]", insertMsg.FieldsData[0].FieldName)
		assert.Equal(t, schemapb.DataType_ArrayOfVector, insertMsg.FieldsData[0].Type)
	})

	t.Run("success - multiple struct array fields", func(t *testing.T) {
		structField1 := &schemapb.StructArrayFieldSchema{
			Name: "struct1",
			Fields: []*schemapb.FieldSchema{
				{Name: "field1", DataType: schemapb.DataType_Array},
			},
		}
		structField2 := &schemapb.StructArrayFieldSchema{
			Name: "struct2",
			Fields: []*schemapb.FieldSchema{
				{Name: "field2", DataType: schemapb.DataType_Array},
				{Name: "field3", DataType: schemapb.DataType_ArrayOfVector},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField1, structField2}, nil)

		field1Data := createScalarArrayFieldData("field1", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}},
		})
		field2Data := createScalarArrayFieldData("field2", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2}}}},
		})
		field3Data := createVectorArrayFieldData("field3", []*schemapb.VectorField{
			{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1.0}}}},
		})

		struct1Data := createStructArrayFieldData("struct1", []*schemapb.FieldData{field1Data})
		struct2Data := createStructArrayFieldData("struct2", []*schemapb.FieldData{field2Data, field3Data})

		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{struct1Data, struct2Data})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 3)
		fieldNames := make([]string, len(insertMsg.FieldsData))
		for i, field := range insertMsg.FieldsData {
			fieldNames[i] = field.FieldName
		}
		assert.Contains(t, fieldNames, "struct1[field1]")
		assert.Contains(t, fieldNames, "struct2[field2]")
		assert.Contains(t, fieldNames, "struct2[field3]")
	})

	t.Run("success - mixed normal and struct fields", func(t *testing.T) {
		normalField := &schemapb.FieldSchema{
			Name:     "id",
			DataType: schemapb.DataType_Int64,
		}
		structField := &schemapb.StructArrayFieldSchema{
			Name: "metadata",
			Fields: []*schemapb.FieldSchema{
				{Name: "tags", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, []*schemapb.FieldSchema{normalField})

		normalFieldData := createNormalFieldData("id", schemapb.DataType_Int64)
		tagsData := createScalarArrayFieldData("tags", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"tag1", "tag2"}}}},
		})
		structFieldData := createStructArrayFieldData("metadata", []*schemapb.FieldData{tagsData})

		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{normalFieldData, structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 2)
		fieldNames := make([]string, len(insertMsg.FieldsData))
		for i, field := range insertMsg.FieldsData {
			fieldNames[i] = field.FieldName
		}
		assert.Contains(t, fieldNames, "id")
		assert.Contains(t, fieldNames, "metadata[tags]")
	})

	t.Run("success - empty struct array fields", func(t *testing.T) {
		normalField := &schemapb.FieldSchema{
			Name:     "normal_field",
			DataType: schemapb.DataType_Int64,
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{}, []*schemapb.FieldSchema{normalField})

		normalFieldData := createNormalFieldData("normal_field", schemapb.DataType_Int64)
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{normalFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 1)
		assert.Equal(t, "normal_field", insertMsg.FieldsData[0].FieldName)
	})

	t.Run("error - struct field not found in schema", func(t *testing.T) {
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{}, nil)

		structFieldData := createStructArrayFieldData("non_existent_struct", []*schemapb.FieldData{})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fieldName non_existent_struct not exist in collection schema")
	})

	t.Run("error - invalid field type conversion", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:   "valid_struct",
			Fields: []*schemapb.FieldSchema{},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		invalidFieldData := &schemapb.FieldData{
			FieldName: "valid_struct",
			Type:      schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{invalidFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field convert FieldData_StructArrays fail")
		assert.Contains(t, err.Error(), "valid_struct")
	})

	t.Run("error - field count mismatch", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "field1", DataType: schemapb.DataType_Array},
				{Name: "field2", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		field1Data := createScalarArrayFieldData("field1", []*schemapb.ScalarField{})
		structFieldData := createStructArrayFieldData("test_struct", []*schemapb.FieldData{field1Data})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "length of fields of struct field mismatch")
		assert.Contains(t, err.Error(), "fieldData fields length:1, schema fields length:2")
	})

	t.Run("error - scalar array data is nil", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "field1", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		nilScalarFieldData := &schemapb.FieldData{
			FieldName: "field1",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: nil,
					},
				},
			},
		}
		structFieldData := createStructArrayFieldData("test_struct", []*schemapb.FieldData{nilScalarFieldData})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scalar array data is nil in struct field")
		assert.Contains(t, err.Error(), "test_struct")
		assert.Contains(t, err.Error(), "field1")
	})

	t.Run("error - vector array data is nil", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "vectors", DataType: schemapb.DataType_ArrayOfVector},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		nilVectorFieldData := &schemapb.FieldData{
			FieldName: "vectors",
			Type:      schemapb.DataType_ArrayOfVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: nil,
					},
				},
			},
		}
		structFieldData := createStructArrayFieldData("test_struct", []*schemapb.FieldData{nilVectorFieldData})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "vector array data is nil in struct field")
		assert.Contains(t, err.Error(), "test_struct")
		assert.Contains(t, err.Error(), "vectors")
	})

	t.Run("error - unsupported field data type", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "field1", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		unsupportedFieldData := &schemapb.FieldData{
			FieldName: "field1",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{},
			},
		}
		structFieldData := createStructArrayFieldData("test_struct", []*schemapb.FieldData{unsupportedFieldData})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected field data type in struct array field")
		assert.Contains(t, err.Error(), "test_struct")
	})

	t.Run("error - inconsistent array length", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "field1", DataType: schemapb.DataType_Array},
				{Name: "field2", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		field1Data := createScalarArrayFieldData("field1", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4}}}},
		})
		field2Data := createScalarArrayFieldData("field2", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 8}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{9, 10}}}},
		})

		structFieldData := createStructArrayFieldData("test_struct", []*schemapb.FieldData{field1Data, field2Data})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "inconsistent array length in struct field")
		assert.Contains(t, err.Error(), "expected 2, got 3")
		assert.Contains(t, err.Error(), "field2")
	})

	t.Run("error - struct field count mismatch", func(t *testing.T) {
		structField1 := &schemapb.StructArrayFieldSchema{Name: "struct1", Fields: []*schemapb.FieldSchema{}}
		structField2 := &schemapb.StructArrayFieldSchema{Name: "struct2", Fields: []*schemapb.FieldSchema{}}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField1, structField2}, nil)

		structFieldData := createStructArrayFieldData("struct1", []*schemapb.FieldData{})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "the number of struct array fields is not the same as needed")
		assert.Contains(t, err.Error(), "expected: 2, actual: 1")
	})

	t.Run("edge case - empty struct fields", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:   "empty_struct",
			Fields: []*schemapb.FieldSchema{},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		structFieldData := createStructArrayFieldData("empty_struct", []*schemapb.FieldData{})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 0)
	})

	t.Run("edge case - single element arrays", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "single_element_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "single_field", DataType: schemapb.DataType_Array},
			},
		}
		schema := createTestSchema("test_collection", []*schemapb.StructArrayFieldSchema{structField}, nil)

		singleFieldData := createScalarArrayFieldData("single_field", []*schemapb.ScalarField{
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42}}}},
		})
		structFieldData := createStructArrayFieldData("single_element_struct", []*schemapb.FieldData{singleFieldData})
		insertMsg := createTestInsertMsg("test_collection", []*schemapb.FieldData{structFieldData})

		err := checkAndFlattenStructFieldData(schema, insertMsg)

		assert.NoError(t, err)
		assert.Len(t, insertMsg.FieldsData, 1)
		assert.Equal(t, "single_element_struct[single_field]", insertMsg.FieldsData[0].FieldName)
	})
}

func TestValidateFieldsInStruct(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	t.Run("valid array field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "valid_array",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("valid array of vector field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "valid_array_vector",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("invalid field name", func(t *testing.T) {
		testCases := []struct {
			name     string
			expected string
		}{
			{"", "field name should not be empty"},
			{"123abc", "The first character of a field name must be an underscore or letter"},
			{"abc-def", "Field name can only contain numbers, letters, and underscores"},
		}

		for _, tc := range testCases {
			field := &schemapb.FieldSchema{
				Name:        tc.name,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expected)
		}
	})

	t.Run("invalid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:     "invalid_type",
			DataType: schemapb.DataType_Int32, // Not array or array of vector
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Fields in StructArrayField can only be array or array of struct")
	})

	t.Run("JSON not supported in struct", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "json_field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_JSON,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported")
	})

	t.Run("nested array not supported", func(t *testing.T) {
		testCases := []struct {
			elementType schemapb.DataType
		}{
			{schemapb.DataType_ArrayOfStruct},
			{schemapb.DataType_ArrayOfVector},
			{schemapb.DataType_Array},
		}

		for _, tc := range testCases {
			field := &schemapb.FieldSchema{
				Name:        "nested_array",
				DataType:    schemapb.DataType_Array,
				ElementType: tc.elementType,
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Nested array is not supported")
		}
	})

	t.Run("array field with vector element type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_with_vector",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_FloatVector,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element type FloatVector is not supported")
	})

	t.Run("array of vector field with non-vector element type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_with_scalar",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_Int32,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Unsupported element type of array field array_vector_with_scalar, now only float vector is supported")
	})

	t.Run("array of vector missing dimension", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_no_dim",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams:  []*commonpb.KeyValuePair{}, // No dimension specified
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dimension is not defined in field")
	})

	t.Run("array of vector with invalid dimension", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_invalid_dim",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "not_a_number"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
	})

	t.Run("varchar array without max_length", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_no_max_length",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams:  []*commonpb.KeyValuePair{}, // No max_length specified
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type param(max_length) should be specified")
	})

	t.Run("varchar array with valid max_length", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_valid",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "100"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("varchar array with invalid max_length", func(t *testing.T) {
		// Test with max_length exceeding limit
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_invalid_length",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "99999999"}, // Exceeds limit
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "the maximum length specified for the field")
	})

	t.Run("nullable field not supported", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "nullable_field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			Nullable:    true,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nullable is not supported for fields in struct array now")
	})

	// t.Run("sparse float vector in array of vector", func(t *testing.T) {
	// 	// Note: ArrayOfVector with sparse vector element type still requires dimension
	// 	// because validateDimension checks the field's DataType (ArrayOfVector), not ElementType
	// 	field := &schemapb.FieldSchema{
	// 		Name:        "sparse_vector_array",
	// 		DataType:    schemapb.DataType_ArrayOfVector,
	// 		ElementType: schemapb.DataType_SparseFloatVector,
	// 		TypeParams:  []*commonpb.KeyValuePair{},
	// 	}
	// 	err := ValidateFieldsInStruct(field, schema)
	// 	assert.Error(t, err)
	// 	assert.Contains(t, err.Error(), "dimension is not defined")
	// })

	t.Run("array with various scalar element types", func(t *testing.T) {
		validScalarTypes := []schemapb.DataType{
			schemapb.DataType_Bool,
			schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
		}

		for _, dt := range validScalarTypes {
			field := &schemapb.FieldSchema{
				Name:        "array_" + dt.String(),
				DataType:    schemapb.DataType_Array,
				ElementType: dt,
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.NoError(t, err)
		}
	})

	t.Run("array of vector with various vector types", func(t *testing.T) {
		validVectorTypes := []schemapb.DataType{
			schemapb.DataType_FloatVector,
			// schemapb.DataType_BinaryVector,
			// schemapb.DataType_Float16Vector,
			// schemapb.DataType_BFloat16Vector,
			// Note: SparseFloatVector is excluded because validateDimension checks
			// the field's DataType (ArrayOfVector), not ElementType, so it still requires dimension
		}

		for _, vt := range validVectorTypes {
			field := &schemapb.FieldSchema{
				Name:        "vector_array_" + vt.String(),
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: vt,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.NoError(t, err)
		}
	})
}

func Test_reconstructStructFieldDataCommon(t *testing.T) {
	t.Run("count(*) query - should return early", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "count(*)",
				FieldId:   0,
				Type:      schemapb.DataType_Int64,
			},
		}
		outputFields := []string{"count(*)"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(fieldsData))
		copy(originalFieldsData, fieldsData)
		originalOutputFields := make([]string, len(outputFields))
		copy(originalOutputFields, outputFields)

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Should not modify anything for count(*) query
		assert.Equal(t, originalFieldsData, resultFieldsData)
		assert.Equal(t, originalOutputFields, resultOutputFields)
	})

	t.Run("struct field query - should reconstruct struct field", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "test_struct[sub_field]",
				FieldId:   1021, // Use the correct field ID that matches the schema
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
			},
		}
		outputFields := []string{"test_struct[sub_field]"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Should reconstruct the struct field with the restored field name
		assert.Len(t, resultFieldsData, 1)
		assert.Equal(t, "test_struct", resultFieldsData[0].FieldName)
		assert.Equal(t, int64(102), resultFieldsData[0].FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, resultFieldsData[0].Type)

		// Check that the sub-field name has been restored
		structArrayField := resultFieldsData[0].GetStructArrays()
		assert.NotNil(t, structArrayField)
		assert.Len(t, structArrayField.Fields, 1)
		assert.Equal(t, "sub_field", structArrayField.Fields[0].FieldName) // Name should be restored

		assert.Equal(t, []string{"test_struct"}, resultOutputFields)
	})

	t.Run("no struct array fields - should return early", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
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
		}
		outputFields := []string{"field1", "field2"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(fieldsData))
		copy(originalFieldsData, fieldsData)
		originalOutputFields := make([]string, len(outputFields))
		copy(originalOutputFields, outputFields)

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Should not modify anything when no struct array fields
		assert.Equal(t, originalFieldsData, resultFieldsData)
		assert.Equal(t, originalOutputFields, resultOutputFields)
	})

	t.Run("reconstruct single struct field", func(t *testing.T) {
		// Create mock data with transformed field names (as they would be internally)
		subField1Data := &schemapb.FieldData{
			FieldName: "test_struct[sub_int_array]",
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
			FieldName: "test_struct[sub_text_array]",
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

		fieldsData := []*schemapb.FieldData{subField1Data, subField2Data}
		outputFields := []string{"test_struct[sub_int_array]", "test_struct[sub_text_array]"}

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
							Name:        "test_struct[sub_int_array]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							FieldID:     1022,
							Name:        "test_struct[sub_text_array]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Check result
		assert.Len(t, resultFieldsData, 1, "Should only have one reconstructed struct field")
		assert.Len(t, resultOutputFields, 1, "Output fields should only have one")

		structField := resultFieldsData[0]
		assert.Equal(t, "test_struct", structField.FieldName)
		assert.Equal(t, int64(102), structField.FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, structField.Type)
		assert.Equal(t, "test_struct", resultOutputFields[0])

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

		// Create struct sub field data with transformed name
		subFieldData := &schemapb.FieldData{
			FieldName: "test_struct[sub_field]",
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

		fieldsData := []*schemapb.FieldData{regularField, subFieldData}
		outputFields := []string{"regular_field", "test_struct[sub_field]"}

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
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Check result: should have 2 fields (1 regular + 1 reconstructed struct)
		assert.Len(t, resultFieldsData, 2)
		assert.Len(t, resultOutputFields, 2)

		// Check regular and struct fields both exist
		var foundRegularField, foundStructField bool
		for i, field := range resultFieldsData {
			switch field.FieldId {
			case 100:
				assert.Equal(t, "regular_field", field.FieldName)
				assert.Equal(t, schemapb.DataType_Int64, field.Type)
				assert.Equal(t, "regular_field", resultOutputFields[i])
				foundRegularField = true
			case 102:
				assert.Equal(t, "test_struct", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				assert.Equal(t, "test_struct", resultOutputFields[i])
				foundStructField = true
			}
		}
		assert.True(t, foundRegularField, "Should find regular field")
		assert.True(t, foundStructField, "Should find reconstructed struct field")
	})

	t.Run("multiple struct fields", func(t *testing.T) {
		// Create sub field for first struct
		struct1SubField := &schemapb.FieldData{
			FieldName: "struct1[struct1_sub]",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data:        []*schemapb.ScalarField{},
						},
					},
				},
			},
		}

		// Create sub fields for second struct
		struct2SubField1 := &schemapb.FieldData{
			FieldName: "struct2[struct2_sub1]",
			FieldId:   1031,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data:        []*schemapb.ScalarField{},
						},
					},
				},
			},
		}

		struct2SubField2 := &schemapb.FieldData{
			FieldName: "struct2[struct2_sub2]",
			FieldId:   1032,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"test"}},
					},
				},
			},
		}

		fieldsData := []*schemapb.FieldData{struct1SubField, struct2SubField1, struct2SubField2}
		outputFields := []string{"struct1[struct1_sub]", "struct2[struct2_sub1]", "struct2[struct2_sub2]"}

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
					Name:    "struct1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "struct1[struct1_sub]",
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
							Name:        "struct2[struct2_sub1]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							FieldID:  1032,
							Name:     "struct2[struct2_sub2]",
							DataType: schemapb.DataType_VarChar,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Check result: should have 2 struct fields
		assert.Len(t, resultFieldsData, 2)
		assert.Len(t, resultOutputFields, 2)

		// Check both struct fields
		var foundStruct1, foundStruct2 bool
		for _, field := range resultFieldsData {
			switch field.FieldId {
			case 102:
				assert.Equal(t, "struct1", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				foundStruct1 = true
				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 1)
			case 103:
				assert.Equal(t, "struct2", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				foundStruct2 = true
				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 2)
			}
		}
		assert.True(t, foundStruct1, "Should find struct1")
		assert.True(t, foundStruct2, "Should find struct2")
	})

	t.Run("partial struct fields query - only return queried fields", func(t *testing.T) {
		// Create a struct with 3 fields, but only query 2 of them
		// This tests that we only return what the user requested

		// Create mock data for only 2 out of 3 struct fields
		clipStrData := &schemapb.FieldData{
			FieldName: "clip[str]",
			FieldId:   2001,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"text1", "text2"}},
					},
				},
			},
		}

		clipIntData := &schemapb.FieldData{
			FieldName: "clip[int]",
			FieldId:   2002,
			Type:      schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{100, 200}},
					},
				},
			},
		}

		// Note: clip[embedding] is NOT included in query results
		fieldsData := []*schemapb.FieldData{clipStrData, clipIntData}
		outputFields := []string{"clip[str]", "clip[int]"}

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
					FieldID: 200,
					Name:    "clip",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:  2001,
							Name:     "clip[str]",
							DataType: schemapb.DataType_VarChar,
						},
						{
							FieldID:  2002,
							Name:     "clip[int]",
							DataType: schemapb.DataType_Int32,
						},
						{
							FieldID:  2003,
							Name:     "clip[embedding]",
							DataType: schemapb.DataType_FloatVector,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: "dim", Value: "128"},
							},
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldDataCommon(fieldsData, outputFields, schema)

		// Check result
		assert.Len(t, resultFieldsData, 1, "Should have one reconstructed struct field")
		assert.Len(t, resultOutputFields, 1, "Output fields should have one")

		structField := resultFieldsData[0]
		assert.Equal(t, "clip", structField.FieldName)
		assert.Equal(t, int64(200), structField.FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, structField.Type)
		assert.Equal(t, "clip", resultOutputFields[0])

		// Check that struct only contains the 2 queried fields, NOT the embedding field
		structArrays := structField.GetStructArrays()
		assert.NotNil(t, structArrays)
		assert.Len(t, structArrays.Fields, 2, "Struct should only contain 2 queried fields, not 3")

		// Verify the field names have been restored to original names
		var foundStr, foundInt bool
		for _, field := range structArrays.Fields {
			switch field.FieldId {
			case 2001:
				assert.Equal(t, "str", field.FieldName, "Field name should be restored to original")
				assert.Equal(t, schemapb.DataType_VarChar, field.Type)
				foundStr = true
			case 2002:
				assert.Equal(t, "int", field.FieldName, "Field name should be restored to original")
				assert.Equal(t, schemapb.DataType_Int32, field.Type)
				foundInt = true
			case 2003:
				assert.Fail(t, "Should not include embedding field as it was not queried")
			}
		}
		assert.True(t, foundStr, "Should find str field")
		assert.True(t, foundInt, "Should find int field")
	})
}

func TestLackOfFieldsDataBySchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk_field", IsPrimaryKey: true, DataType: schemapb.DataType_Int64, AutoID: true},
			{FieldID: 101, Name: "required_field", DataType: schemapb.DataType_Float},
			{FieldID: 102, Name: "nullable_field", DataType: schemapb.DataType_VarChar, Nullable: true},
			{FieldID: 103, Name: "default_value_field", DataType: schemapb.DataType_JSON, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "{}"}}},
		},
	}

	tests := []struct {
		name             string
		fieldsData       []*schemapb.FieldData
		skipPkFieldCheck bool
		expectErr        bool
	}{
		{
			name: "all required fields present",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "pk_field"},
				{FieldName: "required_field"},
				{FieldName: "nullable_field"},
				{FieldName: "default_value_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        false,
		},
		{
			name: "missing required field",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "pk_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        true,
		},
		{
			name: "missing nullable field is ok",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "pk_field"},
				{FieldName: "required_field"},
				{FieldName: "default_value_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        false,
		},
		{
			name: "missing default value field is ok",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "pk_field"},
				{FieldName: "required_field"},
				{FieldName: "nullable_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        false,
		},
		{
			name: "missing nullable and default value field is ok",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "pk_field"},
				{FieldName: "required_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        false,
		},
		{
			name:             "empty fields data",
			fieldsData:       []*schemapb.FieldData{},
			skipPkFieldCheck: false,
			expectErr:        true,
		},
		{
			name: "skip pk check",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "required_field"},
			},
			skipPkFieldCheck: true,
			expectErr:        false,
		},
		{
			name: "missing pk without skip",
			fieldsData: []*schemapb.FieldData{
				{FieldName: "required_field"},
			},
			skipPkFieldCheck: false,
			expectErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := LackOfFieldsDataBySchema(schema, tt.fieldsData, tt.skipPkFieldCheck, false)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetStorageCost(t *testing.T) {
	// nil and empty cases
	t.Run("nil or empty status", func(t *testing.T) {
		{
			remote, total, ratio, ok := GetStorageCost(nil)
			assert.Equal(t, int64(0), remote)
			assert.Equal(t, int64(0), total)
			assert.Equal(t, 0.0, ratio)
			assert.False(t, ok)
		}
		{
			remote, total, ratio, ok := GetStorageCost(&commonpb.Status{})
			assert.Equal(t, int64(0), remote)
			assert.Equal(t, int64(0), total)
			assert.Equal(t, 0.0, ratio)
			assert.False(t, ok)
		}
	})

	// missing keys should result in zeros
	t.Run("missing keys", func(t *testing.T) {
		st := &commonpb.Status{ExtraInfo: map[string]string{
			"scanned_remote_bytes": "100",
		}}
		remote, total, ratio, ok := GetStorageCost(st)
		assert.Equal(t, int64(0), remote)
		assert.Equal(t, int64(0), total)
		assert.Equal(t, 0.0, ratio)
		assert.False(t, ok)
	})

	// invalid number formats should result in zeros
	t.Run("invalid formats", func(t *testing.T) {
		st := &commonpb.Status{ExtraInfo: map[string]string{
			"scanned_remote_bytes": "x",
			"scanned_total_bytes":  "200",
			"cache_hit_ratio":      "0.5",
		}}
		remote, total, ratio, ok := GetStorageCost(st)
		assert.Equal(t, int64(0), remote)
		assert.Equal(t, int64(0), total)
		assert.Equal(t, 0.0, ratio)
		assert.False(t, ok)

		st = &commonpb.Status{ExtraInfo: map[string]string{
			"scanned_remote_bytes": "100",
			"scanned_total_bytes":  "y",
			"cache_hit_ratio":      "0.5",
		}}
		remote, total, ratio, ok = GetStorageCost(st)
		assert.Equal(t, int64(0), remote)
		assert.Equal(t, int64(0), total)
		assert.Equal(t, 0.0, ratio)
		assert.False(t, ok)

		st = &commonpb.Status{ExtraInfo: map[string]string{
			"scanned_remote_bytes": "100",
			"scanned_total_bytes":  "200",
			"cache_hit_ratio":      "abc",
		}}
		remote, total, ratio, ok = GetStorageCost(st)
		assert.Equal(t, int64(0), remote)
		assert.Equal(t, int64(0), total)
		assert.Equal(t, 0.0, ratio)
		assert.False(t, ok)
	})

	// success case
	t.Run("success", func(t *testing.T) {
		st := &commonpb.Status{ExtraInfo: map[string]string{
			"scanned_remote_bytes": "123",
			"scanned_total_bytes":  "456",
			"cache_hit_ratio":      "0.27",
		}}
		remote, total, ratio, ok := GetStorageCost(st)
		assert.Equal(t, int64(123), remote)
		assert.Equal(t, int64(456), total)
		assert.InDelta(t, 0.27, ratio, 1e-9)
		assert.True(t, ok)
	})
}

func TestCheckDuplicatePkExist_Int64PK(t *testing.T) {
	primaryFieldSchema := &schemapb.FieldSchema{
		Name:     "id",
		FieldID:  100,
		DataType: schemapb.DataType_Int64,
	}

	t.Run("with duplicates", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "id",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 1, 4, 2}, // duplicates: 1, 2
							},
						},
					},
				},
			},
		}

		hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
		assert.NoError(t, err)
		assert.True(t, hasDup)
	})

	t.Run("without duplicates", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "id",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4, 5},
							},
						},
					},
				},
			},
		}

		hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
		assert.NoError(t, err)
		assert.False(t, hasDup)
	})
}

func TestCheckDuplicatePkExist_VarCharPK(t *testing.T) {
	primaryFieldSchema := &schemapb.FieldSchema{
		Name:     "id",
		FieldID:  100,
		DataType: schemapb.DataType_VarChar,
	}

	t.Run("with duplicates", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "id",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a", "b", "c", "a", "d"}, // duplicate: "a"
							},
						},
					},
				},
			},
		}

		hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
		assert.NoError(t, err)
		assert.True(t, hasDup)
	})

	t.Run("without duplicates", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "id",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a", "b", "c", "d", "e"},
							},
						},
					},
				},
			},
		}

		hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
		assert.NoError(t, err)
		assert.False(t, hasDup)
	})
}

func TestCheckDuplicatePkExist_EmptyData(t *testing.T) {
	primaryFieldSchema := &schemapb.FieldSchema{
		Name:     "id",
		FieldID:  100,
		DataType: schemapb.DataType_Int64,
	}

	hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, []*schemapb.FieldData{})
	assert.NoError(t, err)
	assert.False(t, hasDup)
}

func TestCheckDuplicatePkExist_MissingPrimaryKey(t *testing.T) {
	primaryFieldSchema := &schemapb.FieldSchema{
		Name:     "id",
		FieldID:  100,
		DataType: schemapb.DataType_Int64,
	}

	fieldsData := []*schemapb.FieldData{
		{
			FieldName: "other_field",
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
			},
		},
	}

	hasDup, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
	assert.Error(t, err)
	assert.False(t, hasDup)
}
