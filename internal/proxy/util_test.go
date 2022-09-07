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

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
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
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateCollectionName(name))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "name"))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "alias"))
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
				Key:   "dim",
				Value: "1",
			},
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension)),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	// invalid dim
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "-1",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_BinaryVector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "8",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension)),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "9",
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
		Key:   "metric_type",
		Value: "",
	})
	assert.Nil(t, validateVectorFieldMetricType(field1))
}

func TestValidateDuplicatedFieldName(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{Name: "abc"},
		{Name: "def"},
	}
	assert.Nil(t, validateDuplicatedFieldName(fields))
	fields = append(fields, &schemapb.FieldSchema{
		Name: "abc",
	})
	assert.NotNil(t, validateDuplicatedFieldName(fields))
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
				Key:   "max_length",
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
	assert.Error(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, VarCharField},
	}))
}

func TestValidateFieldType(t *testing.T) {
	type testCase struct {
		dt       schemapb.DataType
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
	}

	for _, tc := range cases {
		t.Run(tc.dt.String(), func(t *testing.T) {
			sch := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						DataType: tc.dt,
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

func TestValidateSchema(t *testing.T) {
	coll := &schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      false,
		Fields:      nil,
	}
	assert.NotNil(t, validateSchema(coll))

	pf1 := &schemapb.FieldSchema{
		Name:         "f1",
		FieldID:      100,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf1)
	assert.NotNil(t, validateSchema(coll))

	pf1.IsPrimaryKey = true
	assert.Nil(t, validateSchema(coll))

	pf1.DataType = schemapb.DataType_Int32
	assert.NotNil(t, validateSchema(coll))

	pf1.DataType = schemapb.DataType_Int64
	assert.Nil(t, validateSchema(coll))

	pf2 := &schemapb.FieldSchema{
		Name:         "f2",
		FieldID:      101,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf2)
	assert.NotNil(t, validateSchema(coll))

	pf2.IsPrimaryKey = false
	assert.Nil(t, validateSchema(coll))

	pf2.Name = "f1"
	assert.NotNil(t, validateSchema(coll))
	pf2.Name = "f2"
	assert.Nil(t, validateSchema(coll))

	pf2.FieldID = 100
	assert.NotNil(t, validateSchema(coll))

	pf2.FieldID = 101
	assert.Nil(t, validateSchema(coll))

	pf2.DataType = -1
	assert.NotNil(t, validateSchema(coll))

	pf2.DataType = schemapb.DataType_FloatVector
	assert.NotNil(t, validateSchema(coll))

	pf2.DataType = schemapb.DataType_Int64
	assert.Nil(t, validateSchema(coll))

	tp3Good := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
	}

	tp3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "asdfa",
		},
	}

	tp3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "-1",
		},
	}

	tp3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   "dimX",
			Value: "128",
		},
	}

	tp3Bad4 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
		{
			Key:   "dim",
			Value: "64",
		},
	}

	ip3Good := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "IP",
		},
	}

	ip3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "JACCARD",
		},
	}

	ip3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "xxxxxx",
		},
	}

	ip3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "L2",
		},
		{
			Key:   "metric_type",
			Value: "IP",
		},
	}

	pf3 := &schemapb.FieldSchema{
		Name:         "f3",
		FieldID:      102,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   tp3Good,
		IndexParams:  ip3Good,
	}

	coll.Fields = append(coll.Fields, pf3)
	assert.Nil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad1
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad2
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad3
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad4
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Good
	assert.Nil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad1
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad2
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad3
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Good
	assert.Nil(t, validateSchema(coll))
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
	schema := &schemapb.CollectionSchema{}
	columns := []*schemapb.FieldData{
		{
			FieldName: "TestFillFieldIDBySchema",
		},
	}

	// length mismatch
	assert.Error(t, fillFieldIDBySchema(columns, schema))
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "TestFillFieldIDBySchema",
				DataType: schemapb.DataType_Int64,
				FieldID:  1,
			},
		},
	}
	assert.NoError(t, fillFieldIDBySchema(columns, schema))
	assert.Equal(t, "TestFillFieldIDBySchema", columns[0].FieldName)
	assert.Equal(t, schemapb.DataType_Int64, columns[0].Type)
	assert.Equal(t, int64(1), columns[0].FieldId)
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
	// illegal character which not alphabet, _, or number
	res = ValidateUsername("a1^7*).,")
	assert.Error(t, res)
	// normal username that only contains alphabet, _, and number
	Params.InitOnce()
	res = ValidateUsername("a17_good")
	assert.Nil(t, res)
}

func TestValidatePassword(t *testing.T) {
	Params.InitOnce()
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
	Params.InitOnce()
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
		assert.Nil(t, ValidatePrincipalName(name))
		assert.Nil(t, ValidatePrincipalType(name))
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
		assert.NotNil(t, ValidatePrincipalName(name))
		assert.NotNil(t, ValidatePrincipalType(name))
		assert.NotNil(t, ValidatePrivilege(name))
	}
	assert.NotNil(t, ValidateObjectName(" "))
	assert.NotNil(t, ValidateObjectName(string(longName)))
	assert.Nil(t, ValidateObjectName("*"))
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

func TestGetCurUserFromContext(t *testing.T) {
	_, err := GetCurUserFromContext(context.Background())
	assert.NotNil(t, err)

	_, err = GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.NotNil(t, err)

	_, err = GetCurUserFromContext(GetContext(context.Background(), "123456"))
	assert.NotNil(t, err)

	root := "root"
	password := "123456"
	username, err := GetCurUserFromContext(GetContext(context.Background(), fmt.Sprintf("%s%s%s", root, util.CredentialSeperator, password)))
	assert.Nil(t, err)
	assert.Equal(t, "root", username)
}

func TestGetRole(t *testing.T) {
	globalMetaCache = nil
	_, err := GetRole("foo")
	assert.NotNil(t, err)
	globalMetaCache = &mockCache{
		getUserRoleFunc: func(username string) []string {
			if username == "root" {
				return []string{"role1", "admin", "role2"}
			}
			return []string{"role1"}
		},
	}
	roles, err := GetRole("root")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(roles))

	roles, err = GetRole("foo")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(roles))
}

func TestPasswordVerify(t *testing.T) {
	username := "user-test00"
	password := "PasswordVerify"

	// credential does not exist within cache
	credCache := make(map[string]*internalpb.CredentialInfo, 0)
	invokedCount := 0

	mockedRootCoord := newMockRootCoord()
	mockedRootCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		invokedCount++
		return nil, fmt.Errorf("get cred not found credential")
	}

	metaCache := &MetaCache{
		credMap:   credCache,
		rootCoord: mockedRootCoord,
	}
	ret, ok := credCache[username]
	assert.False(t, ok)
	assert.Nil(t, ret)
	assert.False(t, passwordVerify(context.TODO(), username, password, metaCache))
	assert.Equal(t, 1, invokedCount)

	// Sha256Password has not been filled into cache during establish connection firstly
	encryptedPwd, err := crypto.PasswordEncrypt(password)
	assert.Nil(t, err)
	credCache[username] = &internalpb.CredentialInfo{
		Username:          username,
		EncryptedPassword: encryptedPwd,
	}
	assert.True(t, passwordVerify(context.TODO(), username, password, metaCache))
	ret, ok = credCache[username]
	assert.True(t, ok)
	assert.NotNil(t, ret)
	assert.Equal(t, username, ret.Username)
	assert.NotNil(t, ret.Sha256Password)
	assert.Equal(t, 1, invokedCount)

	// Sha256Password already exists within cache
	assert.True(t, passwordVerify(context.TODO(), username, password, metaCache))
	assert.Equal(t, 1, invokedCount)
}
