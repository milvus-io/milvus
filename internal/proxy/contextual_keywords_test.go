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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

var contextualFieldKeywordsForTest = []string{
	"text_match_fuzzy", "match_all", "match_any", "match_least", "match_most", "match_exact",
	"iso", "interval", "minimum_should_match", "threshold", "element_filter",
	"st_equals", "st_touches", "st_overlaps", "st_crosses", "st_contains",
	"st_intersects", "st_within", "st_dwithin", "st_isvalid",
}

func fieldKeywordCaseVariantsForTest(keyword string) []string {
	names := make([]string, 0, 1<<len(keyword))
	for mask := 0; mask < 1<<len(keyword); mask++ {
		name := []byte(keyword)
		for i := range name {
			if mask&(1<<i) != 0 {
				name[i] -= 'a' - 'A'
			}
		}
		names = append(names, string(name))
	}
	return names
}

func keywordValidationSchemaForTest() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "keyword_validation",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
	}
}

func keywordArrayFieldForTest(name string) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: name, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
	}
}

func keywordStructFieldForTest(name string) *schemapb.StructArrayFieldSchema {
	return &schemapb.StructArrayFieldSchema{
		Name: name, Nullable: true,
		Fields: []*schemapb.FieldSchema{keywordArrayFieldForTest("values")},
	}
}

func createKeywordSchemaForTest(t *testing.T, schema *schemapb.CollectionSchema) error {
	t.Helper()
	bytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	task := &createCollectionTask{
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{}, CollectionName: schema.GetName(), Schema: bytes,
		},
	}
	return task.PreExecute(context.Background())
}

func requireKeywordValidationForTest(t *testing.T, err error, reserved bool) {
	t.Helper()
	if reserved {
		requireInputValidationErrorForTest(t, err, merr.ErrFieldInvalidName, 1701)
	} else {
		require.NoError(t, err)
	}
}

func requireInputValidationErrorForTest(t *testing.T, err, expectedErr error, expectedCode int32) {
	t.Helper()
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, expectedCode, merr.Code(err))
	require.Equal(t, merr.InputError, merr.GetErrorType(err))

	// Create/Add RPCs project the task error with merr.Status; REST then
	// reconstructs it with merr.Error. Classification must survive both.
	status := merr.Status(err)
	require.Equal(t, expectedCode, status.GetCode())
	require.Equal(t, err.Error(), status.GetReason())
	require.False(t, status.GetRetriable())
	require.Equal(t, "true", status.GetExtraInfo()[merr.InputErrorFlagKey])
	label, cause := requestutil.ParseMetricLabel(status, nil)
	require.Equal(t, metrics.FailLabel, label)
	require.Equal(t, metrics.CauseUser, cause)

	roundTrip := merr.Error(status)
	require.ErrorIs(t, roundTrip, expectedErr)
	require.Equal(t, expectedCode, merr.Code(roundTrip))
	require.Equal(t, merr.InputError, merr.GetErrorType(roundTrip))
	require.False(t, merr.IsRetryableErr(roundTrip))
}

func TestFieldNameKeywordPolicy(t *testing.T) {
	check := func(t *testing.T, name string, reserved bool) {
		schema := keywordValidationSchemaForTest()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name: name, DataType: schemapb.DataType_Int64,
		})
		requireKeywordValidationForTest(t, createKeywordSchemaForTest(t, schema), reserved)

		// The public add-field path must enforce the same policy as creation.
		field, err := proto.Marshal(&schemapb.FieldSchema{
			Name: name, DataType: schemapb.DataType_Int64, Nullable: true,
		})
		require.NoError(t, err)
		add := &addCollectionFieldTask{
			oldSchema:                 keywordValidationSchemaForTest(),
			AddCollectionFieldRequest: &milvuspb.AddCollectionFieldRequest{Schema: field},
		}
		requireKeywordValidationForTest(t, add.PreExecute(context.Background()), reserved)

		// Struct sub-fields already use the shared field-name validator.
		requireKeywordValidationForTest(t, ValidateFieldsInStruct(
			keywordArrayFieldForTest(name), keywordValidationSchemaForTest()), reserved)

		// Check sub-field failures through actual create/add struct tasks too,
		// before the names are transformed to profile[field].
		structField := keywordStructFieldForTest("profile")
		structField.Fields[0].Name = name
		structSchema := keywordValidationSchemaForTest()
		structSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{structField}
		requireKeywordValidationForTest(t, createKeywordSchemaForTest(t, structSchema), reserved)
		addStruct := &addCollectionStructFieldTask{
			oldSchema: keywordValidationSchemaForTest(),
			AddCollectionStructFieldRequest: &milvuspb.AddCollectionStructFieldRequest{
				StructArrayFieldSchema: structField,
			},
		}
		requireKeywordValidationForTest(t, addStruct.PreExecute(context.Background()), reserved)
	}

	for _, keyword := range []string{"like", "and", "or", "not", "in", "null"} {
		for _, name := range fieldKeywordCaseVariantsForTest(keyword) {
			t.Run(name, func(t *testing.T) { check(t, name, true) })
		}
	}
	for _, keyword := range contextualFieldKeywordsForTest {
		for _, name := range []string{keyword, strings.ToUpper(keyword)} {
			t.Run(name, func(t *testing.T) { check(t, name, false) })
		}
	}
	t.Run("ordinary_field", func(t *testing.T) { check(t, "ordinary_field", false) })
	for _, name := range []string{"", "1field", "bad name", "a-b", strings.Repeat("a", 256)} {
		t.Run("invalid_"+name, func(t *testing.T) { check(t, name, true) })
	}
}

func TestStructArrayFieldKeywordPolicy(t *testing.T) {
	check := func(t *testing.T, name string, reserved bool) {
		schema := keywordValidationSchemaForTest()
		schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{keywordStructFieldForTest(name)}
		requireKeywordValidationForTest(t, createKeywordSchemaForTest(t, schema), reserved)

		add := &addCollectionStructFieldTask{
			oldSchema: keywordValidationSchemaForTest(),
			AddCollectionStructFieldRequest: &milvuspb.AddCollectionStructFieldRequest{
				StructArrayFieldSchema: keywordStructFieldForTest(name),
			},
		}
		requireKeywordValidationForTest(t, add.PreExecute(context.Background()), reserved)
	}

	for _, keyword := range []string{"like", "and", "or", "not", "in", "null"} {
		for _, name := range fieldKeywordCaseVariantsForTest(keyword) {
			t.Run(name, func(t *testing.T) { check(t, name, true) })
		}
	}
	for _, keyword := range contextualFieldKeywordsForTest {
		for _, name := range []string{keyword, strings.ToUpper(keyword)} {
			t.Run(name, func(t *testing.T) { check(t, name, false) })
		}
	}
	t.Run("ordinary_struct", func(t *testing.T) { check(t, "profile", false) })
	for _, name := range []string{"", "1field", "bad name", "$meta", "a-b", strings.Repeat("a", 256)} {
		t.Run("invalid_"+name, func(t *testing.T) { check(t, name, true) })
	}
	t.Run("parent_name_is_checked_before_children", func(t *testing.T) {
		err := ValidateStructArrayField(&schemapb.StructArrayFieldSchema{Name: "AND"}, keywordValidationSchemaForTest())
		requireKeywordValidationForTest(t, err, true)
	})
}

func TestFieldNameErrorClassificationBoundary(t *testing.T) {
	// The global factory also represents internal schema failures. Its default
	// must stay SystemError; only request-name validation stamps InputError.
	internalErr := merr.WrapErrFieldNameInvalid("Like", "Invalid field name: Like. Like is keyword in milvus.")
	require.Equal(t, merr.SystemError, merr.GetErrorType(internalErr))
	internalStatus := merr.Status(internalErr)
	require.Equal(t, int32(1701), internalStatus.GetCode())
	require.False(t, internalStatus.GetRetriable())
	require.Empty(t, internalStatus.GetExtraInfo())
	label, cause := requestutil.ParseMetricLabel(internalStatus, nil)
	require.Equal(t, metrics.FailLabel, label)
	require.Equal(t, metrics.CauseSystem, cause)

	inputErr := validateFieldName("Like")
	requireKeywordValidationForTest(t, inputErr, true)
	require.Equal(t, internalErr.Error(), inputErr.Error())
	require.Equal(t, internalStatus.GetErrorCode(), merr.Status(inputErr).GetErrorCode())

	t.Run("meta_keeps_existing_api_error_codes", func(t *testing.T) {
		// Creation and direct name validation reject '$' with 1701. AddField
		// already rejects system fields earlier with 1100; preserve that code.
		requireKeywordValidationForTest(t, validateFieldName(common.MetaFieldName), true)
		schema := keywordValidationSchemaForTest()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name: common.MetaFieldName, DataType: schemapb.DataType_Int64,
		})
		requireKeywordValidationForTest(t, createKeywordSchemaForTest(t, schema), true)

		field, err := proto.Marshal(&schemapb.FieldSchema{
			Name: common.MetaFieldName, DataType: schemapb.DataType_Int64, Nullable: true,
		})
		require.NoError(t, err)
		add := &addCollectionFieldTask{
			oldSchema:                 keywordValidationSchemaForTest(),
			AddCollectionFieldRequest: &milvuspb.AddCollectionFieldRequest{Schema: field},
		}
		requireInputValidationErrorForTest(t, add.PreExecute(context.Background()), merr.ErrParameterInvalid, 1100)
	})
}
