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

package rlsutil

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type testStorageFieldData struct {
	data        any
	dataType    schemapb.DataType
	elementType schemapb.DataType
	validData   []bool
}

func (f *testStorageFieldData) GetDataRows() any               { return f.data }
func (f *testStorageFieldData) GetDataType() schemapb.DataType { return f.dataType }
func (f *testStorageFieldData) GetElementType() schemapb.DataType {
	return f.elementType
}
func (f *testStorageFieldData) GetValidData() []bool { return f.validData }

func validateRows(ctx context.Context, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, expr string, operation string, exprKind string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" || rowNum == 0 {
		return nil
	}
	parsedExpr, err := planparserv2.ParseExpr(schemaHelper, expr, nil)
	if err != nil {
		return merr.Wrapf(err, "failed to parse RLS %s expression for %s", exprKind, operation)
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, parsedExpr, operation, exprKind)
}

func newManagerTestSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Name: "rls_manager_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "score", DataType: schemapb.DataType_Double},
		},
	})
	require.NoError(t, err)
	return helper
}

func managerTestFieldsData(dept string) []*schemapb.FieldData {
	return managerTestFieldsDataWithAgeAndScore(dept, 18, 0)[:2]
}

func managerTestFieldsDataWithAgeAndScore(dept string, age int64, score float64) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId: 100, FieldName: "id", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId: 101, FieldName: "dept", Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
		{
			FieldId: 102, FieldName: "age", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{age}}}}},
		},
		{
			FieldId: 103, FieldName: "score", Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{score}}}}},
		},
	}
}

func TestValidateRowsByParsedExpression(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		},
		{
			FieldId:   101,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "alice"}}}}},
		},
		{
			FieldId:   102,
			FieldName: "age",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18, 19}}}}},
		},
		{
			FieldId:   103,
			FieldName: "tags",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "blue"}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				},
			}}}},
		},
	}

	allowedExpr := `owner == "alice" and age in [18, 19] and array_contains(tags, "red")`
	err = validateRows(context.Background(), fieldsData, helper, 2, allowedExpr, "insert", "check")
	require.NoError(t, err)
	parsedExpr, err := planparserv2.ParseExpr(helper, allowedExpr, nil)
	require.NoError(t, err)
	rows := newRowData(fieldsData, ReferencedFieldIDs(parsedExpr))
	result, err := evalExpr(parsedExpr, rows, 0)
	require.NoError(t, err)
	require.Equal(t, truthTrue, result)
	require.Len(t, rows.termMatchers, 1)

	err = validateRows(context.Background(), fieldsData, helper, 2, `age == 18`, "insert", "check")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "row 1")
}

func TestLiteralMatcherRejectsMalformedExpression(t *testing.T) {
	_, err := newLiteralMatcher(schemapb.DataType_Int64, []*planpb.GenericValue{planparserv2.NewString("not an integer")})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestNullableArrayUsesFieldSpecificValidData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_nullable_array_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, `array_contains(tags, "blue")`, nil)
	require.NoError(t, err)
	red := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}}
	blue := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"blue"}}}}
	for _, storage := range []struct {
		name   string
		values []*schemapb.ScalarField
	}{
		{name: "dense", values: []*schemapb.ScalarField{red, {}, blue}},
		{name: "compact", values: []*schemapb.ScalarField{red, blue}},
	} {
		t.Run(storage.name, func(t *testing.T) {
			fieldData := &schemapb.FieldData{
				FieldId:   101,
				FieldName: "tags",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					ValidData: []bool{true, false, true},
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data:        storage.values,
					}},
				}},
			}
			rows := newRowData([]*schemapb.FieldData{fieldData}, []int64{101})
			for rowIdx, expected := range []truthValue{truthFalse, truthUnknown, truthTrue} {
				actual, err := evalExpr(expr, rows, rowIdx)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
			err := ValidateRowsByPredicate(context.Background(), []*schemapb.FieldData{fieldData}, 3, expr, "upsert", "check")
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}
}

func TestFieldReaderNullableScalarCursor(t *testing.T) {
	column := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_VarChar}
	for _, storage := range []struct {
		name   string
		values []string
	}{
		{name: "compact", values: []string{"first", "third"}},
		{name: "full_size", values: []string{"first", "", "third"}},
	} {
		t.Run(storage.name, func(t *testing.T) {
			rows := newRowData([]*schemapb.FieldData{{
				FieldId:   101,
				FieldName: "owner",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					ValidData: []bool{true, false, true},
					Data:      &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: storage.values}},
				}},
			}}, []int64{101})

			for _, test := range []struct {
				row      int
				expected any
			}{
				{row: 0, expected: "first"},
				{row: 0, expected: "first"},
				{row: 2, expected: "third"},
				{row: 1, expected: nil},
				{row: 2, expected: "third"},
			} {
				value, err := rows.value(column, test.row)
				require.NoError(t, err)
				require.Equal(t, test.expected, value)
			}
		})
	}
}

func TestArrayMatcherSkipsNullElements(t *testing.T) {
	tests := []struct {
		name        string
		dataType    schemapb.DataType
		array       *schemapb.ScalarField
		nullTarget  *planpb.GenericValue
		validTarget *planpb.GenericValue
	}{
		{
			name:     "bool",
			dataType: schemapb.DataType_Bool,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: []bool{false, true}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
		},
		{
			name:     "int",
			dataType: schemapb.DataType_Int32,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{0, 7}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}},
		},
		{
			name:     "long",
			dataType: schemapb.DataType_Int64,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{0, 7}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}},
		},
		{
			name:     "float",
			dataType: schemapb.DataType_Float,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{0, 1.5}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 1.5}},
		},
		{
			name:     "double",
			dataType: schemapb.DataType_Double,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: []float64{0, 2.5}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 2.5}},
		},
		{
			name:     "string",
			dataType: schemapb.DataType_VarChar,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: []string{"", "present"}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: ""}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "present"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := &rowData{}
			newMatcher := func(target *planpb.GenericValue) *arrayLiteralMatcher {
				literals, err := newLiteralMatcher(test.dataType, []*planpb.GenericValue{target})
				require.NoError(t, err)
				return &arrayLiteralMatcher{literalMatcher: literals, op: planpb.JSONContainsExpr_Contains, seen: make([]uint32, len(literals.values))}
			}

			contains, err := newMatcher(test.nullTarget).matches(test.array, rows)
			require.NoError(t, err)
			require.False(t, contains)

			contains, err = newMatcher(test.validTarget).matches(test.array, rows)
			require.NoError(t, err)
			require.True(t, contains)
			require.Len(t, rows.arrayElementLayouts, 1)
		})
	}

	tests[0].array.ValidData = []bool{false}
	literals, err := newLiteralMatcher(tests[0].dataType, []*planpb.GenericValue{tests[0].validTarget})
	require.NoError(t, err)
	matcher := &arrayLiteralMatcher{literalMatcher: literals, op: planpb.JSONContainsExpr_Contains, seen: make([]uint32, len(literals.values))}
	rows := &rowData{}
	_, err = matcher.matches(tests[0].array, rows)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = matcher.matches(tests[0].array, rows)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Len(t, rows.arrayElementLayouts, 1)

	for _, op := range []planpb.JSONContainsExpr_JSONOp{
		planpb.JSONContainsExpr_ContainsAll,
		planpb.JSONContainsExpr_ContainsAny,
	} {
		literals, err := newLiteralMatcher(tests[0].dataType, nil)
		require.NoError(t, err)
		matcher := &arrayLiteralMatcher{literalMatcher: literals, op: op}
		_, err = matcher.matches(tests[0].array, &rowData{})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func TestNegatedPredicateRejectsMismatchedRuntimeTypes(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64,
		}}})
		require.NoError(t, err)
		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "age", Type: schemapb.DataType_Bool,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}},
			}},
		}}
		err = validateRows(context.Background(), fields, helper, 1, "not (age == 7)", "insert", "check")
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("array element", func(t *testing.T) {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		}}})
		require.NoError(t, err)
		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "values", Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_VarChar,
					Data: []*schemapb.ScalarField{{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x"}}},
					}},
				}},
			}},
		}}
		err = validateRows(context.Background(), fields, helper, 1, "not array_contains(values, 7)", "insert", "check")
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("array declared element type", func(t *testing.T) {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int8,
		}}})
		require.NoError(t, err)
		expr, err := planparserv2.ParseExpr(helper, "not array_contains(values, 8)", nil)
		require.NoError(t, err)
		row := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7}}}}

		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "values", Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int32,
					Data:        []*schemapb.ScalarField{row},
				}},
			}},
		}}
		err = ValidateRowsByPredicate(context.Background(), fields, 1, expr, "insert", "check")
		require.ErrorIs(t, err, merr.ErrServiceInternal)

		storageFields := map[int64]StorageFieldData{
			100: &testStorageFieldData{
				data:        []*schemapb.ScalarField{row},
				dataType:    schemapb.DataType_Array,
				elementType: schemapb.DataType_Int32,
			},
		}
		err = ValidateInsertDataByPredicate(context.Background(), storageFields, 1, expr, "import", "check")
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("comparison literal", func(t *testing.T) {
		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "age", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}},
			}},
		}}
		for _, value := range []*planpb.GenericValue{planparserv2.NewString("not-an-int"), planparserv2.NewFloat(1.5)} {
			predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_NotEqual,
				Value:      value,
			}}}
			// Batch preparation must visit comparisons under NOT and on both
			// sides of combined policies, including short-circuited branches.
			predicates := []*planpb.Expr{
				predicate,
				{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
					Op: planpb.UnaryExpr_Not, Child: predicate,
				}}},
				{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
					Op: planpb.BinaryExpr_LogicalOr, Left: predicate, Right: alwaysTruePredicate(),
				}}},
				{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
					Op: planpb.BinaryExpr_LogicalOr, Left: alwaysTruePredicate(), Right: predicate,
				}}},
			}
			storageFields := map[int64]StorageFieldData{
				100: &testStorageFieldData{data: []int64{7}, dataType: schemapb.DataType_Int64},
			}
			for _, expr := range predicates {
				err := ValidateRowsByPredicate(context.Background(), fields, 1, expr, "insert", "check")
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				err = ValidateInsertDataByPredicate(context.Background(), storageFields, 1, expr, "import", "check")
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		}
	})

	t.Run("term literal", func(t *testing.T) {
		predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
			Op: planpb.UnaryExpr_Not,
			Child: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
				Values:     []*planpb.GenericValue{planparserv2.NewFloat(1.5)},
			}}},
		}}}
		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "age", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}},
			}},
		}}
		err := ValidateRowsByPredicate(context.Background(), fields, 1, predicate, "insert", "check")
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("array literal", func(t *testing.T) {
		predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
			Op: planpb.UnaryExpr_Not,
			Child: &planpb.Expr{Expr: &planpb.Expr_JsonContainsExpr{JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
				Op:         planpb.JSONContainsExpr_Contains,
				Elements:   []*planpb.GenericValue{planparserv2.NewFloat(1.5)},
			}}},
		}}}
		fields := []*schemapb.FieldData{{
			FieldId: 100, FieldName: "values", Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int64,
					Data: []*schemapb.ScalarField{{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}},
					}},
				}},
			}},
		}}
		err := ValidateRowsByPredicate(context.Background(), fields, 1, predicate, "insert", "check")
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})
}

func TestArrayContainsOperationsSkipNullElements(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_nullable_array_element_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	tests := []struct {
		expr     string
		expected truthValue
	}{
		{expr: "array_contains(values, 0)", expected: truthFalse},
		{expr: "not array_contains(values, 0)", expected: truthTrue},
		{expr: "array_contains(values, 7)", expected: truthTrue},
		{expr: "array_contains_any(values, [0, 8])", expected: truthFalse},
		{expr: "array_contains_any(values, [0, 7])", expected: truthTrue},
		{expr: "array_contains_all(values, [7, 0])", expected: truthFalse},
		{expr: "array_contains_all(values, [7])", expected: truthTrue},
		{expr: "array_contains_all(values, [7, 7])", expected: truthTrue},
		{expr: "array_contains_all(values, [])", expected: truthTrue},
		{expr: "array_contains_any(values, [])", expected: truthFalse},
	}
	for _, storage := range []struct {
		name   string
		values []int64
	}{
		{name: "dense", values: []int64{0, 7}},
		{name: "compact", values: []int64{7}},
	} {
		t.Run(storage.name, func(t *testing.T) {
			rows := newRowData([]*schemapb.FieldData{{
				FieldId:   100,
				FieldName: "values",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int64,
					Data: []*schemapb.ScalarField{{
						ValidData: []bool{false, true},
						Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: storage.values}},
					}},
				}}}},
			}}, []int64{100})

			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					expr, err := planparserv2.ParseExpr(helper, test.expr, nil)
					require.NoError(t, err)
					actual, err := evalExpr(expr, rows, 0)
					require.NoError(t, err)
					require.Equal(t, test.expected, actual)
					if contains := expr.GetJsonContainsExpr(); contains != nil {
						require.NotNil(t, rows.arrayMatchers[contains])
					}
				})
			}
			require.Len(t, rows.arrayElementLayouts, 1)
		})
	}
}

func TestArrayContainsAllMatcherDoesNotLeakAcrossRows(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
	}}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, "array_contains_all(values, [7, 8])", nil)
	require.NoError(t, err)
	rows := newRowData([]*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "values",
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_Int64,
			Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7, 8}}}},
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}},
			},
		}}}},
	}}, []int64{100})

	result, err := evalExpr(expr, rows, 0)
	require.NoError(t, err)
	require.Equal(t, truthTrue, result)
	require.NotNil(t, rows.arrayMatchers[expr.GetJsonContainsExpr()])
	result, err = evalExpr(expr, rows, 1)
	require.NoError(t, err)
	require.Equal(t, truthFalse, result)
}

func TestValidateRowsInternalRowShapeErrorsAreSystemErrors(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	assertSystemError := func(fieldsData []*schemapb.FieldData, rowNum int, expr string) {
		t.Helper()
		err := validateRows(context.Background(), fieldsData, helper, rowNum, expr, "insert", "check")
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.NotErrorIs(t, err, merr.ErrParameterInvalid)
	}

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "id",
		Type:      schemapb.DataType_Int64,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
	}}, 1, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "age",
		Type:      schemapb.DataType_Int64,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}}},
	}}, 1, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "age",
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			ValidData: []bool{true, true},
			Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}},
		}},
	}}, 2, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   102,
		FieldName: "tags",
		Type:      schemapb.DataType_Array,
		ValidData: []bool{true, false, false},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_VarChar,
			Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"ignored"}}}},
			},
		}}}},
	}}, 3, `array_contains(tags, "red")`)
}

func TestValidateRowsByPredicateValidatesReferencedFieldRowCount(t *testing.T) {
	helper := newManagerTestSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `dept == "sales"`, nil)
	require.NoError(t, err)

	twoRows := []*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "dept",
		Type:      schemapb.DataType_VarChar,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"sales", "engineering"}}}}},
	}}

	for _, test := range []struct {
		name       string
		fieldsData []*schemapb.FieldData
		rowNum     int
	}{
		{name: "negative", fieldsData: twoRows, rowNum: -1},
		{name: "zero with data", fieldsData: twoRows, rowNum: 0},
		{name: "trailing row", fieldsData: twoRows, rowNum: 1},
		{name: "count exceeds data", fieldsData: twoRows, rowNum: 3},
		{name: "missing data", rowNum: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRowsByPredicate(context.Background(), test.fieldsData, test.rowNum, expr, "insert", "check")
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}

	require.NoError(t, ValidateRowsByPredicate(context.Background(), nil, 0, expr, "insert", "check"))
	require.ErrorIs(t, ValidateRowsByPredicate(context.Background(), twoRows, 0, alwaysFalsePredicate(), "insert", "check"), merr.ErrServiceInternal)
	require.NoError(t, ValidateRowsByPredicate(context.Background(), []*schemapb.FieldData{
		managerTestFieldsData("sales")[1],
		{
			FieldId:   200,
			FieldName: "location",
			Type:      schemapb.DataType_Geometry,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryWktData{
				GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT (1 2)", "POINT (3 4)"}},
			}}},
		},
	}, 1, expr, "insert", "check"))
}

func TestValidateRowsRejectsUnsupportedComparisonOperator(t *testing.T) {
	helper := newManagerTestSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `age > 17`, nil)
	require.NoError(t, err)
	err = ValidateRowsByPredicate(
		context.Background(),
		managerTestFieldsDataWithAgeAndScore("sales", 18, 0),
		1,
		expr,
		"insert",
		"check",
	)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestValidateRowsStopsOnCanceledContext(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, `age == 18`, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = ValidateRowsByPredicate(ctx, []*schemapb.FieldData{{
		FieldId: 100,
		Type:    schemapb.DataType_Int64,
		Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}}}},
	}}, 1, expr, "insert", "check")
	require.ErrorIs(t, err, context.Canceled)
}

func TestValidateInsertDataByPredicateNarrowIntegers(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "tiny", DataType: schemapb.DataType_Int8},
		{FieldID: 102, Name: "small", DataType: schemapb.DataType_Int16},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, `tiny == 7 and small == 300`, nil)
	require.NoError(t, err)
	data := map[int64]StorageFieldData{
		101: &testStorageFieldData{data: []int8{7}, dataType: schemapb.DataType_Int8},
		102: &testStorageFieldData{data: []int16{300}, dataType: schemapb.DataType_Int16},
	}

	require.NoError(t, ValidateInsertDataByPredicate(context.Background(), data, 1, expr, "import", "check"))
	data[101].(*testStorageFieldData).data.([]int8)[0] = 8
	require.ErrorIs(t, ValidateInsertDataByPredicate(context.Background(), data, 1, expr, "import", "check"), merr.ErrPrivilegeNotPermitted)

	compact := map[int64]StorageFieldData{
		101: &testStorageFieldData{data: []int8{7, 8}, dataType: schemapb.DataType_Int8, validData: []bool{true, false, true}},
	}
	rows := newInsertRowData(compact, []int64{101})
	for row, expected := range []any{int8(7), nil, int8(8)} {
		actual, err := rows.value(&planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int8}, row)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}
