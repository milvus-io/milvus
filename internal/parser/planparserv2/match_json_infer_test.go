package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReduceJSONMatchElementType(t *testing.T) {
	cases := []struct {
		name    string
		input   []schemapb.DataType
		want    schemapb.DataType
		wantErr bool
	}{
		{"empty", nil, schemapb.DataType_None, true},
		{"single_int", []schemapb.DataType{schemapb.DataType_Int64}, schemapb.DataType_Int64, false},
		{"single_str", []schemapb.DataType{schemapb.DataType_VarChar}, schemapb.DataType_VarChar, false},
		{"single_string_alias", []schemapb.DataType{schemapb.DataType_String}, schemapb.DataType_VarChar, false},
		{"single_bool", []schemapb.DataType{schemapb.DataType_Bool}, schemapb.DataType_Bool, false},
		{"single_double", []schemapb.DataType{schemapb.DataType_Double}, schemapb.DataType_Double, false},
		{"int_and_double_widen", []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double}, schemapb.DataType_Double, false},
		{"all_strings", []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_VarChar}, schemapb.DataType_VarChar, false},
		{"all_bools", []schemapb.DataType{schemapb.DataType_Bool, schemapb.DataType_Bool}, schemapb.DataType_Bool, false},
		{"mix_string_numeric", []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Int64}, schemapb.DataType_None, true},
		{"mix_bool_numeric", []schemapb.DataType{schemapb.DataType_Bool, schemapb.DataType_Int64}, schemapb.DataType_None, true},
		{"mix_bool_string", []schemapb.DataType{schemapb.DataType_Bool, schemapb.DataType_VarChar}, schemapb.DataType_None, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := reduceJSONMatchElementType(c.input)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

// TestExpr_Match_JSON exercises MATCH_* on a JSON field. The supported shape
// is: first arg is a JSON path that resolves to an array-of-scalars leaf,
// and the predicate uses bare $ for the scalar element. $[ "..." ] accessors
// are not supported inside the predicate.
func TestExpr_Match_JSON(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	valid := []string{
		// string family
		`MATCH_ANY(JSONField["path"], $ == "x")`,
		`MATCH_ANY(JSONField["path"]["color"], $ == "x" || $ == "y")`,
		`MATCH_ALL(JSONField["path"], $ != "")`,
		`MATCH_ANY(JSONField["path"], $ in ["a", "b", "c"])`,
		// int family
		`MATCH_ANY(JSONField, $ > 1)`,
		`MATCH_ANY(JSONField["path"]["n"], $ > 5)`,
		`MATCH_ALL(JSONField["path"], $ > 5 && $ < 10)`,
		`MATCH_ANY(JSONField["path"], $ in [1, 2, 3])`,
		// int + float widens within numeric family
		`MATCH_ANY(JSONField["path"], $ > 5 || $ < 10.5)`,
		`MATCH_ANY(JSONField["path"], $ > 1.5 && $ < 10)`,
		// bool family
		`MATCH_ANY(JSONField["path"], $ == true)`,
		`MATCH_ANY(JSONField["path"], $ == true || $ == false)`,
		// compound range predicates
		`MATCH_ANY(JSONField["path"], $ > 1 && $ < 10)`,
		`MATCH_ANY(JSONField["path"], $ > "a" && $ < "z")`,
		// MATCH_LEAST / MOST / EXACT with threshold
		`MATCH_LEAST(JSONField["path"], $ == "x", threshold=2)`,
		`MATCH_MOST(JSONField["path"], $ > 1, threshold=3)`,
		`MATCH_EXACT(JSONField["path"], $ == true, threshold=1)`,
		// case insensitive
		`match_any(JSONField["path"], $ == "x")`,
	}
	for _, e := range valid {
		assertValidExpr(t, helper, e)
	}

	invalid := []string{
		// cross-family: string + numeric
		`MATCH_ANY(JSONField, $ == "x" || $ > 5)`,
		`MATCH_ANY(JSONField["path"], $ == "x" || $ > 5)`,
		// cross-family: bool + numeric
		`MATCH_ANY(JSONField["path"], $ == true || $ == 1)`,
		// cross-family: bool + string
		`MATCH_ANY(JSONField["path"], $ == true || $ == "x")`,
		// cross-family inside a Term literal list
		`MATCH_ANY(JSONField["path"], $ in [1, "a"])`,
	}
	for _, e := range invalid {
		assertInvalidExpr(t, helper, e)
	}
}

// TestGenericValueScalarType verifies the GenericValue oneof -> scalar DataType
// mapping used while collecting literal types for a JSON MATCH_* predicate.
func TestGenericValueScalarType(t *testing.T) {
	assert.Equal(t, schemapb.DataType_None, genericValueScalarType(nil))
	assert.Equal(t, schemapb.DataType_Bool, genericValueScalarType(NewBool(true)))
	assert.Equal(t, schemapb.DataType_Int64, genericValueScalarType(NewInt(7)))
	assert.Equal(t, schemapb.DataType_Double, genericValueScalarType(NewFloat(1.5)))
	assert.Equal(t, schemapb.DataType_VarChar, genericValueScalarType(NewString("x")))
}

// TestValidateJSONMatchElementType_TemplateDeferred verifies that a predicate
// flagged as a template is not validated at visitor time: validation is
// deferred until FillExpressionValue substitutes concrete template values
// (see validateFilledJSONMatchExprs), so it must not error here.
func TestValidateJSONMatchElementType_TemplateDeferred(t *testing.T) {
	mctx := &jsonMatchContext{fieldID: 123, jsonPath: []string{"a"}}

	// nil context -> no-op, never errors regardless of the predicate.
	assert.NoError(t, validateJSONMatchElementType(&planpb.Expr{IsTemplate: true}, nil))

	// template predicate under a real context -> deferred, no error yet.
	assert.NoError(t, validateJSONMatchElementType(&planpb.Expr{IsTemplate: true}, mctx))
}

// TestCollectJSONMatchLiteralTypes_AnchorsOnPath confirms that only literals
// compared against an element-level JSON column at the context's field/path
// contribute to the collected type set.
func TestCollectJSONMatchLiteralTypes_AnchorsOnPath(t *testing.T) {
	mctx := &jsonMatchContext{fieldID: 100, jsonPath: []string{"a"}}

	elemCol := &planpb.ColumnInfo{
		FieldId:        100,
		DataType:       schemapb.DataType_JSON,
		NestedPath:     []string{"a"},
		IsElementLevel: true,
	}
	// A column that does NOT reference the element accessor (different field).
	otherCol := &planpb.ColumnInfo{
		FieldId:        200,
		DataType:       schemapb.DataType_JSON,
		NestedPath:     []string{"a"},
		IsElementLevel: true,
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op: planpb.BinaryExpr_LogicalOr,
				Left: &planpb.Expr{
					Expr: &planpb.Expr_UnaryRangeExpr{
						UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: elemCol,
							Op:         planpb.OpType_GreaterThan,
							Value:      NewInt(1),
						},
					},
				},
				Right: &planpb.Expr{
					Expr: &planpb.Expr_UnaryRangeExpr{
						UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: otherCol,
							Op:         planpb.OpType_Equal,
							Value:      NewString("x"),
						},
					},
				},
			},
		},
	}

	var types []schemapb.DataType
	collectJSONMatchLiteralTypes(expr, mctx, &types)
	// Only the element-level column at field 100 / path ["a"] contributes; the
	// string literal compared against the unrelated column is ignored.
	assert.Equal(t, []schemapb.DataType{schemapb.DataType_Int64}, types)

	reduced, err := reduceJSONMatchElementType(types)
	assert.NoError(t, err)
	assert.Equal(t, schemapb.DataType_Int64, reduced)
}
