package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestReduceJsonMatchElementType(t *testing.T) {
	cases := []struct {
		name    string
		input   []schemapb.DataType
		want    schemapb.DataType
		wantErr bool
	}{
		{"empty", nil, schemapb.DataType_None, true},
		{"single_int", []schemapb.DataType{schemapb.DataType_Int64}, schemapb.DataType_Int64, false},
		{"single_str", []schemapb.DataType{schemapb.DataType_VarChar}, schemapb.DataType_VarChar, false},
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
			got, err := reduceJsonMatchElementType(c.input)
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
