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

package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func parseGrep(t *testing.T, expression string) *planpb.GrepExpr {
	t.Helper()
	expr, err := ParseExpr(newTestSchemaHelper(t), expression, nil)
	require.NoError(t, err, expression)
	node := expr.GetGrepExpr()
	require.NotNil(t, node, expression)
	return node
}

func TestExpr_Grep(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("normalized specification", func(t *testing.T) {
		node := parseGrep(t, `grep(VarCharField, "-e timeout")`)
		assert.Equal(t, schemapb.DataType_VarChar, node.GetColumnInfo().GetDataType())
		assert.Equal(t, []string{"timeout"}, node.GetPatterns())
		assert.Equal(t, uint32(grep.PatternRegex), node.GetPatternKind())
		assert.Equal(t, uint32(grep.CaseSensitive), node.GetCaseMode())
		assert.Equal(t, uint32(grep.BoundaryNone), node.GetBoundary())
		assert.False(t, node.GetSelectNone())

		node = parseGrep(t, `grep(VarCharField, "-F -i -w -e a.b -e 'c d'")`)
		assert.Equal(t, []string{"a.b", "c d"}, node.GetPatterns())
		assert.Equal(t, uint32(grep.PatternLiteral), node.GetPatternKind())
		assert.Equal(t, uint32(grep.CaseInsensitive), node.GetCaseMode())
		assert.Equal(t, uint32(grep.BoundaryWord), node.GetBoundary())

		node = parseGrep(t, `grep(VarCharField, "-x -e ok")`)
		assert.Equal(t, uint32(grep.BoundaryLine), node.GetBoundary())
	})

	t.Run("text fields are accepted", func(t *testing.T) {
		node := parseGrep(t, `grep(TextField, "-e timeout")`)
		assert.Equal(t, schemapb.DataType_Text, node.GetColumnInfo().GetDataType())
		// Ordinary operands still reject Text.
		_, err := ParseExpr(helper, `TextField == "a"`, nil)
		assert.Error(t, err)
	})

	t.Run("the function name is a case-insensitive soft keyword", func(t *testing.T) {
		for _, expression := range []string{
			`GREP(VarCharField, "-e a")`,
			`Grep(VarCharField, "-e a")`,
		} {
			parseGrep(t, expression)
		}
	})

	t.Run("raw strings keep regex backslashes", func(t *testing.T) {
		node := parseGrep(t, `grep(VarCharField, r"-e '\d+ ms'")`)
		assert.Equal(t, []string{`\d+ ms`}, node.GetPatterns())
		// In an ordinary literal the string layer consumes one backslash level.
		node = parseGrep(t, `grep(VarCharField, "-e '\\d+ ms'")`)
		assert.Equal(t, []string{`\d+ ms`}, node.GetPatterns())
	})

	t.Run("composes with other predicates", func(t *testing.T) {
		for _, expression := range []string{
			`Int64Field > 0 and grep(VarCharField, "-e timeout")`,
			`grep(VarCharField, "-e timeout") or grep(TextField, "-F -e OOM")`,
			`not grep(VarCharField, "-e timeout")`,
			`Int64Field in [1, 2] and (grep(VarCharField, "-e a") or not grep(VarCharField, "-e b"))`,
		} {
			expr, err := ParseExpr(helper, expression, nil)
			require.NoError(t, err, expression)
			require.NotNil(t, expr, expression)
		}
	})

	t.Run("composes with template predicates", func(t *testing.T) {
		values := map[string]*schemapb.TemplateValue{
			"min": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 3}},
		}
		expr, err := ParseExpr(helper, `Int64Field > {min} and grep(VarCharField, "-e timeout")`, values)
		require.NoError(t, err)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		// The optimizer may reorder the two items.
		assert.True(t, binary.GetLeft().GetGrepExpr() != nil || binary.GetRight().GetGrepExpr() != nil)
	})

	t.Run("-m 0 selects no row", func(t *testing.T) {
		node := parseGrep(t, `grep(VarCharField, "-m 0 -e timeout")`)
		assert.True(t, node.GetSelectNone())
		// The last -m wins; a later positive cap is an output option again.
		_, err := ParseExpr(helper, `grep(VarCharField, "-m 0 -m 2 -e timeout")`, nil)
		assert.Error(t, err)
	})

	t.Run("invalid", func(t *testing.T) {
		for _, expression := range []string{
			// arity
			`grep(VarCharField)`,
			`grep(VarCharField, "-e a", "-e b")`,
			`grep()`,
			// field
			`grep(Int64Field, "-e a")`,
			`grep(JSONField, "-e a")`,
			`grep(JSONField["a"], "-e a")`,
			`grep(StringArrayField, "-e a")`,
			`grep(NoSuchField, "-e a")`,
			`grep("VarCharField", "-e a")`,
			`grep(VarCharField + 1, "-e a")`,
			// arguments
			`grep(VarCharField, 1)`,
			`grep(VarCharField, VarCharField)`,
			`grep(VarCharField, {args})`,
			`grep(VarCharField, "")`,
			`grep(VarCharField, "timeout")`,
			`grep(VarCharField, "-e")`,
			`grep(VarCharField, "--unknown -e a")`,
			`grep(VarCharField, "-v -e a")`,
			// options that only shape returned details
			`grep(VarCharField, "-A 1 -e a")`,
			`grep(VarCharField, "-B 2 -e a")`,
			`grep(VarCharField, "-C 1 -e a")`,
			`grep(VarCharField, "-m 3 -e a")`,
		} {
			_, err := ParseExpr(helper, expression, map[string]*schemapb.TemplateValue{
				"args": {Val: &schemapb.TemplateValue_StringVal{StringVal: "-e a"}},
			})
			assert.Error(t, err, expression)
		}
	})

	t.Run("zero context is not an output option", func(t *testing.T) {
		parseGrep(t, `grep(VarCharField, "-A 0 -e a")`)
	})
}

func TestExpr_GrepIdentifierStillUsable(t *testing.T) {
	// grep is a soft keyword: only the call shape is special.
	schema := newTestSchema(true)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 9000, Name: "grep", DataType: schemapb.DataType_Int64,
	})
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := ParseExpr(helper, `grep > 3`, nil)
	require.NoError(t, err)
	assert.NotNil(t, expr.GetUnaryRangeExpr())
}

func TestExplain_Grep(t *testing.T) {
	expr, err := ParseExpr(newTestSchemaHelper(t), `grep(VarCharField, "-e timeout")`, nil)
	require.NoError(t, err)
	ShowExpr(expr)
}

func TestSelectItem_GrepMatches(t *testing.T) {
	item, err := ParseSelectItem(`GREP_MATCHES(VarCharField, "-i -C1 -e timeout") AS matches`)
	require.NoError(t, err)
	assert.Equal(t, SelectItemGrepMatches, item.Kind)
	assert.Equal(t, "VarCharField", item.GrepField)
	assert.Equal(t, "-i -C1 -e timeout", item.GrepArguments)
	assert.Equal(t, "matches", item.Alias)
	assert.Equal(t, `GREP_MATCHES(VarCharField, "-i -C1 -e timeout")`, item.Source)

	// The name is matched in any casing, the alias is optional, and a raw
	// string keeps its backslashes.
	item, err = ParseSelectItem(`grep_matches(VarCharField, r"-e '\d+ ms'")`)
	require.NoError(t, err)
	assert.Equal(t, SelectItemGrepMatches, item.Kind)
	assert.Equal(t, `-e '\d+ ms'`, item.GrepArguments)
	assert.Empty(t, item.Alias)

	item, err = ParseSelectItem(`Grep_Matches(VarCharField, "-e '\\d+'")`)
	require.NoError(t, err)
	assert.Equal(t, `-e '\d+'`, item.GrepArguments)

	for _, invalid := range []string{
		`GREP_MATCHES(VarCharField)`,
		`GREP_MATCHES(VarCharField, "-e a", "-e b")`,
		`GREP_MATCHES("-e a", VarCharField)`,
		`GREP_MATCHES(JSONField["a"], "-e a")`,
		`GREP_MATCHES(VarCharField, 1)`,
		`GREP_MATCHES(VarCharField, {args})`,
		`GREP_MATCHES(VarCharField, VarCharField)`,
		`other_function(VarCharField, "-e a")`,
	} {
		_, err := ParseSelectItem(invalid)
		assert.Error(t, err, invalid)
	}

	// grep_matches stays usable as a field name and as an alias.
	item, err = ParseSelectItem(`grep_matches`)
	require.NoError(t, err)
	assert.Equal(t, SelectItemField, item.Kind)
	item, err = ParseSelectItem(`VarCharField AS grep_matches`)
	require.NoError(t, err)
	assert.Equal(t, "grep_matches", item.Alias)
}

func TestNewGrepMatches(t *testing.T) {
	varchar := &schemapb.FieldSchema{FieldID: 105, Name: "message", DataType: schemapb.DataType_VarChar, Nullable: true}
	item, err := NewGrepMatches(varchar, `-F -i -w -m 3 -B1 -A2 -e timeout -e refused`)
	require.NoError(t, err)
	assert.Equal(t, int64(105), item.GetSpec().GetColumnInfo().GetFieldId())
	assert.True(t, item.GetSpec().GetColumnInfo().GetNullable())
	assert.Equal(t, []string{"timeout", "refused"}, item.GetSpec().GetPatterns())
	assert.Equal(t, uint32(1), item.GetSpec().GetPatternKind())
	assert.Equal(t, uint32(1), item.GetSpec().GetCaseMode())
	assert.Equal(t, uint32(1), item.GetSpec().GetBoundary())
	assert.False(t, item.GetSpec().GetSelectNone())
	assert.True(t, item.GetHasMaxCount())
	assert.Equal(t, uint64(3), item.GetMaxCount())
	assert.Equal(t, uint64(1), item.GetBeforeContext())
	assert.Equal(t, uint64(2), item.GetAfterContext())

	item, err = NewGrepMatches(&schemapb.FieldSchema{FieldID: 106, Name: "body", DataType: schemapb.DataType_Text}, `-e x -m 0`)
	require.NoError(t, err)
	assert.True(t, item.GetSpec().GetSelectNone())
	assert.False(t, item.GetHasMaxCount())

	_, err = NewGrepMatches(&schemapb.FieldSchema{FieldID: 107, Name: "n", DataType: schemapb.DataType_Int64}, `-e x`)
	assert.Error(t, err)
	_, err = NewGrepMatches(varchar, `--no-such-option x`)
	assert.Error(t, err)
	_, err = NewGrepMatches(varchar, ``)
	assert.Error(t, err)
}

func TestAndGrepPredicate(t *testing.T) {
	spec := &planpb.GrepExpr{Patterns: []string{"x"}}
	alone := AndGrepPredicate(nil, spec)
	assert.Same(t, spec, alone.GetGrepExpr())

	alwaysTrue := &planpb.Expr{Expr: &planpb.Expr_AlwaysTrueExpr{AlwaysTrueExpr: &planpb.AlwaysTrueExpr{}}}
	assert.Same(t, spec, AndGrepPredicate(alwaysTrue, spec).GetGrepExpr())

	base := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{}}}
	joined := AndGrepPredicate(base, spec)
	assert.Equal(t, planpb.BinaryExpr_LogicalAnd, joined.GetBinaryExpr().GetOp())
	assert.Same(t, base, joined.GetBinaryExpr().GetLeft())
	assert.Same(t, spec, joined.GetBinaryExpr().GetRight().GetGrepExpr())

	// element_filter and random_sample stay on top; grep joins the
	// document-level predicate.
	element := &planpb.Expr{Expr: &planpb.Expr_ElementFilterExpr{ElementFilterExpr: &planpb.ElementFilterExpr{StructName: "s"}}}
	out := AndGrepPredicate(element, spec)
	assert.Same(t, element, out)
	assert.Same(t, spec, out.GetElementFilterExpr().GetPredicate().GetGrepExpr())
	out = AndGrepPredicate(element, &planpb.GrepExpr{Patterns: []string{"y"}})
	assert.Equal(t, planpb.BinaryExpr_LogicalAnd, out.GetElementFilterExpr().GetPredicate().GetBinaryExpr().GetOp())

	sample := &planpb.Expr{Expr: &planpb.Expr_RandomSampleExpr{RandomSampleExpr: &planpb.RandomSampleExpr{SampleFactor: 0.5, Predicate: base}}}
	out = AndGrepPredicate(sample, spec)
	assert.Same(t, sample, out)
	assert.Same(t, spec, out.GetRandomSampleExpr().GetPredicate().GetBinaryExpr().GetRight().GetGrepExpr())
}
