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
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestParseSelectItem_FieldsAndAliases(t *testing.T) {
	for _, tc := range []struct {
		in     string
		source string
		alias  string
	}{
		{"a", "a", ""},
		{"  a  ", "a", ""},
		{"a AS b", "a", "b"},
		{"a as b", "a", "b"},
		{"a\tAS\nb", "a", "b"},
		{"_x1 AS _y2", "_x1", "_y2"},
		// AS is a contextual keyword: usable as a field name and as an alias.
		{"as", "as", ""},
		{"AS", "AS", ""},
		{"a AS as", "a", "as"},
		{"as AS as", "as", "as"},
		{"AS AS AS", "AS", "AS"},
		// Other contextual keywords stay usable on both sides.
		{"iso AS interval", "iso", "interval"},
		{"threshold", "threshold", ""},
		// Aggregate function names are ordinary identifiers.
		{"count", "count", ""},
		{"sum AS max", "sum", "max"},
		// Dynamic field, JSON paths and struct forms.
		{"$meta", "$meta", ""},
		{`$meta["color"]`, `$meta["color"]`, ""},
		{`$meta["color"] AS c`, `$meta["color"]`, "c"},
		{`a["x"]["y"]`, `a["x"]["y"]`, ""},
		{`a["x"][0] AS first`, `a["x"][0]`, "first"},
		{"s[sub]", "s[sub]", ""},
		{"s[0][sub] AS v", "s[0][sub]", "v"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			item, err := parseSelectItemUncached(tc.in)
			require.NoError(t, err)
			require.Equal(t, SelectItemField, item.Kind)
			require.Equal(t, tc.source, item.Source)
			require.Equal(t, tc.alias, item.Alias)
		})
	}
}

func TestParseSelectItem_Star(t *testing.T) {
	for _, in := range []string{"*", " * "} {
		item, err := parseSelectItemUncached(in)
		require.NoError(t, err)
		require.Equal(t, SelectItemStar, item.Kind)
		require.Equal(t, "*", item.Source)
	}
	_, err := parseSelectItemUncached("* AS all")
	require.Error(t, err)
}

func TestParseSelectItem_SyntaxErrors(t *testing.T) {
	for _, in := range []string{
		"",
		"   ",
		"a b",         // AS is mandatory
		"a As b",      // only "as" and "AS" are the keyword; "As" is an identifier
		"a AS",        // alias missing
		"AS b",        // "AS" is the field, "b" is a stray token
		"a AS b c",    // trailing token
		"a AS 1",      // alias must be an identifier
		"a AS $score", // alias cannot start with "$"
		"a AS $meta",
		`a AS "b"`,
		"a AS b AS c",
		"a, b",
		"$foo",
		"$Score",
		"$score()",
		"$score(a, b)",
		`$score("a")`,
		"$score(-1)",
		"count(*) total",
		"count(**)",
		"count(1a)",
	} {
		t.Run(in, func(t *testing.T) {
			_, err := parseSelectItemUncached(in)
			require.Error(t, err)
		})
	}
}

func TestParseSelectItem_ExpressionsRejected(t *testing.T) {
	for _, in := range []string{
		"price * 0.8 AS d",
		"a + b",
		"(a)",
		"1",
		"1.5 AS x",
		`"text"`,
		"true",
		"a > 1",
		"a == 1 AS flag",
		"not a",
		"a in [1, 2]",
		"array_length(a)",
		"foo(a)",
		"foo(*)",
		"foo()",
		"sum(a, b)",
		"sum(a + 1)",
		`sum(a["x"])`,
		"sum(sum(a))",
		"{tmpl}",
		"$[sub]",
	} {
		t.Run(in, func(t *testing.T) {
			_, err := parseSelectItemUncached(in)
			require.Error(t, err)
			require.Contains(t, err.Error(), unsupportedSelectExprMsg)
		})
	}
}

func TestParseSelectItem_Aggregates(t *testing.T) {
	for _, tc := range []struct {
		in     string
		source string
		op     string
		arg    string
		alias  string
	}{
		{"count(*)", "count(*)", "count", "*", ""},
		{"COUNT(*)", "COUNT(*)", "count", "*", ""},
		{"Count ( * )", "Count ( * )", "count", "*", ""},
		{"count()", "count()", "count", "", ""},
		{"count(a)", "count(a)", "count", "a", ""},
		{"sum(price)", "sum(price)", "sum", "price", ""},
		{"SUM( price )", "SUM( price )", "sum", "price", ""},
		{"avg(price) AS mean", "avg(price)", "avg", "price", "mean"},
		{"min(a) as lo", "min(a)", "min", "a", "lo"},
		{"Max(a)", "Max(a)", "max", "a", ""},
		{"count(*) AS n", "count(*)", "count", "*", "n"},
		// Parsed here; the binder rejects everything but count for "*".
		{"sum(*)", "sum(*)", "sum", "*", ""},
		// The argument may be a contextual keyword or an aggregate name.
		{"sum(as)", "sum(as)", "sum", "as", ""},
		{"max(iso)", "max(iso)", "max", "iso", ""},
		{"min(count)", "min(count)", "min", "count", ""},
		{"count(count) AS count", "count(count)", "count", "count", "count"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			item, err := parseSelectItemUncached(tc.in)
			require.NoError(t, err)
			require.Equal(t, SelectItemAggregate, item.Kind)
			require.Equal(t, tc.source, item.Source)
			require.Equal(t, tc.op, item.AggregateOp)
			require.Equal(t, tc.arg, item.AggregateArg)
			require.Equal(t, tc.alias, item.Alias)
		})
	}
}

// The grammar replaces this pattern (internal/agg/aggregate.go). Every string
// the pattern accepts whose argument is a possible field name, "*", or empty
// must parse to the same operator and argument, and keep its text as the
// output name.
var legacyAggregationPattern = regexp.MustCompile(`(?i)^(sum|count|avg|min|max)\s*\(\s*([\w\*]*)\s*\)$`)

func TestParseSelectItem_AggregateMatchesLegacyPattern(t *testing.T) {
	ops := []string{"count", "COUNT", "Count", "sum", "SUM", "avg", "Avg", "min", "MIN", "max", "mAx"}
	args := []string{"", "*", "a", "price", "_x", "A1", "count", "as", "iso", "threshold", "match_all"}
	gaps := []string{"", " ", "  ", "\t", "\n", " \t\n"}
	checked := 0
	for _, op := range ops {
		for _, arg := range args {
			for _, g1 := range gaps {
				for _, g2 := range gaps {
					in := op + g1 + "(" + g2 + arg + g1 + ")"
					m := legacyAggregationPattern.FindStringSubmatch(in)
					require.NotNil(t, m, in)
					item, err := parseSelectItemUncached(in)
					require.NoError(t, err, in)
					require.Equal(t, SelectItemAggregate, item.Kind, in)
					require.Equal(t, strings.ToLower(m[1]), item.AggregateOp, in)
					require.Equal(t, strings.TrimSpace(m[2]), item.AggregateArg, in)
					require.Equal(t, in, item.Source, in)
					checked++
				}
			}
		}
	}
	require.Equal(t, len(ops)*len(args)*len(gaps)*len(gaps), checked)

	// Accepted by the pattern, never a valid aggregate: the argument is not a
	// field name, so the request failed at field lookup. It now fails here.
	for _, in := range []string{"count(1)", "sum(1a)", "max(**)", "min(a*)", "avg(*a)"} {
		require.NotNil(t, legacyAggregationPattern.FindStringSubmatch(in), in)
		_, err := parseSelectItemUncached(in)
		require.Error(t, err, in)
	}
}

func TestParseSelectItem_VirtualColumns(t *testing.T) {
	for _, tc := range []struct {
		in      string
		source  string
		name    string
		argKind VirtualColumnArgKind
		field   string
		index   int64
		alias   string
	}{
		{"$score", "$score", VirtualColumnScore, VirtualColumnNoArg, "", 0, ""},
		{"$score AS s", "$score", VirtualColumnScore, VirtualColumnNoArg, "", 0, "s"},
		{"$distance", "$distance", VirtualColumnDistance, VirtualColumnNoArg, "", 0, ""},
		{"$distance AS distance", "$distance", VirtualColumnDistance, VirtualColumnNoArg, "", 0, "distance"},
		{"$seg_id", "$seg_id", VirtualColumnSegID, VirtualColumnNoArg, "", 0, ""},
		{"$seg_offset AS off", "$seg_offset", VirtualColumnSegOffset, VirtualColumnNoArg, "", 0, "off"},
		{"$score(dense)", "$score(dense)", VirtualColumnScore, VirtualColumnArgField, "dense", 0, ""},
		{"$score( dense ) AS ds", "$score( dense )", VirtualColumnScore, VirtualColumnArgField, "dense", 0, "ds"},
		{"$score(0)", "$score(0)", VirtualColumnScore, VirtualColumnArgIndex, "", 0, ""},
		{"$distance(1) AS d1", "$distance(1)", VirtualColumnDistance, VirtualColumnArgIndex, "", 1, "d1"},
		{"$distance(sparse)", "$distance(sparse)", VirtualColumnDistance, VirtualColumnArgField, "sparse", 0, ""},
		{"$rank(12)", "$rank(12)", VirtualColumnRank, VirtualColumnArgIndex, "", 12, ""},
		{"$rank(vec) AS r", "$rank(vec)", VirtualColumnRank, VirtualColumnArgField, "vec", 0, "r"},
		{"$rank(as)", "$rank(as)", VirtualColumnRank, VirtualColumnArgField, "as", 0, ""},
	} {
		t.Run(tc.in, func(t *testing.T) {
			item, err := parseSelectItemUncached(tc.in)
			require.NoError(t, err)
			require.Equal(t, SelectItemVirtualColumn, item.Kind)
			require.Equal(t, tc.source, item.Source)
			require.Equal(t, tc.name, item.VirtualColumn)
			require.Equal(t, tc.argKind, item.VirtualArgKind)
			require.Equal(t, tc.field, item.VirtualArgField)
			require.Equal(t, tc.index, item.VirtualArgIndex)
			require.Equal(t, tc.alias, item.Alias)
		})
	}

	for in, want := range map[string]string{
		"$rank":                       "requires a sub-request argument",
		"$rank AS r":                  "requires a sub-request argument",
		"$seg_id(a)":                  "does not take an argument",
		"$seg_offset(0)":              "does not take an argument",
		"$score(01)":                  "decimal position",
		"$score(0x1)":                 "decimal position",
		"$distance(0b1)":              "decimal position",
		"$rank(99999999999999999999)": "decimal position",
	} {
		t.Run(in, func(t *testing.T) {
			_, err := parseSelectItemUncached(in)
			require.Error(t, err)
			require.Contains(t, err.Error(), want)
		})
	}
}

func TestVirtualColumnRejectedInFilter(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	require.NoError(t, err)
	for _, expr := range []string{
		"$score > 0.5",
		"Int64Field > 1 and $distance < 0.3",
		"$seg_id == 1",
		"$seg_offset in [1, 2]",
		"$rank(0) < 3",
		"$score(FloatVectorField) > 0",
	} {
		t.Run(expr, func(t *testing.T) {
			_, err := ParseExpr(helper, expr, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot be used in a filter expression")
		})
	}
}

// A filter and an output_fields item with the same text parse under different
// entry rules and must not share a cache entry in either order.
func TestParseSelectItem_CacheIsSeparateFromFilters(t *testing.T) {
	configureExprCache(t, "true", "64")
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	require.NoError(t, err)

	// Valid as an item, invalid as a filter (a filter must be boolean), and
	// the other way round.
	for range 2 {
		item, err := ParseSelectItem("Int64Field AS x")
		require.NoError(t, err)
		require.Equal(t, "x", item.Alias)
		_, err = ParseExpr(helper, "Int64Field AS x", nil)
		require.Error(t, err)

		_, err = ParseExpr(helper, "Int64Field > 1", nil)
		require.NoError(t, err)
		_, err = ParseSelectItem("Int64Field > 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), unsupportedSelectExprMsg)

		// Same text, valid under both rules.
		item, err = ParseSelectItem("BoolField")
		require.NoError(t, err)
		require.Equal(t, SelectItemField, item.Kind)
		_, err = handleInternal("BoolField")
		require.NoError(t, err)
	}
	require.Equal(t, 6, exprCache().cache.Len())
}

func TestParseSelectItem_CachedResultEqualsUncached(t *testing.T) {
	configureExprCache(t, "true", "64")
	for _, in := range []string{"a AS b", "count(*) AS n", "$rank(1) AS r", "*", "a b", "a + 1"} {
		want, wantErr := parseSelectItemUncached(in)
		for range 3 {
			got, err := ParseSelectItem(in)
			require.Equal(t, want, got, in)
			if wantErr == nil {
				require.NoError(t, err, in)
			} else {
				require.EqualError(t, err, wantErr.Error(), in)
			}
		}
	}
}

func TestParseSelectItem_CacheDisabled(t *testing.T) {
	configureExprCache(t, "false", "64")
	item, err := ParseSelectItem("a AS b")
	require.NoError(t, err)
	require.Equal(t, "b", item.Alias)
	_, err = ParseSelectItem("a b")
	require.Error(t, err)
}

func BenchmarkParseSelectItemUncached(b *testing.B) {
	for _, in := range []string{"title", "title AS t", "count(*)", "$score AS s"} {
		b.Run(in, func(b *testing.B) {
			for b.Loop() {
				if _, err := parseSelectItemUncached(in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
