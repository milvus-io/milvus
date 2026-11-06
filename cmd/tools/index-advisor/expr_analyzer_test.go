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

package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// testSchemaHelper returns the fixture's products collection plus fields the
// fixture does not exercise (nullable, geometry, text match).
func testSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	schema := &schemapb.CollectionSchema{Name: "products", EnableDynamicField: true, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
		{FieldID: 102, Name: "city", DataType: schemapb.DataType_VarChar},
		{FieldID: 103, Name: "sku", DataType: schemapb.DataType_VarChar},
		{FieldID: 104, Name: "price", DataType: schemapb.DataType_Int64},
		{FieldID: 105, Name: "rating", DataType: schemapb.DataType_Double},
		{FieldID: 106, Name: "in_stock", DataType: schemapb.DataType_Bool},
		{FieldID: 107, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		{FieldID: 108, Name: "meta", DataType: schemapb.DataType_JSON},
		{FieldID: 110, Name: "note", DataType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 112, Name: "loc", DataType: schemapb.DataType_Geometry},
		{FieldID: 113, Name: "doc", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
			{Key: "enable_analyzer", Value: "true"}, {Key: "enable_match", Value: "true"},
		}},
		{FieldID: 109, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// describePredicate renders a predicate compactly for table comparisons.
func describePredicate(p predicate) string {
	s := fmt.Sprintf("%s %s", p.column, p.op)
	if p.valueKind != "" {
		s += " " + p.valueKind
	}
	if p.literalLen != 0 {
		s += fmt.Sprintf(" lit=%d", p.literalLen)
	}
	for flag, set := range map[string]bool{" pk": p.column.isPrimaryKey, " dynamic": p.column.isDynamic} {
		if set {
			s += flag
		}
	}
	return s
}

func TestAnalyzeExpr(t *testing.T) {
	helper := testSchemaHelper(t)
	cases := []struct {
		expr        string
		fingerprint string
		predicates  []string
	}{
		{"price >= 100 and price < 200", "price < ? and price >= ?", []string{"price range int"}},
		{"price > 1\nand price < 10", "price < ? and price > ?", []string{"price range int"}},
		{"100 <= price < 200", "? <= price < ?", []string{"price range int"}},
		{"price > {p}", "price > ?", []string{"price range"}},
		{`city == "SF"`, "city == ?", []string{"city eq string"}},
		{`city != "SF"`, "city != ?", []string{"city ne string"}},
		{`city in ["SF", "LA"]`, "city in [?]", []string{"city in string"}},
		{"city in {cs}", "city in [?]", []string{"city in"}},
		{`city not in ["SF"]`, "not (city in [?])", []string{"city in string"}},
		{`not (city == "SF")`, "not (city == ?)", []string{"city eq string"}},
		{`sku like "SKU-1%"`, "sku like ?%", []string{"sku like_prefix string lit=5"}},
		{`sku like "%123%"`, "sku like %?%", []string{"sku like_infix string lit=3"}},
		{`sku like "%123"`, "sku like %?", []string{"sku like_suffix string lit=3"}},
		{`sku like "S_K%U"`, "sku like ?", []string{"sku like_pattern string lit=1"}},
		{`sku =~ "^S.*"`, "sku =~ ?", []string{"sku regex string"}},
		{`array_contains(tags, "a")`, "array_contains(tags, ?)", []string{"tags contains string"}},
		{`array_contains_any(tags, ["a", "b"])`, "array_contains_any(tags, ?)", []string{"tags contains string"}},
		{`json_contains(meta["t"], "x")`, `json_contains(meta["t"], ?)`, []string{`meta["t"] contains array_string`}},
		{`meta["brand"] == "x"`, `meta["brand"] == ?`, []string{`meta["brand"] eq string`}},
		{`meta["a"]["b"] == 1`, `meta["a"]["b"] == ?`, []string{`meta["a"]["b"] eq int`}},
		{`meta["brand"] like "x%"`, `meta["brand"] like ?%`, []string{`meta["brand"] like_prefix string lit=1`}},
		{`exists meta["brand"]`, `exists meta["brand"]`, []string{`meta["brand"] exists`}},
		{`color == "red"`, `$meta["color"] == ?`, []string{`$meta["color"] eq string dynamic`}},
		// With dynamic fields enabled, any unknown name is a dynamic key, never an error.
		{"nope == 1", `$meta["nope"] == ?`, []string{`$meta["nope"] eq int dynamic`}},
		{"id in [1, 2, 3]", "id in [?]", []string{"id in int pk"}},
		{"in_stock == true and rating > 4.5", "in_stock == ? and rating > ?", []string{"in_stock eq bool", "rating range float"}},
		// AND/OR operands are sorted, so operand order does not split a shape.
		{`price > 1 or city == "SF"`, "city == ? or price > ?", []string{"city eq string", "price range int"}},
		{`city == "SF" or price > 1`, "city == ? or price > ?", []string{"city eq string", "price range int"}},
		{`(city == "SF" or city == "LA") and price > 1`, "(city == ? or city == ?) and price > ?", []string{"city eq string", "price range int"}},
		{"price in {ps} and city == {c}", "city == ? and price in [?]", []string{"city eq", "price in"}},
		{"price + 1 > 5", "price + ? > ?", []string{"price not_indexable"}},
		{"price - 1 >= 5 or price * 2 <= 5 or price / 2 != 1 or price % 2 == 0", "price % ? == ? or price * ? <= ? or price - ? >= ? or price / ? != ?", []string{"price not_indexable"}},
		{"price <= rating", "price <= rating", []string{"price not_indexable", "rating not_indexable"}},
		{`meta["x"] == [1, 2] and meta["y"] == 1.5`, `meta["x"] == ? and meta["y"] == ?`, []string{`meta["x"] eq array`, `meta["y"] eq float`}},
		{`(city == "SF" and price > 1) or not (in_stock == true or rating < 2)`, "(city == ? and price > ?) or not (in_stock == ? or rating < ?)", []string{"city eq string", "in_stock eq bool", "price range int", "rating range float"}},
		{"array_length(tags) == 2", "array_length(tags) == ?", []string{"tags not_indexable"}},
		{"note is null", "note is null", []string{"note is_null"}},
		{"note is not null", "note is not null", []string{"note is_null"}},
		{`text_match(doc, "hello")`, "text_match(doc, ?)", []string{"doc text_match string"}},
		{`phrase_match(doc, "a b")`, "phrase_match(doc, ?)", []string{"doc text_match string"}},
		{`st_contains(loc, "POINT (1 1)")`, "st_contains(loc, ?)", []string{"loc geo"}},
		{"random_sample(0.1)", "random_sample(?)", nil},
		{`city == "SF" and random_sample(0.5)`, "city == ? and random_sample(?)", []string{"city eq string"}},
		{"true", "true", nil},
		// Function calls parse generically; Milvus checks the name at execution.
		{"my_udf(price, 3)", "my_udf(price, ?)", nil},
	}
	for _, c := range cases {
		got, err := analyzeExpr(helper, c.expr)
		require.NoError(t, err, c.expr)
		assert.Equal(t, c.fingerprint, got.fingerprint, c.expr)
		var preds []string
		for _, p := range got.predicates {
			preds = append(preds, describePredicate(p))
		}
		assert.Equal(t, c.predicates, preds, c.expr)
	}
}

func TestAnalyzeExprErrors(t *testing.T) {
	helper := testSchemaHelper(t)
	for _, expr := range []string{"price >", "price > > 1", `city > 1 and`} {
		_, err := analyzeExpr(helper, expr)
		assert.True(t, errors.Is(err, merr.ErrQueryPlan), expr)
	}
}

// A comparison on one element of an ARRAY field counts for the field, as an
// operator no index serves; json_contains with elements of several types gets
// no cast.
func TestAnalyzeArrayElementAndMixedContains(t *testing.T) {
	helper := testSchemaHelper(t)
	got, err := analyzeExpr(helper, `tags[0] == "a" and array_contains(tags, "b")`)
	require.NoError(t, err)
	assert.Equal(t, `array_contains(tags, ?) and tags[0] == ?`, got.fingerprint)
	assert.Equal(t, []string{"tags array_element", "tags contains string"}, lo.Map(got.predicates, func(p predicate, _ int) string { return describePredicate(p) }))

	got, err = analyzeExpr(helper, `json_contains_any(meta["t"], [1, "a"])`)
	require.NoError(t, err)
	assert.Equal(t, []string{`meta["t"] contains mixed`}, lo.Map(got.predicates, func(p predicate, _ int) string { return describePredicate(p) }))
}

func TestLikeLiteralLen(t *testing.T) {
	helper := testSchemaHelper(t)
	for expr, want := range map[string]int{
		`sku like "%abc%"`:     3,
		`sku like "a%bcde_f"`:  1, // pattern: the shortest fixed part
		`sku like "ab_cd%ef"`:  2,
		`sku like "50\\%off%"`: 6, // prefix: the parser unescapes the operand to 50%off
		`sku like "a\\_bc_d%"`: 1, // pattern: an escaped _ is a literal character, so the parts are a_bc and d
		`sku like "%a\\%b%"`:   3, // infix: unescaped to a%b
		`sku like "日本語%"`:      3, // runes, not bytes
		"sku like {pat}":       literalLenUnknown,
	} {
		got, err := analyzeExpr(helper, expr)
		require.NoError(t, err, expr)
		require.Len(t, got.predicates, 1, expr)
		assert.Equal(t, want, got.predicates[0].literalLen, expr)
	}
}

// TestFingerprintsCarryNoLiterals checks the redaction boundary on the fixture:
// no quoted string or number from a logged expression survives into its
// fingerprint.
func TestFingerprintsCarryNoLiterals(t *testing.T) {
	helper := testSchemaHelper(t)
	var records []accessRecord
	_, err := readRecordsFile("testdata/access_log.txt", defaultFormatterSet(t), skipCounts{}, appendTo(&records))
	require.NoError(t, err)

	literal := regexp.MustCompile(`"[^"]*"|\b\d+(\.\d+)?\b`)
	parsed := 0
	for _, r := range records {
		expr, ok := r.expr()
		if status, _ := r.status(); !ok || expr == "" || status != "Successful" || r.method() == "HybridSearch" {
			continue
		}
		got, err := analyzeExpr(helper, expr)
		require.NoError(t, err, expr)
		parsed++
		for _, lit := range literal.FindAllString(expr, -1) {
			if strings.HasPrefix(lit, `"`) && strings.Contains(got.fingerprint, "[\"") {
				// JSON path keys such as meta["brand"] are schema identifiers, not values.
				if strings.Contains(got.fingerprint, "["+lit+"]") {
					continue
				}
			}
			assert.NotContains(t, got.fingerprint, lit, "fingerprint of %q leaks %s", expr, lit)
		}
	}
	assert.Equal(t, 63, parsed)
}
