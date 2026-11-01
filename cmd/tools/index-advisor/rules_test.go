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
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// testField builds the field context of identifier in testSchemaHelper's
// collection, as if each expression had been logged once.
func testField(t *testing.T, identifier string, exprs ...string) *fieldContext {
	info := newCollectionInfo("default", "t", testSchemaHelper(t))
	col := mustColumn(t, info, identifier)
	wl := newFieldWorkload()
	for _, e := range exprs {
		parsed, err := analyzeExpr(info.helper, e)
		require.NoError(t, err, e)
		preds := lo.Filter(parsed.predicates, func(p predicate, _ int) bool { return p.column.String() == col.String() })
		require.NotEmpty(t, preds, "%s does not filter on %s", e, identifier)
		wl.add(preds, parsed.fingerprint, 0, false)
	}
	return newFieldContext(info, col, wl, nil)
}

func applyRule(r rule, fc *fieldContext) (verdict, advice) {
	var adv advice
	return r.apply(fc, &adv), adv
}

func int64p(n int64) *int64 { return &n }

func TestPrimaryKeyRule(t *testing.T) {
	v, adv := applyRule(primaryKeyRule{}, testField(t, "id", "id in [1, 2]"))
	assert.Equal(t, stop, v)
	assert.Contains(t, adv.notes[0], "Primary key")

	v, _ = applyRule(primaryKeyRule{}, testField(t, "price", "price > 1"))
	assert.Equal(t, proceed, v)
}

func TestUnfilteredColumnRule(t *testing.T) {
	v, adv := applyRule(unfilteredColumnRule{}, testField(t, "price"))
	assert.Equal(t, stop, v)
	assert.Contains(t, adv.notes[0], "No analyzed request filters on this column")

	v, _ = applyRule(unfilteredColumnRule{}, testField(t, "price", "price > 1"))
	assert.Equal(t, proceed, v)
}

func TestJSONCastTypeRule(t *testing.T) {
	for _, exprs := range [][]string{
		{`meta["k"] == 1`, `meta["k"] == "x"`},
		{`meta["k"] == {v}`},
		{`exists meta["k"]`},
		{`json_contains(meta["k"], "x")`, `meta["k"] == "x"`},
	} {
		v, adv := applyRule(jsonCastTypeRule{}, testField(t, `meta["k"]`, exprs...))
		assert.Equal(t, stop, v, exprs)
		assert.Contains(t, adv.notes[0], "json_cast_type cannot be inferred")
	}

	fc := testField(t, `meta["k"]`, `meta["k"] == 1`, `meta["k"] > 2.5`)
	assert.Equal(t, "DOUBLE", fc.castType, "ints and floats share DOUBLE")
	v, _ := applyRule(jsonCastTypeRule{}, fc)
	assert.Equal(t, proceed, v)
	v, _ = applyRule(jsonCastTypeRule{}, testField(t, "price", "price == {v}"))
	assert.Equal(t, proceed, v, "only JSON paths need a cast type")
}

func TestIndexableOperatorRule(t *testing.T) {
	v, adv := applyRule(indexableOperatorRule{}, testField(t, "rating", "price > rating", "rating * 2 > 3"))
	assert.Equal(t, stop, v)
	assert.Equal(t, "No scalar index serves these predicates: field-to-field comparisons and arithmetic filters.", adv.notes[0])

	_, adv = applyRule(indexableOperatorRule{}, testField(t, "doc", `text_match(doc, "x")`))
	assert.Contains(t, adv.notes[0], "enable_match")

	v, _ = applyRule(indexableOperatorRule{}, testField(t, "rating", "price > rating", "rating > 3"))
	assert.Equal(t, proceed, v, "one served operator is enough")
}

func TestBaselineAutoIndexRule(t *testing.T) {
	v, adv := applyRule(baselineAutoIndexRule{}, testField(t, "city", `city == "a"`, `city like "a%"`, "price > 1 and city in {x}"))
	assert.Equal(t, proceed, v)
	require.Len(t, adv.candidates, 1)
	c := adv.candidates[0]
	assert.Equal(t, indexAuto, c.IndexType)
	assert.Equal(t, roleBaseline, c.Role)
	assert.Equal(t, indexHybrid, c.resolvesTo)
	assert.Equal(t, []string{"eq", "in", "like_prefix"}, c.Serves)
	assert.Equal(t, 3, c.Predicates)
	assert.Equal(t, []string{
		"this column is filtered by 3 analyzed requests",
		"AUTOINDEX lets Milvus pick the index type; with the default scalarAutoIndex.params.build it builds HYBRID for VarChar",
	}, c.Evidence)
	assert.Equal(t, docScalarIndex, c.Source)

	_, adv = applyRule(baselineAutoIndexRule{}, testField(t, `meta["b"]`, `meta["b"] == true`))
	assert.Equal(t, map[string]string{"json_cast_type": "BOOL", "json_path": `meta["b"]`}, adv.candidates[0].Params)
	assert.Equal(t, "meta", adv.candidates[0].FieldName)
	assert.Equal(t, "AUTOINDEX lets Milvus pick the index type; with the default scalarAutoIndex.params.build it builds HYBRID for JSON path, or INVERTED on a cluster whose scalar index engine version is below 4",
		adv.candidates[0].Evidence[1], "older clusters build INVERTED for AUTOINDEX on a JSON path")

	// A dynamic-field path is indexed under its top-level key, with the path as
	// $meta[...] so that keys which are not identifiers parse too.
	for identifier, want := range map[string]candidate{
		`color`:                  {FieldName: "color", Params: map[string]string{"json_cast_type": "VARCHAR", "json_path": `$meta["color"]`}},
		`dyn["a"]`:               {FieldName: "dyn", Params: map[string]string{"json_cast_type": "DOUBLE", "json_path": `$meta["dyn"]["a"]`}},
		`$meta["my key"]`:        {FieldName: "my key", Params: map[string]string{"json_cast_type": "VARCHAR", "json_path": `$meta["my key"]`}},
		`$meta["my key"]["sub"]`: {FieldName: "my key", Params: map[string]string{"json_cast_type": "VARCHAR", "json_path": `$meta["my key"]["sub"]`}},
	} {
		literal := `"red"`
		if want.Params["json_cast_type"] == "DOUBLE" {
			literal = "1"
		}
		_, adv = applyRule(baselineAutoIndexRule{}, testField(t, identifier, identifier+" == "+literal))
		require.Len(t, adv.candidates, 1, identifier)
		assert.Equal(t, want.FieldName, adv.candidates[0].FieldName, identifier)
		assert.Equal(t, want.Params, adv.candidates[0].Params, identifier)
	}

	whole := &fieldContext{col: column{field: "meta", dataType: schemapb.DataType_JSON}, kind: kindOther, workload: newFieldWorkload()}
	v, adv = applyRule(baselineAutoIndexRule{}, whole)
	assert.Equal(t, proceed, v)
	assert.Empty(t, adv.candidates)
	assert.Equal(t, "This tool proposes no index for JSON columns.", adv.notes[0])

	v, adv = applyRule(baselineAutoIndexRule{}, testField(t, `meta["tags"]`, `json_contains(meta["tags"], "x")`))
	assert.Equal(t, proceed, v, "an alternative may still serve the column")
	assert.Empty(t, adv.candidates)
	assert.Equal(t, "Milvus does not accept AUTOINDEX on a JSON path with json_cast_type ARRAY_VARCHAR.", adv.notes[0])
}

func TestJSONArrayRule(t *testing.T) {
	_, adv := applyRule(jsonArrayRule{}, testField(t, `meta["tags"]`, `json_contains(meta["tags"], "x")`, `json_contains_any(meta["tags"], ["y", "z"])`))
	require.Len(t, adv.candidates, 1)
	c := adv.candidates[0]
	assert.Equal(t, indexInverted, c.IndexType)
	assert.Equal(t, roleAlternative, c.Role)
	assert.Equal(t, "meta", c.FieldName)
	assert.Equal(t, map[string]string{"json_cast_type": "ARRAY_VARCHAR", "json_path": `meta["tags"]`}, c.Params)
	assert.Equal(t, 2, c.Predicates)
	assert.Equal(t, docJSONIndex, c.Source)
	assert.Contains(t, c.Evidence[0], "used by 2 requests")

	_, adv = applyRule(jsonArrayRule{}, testField(t, `meta["ids"]`, `json_contains(meta["ids"], 3)`))
	assert.Equal(t, "ARRAY_DOUBLE", adv.candidates[0].Params["json_cast_type"])

	_, adv = applyRule(jsonArrayRule{}, testField(t, `meta["k"]`, `meta["k"] == "x"`))
	assert.Empty(t, adv.candidates, "a scalar path is not an array")
	_, adv = applyRule(jsonArrayRule{}, testField(t, "tags", `array_contains(tags, "x")`))
	assert.Empty(t, adv.candidates, "an ARRAY field is indexed by element without a cast")
}

func TestJSONShreddingRule(t *testing.T) {
	path := testField(t, `meta["k"]`, `meta["k"] == "x"`)
	_, adv := applyRule(jsonShreddingRule{}, path)
	assert.Empty(t, adv.notes, "no candidate and no existing index")

	adv = advice{candidates: []candidate{{IndexType: indexAuto}}}
	jsonShreddingRule{}.apply(path, &adv)
	require.Len(t, adv.notes, 1)
	assert.Contains(t, adv.notes[0], "common.usingJSONShreddingForQuery")

	path.existing = []indexSnapshot{{IndexName: "k", IndexType: indexInverted}}
	_, adv = applyRule(jsonShreddingRule{}, path)
	assert.Len(t, adv.notes, 1, "an existing JSON path index is affected too")

	_, adv = applyRule(jsonShreddingRule{}, testField(t, "city", `city == "x"`))
	assert.Empty(t, adv.notes, "only JSON paths")

	element := testField(t, `meta["a"][0]`, `meta["a"][0] == "x"`)
	adv = advice{candidates: []candidate{{IndexType: indexAuto}}}
	jsonShreddingRule{}.apply(element, &adv)
	assert.Empty(t, adv.notes, "Milvus does not use JSON shredding for a path with an array position")
}

// An alternative the column already has is neither proposed nor noted. On a
// JSON path, an index with another json_cast_type does not count.
func TestAlternativesSkipExistingType(t *testing.T) {
	city := testField(t, "city", `city == "a"`)
	city.existing = []indexSnapshot{{IndexName: "c", IndexType: "bitmap"}}
	_, adv := applyRule(bitmapRule{}, city)
	assert.Empty(t, adv.candidates)
	assert.Empty(t, adv.notes, "no request for distinct_count when BITMAP exists")

	sku := testField(t, "sku", `sku like "%a%"`)
	sku.existing = []indexSnapshot{{IndexName: "s", IndexType: indexNgram}}
	_, adv = applyRule(ngramRule{}, sku)
	assert.Empty(t, adv.notes)

	tags := testField(t, `meta["tags"]`, `json_contains(meta["tags"], "x")`)
	tags.existing = []indexSnapshot{{IndexName: "t", IndexType: indexInverted, Params: map[string]any{"json_cast_type": "VARCHAR"}}}
	_, adv = applyRule(jsonArrayRule{}, tags)
	assert.Len(t, adv.candidates, 1, "an INVERTED index with a scalar cast does not serve json_contains")
}

// BITMAP is not proposed again where AUTOINDEX already builds it.
func TestBitmapRuleAfterBitmapBaseline(t *testing.T) {
	fc := testField(t, "tags", `array_contains(tags, "a")`)
	baseline := candidate{IndexType: indexAuto, resolvesTo: indexBitmap}
	for _, distinct := range []*int64{nil, int64p(3)} {
		fc.stats.distinctCount = distinct
		adv := advice{candidates: []candidate{baseline}}
		bitmapRule{}.apply(fc, &adv)
		assert.Equal(t, []candidate{baseline}, adv.candidates)
		assert.Empty(t, adv.notes)
	}
}

func TestBitmapRule(t *testing.T) {
	fc := testField(t, "city", `city == "a"`, `city in ["b"]`)
	_, adv := applyRule(bitmapRule{}, fc)
	assert.Empty(t, adv.candidates)
	assert.Contains(t, adv.notes[0], "distinct_count is unknown", "no statistics, no evidence")

	fc.stats.distinctCount = int64p(bitmapMaxDistinct)
	_, adv = applyRule(bitmapRule{}, fc)
	assert.Empty(t, adv.candidates)
	assert.Empty(t, adv.notes, "high cardinality rules BITMAP out silently")

	fc.stats.distinctCount = int64p(4)
	v, adv := applyRule(bitmapRule{}, fc)
	assert.Equal(t, proceed, v)
	require.Len(t, adv.candidates, 1)
	assert.Equal(t, indexBitmap, adv.candidates[0].IndexType)
	assert.Equal(t, roleAlternative, adv.candidates[0].Role)
	assert.Equal(t, []string{"eq", "in"}, adv.candidates[0].Serves)
	assert.Contains(t, adv.candidates[0].Evidence[0], "distinct_count 4 is below 500")
	assert.Contains(t, adv.candidates[0].Evidence[1], "bitmap_cardinality_limit")

	fc = testField(t, "price", "price > 1")
	fc.stats.distinctCount = int64p(4)
	_, adv = applyRule(bitmapRule{}, fc)
	assert.Empty(t, adv.candidates, "BITMAP is not proposed for range filters")
	assert.Empty(t, adv.notes)

	fc = testField(t, "rating", "rating == 1.5")
	fc.stats.distinctCount = int64p(4)
	_, adv = applyRule(bitmapRule{}, fc)
	assert.Empty(t, adv.candidates, "Milvus rejects BITMAP on floats")
}

func TestNgramRule(t *testing.T) {
	_, adv := applyRule(ngramRule{}, testField(t, "sku", `sku like "%ab%"`, `sku like "%abc"`, `sku like "xy%"`))
	require.Len(t, adv.candidates, 1)
	c := adv.candidates[0]
	assert.Equal(t, indexNgram, c.IndexType)
	assert.Equal(t, map[string]string{"min_gram": "2", "max_gram": "3"}, c.Params)
	assert.Equal(t, []string{"like_infix", "like_suffix"}, c.Serves)
	assert.Equal(t, 2, c.Predicates, "the prefix LIKE is not served by NGRAM here")
	assert.Equal(t, "every fixed part of the observed LIKE filters has at least 2 characters (shortest: 2)", c.Evidence[1])

	_, adv = applyRule(ngramRule{}, testField(t, "sku", `sku like "ab_c%de"`))
	assert.Empty(t, adv.candidates, "NGRAM needs every fixed part of a pattern to reach min_gram")
	assert.Contains(t, adv.notes[0], "a fixed part of 1 characters")

	for _, unknown := range []string{"sku like {p}", `sku =~ "a.*"`} {
		_, adv = applyRule(ngramRule{}, testField(t, "sku", `sku like "%ab%"`, unknown))
		assert.Empty(t, adv.candidates, unknown)
		assert.Contains(t, adv.notes[0], "literal lengths are unknown", unknown)
	}

	_, adv = applyRule(ngramRule{}, testField(t, "sku", `sku like "ab%"`))
	assert.Empty(t, adv.candidates, "prefix LIKE alone is no evidence for NGRAM")
	assert.Empty(t, adv.notes)

	_, adv = applyRule(ngramRule{}, testField(t, `meta["s"]`, `meta["s"] like "%ab%"`))
	require.Len(t, adv.candidates, 1)
	assert.Equal(t, "VARCHAR", adv.candidates[0].Params["json_cast_type"])
}

func TestExistingIndexRule(t *testing.T) {
	fc := testField(t, "city", `city == "a"`)
	adv := advice{candidates: []candidate{
		{IndexType: indexAuto, resolvesTo: indexHybrid},
		{IndexType: indexBitmap},
		{IndexType: indexNgram},
	}}
	fc.existing = []indexSnapshot{{IndexName: "c", IndexType: "bitmap"}}
	assert.Equal(t, proceed, existingIndexRule{}.apply(fc, &adv))
	assert.Equal(t, []string{indexAuto, indexNgram}, lo.Map(adv.candidates, func(c candidate, _ int) string { return c.IndexType }), "index types compare case-insensitively")

	fc.existing = []indexSnapshot{{IndexName: "untyped"}}
	existingIndexRule{}.apply(fc, &adv)
	assert.Len(t, adv.candidates, 2, "an index with no recorded type matches no candidate")

	fc.existing = []indexSnapshot{{IndexName: "c", IndexType: indexHybrid}}
	existingIndexRule{}.apply(fc, &adv)
	assert.Equal(t, []string{indexNgram}, lo.Map(adv.candidates, func(c candidate, _ int) string { return c.IndexType }), "AUTOINDEX would build the existing HYBRID")

	// On a JSON path, an index with another json_cast_type serves other filters.
	arrayCandidate := candidate{IndexType: indexInverted, Params: map[string]string{"json_cast_type": "ARRAY_VARCHAR"}}
	for cast, kept := range map[any]bool{"varchar": true, "array_varchar": false, nil: false} {
		idx := indexSnapshot{IndexName: "i", IndexType: indexInverted}
		if cast != nil {
			idx.Params = map[string]any{"json_cast_type": cast}
		}
		fc.existing = []indexSnapshot{idx}
		adv = advice{candidates: []candidate{arrayCandidate}}
		existingIndexRule{}.apply(fc, &adv)
		assert.Equal(t, kept, len(adv.candidates) == 1, "existing cast %v", cast)
	}
}

// TestEvidenceRulesAreAccepted guards the evidence rules: each may only
// propose an index type Milvus accepts on every column kind it lists, and must
// cite a source.
func TestEvidenceRulesAreAccepted(t *testing.T) {
	sample := map[columnKind]column{
		kindInt:      {dataType: schemapb.DataType_Int64},
		kindString:   {dataType: schemapb.DataType_VarChar},
		kindArray:    {dataType: schemapb.DataType_Array, elementType: schemapb.DataType_VarChar},
		kindJSONPath: {dataType: schemapb.DataType_JSON, jsonPath: []string{"a"}},
	}
	found := 0
	for _, r := range defaultRuleEngine().rules {
		er, ok := r.(evidenceRule)
		if !ok {
			continue
		}
		found++
		alt := er.alternative()
		assert.NotEmpty(t, alt.source, "%s needs a source", alt.indexType)
		assert.NotEmpty(t, alt.ops, "%s serves no operator", alt.indexType)
		for _, kind := range alt.kinds {
			col, ok := sample[kind]
			if assert.True(t, ok, "no sample column for %s", kind) {
				assert.True(t, indexAccepts(alt.indexType, col, "VARCHAR"), "%s on %s, which Milvus rejects", alt.indexType, kind)
			}
		}
	}
	assert.Equal(t, 3, found, "BITMAP, NGRAM and INVERTED for JSON arrays")
}

// Clusters whose scalar index engine version is below 4 reject an explicit
// BITMAP, HYBRID or STL_SORT on a JSON path, and this tool cannot see the
// version, so no evidence rule may propose one of them on a JSON path.
func TestEvidenceRulesAvoidVersionGatedJSONPathTypes(t *testing.T) {
	for _, r := range defaultRuleEngine().rules {
		er, ok := r.(evidenceRule)
		if !ok {
			continue
		}
		alt := er.alternative()
		if lo.Contains(alt.kinds, kindJSONPath) {
			assert.False(t, jsonPathVersionGated(alt.indexType), "%s on a JSON path needs a scalar index engine version this tool cannot check", alt.indexType)
		}
	}
}

func TestPlural(t *testing.T) {
	assert.Equal(t, "1 analyzed request", plural(1, "analyzed request"))
	assert.Equal(t, "0 requests", plural(0, "request"))
	assert.Equal(t, "16 requests", plural(16, "request"))
}
