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
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/sbbf"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// bloomBytesTemplate builds an SBBF blob from int64 members and wraps it as a
// raw bytes template value — mimicking a client that pre-built the filter and
// ships the blob (the only supported wire form; the proxy never builds).
func bloomBytesTemplate(t *testing.T, fpr float64, members ...int64) (*schemapb.TemplateValue, []byte) {
	b, err := sbbf.NewBuilder(uint64(len(members)), fpr)
	require.NoError(t, err)
	for _, v := range members {
		b.AddInt64(v)
	}
	blob := b.Marshal()
	return bytesTemplate(blob), blob
}

// bloomBytesTemplateStr is the VARCHAR variant of bloomBytesTemplate.
func bloomBytesTemplateStr(t *testing.T, fpr float64, members ...string) (*schemapb.TemplateValue, []byte) {
	b, err := sbbf.NewBuilder(uint64(len(members)), fpr)
	require.NoError(t, err)
	for _, v := range members {
		b.AddString(v)
	}
	blob := b.Marshal()
	return bytesTemplate(blob), blob
}

func bytesTemplate(blob []byte) *schemapb.TemplateValue {
	return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}}
}

// requireBloomFilterExpr asserts that the expression node is a materialized
// BloomFilterExpr whose blob parses through the sbbf package, and returns the
// parsed read-only filter for probing.
func requireBloomFilterExpr(t *testing.T, expr *planpb.Expr) *sbbf.Filter {
	bfe := expr.GetBloomFilterExpr()
	require.NotNil(t, bfe, "expected a BloomFilterExpr node, got: %s", expr.String())
	require.NotNil(t, bfe.GetColumnInfo())
	filter, err := sbbf.Parse(bfe.GetFilterBlob())
	require.NoError(t, err)
	return filter
}

func TestExpr_BloomMatch(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("int64 pre-built blob is embedded verbatim", func(t *testing.T) {
		tv, blob := bloomBytesTemplate(t, 0.001, 1, 5, 9, -42)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)

		filter := requireBloomFilterExpr(t, expr)
		assert.Equal(t, schemapb.DataType_Int64, expr.GetBloomFilterExpr().GetColumnInfo().GetDataType())
		// embedded verbatim: byte-identical to the client-built blob, no rebuild.
		assert.Equal(t, blob, expr.GetBloomFilterExpr().GetFilterBlob())
		assert.Equal(t, uint64(4), filter.NDeclared())
		assert.Equal(t, 0.001, filter.FPRDeclared())
		// no false negatives: every member must probe true.
		for _, v := range []int64{1, 5, 9, -42} {
			assert.True(t, filter.TestInt64(v), "member %d must probe true", v)
		}
	})

	t.Run("int32 field probes widened int64 blob", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001, 7, 8)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(Int32Field, {bf})", mv)
		require.NoError(t, err)
		filter := requireBloomFilterExpr(t, expr)
		assert.True(t, filter.TestInt64(7))
		assert.True(t, filter.TestInt64(8))
	})

	t.Run("varchar pre-built blob", func(t *testing.T) {
		tv, _ := bloomBytesTemplateStr(t, 0.01, "alice", "bob", "小明")
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(VarCharField, {bf})", mv)
		require.NoError(t, err)
		filter := requireBloomFilterExpr(t, expr)
		assert.Equal(t, 0.01, filter.FPRDeclared())
		for _, s := range []string{"alice", "bob", "小明"} {
			assert.True(t, filter.TestString(s), "member %q must probe true", s)
		}
	})

	t.Run("not bloom_match", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001, 11, 12)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "not bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary)
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		filter := requireBloomFilterExpr(t, unary.GetChild())
		assert.True(t, filter.TestInt64(11))
	})

	t.Run("combined with other predicates", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001, 1)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "Int64Field > 0 and bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		// the expression rewriter may reorder AND operands; find the bloom side.
		bloomSide := binary.GetRight()
		if bloomSide.GetBloomFilterExpr() == nil {
			bloomSide = binary.GetLeft()
		}
		filter := requireBloomFilterExpr(t, bloomSide)
		assert.True(t, filter.TestInt64(1))
	})

	t.Run("search plan carries BloomFilterExpr", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001, 21, 22)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		plan, err := CreateSearchPlan(helper, "bloom_match(Int64Field, {bf})", "FloatVectorField", &planpb.QueryInfo{
			Topk:       10,
			MetricType: "L2",
		}, mv, nil)
		require.NoError(t, err)
		filter := requireBloomFilterExpr(t, plan.GetVectorAnns().GetPredicates())
		assert.True(t, filter.TestInt64(21))
		assert.True(t, filter.TestInt64(22))
	})

	t.Run("empty-set blob is allowed and matches nothing", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		filter := requireBloomFilterExpr(t, expr)
		assert.False(t, filter.TestInt64(1))
	})

	t.Run("json path", func(t *testing.T) {
		tv, blob := bloomBytesTemplate(t, 0.001, 100, 200)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, `bloom_match(JSONField["user_id"], {bf})`, mv)
		require.NoError(t, err)
		bfe := expr.GetBloomFilterExpr()
		require.NotNil(t, bfe)
		assert.Equal(t, schemapb.DataType_JSON, bfe.GetColumnInfo().GetDataType())
		assert.Equal(t, []string{"user_id"}, bfe.GetColumnInfo().GetNestedPath())
		assert.Equal(t, blob, bfe.GetFilterBlob())
	})

	t.Run("json nested path", func(t *testing.T) {
		tv, _ := bloomBytesTemplateStr(t, 0.01, "alice")
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, `bloom_match(JSONField["a"]["b"], {bf})`, mv)
		require.NoError(t, err)
		bfe := expr.GetBloomFilterExpr()
		require.NotNil(t, bfe)
		assert.Equal(t, []string{"a", "b"}, bfe.GetColumnInfo().GetNestedPath())
	})

	t.Run("whole json field (root path)", func(t *testing.T) {
		tv, _ := bloomBytesTemplate(t, 0.001, 1)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(JSONField, {bf})", mv)
		require.NoError(t, err)
		bfe := expr.GetBloomFilterExpr()
		require.NotNil(t, bfe)
		assert.Empty(t, bfe.GetColumnInfo().GetNestedPath())
	})

	t.Run("bloom_match composes with random_sample", func(t *testing.T) {
		// The random_sample wrapper absorbs the left predicate; it must carry
		// the predicate's IsTemplate flag or FillExpressionValue never runs
		// and the deferred bloom_match fans out unfilled (P1 regression).
		tv, blob := bloomBytesTemplate(t, 0.001, 3, 4)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(Int64Field, {bf}) && random_sample(0.1)", mv)
		require.NoError(t, err)
		sample := expr.GetRandomSampleExpr()
		require.NotNil(t, sample)
		bfe := sample.GetPredicate().GetBloomFilterExpr()
		require.NotNil(t, bfe, "predicate must be materialized, got: %s", sample.GetPredicate().String())
		assert.Equal(t, blob, bfe.GetFilterBlob())
	})

	t.Run("dynamic field resolves to json path", func(t *testing.T) {
		// an unknown identifier resolves to the dynamic (JSON) field with the
		// identifier as the nested path.
		tv, _ := bloomBytesTemplate(t, 0.001, 7)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		expr, err := ParseExpr(helper, "bloom_match(unknown_field, {bf})", mv)
		require.NoError(t, err)
		bfe := expr.GetBloomFilterExpr()
		require.NotNil(t, bfe)
		assert.Equal(t, schemapb.DataType_JSON, bfe.GetColumnInfo().GetDataType())
		assert.Equal(t, []string{"unknown_field"}, bfe.GetColumnInfo().GetNestedPath())
	})
}

func TestExpr_BloomMatch_Errors(t *testing.T) {
	helper := newTestSchemaHelper(t)
	blobTV, _ := bloomBytesTemplate(t, 0.001, 1, 2, 3)
	bf := map[string]*schemapb.TemplateValue{"bf": blobTV}

	expectError := func(t *testing.T, exprStr string, mv map[string]*schemapb.TemplateValue, contains string) {
		_, err := ParseExpr(helper, exprStr, mv)
		require.Error(t, err, exprStr)
		if contains != "" {
			assert.ErrorContains(t, err, contains, exprStr)
		}
	}

	t.Run("wrong arg count", func(t *testing.T) {
		expectError(t, "bloom_match(Int64Field)", bf, "requires exactly 2 arguments")
		// the fpr third argument was removed; three args is now invalid.
		expectError(t, "bloom_match(Int64Field, {bf}, 0.01)", bf, "requires exactly 2 arguments")
	})

	t.Run("wrong field type", func(t *testing.T) {
		expectError(t, "bloom_match(BoolField, {bf})", bf, "only supports INT8/INT16/INT32/INT64/VARCHAR")
		expectError(t, "bloom_match(FloatField, {bf})", bf, "only supports INT8/INT16/INT32/INT64/VARCHAR")
		expectError(t, "bloom_match(DoubleField, {bf})", bf, "only supports INT8/INT16/INT32/INT64/VARCHAR")
		expectError(t, "bloom_match(ArrayField, {bf})", bf, "only supports INT8/INT16/INT32/INT64/VARCHAR")
		// first argument must be a field, not a literal.
		expectError(t, "bloom_match(1, {bf})", bf, "must be a scalar field name")
	})

	t.Run("second argument must be a template placeholder", func(t *testing.T) {
		// No proxy-side build: a literal array/scalar is not accepted.
		expectError(t, "bloom_match(Int64Field, [1, 2, 3])", nil, "must be a {template} placeholder")
		expectError(t, "bloom_match(Int64Field, 5)", nil, "must be a {template} placeholder")
		expectError(t, `bloom_match(Int64Field, "abc")`, nil, "must be a {template} placeholder")
	})

	t.Run("unknown template name", func(t *testing.T) {
		expectError(t, "bloom_match(Int64Field, {missing})", bf, "{missing} is not found")
	})

	t.Run("template value must be a bytes blob", func(t *testing.T) {
		// A non-bytes template value (bare int, array) is not a pre-built blob.
		intMV := map[string]*schemapb.TemplateValue{
			"bf": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		}
		expectError(t, "bloom_match(Int64Field, {bf})", intMV, "must be a client pre-built filter blob (bytes)")

		arrMV := map[string]*schemapb.TemplateValue{
			"bf": generateTemplateValue(schemapb.DataType_Array,
				generateTemplateArrayValue(schemapb.DataType_Int64, []int64{1, 2, 3})),
		}
		expectError(t, "bloom_match(Int64Field, {bf})", arrMV, "must be a client pre-built filter blob (bytes)")
	})

	t.Run("blob body over proxy.maxBloomFilterSize is rejected; 32-byte header allowed on top", func(t *testing.T) {
		pt := paramtable.Get()
		// proxy.maxBloomFilterSize budgets the SBBF *body*; the fixed 32-byte
		// MBF1 header is always allowed on top. Derive the actual body size from
		// the built blob so the budgets are exact regardless of the SBBF tier.
		tv, blob := bloomBytesTemplate(t, 0.001, 1, 2, 3)
		body := len(blob) - mbf1HeaderSize
		mv := map[string]*schemapb.TemplateValue{"bf": tv}

		// A body budget one byte below the body rejects the blob.
		pt.Save(pt.ProxyCfg.MaxBloomFilterSize.Key, strconv.Itoa(body-1))
		expectError(t, "bloom_match(Int64Field, {bf})", mv, "exceeding proxy.maxBloomFilterSize")
		pt.Reset(pt.ProxyCfg.MaxBloomFilterSize.Key)

		// A body budget exactly equal to the body admits the blob even though the
		// whole blob is body+32 bytes — the header rides on top. Regression pin
		// for the off-by-header bug that would otherwise halve the usable tier.
		pt.Save(pt.ProxyCfg.MaxBloomFilterSize.Key, strconv.Itoa(body))
		defer pt.Reset(pt.ProxyCfg.MaxBloomFilterSize.Key)
		_, err := ParseExpr(helper, "bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err, "a body-sized budget must admit the blob; the 32-byte header is allowed on top")
	})

	t.Run("blob bytes are not a valid MBF1 filter", func(t *testing.T) {
		mv := map[string]*schemapb.TemplateValue{"bf": bytesTemplate([]byte("not-a-real-blob"))}
		expectError(t, "bloom_match(Int64Field, {bf})", mv, "filter blob is invalid")
	})

	t.Run("bloom_match rejected inside MATCH_* element predicates", func(t *testing.T) {
		// bloom_match's one-sided error is unsafe for MATCH_MOST/EXACT
		// (upper-bounded hit counts) — a false positive would wrongly drop a
		// true row — so bloom_match is rejected inside every MATCH_*.
		// A scalar/JSON target reaches the MatchExpr guard; an element ref
		// ($[...]) is stopped earlier by the ARRAY field-type check.
		expectError(t, `MATCH_ANY(struct_array, bloom_match(Int64Field, {bf}) && $[sub_int] > 0)`, bf,
			"not supported inside MATCH_ANY")
		expectError(t, `MATCH_ALL(struct_array, bloom_match(JSONField["a"], {bf}) && $[sub_int] > 0)`, bf,
			"not supported inside MATCH_ALL")
		expectError(t, `MATCH_ANY(struct_array, bloom_match($[sub_int], {bf}))`, bf,
			"of type Array")
	})

	t.Run("bloom_match rejected inside element_filter element expression", func(t *testing.T) {
		// element_filter evaluates its expression per element (element IDs, not
		// row offsets), so a row-level bloom_match there would misread rows.
		expectError(t, `element_filter(struct_array, bloom_match(Int64Field, {bf}) && $[sub_int] > 0)`, bf,
			"not supported inside element_filter")
		expectError(t, `element_filter(struct_array, bloom_match(JSONField["a"], {bf}))`, bf,
			"not supported inside element_filter")
	})

	t.Run("bloom_match as element_filter sibling stays legal", func(t *testing.T) {
		// The doc-level combination is fine: bloom_match here is a SIBLING of
		// element_filter, evaluated on doc rows, not inside the element expr.
		_, err := ParseExpr(helper,
			`bloom_match(Int64Field, {bf}) and element_filter(struct_array, $[sub_int] > 0)`, bf)
		require.NoError(t, err)
	})

	t.Run("bytes template rejected outside bloom_match", func(t *testing.T) {
		// A bytes template value has exactly one consumer (the bloom_match
		// blob). Bound to any comparison it must die at the proxy, not fan out
		// a kBytesVal GenericValue that segcore cannot evaluate.
		expectError(t, `JSONField["a"] == {bf}`, bf,
			"bytes template value can only be used as the bloom_match filter argument")
		expectError(t, "Int64Field == {bf}", bf,
			"bytes template value can only be used as the bloom_match filter argument")
		expectError(t, "Int64Field in {bf}", bf, "")
	})
}

// TestHasBloomFilterExpr_MatchExprRecursion guards the delete-safety check: a
// bloom_match nested inside a MATCH_*(...) predicate must still be detected, so the
// delete path cannot be tricked into running an approximate filter destructively.
func TestHasBloomFilterExpr_MatchExprRecursion(t *testing.T) {
	bloomNode := &planpb.Expr{Expr: &planpb.Expr_BloomFilterExpr{BloomFilterExpr: &planpb.BloomFilterExpr{}}}
	callNode := &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{FunctionName: BloomMatchFunctionName}}}
	plainNode := &planpb.Expr{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{}}}

	wrap := func(pred *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_MatchExpr{MatchExpr: &planpb.MatchExpr{Predicate: pred}}}
	}

	assert.True(t, hasBloomFilterExpr(wrap(bloomNode)), "materialized bloom_match in MatchExpr predicate must be detected")
	assert.True(t, hasBloomFilterExpr(wrap(callNode)), "deferred bloom_match call in MatchExpr predicate must be detected")
	assert.False(t, hasBloomFilterExpr(wrap(plainNode)), "MatchExpr without bloom_match must not be flagged")
}

// TestCheckBloomMatchFieldTypeMatrix pins the accepted type set to exactly what
// segcore executes (INT8/16/32/64 + VARCHAR + JSON paths). STRING/TEXT are
// string-ish in typeutil.IsStringType but are NOT executable by the C++ prober,
// so they must be rejected at the proxy rather than failing later at the
// QueryNode.
func TestCheckBloomMatchFieldTypeMatrix(t *testing.T) {
	accept := []schemapb.DataType{
		schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_VarChar,
		schemapb.DataType_JSON,
	}
	reject := []schemapb.DataType{
		schemapb.DataType_String, schemapb.DataType_Text,
		schemapb.DataType_Bool, schemapb.DataType_Float, schemapb.DataType_Double,
	}
	for _, dt := range accept {
		require.NoError(t, checkBloomMatchField(&planpb.ColumnInfo{DataType: dt}, "f"), dt.String())
	}
	for _, dt := range reject {
		err := checkBloomMatchField(&planpb.ColumnInfo{DataType: dt}, "f")
		require.Error(t, err, dt.String())
		assert.Contains(t, err.Error(), "only supports INT8/INT16/INT32/INT64/VARCHAR", dt.String())
	}
	// JSON carries a nested path; non-JSON scalars must not.
	require.NoError(t, checkBloomMatchField(
		&planpb.ColumnInfo{DataType: schemapb.DataType_JSON, NestedPath: []string{"a", "b"}}, "f"))
	err := checkBloomMatchField(
		&planpb.ColumnInfo{DataType: schemapb.DataType_Int64, NestedPath: []string{"a"}}, "f")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nested paths on non-JSON")
}

func TestPlanContainsBloomFilter(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, _ := bloomBytesTemplate(t, 0.001, 1, 2, 3)
	mv := map[string]*schemapb.TemplateValue{"bf": tv}

	cases := []struct {
		expr     string
		mv       map[string]*schemapb.TemplateValue
		contains bool
	}{
		{"bloom_match(Int64Field, {bf})", mv, true},
		{"not bloom_match(Int64Field, {bf})", mv, true},
		{"Int64Field > 0 and bloom_match(Int64Field, {bf})", mv, true},
		{"Int64Field in [1, 2, 3]", nil, false},
		{"Int64Field > 0", nil, false},
	}
	for _, c := range cases {
		plan, err := CreateRetrievePlan(helper, c.expr, c.mv)
		require.NoError(t, err, c.expr)
		assert.Equal(t, c.contains, PlanContainsBloomFilter(plan), c.expr)
	}
}

// TestRedactPlanForLog verifies the plan-log redaction elides the (large)
// filter blob while preserving the rest of the plan, and is a no-op string
// path when no bloom_match is present.
func TestRedactPlanForLog(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("bloom blob is elided", func(t *testing.T) {
		tv, blob := bloomBytesTemplate(t, 0.001, 1, 2, 3)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}
		plan, err := CreateRetrievePlan(helper, "bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)

		out := RedactPlanForLog(plan).String()
		// The raw blob bytes must not appear; a size marker must.
		assert.NotContains(t, out, string(blob))
		assert.Contains(t, out, "bytes elided")
		// The original plan is untouched (redaction works on a clone).
		require.True(t, PlanContainsBloomFilter(plan))
		assert.Equal(t, blob, findFirstBloomBlob(plan))
	})

	t.Run("no bloom_match: plain plan string", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "Int64Field > 0", nil)
		require.NoError(t, err)
		assert.Equal(t, plan.String(), RedactPlanForLog(plan).String())
	})
}

// findFirstBloomBlob returns the filter_blob of the first BloomFilterExpr in a
// retrieve plan (test helper for the redaction no-mutation assertion).
func findFirstBloomBlob(plan *planpb.PlanNode) []byte {
	var walk func(e *planpb.Expr) []byte
	walk = func(e *planpb.Expr) []byte {
		if e == nil {
			return nil
		}
		switch x := e.GetExpr().(type) {
		case *planpb.Expr_BloomFilterExpr:
			return x.BloomFilterExpr.GetFilterBlob()
		case *planpb.Expr_UnaryExpr:
			return walk(x.UnaryExpr.GetChild())
		case *planpb.Expr_BinaryExpr:
			if b := walk(x.BinaryExpr.GetLeft()); b != nil {
				return b
			}
			return walk(x.BinaryExpr.GetRight())
		}
		return nil
	}
	return walk(plan.GetQuery().GetPredicates())
}

// --- white-box helpers for the bloom_match plan-tree utilities ---

// mbf1Blob builds an MBF1 envelope with the given num_blocks field and body
// length (bytes). With numBlocks=1 and bodyLen=mbf1BytesPerBlock it is a valid
// smallest blob; callers mutate individual header bytes to hit each error branch.
func mbf1Blob(numBlocks uint32, bodyLen int) []byte {
	blob := make([]byte, mbf1HeaderSize+bodyLen)
	copy(blob[0:4], mbf1Magic)
	binary.LittleEndian.PutUint16(blob[4:6], mbf1Version)
	binary.LittleEndian.PutUint16(blob[6:8], mbf1Algo)
	binary.LittleEndian.PutUint32(blob[24:28], numBlocks)
	return blob
}

func cloneBytes(b []byte) []byte { c := make([]byte, len(b)); copy(c, b); return c }

func bfLeaf(blob []byte) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_BloomFilterExpr{BloomFilterExpr: &planpb.BloomFilterExpr{FilterBlob: blob}}}
}

func bloomCallNode() *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{FunctionName: BloomMatchFunctionName}}}
}

func nonBloomLeaf() *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{Info: &planpb.ColumnInfo{FieldId: 1}}}}
}

func unaryNode(c *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{Child: c}}}
}

func binNode(l, r *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{Left: l, Right: r}}}
}

func sampleNode(p *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_RandomSampleExpr{RandomSampleExpr: &planpb.RandomSampleExpr{Predicate: p}}}
}

func elemFilterNode(el, p *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_ElementFilterExpr{ElementFilterExpr: &planpb.ElementFilterExpr{ElementExpr: el, Predicate: p}}}
}

func matchNode(p *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_MatchExpr{MatchExpr: &planpb.MatchExpr{Predicate: p}}}
}

func callWithParam(p *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{FunctionName: "other_fn", FunctionParameters: []*planpb.Expr{p}}}}
}

// TestFillBloomMatchExpressionValueErrors covers the FillBloomMatchExpressionValue
// defensive branches, reachable only white-box (normal parsing always produces a
// well-formed 2-parameter bloom_match call).
func TestFillBloomMatchExpressionValueErrors(t *testing.T) {
	col := &planpb.Expr{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{Info: &planpb.ColumnInfo{}}}}
	tmpl := &planpb.Expr{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{TemplateVariableName: "bf"}}}

	// not exactly 2 parameters.
	err := FillBloomMatchExpressionValue(&planpb.Expr{},
		&planpb.CallExpr{FunctionName: BloomMatchFunctionName, FunctionParameters: []*planpb.Expr{col}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2 parameters")

	call := &planpb.CallExpr{FunctionName: BloomMatchFunctionName, FunctionParameters: []*planpb.Expr{col, tmpl}}
	// template value not present.
	err = FillBloomMatchExpressionValue(&planpb.Expr{}, call, map[string]*planpb.GenericValue{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// template value present but not a bytes blob.
	err = FillBloomMatchExpressionValue(&planpb.Expr{}, call,
		map[string]*planpb.GenericValue{"bf": {Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client pre-built filter blob")
}

// TestValidateMBF1Envelope covers every structural rejection branch.
func TestValidateMBF1Envelope(t *testing.T) {
	require.NoError(t, validateMBF1Envelope(mbf1Blob(1, mbf1BytesPerBlock)))

	valid := mbf1Blob(1, mbf1BytesPerBlock)
	cases := []struct {
		name     string
		blob     []byte
		contains string
	}{
		{"too short", valid[:mbf1HeaderSize-1], "too short"},
		{"bad magic", func() []byte { c := cloneBytes(valid); c[0] = 'X'; return c }(), "invalid magic"},
		{"bad version", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint16(c[4:6], 2); return c }(), "unsupported bloom filter version"},
		{"bad algo", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint16(c[6:8], 9); return c }(), "unsupported bloom filter algo"},
		{"reserved nonzero", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint32(c[28:32], 1); return c }(), "reserved field must be 0"},
		{"num_blocks zero", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint32(c[24:28], 0); return c }(), "not a power of two"},
		{"num_blocks not pow2", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint32(c[24:28], 3); return c }(), "not a power of two"},
		{"num_blocks too large", func() []byte { c := cloneBytes(valid); binary.LittleEndian.PutUint32(c[24:28], 1<<23); return c }(), "not a power of two"},
		{"body length mismatch", mbf1Blob(1, mbf1BytesPerBlock*2), "body length"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMBF1Envelope(tc.blob)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}

// TestBloomExprTreeWalk exercises hasBloomFilterExpr and collectBloomFilterExprs
// across every expression node type.
func TestBloomExprTreeWalk(t *testing.T) {
	blob := mbf1Blob(1, mbf1BytesPerBlock)
	leaf := func() *planpb.Expr { return bfLeaf(blob) }

	// hasBloomFilterExpr: true for a bloom anywhere in the tree.
	assert.True(t, hasBloomFilterExpr(leaf()))
	assert.True(t, hasBloomFilterExpr(bloomCallNode()))
	assert.True(t, hasBloomFilterExpr(unaryNode(leaf())))
	assert.True(t, hasBloomFilterExpr(binNode(nonBloomLeaf(), leaf())))
	assert.True(t, hasBloomFilterExpr(binNode(leaf(), nonBloomLeaf())))
	assert.True(t, hasBloomFilterExpr(sampleNode(bloomCallNode())))
	assert.True(t, hasBloomFilterExpr(elemFilterNode(leaf(), nonBloomLeaf())))
	assert.True(t, hasBloomFilterExpr(elemFilterNode(nonBloomLeaf(), bloomCallNode())))
	assert.True(t, hasBloomFilterExpr(matchNode(leaf())))
	assert.True(t, hasBloomFilterExpr(callWithParam(leaf())))
	// false when no bloom present.
	assert.False(t, hasBloomFilterExpr(nonBloomLeaf()))
	assert.False(t, hasBloomFilterExpr(binNode(nonBloomLeaf(), nonBloomLeaf())))
	assert.False(t, hasBloomFilterExpr(callWithParam(nonBloomLeaf())))

	// collectBloomFilterExprs: counts materialized BloomFilterExpr leaves only
	// (a still-deferred bloom_match call has none yet).
	count := func(e *planpb.Expr) int {
		var out []*planpb.BloomFilterExpr
		collectBloomFilterExprs(e, &out)
		return len(out)
	}
	assert.Equal(t, 0, count(nil))
	assert.Equal(t, 1, count(leaf()))
	assert.Equal(t, 0, count(bloomCallNode()))
	assert.Equal(t, 1, count(unaryNode(leaf())))
	assert.Equal(t, 2, count(binNode(leaf(), leaf())))
	assert.Equal(t, 1, count(sampleNode(leaf())))
	assert.Equal(t, 2, count(elemFilterNode(leaf(), leaf())))
	assert.Equal(t, 1, count(matchNode(leaf())))
	assert.Equal(t, 1, count(callWithParam(leaf())))
	assert.Equal(t, 0, count(nonBloomLeaf()))
}

// TestPlanContainsBloomFilterAndPredicates covers every PlanNode variant.
func TestPlanContainsBloomFilterAndPredicates(t *testing.T) {
	leaf := bfLeaf(mbf1Blob(1, mbf1BytesPerBlock))

	query := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: leaf}}}
	anns := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{Predicates: leaf}}}
	preds := &planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: leaf}}
	noBloom := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: nonBloomLeaf()}}}
	scorerBloom := &planpb.PlanNode{
		Node:    &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: nonBloomLeaf()}},
		Scorers: []*planpb.ScoreFunction{{Filter: leaf}},
	}
	empty := &planpb.PlanNode{}

	assert.True(t, PlanContainsBloomFilter(query))
	assert.True(t, PlanContainsBloomFilter(anns))
	assert.True(t, PlanContainsBloomFilter(preds))
	assert.True(t, PlanContainsBloomFilter(scorerBloom))
	assert.False(t, PlanContainsBloomFilter(noBloom))
	assert.False(t, PlanContainsBloomFilter(empty))
	assert.False(t, PlanContainsBloomFilter(nil))

	assert.Equal(t, leaf, planPredicates(query))
	assert.Equal(t, leaf, planPredicates(anns))
	assert.Equal(t, leaf, planPredicates(preds))
	assert.Nil(t, planPredicates(empty))
}

// TestRedactPlanForLogEdgeCases covers the bloomRedactedPlan branches the parsed-
// plan test does not: a nil plan and a bloom blob carried in a scorer filter.
func TestRedactPlanForLogEdgeCases(t *testing.T) {
	assert.Equal(t, "<nil>", RedactPlanForLog(nil).String())

	// A bloom blob carried by a scorer filter (not the main predicate) is redacted
	// too, and the original is restored afterwards.
	secret := []byte("REDACT-ME-BLOOM-BLOB-CONTENT")
	scorerFilter := bfLeaf(secret)
	scorerPlan := &planpb.PlanNode{
		Node:    &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{Predicates: nonBloomLeaf()}},
		Scorers: []*planpb.ScoreFunction{{Filter: scorerFilter}},
	}
	out := RedactPlanForLog(scorerPlan).String()
	assert.NotContains(t, out, string(secret))
	assert.Contains(t, out, "bytes elided")
	assert.Equal(t, secret, scorerFilter.GetBloomFilterExpr().GetFilterBlob(), "scorer blob must be restored after stringify")
}
