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
	"bytes"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	clientroaring "github.com/milvus-io/milvus/client/v3/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	serverroaring "github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func roaringBytesTemplate(t *testing.T, members ...int64) (*schemapb.TemplateValue, []byte) {
	blob, err := clientroaring.Build(members)
	require.NoError(t, err)
	return bytesTemplate(blob), blob
}

// requireRoaringFilterExpr asserts the node shape and returns the materialized
// blob decoded. It runs the blob through the proxy validator first, so a test
// asserting on membership also asserts the plan carries something the proxy
// would have accepted.
func requireRoaringFilterExpr(t *testing.T, expr *planpb.Expr) *roaring64.Bitmap {
	rfe := expr.GetRoaringFilterExpr()
	require.NotNil(t, rfe, "expected a RoaringFilterExpr node, got: %s", expr.String())
	require.NotNil(t, rfe.GetColumnInfo())
	blob := rfe.GetBitmapBlob()
	_, err := serverroaring.Validate(blob)
	require.NoError(t, err)
	bitmap := roaring64.New()
	_, err = bitmap.ReadFrom(bytes.NewReader(blob[serverroaring.HeaderSize:]))
	require.NoError(t, err)
	return bitmap
}

// containsSigned probes the bitmap with the normative mapping: sign-extend to
// int64, keep the two's-complement bits as the uint64 key.
func containsSigned(bitmap *roaring64.Bitmap, member int64) bool {
	return bitmap.Contains(uint64(member))
}

func TestExpr_RoaringMatch(t *testing.T) {
	helper := newTestSchemaHelper(t)
	template, blob := roaringBytesTemplate(t, -42, 0, 7, 1<<40)
	values := map[string]*schemapb.TemplateValue{"ids": template}

	for _, field := range []string{"Int8Field", "Int16Field", "Int32Field", "Int64Field"} {
		t.Run(field, func(t *testing.T) {
			expr, err := ParseExpr(helper, "roaring_match("+field+", {ids})", values)
			require.NoError(t, err)
			bitmap := requireRoaringFilterExpr(t, expr)
			assert.Equal(t, blob, expr.GetRoaringFilterExpr().GetBitmapBlob())
			assert.True(t, containsSigned(bitmap, -42))
			assert.True(t, containsSigned(bitmap, 7))
			assert.False(t, containsSigned(bitmap, 8))
		})
	}

	t.Run("not roaring_match", func(t *testing.T) {
		expr, err := ParseExpr(helper, "not roaring_match(Int64Field, {ids})", values)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary)
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		requireRoaringFilterExpr(t, unary.GetChild())
	})

	t.Run("roaring_match nested under and", func(t *testing.T) {
		expr, err := ParseExpr(helper, "roaring_match(Int64Field, {ids}) and Int64Field > 0", values)
		require.NoError(t, err)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		requireRoaringFilterExpr(t, binary.GetLeft())
		require.NotNil(t, binary.GetRight().GetUnaryRangeExpr())
	})

	t.Run("search plan carries dedicated node", func(t *testing.T) {
		plan, err := CreateSearchPlan(helper, "roaring_match(Int64Field, {ids})", "FloatVectorField", &planpb.QueryInfo{
			Topk:       10,
			MetricType: "L2",
		}, values, nil)
		require.NoError(t, err)

		encoded, err := proto.Marshal(plan)
		require.NoError(t, err)
		roundTripped := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(encoded, roundTripped))
		requireRoaringFilterExpr(t, roundTripped.GetVectorAnns().GetPredicates())
	})

	t.Run("exact predicate is not classified as bloom", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "roaring_match(Int64Field, {ids})", values)
		require.NoError(t, err)
		assert.False(t, PlanContainsBloomFilter(plan))
	})
}

func TestExpr_RoaringMatchErrors(t *testing.T) {
	helper := newTestSchemaHelper(t)
	template, _ := roaringBytesTemplate(t, 1, 2, 3)
	values := map[string]*schemapb.TemplateValue{"ids": template}

	expectError := func(t *testing.T, expression string, params map[string]*schemapb.TemplateValue, contains string) {
		_, err := ParseExpr(helper, expression, params)
		require.Error(t, err, expression)
		assert.ErrorContains(t, err, contains, expression)
	}

	t.Run("wrong argument count", func(t *testing.T) {
		expectError(t, "roaring_match(Int64Field)", values, "requires exactly 2 arguments")
		expectError(t, "roaring_match(Int64Field, {ids}, 1)", values, "requires exactly 2 arguments")
	})

	t.Run("wrong field type", func(t *testing.T) {
		for _, field := range []string{"BoolField", "FloatField", "DoubleField", "VarCharField", "ArrayField"} {
			expectError(t, "roaring_match("+field+", {ids})", values, "only supports INT8/INT16/INT32/INT64")
		}
		expectError(t, "roaring_match(JSONField, {ids})", values, "not supported on JSON")
		expectError(t, `roaring_match(JSONField["a"], {ids})`, values, "not supported on JSON")
	})

	t.Run("every non-integer data type is rejected", func(t *testing.T) {
		for value, name := range schemapb.DataType_name {
			dataType := schemapb.DataType(value)
			if typeutil.IsIntegerType(dataType) {
				continue
			}
			t.Run(name, func(t *testing.T) {
				err := checkRoaringMatchField(&planpb.ColumnInfo{DataType: dataType}, "field")
				require.Error(t, err)
			})
		}
	})

	t.Run("bitmap must be a bytes template", func(t *testing.T) {
		expectError(t, "roaring_match(Int64Field, [1, 2, 3])", nil, "must be a {template} placeholder")
		expectError(t, "roaring_match(Int64Field, 1)", nil, "must be a {template} placeholder")
		expectError(t, "roaring_match(Int64Field, {missing})", values, "{missing} is not found")
		nonBytes := map[string]*schemapb.TemplateValue{
			"ids": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		}
		expectError(t, "roaring_match(Int64Field, {ids})", nonBytes, "must be a client pre-built roaring bitmap blob (bytes)")
	})

	t.Run("malformed MRB1", func(t *testing.T) {
		malformed := map[string]*schemapb.TemplateValue{"ids": bytesTemplate([]byte("not-mrb1"))}
		expectError(t, "roaring_match(Int64Field, {ids})", malformed, "bitmap blob is invalid")
		err := validateRoaringBitmapBlob([]byte("not-mrb1"))
		require.ErrorIs(t, err, merr.ErrParameterInvalid,
			"adding roaring_match context must preserve the validator's typed cause")
	})

	t.Run("template values are not returned in errors", func(t *testing.T) {
		_, err := ParseExpr(helper, `roaring_match(Int64Field, {ids}, 3)`, values)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "MRB1")
	})

	t.Run("roaring_match rejected inside element_filter element expression", func(t *testing.T) {
		expectError(t,
			`element_filter(struct_array, roaring_match(Int64Field, {ids}) && $[sub_int] > 0)`,
			values, "roaring_match is not supported inside element_filter")
		expectError(t,
			`element_filter(struct_array, not roaring_match(Int64Field, {ids}))`,
			values, "roaring_match is not supported inside element_filter")
	})

	t.Run("roaring_match as element_filter sibling stays legal", func(t *testing.T) {
		_, err := ParseExpr(helper,
			`roaring_match(Int64Field, {ids}) and element_filter(struct_array, $[sub_int] > 0)`,
			values)
		require.NoError(t, err)
	})
}

func TestRedactPlanForLogRedactsRoaringMembershipBlobs(t *testing.T) {
	helper := newTestSchemaHelper(t)
	roaringTemplate, roaringBlob := roaringBytesTemplate(t, 1, 2, 3)
	bloomTemplate, bloomBlob := bloomBytesTemplate(t, 0.001, 1, 2, 3)

	t.Run("pure roaring", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "roaring_match(Int64Field, {rb})",
			map[string]*schemapb.TemplateValue{"rb": roaringTemplate})
		require.NoError(t, err)

		out := RedactPlanForLog(plan).String()
		assert.NotContains(t, out, "MRB1")
		assert.Contains(t, out, "<blob>")
		assert.Equal(t, roaringBlob,
			plan.GetQuery().GetPredicates().GetRoaringFilterExpr().GetBitmapBlob(),
			"redaction must restore the original roaring blob")
	})

	t.Run("mixed bloom and roaring", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper,
			"bloom_match(Int64Field, {bf}) and roaring_match(Int64Field, {rb})",
			map[string]*schemapb.TemplateValue{
				"bf": bloomTemplate,
				"rb": roaringTemplate,
			})
		require.NoError(t, err)

		out := RedactPlanForLog(plan).String()
		assert.NotContains(t, out, "MBF1")
		assert.NotContains(t, out, "MRB1")
		assert.Equal(t, 2, strings.Count(out, "<blob>"))
		binaryExpr := plan.GetQuery().GetPredicates().GetBinaryExpr()
		require.NotNil(t, binaryExpr)
		assert.Equal(t, bloomBlob, binaryExpr.GetLeft().GetBloomFilterExpr().GetFilterBlob())
		assert.Equal(t, roaringBlob, binaryExpr.GetRight().GetRoaringFilterExpr().GetBitmapBlob())
	})

	t.Run("scorer blobs", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
			Scorers: []*planpb.ScoreFunction{
				{Filter: &planpb.Expr{Expr: &planpb.Expr_BloomFilterExpr{
					BloomFilterExpr: &planpb.BloomFilterExpr{FilterBlob: bloomBlob},
				}}},
				{Filter: &planpb.Expr{Expr: &planpb.Expr_RoaringFilterExpr{
					RoaringFilterExpr: &planpb.RoaringFilterExpr{BitmapBlob: roaringBlob},
				}}},
			},
		}

		out := RedactPlanForLog(plan).String()
		assert.NotContains(t, out, "MBF1")
		assert.NotContains(t, out, "MRB1")
		assert.Equal(t, 2, strings.Count(out, "<blob>"))
		assert.Equal(t, bloomBlob,
			plan.GetScorers()[0].GetFilter().GetBloomFilterExpr().GetFilterBlob())
		assert.Equal(t, roaringBlob,
			plan.GetScorers()[1].GetFilter().GetRoaringFilterExpr().GetBitmapBlob())
	})
}

func TestFillRoaringMatchExpressionValueRejectsMalformedCall(t *testing.T) {
	blob, err := clientroaring.Build([]int64{1, 2, 3})
	require.NoError(t, err)
	templateValues := map[string]*planpb.GenericValue{
		"ids": {Val: &planpb.GenericValue_BytesVal{BytesVal: blob}},
	}
	validColumn := &planpb.Expr{
		Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{
			Info: &planpb.ColumnInfo{DataType: schemapb.DataType_Int64},
		}},
	}
	validTemplate := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{
			TemplateVariableName: "ids",
		}},
		IsTemplate: true,
	}

	tests := map[string][]*planpb.Expr{
		"nil column parameter": {nil, validTemplate},
		"wrong column parameter shape": {
			{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{Value: &planpb.GenericValue{}}}},
			validTemplate,
		},
		"missing column info": {
			{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{}}},
			validTemplate,
		},
		"invalid column type": {
			{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{
				Info: &planpb.ColumnInfo{DataType: schemapb.DataType_Float},
			}}},
			validTemplate,
		},
		"nil template parameter": {validColumn, nil},
		"wrong template parameter shape": {
			validColumn,
			{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{Info: &planpb.ColumnInfo{}}}},
		},
		"template parameter not marked template": {
			validColumn,
			{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{TemplateVariableName: "ids"}}},
		},
		"empty template name": {
			validColumn,
			{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{}}, IsTemplate: true},
		},
	}

	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			call := &planpb.CallExpr{
				FunctionName:       RoaringMatchFunctionName,
				FunctionParameters: params,
			}
			expr := &planpb.Expr{
				Expr:       &planpb.Expr_CallExpr{CallExpr: call},
				IsTemplate: true,
			}
			require.Error(t, FillRoaringMatchExpressionValue(expr, call, templateValues))
		})
	}
}

// TestRoaringMatchSizeGuard pins the per-blob gate this feature needs but the
// original prototype lacked: without it a bitmap whose size is driven by a
// hostile value distribution — a sparse int64 set costs ~22 bytes per member —
// would be fanned out to every QueryNode before anyone measured it.
func TestRoaringMatchSizeGuard(t *testing.T) {
	helper := newTestSchemaHelper(t)
	pt := paramtable.Get()

	// proxy.maxMembershipFilterSize budgets the MRB1 *body*; the fixed 32-byte
	// header is allowed on top, matching bloom_match's convention. Derive the
	// body size from the built blob so the budgets are exact.
	tv, blob := roaringBytesTemplate(t, 1, 2, 3)
	body := len(blob) - mrb1HeaderSize
	mv := map[string]*schemapb.TemplateValue{"rb": tv}

	t.Run("body over the budget is rejected", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, strconv.Itoa(body-1))
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})", mv)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeding proxy.maxMembershipFilterSize")
	})

	t.Run("a body-sized budget admits the blob, header on top", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, strconv.Itoa(body))
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})", mv)
		require.NoError(t, err, "the 32-byte MRB1 header must be allowed on top of the body budget")
	})

	t.Run("oversize is rejected before the body is decoded", func(t *testing.T) {
		// A blob that is both over budget and structurally garbage must fail on
		// size, not on decoding: the gate exists so a hostile body is never
		// walked in the first place.
		garbage := append(append([]byte(nil), blob[:mrb1HeaderSize]...), make([]byte, 4096)...)
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, "16")
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})",
			map[string]*schemapb.TemplateValue{"rb": bytesTemplate(garbage)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeding proxy.maxMembershipFilterSize")
	})
}

func TestRoaringMatchRepeatedFilters(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, blob := roaringBytesTemplate(t, 1, 2, 3)

	expr, err := ParseExpr(helper,
		"roaring_match(Int64Field, {rb}) and roaring_match(Int64Field, {rb})",
		map[string]*schemapb.TemplateValue{"rb": tv})
	require.NoError(t, err)
	require.Equal(t, blob, expr.GetBinaryExpr().GetLeft().GetRoaringFilterExpr().GetBitmapBlob())
	require.Equal(t, blob, expr.GetBinaryExpr().GetRight().GetRoaringFilterExpr().GetBitmapBlob())
}

// TestPlanContainsRoaringFilter pins the accounting predicate that charges a
// roaring plan against the shared membership-filter plan budget, across every
// PlanNode variant and in scorer filters.
func TestPlanContainsRoaringFilter(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, blob := roaringBytesTemplate(t, 1, 2, 3)
	mv := map[string]*schemapb.TemplateValue{"rb": tv}

	for _, expr := range []string{
		"roaring_match(Int64Field, {rb})",
		"not roaring_match(Int64Field, {rb})",
		"roaring_match(Int64Field, {rb}) and Int64Field > 0",
		"Int64Field > 0 or roaring_match(Int64Field, {rb})",
	} {
		parsed, err := ParseExpr(helper, expr, mv)
		require.NoErrorf(t, err, "expr: %s", expr)
		plan := &planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: parsed}}
		require.Truef(t, PlanContainsRoaringFilter(plan), "expr must be charged to the budget: %s", expr)
		require.Truef(t, PlanContainsMembershipFilter(plan), "expr must be recognized as membership: %s", expr)
		require.Falsef(t, PlanContainsBloomFilter(plan),
			"roaring_match must not trip the bloom-only delete guard: %s", expr)
	}

	plain, err := ParseExpr(helper, "Int64Field > 0", nil)
	require.NoError(t, err)
	require.False(t, PlanContainsRoaringFilter(
		&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: plain}}))
	require.False(t, PlanContainsRoaringFilter(nil))
	scorerPlan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		Scorers: []*planpb.ScoreFunction{{
			Filter: &planpb.Expr{Expr: &planpb.Expr_RoaringFilterExpr{
				RoaringFilterExpr: &planpb.RoaringFilterExpr{BitmapBlob: blob},
			}},
		}},
	}
	require.True(t, PlanContainsRoaringFilter(scorerPlan))
	require.True(t, PlanContainsMembershipFilter(scorerPlan))
}

// TestClientBuiltBlobsPassProxyValidation pins the two halves of the MRB1 codec
// together: client/v3/roaringfilter is the only builder, pkg/v3/util/roaringfilter
// is the only Go validator, and they are separate packages because the plan
// parser is compiled into a c-shared library that must not depend on the
// standalone client module. Nothing but a test can catch them drifting, and a
// drift means blobs an SDK can build and the proxy rejects.
//
// This test is the only place in the tree that imports both.
func TestClientBuiltBlobsPassProxyValidation(t *testing.T) {
	require.Equal(t, clientroaring.Magic, serverroaring.Magic)
	require.Equal(t, clientroaring.Version, serverroaring.Version)
	require.Equal(t, clientroaring.FormatPortableRoaring64, serverroaring.FormatPortableRoaring64)
	require.Equal(t, clientroaring.HeaderSize, serverroaring.HeaderSize)
	require.Equal(t, clientroaring.MaxBodyBytes, serverroaring.MaxBodyBytes)
	require.Equal(t, clientroaring.MaxHighContainerCount, serverroaring.MaxHighContainerCount)
	require.Equal(t, clientroaring.MaxEstimatedDecodedBytes, serverroaring.MaxEstimatedDecodedBytes)
	require.Equal(t, clientroaring.EstimatedHighContainerOverheadBytes,
		serverroaring.EstimatedHighContainerOverheadBytes)
	require.Equal(t, clientroaring.EstimatedLowContainerOverheadBytes,
		serverroaring.EstimatedLowContainerOverheadBytes)

	shapes := map[string]func(rng *rand.Rand, n int) []int64{
		"contiguous": func(rng *rand.Rand, n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(i)
			}
			return s
		},
		"descending": func(rng *rand.Rand, n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(n - i)
			}
			return s
		},
		"negative": func(rng *rand.Rand, n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(-i - 1)
			}
			return s
		},
		"int32 shuffled": func(rng *rand.Rand, n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(rng.Uint32())
			}
			return s
		},
		"int64 shuffled": func(rng *rand.Rand, n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(rng.Uint64())
			}
			return s
		},
		"boundaries": func(rng *rand.Rand, n int) []int64 {
			return []int64{0, -1, 1, math.MaxInt64, math.MinInt64, math.MaxInt32, math.MinInt32, -1 << 40, 1 << 40}
		},
		// The shapes above only ever produce array and run containers, and their
		// run-cookie children stay under the offset-table threshold, so without
		// this one the bitmap-container and offset-table branches of the
		// validator would be reached by fixture bytes only -- never by bytes an
		// SDK actually built. Mirrors TestBuildPortableContainerEncodings.
		"container encodings": func(rng *rand.Rand, n int) []int64 {
			members := make([]int64, 0, 5200)
			for value := int64(0); value < 1000; value++ {
				members = append(members, value) // run container
			}
			for value := int64(0); value < 4097; value++ {
				members = append(members, (1<<16)+value*2) // bitmap container
			}
			for key := int64(2); key <= 4; key++ {
				members = append(members, (key<<16)+1) // pushes past the offset threshold
			}
			return append(members, (1<<32)+7) // second high-32 key
		},
	}
	// Sorted names, and a freshly seeded rng for every member set: map iteration
	// order is randomized, so a single shared rng would make the fixed seed pin
	// nothing and a CI failure would not reproduce on a re-run. The cost is that
	// the smaller shuffled sets are prefixes of the larger ones rather than
	// independent draws, which is a fair trade for reproducibility.
	names := make([]string, 0, len(shapes))
	for name := range shapes {
		names = append(names, name)
	}
	sort.Strings(names)

	// The fixed-set shapes exist to reach specific validator branches, so pin
	// what they must encode to. "container encodings" is a copy of the set in
	// pkg/util/roaringfilter (nothing can share it across the module boundary);
	// these counts are what make it reach the bitmap-container and run-cookie
	// offset-table branches, so an edit that drifts them fails here rather than
	// quietly stopping the coverage.
	//
	// The container counts alone are not enough: swapping the 4097 for 4096
	// keeps high=2/low=6 and produces a 4096-entry array container, which is
	// 8192 bytes -- exactly a bitmap container's size -- so the shape and the
	// body length both survive. The member count is what separates them, and it
	// is the same number the other two copies assert about their own sets, so an
	// edit to any one of them fails in its own package. None of this proves the
	// three copies are equal; pkg/util/roaringfilter is the one that classifies
	// the containers directly.
	fixedShapes := map[string]struct {
		high, low, cardinality uint64
	}{
		"container encodings": {high: 2, low: 6, cardinality: 5101},
	}

	pinned := 0
	for _, name := range names {
		gen := shapes[name]
		sizes := []int{0, 1, 2, 1000, 50_000}
		if len(gen(rand.New(rand.NewSource(20260728)), 0)) != 0 {
			// A shape that ignores n has one fixed member set; running it once
			// per size would repeat the same case five times.
			sizes = []int{0}
		}
		for _, n := range sizes {
			members := gen(rand.New(rand.NewSource(20260728)), n)
			blob, err := clientroaring.Build(members)
			require.NoErrorf(t, err, "%s n=%d", name, n)

			summary, err := serverroaring.Validate(blob)
			require.NoErrorf(t, err, "proxy rejected a client blob: %s n=%d", name, n)

			// The validator's structural scan must agree with what the builder
			// actually encoded, not merely accept the bytes.
			distinct := roaring64.New()
			for _, member := range members {
				distinct.Add(uint64(member))
			}
			require.Equalf(t, distinct.GetCardinality(), summary.Cardinality,
				"declared cardinality diverged for %s n=%d", name, n)

			// The container counts are the admission inputs: the SDK derives
			// them from the member slice to pre-reject against
			// MaxHighContainerCount and MaxEstimatedDecodedBytes, and the
			// validator recomputes them from the wire. Recompute a third time
			// from the key set so neither side is graded against itself.
			high, low := map[uint32]struct{}{}, map[uint64]struct{}{}
			for iter := distinct.Iterator(); iter.HasNext(); {
				key := iter.Next()
				high[uint32(key>>32)] = struct{}{}
				low[key>>16] = struct{}{}
			}
			require.Equalf(t, uint64(len(high)), summary.HighContainerCount,
				"high-container count diverged for %s n=%d", name, n)
			require.Equalf(t, uint64(len(low)), summary.LowContainerCount,
				"low-container count diverged for %s n=%d", name, n)

			// The decoded-memory estimate is duplicated across the module and
			// language boundaries, so pin the proxy's copy against the constants
			// it is built from. The SDK's copy is pinned the same way inside its
			// own module (TestDecodedEstimateFormula) -- an unexported function
			// cannot be reached from here, and exporting one so a test in another
			// module could read it would add public SDK surface for a test, which
			// is the thing this change is removing.
			wantEstimate := summary.BodyBytes +
				summary.HighContainerCount*serverroaring.EstimatedHighContainerOverheadBytes +
				summary.LowContainerCount*serverroaring.EstimatedLowContainerOverheadBytes
			require.Equalf(t, wantEstimate, summary.EstimatedDecodedBytes,
				"decoded-size estimate diverged for %s n=%d", name, n)
			if want, isPinned := fixedShapes[name]; isPinned {
				pinned++
				require.Equalf(t, want.high, summary.HighContainerCount,
					"%s no longer has the container shape it exists for", name)
				require.Equalf(t, want.low, summary.LowContainerCount,
					"%s no longer has the container shape it exists for", name)
				require.Equalf(t, want.cardinality, summary.Cardinality,
					"%s no longer has the member count it exists for: one value fewer "+
						"in the dense block encodes as an array rather than the bitmap "+
						"container this shape reaches, without changing the container "+
						"counts or the body length", name)
			}

			decoded := roaring64.New()
			consumed, err := decoded.ReadFrom(bytes.NewReader(blob[serverroaring.HeaderSize:]))
			require.NoErrorf(t, err, "%s n=%d", name, n)
			require.Equalf(t, int64(summary.BodyBytes), consumed, "%s n=%d", name, n)
			require.Truef(t, decoded.Equals(distinct),
				"decoded membership diverged for %s n=%d", name, n)
		}
	}

	// Without this, renaming or deleting a pinned shape makes the lookup miss and
	// takes the digest and container-shape assertions with it, silently, while
	// `go test` prints ok -- the failure mode the rest of this change exists to
	// remove. Each fixed-set shape runs exactly once, so the counts must match.
	require.Len(t, fixedShapes, pinned,
		"a shape named in fixedShapes no longer exists; renaming one must not "+
			"quietly drop its pin")
}
