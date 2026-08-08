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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	serverroaring "github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func roaringBytesTemplate(t *testing.T, members ...int64) (*schemapb.TemplateValue, []byte) {
	blob, err := roaringfilter.Build(members)
	require.NoError(t, err)
	return bytesTemplate(blob), blob
}

func compactHighContainerRoaringBlob(count uint64) []byte {
	child := make([]byte, 11)
	binary.LittleEndian.PutUint16(child[0:2], 12347)
	body := make([]byte, 8, 8+int(count)*15)
	binary.LittleEndian.PutUint64(body, count)
	var key [4]byte
	for i := uint64(0); i < count; i++ {
		binary.LittleEndian.PutUint32(key[:], uint32(i))
		body = append(body, key[:]...)
		body = append(body, child...)
	}

	blob := make([]byte, serverroaring.HeaderSize+len(body))
	copy(blob[:4], serverroaring.Magic)
	binary.LittleEndian.PutUint16(blob[4:6], serverroaring.Version)
	binary.LittleEndian.PutUint16(blob[6:8], serverroaring.FormatPortableRoaring64)
	binary.LittleEndian.PutUint64(blob[8:16], count)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(body)))
	copy(blob[serverroaring.HeaderSize:], body)
	return blob
}

func requireRoaringFilterExpr(t *testing.T, expr *planpb.Expr) *roaringfilter.Filter {
	rfe := expr.GetRoaringFilterExpr()
	require.NotNil(t, rfe, "expected a RoaringFilterExpr node, got: %s", expr.String())
	require.NotNil(t, rfe.GetColumnInfo())
	filter, err := roaringfilter.Parse(rfe.GetBitmapBlob())
	require.NoError(t, err)
	return filter
}

func TestExpr_RoaringMatch(t *testing.T) {
	helper := newTestSchemaHelper(t)
	template, blob := roaringBytesTemplate(t, -42, 0, 7, 1<<40)
	values := map[string]*schemapb.TemplateValue{"ids": template}

	for _, field := range []string{"Int8Field", "Int16Field", "Int32Field", "Int64Field"} {
		t.Run(field, func(t *testing.T) {
			expr, err := ParseExpr(helper, "roaring_match("+field+", {ids})", values)
			require.NoError(t, err)
			filter := requireRoaringFilterExpr(t, expr)
			assert.Equal(t, blob, expr.GetRoaringFilterExpr().GetBitmapBlob())
			assert.True(t, filter.ContainsInt64(-42))
			assert.True(t, filter.ContainsInt64(7))
			assert.False(t, filter.ContainsInt64(8))
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
		_, err := validateRoaringBitmapBlob([]byte("not-mrb1"))
		require.ErrorIs(t, err, merr.ErrParameterInvalid,
			"adding roaring_match context must preserve the validator's typed cause")
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
		assert.Contains(t, out, "bytes elided")
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
		assert.Equal(t, 2, strings.Count(out, "bytes elided"))
		binaryExpr := plan.GetQuery().GetPredicates().GetBinaryExpr()
		require.NotNil(t, binaryExpr)
		assert.Equal(t, bloomBlob, binaryExpr.GetLeft().GetBloomFilterExpr().GetFilterBlob())
		assert.Equal(t, roaringBlob, binaryExpr.GetRight().GetRoaringFilterExpr().GetBitmapBlob())
	})

	t.Run("roaring scorer", func(t *testing.T) {
		secret := []byte("MRB1-EXACT-MEMBER-SET")
		scorerFilter := &planpb.Expr{Expr: &planpb.Expr_RoaringFilterExpr{
			RoaringFilterExpr: &planpb.RoaringFilterExpr{BitmapBlob: secret},
		}}
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
				Predicates: nonBloomLeaf(),
			}},
			Scorers: []*planpb.ScoreFunction{{Filter: scorerFilter}},
		}

		out := RedactPlanForLog(plan).String()
		assert.NotContains(t, out, string(secret))
		assert.Contains(t, out, "bytes elided")
		assert.Equal(t, secret, scorerFilter.GetRoaringFilterExpr().GetBitmapBlob())
	})
}

func TestFillRoaringMatchExpressionValueRejectsMalformedCall(t *testing.T) {
	blob, err := roaringfilter.Build([]int64{1, 2, 3})
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

	// proxy.maxRoaringFilterSize budgets the MRB1 *body*; the fixed 32-byte
	// header is allowed on top, matching bloom_match's convention. Derive the
	// body size from the built blob so the budgets are exact.
	tv, blob := roaringBytesTemplate(t, 1, 2, 3)
	body := len(blob) - mrb1HeaderSize
	mv := map[string]*schemapb.TemplateValue{"rb": tv}

	t.Run("body over the budget is rejected", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxRoaringFilterSize.Key, strconv.Itoa(body-1))
		defer pt.Reset(pt.ProxyCfg.MaxRoaringFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})", mv)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeding proxy.maxRoaringFilterSize")
	})

	t.Run("a body-sized budget admits the blob, header on top", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxRoaringFilterSize.Key, strconv.Itoa(body))
		defer pt.Reset(pt.ProxyCfg.MaxRoaringFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})", mv)
		require.NoError(t, err, "the 32-byte MRB1 header must be allowed on top of the body budget")
	})

	t.Run("oversize is rejected before the body is decoded", func(t *testing.T) {
		// A blob that is both over budget and structurally garbage must fail on
		// size, not on decoding: the gate exists so a hostile body is never
		// walked in the first place.
		garbage := append(append([]byte(nil), blob[:mrb1HeaderSize]...), make([]byte, 4096)...)
		pt.Save(pt.ProxyCfg.MaxRoaringFilterSize.Key, "16")
		defer pt.Reset(pt.ProxyCfg.MaxRoaringFilterSize.Key)
		_, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})",
			map[string]*schemapb.TemplateValue{"rb": bytesTemplate(garbage)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeding proxy.maxRoaringFilterSize")
	})
}

func TestRoaringMatchPreflightBudgetsOccurrencesBeforeValidation(t *testing.T) {
	helper := newTestSchemaHelper(t)
	pt := paramtable.Get()
	garbage := make([]byte, 4096)
	values := map[string]*schemapb.TemplateValue{"rb": bytesTemplate(garbage)}
	expr := "roaring_match(Int64Field, {rb}) or roaring_match(Int64Field, {rb})"

	pt.Save(pt.ProxyCfg.MaxBloomFilterPlanSize.Key, strconv.Itoa(2*len(garbage)-1))
	defer pt.Reset(pt.ProxyCfg.MaxBloomFilterPlanSize.Key)
	_, err := ParseExpr(helper, expr, values)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.ErrorContains(t, err, "before plan materialization")
	require.NotContains(t, err.Error(), "bitmap blob is invalid",
		"the occurrence budget must reject before structural validation")
}

func TestRoaringMatchPreflightCachesValidationByTemplateName(t *testing.T) {
	helper := newTestSchemaHelper(t)
	template, blob := roaringBytesTemplate(t, 1, 2, 3)
	ret := handleExpr(helper,
		"roaring_match(Int64Field, {rb}) and roaring_match(Int64Field, {rb})")
	require.NoError(t, getError(ret))
	predicate := getExpr(ret)
	require.NotNil(t, predicate)
	valueMap, err := UnmarshalExpressionValues(
		map[string]*schemapb.TemplateValue{"rb": template})
	require.NoError(t, err)

	ctx, err := preflightMembershipFilterValues(predicate.expr, valueMap, NewMembershipPreflightBudget())
	require.NoError(t, err)
	require.Equal(t, blob, ctx.validatedRoaringBlobs["rb"].blob)
	require.NotZero(t, ctx.validatedRoaringBlobs["rb"].summary.EstimatedDecodedBytes)
	require.NoError(t, fillExpressionValue(predicate.expr, valueMap, ctx))
	require.NotNil(t, predicate.expr.GetBinaryExpr().GetLeft().GetRoaringFilterExpr())
	require.NotNil(t, predicate.expr.GetBinaryExpr().GetRight().GetRoaringFilterExpr())
}

func TestRoaringMatchPreflightBudgetsAggregateDecodedBytes(t *testing.T) {
	helper := newTestSchemaHelper(t)
	blob := compactHighContainerRoaringBlob(170_000)
	summary, err := serverroaring.Validate(blob)
	require.NoError(t, err)
	require.LessOrEqual(t, summary.EstimatedDecodedBytes, uint64(serverroaring.MaxEstimatedDecodedBytes))
	require.Greater(t, summary.EstimatedDecodedBytes*2, uint64(serverroaring.MaxEstimatedDecodedBytes))

	values := map[string]*schemapb.TemplateValue{"rb": bytesTemplate(blob)}
	_, err = ParseExpr(helper, "roaring_match(Int64Field, {rb})", values)
	require.NoError(t, err, "one admitted bitmap must stay valid")

	_, err = ParseExpr(helper,
		"roaring_match(Int64Field, {rb}) and roaring_match(Int64Field, {rb})",
		values)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.ErrorContains(t, err, "estimated decoded size")
	require.ErrorContains(t, err, "before plan materialization")
}

func TestEstimateRoaringFilterPlanDecodedBytesIncludesScorers(t *testing.T) {
	_, blob := roaringBytesTemplate(t, 1, 2, 3)
	summary, err := serverroaring.Validate(blob)
	require.NoError(t, err)
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: roaringPlanSizeTestExpr(blob),
		}},
		Scorers: []*planpb.ScoreFunction{{Filter: roaringPlanSizeTestExpr(blob)}},
	}

	decodedBytes, err := EstimateRoaringFilterPlanDecodedBytes(plan)
	require.NoError(t, err)
	require.Equal(t, summary.EstimatedDecodedBytes*2, decodedBytes)
}

func roaringPlanSizeTestExpr(blob []byte) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_RoaringFilterExpr{
		RoaringFilterExpr: &planpb.RoaringFilterExpr{BitmapBlob: blob},
	}}
}

// TestPlanContainsRoaringFilter pins the accounting predicate that charges a
// roaring plan against the shared filter-plan budget. A miss here would let a
// bitmap blob skip the budget entirely.
func TestPlanContainsRoaringFilter(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, _ := roaringBytesTemplate(t, 1, 2, 3)
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
		require.Falsef(t, PlanContainsBloomFilter(plan),
			"roaring_match must not trip the bloom-only delete guard: %s", expr)
	}

	plain, err := ParseExpr(helper, "Int64Field > 0", nil)
	require.NoError(t, err)
	require.False(t, PlanContainsRoaringFilter(
		&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: plain}}))
	require.False(t, PlanContainsRoaringFilter(nil))
}

// TestClientAndServerRoaringBuildAgree pins the two copies of the MRB1 codec
// together. client/v3/roaringfilter is what SDK users build with;
// pkg/v3/util/roaringfilter is what the proxy validates with. They are separate
// packages because the plan parser is compiled into a c-shared library that
// must not depend on the standalone client module, so nothing but a test can
// catch them drifting — and a drift here means blobs a client can build and the
// proxy will reject, or worse, accept differently.
//
// This test is the only place in the tree that imports both.
func TestClientAndServerRoaringBuildAgree(t *testing.T) {
	require.Equal(t, roaringfilter.MaxHighContainerCount, serverroaring.MaxHighContainerCount)
	require.Equal(t, roaringfilter.MaxEstimatedDecodedBytes, serverroaring.MaxEstimatedDecodedBytes)
	require.Equal(t, roaringfilter.EstimatedHighContainerOverheadBytes,
		serverroaring.EstimatedHighContainerOverheadBytes)
	require.Equal(t, roaringfilter.EstimatedLowContainerOverheadBytes,
		serverroaring.EstimatedLowContainerOverheadBytes)

	rng := rand.New(rand.NewSource(20260728))
	shapes := map[string]func(n int) []int64{
		"contiguous": func(n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(i)
			}
			return s
		},
		"descending": func(n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(n - i)
			}
			return s
		},
		"negative": func(n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(-i - 1)
			}
			return s
		},
		"int32 shuffled": func(n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(rng.Uint32())
			}
			return s
		},
		"int64 shuffled": func(n int) []int64 {
			s := make([]int64, n)
			for i := range s {
				s[i] = int64(rng.Uint64())
			}
			return s
		},
		"boundaries": func(n int) []int64 {
			return []int64{0, -1, 1, math.MaxInt64, math.MinInt64, math.MaxInt32, math.MinInt32, -1 << 40, 1 << 40}
		},
	}
	for name, gen := range shapes {
		for _, n := range []int{0, 1, 2, 1000, 50_000} {
			members := gen(n)
			clientBlob, err := roaringfilter.Build(members)
			require.NoErrorf(t, err, "%s n=%d", name, n)
			serverBlob, err := serverroaring.Build(members)
			require.NoErrorf(t, err, "%s n=%d", name, n)
			require.Equalf(t, clientBlob, serverBlob,
				"client and server MRB1 builders diverged for %s n=%d", name, n)

			// And each side must accept the other's bytes.
			_, err = serverroaring.Parse(clientBlob)
			require.NoErrorf(t, err, "server rejected a client blob: %s n=%d", name, n)
			_, err = roaringfilter.Parse(serverBlob)
			require.NoErrorf(t, err, "client rejected a server blob: %s n=%d", name, n)
		}
	}
}

// The occurrence budget is documented as a per-request ceiling. It used to be
// per-expression-parse, so a request that parses N expressions (hybrid
// sub-requests, scorer filters) handed out N full quotas.
func TestRoaringMatchPreflightBudgetIsSharedAcrossParses(t *testing.T) {
	helper := newTestSchemaHelper(t)
	pt := paramtable.Get()
	// A structurally valid blob: this test is about the budget, so the parse
	// must not fail for any other reason.
	template, blob := roaringBytesTemplate(t, 1, 2, 3)
	values := map[string]*schemapb.TemplateValue{"rb": template}
	expr := "roaring_match(Int64Field, {rb})"

	// Room for exactly one occurrence across the whole request.
	pt.Save(pt.ProxyCfg.MaxBloomFilterPlanSize.Key, strconv.Itoa(2*len(blob)-1))
	defer pt.Reset(pt.ProxyCfg.MaxBloomFilterPlanSize.Key)

	visitorArgs := &ParserVisitorArgs{MembershipBudget: NewMembershipPreflightBudget()}

	_, err := parseExprInner(helper, expr, values, visitorArgs)
	require.NoError(t, err, "first parse fits in the budget")

	_, err = parseExprInner(helper, expr, values, visitorArgs)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge,
		"second parse must be charged against the same request budget")

	// A fresh budget starts over: that is the single-expression scope callers
	// outside a search request still get.
	_, err = parseExprInner(helper, expr, values,
		&ParserVisitorArgs{MembershipBudget: NewMembershipPreflightBudget()})
	require.NoError(t, err)
}

// Structural validation is a pure function of the bytes, so a shared budget
// must not re-validate a blob it already saw -- but it must also not reuse a
// cache entry when the same template name carries different bytes, which is
// legal across hybrid sub-requests.
func TestRoaringMatchPreflightCacheIsContentAddressed(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templateA, blobA := roaringBytesTemplate(t, 1, 2, 3)
	templateB, blobB := roaringBytesTemplate(t, 40, 50, 60)
	require.NotEqual(t, blobA, blobB)

	budget := NewMembershipPreflightBudget()
	parse := func(template *schemapb.TemplateValue) *fillExpressionContext {
		ret := handleExpr(helper, "roaring_match(Int64Field, {rb})")
		require.NoError(t, getError(ret))
		predicate := getExpr(ret)
		require.NotNil(t, predicate)
		valueMap, err := UnmarshalExpressionValues(
			map[string]*schemapb.TemplateValue{"rb": template})
		require.NoError(t, err)
		ctx, err := preflightMembershipFilterValues(predicate.expr, valueMap, budget)
		require.NoError(t, err)
		return ctx
	}

	require.Equal(t, blobA, parse(templateA).validatedRoaringBlobs["rb"].blob)
	// Same name, same bytes: served from the cache, still correct.
	require.Equal(t, blobA, parse(templateA).validatedRoaringBlobs["rb"].blob)
	// Same name, different bytes: the cache must not alias them.
	require.Equal(t, blobB, parse(templateB).validatedRoaringBlobs["rb"].blob)
}
