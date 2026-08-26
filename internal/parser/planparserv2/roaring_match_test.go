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
	"encoding/binary"
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
	"github.com/milvus-io/milvus/client/v3/membership/roaringfilter"
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
			expr, err := ParseExpr(helper, "membership_match("+field+", {ids}, type=roaring)", values)
			require.NoError(t, err)
			filter := requireRoaringFilterExpr(t, expr)
			assert.Equal(t, blob, expr.GetRoaringFilterExpr().GetBitmapBlob())
			assert.True(t, containsSigned(filter, -42))
			assert.True(t, containsSigned(filter, 7))
			assert.False(t, containsSigned(filter, 8))
		})
	}

	t.Run("not roaring_match", func(t *testing.T) {
		expr, err := ParseExpr(helper, "not membership_match(Int64Field, {ids}, type=roaring)", values)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary)
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		requireRoaringFilterExpr(t, unary.GetChild())
	})

	t.Run("roaring_match nested under and", func(t *testing.T) {
		expr, err := ParseExpr(helper, "membership_match(Int64Field, {ids}, type=roaring) and Int64Field > 0", values)
		require.NoError(t, err)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		requireRoaringFilterExpr(t, binary.GetLeft())
		require.NotNil(t, binary.GetRight().GetUnaryRangeExpr())
	})

	t.Run("search plan carries dedicated node", func(t *testing.T) {
		plan, err := CreateSearchPlan(helper, "membership_match(Int64Field, {ids}, type=roaring)", "FloatVectorField", &planpb.QueryInfo{
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

	t.Run("exact predicate is not delete-unsafe", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "membership_match(Int64Field, {ids}, type=roaring)", values)
		require.NoError(t, err)
		assert.True(t, PlanContainsMembershipFilter(plan),
			"roaring_match must be charged to the shared membership plan-size budget")
		assert.False(t, PlanContainsMembershipFilterUnsafeForDelete(plan),
			"exact roaring_match must stay delete-safe")
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
		expectError(t, "membership_match(Int64Field, type=roaring)", values, "query plan failed")
		expectError(t, "membership_match(Int64Field, {ids}, 1, type=roaring)", values, "query plan failed")
	})

	t.Run("wrong field type", func(t *testing.T) {
		for _, field := range []string{"BoolField", "FloatField", "DoubleField", "VarCharField", "ArrayField"} {
			expectError(t, "membership_match("+field+", {ids}, type=roaring)", values, "only supports INT8/INT16/INT32/INT64")
		}
		expectError(t, "membership_match(JSONField, {ids}, type=roaring)", values, "not supported on JSON")
		expectError(t, `membership_match(JSONField["a"], {ids}, type=roaring)`, values, "not supported on JSON")
	})

	t.Run("every non-integer data type is rejected", func(t *testing.T) {
		for value, name := range schemapb.DataType_name {
			dataType := schemapb.DataType(value)
			if typeutil.IsIntegerType(dataType) {
				continue
			}
			t.Run(name, func(t *testing.T) {
				err := checkRoaringMatchField(
					&planpb.ColumnInfo{DataType: dataType}, "field", MembershipMatchFunctionName)
				require.Error(t, err)
			})
		}
	})

	t.Run("bitmap must be a bytes template", func(t *testing.T) {
		expectError(t, "membership_match(Int64Field, [1, 2, 3], type=roaring)", nil, "must be a {template} placeholder")
		expectError(t, "membership_match(Int64Field, 1, type=roaring)", nil, "must be a {template} placeholder")
		expectError(t, "membership_match(Int64Field, {missing}, type=roaring)", values, "{missing} is not found")
		nonBytes := map[string]*schemapb.TemplateValue{
			"ids": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		}
		expectError(t, "membership_match(Int64Field, {ids}, type=roaring)", nonBytes, "must be a client pre-built membership filter blob (bytes)")
	})

	t.Run("malformed MRB1", func(t *testing.T) {
		malformed := map[string]*schemapb.TemplateValue{"ids": bytesTemplate([]byte("not-mrb1"))}
		expectError(t, "membership_match(Int64Field, {ids}, type=roaring)", malformed, "unknown format magic")
		_, err := validateRoaringBitmapBlob([]byte("not-mrb1"))
		require.ErrorIs(t, err, merr.ErrParameterInvalid,
			"adding roaring_match context must preserve the validator's typed cause")
	})

	t.Run("template values are not returned in errors", func(t *testing.T) {
		// The blob itself never enters the expression text -- the second
		// argument must be a {template} placeholder supplied out of band -- so
		// echoing the caller's own expression cannot leak MRB1 content.
		_, err := ParseExpr(helper, `membership_match(Int64Field, {ids}, 3, type=roaring)`, values)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "MRB1")
	})

	t.Run("roaring_match rejected inside element_filter element expression", func(t *testing.T) {
		expectError(t,
			`element_filter(struct_array, membership_match(Int64Field, {ids}, type=roaring) && $[sub_int] > 0)`,
			values, "membership_match filters are not supported inside element_filter")
		expectError(t,
			`element_filter(struct_array, not membership_match(Int64Field, {ids}, type=roaring))`,
			values, "membership_match filters are not supported inside element_filter")
	})

	t.Run("roaring_match as element_filter sibling stays legal", func(t *testing.T) {
		_, err := ParseExpr(helper,
			`membership_match(Int64Field, {ids}, type=roaring) and element_filter(struct_array, $[sub_int] > 0)`,
			values)
		require.NoError(t, err)
	})
}

func TestRedactPlanForLogRedactsRoaringMembershipBlobs(t *testing.T) {
	helper := newTestSchemaHelper(t)
	roaringTemplate, roaringBlob := roaringBytesTemplate(t, 1, 2, 3)
	bloomTemplate, bloomBlob := bloomBytesTemplate(t, 0.001, 1, 2, 3)

	t.Run("pure roaring", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "membership_match(Int64Field, {rb}, type=roaring)",
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
			"membership_match(Int64Field, {bf}, type=bloom) and membership_match(Int64Field, {rb}, type=roaring)",
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

func TestFillMembershipMatchExpressionValueRejectsMalformedCall(t *testing.T) {
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
				FunctionName:       MembershipMatchFunctionName,
				FunctionParameters: params,
			}
			expr := &planpb.Expr{
				Expr:       &planpb.Expr_CallExpr{CallExpr: call},
				IsTemplate: true,
			}
			require.Error(t, fillMembershipMatchExpressionValue(expr, call, templateValues, &fillExpressionContext{}))
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
	body := len(blob) - roaringfilter.HeaderSize
	mv := map[string]*schemapb.TemplateValue{"rb": tv}

	t.Run("body over the budget is rejected", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, strconv.Itoa(body-1))
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "membership_match(Int64Field, {rb}, type=roaring)", mv)
		require.Error(t, err)
		require.Contains(t, err.Error(), "proxy.maxMembershipFilterSize")
	})

	t.Run("a body-sized budget admits the blob, header on top", func(t *testing.T) {
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, strconv.Itoa(body))
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "membership_match(Int64Field, {rb}, type=roaring)", mv)
		require.NoError(t, err, "the 32-byte MRB1 header must be allowed on top of the body budget")
	})

	t.Run("oversize is rejected before the body is decoded", func(t *testing.T) {
		// A blob that is both over budget and structurally garbage must fail on
		// size, not on decoding: the gate exists so a hostile body is never
		// walked in the first place.
		garbage := append(append([]byte(nil), blob[:roaringfilter.HeaderSize]...), make([]byte, 4096)...)
		pt.Save(pt.ProxyCfg.MaxMembershipFilterSize.Key, "16")
		defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterSize.Key)
		_, err := ParseExpr(helper, "membership_match(Int64Field, {rb}, type=roaring)",
			map[string]*schemapb.TemplateValue{"rb": bytesTemplate(garbage)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "proxy.maxMembershipFilterSize")
	})
}

func TestRoaringMatchPreflightBudgetsOccurrencesBeforeValidation(t *testing.T) {
	helper := newTestSchemaHelper(t)
	pt := paramtable.Get()
	garbage := make([]byte, 4096)
	values := map[string]*schemapb.TemplateValue{"rb": bytesTemplate(garbage)}
	expr := "membership_match(Int64Field, {rb}, type=roaring) or membership_match(Int64Field, {rb}, type=roaring)"

	// The occurrence budget charges the MRB1 *body* (header rides on top), so
	// two occurrences of one body-sized blob exceed a budget of 2*body-1.
	body := len(garbage) - roaringfilter.HeaderSize
	pt.Save(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key, strconv.Itoa(2*body-1))
	defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key)
	_, err := ParseExpr(helper, expr, values)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.ErrorContains(t, err, "before plan materialization")
	require.NotContains(t, err.Error(), "unknown format magic",
		"the occurrence budget must reject before structural validation")
}

func TestRoaringMatchPreflightCachesValidationByTemplateName(t *testing.T) {
	helper := newTestSchemaHelper(t)
	template, blob := roaringBytesTemplate(t, 1, 2, 3)
	ret := handleExpr(helper,
		"membership_match(Int64Field, {rb}, type=roaring) and membership_match(Int64Field, {rb}, type=roaring)")
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
	_, err = ParseExpr(helper, "membership_match(Int64Field, {rb}, type=roaring)", values)
	require.NoError(t, err, "one admitted bitmap must stay valid")

	_, err = ParseExpr(helper,
		"membership_match(Int64Field, {rb}, type=roaring) and membership_match(Int64Field, {rb}, type=roaring)",
		values)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.ErrorContains(t, err, "estimated decoded size")
	require.ErrorContains(t, err, "before plan materialization")
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
	expr := "membership_match(Int64Field, {rb}, type=roaring)"

	// Room for exactly one occurrence across the whole request (body basis;
	// the fixed MRB1 header rides on top of the budget).
	body := len(blob) - roaringfilter.HeaderSize
	pt.Save(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key, strconv.Itoa(2*body-1))
	defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key)

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
		ret := handleExpr(helper, "membership_match(Int64Field, {rb}, type=roaring)")
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

// TestRoaringMatchRepeatedFilters pins that a boolean expression referencing
// the same membership template twice materializes two independent
// RoaringFilterExpr leaves, each carrying the full blob.
func TestRoaringMatchRepeatedFilters(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, blob := roaringBytesTemplate(t, 1, 2, 3)

	expr, err := ParseExpr(helper,
		"membership_match(Int64Field, {rb}, type=roaring) and membership_match(Int64Field, {rb}, type=roaring)",
		map[string]*schemapb.TemplateValue{"rb": tv})
	require.NoError(t, err)
	require.Equal(t, blob, expr.GetBinaryExpr().GetLeft().GetRoaringFilterExpr().GetBitmapBlob())
	require.Equal(t, blob, expr.GetBinaryExpr().GetRight().GetRoaringFilterExpr().GetBitmapBlob())
}

// TestClientBuiltBlobsPassProxyValidation pins the client/server MRB1 codec
// agreement across the member-set shapes an SDK can actually produce: every
// blob built with client/v3/membership/roaringfilter must pass pkg/v3/util/roaringfilter
// validation, and the validator's structural summary must agree with what the
// builder encoded.
func TestClientBuiltBlobsPassProxyValidation(t *testing.T) {
	require.Equal(t, roaringfilter.Magic, serverroaring.Magic)
	require.Equal(t, roaringfilter.Version, serverroaring.Version)
	require.Equal(t, roaringfilter.FormatPortableRoaring64, serverroaring.FormatPortableRoaring64)
	require.Equal(t, roaringfilter.HeaderSize, serverroaring.HeaderSize)
	require.Equal(t, roaringfilter.MaxBodyBytes, serverroaring.MaxBodyBytes)
	require.Equal(t, roaringfilter.MaxHighContainerCount, serverroaring.MaxHighContainerCount)
	require.Equal(t, roaringfilter.MaxEstimatedDecodedBytes, serverroaring.MaxEstimatedDecodedBytes)
	require.Equal(t, roaringfilter.EstimatedHighContainerOverheadBytes,
		serverroaring.EstimatedHighContainerOverheadBytes)
	require.Equal(t, roaringfilter.EstimatedLowContainerOverheadBytes,
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
			blob, err := roaringfilter.Build(members)
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
						"shape or the body length", name)
			}
		}
	}
	require.Equal(t, len(fixedShapes), pinned, "a fixed shape was never exercised")
}
