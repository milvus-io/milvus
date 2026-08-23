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

// membership_match: the unified membership-filter surface syntax. The blob's
// magic header selects the kind — MBF1 lowers to BloomFilterExpr, MRB1 to
// RoaringFilterExpr — so these tests pin that the unified name produces plans
// identical to what the explicit bloom_match / roaring_match aliases produce.

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpr_MembershipMatch(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("MBF1 blob lowers to a BloomFilterExpr", func(t *testing.T) {
		tv, blob := bloomBytesTemplate(t, 0.001, 1, 2, 3)
		mv := map[string]*schemapb.TemplateValue{"bf": tv}

		for _, expression := range []string{
			"membership_match(Int64Field, {bf})",
			"not membership_match(Int64Field, {bf})",
			"Int64Field > 0 and membership_match(Int64Field, {bf})",
		} {
			expr, err := ParseExpr(helper, expression, mv)
			require.NoError(t, err, expression)
			require.True(t, hasMembershipFilterExpr(expr), expression)
		}

		expr, err := ParseExpr(helper, "membership_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		bfe := expr.GetBloomFilterExpr()
		require.NotNil(t, bfe, "an MBF1 blob must materialize as BloomFilterExpr")
		assert.Equal(t, schemapb.DataType_Int64, bfe.GetColumnInfo().GetDataType())
		assert.Equal(t, blob, bfe.GetFilterBlob(), "the client-built blob must be embedded verbatim")

		// The wire node must be indistinguishable from bloom_match's.
		aliasExpr, err := ParseExpr(helper, "bloom_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		assert.Equal(t, aliasExpr.GetBloomFilterExpr().GetFilterBlob(), bfe.GetFilterBlob())
	})

	t.Run("MRB1 blob lowers to a RoaringFilterExpr", func(t *testing.T) {
		tv, blob := roaringBytesTemplate(t, 1, 2, 3)
		mv := map[string]*schemapb.TemplateValue{"rb": tv}

		expr, err := ParseExpr(helper, "membership_match(Int64Field, {rb})", mv)
		require.NoError(t, err)
		rfe := expr.GetRoaringFilterExpr()
		require.NotNil(t, rfe, "an MRB1 blob must materialize as RoaringFilterExpr")
		assert.Equal(t, schemapb.DataType_Int64, rfe.GetColumnInfo().GetDataType())
		assert.Equal(t, blob, rfe.GetBitmapBlob(), "the client-built blob must be embedded verbatim")

		// The wire node must be indistinguishable from roaring_match's.
		aliasExpr, err := ParseExpr(helper, "roaring_match(Int64Field, {rb})", mv)
		require.NoError(t, err)
		assert.Equal(t, aliasExpr.GetRoaringFilterExpr().GetBitmapBlob(), rfe.GetBitmapBlob())
	})

	t.Run("unknown magic fails closed", func(t *testing.T) {
		const secretMagic = "S3CR"
		mv := map[string]*schemapb.TemplateValue{"x": bytesTemplate([]byte(secretMagic + "-not-a-known-format"))}
		_, err := ParseExpr(helper, "membership_match(Int64Field, {x})", mv)
		require.Error(t, err)
		assert.ErrorContains(t, err, "unknown format magic")
		assert.ErrorContains(t, err, "MBF1")
		assert.ErrorContains(t, err, "MRB1")
		assert.NotContains(t, err.Error(), secretMagic, "errors must not echo caller-controlled blob bytes")
	})

	t.Run("kind-specific field domains are enforced at fill time", func(t *testing.T) {
		// VARCHAR is fine for the bloom kind...
		tvBloom, _ := bloomBytesTemplateStr(t, 0.001, "a", "b")
		_, err := ParseExpr(helper, "membership_match(VarCharField, {bf})",
			map[string]*schemapb.TemplateValue{"bf": tvBloom})
		require.NoError(t, err)

		// ...but the same surface syntax with an MRB1 blob on a VARCHAR field
		// must fail: the roaring kind is integer-only. The visitor cannot know
		// this at parse time (no magic yet), so this pins the fill-time check.
		tvRoaring, _ := roaringBytesTemplate(t, 1, 2)
		_, err = ParseExpr(helper, "membership_match(VarCharField, {rb})",
			map[string]*schemapb.TemplateValue{"rb": tvRoaring})
		require.Error(t, err)
		assert.ErrorContains(t, err, "only supports INT8/INT16/INT32/INT64")

		// And an MBF1 int-domain blob against a VARCHAR field is rejected by
		// the domain check, exactly like explicit bloom_match.
		tvInt, _ := bloomBytesTemplate(t, 0.001, 1, 2)
		_, err = ParseExpr(helper, "membership_match(VarCharField, {bf})",
			map[string]*schemapb.TemplateValue{"bf": tvInt})
		require.Error(t, err)
		assert.ErrorContains(t, err, "value domain")
	})
}

func TestMembershipMatchDeleteSafety(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("approximate kinds are delete-unsafe", func(t *testing.T) {
		tvBloom, _ := bloomBytesTemplate(t, 0.001, 1, 2, 3)
		mv := map[string]*schemapb.TemplateValue{"bf": tvBloom}
		plan, err := CreateRetrievePlan(helper, "membership_match(Int64Field, {bf})", mv)
		require.NoError(t, err)
		assert.True(t, PlanContainsMembershipFilterUnsafeForDelete(plan),
			"a deferred-unified call that lowered to bloom must stay delete-unsafe")
	})

	t.Run("exact kinds are delete-safe", func(t *testing.T) {
		tvRoaring, _ := roaringBytesTemplate(t, 1, 2, 3)
		mv := map[string]*schemapb.TemplateValue{"rb": tvRoaring}
		plan, err := CreateRetrievePlan(helper, "membership_match(Int64Field, {rb})", mv)
		require.NoError(t, err)
		assert.False(t, PlanContainsMembershipFilterUnsafeForDelete(plan),
			"an MRB1 blob lowers to the exact kind and must remain delete-safe")
	})

	t.Run("a still-deferred unified call fails closed", func(t *testing.T) {
		deferred := &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{
			FunctionName: MembershipMatchFunctionName,
		}}}
		assert.True(t, hasDeleteUnsafeMembershipFilterExpr(deferred),
			"a unified call whose kind is not yet resolved must be treated as unsafe")
	})
}

func TestMembershipMatchElementLevelGuards(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tvBloom, _ := bloomBytesTemplate(t, 0.001, 1, 2, 3)
	mv := map[string]*schemapb.TemplateValue{"bf": tvBloom}

	expectError := func(t *testing.T, expression string) {
		_, err := ParseExpr(helper, expression, mv)
		require.Error(t, err, expression)
		assert.ErrorContains(t, err, "not supported inside", expression)
	}

	expectError(t, `element_filter(struct_array, membership_match(Int64Field, {bf}))`)
	expectError(t, `element_filter(struct_array, not membership_match(Int64Field, {bf}))`)
}

func TestMembershipMatchPreflightChargesUnifiedOccurrences(t *testing.T) {
	helper := newTestSchemaHelper(t)
	pt := paramtable.Get()
	_, blob := roaringBytesTemplate(t, 1, 2, 3)
	values := map[string]*schemapb.TemplateValue{"rb": bytesTemplate(blob)}
	body := len(blob) - roaringfilter.HeaderSize

	// One occurrence fits; two occurrences of the same body exceed it — the
	// unified syntax shares the aggregate budget with its aliases.
	pt.Save(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key, strconv.Itoa(2*body-1))
	defer pt.Reset(pt.ProxyCfg.MaxMembershipFilterPlanSize.Key)

	_, err := ParseExpr(helper, "membership_match(Int64Field, {rb})", values)
	require.NoError(t, err, "one occurrence fits")

	_, err = ParseExpr(helper,
		"membership_match(Int64Field, {rb}) and membership_match(Int64Field, {rb})", values)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.ErrorContains(t, err, "before plan materialization")
}
