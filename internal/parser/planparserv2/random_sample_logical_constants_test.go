package planparserv2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRandomSampleOrBooleanConstants(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const sample = "random_sample(.5)"
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, tc := range []struct{ filter, want string }{
				{sample + " OR false", sample},
				{"false OR " + sample, sample},
				{"false OR (" + sample + " OR false)", sample},
				{sample + " OR true", "true"},
				{"true OR " + sample, "true"},
				{"false OR true OR " + sample, "true"},
				{"false OR (Int64Field == 1 AND " + sample + ")", "Int64Field == 1 AND " + sample},
				{"(Int64Field == 1 AND " + sample + ") OR false", "Int64Field == 1 AND " + sample},
				{"Int64Field == 1 AND ((Int32Field == 2 AND " + sample + ") OR false)", "Int64Field == 1 AND Int32Field == 2 AND " + sample},
				{"Int64Field == 1 AND (false OR (Int32Field == 2 AND " + sample + "))", "Int64Field == 1 AND Int32Field == 2 AND " + sample},
				{"Int64Field == 1 AND ((Int32Field == 2 AND " + sample + ") OR false) AND true", "Int64Field == 1 AND Int32Field == 2 AND " + sample},
			} {
				t.Run(tc.filter, func(t *testing.T) {
					want, err := ParseExpr(helper, tc.want, nil)
					require.NoError(t, err)
					got, err := ParseExpr(helper, tc.filter, nil)
					require.NoError(t, err)
					require.True(t, proto.Equal(want, got), "folding must preserve the sampler and every predicate: want=%v got=%v", want, got)
				})
			}
		})
	}
}

func TestRandomSampleOrBooleanConstantsValidateTemplates(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const sample = "random_sample(.5)"
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, tc := range []struct{ filter, want string }{
				{"false OR (Int64Field == {value} AND " + sample + ")", "Int64Field == {value} AND " + sample},
				{"(Int64Field == {value} AND " + sample + ") OR false", "Int64Field == {value} AND " + sample},
				{"true OR (Int64Field == {value} AND " + sample + ")", "true"},
				{"(Int64Field == {value} AND " + sample + ") OR true", "true"},
				{"Int32Field == 2 AND (false OR (Int64Field == {value} AND " + sample + "))", "Int32Field == 2 AND Int64Field == {value} AND " + sample},
			} {
				t.Run(tc.filter, func(t *testing.T) {
					templateExpr, err := ParseExprTemplate(helper, tc.filter, nil)
					require.NoError(t, err)
					require.True(t, templateExpr.GetIsTemplate())
					_, err = ParseExpr(helper, tc.filter, nil)
					require.Error(t, err, "constant folding must not hide a missing template")
					_, err = ParseExpr(helper, tc.filter, map[string]*schemapb.TemplateValue{
						"value": {Val: &schemapb.TemplateValue_StringVal{StringVal: "invalid integer"}},
					})
					require.Error(t, err, "constant folding must not hide an invalid template")
					values := map[string]*schemapb.TemplateValue{
						"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}},
					}
					got, err := ParseExpr(helper, tc.filter, values)
					require.NoError(t, err)
					want, err := ParseExpr(helper, tc.want, values)
					require.NoError(t, err)
					require.True(t, proto.Equal(want, got), "want=%v got=%v", want, got)
				})
			}
		})
	}
}

func TestRandomSampleStillRejectsPredicateOr(t *testing.T) {
	helper := newTestSchemaHelper(t)
	for _, filter := range []string{
		"random_sample(.5) OR Int64Field > 0",
		"Int64Field > 0 OR random_sample(.5)",
		"true OR (random_sample(.5) OR Int64Field > 0)",
		"false OR (Int64Field > 0 AND random_sample(.5)) OR Int32Field > 0",
		"random_sample(.5) OR false OR random_sample(.5)",
	} {
		_, err := ParseExpr(helper, filter, nil)
		require.Error(t, err, filter)
	}
}
