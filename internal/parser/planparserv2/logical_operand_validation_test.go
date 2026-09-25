package planparserv2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLogicalOperandsRejectInvalidBeforeConstantFolding(t *testing.T) {
	helper := newTestSchemaHelper(t)
	values := map[string]*schemapb.TemplateValue{"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}}}
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, tc := range []struct{ name, filter string }{
				{"grouped_scalar_and", "false AND (Int64Field AND Int64Field > 0)"},
				{"grouped_scalar_and_reversed", "(Int64Field AND Int64Field > 0) AND false"},
				{"grouped_random_or", "true OR (random_sample(.5) OR Int64Field > 0)"},
				{"grouped_random_or_reversed", "(random_sample(.5) OR Int64Field > 0) OR true"},
				{"random_before_predicate", "false AND (random_sample(.5) AND Int64Field > 0)"},
				{"element_before_or_predicate", "true OR (element_filter(struct_array, $[sub_int] > 0) OR Int64Field > 0)"},
				{"element_before_and_predicate", "false AND (element_filter(struct_array, $[sub_int] > 0) AND Int64Field > 0)"},
				{"bare_bool_left", "BoolField AND false"},
				{"bare_bool_right", "false AND BoolField"},
				{"arithmetic_right", "true OR (Int64Field + 1)"},
				{"string_field_left", "VarCharField OR true"},
				{"json_field_right", "false AND JSONField"},
				{"integer_literal", "false AND 1"},
				{"string_literal", `true OR "text"`},
				{"template_scalar", "false AND (Int64Field AND Int64Field > {value})"},
				{"template_wrapper", "true OR (random_sample(.5) OR Int64Field > {value})"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// A constant result must not make an invalid operand or wrapper
					// position legal. Supply the template so its absence cannot mask it.
					expr, err := ParseExpr(helper, tc.filter, values)
					require.Error(t, err, tc.filter)
					require.Nil(t, expr)
				})
			}
		})
	}
}

func TestLogicalOperandsKeepValidTerminalWrappers(t *testing.T) {
	helper := newTestSchemaHelper(t)
	int64Field, err := helper.GetFieldFromName("Int64Field")
	require.NoError(t, err)
	int32Field, err := helper.GetFieldFromName("Int32Field")
	require.NoError(t, err)
	wantPredicates := map[int64]int64{int64Field.GetFieldID(): 1, int32Field.GetFieldID(): 2}
	values := map[string]*schemapb.TemplateValue{"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}}}
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, wrapper := range []string{"random_sample(.5)", "element_filter(struct_array, $[sub_int] > 0)"} {
				for _, prefix := range []string{"Int64Field == 1", "true AND Int64Field == {value}"} {
					filter := prefix + " AND (Int32Field == 2 AND " + wrapper + ")"
					t.Run(filter, func(t *testing.T) {
						expr, err := ParseExpr(helper, filter, values)
						require.NoError(t, err)
						var predicate *planpb.Expr
						if strings.HasPrefix(wrapper, "random_sample") {
							require.NotNil(t, expr.GetRandomSampleExpr())
							require.Equal(t, float32(0.5), expr.GetRandomSampleExpr().GetSampleFactor())
							predicate = expr.GetRandomSampleExpr().GetPredicate()
						} else {
							require.NotNil(t, expr.GetElementFilterExpr())
							require.NotNil(t, expr.GetElementFilterExpr().GetElementExpr())
							predicate = expr.GetElementFilterExpr().GetPredicate()
						}
						gotPredicates := make(map[int64]int64)
						stack := []*planpb.Expr{predicate}
						for len(stack) > 0 {
							current := stack[len(stack)-1]
							stack = stack[:len(stack)-1]
							require.NotNil(t, current)
							if binary := current.GetBinaryExpr(); binary != nil {
								require.Equal(t, planpb.BinaryExpr_LogicalAnd, binary.GetOp())
								stack = append(stack, binary.GetLeft(), binary.GetRight())
								continue
							}
							comparison := current.GetUnaryRangeExpr()
							require.NotNil(t, comparison)
							require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
							fieldID := comparison.GetColumnInfo().GetFieldId()
							require.NotContains(t, gotPredicates, fieldID)
							gotPredicates[fieldID] = comparison.GetValue().GetInt64Val()
						}
						require.Equal(t, wantPredicates, gotPredicates, "both predicates before the terminal wrapper must survive")
						if strings.Contains(filter, "{value}") {
							_, err = ParseExpr(helper, filter, nil)
							require.Error(t, err)
							_, err = ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
								"value": {Val: &schemapb.TemplateValue_StringVal{StringVal: "not an integer"}},
							})
							require.Error(t, err)
						}
					})
				}
			}
			// Trailing boolean constants do not move a wrapper before another
			// predicate; preserve these previously accepted spellings.
			expr, err := ParseExpr(helper, "random_sample(.5) AND true", nil)
			require.NoError(t, err)
			require.NotNil(t, expr.GetRandomSampleExpr())
			expr, err = ParseExpr(helper, "element_filter(struct_array, $[sub_int] > 0) OR false", nil)
			require.NoError(t, err)
			require.NotNil(t, expr.GetElementFilterExpr())
		})
	}
}

func TestElementFilterKeepsNestedPredicateAcrossOrFalse(t *testing.T) {
	helper := newTestSchemaHelper(t)
	int64Field, err := helper.GetFieldFromName("Int64Field")
	require.NoError(t, err)
	int32Field, err := helper.GetFieldFromName("Int32Field")
	require.NoError(t, err)
	wantPredicates := map[int64]int64{int64Field.GetFieldID(): 1, int32Field.GetFieldID(): 2}

	assertPredicates := func(t *testing.T, expr *planpb.Expr) {
		t.Helper()
		elementFilter := expr.GetElementFilterExpr()
		require.NotNil(t, elementFilter)
		require.NotNil(t, elementFilter.GetElementExpr())
		gotPredicates := make(map[int64]int64)
		stack := []*planpb.Expr{elementFilter.GetPredicate()}
		for len(stack) > 0 {
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			require.NotNil(t, current)
			if binary := current.GetBinaryExpr(); binary != nil {
				require.Equal(t, planpb.BinaryExpr_LogicalAnd, binary.GetOp())
				stack = append(stack, binary.GetLeft(), binary.GetRight())
				continue
			}
			comparison := current.GetUnaryRangeExpr()
			require.NotNil(t, comparison)
			require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
			fieldID := comparison.GetColumnInfo().GetFieldId()
			require.NotContains(t, gotPredicates, fieldID)
			gotPredicates[fieldID] = comparison.GetValue().GetInt64Val()
		}
		require.Equal(t, wantPredicates, gotPredicates, "OR false must not discard the inner document predicate")
	}

	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, tc := range []struct{ name, filter string }{
				{"false_right", "Int64Field == 1 AND ((Int32Field == 2 AND element_filter(struct_array, $[sub_int] > 0)) OR false)"},
				{"false_left", "Int64Field == 1 AND (false OR (Int32Field == 2 AND element_filter(struct_array, $[sub_int] > 0)))"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := ParseExpr(helper, tc.filter, nil)
					require.NoError(t, err)
					assertPredicates(t, expr)
				})
				t.Run(tc.name+"/missing_template", func(t *testing.T) {
					filter := strings.Replace(tc.filter, "Int32Field == 2", "Int32Field == {value}", 1)
					templateExpr, err := ParseExprTemplate(helper, filter, nil)
					require.NoError(t, err)
					require.True(t, templateExpr.GetIsTemplate())
					require.NotNil(t, templateExpr.GetElementFilterExpr())
					require.True(t, templateExpr.GetElementFilterExpr().GetPredicate().GetIsTemplate())
					expr, err := ParseExpr(helper, filter, nil)
					require.Error(t, err, "a discarded predicate must not hide a missing template value")
					require.Nil(t, expr)
				})
				t.Run(tc.name+"/supplied_template", func(t *testing.T) {
					filter := strings.Replace(tc.filter, "Int32Field == 2", "Int32Field == {value}", 1)
					expr, err := ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
						"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 2}},
					})
					require.NoError(t, err)
					assertPredicates(t, expr)
				})
			}
		})
	}
}

func TestElementFilterRejectsDocumentLevelOrButKeepsFoldableCases(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const (
		a = "Int64Field == 1"
		b = "Int32Field == 2"
		e = "element_filter(struct_array, $[sub_int] > 0)"
	)
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })

			for _, tc := range []struct{ name, filter string }{
				{"document_or_element", a + " OR " + e},
				{"element_or_document", e + " OR " + a},
				{"document_or_and_element", a + " OR (" + b + " AND " + e + ")"},
				{"document_or_folded_element", a + " OR (" + e + " OR false)"},
				{"nested_element_and_document", "(" + a + " OR " + e + ") AND " + b},
				{"nested_element_in_root_predicate", "(" + a + " OR " + e + ") AND " + e},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := ParseExpr(helper, tc.filter, nil)
					require.Error(t, err, tc.filter)
					require.Nil(t, expr)
				})
			}

			for _, tc := range []struct{ name, filter string }{
				{"document_or_document_then_element", "(" + a + " OR " + b + ") AND " + e},
				{"false_or_element", "false OR " + e},
				{"element_or_false", e + " OR false"},
				{"or_inside_element", "element_filter(struct_array, $[sub_int] > 0 OR $[sub_int] < -1)"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := ParseExpr(helper, tc.filter, nil)
					require.NoError(t, err, tc.filter)
					require.NotNil(t, expr.GetElementFilterExpr(), tc.filter)
				})
			}

			t.Run("true_or_templated_element", func(t *testing.T) {
				filter := "true OR element_filter(struct_array, $[sub_int] > {value})"
				expr, err := ParseExpr(helper, filter, nil)
				require.Error(t, err, "constant folding must not hide a missing template value")
				require.Nil(t, expr)

				expr, err = ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
					"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 0}},
				})
				require.NoError(t, err)
				require.NotNil(t, expr.GetAlwaysTrueExpr())
			})
		})
	}
}
