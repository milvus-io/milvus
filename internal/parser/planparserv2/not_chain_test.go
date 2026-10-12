package planparserv2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func notChain(n int, operand string, parens bool) string {
	if parens {
		return strings.Repeat("NOT (!(", n/2) + strings.Repeat("NOT (", n%2) + operand + strings.Repeat(")", n)
	}
	return strings.Repeat("NOT ", n) + "(" + operand + ")"
}

func TestConsecutiveNotParity(t *testing.T) {
	helper := newTestSchemaHelper(t)
	field, err := helper.GetFieldFromName("Int64Field")
	require.NoError(t, err)
	field.Nullable = true
	const operand = "Int64Field > 0"
	base, err := ParseExprTemplate(helper, operand, nil)
	require.NoError(t, err)
	require.True(t, base.GetUnaryRangeExpr().GetColumnInfo().GetNullable())
	for _, n := range []int{1, 2, 3, 64, 65} {
		for _, parens := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/parens=%t", n, parens), func(t *testing.T) {
				expr, err := ParseExprTemplate(helper, notChain(n, operand, parens), nil)
				require.NoError(t, err)
				if n%2 != 0 {
					require.NotNil(t, expr.GetUnaryExpr())
					require.Equal(t, planpb.UnaryExpr_Not, expr.GetUnaryExpr().GetOp())
					expr = expr.GetUnaryExpr().GetChild()
				}
				// Retain the nullable predicate unchanged; NOT UNKNOWN is UNKNOWN
				// for either parity, rather than a valid boolean constant.
				require.True(t, proto.Equal(base, expr), "expected at most one NOT with the original nullable operand")
			})
		}
	}

	for _, literal := range []string{"true", "false"} {
		for _, n := range []int{2, 3} {
			expr, err := ParseExpr(helper, notChain(n, literal, true), nil)
			require.NoError(t, err)
			want := literal == "true"
			if n%2 != 0 {
				want = !want
			}
			require.True(t, hasBoolValue(expr, want))
		}
	}
}

func TestConsecutiveNotTemplateValidation(t *testing.T) {
	helper := newTestSchemaHelper(t)
	for _, n := range []int{2, 3} {
		filter := notChain(n, "Int64Field > {value}", true)
		expr, err := ParseExprTemplate(helper, filter, nil)
		require.NoError(t, err)
		require.True(t, expr.GetIsTemplate())
		_, err = ParseExpr(helper, filter, nil)
		require.Error(t, err)
		_, err = ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
			"value": {Val: &schemapb.TemplateValue_StringVal{StringVal: "not an integer"}},
		})
		require.Error(t, err)
		expr, err = ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
			"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 7}},
		})
		require.NoError(t, err)
		if n%2 != 0 {
			require.NotNil(t, expr.GetUnaryExpr())
			expr = expr.GetUnaryExpr().GetChild()
		}
		require.NotNil(t, expr.GetUnaryRangeExpr())
		require.Equal(t, int64(7), expr.GetUnaryRangeExpr().GetValue().GetInt64Val())
	}
}

func TestConsecutiveNotRejectsInvalidOperands(t *testing.T) {
	helper := newTestSchemaHelper(t)
	for _, operand := range []string{
		"1", "-9223372036854775808", `"text"`, "Int64Field", "BoolField", "~Int64Field",
		"random_sample(0.5)", "element_filter(struct_array, $[sub_int] > 0)",
	} {
		for _, n := range []int{2, 3} {
			_, err := ParseExpr(helper, notChain(n, operand, true), nil)
			require.Error(t, err, "even NOT parity must not bypass operand validation: %s", operand)
		}
	}
}
