package rewriter_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func arrayFloat32Schema(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Name: "array_float32_rewrite",
		Fields: []*schemapb.FieldSchema{{
			FieldID: 201, Name: "ArrayFloat32", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_Float, Nullable: true,
		}},
	})
	require.NoError(t, err)
	return helper
}

func parseArrayFloat32Plan(t *testing.T, helper *typeutil.SchemaHelper, filter string, optimize bool) *planpb.Expr {
	t.Helper()
	old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
	defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old)
	expr, err := parser.ParseExpr(helper, filter, nil)
	require.NoError(t, err)
	return expr
}

func TestRewriteArrayFloat32IndexUsesInOptimizations(t *testing.T) {
	helper := arrayFloat32Schema(t)
	singleton := parseArrayFloat32Plan(t, helper, "ArrayFloat32[0] in [1.5]", true).GetUnaryRangeExpr()
	require.NotNil(t, singleton)
	require.Equal(t, planpb.OpType_Equal, singleton.GetOp())
	require.Equal(t, 1.5, singleton.GetValue().GetFloatVal())

	for _, tc := range []struct {
		filter string
		values []float64
	}{
		{"ArrayFloat32[0] == 1.5 OR ArrayFloat32[0] == 2.5", []float64{1.5, 2.5}},
		{"ArrayFloat32[0] in [1.5, 2.5] OR ArrayFloat32[0] == 3.5", []float64{1.5, 2.5, 3.5}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			term := parseArrayFloat32Plan(t, helper, tc.filter, true).GetTermExpr()
			require.NotNil(t, term)
			require.Equal(t, []string{"0"}, term.GetColumnInfo().GetNestedPath())
			values := make([]float64, 0, len(term.GetValues()))
			for _, value := range term.GetValues() {
				values = append(values, value.GetFloatVal())
			}
			require.ElementsMatch(t, tc.values, values)
		})
	}
}

func TestRewriteArrayFloat32KeepsWholeArrayNormalization(t *testing.T) {
	helper := arrayFloat32Schema(t)
	for _, optimize := range []bool{false, true} {
		expr := parseArrayFloat32Plan(t, helper, "ArrayFloat32 in [[1.5], [2.5]]", optimize)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		require.Equal(t, planpb.BinaryExpr_LogicalOr, binary.GetOp())
		for _, child := range []*planpb.Expr{binary.GetLeft(), binary.GetRight()} {
			comparison := child.GetUnaryRangeExpr()
			require.NotNil(t, comparison)
			require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
			require.NotNil(t, comparison.GetValue().GetArrayVal())
			require.Empty(t, comparison.GetColumnInfo().GetNestedPath())
		}
	}
}

func TestRewriteArrayFloat32ElementLevelStillUsesScalarOptimization(t *testing.T) {
	input := &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 201, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float, IsElementLevel: true},
		Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_FloatVal{FloatVal: 16777217}}},
	}}}
	result := rewriter.RewriteExprWithConfig(input, true)
	require.NotNil(t, result.GetUnaryRangeExpr())
	require.Equal(t, planpb.OpType_Equal, result.GetUnaryRangeExpr().GetOp())
	require.True(t, result.GetUnaryRangeExpr().GetColumnInfo().GetIsElementLevel())
}
