package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestSwapTempValueIsolation verifies the scoped isolation and restoration
// of paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue.
func TestSwapTempValueIsolation(t *testing.T) {
	// Baseline should be true
	initial := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	require.True(t, initial, "default EnabledOptimizeExpr must be true")

	func() {
		old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue("false")
		defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old)

		scoped := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
		require.False(t, scoped, "EnabledOptimizeExpr must be false inside scoped block")
	}()

	restored := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	require.True(t, restored, "EnabledOptimizeExpr must be restored to true after scoped defer")
}

// TestWholeArrayMembershipDeduplication_WhenOptimized verifies that:
// 1. With optimize=true, duplicate empty arrays in {arrays} deduplicate from 2 length checks to 1.
// 2. With optimize=false, duplicate empty arrays retain both 2 length checks.
func TestWholeArrayMembershipDeduplication_WhenOptimized(t *testing.T) {
	helper := newTestSchemaHelper(t)
	emptyArray := func() *schemapb.TemplateArrayValue {
		return &schemapb.TemplateArrayValue{}
	}

	for _, optimize := range []bool{false, true} {
		t.Run(map[bool]string{false: "optimize=false", true: "optimize=true"}[optimize], func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(map[bool]string{false: "false", true: "true"}[optimize])
			defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old)

			expr, err := ParseExpr(helper, "ArrayField in {arrays}", map[string]*schemapb.TemplateValue{
				"arrays": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Array, []*schemapb.TemplateArrayValue{emptyArray(), emptyArray()})),
			})
			require.NoError(t, err)
			require.NotNil(t, expr)

			var arrayLengths int
			var walk func(*planpb.Expr)
			walk = func(current *planpb.Expr) {
				if current == nil {
					return
				}
				if al := current.GetBinaryArithOpEvalRangeExpr(); al != nil &&
					al.GetArithOp() == planpb.ArithOpType_ArrayLength &&
					al.GetOp() == planpb.OpType_Equal &&
					al.GetValue().GetInt64Val() == 0 {
					arrayLengths++
				}
				if binary := current.GetBinaryExpr(); binary != nil {
					walk(binary.GetLeft())
					walk(binary.GetRight())
				}
				if unary := current.GetUnaryExpr(); unary != nil {
					walk(unary.GetChild())
				}
			}
			walk(expr)

			if optimize {
				require.Equal(t, 1, arrayLengths, "when optimize=true, A OR A must deduplicate to 1 node")
			} else {
				require.Equal(t, 2, arrayLengths, "when optimize=false, both length checks must be preserved")
			}
		})
	}
}

func newNullableArraySchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	schema := &schemapb.CollectionSchema{
		Name: "test_nullable_array",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 102, Name: "NullableArray", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// TestWholeArrayMembership_ThreeValuedLogic verifies that whole-array IN and NOT IN
// are lowered to array_length comparisons preserving column info and nullability.
func TestWholeArrayMembership_ThreeValuedLogic(t *testing.T) {
	helper := newNullableArraySchemaHelper(t)
	emptyArray := func() *schemapb.TemplateArrayValue {
		return &schemapb.TemplateArrayValue{}
	}

	for _, op := range []string{"in", "not in"} {
		t.Run(op, func(t *testing.T) {
			expr, err := ParseExpr(helper, "NullableArray "+op+" {arrays}", map[string]*schemapb.TemplateValue{
				"arrays": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Array, []*schemapb.TemplateArrayValue{emptyArray(), emptyArray()})),
			})
			require.NoError(t, err)
			require.NotNil(t, expr)

			// Whole array membership must not remain a TermExpr
			require.Nil(t, expr.GetTermExpr())

			if op == "in" {
				// With optimization enabled, should fold to a single array_length == 0 check
				ale := expr.GetBinaryArithOpEvalRangeExpr()
				require.NotNil(t, ale)
				require.Equal(t, planpb.ArithOpType_ArrayLength, ale.GetArithOp())
				require.Equal(t, planpb.OpType_Equal, ale.GetOp())
				require.Equal(t, int64(0), ale.GetValue().GetInt64Val())
				require.True(t, ale.GetColumnInfo().GetNullable(), "NullableArray column info preserves Nullable=true for 3-valued logic")
			} else {
				// For NOT IN, must wrap the check in a UnaryExpr_Not
				unary := expr.GetUnaryExpr()
				require.NotNil(t, unary)
				require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
				ale := unary.GetChild().GetBinaryArithOpEvalRangeExpr()
				require.NotNil(t, ale)
				require.Equal(t, planpb.ArithOpType_ArrayLength, ale.GetArithOp())
				require.Equal(t, planpb.OpType_Equal, ale.GetOp())
				require.Equal(t, int64(0), ale.GetValue().GetInt64Val())
				require.True(t, ale.GetColumnInfo().GetNullable(), "NullableArray column info preserves Nullable=true for 3-valued logic")
			}
		})
	}
}
