package rewriter_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/stretchr/testify/require"
)

// Use the observed expression shape and sizes, with entirely synthetic values.
func TestRewriteArrayContainsLargeParsedExpression(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Name: "synthetic", Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Labels", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 102, Name: "Deleted", DataType: schemapb.DataType_Bool},
		{FieldID: 103, Name: "TitleA", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
		{FieldID: 104, Name: "TitleB", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
	}})
	require.NoError(t, err)
	for _, n := range []int{2164, 2216} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			leaves := make([]string, n)
			for i := range leaves {
				leaves[i] = fmt.Sprintf(`array_contains(Labels, "synthetic_%04d")`, i)
			}
			filter := `(Deleted != true) and ((text_match(TitleA, "alpha") or text_match(TitleB, "beta")) and ((` + strings.Join(leaves[:n/2], " or ") + `) or (` + strings.Join(leaves[n/2:], " or ") + `)))`
			expr, err := parser.ParseExpr(helper, filter, nil)
			require.NoError(t, err)
			containsCount, textCount, guardCount := 0, 0, 0
			var contains *planpb.JSONContainsExpr
			walkExpr(expr, func(e *planpb.Expr) {
				if c := e.GetJsonContainsExpr(); c != nil {
					containsCount++
					contains = c
				}
				if r := e.GetUnaryRangeExpr(); r != nil {
					if r.GetOp() == planpb.OpType_TextMatch {
						textCount++
					}
					if r.GetColumnInfo().GetFieldId() == 102 {
						guardCount++
					}
				}
			})
			require.Equal(t, 1, containsCount)
			require.Equal(t, 2, textCount)
			require.Equal(t, 1, guardCount)
			require.Equal(t, planpb.JSONContainsExpr_ContainsAny, contains.GetOp())
			require.True(t, contains.GetColumnInfo().GetNullable())
			require.Len(t, contains.GetElements(), n)
			values := make(map[string]bool, n)
			for i, v := range contains.GetElements() {
				require.Equal(t, fmt.Sprintf("synthetic_%04d", i), v.GetStringVal())
				values[v.GetStringVal()] = true
			}
			// Exact union membership for every synthetic source value and a miss.
			for i := 0; i < n; i++ {
				require.True(t, values[fmt.Sprintf("synthetic_%04d", i)])
			}
			require.False(t, values["not_in_filter"])
		})
	}
}

func TestRewriteArrayContainsEmptyLists(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	for _, filter := range []string{
		`array_contains_any(NullableArrayInt, []) or array_contains(NullableArrayInt, 1)`,
		`array_contains_all(NullableArrayInt, []) and array_contains(NullableArrayInt, 1)`,
	} {
		expr, err := parser.ParseExpr(helper, filter, nil)
		require.NoError(t, err)
		require.NotNil(t, expr.GetJsonContainsExpr())
		require.True(t, expr.GetJsonContainsExpr().GetColumnInfo().GetNullable())
		require.Equal(t, []int64{1}, arrayContainsIntegrationIntValues(expr.GetJsonContainsExpr().GetElements()))
	}
}
