package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// JSON MATCH is a raw-data operation. Each predicate leaf is evaluated using
// its own literal type, so a single predicate may target heterogeneous JSON
// arrays. Mixed IN lists are normalized by the existing JSON term rewriter.
func TestExpr_Match_JSON(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	valid := []string{
		`MATCH_ANY(JSONField["path"], $ == "x")`,
		`MATCH_ANY(JSONField["path"]["color"], $ == "x" || $ == "y")`,
		`MATCH_ALL(JSONField["path"], $ != "")`,
		`MATCH_ANY(JSONField["path"], $ in ["a", "b", "c"])`,
		`MATCH_ANY(JSONField, $ > 1)`,
		`MATCH_ANY(JSONField["path"]["n"], $ > 5)`,
		`MATCH_ALL(JSONField["path"], $ > 5 && $ < 10)`,
		`MATCH_ANY(JSONField["path"], $ in [1, 2, 3])`,
		`MATCH_ANY(JSONField["path"], $ in [1, 1.5])`,
		`MATCH_ANY(JSONField["path"], $ > 5 || $ < 10.5)`,
		`MATCH_ANY(JSONField["path"], $ > 1.5 && $ < 10)`,
		`MATCH_ANY(JSONField["path"], $ == true)`,
		`MATCH_ANY(JSONField["path"], $ == true || $ == false)`,
		`MATCH_ANY(JSONField["path"], $ > "a" && $ < "z")`,
		`MATCH_ANY(JSONField["path"], $ =~ "x.*")`,
		`MATCH_LEAST(JSONField["path"], $ == "x", threshold=2)`,
		`MATCH_MOST(JSONField["path"], $ > 1, threshold=3)`,
		`MATCH_EXACT(JSONField["path"], $ == true, threshold=1)`,
		// Raw evaluation does not require one cast type for the whole tree.
		`MATCH_ANY(JSONField, $ == "x" || $ > 5)`,
		`MATCH_ANY(JSONField["path"], $ == true || $ == 1)`,
		`MATCH_ANY(JSONField["path"], $ == true || $ == "x")`,
		`MATCH_ANY(JSONField["path"], $ in [1, "a"])`,
		`match_any(JSONField["path"], $ == "x")`,
	}
	for _, expr := range valid {
		assertValidExpr(t, helper, expr)
	}

	t.Run("mixed numeric IN uses existing JSON term normalization", func(t *testing.T) {
		expr, err := ParseExpr(
			helper, `MATCH_ANY(JSONField["path"], $ in [1, 1.5])`, nil)
		assert.NoError(t, err)

		var values []*planpb.GenericValue
		var collect func(*planpb.Expr)
		collect = func(expr *planpb.Expr) {
			if expr == nil {
				return
			}
			if binary := expr.GetBinaryExpr(); binary != nil {
				collect(binary.GetLeft())
				collect(binary.GetRight())
				return
			}
			if unary := expr.GetUnaryExpr(); unary != nil {
				collect(unary.GetChild())
				return
			}
			if unaryRange := expr.GetUnaryRangeExpr(); unaryRange != nil {
				values = append(values, unaryRange.GetValue())
				return
			}
			if term := expr.GetTermExpr(); term != nil {
				values = append(values, term.GetValues()...)
			}
		}
		collect(expr.GetMatchExpr().GetPredicate())

		assert.Len(t, values, 2)
		hasInteger := false
		hasFloating := false
		for _, value := range values {
			hasInteger = hasInteger || IsInteger(value)
			hasFloating = hasFloating || IsFloating(value)
		}
		assert.True(t, hasInteger)
		assert.True(t, hasFloating)
	})
}
