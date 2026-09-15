package planparserv2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestExpr_NestingDepthLimit(t *testing.T) {
	paramtable.Init()

	schema := newTestSchema(false)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	t.Run("expressions within the default limit still parse", func(t *testing.T) {
		nested := strings.Repeat("(", 100) + "FieldID > 0" + strings.Repeat(")", 100)
		_, err := CreateRetrievePlan(helper, nested, nil)
		assert.NoError(t, err)

		unary := strings.Repeat("not ", 100) + "(FieldID > 0)"
		_, err = CreateRetrievePlan(helper, unary, nil)
		assert.NoError(t, err)
	})

	t.Run("depth beyond the configured limit is rejected", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxExpressionDepth.Key, "50")
		defer paramtable.Get().Remove(paramtable.Get().ProxyCfg.MaxExpressionDepth.Key)

		nested := strings.Repeat("(", 51) + "FieldID > 0" + strings.Repeat(")", 51)
		_, err := CreateRetrievePlan(helper, nested, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nesting depth")

		// a deeply nested unary chain is rejected as well
		unary := strings.Repeat("not ", 51) + "(FieldID > 0)"
		_, err = CreateRetrievePlan(helper, unary, nil)
		assert.Error(t, err)
	})

	t.Run("an over-limit value is clamped before use", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxExpressionDepth.Key, "1000000")
		defer paramtable.Get().Remove(paramtable.Get().ProxyCfg.MaxExpressionDepth.Key)

		assert.Equal(t, maxExprNestingDepthLimit, exprNestingLimit())
	})
}
