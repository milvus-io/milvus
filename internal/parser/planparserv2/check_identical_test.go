package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestCheckIdentical(t *testing.T) {
	schema := newTestSchema()
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStr1 := `not (((Int64Field > 0) and (FloatField <= 20.0)) or ((Int32Field in [1, 2, 3]) and (VarCharField < "str")))`
	exprStr2 := `Int32Field in [1, 2, 3]`

	expr1, err := ParseExpr(helper, exprStr1)
	assert.NoError(t, err)
	expr2, err := ParseExpr(helper, exprStr2)
	assert.NoError(t, err)

	assert.True(t, CheckIdentical(expr1, expr1))
	assert.True(t, CheckIdentical(expr2, expr2))
	assert.False(t, CheckIdentical(expr1, expr2))
}
