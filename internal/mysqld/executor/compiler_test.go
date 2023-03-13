package executor

import (
	"testing"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/stretchr/testify/assert"
)

func Test_defaultCompiler_Compile(t *testing.T) {
	plan := &planner.LogicalPlan{
		Node: planner.NewNodeConstant("20230306", planner.WithStringLiteral("20230306")),
	}
	c := NewDefaultCompiler()
	physicalPlan, err := c.Compile(plan)
	assert.NoError(t, err)
	constant := physicalPlan.Node.(*planner.NodeConstant)
	assert.Equal(t, "20230306", constant.StringLiteral.Unwrap())
}
