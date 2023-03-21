package optimizer

import (
	"testing"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/stretchr/testify/assert"
)

func Test_defaultRBO_Optimize(t *testing.T) {
	rbo := NewDefaultRBO()
	plan := &planner.LogicalPlan{}
	optimized := rbo.Optimize(plan)
	assert.Same(t, plan, optimized)
}
