package optimizer

import (
	"testing"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/stretchr/testify/assert"
)

func Test_defaultCBO_Optimize(t *testing.T) {
	cbo := NewDefaultCBO()
	plan := &planner.PhysicalPlan{}
	optimized := cbo.Optimize(plan)
	assert.Same(t, plan, optimized)
}
