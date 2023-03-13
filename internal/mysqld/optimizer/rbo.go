package optimizer

import (
	"github.com/milvus-io/milvus/internal/mysqld/planner"
)

type RuleBasedOptimizer interface {
	Optimize(plan *planner.LogicalPlan) *planner.LogicalPlan
}

type defaultRBO struct{}

func (o defaultRBO) Optimize(plan *planner.LogicalPlan) *planner.LogicalPlan {
	return plan
}

func NewDefaultRBO() RuleBasedOptimizer {
	return &defaultRBO{}
}
