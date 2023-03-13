package optimizer

import "github.com/milvus-io/milvus/internal/mysqld/planner"

type CostBasedOptimizer interface {
	Optimize(plan *planner.PhysicalPlan) *planner.PhysicalPlan
}

type defaultCBO struct{}

func (o defaultCBO) Optimize(plan *planner.PhysicalPlan) *planner.PhysicalPlan {
	return plan
}

func NewDefaultCBO() CostBasedOptimizer {
	return &defaultCBO{}
}
