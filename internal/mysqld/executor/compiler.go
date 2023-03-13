package executor

import "github.com/milvus-io/milvus/internal/mysqld/planner"

type Compiler interface {
	Compile(plan *planner.LogicalPlan) (*planner.PhysicalPlan, error)
}

type defaultCompiler struct{}

func (c defaultCompiler) Compile(plan *planner.LogicalPlan) (*planner.PhysicalPlan, error) {
	return &planner.PhysicalPlan{Node: plan.Node}, nil
}

func NewDefaultCompiler() Compiler {
	return &defaultCompiler{}
}
