package parser

import (
	"github.com/milvus-io/milvus/internal/mysqld/planner"
)

type Parser interface {
	Parse(sql string, opts ...Option) (*planner.LogicalPlan, []error, error)
}
