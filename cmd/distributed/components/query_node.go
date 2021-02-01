package components

import (
	"context"
)

func NewQueryNode(ctx context.Context) (*QueryNode, error) {
	return nil, nil
}

type QueryNode struct {
}

func (ps *QueryNode) Run() error {
	return nil
}

func (ps *QueryNode) Stop() error {
	return nil
}
