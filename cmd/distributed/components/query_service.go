package components

import (
	"context"
)

func NewQueryService(ctx context.Context) (*QueryService, error) {
	return nil, nil
}

type QueryService struct {
}

func (ps *QueryService) Run() error {
	return nil
}

func (ps *QueryService) Stop() error {
	return nil
}
