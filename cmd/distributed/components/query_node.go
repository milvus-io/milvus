package components

import (
	"context"

	grpcquerynode "github.com/zilliztech/milvus-distributed/internal/distributed/querynode"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type QueryNode struct {
	ctx context.Context
	svr *grpcquerynode.Server
}

func NewQueryNode(ctx context.Context, factory msgstream.Factory) (*QueryNode, error) {

	svr, err := grpcquerynode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &QueryNode{
		ctx: ctx,
		svr: svr,
	}, nil

}

func (q *QueryNode) Run() error {
	if err := q.svr.Run(); err != nil {
		panic(err)
	}
	return nil
}

func (q *QueryNode) Stop() error {
	if err := q.svr.Stop(); err != nil {
		return err
	}
	return nil
}
