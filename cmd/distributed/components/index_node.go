package components

import (
	"context"

	grpcindexnode "github.com/zilliztech/milvus-distributed/internal/distributed/indexnode"
)

type IndexNode struct {
	svr *grpcindexnode.Server
}

func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	n := &IndexNode{}
	svr, err := grpcindexnode.NewServer(ctx)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil

}
func (n *IndexNode) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
	return nil
}
func (n *IndexNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}
