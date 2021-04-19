package components

import (
	"context"

	grpcproxynode "github.com/zilliztech/milvus-distributed/internal/distributed/proxynode"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type ProxyNode struct {
	svr *grpcproxynode.Server
}

func NewProxyNode(ctx context.Context, factory msgstream.Factory) (*ProxyNode, error) {
	var err error
	n := &ProxyNode{}

	svr, err := grpcproxynode.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil
}

func (n *ProxyNode) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (n *ProxyNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}
