package indexnode

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
)

type mockIndexNodeComponent struct {
	IndexNode
}

var _ types.IndexNodeComponent = &mockIndexNodeComponent{}

func NewMockIndexNodeComponent(ctx context.Context) (types.IndexNodeComponent, error) {
	Params.Init()
	factory := &mockFactory{
		chunkMgr: &mockChunkmgr{},
	}

	node, err := NewIndexNode(ctx, factory)
	if err != nil {
		return nil, err
	}

	startEmbedEtcd()
	etcdCli := getEtcdClient()
	node.SetEtcdClient(etcdCli)
	node.storageFactory = &mockStorageFactory{}
	if err := node.Init(); err != nil {
		return nil, err
	}

	if err := node.Start(); err != nil {
		return nil, err
	}

	if err := node.Register(); err != nil {
		return nil, err
	}
	return &mockIndexNodeComponent{
		IndexNode: *node,
	}, nil
}
