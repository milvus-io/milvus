package indexnode

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type mockIndexNodeComponent struct {
	*IndexNode
}

var _ types.IndexNodeComponent = &mockIndexNodeComponent{}

func NewMockIndexNodeComponent(ctx context.Context) (types.IndexNodeComponent, error) {
	paramtable.Init()
	factory := &mockFactory{
		chunkMgr: &mockChunkmgr{},
	}

	node := NewIndexNode(ctx, factory)

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
		IndexNode: node,
	}, nil
}
