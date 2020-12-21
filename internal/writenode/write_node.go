package writenode

import (
	"context"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"
)

type WriteNode struct {
	ctx             context.Context
	WriteNodeID     uint64
	dataSyncService *dataSyncService

	metaTable *metaTable
}

func NewWriteNode(ctx context.Context, writeNodeID uint64) (*WriteNode, error) {

	node := &WriteNode{
		ctx:             ctx,
		WriteNodeID:     writeNodeID,
		dataSyncService: nil,
	}

	etcdAddress := Params.EtcdAddress
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		return nil, err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	metaKV, err2 := NewMetaTable(etcdKV)
	if err2 != nil {
		return nil, err
	}
	node.metaTable = metaKV

	return node, nil
}

func (node *WriteNode) Start() {
	node.dataSyncService = newDataSyncService(node.ctx)
	// node.searchService = newSearchService(node.ctx)
	// node.metaService = newMetaService(node.ctx)
	// node.statsService = newStatsService(node.ctx)

	go node.dataSyncService.start()
	// go node.searchService.start()
	// go node.metaService.start()
	// node.statsService.start()
}

func (node *WriteNode) Close() {
	<-node.ctx.Done()
	// free collectionReplica
	// (*node.replica).freeAll()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}
	// if node.searchService != nil {
	//     (*node.searchService).close()
	// }
	// if node.statsService != nil {
	//     (*node.statsService).close()
	// }
}
