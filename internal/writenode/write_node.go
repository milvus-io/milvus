package writenode

import (
	"context"
)

type WriteNode struct {
	ctx             context.Context
	WriteNodeID     uint64
	dataSyncService *dataSyncService
}

func NewWriteNode(ctx context.Context, writeNodeID uint64) *WriteNode {

	return &WriteNode{
		ctx:             ctx,
		WriteNodeID:     writeNodeID,
		dataSyncService: nil,
	}
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
