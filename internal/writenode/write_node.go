package writenode

import (
	"context"
)

type WriteNode struct {
	ctx              context.Context
	WriteNodeID      uint64
	dataSyncService  *dataSyncService
	flushSyncService *flushSyncService
}

func NewWriteNode(ctx context.Context, writeNodeID uint64) *WriteNode {

	node := &WriteNode{
		ctx:              ctx,
		WriteNodeID:      writeNodeID,
		dataSyncService:  nil,
		flushSyncService: nil,
	}

	return node
}

func (node *WriteNode) Start() {

	ddChan := make(chan *ddlFlushSyncMsg, 5)
	insertChan := make(chan *insertFlushSyncMsg, 5)
	node.flushSyncService = newFlushSyncService(node.ctx, ddChan, insertChan)

	node.dataSyncService = newDataSyncService(node.ctx, ddChan, insertChan)

	go node.dataSyncService.start()
	go node.flushSyncService.start()
}

func (node *WriteNode) Close() {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}
}
