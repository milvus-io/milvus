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

func Init() {
	Params.Init()
}

func (node *WriteNode) Start() error {

	// TODO GOOSE Init Size??
	chanSize := 100
	ddChan := make(chan *ddlFlushSyncMsg, chanSize)
	insertChan := make(chan *insertFlushSyncMsg, chanSize)
	node.flushSyncService = newFlushSyncService(node.ctx, ddChan, insertChan)

	node.dataSyncService = newDataSyncService(node.ctx, ddChan, insertChan)

	go node.dataSyncService.start()
	go node.flushSyncService.start()
	return nil
}

func (node *WriteNode) Close() {
	<-node.ctx.Done()

	// close services
	if node.dataSyncService != nil {
		(*node.dataSyncService).close()
	}
}
