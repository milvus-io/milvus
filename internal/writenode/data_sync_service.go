package writenode

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type dataSyncService struct {
	ctx context.Context
	fg  *flowgraph.TimeTickedFlowGraph
}

func newDataSyncService(ctx context.Context) *dataSyncService {

	return &dataSyncService{
		ctx: ctx,
		fg:  nil,
	}
}

func (dsService *dataSyncService) start() {
	dsService.initNodes()
	dsService.fg.Start()
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		dsService.fg.Close()
	}
}

func (dsService *dataSyncService) initNodes() {
	// TODO: add delete pipeline support

	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	var dmStreamNode Node = newDmInputNode(dsService.ctx)
	var filterDmNode Node = newFilteredDmNode()
	var writeNode Node = newWriteNode()
	var serviceTimeNode Node = newServiceTimeNode()

	dsService.fg.AddNode(&dmStreamNode)
	dsService.fg.AddNode(&filterDmNode)
	dsService.fg.AddNode(&writeNode)
	dsService.fg.AddNode(&serviceTimeNode)

	var err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", dmStreamNode.Name())
	}

	err = dsService.fg.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{writeNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", filterDmNode.Name())
	}

	err = dsService.fg.SetEdges(writeNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", writeNode.Name())
	}

	err = dsService.fg.SetEdges(serviceTimeNode.Name(),
		[]string{writeNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", serviceTimeNode.Name())
	}
}
