package querynodeimp

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type dataSyncService struct {
	ctx context.Context
	fg  *flowgraph.TimeTickedFlowGraph

	replica collectionReplica
}

func newDataSyncService(ctx context.Context, replica collectionReplica) *dataSyncService {

	return &dataSyncService{
		ctx: ctx,
		fg:  nil,

		replica: replica,
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

	var dmStreamNode node = newDmInputNode(dsService.ctx)
	var ddStreamNode node = newDDInputNode(dsService.ctx)

	var filterDmNode node = newFilteredDmNode()
	var ddNode node = newDDNode(dsService.replica)

	var insertNode node = newInsertNode(dsService.replica)
	var serviceTimeNode node = newServiceTimeNode(dsService.replica)
	var gcNode node = newGCNode(dsService.replica)

	dsService.fg.AddNode(&dmStreamNode)
	dsService.fg.AddNode(&ddStreamNode)

	dsService.fg.AddNode(&filterDmNode)
	dsService.fg.AddNode(&ddNode)

	dsService.fg.AddNode(&insertNode)
	dsService.fg.AddNode(&serviceTimeNode)
	dsService.fg.AddNode(&gcNode)

	// dmStreamNode
	var err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", dmStreamNode.Name())
	}

	// ddStreamNode
	err = dsService.fg.SetEdges(ddStreamNode.Name(),
		[]string{},
		[]string{ddNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", ddStreamNode.Name())
	}

	// filterDmNode
	err = dsService.fg.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name(), ddNode.Name()},
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", filterDmNode.Name())
	}

	// ddNode
	err = dsService.fg.SetEdges(ddNode.Name(),
		[]string{ddStreamNode.Name()},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", ddNode.Name())
	}

	// insertNode
	err = dsService.fg.SetEdges(insertNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", insertNode.Name())
	}

	// serviceTimeNode
	err = dsService.fg.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{gcNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", serviceTimeNode.Name())
	}

	// gcNode
	err = dsService.fg.SetEdges(gcNode.Name(),
		[]string{serviceTimeNode.Name()},
		[]string{})
	if err != nil {
		log.Fatal("set edges failed in node:", gcNode.Name())
	}
}
