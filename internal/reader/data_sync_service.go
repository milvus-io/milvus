package reader

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type dataSyncService struct {
	ctx       context.Context
	pulsarURL string
	fg        *flowgraph.TimeTickedFlowGraph

	// input streams
	dmStream *msgstream.MsgStream
	//	ddStream *msgstream.MsgStream
	//	k2sStream *msgstream.MsgStream

	node *QueryNode
}

func newDataSyncService(ctx context.Context, node *QueryNode, pulsarURL string) *dataSyncService {

	return &dataSyncService{
		ctx:       ctx,
		pulsarURL: pulsarURL,
		fg:        nil,

		dmStream: nil,

		node: node,
	}
}

func (dsService *dataSyncService) start() {
	dsService.initNodes()
	dsService.fg.Start()
}

func (dsService *dataSyncService) close() {
	dsService.fg.Close()
	(*dsService.dmStream).Close()
}

func (dsService *dataSyncService) initNodes() {
	// TODO: add delete pipeline support

	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	var dmStreamNode Node = newDmInputNode(dsService.ctx, dsService.pulsarURL)
	var filterDmNode Node = newFilteredDmNode()
	var insertNode Node = newInsertNode(dsService.node.container)
	var serviceTimeNode Node = newServiceTimeNode(dsService.node)

	dsService.fg.AddNode(&dmStreamNode)
	dsService.fg.AddNode(&filterDmNode)
	dsService.fg.AddNode(&insertNode)
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
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", filterDmNode.Name())
	}

	err = dsService.fg.SetEdges(insertNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", insertNode.Name())
	}

	err = dsService.fg.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", serviceTimeNode.Name())
	}

	dsService.setDmStream(&dmStreamNode)
}

func (dsService *dataSyncService) setDmStream(node *Node) {
	if (*node).IsInputNode() {
		inStream, ok := (*node).(*InputNode)
		dsService.dmStream = inStream.InStream()
		if !ok {
			log.Fatal("Invalid inputNode")
		}
	} else {
		log.Fatal("stream set failed")
	}
}

func (dsService *dataSyncService) setDdStream(node *Node)  {}
func (dsService *dataSyncService) setK2sStream(node *Node) {}
