package reader

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type manipulationService struct {
	ctx       context.Context
	pulsarURL string
	fg        *flowgraph.TimeTickedFlowGraph
	msgStream *msgstream.PulsarMsgStream
	node      *QueryNode
}

func newManipulationService(ctx context.Context, node *QueryNode, pulsarURL string) *manipulationService {

	return &manipulationService{
		ctx:       ctx,
		pulsarURL: pulsarURL,
		msgStream: nil,
		node:      node,
	}
}

func (dmService *manipulationService) start() {
	const (
		pulsarBufSize       = 100
		consumerChannelSize = 100
	)

	consumerChannels := []string{"insert"}
	consumerSubName := "subInsert"

	// TODO:: load receiveBufSize from config file
	outputStream := msgstream.NewPulsarTtMsgStream(dmService.ctx, 100)
	outputStream.SetPulsarCient(dmService.pulsarURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, pulsarBufSize)
	(*outputStream).Start()

	dmService.initNodes()
	go dmService.fg.Start()
	dmService.consumeFromMsgStream()
}

func (dmService *manipulationService) initNodes() {
	dmService.fg = flowgraph.NewTimeTickedFlowGraph(dmService.ctx)

	var msgStreamNode Node = newMsgStreamNode()

	var dmNode Node = newDmNode()
	// var key2SegNode Node = newKey2SegNode()
	//var schemaUpdateNode Node = newSchemaUpdateNode()

	var filteredDmNode Node = newFilteredDmNode()

	var insertNode Node = newInsertNode(&dmService.node.SegmentsMap)
	// var deletePreprocessNode Node = newDeletePreprocessNode()
	// var deleteNode Node = newDeleteNode()
	var serviceTimeNode Node = newServiceTimeNode(dmService.node)

	dmService.fg.AddNode(&msgStreamNode)

	dmService.fg.AddNode(&dmNode)
	// fg.AddNode(&key2SegNode)
	//dmService.fg.AddNode(&schemaUpdateNode)

	dmService.fg.AddNode(&filteredDmNode)

	dmService.fg.AddNode(&insertNode)
	// fg.AddNode(&deletePreprocessNode)
	// fg.AddNode(&deleteNode)
	dmService.fg.AddNode(&serviceTimeNode)

	// TODO: add delete pipeline support
	var err = dmService.fg.SetEdges(msgStreamNode.Name(),
		[]string{},
		[]string{dmNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", dmNode.Name())
	}

	err = dmService.fg.SetEdges(dmNode.Name(),
		[]string{msgStreamNode.Name()},
		[]string{filteredDmNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", dmNode.Name())
	}

	err = dmService.fg.SetEdges(filteredDmNode.Name(),
		[]string{dmNode.Name()},
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", filteredDmNode.Name())
	}

	err = dmService.fg.SetEdges(insertNode.Name(),
		[]string{filteredDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", insertNode.Name())
	}

	err = dmService.fg.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Fatal("set edges failed in node:", serviceTimeNode.Name())
	}

	err = dmService.fg.SetStartNode(msgStreamNode.Name())
	if err != nil {
		log.Fatal("set start node failed")
	}

	// TODO: add top nodes's initialization
}

func (dmService *manipulationService) consumeFromMsgStream() {
	for {
		select {
		case <-dmService.ctx.Done():
			log.Println("service stop")
			return
		default:
			msgPack := dmService.msgStream.Consume()
			var msgStreamMsg Msg = &msgStreamMsg{
				tsMessages: msgPack.Msgs,
				timeRange: TimeRange{
					timestampMin: msgPack.BeginTs,
					timestampMax: msgPack.EndTs,
				},
			}
			dmService.fg.Input(&msgStreamMsg)
		}
	}
}
