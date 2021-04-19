package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func (dsService *dataSyncService) newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := pulsarms.NewFactory(Params.PulsarAddress, Params.InsertReceiveBufSize, Params.InsertPulsarBufSize)

	// query node doesn't need to consume any topic
	insertStream, _ := factory.NewTtMsgStream(ctx)
	dsService.dmStream = insertStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&insertStream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func (dsService *dataSyncService) newDDInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := pulsarms.NewFactory(Params.PulsarAddress, Params.DDReceiveBufSize, Params.DDPulsarBufSize)

	consumeChannels := Params.DDChannelNames
	consumeSubName := Params.MsgChannelSubName

	ddStream, _ := factory.NewTtMsgStream(ctx)
	ddStream.AsConsumer(consumeChannels, consumeSubName)

	dsService.ddStream = ddStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&ddStream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
