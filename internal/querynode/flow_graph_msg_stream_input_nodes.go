package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func (dsService *dataSyncService) newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := pulsarms.NewFactory(Params.PulsarAddress, Params.InsertReceiveBufSize, Params.InsertPulsarBufSize)

	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream, _ := factory.NewTtMsgStream(ctx)
	insertStream.AsConsumer(consumeChannels, consumeSubName)

	var stream msgstream.MsgStream = insertStream
	dsService.dmStream = stream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func (dsService *dataSyncService) newDDInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := pulsarms.NewFactory(Params.PulsarAddress, Params.DDReceiveBufSize, Params.DDPulsarBufSize)

	consumeChannels := Params.DDChannelNames
	consumeSubName := Params.MsgChannelSubName

	ddStream, _ := factory.NewTtMsgStream(ctx)
	ddStream.AsConsumer(consumeChannels, consumeSubName)

	var stream msgstream.MsgStream = ddStream
	dsService.ddStream = stream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
