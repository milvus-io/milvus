package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func (dsService *dataSyncService) newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := msgstream.ProtoUDFactory{}
	receiveBufSize := Params.InsertReceiveBufSize
	pulsarBufSize := Params.InsertPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream := pulsarms.NewPulsarTtMsgStream(ctx, receiveBufSize, pulsarBufSize, factory.NewUnmarshalDispatcher())
	insertStream.SetPulsarClient(msgStreamURL)
	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName)

	var stream msgstream.MsgStream = insertStream
	dsService.dmStream = stream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func (dsService *dataSyncService) newDDInputNode(ctx context.Context) *flowgraph.InputNode {
	factory := msgstream.ProtoUDFactory{}
	receiveBufSize := Params.DDReceiveBufSize
	pulsarBufSize := Params.DDPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.DDChannelNames
	consumeSubName := Params.MsgChannelSubName

	ddStream := pulsarms.NewPulsarTtMsgStream(ctx, receiveBufSize, pulsarBufSize, factory.NewUnmarshalDispatcher())
	ddStream.SetPulsarClient(msgStreamURL)
	ddStream.CreatePulsarConsumers(consumeChannels, consumeSubName)

	var stream msgstream.MsgStream = ddStream
	dsService.ddStream = stream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
