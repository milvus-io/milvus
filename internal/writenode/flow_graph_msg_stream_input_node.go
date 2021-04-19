package writenode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	receiveBufSize := Params.InsertReceiveBufSize
	pulsarBufSize := Params.InsertPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream := msgstream.NewPulsarTtMsgStream(ctx, receiveBufSize)

	// TODO could panic of nil pointer
	insertStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()

	// TODO could panic of nil pointer
	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = insertStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context) *flowgraph.InputNode {
	receiveBufSize := Params.DDReceiveBufSize
	pulsarBufSize := Params.DDPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.DDChannelNames
	consumeSubName := Params.MsgChannelSubName

	ddStream := msgstream.NewPulsarTtMsgStream(ctx, receiveBufSize)
	ddStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	ddStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = ddStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
