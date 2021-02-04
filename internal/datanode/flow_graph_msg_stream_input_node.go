package datanode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	factory := msgstream.ProtoUDFactory{}
	insertStream := pulsarms.NewPulsarTtMsgStream(ctx, 1024, 1024, factory.NewUnmarshalDispatcher())
	insertStream.SetPulsarClient(Params.PulsarAddress)

	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName)

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName

	factory := msgstream.ProtoUDFactory{}
	tmpStream := pulsarms.NewPulsarTtMsgStream(ctx, 1024, 1024, factory.NewUnmarshalDispatcher())
	tmpStream.SetPulsarClient(Params.PulsarAddress)
	tmpStream.CreatePulsarConsumers(Params.DDChannelNames, consumeSubName)

	var stream msgstream.MsgStream = tmpStream
	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
