package datanode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName
	unmarshalDispatcher := util.NewUnmarshalDispatcher()

	insertStream := pulsarms.NewPulsarTtMsgStream(ctx, 1024)
	insertStream.SetPulsarClient(Params.PulsarAddress)

	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, 1024)

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName
	unmarshalDispatcher := util.NewUnmarshalDispatcher()

	tmpStream := pulsarms.NewPulsarTtMsgStream(ctx, 1024)
	tmpStream.SetPulsarClient(Params.PulsarAddress)
	tmpStream.CreatePulsarConsumers(Params.DDChannelNames, consumeSubName, unmarshalDispatcher, 1024)

	var stream msgstream.MsgStream = tmpStream
	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
