package datanode

import (
	"context"
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context, factory msgstream.Factory) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream, _ := factory.NewTtMsgStream(ctx)
	insertStream.AsConsumer(consumeChannels, consumeSubName)
	log.Debug("datanode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context, factory msgstream.Factory) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName

	tmpStream, _ := factory.NewTtMsgStream(ctx)
	tmpStream.AsConsumer(Params.DDChannelNames, consumeSubName)
	log.Debug("datanode AsConsumer: " + strings.Join(Params.DDChannelNames, ", ") + " : " + consumeSubName)

	var stream msgstream.MsgStream = tmpStream
	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
