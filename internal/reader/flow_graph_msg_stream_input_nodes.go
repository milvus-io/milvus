package reader

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	receiveBufSize := Params.dmReceiveBufSize()
	pulsarBufSize := Params.dmPulsarBufSize()

	msgStreamURL, err := Params.pulsarAddress()
	if err != nil {
		log.Fatal(err)
	}

	consumeChannels := []string{"insert"}
	consumeSubName := "insertSub"

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = insertStream

	maxQueueLength := Params.flowGraphMaxQueueLength()
	maxParallelism := Params.flowGraphMaxParallelism()

	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}
