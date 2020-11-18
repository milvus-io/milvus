package reader

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	receiveBufSize := Params.dmMsgStreamReceiveBufSize()
	pulsarBufSize := Params.dmPulsarBufSize()

	msgStreamURL, err := Params.PulsarAddress()
	if err != nil {
		log.Fatal(err)
	}

	consumeChannels := []string{"insert"}
	consumeSubName := "insertSub"

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarCient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = insertStream

	node := flowgraph.NewInputNode(&stream, "dmInputNode")
	return node
}
