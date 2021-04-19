package reader

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

func newDmInputNode(ctx context.Context, pulsarURL string) *flowgraph.InputNode {
	const (
		receiveBufSize = 1024
		pulsarBufSize  = 1024
	)

	consumeChannels := []string{"insert"}
	consumeSubName := "insertSub"

	insertStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	insertStream.SetPulsarCient(pulsarURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	insertStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = insertStream

	node := flowgraph.NewInputNode(&stream, "dmInputNode")
	return node
}
