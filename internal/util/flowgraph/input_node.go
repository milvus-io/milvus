package flowgraph

import (
	"context"
	"log"

	"errors"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type InputNode struct {
	BaseNode
	inStream *msgstream.MsgStream
	name     string
}

func (inNode *InputNode) IsInputNode() bool {
	return true
}

func (inNode *InputNode) Name() string {
	return inNode.name
}

func (inNode *InputNode) InStream() *msgstream.MsgStream {
	return inNode.inStream
}

// empty input and return one *Msg
func (inNode *InputNode) Operate(ctx context.Context, msgs []Msg) ([]Msg, context.Context) {
	//fmt.Println("Do InputNode operation")

	msgPack, ctx := (*inNode.inStream).Consume()

	sp, ctx := trace.StartSpanFromContext(ctx, opentracing.Tag{Key: "NodeName", Value: inNode.Name()})
	defer sp.Finish()

	// TODO: add status
	if msgPack == nil {
		log.Println("null msg pack")
		trace.LogError(sp, errors.New("null msg pack"))
		return nil, ctx
	}

	var msgStreamMsg Msg = &MsgStreamMsg{
		tsMessages:     msgPack.Msgs,
		timestampMin:   msgPack.BeginTs,
		timestampMax:   msgPack.EndTs,
		startPositions: msgPack.StartPositions,
		endPositions:   msgPack.EndPositions,
	}

	return []Msg{msgStreamMsg}, ctx
}

func NewInputNode(inStream *msgstream.MsgStream, nodeName string, maxQueueLength int32, maxParallelism int32) *InputNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &InputNode{
		BaseNode: baseNode,
		inStream: inStream,
		name:     nodeName,
	}
}
