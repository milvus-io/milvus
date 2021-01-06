package flowgraph

import (
	"fmt"
	"log"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
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
func (inNode *InputNode) Operate([]*Msg) []*Msg {
	//fmt.Println("Do InputNode operation")
	msgPack := (*inNode.inStream).Consume()

	var childs []opentracing.Span
	tracer := opentracing.GlobalTracer()
	if tracer != nil && msgPack != nil {
		for _, msg := range msgPack.Msgs {
			if msg.Type() == internalpb.MsgType_kInsert || msg.Type() == internalpb.MsgType_kSearch {
				var child opentracing.Span
				ctx := msg.GetContext()
				if parent := opentracing.SpanFromContext(ctx); parent != nil {
					child = tracer.StartSpan(fmt.Sprintf("through msg input node, start time = %d", msg.BeginTs()),
						opentracing.FollowsFrom(parent.Context()))
				} else {
					child = tracer.StartSpan(fmt.Sprintf("through msg input node, start time = %d", msg.BeginTs()))
				}
				child.SetTag("hash keys", msg.HashKeys())
				child.SetTag("start time", msg.BeginTs())
				child.SetTag("end time", msg.EndTs())
				msg.SetContext(opentracing.ContextWithSpan(ctx, child))
				childs = append(childs, child)
			}
		}
	}

	// TODO: add status
	if msgPack == nil {
		log.Println("null msg pack")
		return nil
	}

	var msgStreamMsg Msg = &MsgStreamMsg{
		tsMessages:   msgPack.Msgs,
		timestampMin: msgPack.BeginTs,
		timestampMax: msgPack.EndTs,
	}

	for _, child := range childs {
		child.Finish()
	}

	return []*Msg{&msgStreamMsg}
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
