package flowgraph

import (
	"log"

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
func (inNode *InputNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do InputNode operation")

	msgPack := (*inNode.inStream).Consume()

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

	return []*Msg{&msgStreamMsg}
}

func NewInputNode(inStream *msgstream.MsgStream, nodeName string) *InputNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength()
	maxParallelism := Params.FlowGraphMaxParallelism()

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &InputNode{
		BaseNode: baseNode,
		inStream: inStream,
		name:     nodeName,
	}
}
