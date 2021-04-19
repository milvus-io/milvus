package querynode

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type serviceTimeNode struct {
	baseNode
	replica           collectionReplica
	timeTickMsgStream msgstream.MsgStream
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in serviceTimeNode, input length = ", len(in))
		// TODO: add error handling
	}

	serviceTimeMsg, ok := (*in[0]).(*serviceTimeMsg)
	if !ok {
		log.Println("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	// update service time
	stNode.replica.getTSafe().set(serviceTimeMsg.timeRange.timestampMax)
	//fmt.Println("update tSafe to:", getPhysicalTime(serviceTimeMsg.timeRange.timestampMax))

	if err := stNode.sendTimeTick(serviceTimeMsg.timeRange.timestampMax); err != nil {
		log.Printf("Error: send time tick into pulsar channel failed, %s\n", err.Error())
	}

	var res Msg = &gcMsg{
		gcRecord:  serviceTimeMsg.gcRecord,
		timeRange: serviceTimeMsg.timeRange,
	}
	return []*Msg{&res}
}

func (stNode *serviceTimeNode) sendTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     0,
				Timestamp: ts,
				SourceID:  Params.QueryNodeID,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return stNode.timeTickMsgStream.Produce(&msgPack)
}

func newServiceTimeNode(ctx context.Context, replica collectionReplica, factory msgstream.Factory) *serviceTimeNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	timeTimeMsgStream, _ := factory.NewMsgStream(ctx)
	timeTimeMsgStream.AsProducer([]string{Params.QueryTimeTickChannelName})

	return &serviceTimeNode{
		baseNode:          baseNode,
		replica:           replica,
		timeTickMsgStream: timeTimeMsgStream,
	}
}
