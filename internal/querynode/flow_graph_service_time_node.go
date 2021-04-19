package querynode

import (
	"context"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type serviceTimeNode struct {
	baseNode
	replica           ReplicaInterface
	timeTickMsgStream msgstream.MsgStream
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {
	//log.Debug("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in serviceTimeNode, input length = ", zap.Int("input node", len(in)))
		// TODO: add error handling
	}

	serviceTimeMsg, ok := in[0].(*serviceTimeMsg)
	if !ok {
		log.Error("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	// update service time
	stNode.replica.getTSafe().set(serviceTimeMsg.timeRange.timestampMax)
	//log.Debug("update tSafe to:", getPhysicalTime(serviceTimeMsg.timeRange.timestampMax))

	if err := stNode.sendTimeTick(serviceTimeMsg.timeRange.timestampMax); err != nil {
		log.Error("Error: send time tick into pulsar channel failed", zap.Error(err))
	}

	var res Msg = &gcMsg{
		gcRecord:  serviceTimeMsg.gcRecord,
		timeRange: serviceTimeMsg.timeRange,
	}
	return []Msg{res}, ctx
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
	return stNode.timeTickMsgStream.Produce(context.TODO(), &msgPack)
}

func newServiceTimeNode(ctx context.Context, replica ReplicaInterface, factory msgstream.Factory) *serviceTimeNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	timeTimeMsgStream, _ := factory.NewMsgStream(ctx)
	timeTimeMsgStream.AsProducer([]string{Params.QueryTimeTickChannelName})
	log.Debug("querynode AsProducer: " + Params.QueryTimeTickChannelName)

	return &serviceTimeNode{
		baseNode:          baseNode,
		replica:           replica,
		timeTickMsgStream: timeTimeMsgStream,
	}
}
