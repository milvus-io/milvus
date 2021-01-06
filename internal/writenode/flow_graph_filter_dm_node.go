package writenode

import (
	"context"
	"log"

	"github.com/opentracing/opentracing-go"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type filterDmNode struct {
	BaseNode
	ddMsg *ddMsg
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do filterDmNode operation")

	if len(in) != 2 {
		log.Println("Invalid operate message input in filterDmNode, input length = ", len(in))
		// TODO: add error handling
	}

	msgStreamMsg, ok := (*in[0]).(*MsgStreamMsg)
	if !ok {
		log.Println("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	var childs []opentracing.Span
	tracer := opentracing.GlobalTracer()
	if tracer != nil {
		for _, msg := range msgStreamMsg.TsMessages() {
			if msg.Type() == internalPb.MsgType_kInsert {
				var child opentracing.Span
				ctx := msg.GetContext()
				if parent := opentracing.SpanFromContext(ctx); parent != nil {
					child = tracer.StartSpan("pass filter node",
						opentracing.FollowsFrom(parent.Context()))
				} else {
					child = tracer.StartSpan("pass filter node")
				}
				child.SetTag("hash keys", msg.HashKeys())
				child.SetTag("start time", msg.BeginTs())
				child.SetTag("end time", msg.EndTs())
				msg.SetContext(opentracing.ContextWithSpan(ctx, child))
				childs = append(childs, child)
			}
		}
	}

	ddMsg, ok := (*in[1]).(*ddMsg)
	if !ok {
		log.Println("type assertion failed for ddMsg")
		// TODO: add error handling
	}

	fdmNode.ddMsg = ddMsg

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		flushMessages:  make([]*msgstream.FlushMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	for _, fmsg := range ddMsg.flushMessages {
		switch fmsg.Type() {
		case internalPb.MsgType_kFlush:
			iMsg.flushMessages = append(iMsg.flushMessages, fmsg)
		default:
			log.Println("Non supporting message type:", fmsg.Type())
		}
	}

	for key, msg := range msgStreamMsg.TsMessages() {
		switch msg.Type() {
		case internalPb.MsgType_kInsert:
			var ctx2 context.Context
			if childs != nil {
				if childs[key] != nil {
					ctx2 = opentracing.ContextWithSpan(msg.GetContext(), childs[key])
				} else {
					ctx2 = context.Background()
				}
			}
			resMsg := fdmNode.filterInvalidInsertMessage(msg.(*msgstream.InsertMsg))
			if resMsg != nil {
				resMsg.SetContext(ctx2)
				iMsg.insertMessages = append(iMsg.insertMessages, resMsg)
			}
		// case internalPb.MsgType_kDelete:
		// dmMsg.deleteMessages = append(dmMsg.deleteMessages, (*msg).(*msgstream.DeleteTask))
		default:
			log.Println("Non supporting message type:", msg.Type())
		}
	}
	var res Msg = &iMsg

	for _, child := range childs {
		child.Finish()
	}
	return []*Msg{&res}
}

func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	// No dd record, do all insert requests.
	records, ok := fdmNode.ddMsg.collectionRecords[msg.CollectionName]
	if !ok {
		return msg
	}

	// TODO: If the last record is drop type, all insert requests are invalid.
	//if !records[len(records)-1].createOrDrop {
	//	return nil
	//}

	// Filter insert requests before last record.
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Println("Error, misaligned messages detected")
		return nil
	}
	tmpTimestamps := make([]Timestamp, 0)
	tmpRowIDs := make([]int64, 0)
	tmpRowData := make([]*commonpb.Blob, 0)
	targetTimestamp := records[len(records)-1].timestamp
	for i, t := range msg.Timestamps {
		if t >= targetTimestamp {
			tmpTimestamps = append(tmpTimestamps, t)
			tmpRowIDs = append(tmpRowIDs, msg.RowIDs[i])
			tmpRowData = append(tmpRowData, msg.RowData[i])
		}
	}
	msg.Timestamps = tmpTimestamps
	msg.RowIDs = tmpRowIDs
	msg.RowData = tmpRowData
	return msg
}

func newFilteredDmNode() *filterDmNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		BaseNode: baseNode,
	}
}
