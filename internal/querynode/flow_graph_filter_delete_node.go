package querynode

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type filterDeleteNode struct {
	baseNode
	collectionID UniqueID
	partitionID  UniqueID
	replica      ReplicaInterface
}

func (fddNode *filterDeleteNode) Name() string {
	return "fddNode"
}

func (fddNode *filterDeleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Error("Invalid operate message input in filterDDNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	if msgStreamMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range msgStreamMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	var dMsg = deleteMsg{
		deleteMessages: make([]*msgstream.DeleteMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	for _, msg := range msgStreamMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_Delete:
			resMsg := fddNode.filterInvalidDeleteMessage(msg.(*msgstream.DeleteMsg))
			if resMsg != nil {
				dMsg.deleteMessages = append(dMsg.deleteMessages, resMsg)
			}
		default:
			log.Warn("Non supporting", zap.Int32("message type", int32(msg.Type())))
		}
	}
	var res Msg = &dMsg
	for _, sp := range spans {
		sp.Finish()
	}
	return []Msg{res}
}

func (fddNode *filterDeleteNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg) *msgstream.DeleteMsg {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	if msg.CollectionID != fddNode.collectionID {
		return nil
	}

	if len(msg.PrimaryKeys) != len(msg.Timestamps) {
		log.Warn("Error, misaligned messages detected")
		return nil
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid delete message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}
	return msg
}

func newFilteredDeleteNode(replica ReplicaInterface,
	collectionID UniqueID,
	partitionID UniqueID) *filterDeleteNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDeleteNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		partitionID:  partitionID,
		replica:      replica,
	}
}
