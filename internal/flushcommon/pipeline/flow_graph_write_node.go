package pipeline

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type writeNode struct {
	BaseNode

	channelName string
	wbManager   writebuffer.BufferManager
	updater     util.StatsUpdater
	metacache   metacache.MetaCache
}

// Name returns node name, implementing flowgraph.Node
func (wNode *writeNode) Name() string {
	return fmt.Sprintf("writeNode-%s", wNode.channelName)
}

func (wNode *writeNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)

	// close msg, ignore all data
	if fgMsg.IsCloseMsg() {
		return []Msg{fgMsg}
	}

	// replace pchannel with vchannel
	startPositions := make([]*msgpb.MsgPosition, 0, len(fgMsg.StartPositions))
	for idx := range fgMsg.StartPositions {
		pos := proto.Clone(fgMsg.StartPositions[idx]).(*msgpb.MsgPosition)
		pos.ChannelName = wNode.channelName
		startPositions = append(startPositions, pos)
	}
	fgMsg.StartPositions = startPositions
	endPositions := make([]*msgpb.MsgPosition, 0, len(fgMsg.EndPositions))
	for idx := range fgMsg.EndPositions {
		pos := proto.Clone(fgMsg.EndPositions[idx]).(*msgpb.MsgPosition)
		pos.ChannelName = wNode.channelName
		endPositions = append(endPositions, pos)
	}
	fgMsg.EndPositions = endPositions

	if len(fgMsg.StartPositions) == 0 {
		return []Msg{}
	}
	if len(fgMsg.EndPositions) == 0 {
		return []Msg{}
	}

	var spans []trace.Span
	for _, msg := range fgMsg.InsertMessages {
		ctx, sp := util.StartTracer(msg, "WriteNode")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}
	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	start, end := fgMsg.StartPositions[0], fgMsg.EndPositions[0]

	err := wNode.wbManager.BufferData(wNode.channelName, fgMsg.InsertMessages, fgMsg.DeleteMessages, start, end)
	if err != nil {
		log.Error("failed to buffer data", zap.Error(err))
		panic(err)
	}

	stats := lo.FilterMap(
		lo.Keys(lo.SliceToMap(fgMsg.InsertMessages, func(msg *msgstream.InsertMsg) (int64, struct{}) { return msg.GetSegmentID(), struct{}{} })),
		func(id int64, _ int) (*commonpb.SegmentStats, bool) {
			segInfo, ok := wNode.metacache.GetSegmentByID(id)
			if !ok {
				log.Warn("segment not found for stats", zap.Int64("segment", id))
				return nil, false
			}
			return &commonpb.SegmentStats{
				SegmentID: id,
				NumRows:   segInfo.NumOfRows(),
			}, true
		})

	if !streamingutil.IsStreamingServiceEnabled() {
		wNode.updater.Update(wNode.channelName, end.GetTimestamp(), stats)
	}

	res := FlowGraphMsg{
		TimeRange:      fgMsg.TimeRange,
		StartPositions: fgMsg.StartPositions,
		EndPositions:   fgMsg.EndPositions,
		dropCollection: fgMsg.dropCollection,
	}

	if fgMsg.dropCollection {
		wNode.wbManager.DropChannel(wNode.channelName)
	}

	if len(fgMsg.dropPartitions) > 0 {
		wNode.wbManager.DropPartitions(wNode.channelName, fgMsg.dropPartitions)
	}

	// send delete msg to DeleteNode
	return []Msg{&res}
}

func newWriteNode(
	_ context.Context,
	writeBufferManager writebuffer.BufferManager,
	updater util.StatsUpdater,
	config *nodeConfig,
) *writeNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	return &writeNode{
		BaseNode:    baseNode,
		channelName: config.vChannelName,
		wbManager:   writeBufferManager,
		updater:     updater,
		metacache:   config.metacache,
	}
}
