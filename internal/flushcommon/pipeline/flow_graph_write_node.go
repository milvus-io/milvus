package pipeline

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type writeNode struct {
	BaseNode

	channelName  string
	collectionID int64
	wbManager    writebuffer.BufferManager
	updater      util.StatsUpdater
	metacache    metacache.MetaCache
	pkField      *schemapb.FieldSchema

	functionRunners        map[int32][]function.FunctionRunner
	functionOutputFieldIDs map[int32][]int64
}

// Name returns node name, implementing flowgraph.Node
func (wNode *writeNode) Name() string {
	return fmt.Sprintf("writeNode-%s", wNode.channelName)
}

func (wNode *writeNode) Free() {
	wNode.releaseFunctionRunners()
}

func (wNode *writeNode) releaseFunctionRunners() {
	for _, runners := range wNode.functionRunners {
		function.CloseRunners(runners)
	}
	wNode.functionRunners = make(map[int32][]function.FunctionRunner)
	wNode.functionOutputFieldIDs = make(map[int32][]int64)
}

func (wNode *writeNode) getEmbeddingOutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	schemaVersion := schema.GetVersion()
	if outputFieldIDs, ok := wNode.functionOutputFieldIDs[schemaVersion]; ok {
		return outputFieldIDs, nil
	}

	if !function.HasEmbeddingFunctions(schema) {
		wNode.functionOutputFieldIDs[schemaVersion] = nil
		return nil, nil
	}
	outputFieldIDs, err := function.EmbeddingOutputFieldIDs(schema)
	if err != nil {
		return nil, err
	}
	wNode.functionOutputFieldIDs[schemaVersion] = outputFieldIDs
	return outputFieldIDs, nil
}

// fillEmbeddingData is only used to handle old insert messages that were not embedded before WAL append.
func (wNode *writeNode) fillEmbeddingData(schema *schemapb.CollectionSchema, msg *msgstream.InsertMsg) error {
	if !function.HasEmbeddingFunctions(schema) {
		return nil
	}
	schemaVersion := schema.GetVersion()
	_, ok, err := function.TryMaterialize(wNode.collectionID, schemaVersion, msg.InsertRequest)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	runners, ok := wNode.functionRunners[schemaVersion]
	if !ok {
		runners, err = function.BuildEmbeddingRunners(schema)
		if err != nil {
			return err
		}
		wNode.functionRunners[schemaVersion] = runners
	}
	_, err = function.FillFunctionFields(runners, msg.InsertRequest)
	return err
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
	currentSchema := wNode.metacache.GetSchema(fgMsg.TimeTick())
	schemaVersion := currentSchema.GetVersion()
	functionOutputFieldIDs, err := wNode.getEmbeddingOutputFieldIDs(currentSchema)
	if err != nil {
		log.Error("failed to get embedding output fields", zap.Error(err))
		panic(err)
	}

	insertData := make([]*writebuffer.InsertData, 0)
	if len(fgMsg.InsertMessages) > 0 {
		for _, msg := range fgMsg.InsertMessages {
			if len(functionOutputFieldIDs) == 0 || function.HasAllFieldDataByID(msg.GetFieldsData(), functionOutputFieldIDs) {
				continue
			}
			if err := wNode.fillEmbeddingData(currentSchema, msg); err != nil {
				log.Error("failed to fill embedding data", zap.Error(err))
				panic(err)
			}
		}
		preparedInsertData, err := writebuffer.PrepareInsert(currentSchema, wNode.pkField, fgMsg.InsertMessages)
		if err != nil {
			log.Error("failed to prepare data", zap.Error(err))
			panic(err)
		}
		insertData = preparedInsertData
	}
	fgMsg.InsertData = insertData

	err = wNode.wbManager.BufferData(wNode.channelName, fgMsg.InsertData, fgMsg.DeleteMessages, start, end, schemaVersion)
	if err != nil {
		log.Error("failed to buffer data", zap.Error(err))
		panic(err)
	}

	stats := lo.FilterMap(
		lo.Keys(lo.SliceToMap(fgMsg.InsertData, func(data *writebuffer.InsertData) (int64, struct{}) { return data.GetSegmentID(), struct{}{} })),
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
		TimeRange:        fgMsg.TimeRange,
		StartPositions:   fgMsg.StartPositions,
		EndPositions:     fgMsg.EndPositions,
		dropCollection:   fgMsg.dropCollection,
		isAlterWal:       fgMsg.isAlterWal,
		alterWalTimeTick: fgMsg.alterWalTimeTick,
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
) (*writeNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	// pkfield is a immutable property of the collection, so we can get it from any schema
	collSchema := config.metacache.GetSchema(0)
	pkField, err := typeutil.GetPrimaryFieldSchema(collSchema)
	if err != nil {
		return nil, err
	}

	wNode := &writeNode{
		BaseNode:     baseNode,
		channelName:  config.vChannelName,
		collectionID: config.collectionID,
		wbManager:    writeBufferManager,
		updater:      updater,
		metacache:    config.metacache,
		pkField:      pkField,

		functionRunners:        make(map[int32][]function.FunctionRunner),
		functionOutputFieldIDs: make(map[int32][]int64),
	}
	if _, err := wNode.getEmbeddingOutputFieldIDs(collSchema); err != nil {
		return nil, err
	}
	return wNode, nil
}
