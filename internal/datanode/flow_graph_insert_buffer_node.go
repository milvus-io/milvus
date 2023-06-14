// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type insertBufferNode struct {
	BaseNode

	ctx              context.Context
	channelName      string
	delBufferManager *DeltaBufferManager // manager of delete msg
	channel          Channel
	idAllocator      allocator.Allocator

	flushMap         sync.Map
	flushChan        <-chan flushMsg
	resendTTChan     <-chan resendTTMsg
	flushingSegCache *Cache
	flushManager     flushManager

	timeTickStream msgstream.MsgStream
	ttLogger       *timeTickLogger
	ttMerger       *mergedTimeTickerSender

	timeTickSender *timeTickSender

	lastTimestamp Timestamp
}

type timeTickLogger struct {
	start        atomic.Uint64
	counter      atomic.Int32
	vChannelName string
}

func (l *timeTickLogger) LogTs(ts Timestamp) {
	if l.counter.Load() == 0 {
		l.start.Store(ts)
	}
	l.counter.Inc()
	if l.counter.Load() == 1000 {
		min := l.start.Load()
		l.start.Store(ts)
		l.counter.Store(0)
		go l.printLogs(min, ts)
	}
}

func (l *timeTickLogger) printLogs(start, end Timestamp) {
	t1, _ := tsoutil.ParseTS(start)
	t2, _ := tsoutil.ParseTS(end)
	log.Debug("IBN timetick log", zap.Time("from", t1), zap.Time("to", t2), zap.Duration("elapsed", t2.Sub(t1)), zap.Uint64("start", start), zap.Uint64("end", end), zap.String("vChannelName", l.vChannelName))
}

func (ibNode *insertBufferNode) Name() string {
	return "ibNode-" + ibNode.channelName
}

func (ibNode *insertBufferNode) Close() {
	if ibNode.ttMerger != nil {
		ibNode.ttMerger.close()
	}

	if ibNode.timeTickStream != nil {
		ibNode.timeTickStream.Close()
	}
}

func (ibNode *insertBufferNode) IsValidInMsg(in []Msg) bool {
	if !ibNode.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Warn("type assertion failed for flowGraphMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

func (ibNode *insertBufferNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*flowGraphMsg)

	// replace pchannel with vchannel
	startPositions := make([]*msgpb.MsgPosition, 0, len(fgMsg.startPositions))
	for idx := range fgMsg.startPositions {
		pos := proto.Clone(fgMsg.startPositions[idx]).(*msgpb.MsgPosition)
		pos.ChannelName = ibNode.channelName
		startPositions = append(startPositions, pos)
	}
	fgMsg.startPositions = startPositions
	endPositions := make([]*msgpb.MsgPosition, 0, len(fgMsg.endPositions))
	for idx := range fgMsg.endPositions {
		pos := proto.Clone(fgMsg.endPositions[idx]).(*msgpb.MsgPosition)
		pos.ChannelName = ibNode.channelName
		endPositions = append(endPositions, pos)
	}
	fgMsg.endPositions = endPositions

	if fgMsg.IsCloseMsg() {
		if len(fgMsg.endPositions) != 0 {
			// try to sync all segments
			segmentsToSync := ibNode.Sync(fgMsg, make([]UniqueID, 0), fgMsg.endPositions[0])
			res := flowGraphMsg{
				deleteMessages: []*msgstream.DeleteMsg{},
				timeRange:      fgMsg.timeRange,
				startPositions: fgMsg.startPositions,
				endPositions:   fgMsg.endPositions,
				segmentsToSync: segmentsToSync,
				dropCollection: fgMsg.dropCollection,
				BaseMsg:        flowgraph.NewBaseMsg(true),
			}
			return []Msg{&res}
		}
		return in
	}

	if fgMsg.dropCollection {
		ibNode.flushManager.startDropping()
	}

	var spans []trace.Span
	for _, msg := range fgMsg.insertMessages {
		ctx, sp := startTracer(msg, "InsertBuffer-Node")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	if startPositions[0].Timestamp < ibNode.lastTimestamp {
		// message stream should guarantee that this should not happen
		err := fmt.Errorf("insert buffer node consumed old messages, channel = %s, timestamp = %d, lastTimestamp = %d",
			ibNode.channelName, startPositions[0].Timestamp, ibNode.lastTimestamp)
		log.Error(err.Error())
		panic(err)
	}

	ibNode.lastTimestamp = endPositions[0].Timestamp

	// Add segment in channel if need and updating segment row number
	seg2Upload, err := ibNode.addSegmentAndUpdateRowNum(fgMsg.insertMessages, startPositions[0], endPositions[0])
	if err != nil {
		// Occurs only if the collectionID is mismatch, should not happen
		err = errors.Wrap(err, "update segment states in channel meta wrong")
		log.Error(err.Error())
		panic(err)
	}

	// insert messages -> buffer
	for _, msg := range fgMsg.insertMessages {
		err := ibNode.bufferInsertMsg(msg, startPositions[0], endPositions[0])
		if err != nil {
			// error occurs when missing schema info or data is misaligned, should not happen
			err = errors.Wrap(err, "insertBufferNode msg to buffer failed")
			log.Error(err.Error())
			panic(err)
		}
	}

	ibNode.updateSegmentsMemorySize(seg2Upload)
	ibNode.DisplayStatistics(seg2Upload)

	segmentsToSync := ibNode.Sync(fgMsg, seg2Upload, endPositions[0])

	ibNode.WriteTimeTick(fgMsg.timeRange.timestampMax, seg2Upload)

	res := flowGraphMsg{
		deleteMessages: fgMsg.deleteMessages,
		timeRange:      fgMsg.timeRange,
		startPositions: fgMsg.startPositions,
		endPositions:   fgMsg.endPositions,
		segmentsToSync: segmentsToSync,
		dropCollection: fgMsg.dropCollection,
	}

	// send delete msg to DeleteNode
	return []Msg{&res}
}

func (ibNode *insertBufferNode) GetBufferIfFull(segID UniqueID) (*BufferData, bool) {
	if bd, ok := ibNode.channel.getCurInsertBuffer(segID); ok && bd.effectiveCap() <= 0 {
		return bd, true
	}

	return nil, false
}

// GetBuffer returns buffer data for a segment, returns nil if segment's not in buffer
func (ibNode *insertBufferNode) GetBuffer(segID UniqueID) *BufferData {
	var buf *BufferData
	if bd, ok := ibNode.channel.getCurInsertBuffer(segID); ok {
		buf = bd
	}
	return buf
}

// CollectSegmentsToSync collects segments from flushChan from DataCoord
func (ibNode *insertBufferNode) CollectSegmentsToSync() (flushedSegments []UniqueID) {
	var (
		maxBatch    = 10
		targetBatch int
	)

	size := len(ibNode.flushChan)
	if size >= maxBatch {
		targetBatch = maxBatch
	} else {
		targetBatch = size
	}

	for i := 1; i <= targetBatch; i++ {
		fmsg := <-ibNode.flushChan
		flushedSegments = append(flushedSegments, fmsg.segmentID)
	}

	if targetBatch > 0 {
		log.Info("(Manual Sync) batch processing flush messages",
			zap.Int("batchSize", targetBatch),
			zap.Int64s("flushedSegments", flushedSegments),
			zap.String("channel", ibNode.channelName),
		)
	}

	return flushedSegments
}

// DisplayStatistics logs the statistic changes of segment in mem
func (ibNode *insertBufferNode) DisplayStatistics(seg2Upload []UniqueID) {
	// Find and return the smaller input
	min := func(former, latter int) (smaller int) {
		if former <= latter {
			return former
		}
		return latter
	}

	// limit the logging size
	displaySize := min(10, len(seg2Upload))

	for k, segID := range seg2Upload[:displaySize] {
		if bd, ok := ibNode.channel.getCurInsertBuffer(segID); ok {
			log.Info("segment buffer status",
				zap.Int("no.", k),
				zap.Int64("segmentID", segID),
				zap.String("channel", ibNode.channelName),
				zap.Int64("size", bd.size),
				zap.Int64("limit", bd.limit),
				zap.Int64("memorySize", bd.memorySize()))
		}
	}
}

// updateSegmentsMemorySize updates segments' memory size in channel meta
func (ibNode *insertBufferNode) updateSegmentsMemorySize(seg2Upload []UniqueID) {
	for _, segID := range seg2Upload {
		var memorySize int64
		if buffer, ok := ibNode.channel.getCurInsertBuffer(segID); ok {
			memorySize += buffer.memorySize()
		}
		if buffer, ok := ibNode.channel.getCurDeleteBuffer(segID); ok {
			memorySize += buffer.GetLogSize()
		}
		ibNode.channel.updateSegmentMemorySize(segID, memorySize)
	}
}

type syncTask struct {
	buffer    *BufferData
	segmentID UniqueID
	flushed   bool
	dropped   bool
	auto      bool
}

func (ibNode *insertBufferNode) FillInSyncTasks(fgMsg *flowGraphMsg, seg2Upload []UniqueID) map[UniqueID]*syncTask {
	var syncTasks = make(map[UniqueID]*syncTask)

	if fgMsg.dropCollection {
		// All segments in the collection will be synced, not matter empty buffer or not
		segmentIDs := ibNode.channel.listAllSegmentIDs()
		log.Info("Receive drop collection request and syncing all segments",
			zap.Int64s("segments", segmentIDs),
			zap.String("channel", ibNode.channelName),
		)

		for _, segID := range segmentIDs {
			buf := ibNode.GetBuffer(segID)
			syncTasks[segID] = &syncTask{
				buffer:    buf, // nil is valid
				segmentID: segID,
				flushed:   false,
				dropped:   true,
			}
		}
		return syncTasks
	}

	if fgMsg.IsCloseMsg() {
		// All segments in the collection will be synced, not matter empty buffer or not
		segmentIDs := ibNode.channel.listAllSegmentIDs()
		log.Info("Receive close request and syncing all segments",
			zap.Int64s("segments", segmentIDs),
			zap.String("channel", ibNode.channelName),
		)

		for _, segID := range segmentIDs {
			// if segment has data or delete then force sync
			insertBuf, hasInsert := ibNode.channel.getCurInsertBuffer(segID)
			deleteEntry := ibNode.delBufferManager.GetEntriesNum(segID)
			// if insert buf or or delete buf is not empty, trigger sync
			if (hasInsert && insertBuf.size > 0) || (deleteEntry > 0) {
				syncTasks[segID] = &syncTask{
					buffer:    insertBuf, // nil is valid
					segmentID: segID,
					flushed:   false,
					dropped:   false,
					auto:      true,
				}
			}
		}
		return syncTasks
	}

	// Auto Sync // TODO: move to segment_sync_policy
	for _, segID := range seg2Upload {
		if ibuffer, ok := ibNode.GetBufferIfFull(segID); ok {
			log.Info("(Auto Sync)",
				zap.Int64("segmentID", segID),
				zap.Int64("numRows", ibuffer.size),
				zap.Int64("limit", ibuffer.limit),
				zap.String("channel", ibNode.channelName))

			syncTasks[segID] = &syncTask{
				buffer:    ibuffer,
				segmentID: segID,
				flushed:   false,
				dropped:   false,
				auto:      true,
			}
		}
	}

	// sync delete
	//here we adopt a quite radical strategy:
	//every time we make sure that the N biggest delDataBuf can be flushed
	//when memsize usage reaches a certain level
	//the aim for taking all these actions is to guarantee that the memory consumed by delBuf will not exceed a limit
	segmentsToFlush := ibNode.delBufferManager.ShouldFlushSegments()
	for _, segID := range segmentsToFlush {
		syncTasks[segID] = &syncTask{
			buffer:    nil, // nil is valid
			segmentID: segID,
		}
	}

	syncSegmentIDs := ibNode.channel.listSegmentIDsToSync(fgMsg.endPositions[0].Timestamp)
	for _, segID := range syncSegmentIDs {
		buf := ibNode.GetBuffer(segID)
		syncTasks[segID] = &syncTask{
			buffer:    buf, // nil is valid
			segmentID: segID,
		}
	}
	if len(syncSegmentIDs) > 0 {
		log.Info("sync segments", zap.String("vChannel", ibNode.channelName),
			zap.Int64s("segIDs", syncSegmentIDs)) // TODO: maybe too many prints here
	}

	mergeSyncTask := func(segmentIDs []UniqueID, syncTasks map[UniqueID]*syncTask, setupTask func(task *syncTask)) {
		// Merge auto & manual sync tasks with the same segment ID.
		for _, segmentID := range segmentIDs {
			if task, ok := syncTasks[segmentID]; ok {
				setupTask(task)
				log.Info("merging sync task, updating flushed flag",
					zap.Int64("segmentID", segmentID),
					zap.Bool("flushed", task.flushed),
					zap.Bool("dropped", task.dropped),
				)
				continue
			}

			buf := ibNode.GetBuffer(segmentID)
			task := syncTask{
				buffer:    buf, // nil is valid
				segmentID: segmentID,
			}
			setupTask(&task)
			syncTasks[segmentID] = &task
		}
	}

	flushedSegments := ibNode.CollectSegmentsToSync()
	mergeSyncTask(flushedSegments, syncTasks, func(task *syncTask) {
		task.flushed = true
	})
	mergeSyncTask(syncSegmentIDs, syncTasks, func(task *syncTask) {})

	// process drop partition
	for _, partitionDrop := range fgMsg.dropPartitions {
		segmentIDs := ibNode.channel.listPartitionSegments(partitionDrop)
		log.Info("(Drop Partition) syncing all segments in the partition",
			zap.Int64("collectionID", ibNode.channel.getCollectionID()),
			zap.Int64("partitionID", partitionDrop),
			zap.Int64s("segmentIDs", segmentIDs),
			zap.String("channel", ibNode.channelName),
		)
		mergeSyncTask(segmentIDs, syncTasks, func(task *syncTask) {
			task.flushed = true
			task.dropped = true
		})
	}
	return syncTasks
}

func (ibNode *insertBufferNode) Sync(fgMsg *flowGraphMsg, seg2Upload []UniqueID, endPosition *msgpb.MsgPosition) []UniqueID {
	syncTasks := ibNode.FillInSyncTasks(fgMsg, seg2Upload)
	segmentsToSync := make([]UniqueID, 0, len(syncTasks))
	ibNode.channel.(*ChannelMeta).needToSync.Store(false)

	for _, task := range syncTasks {
		log.Info("insertBufferNode syncing BufferData",
			zap.Int64("segmentID", task.segmentID),
			zap.Bool("flushed", task.flushed),
			zap.Bool("dropped", task.dropped),
			zap.Bool("auto", task.auto),
			zap.Any("position", endPosition),
			zap.String("channel", ibNode.channelName),
		)
		// use the flushed pk stats to take current stat
		var pkStats *storage.PrimaryKeyStats
		// TODO, this has to be async flush, no need to block here.
		err := retry.Do(ibNode.ctx, func() error {
			var err error
			pkStats, err = ibNode.flushManager.flushBufferData(task.buffer,
				task.segmentID,
				task.flushed,
				task.dropped,
				endPosition)
			if err != nil {
				return err
			}
			return nil
		}, getFlowGraphRetryOpt())
		if err != nil {
			metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
			metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.TotalLabel).Inc()
			if task.auto {
				metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
				metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.TotalLabel).Inc()
			}
			err = fmt.Errorf("insertBufferNode flushBufferData failed, err = %s", err)
			log.Error(err.Error())
			panic(err)
		}
		segmentsToSync = append(segmentsToSync, task.segmentID)
		ibNode.channel.rollInsertBuffer(task.segmentID)
		ibNode.channel.RollPKstats(task.segmentID, pkStats)
		ibNode.channel.setSegmentLastSyncTs(task.segmentID, endPosition.GetTimestamp())
		metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
		metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.TotalLabel).Inc()
		if task.auto {
			metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.TotalLabel).Inc()
			metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		}
	}
	return segmentsToSync
}

//	 addSegmentAndUpdateRowNum updates row number in channel meta for the segments in insertMsgs.
//
//		If the segment doesn't exist, a new segment will be created.
//		The segment number of rows will be updated in mem, waiting to be uploaded to DataCoord.
func (ibNode *insertBufferNode) addSegmentAndUpdateRowNum(insertMsgs []*msgstream.InsertMsg, startPos, endPos *msgpb.MsgPosition) (seg2Upload []UniqueID, err error) {
	uniqueSeg := make(map[UniqueID]int64)
	for _, msg := range insertMsgs {

		currentSegID := msg.GetSegmentID()
		collID := msg.GetCollectionID()
		partitionID := msg.GetPartitionID()

		if !ibNode.channel.hasSegment(currentSegID, true) {
			err = ibNode.channel.addSegment(
				addSegmentReq{
					segType:     datapb.SegmentType_New,
					segID:       currentSegID,
					collID:      collID,
					partitionID: partitionID,
					startPos:    startPos,
					endPos:      endPos,
				})
			if err != nil {
				log.Warn("add segment wrong",
					zap.Int64("segID", currentSegID),
					zap.Int64("collID", collID),
					zap.Int64("partID", partitionID),
					zap.String("chanName", msg.GetShardName()),
					zap.Error(err))
				return
			}
		}

		segNum := uniqueSeg[currentSegID]
		uniqueSeg[currentSegID] = segNum + int64(len(msg.RowIDs))
	}

	seg2Upload = make([]UniqueID, 0, len(uniqueSeg))
	for id, num := range uniqueSeg {
		seg2Upload = append(seg2Upload, id)
		ibNode.channel.updateSegmentRowNumber(id, num)
	}

	return
}

/* #nosec G103 */
// bufferInsertMsg put InsertMsg into buffer
// 	1.1 fetch related schema from channel meta
// 	1.2 Get buffer data and put data into each field buffer
// 	1.3 Put back into buffer
// 	1.4 Update related statistics
func (ibNode *insertBufferNode) bufferInsertMsg(msg *msgstream.InsertMsg, startPos, endPos *msgpb.MsgPosition) error {
	if err := msg.CheckAligned(); err != nil {
		return err
	}
	currentSegID := msg.GetSegmentID()
	collectionID := msg.GetCollectionID()

	collSchema, err := ibNode.channel.getCollectionSchema(collectionID, msg.EndTs())
	if err != nil {
		log.Warn("Get schema wrong:", zap.Error(err))
		return err
	}

	// load or store insertBuffer
	var buffer *BufferData
	var loaded bool
	buffer, loaded = ibNode.channel.getCurInsertBuffer(currentSegID)
	if !loaded {
		buffer, err = newBufferData(collSchema)
		if err != nil {
			return fmt.Errorf("newBufferData failed, segment=%d, channel=%s, err=%w", currentSegID, ibNode.channelName, err)
		}
	}

	addedBuffer, err := storage.InsertMsgToInsertData(msg, collSchema)
	if err != nil {
		log.Warn("failed to transfer insert msg to insert data", zap.Error(err))
		return err
	}

	addedPfData, err := storage.GetPkFromInsertData(collSchema, addedBuffer)
	if err != nil {
		log.Warn("no primary field found in insert msg", zap.Error(err))
	} else {
		ibNode.channel.updateSegmentPKRange(currentSegID, addedPfData)
	}

	// Maybe there are large write zoom if frequent insert requests are met.
	buffer.buffer = storage.MergeInsertData(buffer.buffer, addedBuffer)

	tsData, err := storage.GetTimestampFromInsertData(addedBuffer)
	if err != nil {
		log.Warn("no timestamp field found in insert msg", zap.Error(err))
		return err
	}

	// update buffer size
	buffer.updateSize(int64(msg.NRows()))
	// update timestamp range and start-end position
	buffer.updateTimeRange(ibNode.getTimestampRange(tsData))
	buffer.updateStartAndEndPosition(startPos, endPos)

	metrics.DataNodeConsumeMsgRowsCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Add(float64(len(msg.RowData)))

	// store in buffer
	ibNode.channel.setCurInsertBuffer(currentSegID, buffer)

	return nil
}

func (ibNode *insertBufferNode) getTimestampRange(tsData *storage.Int64FieldData) TimeRange {
	tr := TimeRange{
		timestampMin: math.MaxUint64,
		timestampMax: 0,
	}

	for _, data := range tsData.Data {
		if uint64(data) < tr.timestampMin {
			tr.timestampMin = Timestamp(data)
		}
		if uint64(data) > tr.timestampMax {
			tr.timestampMax = Timestamp(data)
		}
	}
	return tr
}

// WriteTimeTick writes timetick once insertBufferNode operates.
func (ibNode *insertBufferNode) WriteTimeTick(ts Timestamp, segmentIDs []int64) {

	select {
	case resendTTMsg := <-ibNode.resendTTChan:
		log.Info("resend TT msg received in insertBufferNode",
			zap.Int64s("segmentIDs", resendTTMsg.segmentIDs))
		segmentIDs = append(segmentIDs, resendTTMsg.segmentIDs...)
	default:
	}

	if Params.DataNodeCfg.DataNodeTimeTickByRPC.GetAsBool() {
		stats := make([]*commonpb.SegmentStats, 0, len(segmentIDs))
		for _, sid := range segmentIDs {
			stat, err := ibNode.channel.getSegmentStatisticsUpdates(sid)
			if err != nil {
				log.Warn("failed to get segment statistics info", zap.Int64("segmentID", sid), zap.Error(err))
				continue
			}
			stats = append(stats, stat)
		}
		ibNode.timeTickSender.update(ibNode.channelName, ts, stats)
	} else {
		ibNode.ttLogger.LogTs(ts)
		ibNode.ttMerger.bufferTs(ts, segmentIDs)
	}

	rateCol.updateFlowGraphTt(ibNode.channelName, ts)
}

func (ibNode *insertBufferNode) getCollectionandPartitionIDbySegID(segmentID UniqueID) (collID, partitionID UniqueID, err error) {
	return ibNode.channel.getCollectionAndPartitionID(segmentID)
}

func newInsertBufferNode(ctx context.Context, collID UniqueID, delBufManager *DeltaBufferManager, flushCh <-chan flushMsg, resendTTCh <-chan resendTTMsg,
	fm flushManager, flushingSegCache *Cache, config *nodeConfig, timeTickManager *timeTickSender) (*insertBufferNode, error) {

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(config.maxQueueLength)
	baseNode.SetMaxParallelism(config.maxParallelism)

	if Params.DataNodeCfg.DataNodeTimeTickByRPC.GetAsBool() {
		return &insertBufferNode{
			ctx:      ctx,
			BaseNode: baseNode,

			flushMap:         sync.Map{},
			flushChan:        flushCh,
			resendTTChan:     resendTTCh,
			flushingSegCache: flushingSegCache,
			flushManager:     fm,

			delBufferManager: delBufManager,
			channel:          config.channel,
			idAllocator:      config.allocator,
			channelName:      config.vChannelName,
			timeTickSender:   timeTickManager,
		}, nil
	}

	//input stream, data node time tick
	wTt, err := config.msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	wTt.AsProducer([]string{Params.CommonCfg.DataCoordTimeTick.GetValue()})
	metrics.DataNodeNumProducers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	log.Info("datanode AsProducer", zap.String("TimeTickChannelName", Params.CommonCfg.DataCoordTimeTick.GetValue()))
	var wTtMsgStream msgstream.MsgStream = wTt

	mt := newMergedTimeTickerSender(func(ts Timestamp, segmentIDs []int64) error {
		stats := make([]*commonpb.SegmentStats, 0, len(segmentIDs))
		for _, sid := range segmentIDs {
			stat, err := config.channel.getSegmentStatisticsUpdates(sid)
			if err != nil {
				log.Warn("failed to get segment statistics info", zap.Int64("segmentID", sid), zap.Error(err))
				continue
			}
			stats = append(stats, stat)
		}
		msgPack := msgstream.MsgPack{}
		timeTickMsg := msgstream.DataNodeTtMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: ts,
				EndTimestamp:   ts,
				HashValues:     []uint32{0},
			},
			DataNodeTtMsg: msgpb.DataNodeTtMsg{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
					commonpbutil.WithMsgID(0),
					commonpbutil.WithTimeStamp(ts),
					commonpbutil.WithSourceID(config.serverID),
				),
				ChannelName:   config.vChannelName,
				Timestamp:     ts,
				SegmentsStats: stats,
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
		sub := tsoutil.SubByNow(ts)
		pChan := funcutil.ToPhysicalChannel(config.vChannelName)
		metrics.DataNodeProduceTimeTickLag.
			WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(collID), pChan).
			Set(float64(sub))
		return wTtMsgStream.Produce(&msgPack)
	})

	return &insertBufferNode{
		ctx:      ctx,
		BaseNode: baseNode,

		timeTickStream:   wTtMsgStream,
		flushMap:         sync.Map{},
		flushChan:        flushCh,
		resendTTChan:     resendTTCh,
		flushingSegCache: flushingSegCache,
		flushManager:     fm,

		delBufferManager: delBufManager,
		channel:          config.channel,
		idAllocator:      config.allocator,
		channelName:      config.vChannelName,
		ttMerger:         mt,
		ttLogger:         &timeTickLogger{vChannelName: config.vChannelName},
	}, nil
}
