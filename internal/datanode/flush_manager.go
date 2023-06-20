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
	"path"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// flushManager defines a flush manager signature
type flushManager interface {
	// notify flush manager insert buffer data
	flushBufferData(data *BufferData, segmentID UniqueID, flushed bool, dropped bool, pos *msgpb.MsgPosition) (*storage.PrimaryKeyStats, error)
	// notify flush manager del buffer data
	flushDelData(data *DelDataBuf, segmentID UniqueID, pos *msgpb.MsgPosition) error
	// isFull return true if the task pool is full
	isFull() bool
	// injectFlush injects compaction or other blocking task before flush sync
	injectFlush(injection *taskInjection, segments ...UniqueID)
	// startDropping changes flush manager into dropping mode
	startDropping()
	// notifyAllFlushed tells flush manager there is not future incoming flush task for drop mode
	notifyAllFlushed()
	// close handles resource clean up
	close()
}

// segmentFlushPack contains result to save into meta
type segmentFlushPack struct {
	segmentID  UniqueID
	insertLogs map[UniqueID]*datapb.Binlog
	statsLogs  map[UniqueID]*datapb.Binlog
	deltaLogs  []*datapb.Binlog
	pos        *msgpb.MsgPosition
	flushed    bool
	dropped    bool
	err        error // task execution error, if not nil, notify func should stop datanode
}

// notifyMetaFunc notify meta to persistent flush result
type notifyMetaFunc func(*segmentFlushPack)

// flushAndDropFunc notifies meta to flush current state and drop virtual channel
type flushAndDropFunc func([]*segmentFlushPack)

// taskPostFunc clean up function after single flush task done
type taskPostFunc func(pack *segmentFlushPack, postInjection postInjectionFunc)

// postInjectionFunc post injection pack process logic
type postInjectionFunc func(pack *segmentFlushPack)

// make sure implementation
var _ flushManager = (*rendezvousFlushManager)(nil)

// orderFlushQueue keeps the order of task notifyFunc execution in order
type orderFlushQueue struct {
	sync.Once
	segmentID UniqueID
	injectCh  chan *taskInjection

	// MsgID => flushTask
	working    sync.Map
	notifyFunc notifyMetaFunc

	tailMut sync.Mutex
	tailCh  chan struct{}

	injectMut     sync.Mutex
	runningTasks  int32
	postInjection postInjectionFunc
}

// newOrderFlushQueue creates an orderFlushQueue
func newOrderFlushQueue(segID UniqueID, f notifyMetaFunc) *orderFlushQueue {
	q := &orderFlushQueue{
		segmentID:  segID,
		notifyFunc: f,
		injectCh:   make(chan *taskInjection, 100),
	}
	return q
}

// init orderFlushQueue use once protect init, init tailCh
func (q *orderFlushQueue) init() {
	q.Once.Do(func() {
		// new queue acts like tailing task is done
		q.tailCh = make(chan struct{})
		close(q.tailCh)
	})
}

func (q *orderFlushQueue) getFlushTaskRunner(pos *msgpb.MsgPosition) *flushTaskRunner {
	actual, loaded := q.working.LoadOrStore(getSyncTaskID(pos), newFlushTaskRunner(q.segmentID, q.injectCh))
	t := actual.(*flushTaskRunner)
	// not loaded means the task runner is new, do initializtion
	if !loaded {
		// take over injection if task queue is handling it
		q.injectMut.Lock()
		q.runningTasks++
		q.injectMut.Unlock()
		// add task to tail
		q.tailMut.Lock()
		t.init(q.notifyFunc, q.postTask, q.tailCh)
		q.tailCh = t.finishSignal
		q.tailMut.Unlock()
		log.Info("new flush task runner created and initialized",
			zap.Int64("segment ID", q.segmentID),
			zap.String("pos message ID", string(pos.GetMsgID())),
		)
	}
	return t
}

// postTask handles clean up work after a task is done
func (q *orderFlushQueue) postTask(pack *segmentFlushPack, postInjection postInjectionFunc) {
	// delete task from working map
	q.working.Delete(getSyncTaskID(pack.pos))
	// after descreasing working count, check whether flush queue is empty
	q.injectMut.Lock()
	q.runningTasks--
	// set postInjection function if injection is handled in task
	if postInjection != nil {
		q.postInjection = postInjection
	}

	if q.postInjection != nil {
		q.postInjection(pack)
	}

	// if flush queue is empty, drain all injection from injectCh
	if q.runningTasks == 0 {
		for i := 0; i < len(q.injectCh); i++ {
			inject := <-q.injectCh
			go q.handleInject(inject)
		}
	}

	q.injectMut.Unlock()
}

// enqueueInsertBuffer put insert buffer data into queue
func (q *orderFlushQueue) enqueueInsertFlush(task flushInsertTask, binlogs, statslogs map[UniqueID]*datapb.Binlog, flushed bool, dropped bool, pos *msgpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushInsert(task, binlogs, statslogs, flushed, dropped, pos)
}

// enqueueDelBuffer put delete buffer data into queue
func (q *orderFlushQueue) enqueueDelFlush(task flushDeleteTask, deltaLogs *DelDataBuf, pos *msgpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushDel(task, deltaLogs)
}

// inject performs injection for current task queue
// send into injectCh in there is running task
// or perform injection logic here if there is no injection
func (q *orderFlushQueue) inject(inject *taskInjection) {
	q.injectMut.Lock()
	defer q.injectMut.Unlock()
	// check if there are running task(s)
	// if true, just put injection into injectCh
	// in case of task misses an injection, the injectCh shall be drained in `postTask`
	if q.runningTasks > 0 {
		q.injectCh <- inject
		return
	}
	// otherwise just handle injection here
	go q.handleInject(inject)
}

func (q *orderFlushQueue) handleInject(inject *taskInjection) {
	// notify one injection done
	inject.injectOne()
	ok := <-inject.injectOver
	// apply injection
	if ok {
		q.injectMut.Lock()
		defer q.injectMut.Unlock()
		q.postInjection = inject.postInjection
	}
}

/*
// injectionHandler handles injection for empty flush queue
type injectHandler struct {
	once sync.Once
	wg   sync.WaitGroup
	done chan struct{}
}

// newInjectHandler create injection handler for flush queue
func newInjectHandler(q *orderFlushQueue) *injectHandler {
	h := &injectHandler{
		done: make(chan struct{}),
	}
	h.wg.Add(1)
	go h.handleInjection(q)
	return h
}

func (h *injectHandler) handleInjection(q *orderFlushQueue) {
	defer h.wg.Done()
	for {
		select {
		case inject := <-q.injectCh:
			q.tailMut.Lock() //Maybe double check
			injectDone := make(chan struct{})
			q.tailCh = injectDone
			q.tailMut.Unlock()
		case <-h.done:
			return
		}
	}
}

func (h *injectHandler) close() {
	h.once.Do(func() {
		close(h.done)
		h.wg.Wait()
	})
}
*/

type dropHandler struct {
	sync.Mutex
	dropFlushWg  sync.WaitGroup
	flushAndDrop flushAndDropFunc
	allFlushed   chan struct{}
	packs        []*segmentFlushPack
}

// rendezvousFlushManager makes sure insert & del buf all flushed
type rendezvousFlushManager struct {
	allocator.Allocator
	storage.ChunkManager
	Channel

	// segment id => flush queue
	dispatcher sync.Map
	notifyFunc notifyMetaFunc

	dropping    atomic.Bool
	dropHandler dropHandler
}

// getFlushQueue gets or creates an orderFlushQueue for segment id if not found
func (m *rendezvousFlushManager) getFlushQueue(segmentID UniqueID) *orderFlushQueue {
	newQueue := newOrderFlushQueue(segmentID, m.notifyFunc)
	actual, _ := m.dispatcher.LoadOrStore(segmentID, newQueue)
	// all operation on dispatcher is private, assertion ok guaranteed
	queue := actual.(*orderFlushQueue)
	queue.init()
	return queue
}

func (m *rendezvousFlushManager) handleInsertTask(segmentID UniqueID, task flushInsertTask, binlogs, statslogs map[UniqueID]*datapb.Binlog, flushed bool, dropped bool, pos *msgpb.MsgPosition) {
	log.Info("handling insert task",
		zap.Int64("segment ID", segmentID),
		zap.Bool("flushed", flushed),
		zap.Bool("dropped", dropped),
		zap.Any("position", pos),
	)
	// in dropping mode
	if m.dropping.Load() {
		r := &flushTaskRunner{
			WaitGroup: sync.WaitGroup{},
			segmentID: segmentID,
		}
		r.WaitGroup.Add(1) // insert and delete are not bound in drop mode
		r.runFlushInsert(task, binlogs, statslogs, flushed, dropped, pos)
		r.WaitGroup.Wait()

		m.dropHandler.Lock()
		defer m.dropHandler.Unlock()
		m.dropHandler.packs = append(m.dropHandler.packs, r.getFlushPack())

		return
	}
	// normal mode
	m.getFlushQueue(segmentID).enqueueInsertFlush(task, binlogs, statslogs, flushed, dropped, pos)
}

func (m *rendezvousFlushManager) handleDeleteTask(segmentID UniqueID, task flushDeleteTask, deltaLogs *DelDataBuf, pos *msgpb.MsgPosition) {
	log.Info("handling delete task", zap.Int64("segment ID", segmentID))
	// in dropping mode
	if m.dropping.Load() {
		// preventing separate delete, check position exists in queue first
		q := m.getFlushQueue(segmentID)
		_, ok := q.working.Load(getSyncTaskID(pos))
		// if ok, means position insert data already in queue, just handle task in normal mode
		// if not ok, means the insert buf should be handle in drop mode
		if !ok {
			r := &flushTaskRunner{
				WaitGroup: sync.WaitGroup{},
				segmentID: segmentID,
			}
			r.WaitGroup.Add(1) // insert and delete are not bound in drop mode
			r.runFlushDel(task, deltaLogs)
			r.WaitGroup.Wait()

			m.dropHandler.Lock()
			defer m.dropHandler.Unlock()
			m.dropHandler.packs = append(m.dropHandler.packs, r.getFlushPack())
			return
		}
	}
	// normal mode
	m.getFlushQueue(segmentID).enqueueDelFlush(task, deltaLogs, pos)
}

func (m *rendezvousFlushManager) serializeBinLog(segmentID, partID int64, data *BufferData, inCodec *storage.InsertCodec) ([]*Blob, map[int64]int, error) {
	fieldMemorySize := make(map[int64]int)

	if data == nil || data.buffer == nil {
		return []*Blob{}, fieldMemorySize, nil
	}

	// get memory size of buffer data
	for fieldID, fieldData := range data.buffer.Data {
		fieldMemorySize[fieldID] = fieldData.GetMemorySize()
	}

	// encode data and convert output data
	blobs, err := inCodec.Serialize(partID, segmentID, data.buffer)
	if err != nil {
		return nil, nil, err
	}
	return blobs, fieldMemorySize, nil
}

func (m *rendezvousFlushManager) serializePkStatsLog(segmentID int64, flushed bool, data *BufferData, inCodec *storage.InsertCodec) (*Blob, *storage.PrimaryKeyStats, error) {
	var err error
	var stats *storage.PrimaryKeyStats

	pkField := getPKField(inCodec.Schema)
	if pkField == nil {
		log.Error("No pk field in schema", zap.Int64("segmentID", segmentID), zap.Int64("collectionID", inCodec.Schema.GetID()))
		return nil, nil, fmt.Errorf("no primary key in meta")
	}

	var insertData storage.FieldData
	rowNum := int64(0)
	if data != nil && data.buffer != nil {
		insertData = data.buffer.Data[pkField.FieldID]
		rowNum = int64(insertData.RowNum())
		if insertData.RowNum() > 0 {
			// gen stats of buffer insert data
			stats = storage.NewPrimaryKeyStats(pkField.FieldID, int64(pkField.DataType), rowNum)
			stats.UpdateByMsgs(insertData)
		}
	}

	// get all stats log as a list, serialize to blob
	// if flushed
	if flushed {
		seg := m.getSegment(segmentID)
		if seg == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}

		statsList, oldRowNum := seg.getHistoricalStats(pkField)
		if stats != nil {
			statsList = append(statsList, stats)
		}

		blob, err := inCodec.SerializePkStatsList(statsList, oldRowNum+rowNum)
		if err != nil {
			return nil, nil, err
		}
		return blob, stats, nil
	}

	if rowNum == 0 {
		return nil, nil, nil
	}

	// only serialize stats gen from new insert data
	// if not flush
	blob, err := inCodec.SerializePkStats(stats, rowNum)
	if err != nil {
		return nil, nil, err
	}

	return blob, stats, nil
}

// isFull return true if the task pool is full
func (m *rendezvousFlushManager) isFull() bool {
	var num int
	m.dispatcher.Range(func(_, q any) bool {
		queue := q.(*orderFlushQueue)
		queue.working.Range(func(_, _ any) bool {
			num++
			return true
		})
		return true
	})
	return num >= Params.DataNodeCfg.MaxParallelSyncTaskNum.GetAsInt()
}

// flushBufferData notifies flush manager insert buffer data.
// This method will be retired on errors. Final errors will be propagated upstream and logged.
func (m *rendezvousFlushManager) flushBufferData(data *BufferData, segmentID UniqueID, flushed bool, dropped bool, pos *msgpb.MsgPosition) (*storage.PrimaryKeyStats, error) {
	field2Insert := make(map[UniqueID]*datapb.Binlog)
	field2Stats := make(map[UniqueID]*datapb.Binlog)
	kvs := make(map[string][]byte)

	tr := timerecord.NewTimeRecorder("flushDuration")
	// get segment info
	collID, partID, meta, err := m.getSegmentMeta(segmentID, pos)
	if err != nil {
		return nil, err
	}
	inCodec := storage.NewInsertCodecWithSchema(meta)
	// build bin log blob
	binLogBlobs, fieldMemorySize, err := m.serializeBinLog(segmentID, partID, data, inCodec)
	if err != nil {
		return nil, err
	}

	// build stats log blob
	pkStatsBlob, stats, err := m.serializePkStatsLog(segmentID, flushed, data, inCodec)
	if err != nil {
		return nil, err
	}

	// allocate
	// alloc for stats log if have new stats log and not flushing
	var logidx int64
	allocNum := uint32(len(binLogBlobs) + boolToInt(!flushed && pkStatsBlob != nil))
	if allocNum != 0 {
		logidx, _, err = m.Alloc(allocNum)
		if err != nil {
			return nil, err
		}
	}

	// binlogs
	for _, blob := range binLogBlobs {
		defer func() { logidx++ }()
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, err
		}

		k := metautil.JoinIDPath(collID, partID, segmentID, fieldID, logidx)
		// [rootPath]/[insert_log]/key
		key := path.Join(m.ChunkManager.RootPath(), common.SegmentInsertLogPath, k)
		kvs[key] = blob.Value[:]
		field2Insert[fieldID] = &datapb.Binlog{
			EntriesNum:    data.size,
			TimestampFrom: data.tsFrom,
			TimestampTo:   data.tsTo,
			LogPath:       key,
			LogSize:       int64(fieldMemorySize[fieldID]),
		}
	}

	// pk stats binlog
	if pkStatsBlob != nil {
		fieldID, err := strconv.ParseInt(pkStatsBlob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, err
		}

		// use storage.FlushedStatsLogIdx as logidx if flushed
		// else use last idx we allocated
		var key string
		if flushed {
			k := metautil.JoinIDPath(collID, partID, segmentID, fieldID)
			key = path.Join(m.ChunkManager.RootPath(), common.SegmentStatslogPath, k, storage.CompoundStatsType.LogIdx())
		} else {
			k := metautil.JoinIDPath(collID, partID, segmentID, fieldID, logidx)
			key = path.Join(m.ChunkManager.RootPath(), common.SegmentStatslogPath, k)
		}

		kvs[key] = pkStatsBlob.Value
		field2Stats[fieldID] = &datapb.Binlog{
			EntriesNum:    0,
			TimestampFrom: 0, //TODO
			TimestampTo:   0, //TODO,
			LogPath:       key,
			LogSize:       int64(len(pkStatsBlob.Value)),
		}
	}

	m.handleInsertTask(segmentID, &flushBufferInsertTask{
		ChunkManager: m.ChunkManager,
		data:         kvs,
	}, field2Insert, field2Stats, flushed, dropped, pos)

	metrics.DataNodeEncodeBufferLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return stats, nil
}

// notify flush manager del buffer data
func (m *rendezvousFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID,
	pos *msgpb.MsgPosition) error {

	// del signal with empty data
	if data == nil || data.delData == nil {
		m.handleDeleteTask(segmentID, &flushBufferDeleteTask{}, nil, pos)
		return nil
	}

	collID, partID, err := m.getCollectionAndPartitionID(segmentID)
	if err != nil {
		return err
	}

	delCodec := storage.NewDeleteCodec()

	blob, err := delCodec.Serialize(collID, partID, segmentID, data.delData)
	if err != nil {
		return err
	}

	logID, err := m.AllocOne()
	if err != nil {
		log.Error("failed to alloc ID", zap.Error(err))
		return err
	}

	blobKey := metautil.JoinIDPath(collID, partID, segmentID, logID)
	blobPath := path.Join(m.ChunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)
	kvs := map[string][]byte{blobPath: blob.Value[:]}
	data.LogSize = int64(len(blob.Value))
	data.LogPath = blobPath
	log.Info("delete blob path", zap.String("path", blobPath))
	m.handleDeleteTask(segmentID, &flushBufferDeleteTask{
		ChunkManager: m.ChunkManager,
		data:         kvs,
	}, data, pos)
	return nil
}

// injectFlush inject process before task finishes
func (m *rendezvousFlushManager) injectFlush(injection *taskInjection, segments ...UniqueID) {
	go injection.waitForInjected()
	for _, segmentID := range segments {
		m.getFlushQueue(segmentID).inject(injection)
	}
}

// fetch meta info for segment
func (m *rendezvousFlushManager) getSegmentMeta(segmentID UniqueID, pos *msgpb.MsgPosition) (UniqueID, UniqueID, *etcdpb.CollectionMeta, error) {
	if !m.hasSegment(segmentID, true) {
		return -1, -1, nil, fmt.Errorf("no such segment %d in the channel", segmentID)
	}

	// fetch meta information of segment
	collID, partID, err := m.getCollectionAndPartitionID(segmentID)
	if err != nil {
		return -1, -1, nil, err
	}
	sch, err := m.getCollectionSchema(collID, pos.GetTimestamp())
	if err != nil {
		return -1, -1, nil, err
	}

	meta := &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return collID, partID, meta, nil
}

// waitForAllTaskQueue waits for all flush queues in dispatcher become empty
func (m *rendezvousFlushManager) waitForAllFlushQueue() {
	var wg sync.WaitGroup
	m.dispatcher.Range(func(k, v interface{}) bool {
		queue := v.(*orderFlushQueue)
		wg.Add(1)
		go func() {
			<-queue.tailCh
			wg.Done()
		}()
		return true
	})
	wg.Wait()
}

// startDropping changes flush manager into dropping mode
func (m *rendezvousFlushManager) startDropping() {
	m.dropping.Store(true)
	m.dropHandler.allFlushed = make(chan struct{})
	go func() {
		<-m.dropHandler.allFlushed       // all needed flush tasks are in flush manager now
		m.waitForAllFlushQueue()         // waits for all the normal flush queue done
		m.dropHandler.dropFlushWg.Wait() // waits for all drop mode task done
		m.dropHandler.Lock()
		defer m.dropHandler.Unlock()
		// apply injection if any
		for _, pack := range m.dropHandler.packs {
			q := m.getFlushQueue(pack.segmentID)
			// queue will never be nil, sincde getFlushQueue will initialize one if not found
			q.injectMut.Lock()
			if q.postInjection != nil {
				q.postInjection(pack)
			}
			q.injectMut.Unlock()
		}
		m.dropHandler.flushAndDrop(m.dropHandler.packs) // invoke drop & flush
	}()
}

func (m *rendezvousFlushManager) notifyAllFlushed() {
	close(m.dropHandler.allFlushed)
}

func getSyncTaskID(pos *msgpb.MsgPosition) string {
	// use msgID & timestamp to generate unique taskID, see also #20926
	return fmt.Sprintf("%s%d", string(pos.GetMsgID()), pos.GetTimestamp())
}

// close cleans up all the left members
func (m *rendezvousFlushManager) close() {
	m.dispatcher.Range(func(k, v interface{}) bool {
		//assertion ok
		queue := v.(*orderFlushQueue)
		queue.injectMut.Lock()
		for i := 0; i < len(queue.injectCh); i++ {
			go queue.handleInject(<-queue.injectCh)
		}
		queue.injectMut.Unlock()
		return true
	})
	m.waitForAllFlushQueue()
}

type flushBufferInsertTask struct {
	storage.ChunkManager
	data map[string][]byte
}

// flushInsertData implements flushInsertTask
func (t *flushBufferInsertTask) flushInsertData() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if t.ChunkManager != nil && len(t.data) > 0 {
		tr := timerecord.NewTimeRecorder("insertData")
		err := t.MultiWrite(ctx, t.data)
		metrics.DataNodeSave2StorageLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		if err == nil {
			for _, d := range t.data {
				metrics.DataNodeFlushedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Add(float64(len(d)))
			}
		}
		return err
	}
	return nil
}

type flushBufferDeleteTask struct {
	storage.ChunkManager
	data map[string][]byte
}

// flushDeleteData implements flushDeleteTask
func (t *flushBufferDeleteTask) flushDeleteData() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if len(t.data) > 0 && t.ChunkManager != nil {
		tr := timerecord.NewTimeRecorder("deleteData")
		err := t.MultiWrite(ctx, t.data)
		metrics.DataNodeSave2StorageLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		if err == nil {
			for _, d := range t.data {
				metrics.DataNodeFlushedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Add(float64(len(d)))
			}
		}
		return err
	}
	return nil
}

// NewRendezvousFlushManager create rendezvousFlushManager with provided allocator and kv
func NewRendezvousFlushManager(allocator allocator.Allocator, cm storage.ChunkManager, channel Channel, f notifyMetaFunc, drop flushAndDropFunc) *rendezvousFlushManager {
	fm := &rendezvousFlushManager{
		Allocator:    allocator,
		ChunkManager: cm,
		notifyFunc:   f,
		Channel:      channel,
		dropHandler: dropHandler{
			flushAndDrop: drop,
		},
	}
	// start with normal mode
	fm.dropping.Store(false)
	return fm
}

func getFieldBinlogs(fieldID UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
	for _, binlog := range binlogs {
		if fieldID == binlog.GetFieldID() {
			return binlog
		}
	}
	return nil
}

func dropVirtualChannelFunc(dsService *dataSyncService, opts ...retry.Option) flushAndDropFunc {
	return func(packs []*segmentFlushPack) {
		req := &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(0), //TODO msg type
				commonpbutil.WithMsgID(0),   //TODO msg id
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ChannelName: dsService.vchannelName,
		}

		segmentPack := make(map[UniqueID]*datapb.DropVirtualChannelSegment)
		for _, pack := range packs {
			segment, has := segmentPack[pack.segmentID]
			if !has {
				segment = &datapb.DropVirtualChannelSegment{
					SegmentID:    pack.segmentID,
					CollectionID: dsService.collectionID,
				}

				segmentPack[pack.segmentID] = segment
			}
			for k, v := range pack.insertLogs {
				fieldBinlogs := getFieldBinlogs(k, segment.Field2BinlogPaths)
				if fieldBinlogs == nil {
					segment.Field2BinlogPaths = append(segment.Field2BinlogPaths, &datapb.FieldBinlog{
						FieldID: k,
						Binlogs: []*datapb.Binlog{v},
					})
				} else {
					fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, v)
				}
			}
			for k, v := range pack.statsLogs {
				fieldStatsLogs := getFieldBinlogs(k, segment.Field2StatslogPaths)
				if fieldStatsLogs == nil {
					segment.Field2StatslogPaths = append(segment.Field2StatslogPaths, &datapb.FieldBinlog{
						FieldID: k,
						Binlogs: []*datapb.Binlog{v},
					})
				} else {
					fieldStatsLogs.Binlogs = append(fieldStatsLogs.Binlogs, v)
				}
			}
			segment.Deltalogs = append(segment.Deltalogs, &datapb.FieldBinlog{
				Binlogs: pack.deltaLogs,
			})
			updates, _ := dsService.channel.getSegmentStatisticsUpdates(pack.segmentID)
			segment.NumOfRows = updates.GetNumRows()
			if pack.pos != nil {
				if segment.CheckPoint == nil || pack.pos.Timestamp > segment.CheckPoint.Timestamp {
					segment.CheckPoint = pack.pos
				}
			}
		}

		startPos := dsService.channel.listNewSegmentsStartPositions()
		// start positions for all new segments
		for _, pos := range startPos {
			segment, has := segmentPack[pos.GetSegmentID()]
			if !has {
				segment = &datapb.DropVirtualChannelSegment{
					SegmentID:    pos.GetSegmentID(),
					CollectionID: dsService.collectionID,
				}

				segmentPack[pos.GetSegmentID()] = segment
			}
			segment.StartPosition = pos.GetStartPosition()
		}

		// assign segments to request
		segments := make([]*datapb.DropVirtualChannelSegment, 0, len(segmentPack))
		for _, segment := range segmentPack {
			segments = append(segments, segment)
		}
		req.Segments = segments

		err := retry.Do(context.Background(), func() error {
			rsp, err := dsService.dataCoord.DropVirtualChannel(context.Background(), req)
			// should be network issue, return error and retry
			if err != nil {
				return fmt.Errorf(err.Error())
			}

			// meta error, datanode handles a virtual channel does not belong here
			if rsp.GetStatus().GetErrorCode() == commonpb.ErrorCode_MetaFailed {
				log.Warn("meta error found, skip sync and start to drop virtual channel", zap.String("channel", dsService.vchannelName))
				return nil
			}

			// retry for other error
			if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return fmt.Errorf("data service DropVirtualChannel failed, reason = %s", rsp.GetStatus().GetReason())
			}
			dsService.channel.transferNewSegments(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) UniqueID {
				return pos.GetSegmentID()
			}))
			return nil
		}, opts...)
		if err != nil {
			log.Warn("failed to DropVirtualChannel", zap.String("channel", dsService.vchannelName), zap.Error(err))
			panic(err)
		}
		for segID := range segmentPack {
			dsService.channel.segmentFlushed(segID)
			dsService.flushingSegCache.Remove(segID)
		}
	}
}

func flushNotifyFunc(dsService *dataSyncService, opts ...retry.Option) notifyMetaFunc {
	return func(pack *segmentFlushPack) {
		if pack.err != nil {
			log.Error("flush pack with error, DataNode quit now", zap.Error(pack.err))
			// TODO silverxia change to graceful stop datanode
			panic(pack.err)
		}

		var (
			fieldInsert = []*datapb.FieldBinlog{}
			fieldStats  = []*datapb.FieldBinlog{}
			deltaInfos  = make([]*datapb.FieldBinlog, 1)
			checkPoints = []*datapb.CheckPoint{}
		)

		for k, v := range pack.insertLogs {
			fieldInsert = append(fieldInsert, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
		}
		for k, v := range pack.statsLogs {
			fieldStats = append(fieldStats, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
		}
		deltaInfos[0] = &datapb.FieldBinlog{Binlogs: pack.deltaLogs}

		// only current segment checkpoint info,
		updates, _ := dsService.channel.getSegmentStatisticsUpdates(pack.segmentID)
		checkPoints = append(checkPoints, &datapb.CheckPoint{
			SegmentID: pack.segmentID,
			// this shouldn't be used because we are not sure this is aligned
			NumOfRows: updates.GetNumRows(),
			Position:  pack.pos,
		})

		startPos := dsService.channel.listNewSegmentsStartPositions()

		log.Info("SaveBinlogPath",
			zap.Int64("SegmentID", pack.segmentID),
			zap.Int64("CollectionID", dsService.collectionID),
			zap.Any("startPos", startPos),
			zap.Any("checkPoints", checkPoints),
			zap.Int("Length of Field2BinlogPaths", len(fieldInsert)),
			zap.Int("Length of Field2Stats", len(fieldStats)),
			zap.Int("Length of Field2Deltalogs", len(deltaInfos[0].GetBinlogs())),
			zap.String("vChannelName", dsService.vchannelName),
		)

		req := &datapb.SaveBinlogPathsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(0),
				commonpbutil.WithMsgID(0),
				commonpbutil.WithSourceID(dsService.serverID),
			),
			SegmentID:           pack.segmentID,
			CollectionID:        dsService.collectionID,
			Field2BinlogPaths:   fieldInsert,
			Field2StatslogPaths: fieldStats,
			Deltalogs:           deltaInfos,

			CheckPoints: checkPoints,

			StartPositions: startPos,
			Flushed:        pack.flushed,
			Dropped:        pack.dropped,
		}
		err := retry.Do(context.Background(), func() error {
			rsp, err := dsService.dataCoord.SaveBinlogPaths(context.Background(), req)
			// should be network issue, return error and retry
			if err != nil {
				return err
			}

			// Segment not found during stale segment flush. Segment might get compacted already.
			// Stop retry and still proceed to the end, ignoring this error.
			if !pack.flushed && rsp.GetErrorCode() == commonpb.ErrorCode_SegmentNotFound {
				log.Warn("stale segment not found, could be compacted",
					zap.Int64("segment ID", pack.segmentID))
				log.Warn("failed to SaveBinlogPaths",
					zap.Int64("segment ID", pack.segmentID),
					zap.Error(errors.New(rsp.GetReason())))
				return fmt.Errorf("segment %d not found", pack.segmentID)
			}
			// meta error, datanode handles a virtual channel does not belong here
			if rsp.GetErrorCode() == commonpb.ErrorCode_MetaFailed {
				log.Warn("meta error found, skip sync and start to drop virtual channel", zap.String("channel", dsService.vchannelName))
				return nil
			}

			if rsp.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("data service save bin log path failed, reason = %s", rsp.Reason)
			}

			dsService.channel.transferNewSegments(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) UniqueID {
				return pos.GetSegmentID()
			}))
			return nil
		}, opts...)
		if err != nil {
			log.Warn("failed to SaveBinlogPaths",
				zap.Int64("segment ID", pack.segmentID),
				zap.Error(err))
			// TODO change to graceful stop
			panic(err)
		}
		if pack.dropped {
			dsService.channel.removeSegments(pack.segmentID)
		} else if pack.flushed {
			dsService.channel.segmentFlushed(pack.segmentID)
		}

		if dsService.flushListener != nil {
			dsService.flushListener <- pack
		}
		dsService.flushingSegCache.Remove(req.GetSegmentID())
		dsService.channel.evictHistoryInsertBuffer(req.GetSegmentID(), pack.pos)
		dsService.channel.evictHistoryDeleteBuffer(req.GetSegmentID(), pack.pos)
		segment := dsService.channel.getSegment(req.GetSegmentID())
		segment.setSyncing(false)
		// dsService.channel.saveBinlogPath(fieldStats)
	}
}
