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
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"go.uber.org/zap"
)

// flushManager defines a flush manager signature
type flushManager interface {
	// notify flush manager insert buffer data
	flushBufferData(data *BufferData, segmentID UniqueID, flushed bool, pos *internalpb.MsgPosition) error
	// notify flush manager del buffer data
	flushDelData(data *DelDataBuf, segmentID UniqueID, pos *internalpb.MsgPosition) error
}

// segmentFlushPack contains result to save into meta
type segmentFlushPack struct {
	segmentID  UniqueID
	insertLogs map[UniqueID]string
	statsLogs  map[UniqueID]string
	deltaLogs  []*DelDataBuf
	pos        *internalpb.MsgPosition
	flushed    bool
}

// notifyMetaFunc notify meta to persistent flush result
type notifyMetaFunc func(*segmentFlushPack) error

// taskPostFunc clean up function after single flush task done
type taskPostFunc func()

// make sure implementation
var _ flushManager = (*rendezvousFlushManager)(nil)

type orderFlushQueue struct {
	sync.Once
	segmentID UniqueID
	// MsgID => flushTask
	working    sync.Map
	notifyFunc notifyMetaFunc

	tailMut sync.Mutex
	tailCh  chan struct{}
}

// newOrderFlushQueue creates a orderFlushQueue
func newOrderFlushQueue(segID UniqueID, f notifyMetaFunc) *orderFlushQueue {
	return &orderFlushQueue{
		segmentID:  segID,
		notifyFunc: f,
	}
}

// init orderFlushQueue use once protect init, init tailCh
func (q *orderFlushQueue) init() {
	q.Once.Do(func() {
		// new queue acts like tailing task is done
		q.tailCh = make(chan struct{})
		close(q.tailCh)
	})
}

func (q *orderFlushQueue) getFlushTaskRunner(pos *internalpb.MsgPosition) *flushTaskRunner {
	actual, loaded := q.working.LoadOrStore(string(pos.MsgID), newFlushTaskRunner(q.segmentID))
	t := actual.(*flushTaskRunner)
	if !loaded {
		q.tailMut.Lock()
		t.init(q.notifyFunc, func() {
			q.working.Delete(string(pos.MsgID))
		}, q.tailCh)
		q.tailCh = t.finishSignal
		q.tailMut.Unlock()
	}
	return t
}

// enqueueInsertBuffer put insert buffer data into queue
func (q *orderFlushQueue) enqueueInsertFlush(task flushInsertTask, binlogs, statslogs map[UniqueID]string, flushed bool, pos *internalpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushInsert(task, binlogs, statslogs, flushed, pos)
}

// enqueueDelBuffer put delete buffer data into queue
func (q *orderFlushQueue) enqueueDelFlush(task flushDeleteTask, deltaLogs *DelDataBuf, pos *internalpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushDel(task, deltaLogs)
}

// rendezvousFlushManager makes sure insert & del buf all flushed
type rendezvousFlushManager struct {
	allocatorInterface
	kv.BaseKV
	Replica

	// segment id => flush queue
	dispatcher sync.Map
	notifyFunc notifyMetaFunc
}

// getFlushQueue
func (m *rendezvousFlushManager) getFlushQueue(segmentID UniqueID) *orderFlushQueue {
	actual, loaded := m.dispatcher.LoadOrStore(segmentID, newOrderFlushQueue(segmentID, m.notifyFunc))
	// all operation on dispatcher is private, assertion ok guaranteed
	queue := actual.(*orderFlushQueue)
	if !loaded {
		queue.init()
	}
	return queue
}

// notify flush manager insert buffer data
func (m *rendezvousFlushManager) flushBufferData(data *BufferData, segmentID UniqueID, flushed bool,
	pos *internalpb.MsgPosition) error {

	// empty flush
	if data == nil || data.buffer == nil {
		m.getFlushQueue(segmentID).enqueueInsertFlush(&flushBufferInsertTask{},
			map[UniqueID]string{}, map[UniqueID]string{}, flushed, pos)
		return nil
	}

	collID, partID, meta, err := m.getSegmentMeta(segmentID, pos)
	if err != nil {
		return err
	}

	// encode data and convert output data
	inCodec := storage.NewInsertCodec(meta)

	binLogs, statsBinlogs, err := inCodec.Serialize(partID, segmentID, data.buffer)
	if err != nil {
		return err
	}

	start, _, err := m.allocIDBatch(uint32(len(binLogs)))
	if err != nil {
		return err
	}

	field2Insert := make(map[UniqueID]string, len(binLogs))
	kvs := make(map[string]string, len(binLogs))
	paths := make([]string, 0, len(binLogs))
	field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))
	for idx, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return err
		}

		logidx := start + int64(idx)

		// no error raise if alloc=false
		k, _ := m.genKey(false, collID, partID, segmentID, fieldID, logidx)

		key := path.Join(Params.InsertBinlogRootPath, k)
		paths = append(paths, key)
		kvs[key] = string(blob.Value[:])
		field2Insert[fieldID] = key
		field2Logidx[fieldID] = logidx
	}

	field2Stats := make(map[UniqueID]string)
	// write stats binlog
	for _, blob := range statsBinlogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return err
		}

		logidx := field2Logidx[fieldID]

		// no error raise if alloc=false
		k, _ := m.genKey(false, collID, partID, segmentID, fieldID, logidx)

		key := path.Join(Params.StatsBinlogRootPath, k)
		kvs[key] = string(blob.Value[:])
		field2Stats[fieldID] = key
	}

	m.updateSegmentCheckPoint(segmentID)
	m.getFlushQueue(segmentID).enqueueInsertFlush(&flushBufferInsertTask{
		BaseKV: m.BaseKV,
		data:   kvs,
	}, field2Insert, field2Stats, flushed, pos)
	return nil
}

// notify flush manager del buffer data
func (m *rendezvousFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID,
	pos *internalpb.MsgPosition) error {

	// del signal with empty data
	if data == nil || data.delData == nil {
		m.getFlushQueue(segmentID).enqueueDelFlush(&flushBufferDeleteTask{}, nil, pos)
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

	logID, err := m.allocID()
	if err != nil {
		log.Error("failed to alloc ID", zap.Error(err))
		return err
	}

	blobKey, _ := m.genKey(false, collID, partID, segmentID, logID)
	blobPath := path.Join(Params.DeleteBinlogRootPath, blobKey)
	kvs := map[string]string{blobPath: string(blob.Value[:])}
	data.fileSize = int64(len(blob.Value))
	data.filePath = blobPath
	log.Debug("delete blob path", zap.String("path", blobPath))

	m.getFlushQueue(segmentID).enqueueDelFlush(&flushBufferDeleteTask{
		BaseKV: m.BaseKV,
		data:   kvs,
	}, data, pos)
	return nil
}

// fetch meta info for segment
func (m *rendezvousFlushManager) getSegmentMeta(segmentID UniqueID, pos *internalpb.MsgPosition) (UniqueID, UniqueID, *etcdpb.CollectionMeta, error) {
	if !m.hasSegment(segmentID, true) {
		return -1, -1, nil, fmt.Errorf("No such segment %d in the replica", segmentID)
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

type flushBufferInsertTask struct {
	kv.BaseKV
	data map[string]string
}

// flushInsertData implements flushInsertTask
func (t *flushBufferInsertTask) flushInsertData() error {
	if t.BaseKV != nil && len(t.data) > 0 {
		return t.MultiSave(t.data)
	}
	return nil
}

type flushBufferDeleteTask struct {
	kv.BaseKV
	data map[string]string
}

// flushDeleteData implements flushDeleteTask
func (t *flushBufferDeleteTask) flushDeleteData() error {
	if len(t.data) > 0 && t.BaseKV != nil {
		return t.MultiSave(t.data)
	}
	return nil
}

// NewRendezvousFlushManager create rendezvousFlushManager with provided allocator and kv
func NewRendezvousFlushManager(allocator allocatorInterface, kv kv.BaseKV, replica Replica, f notifyMetaFunc) *rendezvousFlushManager {
	return &rendezvousFlushManager{
		allocatorInterface: allocator,
		BaseKV:             kv,
		notifyFunc:         f,
		Replica:            replica,
	}
}
