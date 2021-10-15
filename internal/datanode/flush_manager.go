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
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// flushManager defines a flush manager signature
type flushManager interface {
	// notify flush manager insert buffer data
	flushBufferData(data *BufferData, segmentID UniqueID, pos *internalpb.MsgPosition)
	// notify flush manager del buffer data
	flushDelData(data *DelDataBuf, segmentID UniqueID, pos *internalpb.MsgPosition)
}

// segmentFlushPack contains result to save into meta
type segmentFlushPack struct {
	segmentID  UniqueID
	insertLogs []string
	statsLogs  []string
	deltaLogs  []string
	pos        *internalpb.MsgPosition
}

// notifyMetaFunc notify meta to persistent flush result
type notifyMetaFunc func(*segmentFlushPack) error

// taskPostFunc clean up function after single flush task done
type taskPostFunc func()

// make sure implementation
var _ flushManager = (*rendezvousFlushManager)(nil)

type orderFlushQueue struct {
	sync.Once
	// MsgID => flushTask
	working    sync.Map
	notifyFunc notifyMetaFunc

	tailMut sync.Mutex
	tailCh  chan struct{}
}

// newOrderFlushQueue creates a orderFlushQueue
func newOrderFlushQueue(f notifyMetaFunc) *orderFlushQueue {
	return &orderFlushQueue{
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
	actual, loaded := q.working.LoadOrStore(string(pos.MsgID), newFlushTaskRunner())
	t := actual.(*flushTaskRunner)
	if !loaded {
		q.tailMut.Lock()
		t.init(q.notifyFunc, func() {
			q.working.Delete(string(pos.MsgID))
		}, q.tailCh)
		t.pos = pos
		q.tailCh = t.finishSignal
		q.tailMut.Unlock()
	}
	return t
}

// enqueueInsertBuffer put insert buffer data into queue
func (q *orderFlushQueue) enqueueInsertFlush(task flushInsertTask, pos *internalpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushInsert(task)
}

// enqueueDelBuffer put delete buffer data into queue
func (q *orderFlushQueue) enqueueDelFlush(task flushDeleteTask, pos *internalpb.MsgPosition) {
	q.getFlushTaskRunner(pos).runFlushDel(task)
}

// rendezvousFlushManager makes sure insert & del buf all flushed
type rendezvousFlushManager struct {
	allocatorInterface
	kv.BaseKV

	// segment id => flush queue
	dispatcher sync.Map
	notifyFunc notifyMetaFunc
}

// getFlushQueue
func (m *rendezvousFlushManager) getFlushQueue(segmentID UniqueID) *orderFlushQueue {
	actual, loaded := m.dispatcher.LoadOrStore(segmentID, newOrderFlushQueue(m.notifyFunc))
	// all operation on dispatcher is private, assertion ok guaranteed
	queue := actual.(*orderFlushQueue)
	if !loaded {
		queue.init()
	}
	return queue
}

// notify flush manager insert buffer data
func (m *rendezvousFlushManager) flushBufferData(data *BufferData, segmentID UniqueID,
	pos *internalpb.MsgPosition) {
	m.getFlushQueue(segmentID).enqueueInsertFlush(&flushBufferInsertTask{
		BaseKV:             m.BaseKV,
		allocatorInterface: m.allocatorInterface,
		data:               data,
	}, pos)
}

// notify flush manager del buffer data
func (m *rendezvousFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID,
	pos *internalpb.MsgPosition) {
	m.getFlushQueue(segmentID).enqueueDelFlush(&flushBufferDeleteTask{
		BaseKV:             m.BaseKV,
		allocatorInterface: m.allocatorInterface,
		data:               data,
	}, pos)
}

type flushBufferInsertTask struct {
	kv.BaseKV
	allocatorInterface
	data *BufferData
}

// flushInsertData implements flushInsertTask
func (t *flushBufferInsertTask) flushInsertData() error {
	//TODO implement
	return nil
}

type flushBufferDeleteTask struct {
	kv.BaseKV
	allocatorInterface
	data *DelDataBuf
}

// flushDeleteData implements flushDeleteTask
func (t *flushBufferDeleteTask) flushDeleteData() error {
	//TODO implement
	return nil
}

// NewRendezvousFlushManager create rendezvousFlushManager with provided allocator and kv
func NewRendezvousFlushManager(allocator allocatorInterface, kv kv.BaseKV, f notifyMetaFunc) *rendezvousFlushManager {
	return &rendezvousFlushManager{
		allocatorInterface: allocator,
		BaseKV:             kv,
		notifyFunc:         f,
	}
}
