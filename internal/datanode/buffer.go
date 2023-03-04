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
	"container/heap"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// DelBufferManager is in charge of managing insertBuf and delBuf from an overall prospect
// not only controlling buffered data size based on every segment size, but also triggering
// insert/delete flush when the memory usage of the whole manager reach a certain level.
// but at the first stage, this struct is only used for delete buff
type DelBufferManager struct {
	channel       Channel
	mu            sync.Mutex // guards delMemorySize and delBufHeap
	delMemorySize int64
	delBufHeap    *PriorityQueue
}

func (bm *DelBufferManager) GetSegDelBufMemSize(segID UniqueID) int64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if delDataBuf, ok := bm.channel.getCurDeleteBuffer(segID); ok {
		return delDataBuf.item.memorySize
	}
	return 0
}

func (bm *DelBufferManager) GetEntriesNum(segID UniqueID) int64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if delDataBuf, ok := bm.channel.getCurDeleteBuffer(segID); ok {
		return delDataBuf.GetEntriesNum()
	}
	return 0
}

// Store :the method only for unit test
func (bm *DelBufferManager) Store(segID UniqueID, delDataBuf *DelDataBuf) {
	bm.channel.setCurDeleteBuffer(segID, delDataBuf)
}

func (bm *DelBufferManager) StoreNewDeletes(segID UniqueID, pks []primaryKey,
	tss []Timestamp, tr TimeRange, startPos, endPos *msgpb.MsgPosition) {
	//1. load or create delDataBuf
	var delDataBuf *DelDataBuf
	buffer, loaded := bm.channel.getCurDeleteBuffer(segID)
	if loaded {
		delDataBuf = buffer
	} else {
		delDataBuf = newDelDataBuf()
	}

	//2. fill in new delta
	delData := delDataBuf.delData
	rowCount := len(pks)
	var bufSize int64
	for i := 0; i < rowCount; i++ {
		delData.Pks = append(delData.Pks, pks[i])
		delData.Tss = append(delData.Tss, tss[i])
		switch pks[i].Type() {
		case schemapb.DataType_Int64:
			bufSize += 8
		case schemapb.DataType_VarChar:
			varCharPk := pks[i].(*varCharPrimaryKey)
			bufSize += int64(len(varCharPk.Value))
		}
		//accumulate buf size for timestamp, which is 8 bytes
		bufSize += 8
	}
	//3. update statistics of del data
	delDataBuf.accumulateEntriesNum(int64(rowCount))
	delDataBuf.updateTimeRange(tr)
	delDataBuf.updateStartAndEndPosition(startPos, endPos)

	//4. update and sync memory size with priority queue
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if !loaded {
		delDataBuf.item.segmentID = segID
		delDataBuf.item.memorySize = bufSize
		heap.Push(bm.delBufHeap, delDataBuf.item)
	} else {
		bm.delBufHeap.update(delDataBuf.item, delDataBuf.item.memorySize+bufSize)
	}
	bm.channel.setCurDeleteBuffer(segID, delDataBuf)
	bm.delMemorySize += bufSize
	//4. sync metrics
	metrics.DataNodeConsumeMsgRowsCount.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Add(float64(rowCount))
}

func (bm *DelBufferManager) Load(segID UniqueID) (delDataBuf *DelDataBuf, ok bool) {
	buffer, ok := bm.channel.getCurDeleteBuffer(segID)
	if ok {
		return buffer, ok
	}
	return nil, false
}

func (bm *DelBufferManager) Delete(segID UniqueID) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if buf, ok := bm.channel.getCurDeleteBuffer(segID); ok {
		item := buf.item
		bm.delMemorySize -= item.memorySize
		heap.Remove(bm.delBufHeap, item.index)
		bm.channel.rollDeleteBuffer(segID)
	}
}

func (bm *DelBufferManager) CompactSegBuf(compactedToSegID UniqueID, compactedFromSegIDs []UniqueID) {
	var compactToDelBuff *DelDataBuf
	compactToDelBuff, loaded := bm.Load(compactedToSegID)
	if !loaded {
		compactToDelBuff = newDelDataBuf()
	}

	for _, segID := range compactedFromSegIDs {
		if delDataBuf, loaded := bm.Load(segID); loaded {
			compactToDelBuff.mergeDelDataBuf(delDataBuf)
			bm.Delete(segID)
		}
	}
	bm.mu.Lock()
	defer bm.mu.Unlock()
	// only store delBuf if EntriesNum > 0
	if compactToDelBuff.EntriesNum > 0 {
		if loaded {
			bm.delBufHeap.update(compactToDelBuff.item, compactToDelBuff.item.memorySize)
		} else {
			heap.Push(bm.delBufHeap, compactToDelBuff.item)
		}
		//note that when compacting segment in del buffer manager
		//there is no need to modify the general memory size as there is no new
		//added del into the memory
		bm.channel.setCurDeleteBuffer(compactedToSegID, compactToDelBuff)
	}
}

func (bm *DelBufferManager) ShouldFlushSegments() []UniqueID {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	var shouldFlushSegments []UniqueID
	if bm.delMemorySize < Params.DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64() {
		return shouldFlushSegments
	}
	mmUsage := bm.delMemorySize
	var poppedSegMem []*Item
	for {
		segMem := heap.Pop(bm.delBufHeap).(*Item)
		poppedSegMem = append(poppedSegMem, segMem)
		shouldFlushSegments = append(shouldFlushSegments, segMem.segmentID)
		log.Debug("add segment for delete buf flush", zap.Int64("segmentID", segMem.segmentID))
		mmUsage -= segMem.memorySize
		if mmUsage < Params.DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64() {
			break
		}
	}
	//here we push all selected segment back into the heap
	//in order to keep the heap semantically correct
	for _, segMem := range poppedSegMem {
		heap.Push(bm.delBufHeap, segMem)
	}
	return shouldFlushSegments
}

// An Item is something we manage in a memorySize priority queue.
type Item struct {
	segmentID  UniqueID // The segmentID
	memorySize int64    // The size of memory consumed by del buf
	index      int      // The index of the item in the heap.
	// The index is needed by update and is maintained by the heap.Interface methods.
}

// A PriorityQueue implements heap.Interface and holds Items.
// We use PriorityQueue to manage memory consumed by del buf
type PriorityQueue struct {
	items []*Item
}

func (pq *PriorityQueue) Len() int { return len(pq.items) }

func (pq *PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, memorySize so we use greater than here.
	return pq.items[i].memorySize > pq.items[j].memorySize
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(pq.items)
	item := x.(*Item)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, memorySize int64) {
	item.memorySize = memorySize
	heap.Fix(pq, item.index)
}

// BufferData buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type BufferData struct {
	buffer   *InsertData
	size     int64
	limit    int64
	tsFrom   Timestamp
	tsTo     Timestamp
	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition
}

func (bd *BufferData) effectiveCap() int64 {
	return bd.limit - bd.size
}

func (bd *BufferData) updateSize(no int64) {
	bd.size += no
}

// updateTimeRange update BufferData tsFrom, tsTo range according to input time range
func (bd *BufferData) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < bd.tsFrom {
		bd.tsFrom = tr.timestampMin
	}
	if tr.timestampMax > bd.tsTo {
		bd.tsTo = tr.timestampMax
	}
}

func (bd *BufferData) updateStartAndEndPosition(startPos *msgpb.MsgPosition, endPos *msgpb.MsgPosition) {
	if bd.startPos == nil || startPos.Timestamp < bd.startPos.Timestamp {
		bd.startPos = startPos
	}
	if bd.endPos == nil || endPos.Timestamp > bd.endPos.Timestamp {
		bd.endPos = endPos
	}
}

func (bd *BufferData) memorySize() int64 {
	var size int64
	for _, field := range bd.buffer.Data {
		size += int64(field.GetMemorySize())
	}
	return size
}

// DelDataBuf buffers delete data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type DelDataBuf struct {
	datapb.Binlog
	delData  *DeleteData
	item     *Item
	startPos *msgpb.MsgPosition
	endPos   *msgpb.MsgPosition
}

func (ddb *DelDataBuf) accumulateEntriesNum(entryNum int64) {
	ddb.EntriesNum += entryNum
}

func (ddb *DelDataBuf) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < ddb.TimestampFrom {
		ddb.TimestampFrom = tr.timestampMin
	}
	if tr.timestampMax > ddb.TimestampTo {
		ddb.TimestampTo = tr.timestampMax
	}
}

func (ddb *DelDataBuf) mergeDelDataBuf(buf *DelDataBuf) {
	ddb.accumulateEntriesNum(buf.EntriesNum)

	tr := TimeRange{timestampMax: buf.TimestampTo, timestampMin: buf.TimestampFrom}
	ddb.updateTimeRange(tr)
	ddb.updateStartAndEndPosition(buf.startPos, buf.endPos)

	ddb.delData.Pks = append(ddb.delData.Pks, buf.delData.Pks...)
	ddb.delData.Tss = append(ddb.delData.Tss, buf.delData.Tss...)
	ddb.item.memorySize += buf.item.memorySize
}

func (ddb *DelDataBuf) updateStartAndEndPosition(startPos *msgpb.MsgPosition, endPos *msgpb.MsgPosition) {
	if ddb.startPos == nil || startPos.Timestamp < ddb.startPos.Timestamp {
		ddb.startPos = startPos
	}
	if ddb.endPos == nil || endPos.Timestamp > ddb.endPos.Timestamp {
		ddb.endPos = endPos
	}
}

// newBufferData needs an input dimension to calculate the limit of this buffer
//
// `limit` is the segment numOfRows a buffer can buffer at most.
//
// For a float32 vector field:
//
//	limit = 16 * 2^20 Byte [By default] / (dimension * 4 Byte)
//
// For a binary vector field:
//
//	limit = 16 * 2^20 Byte [By default]/ (dimension / 8 Byte)
//
// But since the buffer of binary vector fields is larger than the float32 one
//
//	with the same dimension, newBufferData takes the smaller buffer limit
//	to fit in both types of vector fields
//
// * This need to change for string field support and multi-vector fields support.
func newBufferData(collSchema *schemapb.CollectionSchema) (*BufferData, error) {
	// Get Dimension
	size, err := typeutil.EstimateSizePerRecord(collSchema)
	if err != nil {
		log.Warn("failed to estimate size per record", zap.Error(err))
		return nil, err
	}

	if size == 0 {
		return nil, errors.New("Invalid schema")
	}

	limit := Params.DataNodeCfg.FlushInsertBufferSize.GetAsInt64() / int64(size)
	if Params.DataNodeCfg.FlushInsertBufferSize.GetAsInt64()%int64(size) != 0 {
		limit++
	}

	//TODO::xige-16 eval vec and string field
	return &BufferData{
		buffer: &InsertData{Data: make(map[UniqueID]storage.FieldData)},
		size:   0,
		limit:  limit,
		tsFrom: math.MaxUint64,
		tsTo:   0}, nil
}

func newDelDataBuf() *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{},
		Binlog: datapb.Binlog{
			EntriesNum:    0,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		item: &Item{
			memorySize: 0,
		},
	}
}
