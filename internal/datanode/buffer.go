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
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DeltaBufferManager is in charge of managing insertBuf and delBuf from an overall prospect
// not only controlling buffered data size based on every segment size, but also triggering
// insert/delete flush when the memory usage of the whole manager reach a certain level.
// but at the first stage, this struct is only used for delete buff
//
// DeltaBufferManager manages channel, usedMemory and delBufHeap.
type DeltaBufferManager struct {
	channel    Channel
	usedMemory atomic.Int64

	heapGuard  sync.Mutex // guards delBufHeap
	delBufHeap *PriorityQueue
}

func (m *DeltaBufferManager) GetEntriesNum(segID UniqueID) int64 {
	if buffer, ok := m.Load(segID); ok {
		return buffer.GetEntriesNum()
	}

	return 0
}

func (m *DeltaBufferManager) UpdateCompactedSegments() {
	compactedTo2From := m.channel.listCompactedSegmentIDs()
	for compactedTo, compactedFrom := range compactedTo2From {

		// if the compactedTo segment has 0 numRows, there'll be no segments
		// in the channel meta, so remove all compacted from segments related
		if !m.channel.hasSegment(compactedTo, true) {
			for _, segID := range compactedFrom {
				m.Delete(segID)
			}
			m.channel.removeSegments(compactedFrom...)
			continue
		}

		compactToDelBuff, loaded := m.Load(compactedTo)
		if !loaded {
			compactToDelBuff = newDelDataBuf(compactedTo)
		}

		for _, segID := range compactedFrom {
			if delDataBuf, loaded := m.Load(segID); loaded {
				compactToDelBuff.MergeDelDataBuf(delDataBuf)
				m.Delete(segID)
			}
		}

		// only store delBuf if EntriesNum > 0
		if compactToDelBuff.EntriesNum > 0 {

			m.pushOrFixHeap(compactedTo, compactToDelBuff)
			// We need to re-add the memorySize because m.Delete(segID) sub them all.
			m.usedMemory.Add(compactToDelBuff.GetMemorySize())
			m.updateMeta(compactedTo, compactToDelBuff)
		}

		log.Info("update delBuf for compacted segments",
			zap.Int64("compactedTo segmentID", compactedTo),
			zap.Int64s("compactedFrom segmentIDs", compactedFrom),
			zap.Int64("usedMemory", m.usedMemory.Load()),
		)
		m.channel.removeSegments(compactedFrom...)
	}
}

func (m *DeltaBufferManager) updateMeta(segID UniqueID, delDataBuf *DelDataBuf) {
	m.channel.setCurDeleteBuffer(segID, delDataBuf)
}

// pushOrFixHeap updates and sync memory size with priority queue
func (m *DeltaBufferManager) pushOrFixHeap(segID UniqueID, buffer *DelDataBuf) {
	m.heapGuard.Lock()
	defer m.heapGuard.Unlock()
	if _, loaded := m.Load(segID); loaded {
		heap.Fix(m.delBufHeap, buffer.item.index)
	} else {
		heap.Push(m.delBufHeap, buffer.item)
	}
}

// deleteFromHeap deletes an item from the heap
func (m *DeltaBufferManager) deleteFromHeap(buffer *DelDataBuf) {
	m.heapGuard.Lock()
	defer m.heapGuard.Unlock()

	if itemIdx, ok := buffer.GetItemIndex(); ok {
		heap.Remove(m.delBufHeap, itemIdx)
	}
}

func (m *DeltaBufferManager) StoreNewDeletes(segID UniqueID, pks []primaryKey,
	tss []Timestamp, tr TimeRange, startPos, endPos *msgpb.MsgPosition) {
	buffer, loaded := m.Load(segID)
	if !loaded {
		buffer = newDelDataBuf(segID)
	}

	size := buffer.Buffer(pks, tss, tr, startPos, endPos)

	m.pushOrFixHeap(segID, buffer)
	m.updateMeta(segID, buffer)
	m.usedMemory.Add(size)

	metrics.DataNodeConsumeMsgRowsCount.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Add(float64(len(pks)))
}

func (m *DeltaBufferManager) Load(segID UniqueID) (delDataBuf *DelDataBuf, ok bool) {
	return m.channel.getCurDeleteBuffer(segID)
}

func (m *DeltaBufferManager) Delete(segID UniqueID) {
	if buffer, loaded := m.Load(segID); loaded {
		m.usedMemory.Sub(buffer.GetMemorySize())
		m.deleteFromHeap(buffer)
		m.channel.rollDeleteBuffer(segID)

	}
}

func (m *DeltaBufferManager) popHeapItem() *Item {
	m.heapGuard.Lock()
	defer m.heapGuard.Unlock()
	return heap.Pop(m.delBufHeap).(*Item)
}

func (m *DeltaBufferManager) ShouldFlushSegments() []UniqueID {
	var memUsage = m.usedMemory.Load()
	if memUsage < Params.DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64() {
		return nil
	}

	var (
		poppedSegmentIDs []UniqueID
		poppedItems      []*Item
	)
	for {
		segItem := m.popHeapItem()
		poppedItems = append(poppedItems, segItem)
		poppedSegmentIDs = append(poppedSegmentIDs, segItem.segmentID)
		memUsage -= segItem.memorySize
		if memUsage < Params.DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64() {
			break

		}
	}

	//here we push all selected segment back into the heap
	//in order to keep the heap semantically correct
	m.heapGuard.Lock()
	for _, segMem := range poppedItems {
		heap.Push(m.delBufHeap, segMem)
	}
	m.heapGuard.Unlock()

	log.Info("Add segments to sync delete buffer for stressfull memory", zap.Any("segments", poppedItems))
	return poppedSegmentIDs
}

// An Item is something we manage in a memorySize priority queue.
type Item struct {
	segmentID  UniqueID // The segmentID
	memorySize int64    // The size of memory consumed by del buf
	index      int      // The index of the item in the heap.
	// The index is needed by update and is maintained by the heap.Interface methods.
}

// String format Item as <segmentID=0, memorySize=1>
func (i *Item) String() string {
	return fmt.Sprintf("<segmentID=%d, memorySize=%d>", i.segmentID, i.memorySize)
}

// A PriorityQueue implements heap.Interface and holds Items.
// We use PriorityQueue to manage memory consumed by del buf
type PriorityQueue struct {
	items []*Item
}

// String format PriorityQueue as [item, item]
func (pq *PriorityQueue) String() string {
	var items []string
	for _, item := range pq.items {
		items = append(items, item.String())
	}
	return fmt.Sprintf("[%s]", strings.Join(items, ","))
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

// Buffer returns the memory size buffered
func (ddb *DelDataBuf) Buffer(pks []primaryKey, tss []Timestamp, tr TimeRange, startPos, endPos *msgpb.MsgPosition) int64 {
	var (
		rowCount = len(pks)
		bufSize  int64
	)
	for i := 0; i < rowCount; i++ {
		ddb.delData.Append(pks[i], tss[i])

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

	ddb.accumulateEntriesNum(int64(rowCount))
	ddb.updateTimeRange(tr)
	ddb.updateStartAndEndPosition(startPos, endPos)
	// update memorysize
	ddb.item.memorySize += bufSize

	return bufSize
}

func (ddb *DelDataBuf) GetMemorySize() int64 {
	if ddb.item != nil {
		return ddb.item.memorySize
	}
	return 0
}

func (ddb *DelDataBuf) GetItemIndex() (int, bool) {
	if ddb.item != nil {
		return ddb.item.index, true
	}
	return 0, false
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

func (ddb *DelDataBuf) MergeDelDataBuf(buf *DelDataBuf) {
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

func newDelDataBuf(segmentID UniqueID) *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{},
		Binlog: datapb.Binlog{
			EntriesNum:    0,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
		item: &Item{
			segmentID: segmentID,
		},
	}
}
