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
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Segment contains the latest segment infos from channel.
type Segment struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	sType        atomic.Value // datapb.SegmentType

	numRows     int64
	memorySize  int64
	compactedTo UniqueID

	curInsertBuf     *BufferData
	curDeleteBuf     *DelDataBuf
	historyInsertBuf []*BufferData
	historyDeleteBuf []*DelDataBuf

	statLock     sync.RWMutex
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics

	lastSyncTs  Timestamp
	startPos    *msgpb.MsgPosition // TODO readonly
	lazyLoading atomic.Value
	syncing     atomic.Value
	released    atomic.Value
}

func (s *Segment) isSyncing() bool {
	if s != nil {
		b, ok := s.syncing.Load().(bool)
		if ok {
			return b
		}
	}
	return false
}

func (s *Segment) setSyncing(syncing bool) {
	if s != nil {
		s.syncing.Store(syncing)
	}
}

func (s *Segment) isLoadingLazy() bool {
	b, ok := s.lazyLoading.Load().(bool)
	if !ok {
		return false
	}
	return b
}

func (s *Segment) setLoadingLazy(b bool) {
	s.lazyLoading.Store(b)
}

func (s *Segment) isReleased() bool {
	b, ok := s.released.Load().(bool)
	if !ok {
		return false
	}
	return b
}

func (s *Segment) setReleased(b bool) {
	s.released.Store(b)
}

func (s *Segment) isValid() bool {
	return s.getType() != datapb.SegmentType_Compacted
}

func (s *Segment) notFlushed() bool {
	return s.isValid() && s.getType() != datapb.SegmentType_Flushed
}

func (s *Segment) getType() datapb.SegmentType {
	return s.sType.Load().(datapb.SegmentType)
}

func (s *Segment) setType(t datapb.SegmentType) {
	s.sType.Store(t)
}

func (s *Segment) updatePKRange(ids storage.FieldData) {
	s.statLock.Lock()
	defer s.statLock.Unlock()
	s.InitCurrentStat()
	err := s.currentStat.UpdatePKRange(ids)
	if err != nil {
		panic(err)
	}
}

func (s *Segment) getHistoricalStats(pkField *schemapb.FieldSchema) ([]*storage.PrimaryKeyStats, int64) {
	statsList := []*storage.PrimaryKeyStats{}
	for _, stats := range s.historyStats {
		statsList = append(statsList, &storage.PrimaryKeyStats{
			FieldID: pkField.FieldID,
			PkType:  int64(pkField.DataType),
			BF:      stats.PkFilter,
			MaxPk:   stats.MaxPK,
			MinPk:   stats.MinPK,
		})
	}

	if s.currentStat != nil {
		statsList = append(statsList, &storage.PrimaryKeyStats{
			FieldID: pkField.FieldID,
			PkType:  int64(pkField.DataType),
			BF:      s.currentStat.PkFilter,
			MaxPk:   s.currentStat.MaxPK,
			MinPk:   s.currentStat.MinPK,
		})
	}
	return statsList, s.numRows
}

func (s *Segment) InitCurrentStat() {
	if s.currentStat == nil {
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive),
		}
	}
}

// check if PK exists is current
func (s *Segment) isPKExist(pk primaryKey) bool {
	// for integrity, report false positive while lazy loading
	if s.isLoadingLazy() {
		return true
	}
	s.statLock.Lock()
	defer s.statLock.Unlock()
	if s.currentStat != nil && s.currentStat.PkExist(pk) {
		return true
	}

	for _, historyStats := range s.historyStats {
		if historyStats.PkExist(pk) {
			return true
		}
	}
	return false
}

// setInsertBuffer set curInsertBuf.
func (s *Segment) setInsertBuffer(buf *BufferData) {
	s.curInsertBuf = buf

	if buf != nil && buf.buffer != nil {
		dataSize := 0
		for _, data := range buf.buffer.Data {
			dataSize += data.GetMemorySize()
		}
		metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			strconv.FormatInt(s.collectionID, 10)).Add(float64(dataSize))
	}
}

// rollInsertBuffer moves curInsertBuf to historyInsertBuf, and then sets curInsertBuf to nil.
func (s *Segment) rollInsertBuffer() {
	if s.curInsertBuf == nil {
		return
	}

	if s.curInsertBuf.buffer != nil {
		dataSize := 0
		for _, data := range s.curInsertBuf.buffer.Data {
			dataSize += data.GetMemorySize()
		}
		metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			strconv.FormatInt(s.collectionID, 10)).Sub(float64(dataSize))
	}

	s.curInsertBuf.buffer = nil // free buffer memory, only keep meta infos in historyInsertBuf
	s.historyInsertBuf = append(s.historyInsertBuf, s.curInsertBuf)
	s.curInsertBuf = nil
}

// evictHistoryInsertBuffer removes flushed buffer from historyInsertBuf after saveBinlogPath.
func (s *Segment) evictHistoryInsertBuffer(endPos *msgpb.MsgPosition) {
	tmpBuffers := make([]*BufferData, 0)
	for _, buf := range s.historyInsertBuf {
		if buf.endPos.Timestamp > endPos.Timestamp {
			tmpBuffers = append(tmpBuffers, buf)
		}
	}
	s.historyInsertBuf = tmpBuffers
	ts, _ := tsoutil.ParseTS(endPos.Timestamp)
	log.Info("evictHistoryInsertBuffer done", zap.Int64("segmentID", s.segmentID), zap.Time("ts", ts), zap.String("channel", endPos.ChannelName))
}

// rollDeleteBuffer moves curDeleteBuf to historyDeleteBuf, and then sets curDeleteBuf to nil.
func (s *Segment) rollDeleteBuffer() {
	if s.curDeleteBuf == nil {
		return
	}
	s.curDeleteBuf.delData = nil // free buffer memory, only keep meta infos in historyDeleteBuf
	s.historyDeleteBuf = append(s.historyDeleteBuf, s.curDeleteBuf)
	s.curDeleteBuf = nil
}

// evictHistoryDeleteBuffer removes flushed buffer from historyDeleteBuf after saveBinlogPath.
func (s *Segment) evictHistoryDeleteBuffer(endPos *msgpb.MsgPosition) {
	tmpBuffers := make([]*DelDataBuf, 0)
	for _, buf := range s.historyDeleteBuf {
		if buf.endPos.Timestamp > endPos.Timestamp {
			tmpBuffers = append(tmpBuffers, buf)
		}
	}
	s.historyDeleteBuf = tmpBuffers
	ts, _ := tsoutil.ParseTS(endPos.Timestamp)
	log.Info("evictHistoryDeleteBuffer done", zap.Int64("segmentID", s.segmentID), zap.Time("ts", ts), zap.String("channel", endPos.ChannelName))
}

func (s *Segment) isBufferEmpty() bool {
	return s.curInsertBuf == nil &&
		s.curDeleteBuf == nil &&
		len(s.historyInsertBuf) == 0 &&
		len(s.historyDeleteBuf) == 0
}
