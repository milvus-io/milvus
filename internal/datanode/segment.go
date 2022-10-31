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
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
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

	statLock     sync.Mutex
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics

	startPos *internalpb.MsgPosition // TODO readonly
	endPos   *internalpb.MsgPosition
}

type addSegmentReq struct {
	segType                    datapb.SegmentType
	segID, collID, partitionID UniqueID
	numOfRows                  int64
	startPos, endPos           *internalpb.MsgPosition
	statsBinLogs               []*datapb.FieldBinlog
	recoverTs                  Timestamp
	importing                  bool
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

func (s *Segment) InitCurrentStat() {
	if s.currentStat == nil {
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive),
		}
	}
}

// check if PK exists is current
func (s *Segment) isPKExist(pk primaryKey) bool {
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
