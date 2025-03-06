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

package metacache

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type SegmentInfo struct {
	segmentID        int64
	partitionID      int64
	state            commonpb.SegmentState
	startPosition    *msgpb.MsgPosition
	checkpoint       *msgpb.MsgPosition
	startPosRecorded bool
	flushedRows      int64
	bufferRows       int64
	syncingRows      int64
	bfs              pkoracle.PkStat
	bm25stats        *SegmentBM25Stats
	level            datapb.SegmentLevel
	syncingTasks     int32
	storageVersion   int64
}

func (s *SegmentInfo) SegmentID() int64 {
	return s.segmentID
}

func (s *SegmentInfo) PartitionID() int64 {
	return s.partitionID
}

func (s *SegmentInfo) State() commonpb.SegmentState {
	return s.state
}

// NumOfRows returns sum of number of rows,
// including flushed, syncing and buffered
func (s *SegmentInfo) NumOfRows() int64 {
	return s.flushedRows + s.syncingRows + s.bufferRows
}

// FlushedRows return flushed rows number.
func (s *SegmentInfo) FlushedRows() int64 {
	return s.flushedRows
}

func (s *SegmentInfo) StartPosition() *msgpb.MsgPosition {
	return s.startPosition
}

func (s *SegmentInfo) Checkpoint() *msgpb.MsgPosition {
	return s.checkpoint
}

func (s *SegmentInfo) GetHistory() []*storage.PkStatistics {
	return s.bfs.GetHistory()
}

func (s *SegmentInfo) GetBloomFilterSet() pkoracle.PkStat {
	return s.bfs
}

func (s *SegmentInfo) GetBM25Stats() *SegmentBM25Stats {
	return s.bm25stats
}

func (s *SegmentInfo) Level() datapb.SegmentLevel {
	return s.level
}

func (s *SegmentInfo) BufferRows() int64 {
	return s.bufferRows
}

func (s *SegmentInfo) SyncingRows() int64 {
	return s.syncingRows
}

func (s *SegmentInfo) GetStorageVersion() int64 {
	return s.storageVersion
}

func (s *SegmentInfo) Clone() *SegmentInfo {
	return &SegmentInfo{
		segmentID:        s.segmentID,
		partitionID:      s.partitionID,
		state:            s.state,
		startPosition:    s.startPosition,
		checkpoint:       s.checkpoint,
		startPosRecorded: s.startPosRecorded,
		flushedRows:      s.flushedRows,
		bufferRows:       s.bufferRows,
		syncingRows:      s.syncingRows,
		bfs:              s.bfs,
		level:            s.level,
		syncingTasks:     s.syncingTasks,
		bm25stats:        s.bm25stats,
	}
}

func NewSegmentInfo(info *datapb.SegmentInfo, bfs pkoracle.PkStat, bm25Stats *SegmentBM25Stats) *SegmentInfo {
	level := info.GetLevel()
	if level == datapb.SegmentLevel_Legacy {
		level = datapb.SegmentLevel_L1
	}
	return &SegmentInfo{
		segmentID:        info.GetID(),
		partitionID:      info.GetPartitionID(),
		state:            info.GetState(),
		flushedRows:      info.GetNumOfRows(),
		startPosition:    info.GetStartPosition(),
		checkpoint:       info.GetDmlPosition(),
		startPosRecorded: true,
		level:            level,
		bfs:              bfs,
		bm25stats:        bm25Stats,
		storageVersion:   info.GetStorageVersion(),
	}
}
