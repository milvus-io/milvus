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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type SegmentInfo struct {
	segmentID        int64
	partitionID      int64
	state            commonpb.SegmentState
	startPosition    *msgpb.MsgPosition
	checkpoint       *msgpb.MsgPosition
	startPosRecorded bool
	flushedRows      int64
	// lastFlushPosition is the WAL position this segment was last flushed
	// through. It is the lower fence of the next growing-source flush, and it
	// has to be a POSITION rather than a row count: after a restart the growing
	// segment is rebuilt by a WAL replay and its row offsets start over, so a
	// count kept here would have no common origin with it. Restored from the
	// segment's persisted DML position on recovery.
	lastFlushPosition *msgpb.MsgPosition
	// pendingFlushCheckpoint is the WAL position a replay has to resume from in
	// order to regenerate this segment's OUTSTANDING flush obligation, i.e. the
	// seal message (ManualFlush/Flush fence) that moved it out of Growing. It is
	// set when the segment is sealed and lives exactly as long as the obligation:
	// the segment leaves the metacache when the flush (or drop) commits, so there
	// is nothing to unregister and nothing to leak.
	//
	// It is deliberately set-if-nil: a second seal of an already sealed segment
	// must never push the pin forward, because the FIRST fence is the one whose
	// redelivery recovery depends on. Not persisted - after a restart the fence
	// is either already committed or redelivered by the replay.
	pendingFlushCheckpoint *msgpb.MsgPosition
	bufferRows             int64
	syncingRows            int64
	bfs                    pkoracle.PkStat
	bm25stats              *SegmentBM25Stats
	stats                  *SegmentStats
	level                  datapb.SegmentLevel
	syncingTasks           int32
	storageVersion         int64
	binlogs                []*datapb.FieldBinlog
	statslogs              []*datapb.FieldBinlog
	deltalogs              []*datapb.FieldBinlog
	bm25logs               []*datapb.FieldBinlog
	currentSplit           []storagecommon.ColumnGroup
	manifestPath           string

	// flushSourceMode is process-local runtime state; not persisted.
	// See FlushSourceMode docs for lifecycle semantics.
	flushSourceMode FlushSourceMode
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

// LastFlushPosition returns the WAL position this segment was last flushed
// through, or nil if it has never been flushed.
func (s *SegmentInfo) LastFlushPosition() *msgpb.MsgPosition {
	return s.lastFlushPosition
}

// PendingFlushCheckpoint returns the WAL position whose replay regenerates this
// segment's outstanding flush obligation, or nil if the segment does not owe a
// flush that was pinned at seal time.
func (s *SegmentInfo) PendingFlushCheckpoint() *msgpb.MsgPosition {
	return s.pendingFlushCheckpoint
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

func (s *SegmentInfo) Statistics() *SegmentStats {
	return s.stats
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

func (s *SegmentInfo) GetCurrentSplit() []storagecommon.ColumnGroup {
	return s.currentSplit
}

func (s *SegmentInfo) Binlogs() []*datapb.FieldBinlog {
	return s.binlogs
}

func (s *SegmentInfo) Statslogs() []*datapb.FieldBinlog {
	return s.statslogs
}

func (s *SegmentInfo) Deltalogs() []*datapb.FieldBinlog {
	return s.deltalogs
}

func (s *SegmentInfo) Bm25logs() []*datapb.FieldBinlog {
	return s.bm25logs
}

func (s *SegmentInfo) ManifestPath() string {
	return s.manifestPath
}

// FlushSourceMode returns the sticky decision of which subsystem owns this
// segment's payload at flush time. The value is process-local and not
// persisted; see FlushSourceMode docs for details.
func (s *SegmentInfo) FlushSourceMode() FlushSourceMode {
	return s.flushSourceMode
}

func (s *SegmentInfo) Clone() *SegmentInfo {
	return &SegmentInfo{
		segmentID:         s.segmentID,
		partitionID:       s.partitionID,
		state:             s.state,
		startPosition:     s.startPosition,
		checkpoint:        s.checkpoint,
		startPosRecorded:  s.startPosRecorded,
		flushedRows:       s.flushedRows,
		lastFlushPosition: s.lastFlushPosition,
		// Carried by the clone: metacache is copy-on-write, so dropping this here
		// would silently release the checkpoint pin on the next segment update.
		pendingFlushCheckpoint: s.pendingFlushCheckpoint,
		bufferRows:             s.bufferRows,
		syncingRows:            s.syncingRows,
		bfs:                    s.bfs,
		level:                  s.level,
		syncingTasks:           s.syncingTasks,
		bm25stats:              s.bm25stats,
		stats:                  s.stats,
		storageVersion:         s.storageVersion,
		binlogs:                s.binlogs,
		statslogs:              s.statslogs,
		deltalogs:              s.deltalogs,
		bm25logs:               s.bm25logs,
		currentSplit:           s.currentSplit,
		manifestPath:           s.manifestPath,
		flushSourceMode:        s.flushSourceMode,
	}
}

func NewSegmentInfo(info *datapb.SegmentInfo, bfs pkoracle.PkStat, bm25Stats *SegmentBM25Stats, stats *SegmentStats) *SegmentInfo {
	if stats == nil {
		stats = NewEmptySegmentStats()
	}
	level := info.GetLevel()
	if level == datapb.SegmentLevel_Legacy {
		level = datapb.SegmentLevel_L1
	}
	// legacy split also share same field here
	// shall be checked by caller
	var currentSplit []storagecommon.ColumnGroup
	if info.GetStorageVersion() >= storage.StorageV2 && len(info.Binlogs) > 0 {
		currentSplit = make([]storagecommon.ColumnGroup, 0, len(info.Binlogs))
		for _, group := range info.Binlogs {
			currentSplit = append(currentSplit, storagecommon.ColumnGroup{
				GroupID: group.GetFieldID(),
				Fields:  group.GetChildFields(),
				Format:  group.GetFormat(),
			})
		}
		mlog.Info(context.TODO(), "recover split info", mlog.FieldSegmentID(info.GetID()), mlog.Stringers("columnGroup", currentSplit))
	}
	return &SegmentInfo{
		segmentID:     info.GetID(),
		partitionID:   info.GetPartitionID(),
		state:         info.GetState(),
		flushedRows:   info.GetNumOfRows(),
		startPosition: info.GetStartPosition(),
		checkpoint:    info.GetDmlPosition(),
		// The DML position IS what the last successful flush persisted for this
		// segment (SaveBinlogPaths CheckPoints[].Position), so it is the fence
		// the next growing-source flush must resume from.
		lastFlushPosition: info.GetDmlPosition(),
		startPosRecorded:  true,
		level:             level,
		bfs:               bfs,
		bm25stats:         bm25Stats,
		stats:             stats,
		storageVersion:    info.GetStorageVersion(),
		binlogs:           info.GetBinlogs(),
		statslogs:         info.GetStatslogs(),
		deltalogs:         info.GetDeltalogs(),
		bm25logs:          info.GetBm25Statslogs(),
		currentSplit:      currentSplit,
		manifestPath:      info.GetManifestPath(),
	}
}
