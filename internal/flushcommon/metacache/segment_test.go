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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type SegmentSuite struct {
	suite.Suite

	info *datapb.SegmentInfo
}

func (s *SegmentSuite) TestBasic() {
	bfs := pkoracle.NewBloomFilterSet()
	stats := NewEmptySegmentBM25Stats()
	segment := NewSegmentInfo(s.info, bfs, stats, NewEmptySegmentStats())
	s.Equal(s.info.GetID(), segment.SegmentID())
	s.Equal(s.info.GetPartitionID(), segment.PartitionID())
	s.Equal(s.info.GetNumOfRows(), segment.NumOfRows())
	s.Equal(s.info.GetStartPosition(), segment.StartPosition())
	s.Equal(s.info.GetDmlPosition(), segment.Checkpoint())
	s.Equal(bfs.GetHistory(), segment.GetHistory())
	s.True(segment.startPosRecorded)
}

func (s *SegmentSuite) TestClone() {
	bfs := pkoracle.NewBloomFilterSet()
	stats := NewEmptySegmentBM25Stats()
	segment := NewSegmentInfo(s.info, bfs, stats, NewEmptySegmentStats())
	cloned := segment.Clone()
	s.Equal(segment.SegmentID(), cloned.SegmentID())
	s.Equal(segment.PartitionID(), cloned.PartitionID())
	s.Equal(segment.NumOfRows(), cloned.NumOfRows())
	s.Equal(segment.StartPosition(), cloned.StartPosition())
	s.Equal(segment.Checkpoint(), cloned.Checkpoint())
	s.Equal(segment.GetHistory(), cloned.GetHistory())
	s.Equal(segment.startPosRecorded, cloned.startPosRecorded)
	s.Equal(segment.Binlogs(), cloned.Binlogs())
	s.Equal(segment.Statslogs(), cloned.Statslogs())
	s.Equal(segment.Deltalogs(), cloned.Deltalogs())
	s.Equal(segment.Bm25logs(), cloned.Bm25logs())
	s.Equal(segment.GetBM25Stats(), cloned.GetBM25Stats())
}

func (s *SegmentSuite) TestRecoverCurrentSplitFormat() {
	info := &datapb.SegmentInfo{
		ID:             10,
		StorageVersion: storage.StorageV3,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID:     100,
				ChildFields: []int64{100, 101},
				Format:      "parquet",
			},
			{
				FieldID:     102,
				ChildFields: []int64{102},
				Format:      "vortex",
			},
		},
	}

	segment := NewSegmentInfo(info, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())

	s.Equal("parquet", segment.GetCurrentSplit()[0].Format)
	s.Equal("vortex", segment.GetCurrentSplit()[1].Format)
}

// The growing-source flush resumes from a POSITION, so recovery has to restore
// one. A row count cannot serve: after a restart the growing segment is rebuilt
// by a WAL replay and its offsets start over at zero, sharing no origin with any
// count persisted before the restart.
//
// The position DataCoord hands back is the one the last successful flush
// reported through SaveBinlogPaths CheckPoints[].Position, which DataCoord
// stores verbatim as the segment's DML position.
func (s *SegmentSuite) TestRecoverLastFlushPositionFromDmlPosition() {
	flushedThrough := &msgpb.MsgPosition{
		ChannelName: "by-dev-rootcoord-dml_0_1v0",
		MsgID:       []byte{1, 2, 3, 4},
		Timestamp:   4242,
	}
	info := &datapb.SegmentInfo{
		ID:          11,
		NumOfRows:   100,
		DmlPosition: flushedThrough,
	}

	segment := NewSegmentInfo(info, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())

	s.Require().NotNil(segment.LastFlushPosition())
	s.EqualValues(4242, segment.LastFlushPosition().GetTimestamp())
	// The MsgID has to survive too: it is the only thing the WAL can seek by,
	// and it exists nowhere else once the process restarts.
	s.Equal([]byte{1, 2, 3, 4}, segment.LastFlushPosition().GetMsgID())

	// A segment that has never been flushed reports no fence, which resolves to
	// "flush from the beginning" rather than to some row offset.
	fresh := NewSegmentInfo(&datapb.SegmentInfo{ID: 12}, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())
	s.Nil(fresh.LastFlushPosition())
	s.Zero(fresh.LastFlushPosition().GetTimestamp())
}

// The fence only ever moves forward, and only in the transaction that publishes
// the data it names. An out-of-order or stale commit must not walk it back —
// doing so would re-flush rows that are already persisted.
func (s *SegmentSuite) TestSetLastFlushPositionOnlyAdvances() {
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 13}, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())

	SetLastFlushPosition(&msgpb.MsgPosition{Timestamp: 200})(segment)
	s.EqualValues(200, segment.LastFlushPosition().GetTimestamp())

	SetLastFlushPosition(&msgpb.MsgPosition{Timestamp: 100})(segment)
	s.EqualValues(200, segment.LastFlushPosition().GetTimestamp())

	SetLastFlushPosition(&msgpb.MsgPosition{Timestamp: 300})(segment)
	s.EqualValues(300, segment.LastFlushPosition().GetTimestamp())

	SetLastFlushPosition(nil)(segment)
	s.EqualValues(300, segment.LastFlushPosition().GetTimestamp())
}

// The pending-flush fence is set-if-nil, the mirror image of the last-flush
// fence: it names the EARLIEST un-committed seal, so a second seal must never
// push it forward. Recovery has to replay from the fence it has not yet
// committed, not from the newest one it has seen.
func (s *SegmentSuite) TestSetPendingFlushCheckpointIfNil() {
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 14}, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())
	s.Nil(segment.PendingFlushCheckpoint())

	SetPendingFlushCheckpointIfNil(nil)(segment)
	s.Nil(segment.PendingFlushCheckpoint(), "a nil position records nothing")

	SetPendingFlushCheckpointIfNil(&msgpb.MsgPosition{MsgID: []byte{9, 9}, Timestamp: 200})(segment)
	s.Require().NotNil(segment.PendingFlushCheckpoint())
	s.EqualValues(200, segment.PendingFlushCheckpoint().GetTimestamp())
	// The MsgID matters as much as the timestamp: it is what the WAL seeks by.
	s.Equal([]byte{9, 9}, segment.PendingFlushCheckpoint().GetMsgID())

	SetPendingFlushCheckpointIfNil(&msgpb.MsgPosition{Timestamp: 700})(segment)
	s.EqualValues(200, segment.PendingFlushCheckpoint().GetTimestamp(), "a re-seal must not move the fence later")

	SetPendingFlushCheckpointIfNil(&msgpb.MsgPosition{Timestamp: 50})(segment)
	s.EqualValues(200, segment.PendingFlushCheckpoint().GetTimestamp(), "and it must not move it earlier either")
}

// metacache is copy-on-write: every UpdateSegments replaces the SegmentInfo with
// a clone, so a field the clone drops is a checkpoint pin silently released by
// the next unrelated segment update.
func (s *SegmentSuite) TestPendingFlushCheckpointSurvivesClone() {
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 15}, pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats())
	SetPendingFlushCheckpointIfNil(&msgpb.MsgPosition{Timestamp: 321})(segment)

	cloned := segment.Clone()
	s.Require().NotNil(cloned.PendingFlushCheckpoint())
	s.EqualValues(321, cloned.PendingFlushCheckpoint().GetTimestamp())

	// A fresh segment is not pinned by anything it was never sealed with.
	s.Nil(NewSegmentInfo(&datapb.SegmentInfo{ID: 16, DmlPosition: &msgpb.MsgPosition{Timestamp: 999}},
		pkoracle.NewBloomFilterSet(), nil, NewEmptySegmentStats()).Clone().PendingFlushCheckpoint())
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}

func TestSegmentInfo_StatsCarriedByRefThroughClone(t *testing.T) {
	insertBinlog := func(memSize int64, tsTo uint64) map[int64]*datapb.FieldBinlog {
		return map[int64]*datapb.FieldBinlog{
			1: {FieldID: 1, Binlogs: []*datapb.Binlog{{MemorySize: memSize, EntriesNum: 10, TimestampTo: tsTo}}},
		}
	}
	si := NewSegmentInfo(&datapb.SegmentInfo{ID: 1}, nil, nil, NewEmptySegmentStats())
	si.Statistics().Digest(insertBinlog(100, 5), nil, 0, 10, 1, 5)
	cloned := si.Clone()
	// shared by pointer: digesting through the clone is visible on the original
	cloned.Statistics().Digest(insertBinlog(100, 9), nil, 0, 10, 6, 9)
	// SegmentInfo.Clone shares the *SegmentStats by pointer, so the original
	// and the clone are the same accumulator.
	assert.Same(t, si.Statistics(), cloned.Statistics())
	// And the original sees the clone's digest: cumulative 100+100 = 200.
	assert.Equal(t, int64(200), si.Statistics().Publish().GetInsertBinlogSize())
}
