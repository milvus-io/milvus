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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	boundaryTestChannelA = "by-dev-rootcoord-dml_0_100v0"
	boundaryTestChannelB = "by-dev-rootcoord-dml_1_100v1"
	boundaryTestSeekTs   = uint64(100)
)

func boundaryTestBoundary() *SnapshotBoundary {
	return &SnapshotBoundary{
		SeekPositions: []*msgpb.MsgPosition{
			{ChannelName: boundaryTestChannelA, Timestamp: boundaryTestSeekTs},
		},
		SnapshotTs: boundaryTestSeekTs,
	}
}

// boundaryTestSegment is inside the boundary and not yet visible -- the state a
// stream-flushed segment has while it still owes a sort -- so each test
// overrides only the one field it is about.
func boundaryTestSegment(id int64, opts ...func(*datapb.SegmentInfo)) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  100,
		PartitionID:   101,
		InsertChannel: boundaryTestChannelA,
		State:         commonpb.SegmentState_Flushed,
		IsInvisible:   true,
		Binlogs:       []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		Level:         datapb.SegmentLevel_L1,
		StartPosition: &msgpb.MsgPosition{ChannelName: boundaryTestChannelA, Timestamp: 50},
	}
	for _, opt := range opts {
		opt(info)
	}
	return &SegmentInfo{SegmentInfo: info}
}

// boundaryTestManager seeds a channel checkpoint that has already passed the
// boundary, so tests about the visibility half are not silently answered by the
// completeness gate in front of it.
func boundaryTestManager(segments ...*SegmentInfo) *snapshotManager {
	return boundaryTestManagerAt(boundaryTestSeekTs+1, segments...)
}

func boundaryTestManagerAt(checkpointTs uint64, segments ...*SegmentInfo) *snapshotManager {
	m := &meta{
		ctx:         context.Background(),
		segments:    NewSegmentsInfo(),
		channelCPs:  newChannelCps(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	if checkpointTs > 0 {
		m.channelCPs.checkpoints[boundaryTestChannelA] = &msgpb.MsgPosition{
			ChannelName: boundaryTestChannelA,
			Timestamp:   checkpointTs,
		}
	}
	for _, segment := range segments {
		m.segments.SetSegment(segment.GetID(), segment)
	}
	return &snapshotManager{meta: m}
}

// ── boundary construction ────────────────────────────────────────────────────

func TestNewSnapshotBoundary(t *testing.T) {
	t.Run("drops the control channel", func(t *testing.T) {
		// The control-channel copy carries no data and its position is not
		// comparable with segments on the data vchannels, so including it would
		// invent a boundary for a channel no segment lives on.
		boundary, err := NewSnapshotBoundary(map[string]*message.AppendResult{
			"by-dev-rootcoord-dml_0vcchan": {MessageID: rmq.NewRmqID(1), TimeTick: 900},
			boundaryTestChannelA:           {MessageID: rmq.NewRmqID(2), TimeTick: 1000},
			boundaryTestChannelB:           {MessageID: rmq.NewRmqID(3), TimeTick: 1100},
		})
		assert.NoError(t, err)
		assert.Len(t, boundary.SeekPositions, 2)
		assert.Equal(t, boundaryTestChannelA, boundary.SeekPositions[0].GetChannelName())
		assert.Equal(t, boundaryTestChannelB, boundary.SeekPositions[1].GetChannelName())

		// Min over the data vchannels only. The control channel's earlier tick
		// must not drag the summary backwards.
		assert.Equal(t, uint64(1000), boundary.SnapshotTs)

		// The positions double as the restore point, so they carry a real MsgID:
		// a timestamp alone cannot be seeked in the WAL.
		assert.NotEmpty(t, boundary.SeekPositions[0].GetMsgID())
	})

	t.Run("writes a seek position a reader can actually decode", func(t *testing.T) {
		// The bytes are only worth persisting if they survive the round trip
		// the restore path will make. Marshal() does not: for rocksmq it is
		// ASCII decimal while the decoder reads a big-endian uint64, so a
		// position written that way decodes to a different message -- or
		// panics, when the decimal form is under 8 bytes.
		boundary, err := NewSnapshotBoundary(map[string]*message.AppendResult{
			boundaryTestChannelA: {MessageID: rmq.NewRmqID(7), TimeTick: 1000},
		})
		assert.NoError(t, err)

		position := boundary.SeekPositions[0]
		// WALName has to be stamped, or the decoder falls back to whatever WAL
		// is the current default -- which AlterWAL rewrites cluster-wide.
		assert.Equal(t, commonpb.WALName_RocksMQ, position.GetWALName())

		decoded := msgadaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(
			message.WALName(position.GetWALName()), position.GetMsgID())
		assert.Equal(t, rmq.NewRmqID(7), decoded)
	})

	t.Run("rejects a result with no data vchannel", func(t *testing.T) {
		_, err := NewSnapshotBoundary(map[string]*message.AppendResult{
			"by-dev-rootcoord-dml_0vcchan": {MessageID: rmq.NewRmqID(1), TimeTick: 900},
		})
		assert.Error(t, err)
	})

	t.Run("rejects a channel with no append result", func(t *testing.T) {
		_, err := NewSnapshotBoundary(map[string]*message.AppendResult{
			boundaryTestChannelA: nil,
		})
		assert.Error(t, err)
	})

	t.Run("SeekTs reports coverage", func(t *testing.T) {
		boundary := boundaryTestBoundary()
		seekTs, ok := boundary.SeekTs(boundaryTestChannelA)
		assert.True(t, ok)
		assert.Equal(t, boundaryTestSeekTs, seekTs)

		// Not "excluded" -- unplaceable. Callers must surface it.
		_, ok = boundary.SeekTs(boundaryTestChannelB)
		assert.False(t, ok)
	})
}

func TestChannelsBehindBoundary(t *testing.T) {
	boundary := boundaryTestBoundary()

	t.Run("no checkpoint yet", func(t *testing.T) {
		// A channel DataCoord has never heard a checkpoint for cannot be assumed
		// caught up; that is the state in which its segments are most likely
		// missing from meta entirely.
		sm := boundaryTestManagerAt(0)
		assert.Equal(t, []string{boundaryTestChannelA}, sm.channelsBehindBoundary(boundary))
	})

	t.Run("checkpoint behind the boundary", func(t *testing.T) {
		sm := boundaryTestManagerAt(boundaryTestSeekTs - 1)
		assert.Equal(t, []string{boundaryTestChannelA}, sm.channelsBehindBoundary(boundary))
	})

	t.Run("checkpoint at or past the boundary", func(t *testing.T) {
		assert.Empty(t, boundaryTestManagerAt(boundaryTestSeekTs).channelsBehindBoundary(boundary))
		assert.Empty(t, boundaryTestManagerAt(boundaryTestSeekTs+100).channelsBehindBoundary(boundary))
	})
}

// TestWaitForVisibleBoundaryWaitsForCompleteness is the reason the gate exists.
// Before a channel's checkpoint passes the boundary, DataCoord may not have been
// told about the segments the fence sealed -- they are visible on the streaming
// node long before they appear here. Scanning for invisible segments then finds
// nothing, which is indistinguishable from "everything is sorted", and the
// snapshot would capture a boundary whose own data had not arrived.
func TestWaitForVisibleBoundaryWaitsForCompleteness(t *testing.T) {
	// No segments at all: exactly the "meta has not caught up" shape.
	sm := boundaryTestManagerAt(boundaryTestSeekTs - 1)

	paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

	err := sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), true)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
	assert.Contains(t, err.Error(), "reach its boundary")
}

// ── the wait set ─────────────────────────────────────────────────────────────

func TestSegmentsAwaitingVisibility(t *testing.T) {
	tests := []struct {
		name    string
		segment *SegmentInfo
		want    bool
	}{
		{
			name:    "flushed but not yet visible",
			segment: boundaryTestSegment(1),
			want:    true,
		},
		{
			// A segment with a sort task already dispatched is not visible yet.
			// canTriggerSortCompaction excludes it so it is not dispatched
			// twice; the wait includes it for the opposite reason.
			name:    "already compacting",
			segment: func() *SegmentInfo { s := boundaryTestSegment(2); s.isCompacting = true; return s }(),
			want:    true,
		},
		{
			name: "sorted and published visible",
			segment: boundaryTestSegment(3, func(s *datapb.SegmentInfo) {
				s.IsSorted = true
				s.IsInvisible = false
			}),
			want: false,
		},
		{
			name: "sorted by namespace and published visible",
			segment: boundaryTestSegment(4, func(s *datapb.SegmentInfo) {
				s.IsSortedByNamespace = true
				s.IsInvisible = false
			}),
			want: false,
		},
		{
			// A clustering result, and the sort output that inherits its
			// invisibility, are both CreatedByCompaction. Their inputs are alive
			// and serving until the results are published, so the capture takes
			// those instead and must NOT block here -- waiting would mean waiting
			// on the clustering output's index build, which can never finish.
			// dropSupersededByLineage picks the generation at capture time.
			name: "invisible compaction output is not awaited",
			segment: boundaryTestSegment(12, func(s *datapb.SegmentInfo) {
				s.IsSorted = true
				s.CreatedByCompaction = true
				s.CompactionFrom = []int64{99}
			}),
			want: false,
		},
		{
			// Same shape but unsorted -- still a compaction output, still not
			// this wait's problem.
			name: "invisible unsorted compaction output is not awaited",
			segment: boundaryTestSegment(14, func(s *datapb.SegmentInfo) {
				s.CreatedByCompaction = true
				s.CompactionFrom = []int64{99}
			}),
			want: false,
		},
		{
			// The case unsortedness gets wrong in the dangerous direction. With
			// sort compaction off, flushFlushingSegment never stamps IsInvisible,
			// so this segment is indexed, sealed-loaded and backfill-eligible --
			// exactly what a reader sees. Nothing to wait for, and waiting would
			// never end because no sort is coming.
			name: "unsorted but visible",
			segment: boundaryTestSegment(13, func(s *datapb.SegmentInfo) {
				s.IsInvisible = false
			}),
			want: false,
		},
		{
			name:    "L0 is never invisible",
			segment: boundaryTestSegment(5, func(s *datapb.SegmentInfo) { s.Level = datapb.SegmentLevel_L0 }),
			want:    false,
		},
		{
			name:    "importing",
			segment: boundaryTestSegment(6, func(s *datapb.SegmentInfo) { s.IsImporting = true }),
			want:    false,
		},
		{
			// Not this predicate's business. A segment still on its way to Flushed
			// means DataCoord has not caught up with the boundary at all, which is
			// what channelsBehindBoundary is for -- and it is the gate, not this
			// scan, that keeps the snapshot from proceeding while such segments
			// exist. Answering here as well would only duplicate that, in a way
			// that cannot see the segments meta has not heard of yet.
			name:    "growing",
			segment: boundaryTestSegment(7, func(s *datapb.SegmentInfo) { s.State = commonpb.SegmentState_Growing }),
			want:    false,
		},
		{
			name:    "dropped",
			segment: boundaryTestSegment(10, func(s *datapb.SegmentInfo) { s.State = commonpb.SegmentState_Dropped }),
			want:    false,
		},
		{
			// Nothing to capture, so nothing to wait for -- otherwise a segment
			// that never receives data would hang the snapshot.
			name: "empty",
			segment: boundaryTestSegment(11, func(s *datapb.SegmentInfo) {
				s.Binlogs = nil
			}),
			want: false,
		},
		{
			// Started after the fence, so it is not in the snapshot and the
			// snapshot has no reason to wait for it. This is what makes the set
			// closed under continuous ingestion.
			name: "outside the boundary",
			segment: boundaryTestSegment(8, func(s *datapb.SegmentInfo) {
				s.StartPosition = &msgpb.MsgPosition{ChannelName: boundaryTestChannelA, Timestamp: 200}
			}),
			want: false,
		},
		{
			// CommitTimestamp wins over StartPosition, which places an import
			// segment at the point it became visible rather than at whatever
			// timestamps its rows happened to carry.
			name: "import committed after the boundary",
			segment: boundaryTestSegment(9, func(s *datapb.SegmentInfo) {
				s.CommitTimestamp = 200
			}),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := boundaryTestManager(tt.segment)
			awaiting, err := sm.segmentsAwaitingVisibility(context.Background(), 100, boundaryTestBoundary())
			assert.NoError(t, err)
			if tt.want {
				assert.Equal(t, []int64{tt.segment.GetID()}, awaiting)
			} else {
				assert.Empty(t, awaiting)
			}
		})
	}
}

func TestSegmentsAwaitingVisibility_UnknownChannel(t *testing.T) {
	// A boundary that does not cover a segment's channel is not "segment
	// excluded", it is a boundary we cannot evaluate. Reporting "nothing to wait
	// for" there would silently snapshot an unsorted segment.
	sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
		s.InsertChannel = boundaryTestChannelB
	}))

	_, err := sm.segmentsAwaitingVisibility(context.Background(), 100, boundaryTestBoundary())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing snapshot channel seek position")
}

// TestSegmentsAwaitingVisibility_ExternalCollection guards against CreateSnapshot
// hanging forever on an external collection: every compaction policy skips
// IsExternal() collections outright, so a flushed-but-unsorted segment there
// can never be sorted, and waiting on it would never return.
func TestSegmentsAwaitingVisibility_ExternalCollection(t *testing.T) {
	sm := boundaryTestManager(boundaryTestSegment(1))
	sm.meta.collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{ExternalField: "col"}},
		},
	})

	awaiting, err := sm.segmentsAwaitingVisibility(context.Background(), 100, boundaryTestBoundary())
	assert.NoError(t, err)
	assert.Empty(t, awaiting)
}

func TestWaitForVisibleBoundary(t *testing.T) {
	t.Run("returns immediately when nothing awaits visibility", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
			s.IsSorted = true
			s.IsInvisible = false
		}))

		// A canceled context proves the wait never selected on it: a fully
		// visible boundary must not depend on having time left.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.NoError(t, sm.waitForBoundary(ctx, 100, boundaryTestBoundary(), true))
	})

	t.Run("without the opt-in, does not wait for sort", func(t *testing.T) {
		// The default. The segment is invisible and would be awaited under the
		// opt-in, but its rows are served anyway (as a growing segment), so the
		// capture proceeds. A canceled context proves no waiting happened.
		sm := boundaryTestManager(boundaryTestSegment(1))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.NoError(t, sm.waitForBoundary(ctx, 100, boundaryTestBoundary(), false))
	})

	t.Run("without the opt-in, still waits for the boundary to be complete", func(t *testing.T) {
		// Completeness is not optional: until every channel checkpoint has
		// passed the boundary, DataCoord has not heard about the segments the
		// fence just sealed, so capturing now would silently miss them.
		sm := boundaryTestManagerAt(boundaryTestSeekTs - 1)

		paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "0")
		defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

		err := sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reach its boundary")
	})

	t.Run("returns a retryable error past the per-attempt budget", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "0")
		defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

		err := sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), true)
		assert.Error(t, err)
		// Retryable: the ack scheduler has to come back, since the message is
		// already in the WAL and the snapshot must eventually exist. This does
		// NOT release the collection's resource-key lock -- see the function
		// doc -- it only bounds one polling attempt.
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "become visible")
	})

	t.Run("skips the wait entirely when segments flushed visible", func(t *testing.T) {
		// Sort off, nothing invisible: there is no sort coming and none is
		// needed, so the wait must return at once rather than block on a state
		// change that is not going to happen.
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) { s.IsInvisible = false }))

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.NoError(t, sm.waitForBoundary(ctx, 100, boundaryTestBoundary(), true))
	})

	t.Run("fails fast on segments stranded invisible with sort off", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		// A long per-attempt budget proves this returns because the switch is
		// off, not because the attempt timed out.
		paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "60")
		defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

		start := time.Now()
		err := sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), true)
		assert.Less(t, time.Since(start), 5*time.Second)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "stranded invisible")
	})

	t.Run("fails fast when the compaction subsystem is off", func(t *testing.T) {
		// EnableCompaction gates startCompaction(), the only caller of the
		// trigger manager and inspector, so sort never runs even though its own
		// switch is on. Checking EnableSortCompaction alone would miss this and
		// wait out the full budget on every retry, forever.
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)
		paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "60")
		defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

		start := time.Now()
		err := sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), true)
		assert.Less(t, time.Since(start), 5*time.Second)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "stranded invisible")
	})

	t.Run("returns once the segment is published visible", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		go func() {
			time.Sleep(Params.DataCoordCfg.SnapshotSortWaitPollInterval.GetAsDuration(time.Second) + 200*time.Millisecond)
			sm.meta.segMu.Lock()
			defer sm.meta.segMu.Unlock()
			// A sort replaces its input with a new id. Neither id has to be
			// tracked: the old segment leaves the set as Dropped, the new one
			// because it is published visible.
			old := sm.meta.segments.GetSegment(1)
			old.State = commonpb.SegmentState_Dropped
			sm.meta.segments.SetSegment(1, old)
			sm.meta.segments.SetSegment(2, boundaryTestSegment(2, func(s *datapb.SegmentInfo) {
				s.IsSorted = true
				s.IsInvisible = false
				s.CompactionFrom = []int64{1}
			}))
		}()

		assert.NoError(t, sm.waitForBoundary(context.Background(), 100, boundaryTestBoundary(), true))
	})
}

// checkSnapshotVisibilityReachable is the pre-broadcast half of the same question
// waitForBoundary asks after the fact. It has to be the one that actually
// rejects: once the message is appended the client has been told the call
// succeeded, and the callback's error only buys an unbounded retry that never
// gives the collection's DDL resource key back.
func TestCheckSnapshotVisibilityReachable(t *testing.T) {
	protectedSnapshotMeta := func(segmentIDs ...int64) *snapshotMeta {
		until := make(map[int64]uint64, len(segmentIDs))
		for _, id := range segmentIDs {
			until[id] = uint64(time.Now().Add(time.Hour).Unix())
		}
		return &snapshotMeta{
			compactionBlockedCollections: typeutil.NewUniqueSet(),
			snapshotPendingCollections:   typeutil.NewUniqueSet(),
			snapshotStagingCollections:   typeutil.NewUniqueSet(),
			segmentProtectionUntil:       until,
		}
	}

	visible := func(s *datapb.SegmentInfo) { s.IsInvisible = false }

	t.Run("passes when everything is already visible", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1, visible))
		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("passes with sort off when segments flushed visible", func(t *testing.T) {
		// The case that must not be refused. With sort compaction off,
		// flushFlushingSegment never stamps IsInvisible, so these segments are
		// indexed, sealed-loaded and backfill-eligible -- there is nothing to
		// wait for and nothing wrong with capturing them. Refusing here would
		// make snapshots unusable on any cluster running with sort off.
		sm := boundaryTestManager(
			boundaryTestSegment(1, visible),
			boundaryTestSegment(2, visible),
		)

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("passes with sort off while a clustering compaction is in flight", func(t *testing.T) {
		// Its results are invisible but CreatedByCompaction, so the wait skips
		// them and the index build will publish them. Refusing here -- with
		// "can never be published" -- would be simply untrue.
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
			s.CreatedByCompaction = true
			s.CompactionFrom = []int64{99}
		}))

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("passes with sort off even while segments are still growing", func(t *testing.T) {
		// The fence will seal these, and with sort off they flush straight to
		// visible -- so predicting that they will join the wait set would be
		// wrong, and refusing on it doubly so.
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
			s.State = commonpb.SegmentState_Growing
			s.NumOfRows = 100
		}))

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("rejects segments stranded invisible with sort off", func(t *testing.T) {
		// The one genuinely unresolvable case: these were flushed while sort was
		// on, so they carry IsInvisible, and only a sort or clustering completion
		// clears it. With the subsystem off neither will ever run.
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		err := checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "stranded")
	})

	t.Run("rejects stranded segments when the compaction subsystem is disabled", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key)

		err := checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "stranded")
	})

	t.Run("rejects an invisible segment pinned by an older snapshot", func(t *testing.T) {
		// Both sort-triggering paths skip a protected segment while
		// segmentsAwaitingVisibility still waits for it, so this would spin until
		// the old protection lapses -- up to 7 days by default.
		sm := boundaryTestManager(boundaryTestSegment(1))
		sm.meta.snapshotMeta = protectedSnapshotMeta(1)

		err := checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "compaction protection")
	})

	t.Run("ignores protection on a segment that is already visible", func(t *testing.T) {
		// A pinned but visible segment needs nothing at all; rejecting on it
		// would refuse snapshots for the whole life of the older snapshot.
		sm := boundaryTestManager(boundaryTestSegment(1, visible))
		sm.meta.snapshotMeta = protectedSnapshotMeta(1)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("counts a growing segment while sort is on", func(t *testing.T) {
		// With sort on the fence seals it into an invisible segment, so it will
		// join the wait set and a pinned-or-unreachable state has to be caught.
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
			s.State = commonpb.SegmentState_Growing
			s.NumOfRows = 100
		}))
		sm.meta.snapshotMeta = protectedSnapshotMeta(1)

		err := checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "compaction protection")
	})

	t.Run("ignores empty and L0 segments", func(t *testing.T) {
		sm := boundaryTestManager(
			boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
				s.Binlogs = nil
				s.NumOfRows = 0
			}),
			boundaryTestSegment(2, func(s *datapb.SegmentInfo) { s.Level = datapb.SegmentLevel_L0 }),
		)

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})

	t.Run("passes for an external collection", func(t *testing.T) {
		// segmentsAwaitingVisibility short-circuits external collections, so the wait
		// never blocks on them and there is nothing to refuse.
		sm := boundaryTestManager(boundaryTestSegment(1))
		sm.meta.collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{ExternalField: "col"}},
			},
		})

		paramtable.Get().Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

		assert.NoError(t, checkSnapshotVisibilityReachable(context.Background(), sm.meta, 100))
	})
}

func TestGenSnapshot_RequiresBoundary(t *testing.T) {
	// There is no fallback path. A snapshot that cannot say where it cuts must
	// fail rather than fall back to channel checkpoints, which is the behavior
	// that let unsorted segments into snapshots in the first place. The check
	// runs before any broker call, so a bare handler reaches it.
	_, err := (&ServerHandler{}).GenSnapshot(context.Background(), 100, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boundary is required")
}

// A compaction publishes its output before its inputs go away -- atomically for
// mix and sort, but across several catalog writes for clustering, which marks
// results visible and only then drops inputs. Anywhere in that gap both
// generations look capturable, and the snapshot would hold the same rows twice.
func TestDropSupersededByLineage(t *testing.T) {
	newMeta := func(segments ...*SegmentInfo) *meta {
		m := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
		for _, segment := range segments {
			m.segments.SetSegment(segment.GetID(), segment)
		}
		return m
	}
	ids := func(segments []*SegmentInfo) []int64 {
		out := make([]int64, 0, len(segments))
		for _, segment := range segments {
			out = append(out, segment.GetID())
		}
		return out
	}
	child := func(id int64, from ...int64) *SegmentInfo {
		return boundaryTestSegment(id, func(s *datapb.SegmentInfo) {
			s.IsInvisible = false
			s.CreatedByCompaction = true
			s.CompactionFrom = from
		})
	}
	leaf := func(id int64) *SegmentInfo {
		return boundaryTestSegment(id, func(s *datapb.SegmentInfo) { s.IsInvisible = false })
	}

	t.Run("drops an output whose inputs are also captured", func(t *testing.T) {
		// The clustering window: results already visible, inputs not yet Dropped.
		in1, in2, out := leaf(1), leaf(2), child(3, 1, 2)
		m := newMeta(in1, in2, out)

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{in1, in2, out})
		assert.ElementsMatch(t, []int64{1, 2}, ids(kept))
	})

	t.Run("keeps the output once its inputs are gone", func(t *testing.T) {
		// Inputs Dropped, so they never entered the selected set. The output is
		// the only representation left and must be kept.
		out := child(3, 1, 2)
		m := newMeta(
			boundaryTestSegment(1, func(s *datapb.SegmentInfo) { s.State = commonpb.SegmentState_Dropped }),
			boundaryTestSegment(2, func(s *datapb.SegmentInfo) { s.State = commonpb.SegmentState_Dropped }),
			out,
		)

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{out})
		assert.Equal(t, []int64{3}, ids(kept))
	})

	t.Run("keeps the output when its inputs were GC'd from meta", func(t *testing.T) {
		out := child(3, 1, 2)
		m := newMeta(out) // parents absent entirely

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{out})
		assert.Equal(t, []int64{3}, ids(kept))
	})

	t.Run("walks through a Dropped intermediate generation", func(t *testing.T) {
		// A -> C -> E with C already Dropped. C is not capturable, but without
		// it as a waypoint the walk stops and both A and E survive.
		a, e := leaf(1), child(5, 3)
		m := newMeta(a, boundaryTestSegment(3, func(s *datapb.SegmentInfo) {
			s.State = commonpb.SegmentState_Dropped
			s.CreatedByCompaction = true
			s.CompactionFrom = []int64{1}
		}), e)

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{a, e})
		assert.Equal(t, []int64{1}, ids(kept))
	})

	t.Run("never adds a segment the caller did not select", func(t *testing.T) {
		// The reason this removes instead of replacing: an out-of-boundary or
		// Dropped input must not be pulled in. Selecting only the output must
		// leave the result exactly the output.
		out := child(3, 1, 2)
		m := newMeta(leaf(1), leaf(2), out)

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{out})
		assert.Equal(t, []int64{3}, ids(kept))
	})

	t.Run("keeps everything rather than nothing on a cyclic lineage", func(t *testing.T) {
		// Impossible with allocator-issued ids, but each segment is the other's
		// selected parent, so the rule would drop both. Capturing nothing is
		// silent data loss; capturing a duplicate is at least detectable.
		a := child(1, 2)
		b := child(2, 1)
		m := newMeta(a, b)

		kept := dropSupersededByLineage(context.Background(), m, []*SegmentInfo{a, b})
		assert.ElementsMatch(t, []int64{1, 2}, ids(kept))
	})
}

// ── what staging blocks ──────────────────────────────────────────────────────

func TestIsCompactionBlockedForType(t *testing.T) {
	snapshotMeta := &snapshotMeta{
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		snapshotStagingCollections:   typeutil.NewUniqueSet(),
	}
	m := &meta{snapshotMeta: snapshotMeta}

	// Staging blocks only what can move a segment boundary. Sort in particular
	// has to keep running: it is what the staging snapshot is waiting for.
	snapshotMeta.SetSnapshotStaging(100)
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_SortCompaction))
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_PartitionKeySortCompaction))
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_BumpSchemaVersionCompaction))
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_Level0DeleteCompaction))
	assert.True(t, m.isCompactionBlockedForType(100, datapb.CompactionType_MixCompaction))
	assert.True(t, m.isCompactionBlockedForType(100, datapb.CompactionType_ClusteringCompaction))
	assert.True(t, m.isCollectionCompactionBlocked(100))

	// Pending: the segment list is being captured, so even a sorted replacement
	// would swap out a segment the snapshot has already chosen.
	snapshotMeta.ClearSnapshotStaging(100)
	snapshotMeta.SetSnapshotPending(100)
	assert.True(t, m.isCompactionBlockedForType(100, datapb.CompactionType_SortCompaction))
	assert.True(t, m.isCompactionBlockedForType(100, datapb.CompactionType_MixCompaction))

	snapshotMeta.ClearSnapshotPending(100)
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_SortCompaction))
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_MixCompaction))

	// Another collection staging says nothing about this one.
	snapshotMeta.SetSnapshotStaging(200)
	assert.False(t, m.isCompactionBlockedForType(100, datapb.CompactionType_MixCompaction))
}

func TestChangesSegmentBoundary(t *testing.T) {
	// Only the N:M types can produce a segment spanning rows its inputs did not
	// span together, which is the only way rows from after a snapshot end up in
	// a segment the snapshot claims.
	assert.True(t, changesSegmentBoundary(datapb.CompactionType_MixCompaction))
	assert.True(t, changesSegmentBoundary(datapb.CompactionType_ClusteringCompaction))

	assert.False(t, changesSegmentBoundary(datapb.CompactionType_SortCompaction))
	assert.False(t, changesSegmentBoundary(datapb.CompactionType_PartitionKeySortCompaction))
	assert.False(t, changesSegmentBoundary(datapb.CompactionType_BumpSchemaVersionCompaction))
	assert.False(t, changesSegmentBoundary(datapb.CompactionType_Level0DeleteCompaction))
}

// ── deletes respect the boundary ─────────────────────────────────────────────

func TestFilterDeltalogsBefore(t *testing.T) {
	deltalogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogID: 1, TimestampFrom: 10, TimestampTo: 20},   // before
				{LogID: 2, TimestampFrom: 150, TimestampTo: 200}, // after
			},
		},
		{
			// Every file after the boundary: the whole entry goes.
			FieldID: 101,
			Binlogs: []*datapb.Binlog{
				{LogID: 3, TimestampFrom: 150, TimestampTo: 200},
			},
		},
	}

	filtered := filterDeltalogsBefore(deltalogs, boundaryTestSeekTs)
	assert.Len(t, filtered, 1)
	assert.Equal(t, int64(100), filtered[0].GetFieldID())
	assert.Len(t, filtered[0].GetBinlogs(), 1)
	assert.Equal(t, int64(1), filtered[0].GetBinlogs()[0].GetLogID())

	// The input must not be mutated: the same SegmentInfo is live in meta.
	assert.Len(t, deltalogs[0].GetBinlogs(), 2)

	t.Run("keeps a file that spans the boundary", func(t *testing.T) {
		// L0 delete compaction merges the deletes of all its input L0 segments
		// into one file per target, stamped with the min/max of that union, and
		// it is exempt from the snapshot compaction block -- so during a staging
		// window it can produce a file straddling the boundary. Dropping it would
		// discard the pre-boundary deletes inside, and its L0 inputs are Dropped
		// by then, so nothing else carries them: rows deleted before the cut
		// would come back. Over-applying the post-boundary deletes it also holds
		// is the less bad way to be wrong.
		kept := filterDeltalogsBefore([]*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{
				{LogID: 7, TimestampFrom: boundaryTestSeekTs - 10, TimestampTo: boundaryTestSeekTs + 10},
			}},
		}, boundaryTestSeekTs)
		assert.Len(t, kept, 1)
		assert.Len(t, kept[0].GetBinlogs(), 1)
		assert.Equal(t, int64(7), kept[0].GetBinlogs()[0].GetLogID())
	})

	t.Run("still drops a file that starts at the boundary", func(t *testing.T) {
		// Nothing in it predates the cut, so it carries no delete the snapshot
		// should apply.
		kept := filterDeltalogsBefore([]*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{
				{LogID: 8, TimestampFrom: boundaryTestSeekTs, TimestampTo: boundaryTestSeekTs + 10},
			}},
		}, boundaryTestSeekTs)
		assert.Empty(t, kept)
	})

	t.Run("keeps files with no TimestampTo", func(t *testing.T) {
		// Incomplete metadata must not drop deletes -- that resurrects rows,
		// which is the wrong direction to fail in.
		kept := filterDeltalogsBefore([]*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}},
		}, boundaryTestSeekTs)
		assert.Len(t, kept, 1)
		assert.Len(t, kept[0].GetBinlogs(), 1)
	})

	t.Run("nil when everything is filtered out", func(t *testing.T) {
		assert.Nil(t, filterDeltalogsBefore([]*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1, TimestampFrom: 150, TimestampTo: 200}}},
		}, boundaryTestSeekTs))
	})

	t.Run("keeps a file with no TimestampFrom", func(t *testing.T) {
		// Same principle as the missing-TimestampTo case: unset metadata cannot
		// prove the file is entirely after the cut, and guessing that it is
		// resurrects rows.
		kept := filterDeltalogsBefore([]*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 9, TimestampTo: 200}}},
		}, boundaryTestSeekTs)
		assert.Len(t, kept, 1)
	})
}

// ── sort keeps running when merging is off ───────────────────────────────────

func TestSingleCompactionPolicyEnable_IgnoresAutoCompaction(t *testing.T) {
	// Sort lives in this policy, and a segment that has not been sorted is still
	// on the growing query path -- and now also blocks any snapshot of its
	// collection. Turning off auto compaction is an operator asking for less
	// merging, not for segments to be stranded, so the policy stays enabled and
	// only its merge half honors the flag.
	policy := newSingleCompactionPolicy(nil, nil, nil)

	paramtable.Get().Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableAutoCompaction.Key)
	assert.True(t, policy.Enable())

	paramtable.Get().Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	assert.True(t, policy.Enable())
}
