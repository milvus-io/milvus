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

// boundaryTestSegment is inside the boundary and still owes a sort, so each test
// overrides only the one field it is about.
func boundaryTestSegment(id int64, opts ...func(*datapb.SegmentInfo)) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  100,
		PartitionID:   101,
		InsertChannel: boundaryTestChannelA,
		State:         commonpb.SegmentState_Flushed,
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
// boundary, so tests about the sort half are not silently answered by the
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

// TestWaitForSortedBoundaryWaitsForCompleteness is the reason the gate exists.
// Before a channel's checkpoint passes the boundary, DataCoord may not have been
// told about the segments the fence sealed -- they are visible on the streaming
// node long before they appear here. Scanning for unsorted segments then finds
// nothing, which is indistinguishable from "everything is sorted", and the
// snapshot would capture a boundary whose own data had not arrived.
func TestWaitForSortedBoundaryWaitsForCompleteness(t *testing.T) {
	// No segments at all: exactly the "meta has not caught up" shape.
	sm := boundaryTestManagerAt(boundaryTestSeekTs - 1)

	paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "0")
	defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

	err := sm.waitForSortedBoundary(context.Background(), 100, boundaryTestBoundary())
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
	assert.Contains(t, err.Error(), "reach its boundary")
}

// ── the wait set ─────────────────────────────────────────────────────────────

func TestSegmentsAwaitingSort(t *testing.T) {
	tests := []struct {
		name    string
		segment *SegmentInfo
		want    bool
	}{
		{
			name:    "flushed and unsorted",
			segment: boundaryTestSegment(1),
			want:    true,
		},
		{
			// A segment with a sort task already dispatched has not been sorted
			// yet. canTriggerSortCompaction excludes it so it is not dispatched
			// twice; the wait includes it for the opposite reason.
			name:    "already compacting",
			segment: func() *SegmentInfo { s := boundaryTestSegment(2); s.isCompacting = true; return s }(),
			want:    true,
		},
		{
			name:    "sorted",
			segment: boundaryTestSegment(3, func(s *datapb.SegmentInfo) { s.IsSorted = true }),
			want:    false,
		},
		{
			name:    "sorted by namespace",
			segment: boundaryTestSegment(4, func(s *datapb.SegmentInfo) { s.IsSortedByNamespace = true }),
			want:    false,
		},
		{
			name:    "L0 never sorts",
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
			awaiting, err := sm.segmentsAwaitingSort(context.Background(), 100, boundaryTestBoundary())
			assert.NoError(t, err)
			if tt.want {
				assert.Equal(t, []int64{tt.segment.GetID()}, awaiting)
			} else {
				assert.Empty(t, awaiting)
			}
		})
	}
}

func TestSegmentsAwaitingSort_UnknownChannel(t *testing.T) {
	// A boundary that does not cover a segment's channel is not "segment
	// excluded", it is a boundary we cannot evaluate. Reporting "nothing to wait
	// for" there would silently snapshot an unsorted segment.
	sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) {
		s.InsertChannel = boundaryTestChannelB
	}))

	_, err := sm.segmentsAwaitingSort(context.Background(), 100, boundaryTestBoundary())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing snapshot channel seek position")
}

// TestSegmentsAwaitingSort_ExternalCollection guards against CreateSnapshot
// hanging forever on an external collection: every compaction policy skips
// IsExternal() collections outright, so a flushed-but-unsorted segment there
// can never be sorted, and waiting on it would never return.
func TestSegmentsAwaitingSort_ExternalCollection(t *testing.T) {
	sm := boundaryTestManager(boundaryTestSegment(1))
	sm.meta.collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{ExternalField: "col"}},
		},
	})

	awaiting, err := sm.segmentsAwaitingSort(context.Background(), 100, boundaryTestBoundary())
	assert.NoError(t, err)
	assert.Empty(t, awaiting)
}

func TestWaitForSortedBoundary(t *testing.T) {
	t.Run("returns immediately when nothing awaits sort", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1, func(s *datapb.SegmentInfo) { s.IsSorted = true }))

		// A canceled context proves the wait never selected on it: a sorted
		// boundary must not depend on having time left.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.NoError(t, sm.waitForSortedBoundary(ctx, 100, boundaryTestBoundary()))
	})

	t.Run("yields the lock past the budget", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		paramtable.Get().Save(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key, "0")
		defer paramtable.Get().Reset(Params.DataCoordCfg.SnapshotSortWaitTimeout.Key)

		err := sm.waitForSortedBoundary(context.Background(), 100, boundaryTestBoundary())
		assert.Error(t, err)
		// Retryable: the ack scheduler has to come back, since the message is
		// already in the WAL and the snapshot must eventually exist.
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable), "got %v", err)
		assert.Contains(t, err.Error(), "sort compaction")
	})

	t.Run("returns once the segment is sorted", func(t *testing.T) {
		sm := boundaryTestManager(boundaryTestSegment(1))

		go func() {
			time.Sleep(Params.DataCoordCfg.SnapshotSortWaitPollInterval.GetAsDuration(time.Second) + 200*time.Millisecond)
			sm.meta.segMu.Lock()
			defer sm.meta.segMu.Unlock()
			// A sort replaces its input with a new id. Neither id has to be
			// tracked: the old segment leaves the set as Dropped, the new one as
			// sorted.
			old := sm.meta.segments.GetSegment(1)
			old.State = commonpb.SegmentState_Dropped
			sm.meta.segments.SetSegment(1, old)
			sm.meta.segments.SetSegment(2, boundaryTestSegment(2, func(s *datapb.SegmentInfo) {
				s.IsSorted = true
				s.CompactionFrom = []int64{1}
			}))
		}()

		assert.NoError(t, sm.waitForSortedBoundary(context.Background(), 100, boundaryTestBoundary()))
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
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1, TimestampTo: 200}}},
		}, boundaryTestSeekTs))
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
