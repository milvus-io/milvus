package recovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSummaryManagerMinRequiredGenerationAggregatesIdempotencySummaries(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})

	summary := newEmptyVChannelSummary("p1", "v1", nil)
	summary.latestAppliedGeneration = 7
	summary.refreshMinRequiredGeneration()
	manager.setSummaries(map[string]*vchannelSummary{
		"v1": summary,
	})

	// With no override, the boundary is the summary's own min-required generation.
	boundary, _ := manager.minRequiredGeneration(nil, 0)
	require.Equal(t, uint64(7), boundary)

	// A supplied meta overrides that vchannel's contribution.
	boundary, _ = manager.minRequiredGeneration(map[string]*streamingpb.VChannelSummaryMeta{
		"v1": {
			Pchannel:              "p1",
			Vchannel:              "v1",
			ViewType:              common.VChannelSummaryViewTypeIdempotency,
			MinRequiredGeneration: 2,
		},
	}, 0)
	require.Equal(t, uint64(2), boundary)
}

func TestSummaryManagerCleanerWaitsForActiveViewInitialization(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})
	require.False(t, manager.canCleanPChannelSummary())

	manager.markActiveViewsInitialized()
	require.True(t, manager.canCleanPChannelSummary())
}

func TestSummaryManagerMaintainsPChannelSummarySnapshotCheckpoint(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})

	manager.setPChannelSummarySnapshotCheckpoint(&WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	})
	checkpoint := manager.getPChannelSummarySnapshotCheckpointUnsafe()
	require.NotNil(t, checkpoint)
	require.Equal(t, uint64(10), checkpoint.TimeTick)
	persisted := manager.getPersistedPChannelSummarySnapshotCheckpointUnsafe()
	require.NotNil(t, persisted)
	require.Equal(t, uint64(10), persisted.TimeTick)

	checkpoint.TimeTick = 1
	require.Equal(t, uint64(10), manager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)

	manager.advancePChannelSummarySnapshotCheckpoint(&WALCheckpoint{
		MessageID: rmq.NewRmqID(9),
		TimeTick:  9,
	})
	require.Equal(t, uint64(10), manager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)

	manager.advancePChannelSummarySnapshotCheckpoint(&WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	})
	require.Equal(t, uint64(20), manager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)
	require.Equal(t, uint64(10), manager.getPersistedPChannelSummarySnapshotCheckpointUnsafe().TimeTick)

	manager.markPChannelSummarySnapshotCheckpointPersisted(&WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	})
	require.Equal(t, uint64(20), manager.getPersistedPChannelSummarySnapshotCheckpointUnsafe().TimeTick)
}

func TestSummaryManagerTracksPersistedConsumeCheckpoint(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, &WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}, summaryEvictionConfig{})

	require.False(t, manager.canPersistConsumeCheckpointUnsafe(&WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}))
	require.True(t, manager.canPersistConsumeCheckpointUnsafe(&WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	}))

	manager.markConsumeCheckpointPersisted(&WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	})
	require.False(t, manager.canPersistConsumeCheckpointUnsafe(&WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	}))
}

func TestEvictForRecoveryByTTL(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	timeticks := []uint64{100, 200, 300, 400, 500}
	populateSummaryEntries(state, timeticks)
	require.Len(t, state.entries, 5)

	state.evictForRecovery(350, 0, 0)
	require.Len(t, state.entries, 2)
	require.Contains(t, state.entries, "key-3")
	require.Contains(t, state.entries, "key-4")
}

func TestEvictForRecoveryRespectsMinEntries(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300})
	require.Len(t, state.entries, 3)

	state.evictForRecovery(500, 2, 0)
	require.Len(t, state.entries, 2)
	require.Contains(t, state.entries, "key-1")
	require.Contains(t, state.entries, "key-2")
}

func TestEvictForRecoveryEnforcesMaxBytes(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	timeticks := make([]uint64, 10)
	for i := range timeticks {
		timeticks[i] = uint64(1000 + i*100)
	}
	populateSummaryEntries(state, timeticks)
	require.Len(t, state.entries, 10)
	require.Positive(t, state.entryBytes)

	capBytes := 0
	for _, key := range state.commitOrder[5:] {
		capBytes += proto.Size(state.entries[key].entry)
	}
	state.evictForRecovery(0, 0, capBytes)
	require.Len(t, state.entries, 5)
}

func TestEvictPersistedRemovesPersistedEntries(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300, 400})

	// key-0, key-1 are persisted; key-2, key-3 are not yet persisted.
	state.markCommittedWriteRecordGeneration(makeRecord("key-0", 100), 3)
	state.markCommittedWriteRecordGeneration(makeRecord("key-1", 200), 3)

	state.evictPersisted()
	require.Len(t, state.entries, 2)
	require.Contains(t, state.entries, "key-2")
	require.Contains(t, state.entries, "key-3")
}

func TestMinRequiredGenerationSkipsUnpersistedEntries(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	// Generations up to 4 have already been persisted on this channel.
	state.latestAppliedGeneration = 4
	// A freshly-observed, not-yet-persisted entry lives in the WAL, not in any
	// chunk, so it must not pin the chunk-retention boundary back to 0.
	populateSummaryEntries(state, []uint64{500})

	state.refreshMinRequiredGeneration()
	require.Equal(t, uint64(4), state.minRequiredGeneration)
}

func TestEvictPersistedAdvancesMinRequiredWithPendingEntries(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	populateSummaryEntries(state, []uint64{100, 200, 300})
	// key-0, key-1 persisted at generations 1 and 2; key-2 still pending.
	state.markCommittedWriteRecordGeneration(makeRecord("key-0", 100), 1)
	state.markCommittedWriteRecordGeneration(makeRecord("key-1", 200), 2)
	state.latestAppliedGeneration = 2

	state.evictPersisted()

	// Persisted entries are dropped; the pending entry is kept.
	require.Len(t, state.entries, 1)
	require.Contains(t, state.entries, "key-2")
	// The retention boundary advances to the latest persisted generation rather
	// than being pinned at 0 by the pending entry, so old chunks become
	// reclaimable on a continuously busy channel.
	require.Equal(t, uint64(2), state.minRequiredGeneration)
}

func TestEvictPersistedStopsAtEntryWithoutGeneration(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300, 400})

	state.markCommittedWriteRecordGeneration(makeRecord("key-0", 100), 1)
	state.markCommittedWriteRecordGeneration(makeRecord("key-1", 200), 2)
	// key-2 has no generation
	state.markCommittedWriteRecordGeneration(makeRecord("key-3", 400), 1)

	state.evictPersisted()
	require.Len(t, state.entries, 2)
	require.Contains(t, state.entries, "key-2")
	require.Contains(t, state.entries, "key-3")
}

func TestObserveTimeTickTriggersRecoveryEviction(t *testing.T) {
	ttl := 5 * time.Second
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{
		entryTTL:   ttl,
		minEntries: 0,
	})
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	nowTT := tsoutil.ComposeTS(100000, 0)
	oldTT := tsoutil.ComposeTS(90000, 0)
	populateSummaryEntriesWithBaseTT(state, oldTT, 5)
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})
	require.Len(t, state.entries, 5)

	msg := buildTimeTickMessage(t, nowTT)
	manager.observeMessage(msg)

	require.Less(t, len(state.entries), 5)
}

func TestObserveTimeTickNoEvictionInNormalMode(t *testing.T) {
	ttl := 5 * time.Second
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{
		entryTTL:   ttl,
		minEntries: 0,
	})
	manager.setNormalMode()
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	nowTT := tsoutil.ComposeTS(100000, 0)
	oldTT := tsoutil.ComposeTS(90000, 0)
	populateSummaryEntriesWithBaseTT(state, oldTT, 5)
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})

	msg := buildTimeTickMessage(t, nowTT)
	manager.observeMessage(msg)

	require.Len(t, state.entries, 5)
}

func TestEvictPersistedEntriesInNormalMode(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})
	manager.setNormalMode()
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300, 400})
	// key-0, key-1 persisted; key-2, key-3 still pending.
	state.markCommittedWriteRecordGeneration(makeRecord("key-0", 100), 1)
	state.markCommittedWriteRecordGeneration(makeRecord("key-1", 200), 2)
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})

	manager.evictPersistedEntries()

	require.Len(t, state.entries, 2)
	require.Contains(t, state.entries, "key-2")
	require.Contains(t, state.entries, "key-3")
}

func TestEvictPersistedEntriesNoOpInRecoveryMode(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300})
	state.markCommittedWriteRecordGeneration(makeRecord("key-0", 100), 1)
	state.markCommittedWriteRecordGeneration(makeRecord("key-1", 200), 1)
	state.markCommittedWriteRecordGeneration(makeRecord("key-2", 300), 1)
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})

	manager.evictPersistedEntries()

	require.Len(t, state.entries, 3)
}

func TestGetSummarySnapshotCheckpointSkipsNilCheckpoint(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})

	summaryWithCP := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(50),
		TimeTick:  50,
	})
	summaryWithoutCP := newEmptyVChannelSummary("p1", "v2", nil)
	manager.setSummaries(map[string]*vchannelSummary{
		"v1": summaryWithCP,
		"v2": summaryWithoutCP,
	})

	cp := manager.getSummarySnapshotCheckpointUnsafe()
	require.NotNil(t, cp)
	require.Equal(t, uint64(50), cp.TimeTick)
}

func TestGetSummarySnapshotCheckpointAllNilReturnsNil(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, nil, summaryEvictionConfig{})

	summary1 := newEmptyVChannelSummary("p1", "v1", nil)
	summary2 := newEmptyVChannelSummary("p1", "v2", nil)
	manager.setSummaries(map[string]*vchannelSummary{
		"v1": summary1,
		"v2": summary2,
	})

	cp := manager.getSummarySnapshotCheckpointUnsafe()
	require.Nil(t, cp)
}

// --- helpers ---

func populateSummaryEntries(state *vchannelSummary, timeticks []uint64) {
	for i, tt := range timeticks {
		key := fmt.Sprintf("key-%d", i)
		record := *committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
			Key:            key,
			CommitTimetick: tt,
			MessageId:      rmq.NewRmqID(int64(tt)).IntoProto(),
		})
		_ = state.applyCommittedWriteRecord(record, false)
	}
}

func populateSummaryEntriesWithBaseTT(state *vchannelSummary, baseTT uint64, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		tt := baseTT + uint64(i)
		record := *committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
			Key:            key,
			CommitTimetick: tt,
			MessageId:      rmq.NewRmqID(int64(tt)).IntoProto(),
		})
		_ = state.applyCommittedWriteRecord(record, false)
	}
}

func makeRecord(key string, timetick uint64) committedWriteRecord {
	return committedWriteRecord{
		SourcePChannel: "p1",
		VChannel:       "v1",
		SourceTimeTick: timetick,
		Idempotency:    &committedWriteIdempotency{Key: key},
	}
}

func buildTimeTickMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	msg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				Timestamp: timetick,
			},
		}).
		WithAllVChannel().
		BuildMutable()
	require.NoError(t, err)
	return msg.
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick) - 1)).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}
