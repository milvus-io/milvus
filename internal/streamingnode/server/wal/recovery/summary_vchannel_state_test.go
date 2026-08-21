package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestVChannelSummaryMaterializesByKey(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	for _, record := range newTestSummaryRecords("key", 100, 3) {
		state.applySummaryRecord(record, true)
	}

	require.Len(t, state.records, 3)
	require.Len(t, state.pendingRecords, 3)
	require.True(t, state.hasPendingSummaryRecords())
	require.Equal(t, uint64(102), state.appliedTimetick)
	require.Greater(t, state.recordBytes, 0)
}

func TestVChannelSummaryKeepsFirstSightingOfRepeatedKey(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	state.applySummaryRecord(newTestSummaryRecord("key-1", 100, 10), true)
	state.applySummaryRecord(newTestSummaryRecord("key-1", 200, 20), true)

	// The chunk still records the repeat, but the served set keeps the first
	// sighting: that is the result a duplicate append must replay.
	require.Len(t, state.records, 1)
	require.Equal(t, uint64(100), state.records["key-1"].SourceTimeTick)
	require.Len(t, state.pendingRecords, 2)
}

func TestVChannelSummaryDoesNotStageKeylessRecord(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	state.applySummaryRecord(&SummaryRecord{
		SourceMessageID: rmq.NewRmqID(50).IntoProto(),
		SourceTimeTick:  50,
	}, true)

	// A keyless write materializes nothing for any consumer, so it never reaches
	// a chunk. It still moves the applied position, which is what keeps replay
	// idempotent.
	require.Empty(t, state.records)
	require.Empty(t, state.pendingRecords)
	require.False(t, state.hasPendingSummaryRecords())
	require.Equal(t, uint64(50), state.appliedTimetick)
}

func TestVChannelSummaryPeekDoesNotRelease(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	require.Nil(t, state.peekPendingSummaryRecords())

	for _, record := range newTestSummaryRecords("key", 100, 3) {
		state.applySummaryRecord(record, true)
	}
	peeked := state.peekPendingSummaryRecords()
	require.Len(t, peeked, 3)
	// Chunk order is by timetick, so a chunk's span is its first and last record.
	require.Equal(t, uint64(100), peeked[0].SourceTimeTick)
	require.Equal(t, uint64(102), peeked[2].SourceTimeTick)

	// Nothing leaves the buffer on a peek: a persist that fails after this point
	// must still find the records here.
	require.True(t, state.hasPendingSummaryRecords())
	require.Len(t, state.peekPendingSummaryRecords(), 3)

	state.dropPersistedSummaryRecords(3)
	require.False(t, state.hasPendingSummaryRecords())
	// Releasing the staging buffer does not drop what the consumer may still be
	// served from.
	require.Len(t, state.records, 3)
}

// A chunk covers what was staged when it was built. Anything observed while it
// was being written must survive into the next one.
func TestVChannelSummaryDropReleasesOnlyWhatWasPersisted(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	for _, record := range newTestSummaryRecords("key", 100, 2) {
		state.applySummaryRecord(record, true)
	}
	persisted := state.peekPendingSummaryRecords()
	require.Len(t, persisted, 2)

	// Observed during the write.
	state.applySummaryRecord(newTestSummaryRecord("late", 200, 9), true)

	state.dropPersistedSummaryRecords(len(persisted))
	remaining := state.peekPendingSummaryRecords()
	require.Len(t, remaining, 1)
	require.Equal(t, "late", remaining[0].IdempotencyKey)
}

func TestVChannelSummarySkipsMessagesAtOrBelowAppliedTimetick(t *testing.T) {
	// Recovery seeds the applied position from the WAL consume checkpoint, so a
	// message already covered by a chunk cannot be folded in a second time.
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(100, 100))

	state.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "old-key", 100))
	require.Empty(t, state.records)

	state.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "new-key", 101))
	require.Contains(t, state.records, "new-key")
}

func TestVChannelSummaryReplayDoesNotStage(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	state.applySummaryRecordsAtGeneration(newTestSummaryRecords("key", 100, 2))

	// Records replayed out of a chunk are already durable; staging them again
	// would rewrite them into a new chunk.
	require.Len(t, state.records, 2)
	require.Empty(t, state.pendingRecords)
	require.False(t, state.hasPendingSummaryRecords())
}

func TestVChannelSummaryCapRecoveryBytesDropsOldestFirst(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	records := newTestSummaryRecords("key", 100, 4)
	state.applySummaryRecordsAtGeneration(records)

	// Keep room for roughly the two newest records.
	state.capRecoveryBytes(records[0].Size() * 2)

	require.NotContains(t, state.records, "key-0")
	require.Contains(t, state.records, "key-3")
	require.LessOrEqual(t, len(state.records), 3)
}

func TestVChannelSummaryEvictPersistedClearsServedSet(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	state.applySummaryRecordsAtGeneration(newTestSummaryRecords("key", 100, 3))

	state.evictPersisted()

	require.Empty(t, state.records)
	require.Empty(t, state.commitOrder)
	require.Equal(t, 0, state.recordBytes)
}

func TestVChannelSummarySnapshotIsSortedAndKeyed(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	state.applySummaryRecord(newTestSummaryRecord("key-late", 300, 3), false)
	state.applySummaryRecord(newTestSummaryRecord("key-early", 100, 1), false)

	snapshot := state.snapshot()
	require.Equal(t, "p1", snapshot.PChannel)
	require.Equal(t, "v1", snapshot.VChannel)
	require.Len(t, snapshot.Records, 2)
	require.Equal(t, uint64(100), snapshot.Records[0].SourceTimeTick)
	require.Equal(t, uint64(300), snapshot.Records[1].SourceTimeTick)
}
