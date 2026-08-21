package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

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

func newTestSummaryManagerWithRecords(t *testing.T, vchannel string, count int) (*summaryManager, *vchannelSummary) {
	t.Helper()
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, summaryEvictionConfig{})
	state := newEmptyVChannelSummary("p1", vchannel, testRecoveryCheckpoint(1, 1))
	for _, record := range newTestSummaryRecords("key", 100, count) {
		state.applySummaryRecord(record, true)
	}
	manager.setSummaries(map[string]*vchannelSummary{vchannel: state})
	return manager, state
}

func TestEvictPersistedRecordsInNormalMode(t *testing.T) {
	manager, state := newTestSummaryManagerWithRecords(t, "v1", 4)
	manager.setNormalMode()

	// In normal mode the staging buffer is released once its contents are in a
	// chunk: the interceptor window, not this summary, answers live dedup.
	manager.evictPersistedRecordsUnsafe()

	require.Empty(t, state.records)
}

func TestEvictPersistedRecordsNoOpInRecoveryMode(t *testing.T) {
	manager, state := newTestSummaryManagerWithRecords(t, "v1", 3)

	// During recovery the records ARE the set being rebuilt for the consumer, so
	// nothing may be released until the handover is done.
	manager.evictPersistedRecordsUnsafe()

	require.Len(t, state.records, 3)
}

func TestSummaryManagerTracksDirtyStaging(t *testing.T) {
	manager, state := newTestSummaryManagerWithRecords(t, "v1", 2)
	require.True(t, manager.hasDirtySummary())

	// Peeking must NOT clear dirtiness: nothing is durable yet, so a persist that
	// fails here has to leave the records for the next attempt to find.
	require.Len(t, state.peekPendingSummaryRecords(), 2)
	require.True(t, manager.hasDirtySummary())

	state.dropPersistedSummaryRecords(2)
	require.False(t, manager.hasDirtySummary())
}

func TestSummaryManagerConsumesPendingRecordsPerVChannel(t *testing.T) {
	manager, _ := newTestSummaryManagerWithRecords(t, "v1", 2)
	second := newEmptyVChannelSummary("p1", "v2", testRecoveryCheckpoint(1, 1))
	second.applySummaryRecord(newTestSummaryRecord("other", 500, 5), true)
	manager.setSummary("v2", second)

	records := manager.peekPendingSummaryRecordsUnsafe()
	require.Len(t, records, 2)
	require.Len(t, records["v1"], 2)
	require.Len(t, records["v2"], 1)

	// Dropping is what releases them, and only what the chunk carried.
	manager.dropPersistedSummaryRecordsUnsafe(records)
	require.Empty(t, manager.peekPendingSummaryRecordsUnsafe())
}

func TestSummaryManagerObserveOnlyTouchesTargetVChannel(t *testing.T) {
	manager, _ := newTestSummaryManagerWithRecords(t, "v1", 1)
	other := newEmptyVChannelSummary("p1", "v2", testRecoveryCheckpoint(1, 1))
	manager.setSummary("v2", other)

	manager.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "target-key", 200))
	require.Contains(t, manager.summaries()["v1"].records, "target-key")
	require.Empty(t, other.records)

	// A timetick is pchannel-wide, so it advances every summary's applied
	// position without materializing anything.
	manager.observeMessage(buildTimeTickMessage(t, 300))
	require.Equal(t, uint64(300), other.appliedTimetick)
	require.Empty(t, other.records)
}

func TestSummaryManagerRemoveSummaryDropsReclaimedVChannel(t *testing.T) {
	manager, _ := newTestSummaryManagerWithRecords(t, "v1", 1)
	require.Len(t, manager.summaries(), 1)

	// Nothing is persisted per vchannel, so a dropped one leaves nothing behind.
	manager.removeSummary("v1")
	require.Empty(t, manager.summaries())
}

// A DDL that removes the rows a record describes must clear the vchannel's
// summary, not just the interceptor's live window.
//
// The auto-derived idempotency key is a hash of the destination and the payload
// with no collection generation and no partition id in it, so re-inserting the
// same rows after a truncate hashes to the same key. If the record survives, the
// re-insert is answered as a duplicate: nothing reaches the WAL and the client is
// told the write succeeded, with the original primary keys, into an empty
// collection. Leaving it staged would be worse still -- the next chunk would make
// it durable and a restart would serve it again.
func TestSummaryManagerDiscardsRecordsOnInvalidatingDDL(t *testing.T) {
	for _, msgType := range []message.MessageType{
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeDropPartition,
	} {
		t.Run(msgType.String(), func(t *testing.T) {
			require.True(t, InvalidatesIdempotencyWindow(msgType))
		})
	}
	// An ordinary DML type must not clear anything.
	require.False(t, InvalidatesIdempotencyWindow(message.MessageTypeInsert))
	require.False(t, InvalidatesIdempotencyWindow(message.MessageTypeCreatePartition))

	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	for _, record := range newTestSummaryRecords("key", 100, 3) {
		state.applySummaryRecord(record, true)
	}
	require.Len(t, state.records, 3)
	require.True(t, state.hasPendingSummaryRecords())

	state.discardForInvalidatedVChannel(500)

	require.Empty(t, state.records)
	require.Empty(t, state.commitOrder)
	require.Equal(t, 0, state.recordBytes)
	// Nothing staged either: a chunk written after this must not carry keys for
	// rows that no longer exist.
	require.False(t, state.hasPendingSummaryRecords())
	// Replay stays idempotent.
	require.Equal(t, uint64(500), state.appliedTimetick)
}
