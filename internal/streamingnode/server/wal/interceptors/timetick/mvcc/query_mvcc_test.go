package mvcc

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func createQueryTestMessage(t *testing.T, tt uint64, vc string, typ message.MessageType, txn bool, persisted bool) message.MutableMessage {
	t.Helper()
	properties := map[string]string{"_t": strconv.FormatInt(int64(typ), 10), "_vc": vc}
	if !persisted {
		properties["_np"] = ""
	}
	msg := message.NewMutableMessageBeforeAppend(nil, properties).WithTimeTick(tt)
	if txn {
		msg = msg.WithTxnContext(message.TxnContext{})
	}
	return msg
}

func publishQueryTestPrefix(t *testing.T, cm *QueryMVCCManager, through uint64, msgs ...message.MutableMessage) {
	t.Helper()
	immutable := make([]message.ImmutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutable = append(immutable, msg.IntoImmutableMessage(nil))
	}
	cm.Publish(queryMVCCMessages(immutable), createQueryTestMessage(t, through, "", message.MessageTypeTimeTick, false, false).IntoImmutableMessage(nil))
}

func TestQueryMVCCPublishesOneCompletePrefix(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("v", 100)
	deletion := createQueryTestMessage(t, 150, "v", message.MessageTypeDelete, false, true)
	insert := createQueryTestMessage(t, 200, "v", message.MessageTypeInsert, false, true)
	cm.ObserveAppend(insert) // Delete's append/ACK is still outstanding.
	minimum := cm.Minimum("v", true)
	require.EqualValues(t, 200, minimum)
	old, ready := cm.Snapshot("v", minimum)
	require.False(t, ready)
	require.Equal(t, QueryVChannelMVCC{100, 100}, old)
	weak, ready := cm.Snapshot("v", cm.Minimum("v", false))
	require.True(t, ready)
	require.Equal(t, old, weak)
	later := createQueryTestMessage(t, 250, "v", message.MessageTypeInsert, false, true)
	cm.ObserveAppend(later)
	publishQueryTestPrefix(t, cm, 200, deletion, insert)
	snapshot, err := cm.WaitSnapshot(t.Context(), "v", minimum)
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{200, 150}, snapshot)
	// ACK publication ran before the delayed append callbacks. They cannot
	// regress the complete pair or replace a newer pending lower bound.
	cm.ObserveAppend(deletion)
	cm.ObserveAppend(insert)
	require.EqualValues(t, 250, cm.Minimum("v", true))
	publishQueryTestPrefix(t, cm, 300, later)
	snapshot, ready = cm.Snapshot("v", 300)
	require.True(t, ready)
	require.Equal(t, QueryVChannelMVCC{250, 150}, snapshot)
	require.Empty(t, cm.pending)
}

func TestQueryMVCCPublicationOwnsCreateAndDrop(t *testing.T) {
	for _, callbacksFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(callbacksFirst), func(t *testing.T) {
			cm := NewQueryMVCCManager(10)
			create := createQueryTestMessage(t, 20, "v", message.MessageTypeCreateCollection, false, true)
			insert := createQueryTestMessage(t, 30, "v", message.MessageTypeInsert, false, true)
			drop := createQueryTestMessage(t, 40, "v", message.MessageTypeDropCollection, false, true)
			if callbacksFirst {
				cm.ObserveAppend(create)
				cm.ObserveAppend(insert)
				cm.ObserveAppend(drop)
			}
			// The full ACK prefix may be published before any append callback runs.
			publishQueryTestPrefix(t, cm, 40, create, insert, drop)
			if !callbacksFirst {
				cm.ObserveAppend(create)
				cm.ObserveAppend(insert)
				cm.ObserveAppend(drop)
			}
			require.Empty(t, cm.frontiers)
			require.Empty(t, cm.pending)
			cm.ApplyRecoveryBarrier("live", 40)
			for _, persisted := range []bool{false, true} {
				barrier := message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"v", "unknown", "live"}}).WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel()
				if !persisted {
					barrier.WithNotPersisted()
				}
				cm.Publish(nil, barrier.MustBuildMutable().WithTimeTick(50).IntoImmutableMessage(nil))
			}
			_, err := cm.WaitSnapshot(t.Context(), "v", 40)
			require.ErrorIs(t, err, merr.ErrChannelNotFound)
			require.Equal(t, map[string]QueryVChannelMVCC{"live": {50, 50}}, cm.frontiers)
			// Internal messages after Drop cannot recreate membership either.
			late := createQueryTestMessage(t, 60, "v", message.MessageTypeFlush, false, true)
			cm.ObserveAppend(late)
			publishQueryTestPrefix(t, cm, 70, late)
			require.NotContains(t, cm.frontiers, "v")
			require.Empty(t, cm.pending)
		})
	}
}

func TestQueryMVCCCreatePublishedBeforeCallback(t *testing.T) {
	cm := NewQueryMVCCManager(10)
	create := createQueryTestMessage(t, 20, "v", message.MessageTypeCreateCollection, false, true)
	insert := createQueryTestMessage(t, 30, "v", message.MessageTypeInsert, false, true)
	publishQueryTestPrefix(t, cm, 20, create)
	cm.ObserveAppend(insert)
	cm.ObserveAppend(create)
	require.EqualValues(t, 30, cm.Minimum("v", true))
	publishQueryTestPrefix(t, cm, 30, insert)
	current, ready := cm.Snapshot("v", 30)
	require.True(t, ready)
	require.Equal(t, QueryVChannelMVCC{30, 20}, current)
}

func TestQueryMVCCRestoreCreatesChannelAtPublishedPrefix(t *testing.T) {
	cm := NewQueryMVCCManager(10)
	restore := createQueryTestMessage(t, 20, "restored", message.MessageTypeRestoreSnapshot, false, true)
	cm.ObserveAppend(restore)
	_, ready := cm.Snapshot("restored", cm.Minimum("restored", false))
	require.False(t, ready)
	publishQueryTestPrefix(t, cm, 20, restore)
	current, err := cm.WaitSnapshot(t.Context(), "restored", 20)
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{20, 20}, current)
	insert := createQueryTestMessage(t, 30, "restored", message.MessageTypeInsert, false, true)
	publishQueryTestPrefix(t, cm, 30, insert)
	cm.ObserveAppend(restore)
	current, err = cm.WaitSnapshot(t.Context(), "restored", 30)
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{30, 20}, current)
}

func TestQueryMVCCMessageProjection(t *testing.T) {
	for _, kind := range []message.MessageType{message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeCommitTxn, message.MessageTypeFlush, message.MessageTypeManualFlush, message.MessageTypeCommitImport, message.MessageTypeCreateSnapshot, message.MessageTypeDropPartition, message.MessageTypeRestoreSnapshot} {
		t.Run(kind.String(), func(t *testing.T) {
			cm := NewQueryMVCCManager(100)
			cm.ApplyRecoveryBarrier("v", 100)
			msg := createQueryTestMessage(t, 150, "v", kind, kind == message.MessageTypeCommitTxn, true)
			cm.ObserveAppend(msg)
			require.EqualValues(t, 150, cm.Minimum("v", true))
			publishQueryTestPrefix(t, cm, 200, msg)
			got, ready := cm.Snapshot("v", 200)
			require.True(t, ready)
			expected := QueryVChannelMVCC{100, 150}
			if kind == message.MessageTypeInsert {
				expected = QueryVChannelMVCC{150, 100}
			}
			if kind == message.MessageTypeDelete || kind == message.MessageTypeCommitTxn || kind == message.MessageTypeRestoreSnapshot {
				expected = QueryVChannelMVCC{150, 150}
			}
			require.Equal(t, expected, got)
		})
	}
}

func TestQueryMVCCUncommittedAndGlobalPublication(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("v", 100)
	cm.ApplyRecoveryBarrier("other", 100)
	body := createQueryTestMessage(t, 150, "v", message.MessageTypeDelete, true, true)
	cm.ObserveAppend(body)
	require.EqualValues(t, 100, cm.Minimum("v", true))
	publishQueryTestPrefix(t, cm, 160, body)
	current, _ := cm.Snapshot("v", 160)
	require.Equal(t, QueryVChannelMVCC{100, 100}, current)
	global := createQueryTestMessage(t, 200, "", message.MessageTypeFlushAll, false, true)
	cm.ObserveAppend(global)
	require.EqualValues(t, 200, cm.Minimum("other", true))
	publishQueryTestPrefix(t, cm, 200, global)
	for _, channel := range []string{"v", "other"} {
		got, _ := cm.Snapshot(channel, 200)
		require.Equal(t, QueryVChannelMVCC{100, 200}, got)
	}
	require.Zero(t, cm.pendingAll)
}

func TestQueryMVCCWaitAndRecovery(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("v", 100)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cm.WaitSnapshot(canceled, "v", 200)
	require.ErrorIs(t, err, context.Canceled)
	// A new channel waits for ordered Create publication, not its append callback.
	create := createQueryTestMessage(t, 200, "new", message.MessageTypeCreateCollection, false, true)
	cm.ObserveAppend(create)
	_, ready := cm.Snapshot("new", cm.Minimum("new", false))
	require.False(t, ready)
	publishQueryTestPrefix(t, cm, 200, create)
	got, err := cm.WaitSnapshot(t.Context(), "new", 200)
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{200, 200}, got)
	// A recovered owner uses only live channels at its own complete opening fence.
	replacement := NewQueryMVCCManager(300)
	replacement.ApplyRecoveryBarrier("new", 300)
	got, err = replacement.WaitSnapshot(t.Context(), "new", replacement.Minimum("new", true))
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{300, 300}, got)
	_, err = replacement.WaitSnapshot(t.Context(), "v", 300)
	require.ErrorIs(t, err, merr.ErrChannelNotFound)
}

func queryMVCCMessages(messages []message.ImmutableMessage) func(func(message.ImmutableMessage, bool) bool) {
	return func(yield func(message.ImmutableMessage, bool) bool) {
		for _, msg := range messages {
			if !yield(msg, false) {
				return
			}
		}
	}
}
