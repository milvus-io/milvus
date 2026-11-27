package walsummary

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func newTestSplitShardCopy(vchannel string, timetick uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(timetick))
	return message.NewSplitShardMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: 1, SplitTaskId: 7, SourceVchannel: "p0_1v0", TargetVchannels: []string{"p1_1v1", "p2_1v2"},
		}).
		WithBody(&message.SplitShardMessageBody{}).MustBuildMutable().
		WithTimeTick(timetick).WithLastConfirmed(id).IntoImmutableMessage(id)
}

// nextTransformEventAt skips the initial progress of a subscription and
// returns the first event at or above timetick. It fails on an event above it.
func nextTransformEventAt(t *testing.T, h *recordingTransformHandler, timetick uint64) (entry bool) {
	t.Helper()
	for {
		event := nextTransformEvent(t, h)
		at := uint64(0)
		if event.Entry != nil {
			at = event.Entry.GetTimeTick()
		} else {
			require.NotNil(t, event.SyncUp)
			at = event.SyncUp.TimeTick
		}
		if at < timetick {
			continue
		}
		require.Equal(t, timetick, at)
		return event.Entry != nil
	}
}

// A reader that holds rows of the source of a shard split reads them at
// (T_switch, T_switch), and waits for the TransformLog stream of the source to
// reach T_switch. The fence carries no delete, so it must reach the subscriber
// as progress.
func TestDispatchStreamSplitShardFenceIsProgressAtTheSwitchTimeTick(t *testing.T) {
	const source = "p0_1v0"

	// A live subscriber: the last delete of the source, then the fence.
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: source, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, source, 90, 1, 5))
	require.True(t, nextTransformEventAt(t, h, 90), "the delete is an entry")
	m.ObserveMessage(t.Context(), newTestSplitShardCopy(source, 100))
	require.False(t, nextTransformEventAt(t, h, 100), "the fence is progress, not an entry")

	// A subscriber that arrives after the fence, as a QueryNode that loads a
	// segment of the source during the window does: its replay delivers the
	// delete and reaches the switch time tick as progress.
	late := newRecordingTransformHandler()
	lateSub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: source, Handler: late})
	require.NoError(t, err)
	defer lateSub.Close()
	require.True(t, nextTransformEventAt(t, late, 90))
	require.False(t, nextTransformEventAt(t, late, 100))
}

// The genesis of a target is the first event of its stream: a subscriber sees
// progress at the genesis tick, and a copy that is not addressed to a vchannel
// of the split moves no stream.
func TestDispatchStreamSplitShardGenesisIsProgressOnTheTarget(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	target, other := newRecordingTransformHandler(), newRecordingTransformHandler()
	targetSub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "p1_1v1", Handler: target})
	require.NoError(t, err)
	defer targetSub.Close()
	otherSub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "p1_1v9", Handler: other})
	require.NoError(t, err)
	defer otherSub.Close()

	m.ObserveMessage(t.Context(), newTestSplitShardCopy("p1_1v9", 110))
	m.ObserveMessage(t.Context(), newTestSplitShardCopy("p1_1v1", 120))
	require.False(t, nextTransformEventAt(t, target, 120))

	// The stream of the unrelated vchannel advances only with its own events:
	// the next thing it delivers at or above 110 is its own delete at 130.
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "p1_1v9", 130, 1, 5))
	for {
		event := nextTransformEvent(t, other)
		if event.Entry == nil {
			require.Less(t, event.SyncUp.TimeTick, uint64(110))
			continue
		}
		require.Equal(t, uint64(130), event.Entry.GetTimeTick())
		break
	}
}
