package recovery

import (
	"context"
	"testing"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type physicalRecoveryBuilder struct{ recordingRecoveryStreamBuilder }

func (*physicalRecoveryBuilder) Build(BuildRecoveryStreamParam) RecoveryStream { panic("mockey") }
func (*physicalRecoveryBuilder) SwitchWAL(context.Context, message.MessageID) (message.ImmutableMessage, error) {
	panic("mockey")
}

func recoveryLeg(t *testing.T, msgs ...message.ImmutableMessage) *blockingRecoveryStream {
	t.Helper()
	s := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, len(msgs)), closed: make(chan struct{})}
	for _, m := range msgs {
		s.ch <- m
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func recoveryBoundary(tick uint64, id, start message.MessageID) message.ImmutableMessage {
	m := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName(start.WALName())}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID()
	message.SetWALSwitchStart(m, start)
	return m.IntoImmutableMessage(id)
}

func recoveryProbe(tick uint64, id message.MessageID) message.ImmutableMessage {
	return message.NewRecoveryBarrierMessageBuilderV2().WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
}

func TestRecoveryStorageOwnsPhysicalWALTraversal(t *testing.T) {
	mockey.PatchConvey("revisit ignores old probes and completes only at the final leg barrier", t, func() {
		resource.InitForTest(t)
		testID := func(n int64) message.MessageID { return pulsar.NewPulsarID(pulsarclient.NewMessageID(1, n, 0, 0)) }
		wpID := func(n int64) message.MessageID { return wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: n}) }
		rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: testID(1), TimeTick: 10})
		defer rs.metrics.Close()
		initial := recoveryProbe(100, testID(10))
		middle := recoveryProbe(110, wpID(10))
		final := recoveryProbe(120, testID(11))
		first := recoveryLeg(t, recoveryBoundary(20, testID(2), wpID(0)), initial)
		second := recoveryLeg(t, recoveryBoundary(40, wpID(2), testID(3)), middle)
		// The initial probe belongs to the revisited backend and is now ordinary history.
		last := recoveryLeg(t, initial, final)
		legs := []*blockingRecoveryStream{first, second, last}
		var params []BuildRecoveryStreamParam
		var starts []message.MessageID
		mockey.Mock((*physicalRecoveryBuilder).Build).To(func(_ *physicalRecoveryBuilder, p BuildRecoveryStreamParam) RecoveryStream {
			params = append(params, p)
			return legs[len(params)-1]
		}).Build()
		mockey.Mock((*physicalRecoveryBuilder).SwitchWAL).To(func(_ *physicalRecoveryBuilder, _ context.Context, start message.MessageID) (message.ImmutableMessage, error) {
			select {
			case <-legs[len(starts)].closed:
			default:
				t.Fatal("previous leg must close before acquiring next writer")
			}
			starts = append(starts, start)
			return []message.ImmutableMessage{middle, final}[len(starts)-1], nil
		}).Build()
		snapshot, err := rs.runBoundedRecovery(context.Background(), &physicalRecoveryBuilder{}, initial)
		require.NoError(t, err)
		require.Same(t, final, snapshot.RecoveryBarrier)
		require.Same(t, last, rs.recoveryStream)
		require.Equal(t, uint64(120), snapshot.Checkpoint.TimeTick)
		require.Len(t, params, 3)
		require.False(t, params[0].StartAfterCheckpoint)
		for i := 1; i < 3; i++ {
			require.True(t, params[i].StartAfterCheckpoint)
			require.True(t, params[i].StopAtWALSwitch)
			require.True(t, starts[i-1].EQ(params[i].StartCheckpoint))
		}
		require.Len(t, first.ch, 1, "old leg's probe must not be replayed there")
		require.Len(t, second.ch, 1)
	})
}

func TestPhysicalRecoveryFailsClosed(t *testing.T) {
	for _, scenario := range []string{"open_failed", "corrupt_link", "cycle", "no_switcher"} {
		t.Run(scenario, func(t *testing.T) {
			mockey.PatchConvey(scenario, t, func() {
				resource.InitForTest(t)
				rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
				defer rs.metrics.Close()
				id := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
				boundary := recoveryBoundary(20, walimplstest.NewTestMessageID(2), id)
				if scenario == "corrupt_link" {
					header := message.MustAsImmutableAlterWALMessageV2(boundary).Header()
					header.TargetStartMessageId.Id = "invalid"
					encoded, err := message.EncodeProto(header)
					require.NoError(t, err)
					boundary.Properties().ToRawMap()["_h"] = encoded
				}
				barrier := recoveryProbe(100, walimplstest.NewTestMessageID(10))
				stream := recoveryLeg(t, boundary)
				var builder RecoveryStreamBuilder = &physicalRecoveryBuilder{}
				mockey.Mock((*physicalRecoveryBuilder).Build).Return(stream).Build()
				mockey.Mock((*physicalRecoveryBuilder).SwitchWAL).To(func(context.Context, message.MessageID) (message.ImmutableMessage, error) {
					if scenario == "open_failed" {
						return nil, context.DeadlineExceeded
					}
					stream.ch <- boundary
					return barrier, nil
				}).Build()
				if scenario == "no_switcher" {
					builder = &recordingRecoveryStreamBuilder{stream: stream}
				}
				snapshot, err := rs.runBoundedRecovery(context.Background(), builder, barrier)
				require.Error(t, err)
				require.Nil(t, snapshot)
				if scenario == "open_failed" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
				if scenario == "cycle" {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				}
			})
		})
	}
}
