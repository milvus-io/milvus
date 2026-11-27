package autowalimpls

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

func TestRecoveryWriterFollowsObservedSwitches(t *testing.T) {
	mockey.PatchConvey("one auto owns checkpoint recovery and the final writer", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		history := New(p, open)
		first, err := history.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		toWoodpecker := alter(20, w.name, first)
		_, err = history.Append(ctx, toWoodpecker)
		require.NoError(t, err)
		woodpeckerStart, err := message.WALSwitchStart(toWoodpecker)
		require.NoError(t, err)
		_, err = history.Append(ctx, ordinary(30, woodpeckerStart))
		require.NoError(t, err)
		toPulsar := alter(40, p.name, woodpeckerStart)
		_, err = history.Append(ctx, toPulsar)
		require.NoError(t, err)
		pulsarStart, err := message.WALSwitchStart(toPulsar)
		require.NoError(t, err)
		_, err = history.Append(ctx, ordinary(50, pulsarStart))
		require.NoError(t, err)

		rwOpens := 0
		auto := New(p, func(ctx context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			if mode == types.AccessModeRW {
				rwOpens++
				require.Equal(t, w.name, name, "returning to Pulsar must reuse its writer")
			}
			return open(ctx, name, mode)
		})
		defer auto.Close()
		appendBarrier := func(tick uint64, start message.MessageID) message.MessageID {
			barrier := message.NewRecoveryBarrierMessageBuilderV2().
				WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).
				WithAllVChannel().MustBuildMutable().WithTimeTick(tick).WithLastConfirmed(start)
			id, err := auto.Append(ctx, barrier)
			require.NoError(t, err)
			require.Equal(t, start.WALName(), id.WALName())
			return id
		}
		oldProbe := appendBarrier(100, first)
		s, err := auto.Read(ctx, walimpls.ReadOption{Name: "recovery", DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		for _, tick := range []uint64{10, 20} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		require.Equal(t, p.name, auto.WALName(), "reader switching must not select a writer")
		require.NoError(t, auto.SwitchWriterForRecovery(ctx, w.name))
		appendBarrier(101, woodpeckerStart)
		for _, tick := range []uint64{30, 40} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		require.Equal(t, w.name, auto.WALName())
		require.NoError(t, auto.SwitchWriterForRecovery(ctx, p.name))
		finalProbe := appendBarrier(102, pulsarStart)
		require.Equal(t, uint64(50), receive(t, s).TimeTick())
		require.True(t, oldProbe.EQ(receive(t, s).MessageID()), "a prior probe may reappear after returning to its backend")
		require.True(t, finalProbe.EQ(receive(t, s).MessageID()), "only the newest exact probe ends recovery")
		require.Equal(t, 1, rwOpens)
		_, err = auto.Append(ctx, ordinary(103, finalProbe))
		require.NoError(t, err)
		require.Equal(t, uint64(103), receive(t, s).TimeTick(), "the same auto continues live reads and writes")
	})
}

func TestRecoveryWriterSelectionFailure(t *testing.T) {
	mockey.PatchConvey("failed acquisition keeps the current writer and creates no link", t, func() {
		p, w, _ := installBackends(t)
		opens := 0
		auto := New(p, func(_ context.Context, _ message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			require.Equal(t, types.AccessModeRW, mode)
			opens++
			if opens == 1 {
				return nil, context.DeadlineExceeded
			}
			return w, nil
		})
		defer auto.Close()
		require.ErrorIs(t, auto.SwitchWriterForRecovery(context.Background(), w.name), context.DeadlineExceeded)
		require.Equal(t, p.name, auto.WALName())
		require.NoError(t, auto.SwitchWriterForRecovery(context.Background(), w.name))
		require.NoError(t, auto.SwitchWriterForRecovery(context.Background(), w.name))
		require.Equal(t, 2, opens)
		require.Empty(t, p.messages)
		require.Empty(t, w.messages, "selecting a recovery writer must not append another anchor or AlterWAL")
	})
}
