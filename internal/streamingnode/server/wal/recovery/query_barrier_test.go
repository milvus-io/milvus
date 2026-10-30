package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestMVCCBarrierConsumptionIsPassiveAndScoped(t *testing.T) {
	mockey.PatchConvey("consume without write-side services or durable state", t, func() {
		module := &vchannel.VChannelRecoveryModule{}
		mockey.Mock((*vchannel.PChannelRecoveryManager).Module).To(func(_ *vchannel.PChannelRecoveryManager, channel string) *vchannel.VChannelRecoveryModule {
			require.Contains(t, []string{"v", "v2"}, channel)
			return module
		}).Build()
		observed := mockey.Mock((*vchannel.VChannelRecoveryModule).ObserveQueryBarrier).To(func(_ *vchannel.VChannelRecoveryModule, _ context.Context, tick uint64) bool {
			require.EqualValues(t, 110, tick)
			return true
		}).Build()
		r := &recoveryStorageImpl{vchannelManager: &vchannel.PChannelRecoveryManager{}, summaryManager: walsummary.NewManager(walsummary.ManagerConfig{}), dirtyCounter: 7}
		msg := message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"v", "v2"}}).WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().WithNotPersisted().MustBuildMutable().WithTimeTick(110).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		released := false
		r.consumedThrough = func(tick uint64) {
			require.EqualValues(t, 110, tick)
			require.Equal(t, 2, observed.Times(), "resource events precede WAB release")
			batch, err := r.summaryManager.ReadTransform(t.Context(), "v", 0, tick, walsummary.ReadLimits{})
			require.NoError(t, err)
			require.Equal(t, tick, batch.CoveredThrough, "scoped Summary coverage precedes WAB release")
			released = true
		}
		r.observeMessage(t.Context(), msg)
		require.True(t, released)
		require.Equal(t, 7, r.dirtyCounter)
		require.Equal(t, 2, observed.Times())
		batch, err := r.summaryManager.ReadTransform(t.Context(), "v", 0, 100, walsummary.ReadLimits{})
		require.NoError(t, err)
		require.EqualValues(t, 100, batch.CoveredThrough)
		other, err := r.summaryManager.ReadTransform(t.Context(), "other", 0, 100, walsummary.ReadLimits{})
		require.NoError(t, err)
		require.Zero(t, other.CoveredThrough)
	})
}

func TestPersistedMVCCBarrierKeepsCompletePrefixDispatch(t *testing.T) {
	mockey.PatchConvey("persisted MVCCBarrier retains complete-prefix resource effects", t, func() {
		ordinary := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).To(func(_ *vchannel.PChannelRecoveryManager, _ context.Context, _ message.RetainedImmutableMessage) {
		}).Build()
		r := &recoveryStorageImpl{vchannelManager: &vchannel.PChannelRecoveryManager{}, summaryManager: walsummary.NewManager(walsummary.ManagerConfig{})}
		msg := message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"v"}}).WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(110).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		owner := message.NewOwnedImmutableMessage(msg, nil)
		defer owner.Release()
		retained := owner.Clone()
		defer retained.Release()
		r.observeModulesMessage(t.Context(), retained)
		require.Equal(t, 1, ordinary.Times(), "MVCCBarrier still advances resource consumers")
		batch, err := r.summaryManager.ReadTransform(t.Context(), "v", 0, 110, walsummary.ReadLimits{})
		require.NoError(t, err)
		require.EqualValues(t, 110, batch.CoveredThrough, "durable complete-prefix coverage is still observed")
	})
}

func TestTimeTickAdvancesRecoveryWithoutVChannelDispatch(t *testing.T) {
	mockey.PatchConvey("ordinary TimeTick remains PChannel progress", t, func() {
		dispatch := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).To(func(_ *vchannel.PChannelRecoveryManager, _ context.Context, _ message.RetainedImmutableMessage) {}).Build()
		r := newTestRecoveryStorage(t, &utility.WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10,
		})
		t.Cleanup(r.metrics.Close)
		t.Cleanup(r.taskScheduler.Close)
		t.Cleanup(r.broadcastAck.Close)
		r.summaryManager = walsummary.NewManager(walsummary.ManagerConfig{})
		r.summaryManager.InitLastAcked(10)
		r.observeMessage(t.Context(), newAckTestTimeTickMessage(t, 20, 2))
		require.Zero(t, dispatch.Times())
		require.EqualValues(t, 20, r.summaryManager.LastAcked())
		require.EqualValues(t, 20, r.ackTracker.CompletedPoint().TimeTick)
		checkpoint, _ := r.ackTracker.CheckpointThrough(r.summaryManager.LastAcked())
		require.EqualValues(t, 20, checkpoint.TimeTick)
		require.True(t, checkpoint.MessageID.EQ(walimplstest.NewTestMessageID(2)))
	})
}

func TestEmptyTransactionAdvancesRecoveryWithoutVChannelDispatch(t *testing.T) {
	mockey.PatchConvey("historical empty commit has no query resource effects", t, func() {
		dispatch := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).To(func(_ *vchannel.PChannelRecoveryManager, _ context.Context, _ message.RetainedImmutableMessage) {}).Build()
		r := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
		t.Cleanup(r.metrics.Close)
		t.Cleanup(r.taskScheduler.Close)
		t.Cleanup(r.broadcastAck.Close)
		r.summaryManager = walsummary.NewManager(walsummary.ManagerConfig{})
		r.summaryManager.InitLastAcked(10)
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v").
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().WithTimeTick(11).WithTxnContext(message.TxnContext{TxnID: 1}).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v").
			WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().WithTimeTick(20).WithTxnContext(message.TxnContext{TxnID: 1}).
			WithLastConfirmed(walimplstest.NewTestMessageID(2)).IntoImmutableMessage(walimplstest.NewTestMessageID(3))
		empty, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		require.NoError(t, err)
		r.observeMessage(t.Context(), empty)
		require.Zero(t, dispatch.Times())
		require.EqualValues(t, 20, r.ackTracker.CompletedPoint().TimeTick)
		batch, err := r.summaryManager.ReadTransform(t.Context(), "v", 0, 20, walsummary.ReadLimits{})
		require.NoError(t, err)
		require.Empty(t, batch.Entries)
		require.EqualValues(t, 20, batch.CoveredThrough)
	})
}
