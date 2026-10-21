package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

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
	mockey.PatchConvey("persisted confirmation retains ordinary TimeTick consumption effects", t, func() {
		ordinary := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).To(func(_ *vchannel.PChannelRecoveryManager, _ context.Context, _ message.RetainedImmutableMessage) {
		}).Build()
		r := &recoveryStorageImpl{vchannelManager: &vchannel.PChannelRecoveryManager{}, summaryManager: walsummary.NewManager(walsummary.ManagerConfig{})}
		msg := message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"v"}}).WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(110).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		owner := message.NewOwnedImmutableMessage(msg, nil)
		defer owner.Release()
		retained := owner.Clone()
		defer retained.Release()
		r.observeModulesMessage(t.Context(), retained)
		require.Equal(t, 1, ordinary.Times(), "insert-only transaction waits still require complete-prefix confirmation")
		batch, err := r.summaryManager.ReadTransform(t.Context(), "v", 0, 110, walsummary.ReadLimits{})
		require.NoError(t, err)
		require.EqualValues(t, 110, batch.CoveredThrough, "durable complete-prefix coverage is still observed")
	})
}
