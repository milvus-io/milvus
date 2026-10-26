package wab

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPressureProportionalToExcess(t *testing.T) {
	for _, tc := range []struct {
		size  int
		state streamingpb.WALRateLimitState
		rate  int64
	}{
		{0, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL, 0},
		{100, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL, 0},
		{125, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, 750},
		{150, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, 500},
		{175, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, 250},
		{200, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, 0},
		{300, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, 0},
	} {
		require.Equal(t, ratelimit.RateLimitState{State: tc.state, Rate: tc.rate}, pressureState(tc.size, 100, 1000, 2))
	}
}

func TestPressureReleasesOnlyItsOwnSource(t *testing.T) {
	mockey.PatchConvey("queue updates drive immediate pressure without mode switches or timers", t, func() {
		registry := ratelimit.NewMuxRateLimitObserverRegistry()
		var state ratelimit.RateLimitState
		mockey.Mock((*mock_ratelimit.MockRateLimitObserver).UpdateRateLimitState).To(func(_ *mock_ratelimit.MockRateLimitObserver, s ratelimit.RateLimitState) { state = s }).Build()
		observer := &mock_ratelimit.MockRateLimitObserver{}
		registry.Register(observer)
		defer registry.Unregister(observer)
		one := createTimeTickMessage(10, true)
		w := NewWriteAheadBuffer("pressure", mlog.With(), 2*one.EstimateSize(), 0, createTimeTickMessage(1, true))
		defer w.Close()
		w.ReleaseThrough(1)
		paramtable.Init()
		params := paramtable.Get()
		for key, value := range map[string]string{
			params.StreamingCfg.WALRateLimitWABSlowdownRatio.Key: "1",
			params.StreamingCfg.WALRateLimitWABSlowdownRate.Key:  "1000",
			params.StreamingCfg.WALRateLimitWABRejectRatio.Key:   "2",
		} {
			require.NoError(t, params.Save(key, value))
			t.Cleanup(func() { _ = params.Reset(key) })
		}
		w.SetPressureController(registry)
		for _, tick := range []uint64{10, 20, 30} {
			msg := createTimeTickMessage(tick, true)
			require.Equal(t, one.EstimateSize(), msg.EstimateSize())
			w.Append(nil, msg)
		}
		require.Equal(t, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, state.State)
		require.EqualValues(t, 500, state.Rate)
		fourth := createTimeTickMessage(35, true)
		require.Equal(t, one.EstimateSize(), fourth.EstimateSize())
		w.Append(nil, fourth)
		require.True(t, registry.IsRejected())
		// Confirmation publication must continue even while production is rejected.
		w.Append(nil, createTimeTickMessage(50, false))
		require.Len(t, w.pendingMessages.buf, 4)
		w.ReleaseThrough(10)
		require.EqualValues(t, 500, state.Rate)
		registry.NotifySourceRateLimitState("recovery-tail", ratelimit.RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT})
		w.ReleaseThrough(50)
		require.True(t, registry.IsRejected(), "draining WAB cannot clear persistence pressure")
		registry.NotifySourceRateLimitState("recovery-tail", ratelimit.NewNormalRateLimitState())
		require.Equal(t, ratelimit.NewNormalRateLimitState(), state)
	})
}

func TestRecoveryAcknowledgesBeforeWABPublication(t *testing.T) {
	w := NewWriteAheadBuffer("late-publication", mlog.With(), 1, 0, createTimeTickMessage(1, true))
	defer w.Close()
	w.ReleaseThrough(20) // Durable catchup can lead the serialized WAB append.
	w.Append([]message.ImmutableMessage{createInsertMessage(10)}, createTimeTickMessage(20, true))
	require.Zero(t, w.pendingMessages.Size())
	require.EqualValues(t, 20, w.lastPersistedTimeTickMessage.TimeTick())
	reader, err := w.ReadFromExclusiveTimeTick(t.Context(), 20)
	require.NoError(t, err)
	w.Append(nil, createMVCCBarrier(30, "a"))
	msg, err := reader.Next(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 30, msg.TimeTick())
}

func TestTenThousandScopesStayOrderedUnderPressure(t *testing.T) {
	w := NewWriteAheadBuffer("many-scopes", mlog.With(), 1024, 0, createTimeTickMessage(1, true))
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 1)
	require.NoError(t, err)
	for batch := 0; batch < 10; batch++ {
		channels := make([]string, 1000)
		for i := range channels {
			channels[i] = fmt.Sprintf("p_%dv0", batch*1000+i)
		}
		w.Append(nil, createMVCCBarrier(uint64(10+batch), channels...))
	}
	w.Append(nil, createTimeTickMessage(20, true))
	require.Greater(t, w.pendingMessages.Size(), 1024)
	for batch := 0; batch < 10; batch++ {
		msg, err := reader.Next(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 10+batch, msg.TimeTick())
		channels := message.MustAsImmutableMVCCBarrierMessageV2(msg).Header().GetVchannels()
		require.Len(t, channels, 1000)
		require.Equal(t, fmt.Sprintf("p_%dv0", batch*1000), channels[0])
		w.ReleaseThrough(msg.TimeTick())
	}
	require.Equal(t, createTimeTickMessage(20, true).EstimateSize(), w.pendingMessages.Size())
}

func TestWABCloseWakesBlockedReaders(t *testing.T) {
	w := NewWriteAheadBuffer("close", mlog.With(), 1024, 0, createTimeTickMessage(1, true))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 1)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for _, read := range []func() error{
		func() error { _, err := reader.Next(ctx); return err },
		func() error { _, err := w.ReadFromExclusiveTimeTick(ctx, 100); return err },
	} {
		wg.Add(1)
		go func() { defer wg.Done(); require.ErrorIs(t, read(), ErrClosed) }()
	}
	w.Close()
	wg.Wait()
	w.ReleaseThrough(100)
	w.Append(nil, createMVCCBarrier(200, "a"))
	w.Close()
}

func TestPressureConfigurationTakesEffectWithoutTraffic(t *testing.T) {
	mockey.PatchConvey("live config re-evaluates an idle queue and Close detaches its watcher", t, func() {
		paramtable.Init()
		params := paramtable.Get()
		cfg := &params.StreamingCfg
		one := createTimeTickMessage(10, true)
		threshold := 2 * one.EstimateSize()
		for key, value := range map[string]string{
			cfg.WALRateLimitWABSlowdownRatio.Key: "1",
			cfg.WALRateLimitWABSlowdownRate.Key:  "1000",
			cfg.WALRateLimitWABRejectRatio.Key:   "2",
		} {
			require.NoError(t, params.Save(key, value))
			t.Cleanup(func() { _ = params.Reset(key) })
		}
		registry := ratelimit.NewMuxRateLimitObserverRegistry()
		var state ratelimit.RateLimitState
		updates := 0
		mockey.Mock((*mock_ratelimit.MockRateLimitObserver).UpdateRateLimitState).To(func(_ *mock_ratelimit.MockRateLimitObserver, s ratelimit.RateLimitState) {
			state = s
			updates++
		}).Build()
		observer := &mock_ratelimit.MockRateLimitObserver{}
		registry.Register(observer)
		defer registry.Unregister(observer)
		// The slowdown threshold is derived from this WAB instance's capacity.
		w := NewWriteAheadBuffer("live-config", mlog.With(), threshold, 0, createTimeTickMessage(1, true))
		defer w.Close()
		w.SetPressureController(registry)
		for _, tick := range []uint64{10, 20, 30} {
			w.Append(nil, createTimeTickMessage(tick, true))
		}
		require.EqualValues(t, 500, state.Rate)
		// No append, consume, or periodic tick occurs during these updates.
		require.NoError(t, params.Save(cfg.WALRateLimitWABRejectRatio.Key, "3"))
		require.EqualValues(t, 750, state.Rate)
		require.NoError(t, params.Save(cfg.WALRateLimitWABRejectRatio.Key, "1.4"))
		require.True(t, registry.IsRejected())
		require.NoError(t, params.Save(cfg.WALRateLimitWABSlowdownRate.Key, "2000"))
		require.NoError(t, params.Save(cfg.WALRateLimitWABRejectRatio.Key, "2"))
		require.EqualValues(t, 1000, state.Rate)
		require.NoError(t, params.Save(cfg.WALRateLimitWABSlowdownRatio.Key, "2"))
		require.Equal(t, ratelimit.NewNormalRateLimitState(), state)
		for _, invalid := range []string{"0", "1", "NaN", "+Inf", "invalid"} {
			require.NoError(t, params.Save(cfg.WALRateLimitWABRejectRatio.Key, invalid))
			require.EqualValues(t, 2, cfg.WALRateLimitWABRejectRatio.GetAsFloat())
		}
		for _, invalid := range []string{"0", "-1", "0.5", "NaN", "+Inf", "invalid"} {
			require.NoError(t, params.Save(cfg.WALRateLimitWABSlowdownRatio.Key, invalid))
			require.EqualValues(t, 1, cfg.WALRateLimitWABSlowdownRatio.GetAsFloat())
			require.EqualValues(t, 1000, state.Rate)
		}
		require.NoError(t, params.Save(cfg.WALRateLimitWABSlowdownRatio.Key, "1.5"))
		require.Equal(t, ratelimit.NewNormalRateLimitState(), state)
		w.Close()
		before := updates
		require.NoError(t, params.Save(cfg.WALRateLimitWABSlowdownRatio.Key, "1"))
		require.Equal(t, before, updates, "closed owners must detach config callbacks")
	})
}
