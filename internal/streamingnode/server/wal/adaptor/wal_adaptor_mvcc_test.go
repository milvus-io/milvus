package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	_ "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRetainedMVCCTargetAndPhysicalMetrics(t *testing.T) {
	mockey.PatchConvey("confirmed retained targets do not request sync", t, func() {
		resource.InitForTest(t)
		cm := mvcc.NewMVCCManager(100)
		cm.ApplyRecoveryBarrier("v1", 100)
		opener, err := registry.MustGetBuilder(message.WALNameTest).Build()
		require.NoError(t, err)
		defer opener.Close()
		inner, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: types.PChannelInfo{Name: "retained-mvcc-test", Term: 1, AccessMode: types.AccessModeRW}})
		require.NoError(t, err)
		defer inner.Close()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{roWALImpls: inner, lifetime: typeutil.NewLifetime()}, rwWALImpls: inner, param: &interceptors.InterceptorBuildParam{MVCCManager: cm}}
		syncs := 0
		mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).To(func(channel types.PChannelInfo, persist bool) {
			require.Equal(t, inner.Channel(), channel)
			require.False(t, persist)
			syncs++
		}).Build()
		write := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&message.InsertRequest{}).MustBuildMutable().WithTimeTick(120)
		cm.UpdateMVCC(write)
		ts, err := w.GetLatestMVCCTimestamp(context.Background(), "v1")
		require.NoError(t, err)
		require.Equal(t, uint64(120), ts)
		require.Equal(t, 1, syncs)
		tick := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).MustBuildMutable().WithTimeTick(300)
		cm.UpdateMVCC(tick)
		for i := 0; i < 1000; i++ {
			ts, err = w.GetLatestMVCCTimestamp(context.Background(), "v1")
			require.NoError(t, err)
			require.Equal(t, uint64(120), ts)
		}
		require.Equal(t, 1, syncs)
		require.Equal(t, uint64(300), w.Metrics().(types.RWWALMetrics).MVCCTimeTick)
		cm.UpdateMVCC(tick.WithTimeTick(400))
		require.Equal(t, uint64(400), w.Metrics().(types.RWWALMetrics).MVCCTimeTick)
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.GetLatestMVCCTimestamp(context.Background(), "v1")
		require.Error(t, err)
	})
}
