package adaptor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func queryMVCCTestMessage(ts uint64, kind message.MessageType) message.MutableMessage {
	return message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.FormatInt(int64(kind), 10), "_vc": "v"}).WithTimeTick(ts)
}

func TestGetMVCCTimestampPrefixOnly(t *testing.T) {
	resource.InitForTest(t)
	mockey.PatchConvey("prefix_only samples through and the Strong minimum without waiting or syncing", t, func() {
		manager := mvcc.NewMVCCManager(100)
		manager.ApplyRecoveryBarrier("v", 100)
		manager.QueryMVCC().Publish(nil, 125, nil)
		// A delete at 127 completed but is not published: through stays at 125.
		manager.UpdateMVCC(queryMVCCTestMessage(127, message.MessageTypeDelete))

		accessMode := types.AccessModeRW
		mockey.Mock((*roWALAdaptorImpl).Channel).To(func(*roWALAdaptorImpl) types.PChannelInfo {
			return types.PChannelInfo{Name: "mvcc-prefix", AccessMode: accessMode}
		}).Build()
		syncMock := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).Return().Build()
		lifetime := typeutil.NewLifetime()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context(), lifetime: lifetime}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}

		resp, err := w.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{Vchannel: "v", PrefixOnly: true})
		require.NoError(t, err)
		require.EqualValues(t, 125, resp.GetThrough())
		require.EqualValues(t, 127, resp.GetStrongMinimum())
		require.EqualValues(t, 100, resp.GetMvcc().GetGrowingTimetick())
		require.EqualValues(t, 100, resp.GetMvcc().GetTransformingTimetick())
		require.Zero(t, syncMock.Times(), "sampling the prefix must not force a sync")

		_, err = w.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{Vchannel: "absent", PrefixOnly: true})
		require.True(t, viewerror.AsViewError(err).IsViewNotFound())
		_, err = w.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{PrefixOnly: true})
		require.Error(t, err)

		// The primary-only rule is the one the request had before.
		accessMode = types.AccessModeRO
		_, err = w.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{Vchannel: "v", PrefixOnly: true})
		require.True(t, viewerror.AsViewError(err).IsNotPrimary())
		accessMode = types.AccessModeRW

		lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{Vchannel: "v", PrefixOnly: true})
		require.True(t, viewerror.AsViewError(err).IsOnShutdown())
	})
}

func TestGetMVCCTimestampReturnsThroughAndStrongMinimum(t *testing.T) {
	resource.InitForTest(t)
	mockey.PatchConvey("the waiting request keeps its behavior and also reports the prefix", t, func() {
		manager := mvcc.NewMVCCManager(100)
		manager.ApplyRecoveryBarrier("v", 100)
		manager.QueryMVCC().Publish(nil, 125, nil)
		deletion := queryMVCCTestMessage(127, message.MessageTypeDelete)
		manager.UpdateMVCC(deletion)

		mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{Name: "mvcc-wait", AccessMode: types.AccessModeRW}).Build()
		syncing := make(chan struct{}, 1)
		mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).To(func(types.PChannelInfo, bool) {
			syncing <- struct{}{}
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context(), lifetime: typeutil.NewLifetime()}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		type result struct {
			resp *viewpb.GetMVCCTimestampResponse
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := w.GetMVCCTimestamp(ctx, &viewpb.GetMVCCTimestampRequest{Vchannel: "v"})
			done <- result{resp, err}
		}()
		select {
		case <-syncing:
		case <-ctx.Done():
			t.Fatal("the waiting request did not ask for publication of the Strong minimum")
		}
		manager.QueryMVCC().Publish(queryMVCCMessages([]message.ImmutableMessage{deletion.IntoImmutableMessage(nil)}), 135, nil)
		got := <-done
		require.NoError(t, got.err)
		require.EqualValues(t, 127, got.resp.GetMvcc().GetGrowingTimetick())
		require.EqualValues(t, 127, got.resp.GetMvcc().GetTransformingTimetick())
		require.EqualValues(t, 135, got.resp.GetThrough())
		require.EqualValues(t, 135, got.resp.GetStrongMinimum())
	})
}

// The proxy hands the source shard of a lineage group a ready-made MVCC that
// also names the read point and the lineage ticks. The plan must carry it
// unchanged: Phase 2 on the QueryNodes depends on those fields.
func TestProxyProvidedLineageMVCCIsUsedAsIs(t *testing.T) {
	provided := &viewpb.QueryPlanMVCC{
		GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135,
		LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t2": 130},
	}
	w := &walAdaptorImpl{}
	got, err := w.resolveQueryPlanMVCC(t.Context(), &viewpb.GetQueryPlanRequest{
		Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: provided},
	}, "s")
	require.NoError(t, err)
	require.Same(t, provided, got)
}
