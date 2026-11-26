package adaptor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryMVCCWaitsForCompletePrefix(t *testing.T) {
	resource.InitForTest(t)
	for _, confirmed := range []bool{false, true} {
		t.Run(strconv.FormatBool(confirmed), func(t *testing.T) {
			mockey.PatchConvey("independent append maxima are not a query snapshot", t, func() {
				manager := mvcc.NewMVCCManager(100)
				manager.ApplyRecoveryBarrier("v", 100)

				msg := func(ts uint64, kind message.MessageType) message.MutableMessage {
					return message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.FormatInt(int64(kind), 10), "_vc": "v"}).WithTimeTick(ts)
				}
				manager.QueryMVCC().Publish(nil, 100, nil)
				insert := msg(200, message.MessageTypeInsert)
				manager.UpdateMVCC(insert)
				if confirmed {
					// Append-side confirmation can precede publication of its projection.
					manager.UpdateMVCC(message.NewTimeTickMessageBuilderV1().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).WithAllVChannel().MustBuildMutable().WithTimeTick(200))
				}
				channel := types.PChannelInfo{Name: "query-mvcc-prefix", AccessMode: types.AccessModeRW}
				mockey.Mock((*roWALAdaptorImpl).Channel).Return(channel).Build()
				syncing := make(chan struct{}, 1)
				syncMock := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).To(func(got types.PChannelInfo, persisted bool) {
					require.Equal(t, channel, got)
					require.False(t, persisted)
					syncing <- struct{}{}
				}).Build()
				w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context(), lifetime: typeutil.NewLifetime()}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				type result struct {
					plan *viewpb.QueryPlanMVCC
					err  error
				}
				done := make(chan result, 1)
				go func() { plan, err := w.GetLatestQueryPlanMVCC(ctx, "v"); done <- result{plan, err} }()
				select {
				case <-syncing:
				case <-ctx.Done():
					t.Fatal("query did not request prefix publication")
				}
				// Publish the missing Delete before the Insert in WAL order. A newer
				// append remains outside this prefix and must not raise the fixed wait target.
				deletion := msg(150, message.MessageTypeDelete)
				manager.UpdateMVCC(deletion)
				later := msg(250, message.MessageTypeInsert)
				manager.UpdateMVCC(later)
				manager.QueryMVCC().Publish(queryMVCCMessages([]message.ImmutableMessage{deletion.IntoImmutableMessage(nil), insert.IntoImmutableMessage(nil)}), 200, nil)
				got := <-done
				require.NoError(t, got.err)
				require.EqualValues(t, 200, got.plan.GetGrowingTimetick())
				require.EqualValues(t, 150, got.plan.GetTransformingTimetick())
				require.Equal(t, 1, syncMock.Times())
				// Subsequent Growing-only progress preserves the QueryNode wait target.
				manager.QueryMVCC().Publish(queryMVCCMessages([]message.ImmutableMessage{later.IntoImmutableMessage(nil)}), 300, nil)
				next, err := w.GetLatestQueryPlanMVCC(ctx, "v")
				require.NoError(t, err)
				require.EqualValues(t, 250, next.GetGrowingTimetick())
				require.EqualValues(t, 150, next.GetTransformingTimetick())
				require.Equal(t, 1, syncMock.Times(), "an adequate published prefix requires no sync")
			})
		})
	}
}

func TestQueryMVCCPrefixWaitCancellation(t *testing.T) {
	resource.InitForTest(t)
	for _, ownerShutdown := range []bool{false, true} {
		t.Run(strconv.FormatBool(ownerShutdown), func(t *testing.T) {
			mockey.PatchConvey("prefix waits must release the WAL lifetime", t, func() {
				manager := mvcc.NewMVCCManager(100)
				manager.ApplyRecoveryBarrier("v", 100)
				manager.UpdateMVCC(message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.FormatInt(int64(message.MessageTypeInsert), 10), "_vc": "v"}).WithTimeTick(200))
				ownerCtx, stopOwner := context.WithCancel(t.Context())
				defer stopOwner()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
				mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).To(func(types.PChannelInfo, bool) {
					if ownerShutdown {
						stopOwner()
					} else {
						cancel()
					}
				}).Build()
				w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: ownerCtx, lifetime: typeutil.NewLifetime()}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}
				plan, err := w.GetLatestQueryPlanMVCC(ctx, "v")
				require.Nil(t, plan)
				if ownerShutdown {
					ve := viewerror.AsViewError(err)
					require.NotNil(t, ve)
					require.True(t, ve.IsOnShutdown())
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				_, err = w.GetLatestQueryPlanMVCC(t.Context(), "unknown")
				require.True(t, viewerror.AsViewError(err).IsViewNotFound())
			})
		})
	}
}

func TestQueryMVCCConsistencySelectsCompleteSnapshots(t *testing.T) {
	resource.InitForTest(t)
	mockey.PatchConvey("all consistency levels sample completed writes on the writer", t, func() {
		manager := mvcc.NewMVCCManager(100)
		manager.ApplyRecoveryBarrier("v", 100)
		insert := message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.FormatInt(int64(message.MessageTypeInsert), 10), "_vc": "v"}).WithTimeTick(200)
		manager.UpdateMVCC(insert)
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		syncs := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "TriggerSync")).To(func(types.PChannelInfo, bool) {
			tick := message.NewTimeTickMessageBuilderV1().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).WithAllVChannel().WithNotPersisted().MustBuildMutable().WithTimeTick(200)
			manager.QueryMVCC().Publish(queryMVCCMessages([]message.ImmutableMessage{insert.IntoImmutableMessage(nil)}), tick.TimeTick(), nil)
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context(), lifetime: typeutil.NewLifetime()}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}
		for _, level := range []commonpb.ConsistencyLevel{commonpb.ConsistencyLevel_Bounded, commonpb.ConsistencyLevel_Session, commonpb.ConsistencyLevel_Eventually, commonpb.ConsistencyLevel_Strong} {
			req := &viewpb.GetQueryPlanRequest{
				Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: level},
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{GuaranteeTimestamp: 300}},
			}
			got, err := w.resolveQueryPlanMVCC(t.Context(), req, "v")
			require.NoError(t, err)
			require.EqualValues(t, 200, got.GrowingTimetick)
			require.EqualValues(t, 100, got.TransformingTimetick)
			require.Equal(t, 1, syncs.Times())
		}
	})
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
