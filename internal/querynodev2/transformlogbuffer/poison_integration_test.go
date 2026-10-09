package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type poisonLoadInfoStream struct{ qnview.SegmentLoadInfoStream }

func (*poisonLoadInfoStream) Subscribe(qnview.SegmentLoadInfoSubscriptionOption) qnview.SegmentLoadInfoSubscription {
	panic("mockey")
}

type poisonLoadInfoSubscription struct {
	qnview.SegmentLoadInfoSubscription
}

func (*poisonLoadInfoSubscription) Close() {}

type poisonPhysicalLoader struct{ qnview.PhysicalSegmentLoader }

func (*poisonPhysicalLoader) Load(context.Context, *querypb.SegmentLoadInfo, qnview.CollectionRuntime) (qnview.TransformSegment, error) {
	panic("mock required")
}

func TestApplyFailureStaysLocalAndGatesQueryMVCC(t *testing.T) {
	for _, stage := range []string{"live", "catchup", "partial"} {
		t.Run(stage, func(t *testing.T) { testLocalPoison(t, stage) })
	}
}

func testLocalPoison(t *testing.T, stage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	segment := &fakeSegment{id: 1000, partitionID: 10, vchannel: "p_1v0"}
	patch := mockey.Mock((*poisonPhysicalLoader).Load).To(func(_ *poisonPhysicalLoader, ctx context.Context, info *querypb.SegmentLoadInfo, _ qnview.CollectionRuntime) (qnview.TransformSegment, error) {
		if info.GetSegmentID() == 2000 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return segment, nil
	}).Build()
	defer patch.UnPatch()
	apply := mockey.Mock((*fakeSegment).ApplyTransform).Return(merr.WrapErrServiceUnavailableMsg("injected delete failure")).Build()
	defer apply.UnPatch()
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	streamPatch := mockey.Mock((*poisonLoadInfoStream).Subscribe).To(func(_ *poisonLoadInfoStream, opt qnview.SegmentLoadInfoSubscriptionOption) qnview.SegmentLoadInfoSubscription {
		require.NoError(t, opt.Handler.Handle(qnview.SegmentLoadInfoSnapshot{CollectionID: opt.CollectionID, SegmentID: opt.SegmentID, DataVersion: opt.DataVersion, Revision: qnview.SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: opt.SegmentID}}))
		return &poisonLoadInfoSubscription{}
	}).Build()
	defer streamPatch.UnPatch()
	sched := nodescheduler.New(2)
	defer sched.Close()
	manager := qnview.NewQueryViewSegmentManager(qnview.QueryViewSegmentManagerConfig{Scheduler: sched, Loader: &poisonPhysicalLoader{}, LoadInfoStream: &poisonLoadInfoStream{}, Buffer: buffer, CatchupConcurrency: 1})
	h := qnview.NewQNQueryViewHandler(manager)
	meta := &viewpb.QueryViewMeta{CollectionId: 1, ReplicaId: 1, Vchannel: "p_1v0", State: viewpb.QueryViewState_QueryViewStatePreparing, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 0}, QueryVersion: 1}}
	assignment := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	if stage == "partial" {
		// The first segment becomes ready while this second segment is loading.
		assignment.Partitions[0].SegmentIds = append(assignment.Partitions[0].SegmentIds, 2000)
	}
	view := qviews.NewQueryViewAtQueryNode(meta, assignment)
	reports := make(chan qviews.QueryViewAtWorkNode, 16)
	report := func(v qviews.QueryViewAtWorkNode) { reports <- v }
	nextReport := func() qviews.QueryViewAtWorkNode {
		select {
		case r := <-reports:
			return r
		case <-ctx.Done():
			t.Fatal("report timeout")
			return nil
		}
	}
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: report}})
	require.Eventually(t, func() bool { return streams.stream("p") != nil }, time.Second, time.Millisecond)
	stream := streams.stream("p")
	if stage == "partial" {
		stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 9}})
		partial := nextReport()
		require.Equal(t, qviews.QueryViewStatePreparing, partial.State())
		require.Equal(t, []int64{1000}, partial.IntoProto().QueryNode[0].Partitions[0].ReadySegmentIds)
	}
	if stage != "live" {
		stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", Entry: &streamingpb.TransformLogEntry{TimeTick: 10}})
		stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 20}})
		failed := nextReport()
		require.Equal(t, qviews.QueryViewStateUnrecoverable, failed.State())
		if stage == "catchup" {
			require.Empty(t, failed.IntoProto().QueryNode[0].Partitions[0].ReadySegmentIds)
		}
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: report}})
		require.True(t, proto.Equal(failed.IntoProto(), nextReport().IntoProto()))
		dropped := proto.Clone(meta).(*viewpb.QueryViewMeta)
		dropped.State = viewpb.QueryViewState_QueryViewStateDropped
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtQueryNode(dropped, assignment), OnReport: report}})
		require.Equal(t, qviews.QueryViewStateDropped, nextReport().State())
		return
	}
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 9}})
	readyReport := nextReport()
	require.Equal(t, qviews.QueryViewStateReady, readyReport.State())
	// Start a query waiting for the failing entry; it must see Poison after wakeup.
	waiting := make(chan error, 1)
	go func() {
		tasks, err := h.AcquireQuerySegmentTasks(ctx, view.ShardID(), view.QueryViewKey().QueryViewVersion, &viewpb.QueryPlanMVCC{TransformingTimetick: 10}, &internalpb.RetrieveRequest{})
		if tasks != nil {
			tasks.Release()
		}
		waiting <- err
	}()
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", Entry: &streamingpb.TransformLogEntry{TimeTick: 10}})
	require.ErrorContains(t, <-waiting, "poisoned")
	require.Empty(t, reports, "live Poison must not emit a Coordinator report")
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 20}})
	for _, ts := range []uint64{9, 10, 20} {
		mvcc := &viewpb.QueryPlanMVCC{TransformingTimetick: ts}
		query, qerr := h.AcquireQuerySegmentTasks(ctx, view.ShardID(), view.QueryViewKey().QueryViewVersion, mvcc, &internalpb.RetrieveRequest{})
		search, serr := h.AcquireSearchSegmentTasks(ctx, view.ShardID(), view.QueryViewKey().QueryViewVersion, mvcc, &internalpb.SearchRequest{})
		if ts < 10 {
			require.NoError(t, qerr)
			require.NoError(t, serr)
			query.Release()
			search.Release()
		} else {
			require.ErrorContains(t, qerr, "poisoned")
			require.ErrorContains(t, serr, "poisoned")
		}
	}
	// Reconnection reports only ordinary view state; Poison remains local.
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: report}})
	require.True(t, proto.Equal(readyReport.IntoProto(), nextReport().IntoProto()))
	require.Empty(t, reports)
	// A new view cannot treat the same poisoned physical instance as healthy.
	nextMeta := proto.Clone(meta).(*viewpb.QueryViewMeta)
	nextMeta.Version.QueryVersion = 2
	h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtQueryNode(nextMeta, assignment), OnReport: report}})
	require.Equal(t, qviews.QueryViewStateUnrecoverable, nextReport().State())
	for _, m := range []*viewpb.QueryViewMeta{meta, nextMeta} {
		dropped := proto.Clone(m).(*viewpb.QueryViewMeta)
		dropped.State = viewpb.QueryViewState_QueryViewStateDropped
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtQueryNode(dropped, assignment), OnReport: report}})
	}
	for n := 0; n < 2; {
		if nextReport().State() == qviews.QueryViewStateDropped {
			n++
		}
	}
}
