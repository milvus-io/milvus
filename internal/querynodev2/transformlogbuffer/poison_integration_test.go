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
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type poisonPhysicalManager struct{ qnview.PhysicalSegmentManager }

func (*poisonPhysicalManager) Acquire(qnview.AcquirePhysicalSegments) { panic("mock required") }
func (*poisonPhysicalManager) Release(qnview.ReleaseSegments)         { panic("mock required") }

func TestApplyFailureReportsPoisonAndGatesQueryMVCC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	segment := &fakeSegment{id: 1000, partitionID: 10, vchannel: "p_1v0"}
	patch := mockey.Mock((*poisonPhysicalManager).Acquire).To(func(_ *poisonPhysicalManager, req qnview.AcquirePhysicalSegments) {
		req.OnLoaded([]qnview.TransformSegment{segment})
	}).Build()
	defer patch.UnPatch()
	release := mockey.Mock((*poisonPhysicalManager).Release).To(func(_ *poisonPhysicalManager, req qnview.ReleaseSegments) { go req.OnDropped() }).Build()
	defer release.UnPatch()
	apply := mockey.Mock((*fakeSegment).ApplyTransform).Return(merr.WrapErrServiceUnavailableMsg("injected delete failure")).Build()
	defer apply.UnPatch()
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	sched := nodescheduler.New(2)
	defer sched.Close()
	manager := qnview.NewQueryViewSegmentReadinessManagerWithScheduler(sched, &poisonPhysicalManager{}, buffer, 1)
	h := qnview.NewQNQueryViewHandler(manager)
	meta := &viewpb.QueryViewMeta{CollectionId: 1, ReplicaId: 1, Vchannel: "p_1v0", State: viewpb.QueryViewState_QueryViewStatePreparing, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}
	assignment := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
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
	stream := streams.stream("p")
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 9}})
	require.Equal(t, qviews.QueryViewStateReady, nextReport().State())
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
	poisonReport := nextReport()
	require.Equal(t, qviews.QueryViewStateReady, poisonReport.State())
	wire, err := proto.Marshal(poisonReport.IntoProto())
	require.NoError(t, err)
	decoded := &viewpb.QueryViewOfShard{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.Len(t, decoded.QueryNode[0].PoisonedSegments, 1)
	require.EqualValues(t, 10, decoded.QueryNode[0].PoisonedSegments[0].FailedTimetick)
	require.NotZero(t, decoded.QueryNode[0].PoisonedSegments[0].Generation)
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
	// Replacing the report stream must replay the sticky Poison snapshot.
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: report}})
	require.Len(t, nextReport().IntoProto().QueryNode[0].PoisonedSegments, 1)
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
