package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestGrowingUpdateRecoveryAndLiveFailure(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	ctx := t.Context()
	schema := mock_segcore.GenTestCollectionSchema("update_delivery", schemapb.DataType_Int64, false)
	var field int64
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_Int32 {
			field = f.FieldID
		}
	}
	insert := newTestSegmentInsertMessage(t, "ch", 100, 3, 10, schema)
	var key int64
	for _, f := range message.MustAsImmutableInsertMessageV1(insert).MustBody().GetFieldsData() {
		for _, sf := range schema.Fields {
			if sf.GetIsPrimaryKey() && sf.FieldID == f.FieldId {
				key = f.GetScalars().GetLongData().GetData()[0]
			}
		}
	}
	update := func(tick uint64, id int64, value int32) message.ImmutableMessage {
		body := &messagespb.UpdateMessageBody{PartitionId: 10, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{key}}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: id, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: value}}}}}}}
		return message.NewUpdateMessageBuilderV2().WithVChannel("ch").WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 1}).WithBody(body).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	summary := walsummary.NewManager(walsummary.ManagerConfig{})
	summary.ObserveMessage(ctx, update(20, field, 321))
	stream := walsummary.NewStream(summary)
	defer stream.Close()
	r := newRuntime()
	defer r.Close()
	retireOld, err := r.BindReadFloor(20)
	require.NoError(t, err)
	retireNew, err := r.BindReadFloor(30)
	require.NoError(t, err)
	defer retireNew()
	require.NoError(t, r.Prepare(ctx, walview.VChannelWALView{CollectionID: 1, VChannel: "ch", Schema: schema, BaseGrowingTimeTick: 20, BaseTransformTimeTick: 20, TransformLogStream: stream, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 100, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{insert}}}}}}))
	native, ok := r.Segment(100)
	require.True(t, ok)
	read := func(ts uint64) int32 {
		encoded, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 3}}, OutputFieldIds: []int64{field}})
		require.NoError(t, err)
		plan, err := segcore.NewRetrievePlan(r.collection, encoded, ts, 100, 0, 0, 0)
		require.NoError(t, err)
		defer plan.Delete()
		result, err := native.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0}})
		require.NoError(t, err)
		defer result.Release()
		data, err := result.GetResult()
		require.NoError(t, err)
		for _, fd := range data.FieldsData {
			if fd.FieldId == field {
				return fd.GetScalars().GetIntData().Data[0]
			}
		}
		t.Fatal("missing requested field")
		return 0
	}
	require.EqualValues(t, 321, read(20))
	require.NoError(t, r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: update(30, field, 876)}))
	require.EqualValues(t, 321, read(29))
	require.EqualValues(t, 876, read(30))
	acquire := func(ts uint64) func() {
		handles, err := r.AcquireGrowingSegmentHandles(ctx, qviews.DataVersion{}, ts, nil)
		require.NoError(t, err)
		require.Len(t, handles, 1)
		return handles[0].Release
	}
	versions := func() uint64 {
		n, err := segcore.UpdateVersionCountForTest(native)
		require.NoError(t, err)
		return n
	}
	// The View still protects an issued plan before its worker acquires a handle.
	acquire(30)()
	require.EqualValues(t, 2, versions())
	oldRead := acquire(20)
	retireOld()
	acquire(30)()
	require.EqualValues(t, 2, versions())
	require.EqualValues(t, 321, read(20))
	oldRead()
	acquire(30)()
	require.Eventually(t, func() bool { return versions() == 1 }, 3*time.Second, time.Millisecond)
	require.EqualValues(t, 876, read(30))
	_, err = r.BindReadFloor(29)
	require.Error(t, err)
	require.Error(t, r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: update(40, -100, 2)}))
	require.Error(t, r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: update(50, field, 999)}))
	require.EqualValues(t, 30, r.AppliedTransformTimeTick())
	require.EqualValues(t, 30, r.AppliedGrowingTimeTick())
	require.Error(t, r.WaitMVCCVisible(ctx, 40, 40))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, r.WaitMVCCVisible(canceled, 40, 40), context.Canceled)
}
