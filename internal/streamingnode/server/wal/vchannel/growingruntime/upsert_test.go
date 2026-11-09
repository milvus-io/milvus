// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestUpsertDecodeFailurePropagatesFromApply(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	for _, growing := range []bool{false, true} {
		runtime := newRuntime()
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, {FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}}}
		require.NoError(t, runtime.Prepare(t.Context(), walview.VChannelWALView{CollectionID: 1, VChannel: "p_1v0", Schema: schema, WriteSchema: schema}))
		msg := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
		broken := message.NewImmutableMesasge(msg.MessageID(), []byte{0xff}, msg.IntoMessageProto().Properties)
		if !growing {
			runtime.appliedGrowingTimeTick.Store(40)
		}
		initialGrowing := runtime.appliedGrowingTimeTick.Load()
		require.Panics(t, func() { runtime.applyLiveMessage(t.Context(), broken) })
		require.Equal(t, initialGrowing, runtime.appliedGrowingTimeTick.Load())
		require.Zero(t, runtime.appliedTransformTimeTick.Load())
		runtime.Close()
	}
}

func TestUpsertAppliesNativeRowsAndDeletes(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, {FieldID: 101, Name: "marker", DataType: schemapb.DataType_Int64}, {FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}}}
	runtime := newRuntime()
	require.NoError(t, runtime.Prepare(context.Background(), walview.VChannelWALView{CollectionID: 1, VChannel: "ch", Schema: schema, WriteSchema: schema, BaseGrowingTimeTick: 1}))
	defer runtime.Close()
	version := int32(1)
	build := func(tick uint64, deleteOld bool) message.ImmutableMessage {
		body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 8}}}}, RowIds: []int64{int64(tick), int64(tick + 1)}, Columns: []*schemapb.FieldData{
			{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(tick), int64(tick)}}}}}},
			{FieldId: 102, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}},
		}}}
		header := &message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}}
		if deleteOld {
			body.DeleteEffects = []*messagespb.UpsertDeleteEffect{{PrimaryKeys: body.Rows.PrimaryKeys, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}
			header.DeleteRows = 2
		}
		return message.NewUpsertMessageBuilderV2().WithVChannel("ch").WithHeader(header).WithBody(body).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	buildInsert := func(tick uint64) message.ImmutableMessage {
		source := message.MustAsImmutableUpsertMessageV2(build(tick, false))
		header := source.Header()
		return message.NewInsertMessageBuilderV2().WithVChannel("ch").WithHeader(&message.InsertMessageHeaderV2{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion}).WithBody(&message.InsertMessageBody{Rows: source.MustBody().GetRows()}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	require.NoError(t, runtime.dispatchMessage(context.Background(), build(10, false), true, true))
	segment, ok := runtime.Segment(100)
	require.True(t, ok)
	require.Equal(t, int64(2), segment.RowNum())
	manager := mvcc.NewQueryMVCCManager(10)
	manager.ApplyRecoveryBarrier("ch", 10)
	runtime.markGrowingTimeTick(10)
	runtime.markTransformTimeTick(10)
	for _, msg := range []message.ImmutableMessage{buildInsert(20), build(25, false), build(30, true)} {
		tick := msg.TimeTick()
		effects := tick == 30
		wire := msg.IntoMessageProto()
		manager.ObserveAppend(message.NewMutableMessageBeforeAppend(wire.Payload, wire.Properties))
		manager.Publish(func(yield func(message.ImmutableMessage, bool) bool) { yield(msg, false) }, tick, nil)
		snapshot, ready := manager.Snapshot("ch", tick)
		require.True(t, ready)
		runtime.applyLiveMessage(context.Background(), msg)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, runtime.WaitMVCCVisible(ctx, snapshot.GrowingTimetick, snapshot.TransformingTimetick))
		cancel()
		if !effects {
			require.EqualValues(t, 10, snapshot.TransformingTimetick)
		}
	}

	require.Equal(t, int64(2), segment.RowNum(), "old versions are deleted and new versions survive")
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	plan, err := planparserv2.CreateRetrievePlan(helper, "pk == 7", nil)
	require.NoError(t, err)
	plan.OutputFieldIds = []int64{101}
	blob, err := proto.Marshal(plan)
	require.NoError(t, err)
	retrieve, err := segcore.NewRetrievePlan(runtime.collection, blob, 30, 0, commonpb.ConsistencyLevel_Strong, 0, 0)
	require.NoError(t, err)
	defer retrieve.Delete()
	result, err := segment.Retrieve(context.Background(), retrieve)
	require.NoError(t, err)
	defer result.Release()
	data, err := result.GetResult()
	require.NoError(t, err)
	require.Len(t, data.GetFieldsData(), 1)
	require.Equal(t, []int64{30}, data.FieldsData[0].GetScalars().GetLongData().Data)
	// The ACK-side insert-only flag and assembled-body classification agree.
	txnContext := message.TxnContext{TxnID: 1}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("ch").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(39).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(39))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("ch").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(41).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(41))
	transaction, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(buildInsert(40)).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	manager.Publish(func(yield func(message.ImmutableMessage, bool) bool) { yield(commit, true) }, 41, nil)
	snapshot, ready := manager.Snapshot("ch", 41)
	require.True(t, ready)
	require.EqualValues(t, 30, snapshot.TransformingTimetick)
	runtime.applyLiveMessage(context.Background(), transaction)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, runtime.WaitMVCCVisible(ctx, snapshot.GrowingTimetick, snapshot.TransformingTimetick))
}
