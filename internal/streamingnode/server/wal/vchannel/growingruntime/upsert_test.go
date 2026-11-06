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
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestIncompleteUpsertCannotApplyInsertOrAdvanceFrontiers(t *testing.T) {
	for _, growing := range []bool{false, true} {
		for _, transform := range []bool{false, true} {
			if !growing && !transform {
				continue
			}
			runtime := newRuntime()
			msg := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
				WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2, Partitions: []*message.PartitionSegmentAssignment{{
					PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
				}}}).
				WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 10, NumRows: 1}, nil)).
				MustBuildMutable().WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
			require.ErrorContains(t, runtime.dispatchMessage(context.Background(), msg, growing, transform), "row placement")
			require.Empty(t, runtime.SegmentIDs())
			require.Zero(t, runtime.appliedGrowingTimeTick.Load())
			require.Zero(t, runtime.appliedTransformTimeTick.Load())
		}
	}
}

func TestCompoundPredecodeValidatesDeleteCountsAndLegacySiblings(t *testing.T) {
	upsert := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2, Partitions: []*message.PartitionSegmentAssignment{{
			PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
		}}}).
		WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 1, RowIDs: []int64{1}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}}}}, &msgpb.DeleteRequest{
			PartitionID: -1, NumRows: 2, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		})).MustBuildMutable()
	immutable := upsert.WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
	require.ErrorContains(t, predecodeUpserts(context.Background(), immutable), "row count differs")
	typed := message.MustAsMutableUpsertMessageV2(upsert)
	header := typed.Header()
	header.DeleteRows = 1
	typed.OverwriteHeader(header)
	immutable = upsert.IntoImmutableMessage(rmq.NewRmqID(40))
	for _, deletion := range []bool{false, true} {
		legacy := newTestAssignedInsertMessage(t, "p_1v0", 100, 41)
		if deletion {
			legacy = newTestTransformDeleteMessage(t, "p_1v0", 41)
		}
		wire := legacy.IntoMessageProto()
		broken := message.NewImmutableMesasge(legacy.MessageID(), []byte{0xff}, wire.Properties)
		txnCtx := message.TxnContext{TxnID: 50}
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).
			WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(39).
			WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(39))
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).
			WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(42).
			WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
		txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(immutable).Add(broken).
			Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		require.NoError(t, err)
		require.ErrorIs(t, predecodeUpserts(context.Background(), txn), message.ErrMalformedBody)
		runtime := newRuntime()
		require.ErrorIs(t, runtime.dispatchMessage(context.Background(), txn, true, true), message.ErrMalformedBody)
		require.Empty(t, runtime.SegmentIDs())
		require.Zero(t, runtime.appliedGrowingTimeTick.Load())
		require.Zero(t, runtime.appliedTransformTimeTick.Load())
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
		body := &message.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 8}}}}, RowIds: []int64{int64(tick), int64(tick + 1)}, Columns: []*schemapb.FieldData{
			{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(tick), int64(tick)}}}}}},
			{FieldId: 102, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}},
		}}
		header := &message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}}
		if deleteOld {
			body.DeleteEffects = []*messagespb.UpsertDeleteEffect{{PrimaryKeys: body.PrimaryKeys, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}
			header.DeleteRows = 2
		}
		return message.NewUpsertMessageBuilderV2().WithVChannel("ch").WithHeader(header).WithBody(body).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	require.NoError(t, runtime.dispatchMessage(context.Background(), build(10, false), true, true))
	segment, ok := runtime.Segment(100)
	require.True(t, ok)
	require.Equal(t, int64(2), segment.RowNum())
	manager := mvcc.NewQueryMVCCManager(10)
	manager.ApplyRecoveryBarrier("ch", 10)
	runtime.markGrowingTimeTick(10)
	runtime.markTransformTimeTick(10)
	for _, effects := range []bool{false, true} {
		tick := uint64(20)
		if effects {
			tick = 30
		}
		msg := build(tick, effects)
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
	transaction, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(build(40, false)).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
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
