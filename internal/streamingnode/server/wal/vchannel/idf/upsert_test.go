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

package idf

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestUpsertBM25LiveAndSnapshotUseSameInsertContent(t *testing.T) {
	primary := &schemapb.FieldSchema{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		primary,
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	build := func(segmentID int64, tick uint64) message.ImmutableMessage {
		insert := message.MustAsImmutableInsertMessageV1(bm25Insert(t, segmentID, 2, tick))
		body := proto.Clone(insert.MustBody()).(*msgpb.InsertRequest)
		body.PartitionID = 10
		body.Version = msgpb.InsertDataVersion_ColumnBased
		body.FieldsData = append(body.FieldsData, &schemapb.FieldData{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}})
		version := int32(0)
		mutable := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{
			CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{insert.Header().GetPartitions()[0]}, DeleteRows: 1,
		}).WithBody(upsertfixture.Body(body, &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}})).MustBuildMutable()
		typed := message.MustAsMutableUpsertMessageV2(mutable)
		require.NoError(t, message.ValidateUpsert(typed.Header(), typed.MustBody(), primary))
		return mutable.WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	raw := build(20, 10)
	txnCtx := message.TxnContext{TxnID: 1}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(19).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(19))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(30).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(30))
	txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(build(21, 20)).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	runtime, err := newOracleRuntime(t.Context(), nil, walview.VChannelWALView{Schema: schema, RequiredFields: testRequiredFields(schema), WriteSchema: schema, CollectionID: 1, VChannel: "v1"}, nil, true)
	require.NoError(t, err)
	defer runtime.Close()
	runtime.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: raw})
	runtime.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: txn})
	for _, segmentID := range []int64{20, 21} {
		require.EqualValues(t, 1, runtime.growingStore.segments[segmentID].stats[102].NumRow())
		require.EqualValues(t, 2, runtime.growingStore.segments[segmentID].stats[102].GetAvgdl())
	}
	recovered, err := newOracleRuntime(t.Context(), &Provider{}, walview.VChannelWALView{
		Schema: schema, RequiredFields: testRequiredFields(schema), WriteSchema: schema, CollectionID: 1, VChannel: "v1",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{
			{SegmentID: 20, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{raw}}},
			{SegmentID: 21, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{txn}}},
		}},
	}, nil, true)
	require.NoError(t, err)
	defer recovered.Close()
	for _, segmentID := range []int64{20, 21} {
		require.EqualValues(t, 1, recovered.growingStore.segments[segmentID].stats[102].NumRow())
		require.EqualValues(t, 2, recovered.growingStore.segments[segmentID].stats[102].GetAvgdl())
	}
}
