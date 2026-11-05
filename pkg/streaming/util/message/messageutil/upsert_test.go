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

package messageutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestUpsertProjections(t *testing.T) {
	mutable := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{CollectionId: 10, Partition: &message.PartitionSegmentAssignment{PartitionId: 20, Rows: 2, SegmentAssignment: &message.SegmentAssignment{SegmentId: 30}}, DeleteRows: 1}).WithBody(&message.UpsertMessageBody{
		Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 10, PartitionID: 20, NumRows: 2, SegmentID: 999, ShardName: "stale", Timestamps: []uint64{1, 2}},
		Delete: &msgpb.DeleteRequest{Base: &commonpb.MsgBase{Timestamp: 1}, CollectionID: 10, PartitionID: -1, SegmentId: 999, ShardName: "stale", NumRows: 1, Timestamps: []uint64{1}, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4}}}}},
	}).MustBuildMutable()
	immutable := mutable.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	raw := message.MustAsImmutableUpsertMessageV2(immutable).MustBody()
	insert, assignments, ok := AsInsertRequest(immutable)
	require.True(t, ok)
	require.Same(t, raw.GetInsert(), insert.MustBody())
	decoded, err := insert.Body(context.Background())
	require.NoError(t, err)
	require.Same(t, raw.GetInsert(), decoded)
	require.Equal(t, int64(30), assignments[0].GetSegmentAssignment().GetSegmentId())
	deletion, ok := AsDeleteRequest(immutable)
	require.True(t, ok)
	require.Equal(t, []uint64{100}, deletion.GetTimestamps())
	require.Equal(t, uint64(100), deletion.GetBase().GetTimestamp())
	require.Equal(t, "v1", deletion.GetShardName())
	require.Zero(t, deletion.GetSegmentId())
	require.Same(t, raw.GetDelete().GetPrimaryKeys(), deletion.GetPrimaryKeys())
	require.Equal(t, []uint64{1}, raw.GetDelete().GetTimestamps())
	require.Equal(t, uint64(1), raw.GetDelete().GetBase().GetTimestamp())
	require.Equal(t, "stale", raw.GetInsert().GetShardName())
}

func TestLegacyDMLProjections(t *testing.T) {
	insert := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}, {PartitionId: 3, Rows: 1}}}).WithBody(&msgpb.InsertRequest{CollectionID: 1, NumRows: 2}).MustBuildMutable().WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	view, assignments, ok := AsInsertRequest(insert)
	require.True(t, ok)
	require.Len(t, assignments, 2)
	require.Same(t, message.MustAsImmutableInsertMessageV1(insert).MustBody(), view.MustBody())
	_, ok = AsDeleteRequest(insert)
	require.False(t, ok)
	deletion := newTestDeleteMessage(t, 100, 10, 1, 2)
	request, ok := AsDeleteRequest(deletion)
	require.True(t, ok)
	require.Equal(t, []uint64{100, 100}, request.GetTimestamps())
	_, _, ok = AsInsertRequest(deletion)
	require.False(t, ok)
	_, _, ok = AsInsertRequest(nil)
	require.False(t, ok)
	_, ok = AsDeleteRequest(nil)
	require.False(t, ok)
}

func TestUpsertProjectionCanceledDecode(t *testing.T) {
	mutable := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased}, Delete: &msgpb.DeleteRequest{}}).MustBuildMutable()
	immutable := mutable.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	view, _, ok := AsInsertRequest(immutable)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := view.Body(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, ok, err = DeleteRequest(ctx, immutable)
	require.True(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUpsertProjectionsRejectIncompleteCompoundContent(t *testing.T) {
	for _, body := range []*message.UpsertMessageBody{
		{Insert: &msgpb.InsertRequest{}},
		{Delete: &msgpb.DeleteRequest{}},
	} {
		msg := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{}).
			WithBody(body).MustBuildMutable().WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		insert, _, ok := AsInsertRequest(msg)
		require.True(t, ok)
		_, err := insert.Body(context.Background())
		require.ErrorContains(t, err, "incomplete compound content")
		_, ok, err = DeleteRequest(context.Background(), msg)
		require.True(t, ok)
		require.ErrorContains(t, err, "incomplete compound content")
	}
}

func TestUpsertProjectionsRejectNonColumnFormat(t *testing.T) {
	for _, version := range []msgpb.InsertDataVersion{msgpb.InsertDataVersion_RowBased, msgpb.InsertDataVersion_ColumnBased} {
		t.Run(version.String(), func(t *testing.T) {
			insert := &msgpb.InsertRequest{Version: version, FieldsData: []*schemapb.FieldData{{FieldId: 100}}}
			if version == msgpb.InsertDataVersion_ColumnBased {
				insert.RowData = []*commonpb.Blob{{Value: []byte{1}}}
			}
			msg := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{Insert: insert, Delete: &msgpb.DeleteRequest{}}).MustBuildMutable().WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
			view, _, ok := AsInsertRequest(msg)
			require.True(t, ok)
			request, err := view.Body(context.Background())
			require.Error(t, err)
			require.Nil(t, request)
			deletion, ok, err := DeleteRequest(context.Background(), msg)
			require.True(t, ok)
			require.Error(t, err)
			require.Nil(t, deletion)
		})
	}
}

func TestTransformLogUpsertAndTransaction(t *testing.T) {
	upsert := message.NewUpsertMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2}).
		WithBody(&message.UpsertMessageBody{
			Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 10, NumRows: 2},
			Delete: &msgpb.DeleteRequest{
				CollectionID: 1, PartitionID: -1, NumRows: 2,
				PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}},
			},
		}).MustBuildMutable().WithTimeTick(90).WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(90))
	before := proto.Clone(message.MustAsImmutableUpsertMessageV2(upsert).MustBody())
	for name, raw := range map[string]message.ImmutableMessage{
		"autocommit":  upsert,
		"transaction": newTestTxnMessage(t, 100, upsert, newTestDeleteMessage(t, 95, 20, 3)),
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, TransformLogKindDelete, ClassifyTransformLogMessage(raw))
			entry := BuildTransformLogEntry(raw, TransformEntryOption{})
			require.NotNil(t, entry)
			require.Equal(t, raw.TimeTick(), entry.GetTimeTick())
			blocks := entry.GetDelete().GetBlocks()
			require.NotEmpty(t, blocks)
			require.Equal(t, int64(-1), blocks[0].GetPartitionId())
			require.Equal(t, []string{"a", "b"}, blocks[0].GetPrimaryKeys().GetStrId().GetData())
			if name == "transaction" {
				require.Len(t, blocks, 2)
				require.Equal(t, []int64{3}, blocks[1].GetPrimaryKeys().GetIntId().GetData())
			}
			filtered := BuildTransformLogEntry(raw, TransformEntryOption{DeleteFilter: func(partition int64, tt uint64) bool {
				require.Equal(t, raw.TimeTick(), tt)
				return partition != -1
			}})
			if name == "autocommit" {
				require.Nil(t, filtered)
			} else {
				require.Len(t, filtered.GetDelete().GetBlocks(), 1)
			}
		})
	}
	require.True(t, proto.Equal(before, message.MustAsImmutableUpsertMessageV2(upsert).MustBody()), "transform projection must preserve the cached Upsert body")
}
