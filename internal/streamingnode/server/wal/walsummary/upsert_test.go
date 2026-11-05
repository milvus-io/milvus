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

package walsummary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestUpsertSummaryOriginalResult(t *testing.T) {
	result := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{2}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}
	msg := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey("key").WithHeader(&message.UpsertMessageHeader{IdempotentResult: result}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTimeTick(42).WithLastConfirmed(rmq.NewRmqID(9)).IntoImmutableMessage(rmq.NewRmqID(10))
	keys, insert := insertSummaryOf(msg)
	require.Equal(t, "key", keys.GetKey())
	require.Equal(t, result.GetRowOffsets(), keys.GetRowOffsets())
	require.True(t, proto.Equal(result.GetIds(), insert.GetIds()))
	require.Equal(t, uint64(42), insert.GetSourceTimetick())
	require.Equal(t, rmq.NewRmqID(10).IntoProto(), insert.GetSourceMessageId())
	require.Equal(t, rmq.NewRmqID(9).IntoProto(), insert.GetLastConfirmedMessageId())
}

func TestUpsertTxnSummaryRequiresEveryInsertedResult(t *testing.T) {
	for _, complete := range []bool{false, true} {
		txnCtx := message.TxnContext{TxnID: 1}
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(1).IntoImmutableMessage(rmq.NewRmqID(1))
		builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
		result := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}
		upsert := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{IdempotentResult: result}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(2).IntoImmutableMessage(rmq.NewRmqID(2))
		builder.Add(upsert)
		header := &message.InsertMessageHeader{CollectionId: 1}
		if complete {
			message.SetInsertHeaderIdempotentInsertResult(header, result)
		}
		insert := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(header).WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(3).IntoImmutableMessage(rmq.NewRmqID(3))
		builder.Add(insert)
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey("key").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(4).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(4))
		txnMsg, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		require.NoError(t, err)
		keys, fact := insertSummaryOf(txnMsg)
		require.NotNil(t, fact)
		if complete {
			require.Equal(t, "key", keys.GetKey())
			require.Len(t, fact.GetIds().GetIntId().GetData(), 2)
		} else {
			require.Empty(t, keys.GetKey())
			require.Nil(t, fact.GetIds())
		}
	}
}

func TestUpsertSummaryResultDoesNotDecodeCompoundBody(t *testing.T) {
	result := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}
	base := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey("header-only").WithHeader(&message.UpsertMessageHeader{IdempotentResult: result}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	raw := message.NewMutableMessageBeforeAppend([]byte{0xff}, base.Properties().ToRawMap()).WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(90)).IntoImmutableMessage(rmq.NewRmqID(100))
	actual, fact := insertResultOf(raw)
	require.True(t, fact)
	require.True(t, proto.Equal(result, actual))
	keys, insert := insertSummaryOf(raw)
	require.Equal(t, "header-only", keys.GetKey())
	require.True(t, proto.Equal(result.GetIds(), insert.GetIds()))
	// Full observation still needs the Delete body and must fail atomically.
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(context.Background()))
	manager.InitLastAcked(50)
	manager.ObserveMessage(context.Background(), raw)
	require.Error(t, manager.terminalErr)
	require.Equal(t, uint64(50), manager.LastAcked())
	require.Empty(t, manager.pending, "neither the insert fact nor the delete effect can be staged")
}

func TestUpsertSummaryPersistsBothEffectsAtOnePosition(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))
	manager.InitLastAcked(50)
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey("replace-7").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 1, IdempotentResult: &messagespb.IdempotentInsertResult{Ids: ids, RowOffsets: []uint32{0}}}).
		WithBody(&message.UpsertMessageBody{
			Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 20, NumRows: 1},
			Delete: &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: ids},
		}).MustBuildMutable().WithTimeTick(100).WithLastConfirmed(walimplstest.NewTestMessageID(90)).IntoImmutableMessage(walimplstest.NewTestMessageID(100))
	manager.ObserveMessage(ctx, raw)
	require.Equal(t, uint64(50), manager.LastAcked(), "Summary persistence independently pins the recovery prefix")
	require.NoError(t, persistSummary(ctx, manager))
	require.Equal(t, uint64(100), manager.LastAcked())
	require.Len(t, manager.Manifest().GetChunks(), 1)

	// Read the existing chunk format directly, then reopen under a new term.
	sections, _, err := store.ReadChunk(ctx, 0, 1)
	require.NoError(t, err)
	require.Len(t, sections["v1"].Inserts, 1)
	require.Len(t, sections["v1"].Idempotency, 1)
	recovered := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	retained, err := recovered.ReadIdempotencyEntries(ctx, "v1", 0, 100)
	require.NoError(t, err)
	require.Len(t, retained.Inserts, 1)
	require.Equal(t, uint64(100), retained.Inserts[0].GetSourceTimetick())
	require.True(t, proto.Equal(messageIDProto(raw.MessageID()), retained.Inserts[0].GetSourceMessageId()))
	require.True(t, proto.Equal(ids, retained.Inserts[0].GetIds()))
	require.Equal(t, "replace-7", retained.Idempotency[0].GetKey())
	transforms, err := recovered.ReadTransform(ctx, "v1", 0, 100, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, transforms.Entries, 1)
	require.Equal(t, raw.TimeTick(), transforms.Entries[0].GetTimeTick())
	blocks := transforms.Entries[0].GetDelete().GetBlocks()
	require.Len(t, blocks, 1)
	require.Equal(t, int64(-1), blocks[0].GetPartitionId())
	require.True(t, proto.Equal(ids, blocks[0].GetPrimaryKeys()))
}
