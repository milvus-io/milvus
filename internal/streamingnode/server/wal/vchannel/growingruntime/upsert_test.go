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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestIncompleteUpsertCannotApplyInsertOrAdvanceFrontiers(t *testing.T) {
	for _, growing := range []bool{false, true} {
		for _, transform := range []bool{false, true} {
			if !growing && !transform {
				continue
			}
			runtime := newRuntime()
			msg := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
				WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partition: &message.PartitionSegmentAssignment{
					PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
				}}).
				WithBody(&message.UpsertMessageBody{Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 10, NumRows: 1}}).
				MustBuildMutable().WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
			require.ErrorContains(t, runtime.dispatchMessage(context.Background(), msg, growing, transform), "incomplete compound content")
			require.Empty(t, runtime.SegmentIDs())
			require.Zero(t, runtime.appliedGrowingTimeTick.Load())
			require.Zero(t, runtime.appliedTransformTimeTick.Load())
		}
	}
}

func TestCompoundPredecodeValidatesDeleteCountsAndLegacySiblings(t *testing.T) {
	upsert := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partition: &message.PartitionSegmentAssignment{
			PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
		}}).
		WithBody(&message.UpsertMessageBody{Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 1}, Delete: &msgpb.DeleteRequest{
			NumRows: 2, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		}}).MustBuildMutable()
	immutable := upsert.WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
	require.ErrorContains(t, predecodeUpserts(context.Background(), immutable), "row count differs")
	typed := message.MustAsMutableUpsertMessageV2(upsert)
	body := typed.MustBody()
	body.Delete.NumRows = 1
	typed.OverwriteBody(body)
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
