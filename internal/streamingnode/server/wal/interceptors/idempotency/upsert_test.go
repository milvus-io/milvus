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

package idempotency

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestUpsertBoundedRetryReturnsOriginalResult(t *testing.T) {
	result := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{2}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}
	build := func(key string) message.MutableMessage {
		header := &message.UpsertMessageHeader{}
		if key != "" {
			header.IdempotentResult = result
		}
		msg := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(1, key)).WithHeader(header).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
		return msg
	}
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	calls := 0
	appendOp := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		calls++
		extra := utility.GetExtraAppendResult(ctx)
		extra.TimeTick = 42
		extra.LastConfirmedMessageID = rmq.NewRmqID(9)
		return rmq.NewRmqID(10), nil
	}
	for i := 0; i < 2; i++ {
		extra := &utility.ExtraAppendResult{}
		ctx := utility.WithExtraAppendResult(context.Background(), extra)
		msg := build("key")
		if i == 1 {
			typed := message.MustAsMutableUpsertMessageV2(msg)
			header := typed.Header()
			header.IdempotentResult = proto.Clone(result).(*messagespb.IdempotentInsertResult)
			header.IdempotentResult.Ids.GetIntId().Data[0] = 999
			typed.OverwriteHeader(header)
		}
		id, err := interceptor.DoAppend(ctx, msg, appendOp)
		require.NoError(t, err)
		require.Equal(t, rmq.NewRmqID(10), id)
		require.Equal(t, uint64(42), extra.TimeTick)
		if i == 1 {
			require.True(t, proto.Equal(result, extra.Extra.(*messagespb.IdempotentInsertResult)))
		}
	}
	require.Equal(t, 1, calls)
	invalid := build("key")
	typedInvalid := message.MustAsMutableUpsertMessageV2(invalid)
	invalidHeader := typedInvalid.Header()
	invalidHeader.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}}
	typedInvalid.OverwriteHeader(invalidHeader)
	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), invalid, appendOp)
	require.Error(t, err, "a cache hit cannot bypass metadata shape validation")
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	require.Equal(t, 1, calls)
	for i := 0; i < 2; i++ {
		ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		_, err := interceptor.DoAppend(ctx, build(""), appendOp)
		require.NoError(t, err)
	}
	require.Equal(t, 3, calls)
}

func TestMissingUpsertTxnResultExpiresBeforeAppend(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithIdempotencyKey("key").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1})
	_, err := interceptor.DoAppend(context.Background(), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("missing results must not reach append")
		return nil, nil
	})
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.Zero(t, interceptor.window("v1").Len())
}

func TestUpsertInvalidEnvelopeCannotReturnCachedSuccess(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	build := func() message.MutableMessage {
		return message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(1, "envelope-key")).WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	}
	calls := 0
	appendOp := func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		calls++
		utility.GetExtraAppendResult(ctx).TimeTick = 42
		return rmq.NewRmqID(1), nil
	}
	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), build(), appendOp)
	require.NoError(t, err)
	for _, marker := range []string{"_np", "_pcl", "_bh", "_ur"} {
		msg := build()
		msg.Properties().ToRawMap()[marker] = ""
		_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), msg, appendOp)
		require.Error(t, err, marker)
	}
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), build(), appendOp)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestUpsertResultExtractionUsesOptionalHeaderWithoutBodyDecode(t *testing.T) {
	for _, result := range []*messagespb.IdempotentInsertResult{
		nil,
		{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}},
		{RowOffsets: []uint32{0}},
	} {
		built := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{IdempotentResult: result}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
		msg := message.NewMutableMessageBeforeAppend([]byte{0xff}, built.Properties().ToRawMap())
		actual, err := getInsertResultFromInsertMessage(msg)
		if message.ValidateIdempotentInsertResult(result) != nil {
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsInvalidArgument())
		} else {
			require.NoError(t, err, "result extraction must not decode the invalid payload")
			require.True(t, proto.Equal(result, actual))
		}
	}
}

func TestUpsertOmittedResultPreservesOriginalKeyedAppend(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	build := func() message.MutableMessage {
		return message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey("optional-result").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	}
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	original, err := interceptor.DoAppend(ctx, build(), func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		extra := utility.GetExtraAppendResult(ctx)
		extra.TimeTick, extra.LastConfirmedMessageID = 42, rmq.NewRmqID(1)
		return rmq.NewRmqID(2), nil
	})
	require.NoError(t, err)
	extra := &utility.ExtraAppendResult{Extra: &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{999}}}}}}
	retry := build()
	typedRetry := message.MustAsMutableUpsertMessageV2(retry)
	header := typedRetry.Header()
	header.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{999}}}}}
	typedRetry.OverwriteHeader(header)
	retried, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), extra), retry, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("an omitted optional result must not disable the keyed window")
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, original, retried)
	require.Equal(t, uint64(42), extra.TimeTick)
	require.Nil(t, extra.Extra, "the retry must not supply new IDs for an original result that omitted them")
}

func upsertForTest(vc string) message.MutableMessage {
	version := int32(1)
	return message.NewUpsertMessageBuilderV2().WithVChannel(vc).WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partition: &message.PartitionSegmentAssignment{PartitionId: 2, Rows: 1}, DeleteRows: 1}).WithBody(&message.UpsertMessageBody{Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 2, NumRows: 1, RowIDs: []int64{7}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}}}}}}, Delete: &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}}).MustBuildMutable().WithTimeTick(10)
}

func TestSequentialKeyedUpsertTransactionUsesBufferedOriginalResult(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "Upsert", true: "mixed Insert"}[mixed], func(t *testing.T) {
			interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
			defer interceptor.Close()
			txnCtx := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
			first := upsertForTest("p_1v0").WithTxnContext(txnCtx)
			typed := message.MustAsMutableUpsertMessageV2(first)
			body := typed.MustBody()
			header := typed.Header()
			header.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: proto.Clone(body.Delete.PrimaryKeys).(*schemapb.IDs)}
			typed.OverwriteHeader(header)
			appended := 0
			appendOp := func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				appended++
				extra := utility.GetExtraAppendResult(ctx)
				extra.TimeTick = uint64(10 + appended)
				extra.LastConfirmedMessageID = rmq.NewRmqID(1)
				return rmq.NewRmqID(int64(appended)), nil
			}
			_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), first, appendOp)
			require.NoError(t, err)
			count := uint64(1)
			if mixed {
				header := &message.InsertMessageHeader{CollectionId: 1}
				result := proto.Clone(typed.Header().GetIdempotentResult()).(*messagespb.IdempotentInsertResult)
				result.RowOffsets = []uint32{1}
				message.SetInsertHeaderIdempotentInsertResult(header, result)
				second := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(header).WithBody(body.Insert).MustBuildMutable().WithTxnContext(txnCtx)
				_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), second, appendOp)
				require.NoError(t, err)
				count++
			}
			commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey("sequential-key").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: count}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx)
			extra := &utility.ExtraAppendResult{}
			id, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), extra), commit, appendOp)
			require.NoError(t, err)
			require.NotNil(t, extra.Extra)
			for i := 0; i < 2; i++ {
				retryExtra := &utility.ExtraAppendResult{}
				retried, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), retryExtra), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
					t.Fatal("duplicate commit reached append")
					return nil, nil
				})
				require.NoError(t, err)
				require.Equal(t, id, retried)
				result := retryExtra.Extra.(*messagespb.IdempotentInsertResult)
				require.True(t, proto.Equal(extra.Extra, result))
				require.Len(t, result.GetRowOffsets(), int(count))
				require.Len(t, result.GetIds().GetIntId().GetData(), int(count))
				require.Equal(t, int64(7), result.GetIds().GetIntId().GetData()[0])
			}
			require.Equal(t, int(count)+1, appended)
		})
	}
}

func TestSequentialKeyedUpsertRejectsMissingBodyResult(t *testing.T) {
	for _, missingFirst := range []bool{true, false} {
		interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
		txnCtx := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
		upsert := upsertForTest("p_1v0").WithTxnContext(txnCtx)
		typed := message.MustAsMutableUpsertMessageV2(upsert)
		body := typed.MustBody()
		header := typed.Header()
		header.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: proto.Clone(body.Delete.PrimaryKeys).(*schemapb.IDs)}
		typed.OverwriteHeader(header)
		legacy := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(body.Insert).MustBuildMutable().WithTxnContext(txnCtx)
		bodies := []message.MutableMessage{legacy, upsert}
		if !missingFirst {
			bodies[0], bodies[1] = bodies[1], bodies[0]
		}
		for _, msg := range bodies {
			_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{TimeTick: 10}), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) { return rmq.NewRmqID(1), nil })
			require.NoError(t, err)
		}
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey("missing-result").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx)
		_, err := interceptor.DoAppend(context.Background(), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("incomplete response reached append")
			return nil, nil
		})
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
		require.Zero(t, interceptor.window("p_1v0").Len())
		require.Nil(t, interceptor.txnInsertResultBuffers.Build(commit), "owner rejection cleans the buffer")
		interceptor.Close()
	}
}

func TestKeylessTransactionRemovesOptionalResultState(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	txnCtx := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
	body := upsertForTest("p_1v0").WithTxnContext(txnCtx)
	appendOp := func(context.Context, message.MutableMessage) (message.MessageID, error) { return rmq.NewRmqID(1), nil }
	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{TimeTick: 10}), body, appendOp)
	require.NoError(t, err)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx)
	_, err = interceptor.DoAppend(context.Background(), commit, appendOp)
	require.NoError(t, err)
	// If cleanup were omitted, this late result would retain missing metadata
	// from the completed transaction and could not build a fresh response.
	interceptor.txnInsertResultBuffers.Add(body, &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}, 11)
	require.NotNil(t, interceptor.txnInsertResultBuffers.Build(commit))
	interceptor.txnInsertResultBuffers.Remove(commit)
}
