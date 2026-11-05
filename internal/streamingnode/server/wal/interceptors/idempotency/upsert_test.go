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

func upsertForTest(vc string) message.MutableMessage {
	version := int32(1)
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}
	return message.NewUpsertMessageBuilderV2().WithVChannel(vc).
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}, DeleteRows: 1}).
		WithBody(&message.UpsertMessageBody{PrimaryKeys: keys, RowIds: []int64{7}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: keys, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}}).MustBuildMutable().WithTimeTick(10)
}

func mapUpsert(msg message.MutableMessage, offsets ...uint32) {
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.RequestRowMapping = &messagespb.UpsertRequestRowMapping{RowOffsets: offsets}
	typed.OverwriteBody(body)
}

func TestUpsertBoundedRetryReturnsOriginalResult(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	build := func() message.MutableMessage {
		msg := upsertForTest("p_1v0")
		msg.Properties().ToRawMap()["_ik"] = "key"
		mapUpsert(msg, 2)
		return msg
	}
	calls := 0
	appendOp := func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		calls++
		extra := utility.GetExtraAppendResult(ctx)
		extra.TimeTick, extra.LastConfirmedMessageID = 42, rmq.NewRmqID(9)
		return rmq.NewRmqID(10), nil
	}
	for i := 0; i < 2; i++ {
		msg := build()
		if i == 1 {
			typed := message.MustAsMutableUpsertMessageV2(msg)
			body := typed.MustBody()
			body.PrimaryKeys.GetIntId().Data[0] = 999
			typed.OverwriteBody(body)
		}
		extra := &utility.ExtraAppendResult{}
		id, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), extra), msg, appendOp)
		require.NoError(t, err)
		require.Equal(t, rmq.NewRmqID(10), id)
		require.Equal(t, uint64(42), extra.TimeTick)
		require.Equal(t, []int64{7}, extra.Extra.(*messagespb.IdempotentInsertResult).Ids.GetIntId().Data)
	}
	require.Equal(t, 1, calls)
	for _, marker := range []string{"_np", "_pcl", "_bh", "_ur"} {
		msg := build()
		msg.Properties().ToRawMap()[marker] = ""
		_, err := interceptor.DoAppend(context.Background(), msg, appendOp)
		require.Error(t, err, marker)
	}
	invalid := build()
	mapUpsert(invalid, 0, 1)
	_, err := interceptor.DoAppend(context.Background(), invalid, appendOp)
	require.Error(t, err, "cache hits cannot bypass mapping validation")
	require.Equal(t, 1, calls)
}

func TestUpsertMissingMappingCannotCachePartialResult(t *testing.T) {
	interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
	defer interceptor.Close()
	msg := upsertForTest("p_1v0")
	result, err := getInsertResultFromInsertMessage(context.Background(), msg)
	require.NoError(t, err)
	require.Nil(t, result, "unkeyed writes can omit request mapping")
	msg.Properties().ToRawMap()["_ik"] = "missing"
	_, err = interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("missing mapping must fail before append")
		return nil, nil
	})
	require.Error(t, err)
	require.Zero(t, interceptor.window("p_1v0").Len())
}

func TestSequentialKeyedUpsertTransactionUsesBufferedOriginalResult(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "Upsert", true: "historical Insert"}[mixed], func(t *testing.T) {
			interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
			defer interceptor.Close()
			txn := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
			body := upsertForTest("p_1v0").WithTxnContext(txn)
			mapUpsert(body, 0)
			appended := 0
			appendOp := func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				appended++
				extra := utility.GetExtraAppendResult(ctx)
				extra.TimeTick, extra.LastConfirmedMessageID = uint64(10+appended), rmq.NewRmqID(1)
				return rmq.NewRmqID(int64(appended)), nil
			}
			_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), body, appendOp)
			require.NoError(t, err)
			count := uint64(1)
			if mixed {
				result, err := message.UpsertRetryResult(context.Background(), body)
				require.NoError(t, err)
				result = proto.Clone(result).(*messagespb.IdempotentInsertResult)
				result.RowOffsets = []uint32{1}
				header := &message.InsertMessageHeader{CollectionId: 1}
				message.SetInsertHeaderIdempotentInsertResult(header, result)
				second := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(header).WithBody(&msgpb.InsertRequest{NumRows: 1}).MustBuildMutable().WithTxnContext(txn)
				_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), second, appendOp)
				require.NoError(t, err)
				count++
			}
			commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey("sequential").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: count}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn)
			original := &utility.ExtraAppendResult{}
			id, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), original), commit, appendOp)
			require.NoError(t, err)
			for i := 0; i < 2; i++ {
				extra := &utility.ExtraAppendResult{}
				duplicate, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), extra), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
					t.Fatal("duplicate commit appended")
					return nil, nil
				})
				require.NoError(t, err)
				require.Equal(t, id, duplicate)
				result := extra.Extra.(*messagespb.IdempotentInsertResult)
				require.True(t, proto.Equal(original.Extra, result))
				require.Len(t, result.RowOffsets, int(count))
				require.Equal(t, int64(7), result.Ids.GetIntId().Data[0])
			}
			require.Equal(t, int(count)+1, appended)
		})
	}
}

func TestSequentialKeyedUpsertRejectsMissingBodyResult(t *testing.T) {
	for _, missingFirst := range []bool{true, false} {
		interceptor := newIdempotencyInterceptor(WindowConfig{MaxBytes: 1 << 20})
		txn := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
		complete := upsertForTest("p_1v0").WithTxnContext(txn)
		mapUpsert(complete, 0)
		missing := upsertForTest("p_1v0").WithTxnContext(txn)
		bodies := []message.MutableMessage{complete, missing}
		if missingFirst {
			bodies[0], bodies[1] = bodies[1], bodies[0]
		}
		for _, body := range bodies {
			_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{TimeTick: 10}), body, func(context.Context, message.MutableMessage) (message.MessageID, error) { return rmq.NewRmqID(1), nil })
			require.NoError(t, err)
		}
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithIdempotencyKey("missing-result").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn)
		_, err := interceptor.DoAppend(context.Background(), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("incomplete result appended")
			return nil, nil
		})
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
		require.Zero(t, interceptor.window("p_1v0").Len())
		require.Nil(t, interceptor.txnInsertResultBuffers.Build(commit))
		interceptor.Close()
	}
}
