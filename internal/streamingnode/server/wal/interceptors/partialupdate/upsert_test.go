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

package partialupdate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func trackingUpsert() message.MutableMessage {
	version := int32(1)
	return message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{CollectionId: 10, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 2}}, DeleteRows: 2}).WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 10, PartitionID: 2, NumRows: 2, RowIDs: []int64{1, 2}, FieldsData: []*schemapb.FieldData{int64PKFieldData(10, 20)}}, &msgpb.DeleteRequest{CollectionID: 10, PartitionID: -1, NumRows: 2, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{20, 10}}}}})).MustBuildMutable()
}

func TestUpsertTrackingMutableAndRecovered(t *testing.T) {
	getter := &staticPrimaryKeyDescriptorGetter{descriptor: shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64}}
	mutable := trackingUpsert()
	for _, msg := range []message.BasicMessage{mutable, mutable.IntoImmutableMessage(rmq.NewRmqID(1))} {
		keys, err := extractPKsFromInsertOrUpsert(context.Background(), msg, getter)
		require.NoError(t, err)
		require.Equal(t, []int64{10, 20, 20, 10}, keys.Int64Values)
	}
	invalid := trackingUpsert()
	typed := message.MustAsMutableUpsertMessageV2(invalid)
	body := typed.MustBody()
	body.DeleteEffects[0].GetPrimaryKeys().GetIntId().Data[0] = 30
	typed.OverwriteBody(body)
	keys, err := extractPKsFromInsertOrUpsert(context.Background(), invalid, getter)
	require.NoError(t, err)
	require.Contains(t, keys.Int64Values, int64(30), "independent delete keys participate in RowLock")
}

func TestUpsertPublishesOnlyAfterSuccessfulAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := trackingUpsert().WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, context.Canceled })
	require.Error(t, err)
	require.NoError(t, interceptor.state.pkVersions.Verify("v1", []any{int64(10), int64(20)}, 119, 121))
	_, err = interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(10)}, 119, 121))
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(20)}, 119, 121))
	require.NoError(t, interceptor.state.pkVersions.Verify("v1", []any{int64(30)}, 119, 121))
}

func TestUpsertWaitsForCASOnExactKeys(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	interceptor.state.rowLocks = newRowLockManager()
	for _, key := range []int64{10, 20, 30} {
		release, err := interceptor.state.rowLocks.acquire(context.Background(), "v1", primarykey.Keys{Kind: primarykey.KindInt64, Int64Values: []int64{key}}, true)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, unlock, err := interceptor.Acquire(ctx, trackingUpsert())
		cancel()
		if key == 30 {
			require.NoError(t, err)
			unlock()
		} else {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		release()
	}
}

func TestUpsertRecoveredWriteSetStagesUntilCommit(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := trackingUpsert().WithTimeTick(110).IntoImmutableMessage(rmq.NewRmqID(1))
	require.NoError(t, interceptor.recoverBody(context.Background(), 1, msg))
	require.Equal(t, []int64{10, 20, 20, 10}, interceptor.state.getTxn(1).pks.Int64Values)
	require.NoError(t, interceptor.state.pkVersions.Verify("v1", []any{int64(10)}, 109, 121))
	interceptor.state.recordTxnBegin(1)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), commit, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(20)}, 119, 121))
	require.Nil(t, interceptor.state.getTxn(1))
}

func TestRepeatedUpsertKeysRemainOrdinaryWritesWithCASEnabled(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	interceptor.state.rowLocks = newRowLockManager()
	msg := trackingUpsert().WithTimeTick(120)
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Rows.PrimaryKeys.GetIntId().Data = []int64{10, 10}
	body.DeleteEffects[0].GetPrimaryKeys().GetIntId().Data = []int64{10, 10}
	typed.OverwriteBody(body)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, unlock, err := interceptor.Acquire(ctx, msg)
	require.NoError(t, err)
	defer unlock()
	_, err = interceptor.DoAppend(ctx, msg, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(10)}, 119, 121))
	require.NoError(t, interceptor.state.pkVersions.Verify("v1", []any{int64(20)}, 119, 121))
}

func TestUpsertPKExtractionOnlyChecksTrackingKeys(t *testing.T) {
	getter := &staticPrimaryKeyDescriptorGetter{descriptor: shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64}}
	msg := trackingUpsert()
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Rows.PartitionIndexes = []uint32{99}
	body.Rows.RequestRowMapping = nil
	body.Rows.Columns = append(body.Rows.Columns, &schemapb.FieldData{FieldId: 100})
	typed.OverwriteBody(body)
	keys, err := extractPKsFromInsertOrUpsert(t.Context(), msg, getter)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 20, 10}, keys.Int64Values)
	strings := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"foreign"}}}}
	body.Rows.PrimaryKeys = strings
	typed.OverwriteBody(body)
	_, err = extractPKsFromInsertOrUpsert(t.Context(), msg, getter)
	require.ErrorContains(t, err, "primary key type")
	body.Rows.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 20}}}}
	body.DeleteEffects[0].KeySource = &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: strings}
	typed.OverwriteBody(body)
	_, err = extractPKsFromInsertOrUpsert(t.Context(), msg, getter)
	require.ErrorContains(t, err, "primary key type")
}

func TestInsertV2OrdinaryTrackingMutableAndRecovered(t *testing.T) {
	getter := &staticPrimaryKeyDescriptorGetter{descriptor: shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64}}
	upsert := message.MustAsMutableUpsertMessageV2(trackingUpsert())
	h := upsert.Header()
	mutable := message.NewInsertMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.InsertMessageHeaderV2{CollectionId: h.CollectionId, SchemaVersion: h.SchemaVersion, Partitions: h.Partitions}).
		WithBody(&message.InsertMessageBody{Rows: upsert.MustBody().GetRows()}).MustBuildMutable()
	for _, msg := range []message.BasicMessage{mutable, mutable.IntoImmutableMessage(rmq.NewRmqID(1))} {
		keys, err := extractPKsFromOrdinaryInsertWithContext(t.Context(), msg, getter)
		require.NoError(t, err)
		require.Equal(t, []int64{10, 20}, keys.Int64Values)
	}
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	_, err := interceptor.DoAppend(t.Context(), mutable.WithTimeTick(120), appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(10)}, 119, 121))
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{int64(20)}, 119, 121))
}
