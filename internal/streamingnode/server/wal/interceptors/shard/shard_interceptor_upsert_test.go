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

package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type upsertSchemaManager struct {
	*mock_shards.MockShardManager
	schema                  *schemapb.CollectionSchema
	deleteRows, deleteBytes uint64
}

func (m *upsertSchemaManager) GetWriteSchema(id int64, vc string) (*streamingpb.CollectionSchemaOfVChannel, error) {
	return &streamingpb.CollectionSchemaOfVChannel{Schema: m.schema}, nil
}

func (m *upsertSchemaManager) RecordDelete(vchannel string, tick, rows, size uint64) {
	m.deleteRows += rows
	m.deleteBytes += size
}

func upsertForTest(vc string) message.MutableMessage {
	version := int32(1)
	return message.NewUpsertMessageBuilderV2().WithVChannel(vc).WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partition: &message.PartitionSegmentAssignment{PartitionId: 2, Rows: 1}, DeleteRows: 1}).WithBody(&message.UpsertMessageBody{Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 2, NumRows: 1, RowIDs: []int64{7}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}}}}}}, Delete: &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}}).MustBuildMutable().WithTimeTick(10)
}

func TestUpsertRejectsInvalidScopeBeforeDispatch(t *testing.T) {
	for _, vc := range []string{"", "p_vcchan", "p_01v0", "p_2v0"} {
		t.Run(vc, func(t *testing.T) {
			i := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
			i.initOpTable()
			appended := false
			_, err := i.DoAppend(context.Background(), upsertForTest(vc), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return nil, nil
			})
			require.Error(t, err)
			require.False(t, appended)
		})
	}
}

func TestUpsertRejectsRowBasedPayloadBeforeAppend(t *testing.T) {
	manager := mock_shards.NewMockShardManager(t)
	manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
	interceptor := &shardInterceptor{shardManager: &upsertSchemaManager{MockShardManager: manager, schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}}}
	msg := upsertForTest("p_1v0")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Insert.Version = msgpb.InsertDataVersion_RowBased
	require.NotEmpty(t, body.Insert.GetFieldsData())
	typed.OverwriteBody(body)
	appended := false
	_, err := interceptor.handleUpsert(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return nil, nil
	})
	require.Error(t, err)
	require.False(t, appended)
}

func TestMalformedUpsertContractIsTerminal(t *testing.T) {
	manager := mock_shards.NewMockShardManager(t)
	manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
	shard := &shardInterceptor{shardManager: &upsertSchemaManager{MockShardManager: manager, schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}}}
	msg := upsertForTest("p_1v0")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Delete.PrimaryKeys.GetIntId().Data[0] = 8
	typed.OverwriteBody(body)
	_, err := shard.handleUpsert(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("invalid contract reached backend")
		return nil, nil
	})
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.True(t, status.AsStreamingError(upsertBodyDecodeError(message.ErrMalformedBody)).IsUnrecoverable())
	require.ErrorIs(t, upsertBodyDecodeError(context.DeadlineExceeded), context.DeadlineExceeded)
}

func TestUpsertEnvelopeRejectedBeforeShardEffects(t *testing.T) {
	for _, marker := range []string{"_np", "_pcl", "_bh", "_ur"} {
		t.Run(marker, func(t *testing.T) {
			interceptor := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
			interceptor.initOpTable()
			msg := upsertForTest("p_1v0")
			msg.Properties().ToRawMap()[marker] = ""
			_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("invalid envelope reached backend")
				return nil, nil
			})
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		})
	}
}

func TestCommitWithoutUpsertPassesThroughOnEveryChannel(t *testing.T) {
	for _, channel := range []string{"p_1v0", "p_vcchan", ""} {
		for _, withSession := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				interceptor := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
				interceptor.initOpTable()
				ctx := context.Background()
				if withSession {
					ctx = txn.WithTxnSession(ctx, &txn.TxnSession{})
				}
				msg := message.NewCommitTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable()
				expectedID := walimplstest.NewTestMessageID(12)
				var expectedError error
				if fail {
					expectedError = status.NewOnShutdownError("pass-through failure")
				}
				calls := 0
				id, err := interceptor.DoAppend(ctx, msg, func(actualCtx context.Context, actual message.MutableMessage) (message.MessageID, error) {
					calls++
					require.Equal(t, ctx, actualCtx)
					require.Same(t, msg, actual)
					return expectedID, expectedError
				})
				require.Equal(t, 1, calls)
				require.Equal(t, expectedID, id)
				require.ErrorIs(t, err, expectedError)
			}
		}
	}
}

func TestControlChannelDDLStillSkipsShardEffects(t *testing.T) {
	interceptor := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
	interceptor.initOpTable()
	msg := message.NewCreatePartitionMessageBuilderV1().WithVChannel("p_vcchan").WithHeader(&message.CreatePartitionMessageHeader{}).WithBody(&msgpb.CreatePartitionRequest{}).MustBuildMutable()
	calls := 0
	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		calls++
		return walimplstest.NewTestMessageID(1), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
