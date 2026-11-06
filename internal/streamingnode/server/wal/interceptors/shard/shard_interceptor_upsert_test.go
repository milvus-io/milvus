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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type upsertSchemaManager struct {
	*mock_shards.MockShardManager
	schema                  *schemapb.CollectionSchema
	deleteRows, deleteBytes uint64
}

func (m *upsertSchemaManager) GetWriteSchema(id int64, vc string) (*schemapb.CollectionSchema, error) {
	return m.schema, nil
}

func (m *upsertSchemaManager) RecordDelete(vchannel string, tick, rows, size uint64) {
	m.deleteRows += rows
	m.deleteBytes += size
}

func (m *upsertSchemaManager) AssignSegments(requests []*shards.AssignSegmentRequest) ([]*shards.AssignSegmentResult, error) {
	results := make([]*shards.AssignSegmentResult, 0, len(requests))
	for _, request := range requests {
		result, err := m.AssignSegment(request)
		if err != nil {
			for _, assigned := range results {
				assigned.Ack()
			}
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func upsertForTest(vc string) message.MutableMessage {
	version := int32(1)
	return message.NewUpsertMessageBuilderV2().WithVChannel(vc).WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}, DeleteRows: 1}).WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 2, NumRows: 1, RowIDs: []int64{7}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}}}}}}, &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}})).MustBuildMutable().WithTimeTick(10)
}

func TestUpsertRejectsForeignOwnershipBeforeAppend(t *testing.T) {
	for _, vc := range []string{"", "p_01v0", "p_2v0"} {
		t.Run(vc, func(t *testing.T) {
			manager := mock_shards.NewMockShardManager(t)
			manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
			manager.EXPECT().GetWriteSchema(int64(1), vc).Return(nil, status.NewUnrecoverableError("collection is not present on vchannel")).Once()
			interceptor := &shardInterceptor{shardManager: manager}
			interceptor.initOpTable()
			_, err := interceptor.DoAppend(t.Context(), upsertForTest(vc), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("foreign ownership reached append")
				return nil, nil
			})
			require.Error(t, err)
		})
	}
}

func TestUpsertRejectsDuplicatedPrimaryColumnBeforeAppend(t *testing.T) {
	manager := mock_shards.NewMockShardManager(t)
	manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
	interceptor := &shardInterceptor{shardManager: &upsertSchemaManager{MockShardManager: manager, schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}}}
	msg := upsertForTest("p_1v0")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Columns = append(body.Columns, &schemapb.FieldData{FieldId: 100})
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
	body.DeleteEffects = nil
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

func TestUpsertRejectsNamespaceCollectionBeforeAssignment(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		manager := mock_shards.NewMockShardManager(t)
		manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
		interceptor := &shardInterceptor{shardManager: &upsertSchemaManager{MockShardManager: manager, schema: &schemapb.CollectionSchema{EnableNamespace: true}}}
		interceptor.initOpTable()
		msg := upsertForTest("p_1v0")
		if replicated {
			msg = msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", TimeTick: 10, MessageID: walimplstest.NewTestMessageID(10), LastConfirmedMessageID: walimplstest.NewTestMessageID(1)})
		}
		_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("namespace write reached backend append")
			return nil, nil
		})
		require.ErrorContains(t, err, "namespace")
	}
}
