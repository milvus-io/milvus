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

package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksCollectionDDL(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	partitionName := "testPartition" + funcutil.RandomString(10)
	testSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "field1",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)

	// drop a collection that db not exist should be ignored.
	status, err := core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         "notExistDB",
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// drop a collection that collection not exist should be ignored.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: "notExistCollection",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// create a collection that database not exist should return error.
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "notExistDB",
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Test CreateCollection
	// create a database and a collection.
	status, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, coll.Name, collectionName)
	// create a collection with same schema should be idempotent.
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Test CreatePartition
	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, coll.Partitions, 2)
	require.Contains(t, lo.Map(coll.Partitions, func(p *model.Partition, _ int) string { return p.PartitionName }), partitionName)
	// create a partition with same name should be idempotent.
	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, coll.Partitions, 2)

	status, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	// drop a partition that partition not exist should be idempotent.
	status, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Test TruncateCollection
	// truncate a collection that collection not exist should return error.
	resp, err := core.TruncateCollection(ctx, &milvuspb.TruncateCollectionRequest{
		DbName:         dbName,
		CollectionName: "notExistCollection",
	})
	require.Error(t, merr.CheckRPCCall(resp.GetStatus(), err))
	// truncate the collection should be ok.
	resp, err = core.TruncateCollection(ctx, &milvuspb.TruncateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(resp.GetStatus(), err))
	// verify collection still exists after truncate
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, coll.Name, collectionName)
	require.Equal(t, 1, len(coll.ShardInfos))
	for _, shardInfo := range coll.ShardInfos {
		require.Greater(t, shardInfo.LastTruncateTimeTick, uint64(0))
	}

	// Test DropCollection
	// drop the collection should be ok.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	_, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.Error(t, err)
	// drop a dropped collection should be idempotent.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

// A WAL append is not proof that Flusher has consumed the preceding encrypted DML.
// Keep the database non-droppable until every channel has acknowledged the drop.
func TestDropCollectionWaitsForConsumingAck(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbName := "dropAckDB" + funcutil.RandomString(10)
	collectionName := "dropAckCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)

	b, err := balance.GetWithContext(ctx)
	require.NoError(t, err)
	b.(*mock_balancer.MockBalancer).EXPECT().ReplicateRole().Return(replicateutil.RolePrimary)
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Once()
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mixCoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	mixCoord.Set(idalloc.NewMockRootCoordClient(t))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptMixCoordClient(mixCoord))

	// Use the real broadcaster, FastAck decision and RootCoord callback with
	// mocked storage, while explicitly controlling consumer acknowledgements.
	originalWAL := streaming.WAL()
	controlChannel := originalWAL.ControlChannel()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(controlChannel)
	channelCount := len(coll.VirtualChannelNames) + 1
	appended := make(chan message.ImmutableMessage, channelCount)
	matchers := make([]interface{}, channelCount)
	for i := range matchers {
		matchers[i] = mock.Anything
	}
	wal.EXPECT().AppendMessages(mock.Anything, matchers...).RunAndReturn(
		func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			responses := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i, msg := range msgs {
				id := rmq.NewRmqID(int64(i + 1))
				tt := tsoutil.ComposeTSByTime(time.Now(), 0)
				responses.Responses[i].AppendResult = &types.AppendResult{
					MessageID: id, LastConfirmedMessageID: id, TimeTick: tt,
				}
				appended <- msg.WithTimeTick(tt).WithLastConfirmed(id).IntoImmutableMessage(id)
			}
			return responses
		}).Once()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(originalWAL)
	bc, err := broadcaster.RecoverBroadcaster(ctx)
	require.NoError(t, err)
	broadcast.ResetBroadcaster()
	broadcast.Register(bc)
	defer broadcast.ResetBroadcaster()

	dropped := make(chan error, 1)
	go func() {
		status, err := core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			DbName: dbName, CollectionName: collectionName,
		})
		dropped <- merr.CheckRPCCall(status, err)
	}()
	messages := make([]message.ImmutableMessage, 0, channelCount)
	for range channelCount {
		select {
		case msg := <-appended:
			messages = append(messages, msg)
		case <-ctx.Done():
			t.Fatal("DropCollection did not reach WAL append")
		}
	}
	assertPending := func() {
		t.Helper()
		select {
		case err := <-dropped:
			t.Fatalf("DropCollection completed before all consuming acknowledgements: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		_, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
		require.NoError(t, err)
		// DropDatabase checks this before RemoveEZByDBProperties.
		require.Error(t, core.meta.CheckIfDatabaseDroppable(ctx, &milvuspb.DropDatabaseRequest{DbName: dbName}))
	}
	assertPending()
	for i, msg := range messages {
		require.NoError(t, bc.Ack(ctx, msg))
		if i < len(messages)-1 {
			assertPending()
		}
	}
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("DropCollection did not finish after all consuming acknowledgements")
	}
	require.NoError(t, core.meta.CheckIfDatabaseDroppable(ctx, &milvuspb.DropDatabaseRequest{DbName: dbName}))
}

func TestCreatePartitionAckCallbackSyncsLoadedPartition(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	partitionName := "testPartition" + funcutil.RandomString(10)
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{Name: "field1", DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)

	status, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(status, err))
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// The first sync fails, so the ack callback must retry until it succeeds.
	type syncCall struct{ collectionID, partitionID int64 }
	var calls []syncCall
	broker := newValidMockBroker()
	broker.SyncNewCreatedPartitionFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID) error {
		calls = append(calls, syncCall{collectionID, partitionID})
		if len(calls) == 1 {
			return merr.WrapErrServiceNotReady("querycoord", 0, "initializing")
		}
		return nil
	}
	core.broker = broker

	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	partition, ok := lo.Find(coll.Partitions, func(p *model.Partition) bool { return p.PartitionName == partitionName })
	require.True(t, ok)
	partitionID := partition.PartitionID
	require.Len(t, calls, 2)
	for _, call := range calls {
		require.Equal(t, syncCall{coll.CollectionID, partitionID}, call)
	}
}
