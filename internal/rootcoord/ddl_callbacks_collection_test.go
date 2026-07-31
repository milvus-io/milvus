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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type dataViewDropTrackingMixCoord struct {
	types.MixCoord
	droppedCollectionIDs       []int64
	finalizedCollectionIDs     []int64
	dropCollectionDataView     func(context.Context, int64) error
	finalizeCollectionDataView func(context.Context, int64) error
}

func (m *dataViewDropTrackingMixCoord) FinalizeDropCollectionDataView(ctx context.Context, collectionID int64) error {
	m.finalizedCollectionIDs = append(m.finalizedCollectionIDs, collectionID)
	if m.finalizeCollectionDataView != nil {
		return m.finalizeCollectionDataView(ctx, collectionID)
	}
	return nil
}

func (m *dataViewDropTrackingMixCoord) DropCollectionDataView(ctx context.Context, collectionID int64) error {
	m.droppedCollectionIDs = append(m.droppedCollectionIDs, collectionID)
	if m.dropCollectionDataView != nil {
		return m.dropCollectionDataView(ctx, collectionID)
	}
	return nil
}

func TestDDLCallbacksCollectionDDL(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	dropAttempts := 0
	finalizeAttempts := 0
	mixCoord := &dataViewDropTrackingMixCoord{
		MixCoord: core.mixCoord,
		dropCollectionDataView: func(context.Context, int64) error {
			dropAttempts++
			if dropAttempts == 1 {
				return merr.WrapErrServiceInternalMsg("injected data view drop failure")
			}
			return nil
		},
		finalizeCollectionDataView: func(ctx context.Context, collectionID int64) error {
			finalizeAttempts++
			_, err := core.meta.GetCollectionByID(ctx, "", collectionID, typeutil.MaxTimestamp, false)
			require.Error(t, err, "the terminal marker must be finalized after RootCoord metadata is dropped")
			if finalizeAttempts == 1 {
				return merr.WrapErrServiceInternalMsg("injected data view finalize failure")
			}
			return nil
		},
	}
	core.mixCoord = mixCoord

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
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
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
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
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
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
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
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
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
	require.Equal(t, []int64{coll.CollectionID, coll.CollectionID, coll.CollectionID}, mixCoord.droppedCollectionIDs)
	require.Equal(t, []int64{coll.CollectionID, coll.CollectionID}, mixCoord.finalizedCollectionIDs)
	_, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.Error(t, err)
	// drop a dropped collection should be idempotent.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

func TestDropCollectionAckOnceCallbackDropsVirtualChannel(t *testing.T) {
	ctx := context.Background()
	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
			require.Equal(t, "v1", req.GetChannelName())
			return &datapb.DropVirtualChannelResponse{Status: merr.Success()}, nil
		})

	callback := &DDLCallback{Core: &Core{mixCoord: mixCoord}}
	err := callback.dropCollectionV1AckOnceCallback(ctx, buildDropCollectionAckResult("v1"))
	require.NoError(t, err)
}

func buildDropCollectionAckResult(vchannel string) message.AckResultDropCollectionMessageV1 {
	msg := message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 100}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))
	return message.AckResultDropCollectionMessageV1{
		Message: message.MustAsImmutableDropCollectionMessageV1(msg),
	}
}

func TestCreatePartitionMaxCountIgnoresDroppedPartitions(t *testing.T) {
	Params.Save(Params.RootCoordCfg.MaxPartitionNum.Key, "2")
	defer Params.Reset(Params.RootCoordCfg.MaxPartitionNum.Key)

	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	testSchema := &schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "field1",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)

	status, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(status, err))
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  "partition_1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  "partition_1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, true)
	require.NoError(t, err)
	require.Len(t, coll.Partitions, 2)
	require.Equal(t, 1, coll.GetPartitionNum(true))

	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  "partition_2",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  "partition_3",
	})
	require.ErrorContains(t, merr.CheckRPCCall(status, err), "partition number (2) exceeds max configuration (2)")
}
