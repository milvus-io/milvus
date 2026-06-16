// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksCommitShardSplitRouting(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testSplitRouting" + funcutil.RandomString(10)

	// a single-shard collection so the post-split topology is exactly the
	// source plus the two split targets.
	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{{Name: "field1", DataType: schemapb.DataType_Int64}},
	})
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		ShardsNum:        1,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Len(t, coll.VirtualChannelNames, 1)
	source := coll.VirtualChannelNames[0]
	collID := coll.CollectionID
	t1, t2 := source+"_split1", source+"_split2"

	buildReq := func(sourceState, targetState pb.ShardState) *rootcoordpb.CommitShardSplitRoutingRequest {
		return &rootcoordpb.CommitShardSplitRoutingRequest{
			DbName:               dbName,
			CollectionName:       collectionName,
			CollectionId:         collID,
			VirtualChannelNames:  []string{source, t1, t2},
			PhysicalChannelNames: []string{funcutil.ToPhysicalChannel(source), funcutil.ToPhysicalChannel(t1), funcutil.ToPhysicalChannel(t2)},
			ShardInfos: []*pb.CollectionShardInfo{
				{State: sourceState},
				{RoutingKeyUpper: []byte{0x80}, State: targetState},
				{RoutingKeyLower: []byte{0x80}, State: targetState},
			},
			RoutingMode: pb.RoutingMode_RoutingModeRange,
		}
	}

	assertStates := func(sourceState, targetState pb.ShardState) {
		coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{source, t1, t2}, coll.VirtualChannelNames)
		require.Equal(t, pb.RoutingMode_RoutingModeRange, coll.RoutingMode)
		require.Equal(t, sourceState, coll.ShardInfos[source].State)
		require.Equal(t, targetState, coll.ShardInfos[t1].State)
		require.Equal(t, targetState, coll.ShardInfos[t2].State)
		require.Equal(t, []byte{0x80}, coll.ShardInfos[t1].RoutingKeyUpper)
		require.Equal(t, []byte{0x80}, coll.ShardInfos[t2].RoutingKeyLower)
	}

	// write-switch commit: source Splitting, two range targets Creating.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(pb.ShardState_ShardSplitting, pb.ShardState_ShardCreating))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(pb.ShardState_ShardSplitting, pb.ShardState_ShardCreating)

	// idempotent: re-committing the same states is a no-op success.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(pb.ShardState_ShardSplitting, pb.ShardState_ShardCreating))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(pb.ShardState_ShardSplitting, pb.ShardState_ShardCreating)

	// adoption commit: source Dropped, targets Normal.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(pb.ShardState_ShardDropped, pb.ShardState_ShardNormal))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(pb.ShardState_ShardDropped, pb.ShardState_ShardNormal)

	// empty collection name is rejected.
	resp, err = core.CommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{DbName: dbName, CollectionId: collID})
	require.Error(t, merr.CheckRPCCall(resp, err))

	// channel and shard-info arrays must be parallel.
	bad := buildReq(pb.ShardState_ShardSplitting, pb.ShardState_ShardCreating)
	bad.ShardInfos = bad.ShardInfos[:2]
	resp, err = core.CommitShardSplitRouting(ctx, bad)
	require.Error(t, merr.CheckRPCCall(resp, err))

	// a collection that does not exist is rejected.
	resp, err = core.CommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               util.DefaultDBName,
		CollectionName:       "does_not_exist",
		CollectionId:         424242,
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos:           []*pb.CollectionShardInfo{{State: pb.ShardState_ShardNormal}},
		RoutingMode:          pb.RoutingMode_RoutingModeRange,
	})
	require.Error(t, merr.CheckRPCCall(resp, err))
}

// TestCommitShardSplitRoutingValidation covers the request validation that
// returns before the broadcast, so it needs no streaming system or etcd.
func TestCommitShardSplitRoutingValidation(t *testing.T) {
	c := &Core{}
	ctx := context.Background()

	// empty collection name.
	err := c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// no vchannels.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{CollectionName: "c"})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// channel and shard-info arrays must be parallel.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:       "c",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos:           []*pb.CollectionShardInfo{{}, {}},
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}
