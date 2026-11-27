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
package shardsplit

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	testSource  = "by-dev-dml_0_100v0"
	testOther   = "by-dev-dml_1_100v1"
	testTarget1 = "by-dev-dml_2_100v2"
	testTarget2 = "by-dev-dml_3_100v3"
)

func newTestPlan(t *testing.T) *Plan {
	t.Helper()
	current := routing.Routing{Shards: []routing.Shard{{VChannel: testSource}, {VChannel: testOther}}}
	split, err := routing.PlanSplit(current, testSource, [2]string{testTarget1, testTarget2})
	require.NoError(t, err)
	split.ShardBy = "hash(pk)"
	return &Plan{
		SplitTaskID:    7,
		CollectionID:   100,
		CollectionName: "c",
		DBID:           1,
		DBName:         "default",
		Schema: &schemapb.CollectionSchema{
			Name:    "c",
			Version: 3,
			Fields:  []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
			Properties: []*commonpb.KeyValuePair{
				{Key: common.CollectionOnTruncatingKey, Value: "true"},
				{Key: "collection.ttl.seconds", Value: "10"},
			},
		},
		PartitionIDs: []int64{11, 12},
		Current:      current,
		Split:        split,
	}
}

func requireSystemError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err), err.Error())
}

func TestNewBroadcastMessage(t *testing.T) {
	plan := newTestPlan(t)
	msg, err := NewBroadcastMessage(plan)
	require.NoError(t, err)
	require.Equal(t, message.MessageTypeSplitShard, msg.MessageType())

	// The broadcast goes to the source and the two targets. The untouched
	// shard gets no copy. The source is appended first, and the message is
	// replicated and fast-acked.
	bh := msg.BroadcastHeader()
	require.ElementsMatch(t, []string{testSource, testTarget1, testTarget2}, bh.VChannels)
	require.Equal(t, []string{testSource}, bh.AppendFirstVChannels)
	require.False(t, bh.AckSyncUp)
	require.False(t, msg.IsUnreplicable())

	split := message.MustAsBroadcastSplitShardMessageV2(msg)
	header, body := split.Header(), split.MustBody()
	require.Equal(t, int64(100), header.GetCollectionId())
	require.Equal(t, int64(7), header.GetSplitTaskId())
	require.Equal(t, testSource, header.GetSourceVchannel())
	require.Equal(t, []string{testTarget1, testTarget2}, header.GetTargetVchannels())

	// The creation payload of a target.
	require.True(t, proto.Equal(&message.CreateCollectionMessageHeader{CollectionId: 100, PartitionIds: []int64{11, 12}, DbId: 1}, body.GetGenesisHeader()))
	genesis := body.GetGenesis()
	require.Equal(t, "default", genesis.GetDbName())
	require.Equal(t, "c", genesis.GetCollectionName())
	require.Equal(t, []int64{11, 12}, genesis.GetPartitionIDs())
	require.Empty(t, genesis.GetVirtualChannelNames())
	require.Equal(t, int32(3), genesis.GetCollectionSchema().GetVersion())
	// collection.on.truncating is not part of the schema of a vchannel, and
	// the schema of the plan is not changed by removing it.
	require.Len(t, genesis.GetCollectionSchema().GetProperties(), 1)
	require.Equal(t, "collection.ttl.seconds", genesis.GetCollectionSchema().GetProperties()[0].GetKey())
	require.Len(t, plan.Schema.GetProperties(), 2)

	// The routing after the split, carried whole: the targets in the place of
	// the source, and the untouched shard expressed at the new modulus.
	next, err := RoutingOf(body)
	require.NoError(t, err)
	require.True(t, next.Equal(routing.Routing{Modulus: 4, ShardBy: "hash(pk)", Shards: []routing.Shard{
		{VChannel: testTarget1, Buckets: []uint64{0}},
		{VChannel: testTarget2, Buckets: []uint64{2}},
		{VChannel: testOther, Buckets: []uint64{1, 3}},
	}}))

	// The message that was built passes the checks of the consumer.
	require.NoError(t, CheckMessage(header, body))
	require.NoError(t, CheckAgainstCollection(plan.Current, nil, plan.Schema.GetProperties(), header, body))
}

func TestNewBroadcastMessageRefusals(t *testing.T) {
	for name, mutate := range map[string]func(*Plan){
		"no split task id":           func(p *Plan) { p.SplitTaskID = 0 },
		"no schema":                  func(p *Plan) { p.Schema = nil },
		"source is not a shard":      func(p *Plan) { p.Split.Source = "by-dev-dml_9_100v9" },
		"a target owns nothing":      func(p *Plan) { p.Split.Targets[1].Buckets = nil },
		"target on a used pchannel":  func(p *Plan) { p.Split.Targets[1].VChannel = "by-dev-dml_1_100v3" },
		"target was split before":    func(p *Plan) { p.SplitSources = []string{testTarget2} },
		"pchannel of a split source": func(p *Plan) { p.SplitSources = []string{"by-dev-dml_3_100v7"} },
		"namespace sharding": func(p *Plan) {
			p.Schema.Properties = append(p.Schema.Properties, &commonpb.KeyValuePair{Key: common.NamespaceShardingEnabledKey, Value: "true"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := newTestPlan(t)
			mutate(plan)
			_, err := NewBroadcastMessage(plan)
			requireSystemError(t, err)
		})
	}
}

func TestCheckMessage(t *testing.T) {
	build := func() (*message.SplitShardMessageHeader, *message.SplitShardMessageBody) {
		msg, err := NewBroadcastMessage(newTestPlan(t))
		require.NoError(t, err)
		split := message.MustAsBroadcastSplitShardMessageV2(msg)
		return split.Header(), split.MustBody()
	}
	header, body := build()
	require.NoError(t, CheckMessage(header, body))

	for name, mutate := range map[string]func(*message.SplitShardMessageHeader, *message.SplitShardMessageBody){
		"no split task id": func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) { h.SplitTaskId = 0 },
		"no source":        func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) { h.SourceVchannel = "" },
		"one target": func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) {
			h.TargetVchannels = h.TargetVchannels[:1]
		},
		"no creation header": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) { b.GenesisHeader = nil },
		"no creation schema": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Genesis.CollectionSchema = nil
		},
		"another collection": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.GenesisHeader.CollectionId = 9
		},
		"no routing": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) { b.Routing = nil },
		"unnamed shard": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Routing.Shards[0].VchannelName = ""
		},
		"routing does not tile": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Routing.RoutingModulus = 8
		},
		"routing lists source": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Routing.Shards[2].VchannelName = testSource
		},
		"routing misses target": func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) {
			h.TargetVchannels[1] = "by-dev-dml_5_100v5"
		},
		"shard in another state": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Routing.Shards[2].State = schemapb.ShardState_ShardSplitting
		},
	} {
		t.Run(name, func(t *testing.T) {
			header, body := build()
			mutate(header, body)
			requireSystemError(t, CheckMessage(header, body))
		})
	}
}

func TestCheckAgainstCollection(t *testing.T) {
	plan := newTestPlan(t)
	msg, err := NewBroadcastMessage(plan)
	require.NoError(t, err)
	split := message.MustAsBroadcastSplitShardMessageV2(msg)
	header, body := split.Header(), split.MustBody()

	// The collection places rows by namespace.
	requireSystemError(t, CheckAgainstCollection(plan.Current, nil,
		[]*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "true"}}, header, body))
	// The collection has no such source any more: the split was applied.
	next, err := RoutingOf(body)
	require.NoError(t, err)
	requireSystemError(t, CheckAgainstCollection(next, []string{testSource}, nil, header, body))
	// The collection changed since the message was built: it has another
	// shard, which the routing of the message does not know.
	grown := routing.Routing{Shards: []routing.Shard{{VChannel: testSource}, {VChannel: testOther}, {VChannel: "by-dev-dml_4_100v4"}}}
	requireSystemError(t, CheckAgainstCollection(grown, nil, nil, header, body))
	// The routing of the message is malformed.
	broken := proto.Clone(body).(*message.SplitShardMessageBody)
	broken.Routing.Shards[0].VchannelName = ""
	requireSystemError(t, CheckAgainstCollection(plan.Current, nil, nil, header, broken))
}

func TestBroadcast(t *testing.T) {
	ctx := context.Background()
	want := &types.BroadcastAppendResult{BroadcastID: 9}

	t.Run("builds the message from the plan read under the lock", func(t *testing.T) {
		api := mock_broadcaster.NewMockBroadcastAPI(t)
		var keys []message.ResourceKey
		start := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(func(_ context.Context, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			keys = resourceKeys
			return api, nil
		}).Build()
		defer start.UnPatch()
		locked := false
		api.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			require.True(t, locked, "the plan is read before the broadcast")
			require.Equal(t, message.MessageTypeSplitShard, msg.MessageType())
			require.Equal(t, []string{testSource}, msg.BroadcastHeader().AppendFirstVChannels)
			return want, nil
		}).Once()
		api.EXPECT().Close().Return().Once()

		got, err := Broadcast(ctx, "default", "c", func(context.Context) (*Plan, error) {
			require.NotNil(t, keys, "the plan is read while the resource keys are held")
			locked = true
			return newTestPlan(t), nil
		})
		require.NoError(t, err)
		require.Same(t, want, got)
		// The same keys every collection DDL takes.
		require.ElementsMatch(t, []message.ResourceKey{
			message.NewSharedDBNameResourceKey("default"),
			message.NewExclusiveCollectionNameResourceKey("default", "c"),
		}, keys)
	})

	t.Run("the lock cannot be taken", func(t *testing.T) {
		start := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(nil, errors.New("not primary")).Build()
		defer start.UnPatch()
		_, err := Broadcast(ctx, "default", "c", func(context.Context) (*Plan, error) {
			t.Fatal("the plan must not be read without the lock")
			return nil, nil
		})
		require.Error(t, err)
	})

	t.Run("the plan cannot be built, or is refused", func(t *testing.T) {
		api := mock_broadcaster.NewMockBroadcastAPI(t)
		start := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
		defer start.UnPatch()
		// Nothing is broadcast, and the resource keys are released.
		api.EXPECT().Close().Return().Times(2)

		planErr := errors.New("collection is gone")
		_, err := Broadcast(ctx, "default", "c", func(context.Context) (*Plan, error) { return nil, planErr })
		require.ErrorIs(t, err, planErr)

		_, err = Broadcast(ctx, "default", "c", func(context.Context) (*Plan, error) {
			plan := newTestPlan(t)
			plan.SplitTaskID = 0
			return plan, nil
		})
		requireSystemError(t, err)
	})

	t.Run("the broadcast fails", func(t *testing.T) {
		api := mock_broadcaster.NewMockBroadcastAPI(t)
		start := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
		defer start.UnPatch()
		broadcastErr := errors.New("broadcast failed")
		api.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, broadcastErr).Once()
		api.EXPECT().Close().Return().Once()
		_, err := Broadcast(ctx, "default", "c", func(context.Context) (*Plan, error) { return newTestPlan(t), nil })
		require.ErrorIs(t, err, broadcastErr)
	})
}
