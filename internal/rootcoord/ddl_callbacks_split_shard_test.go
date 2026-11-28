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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/shardsplit"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitShardTestEnv is a RootCoord with one collection of two shards.
type splitShardTestEnv struct {
	t        *testing.T
	core     *Core
	callback *DDLCallback
	dbName   string
	name     string
	collID   int64
	source   string
	other    string
	targets  [2]string
	// expired counts the proxy cache invalidations the callback issued.
	expired int
	// expireErr, if set, fails every proxy cache invalidation.
	expireErr error
}

func newSplitShardTestEnv(t *testing.T, properties ...*commonpb.KeyValuePair) *splitShardTestEnv {
	mockQueryViewDDLResource(t)
	core := initStreamingSystemAndCore(t)
	t.Cleanup(func() { RegisterSplitShardAckHook(nil) })
	ctx := context.Background()
	env := &splitShardTestEnv{
		t: t, core: core, callback: &DDLCallback{Core: core},
		dbName: "db" + funcutil.RandomString(8), name: "coll" + funcutil.RandomString(8),
	}
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
		Name: env.name,
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	})
	require.NoError(t, err)
	status, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: env.dbName})
	require.NoError(t, merr.CheckRPCCall(status, err))
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: env.dbName, CollectionName: env.name, Schema: schemaBytes, ShardsNum: 2, Properties: properties,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	coll := env.collection()
	require.Len(t, coll.VirtualChannelNames, 2)
	env.collID = coll.CollectionID
	env.source, env.other = coll.VirtualChannelNames[0], coll.VirtualChannelNames[1]
	// Two vchannels on pchannels that hold no vchannel of the collection.
	env.targets = [2]string{env.vchannelOn(5, 2), env.vchannelOn(6, 3)}

	patch := mockey.Mock((*DDLCallback).ExpireCaches).To(func(*DDLCallback, context.Context, any) error {
		if env.expireErr != nil {
			return env.expireErr
		}
		env.expired++
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return env
}

// vchannelOn names a vchannel of the collection on another pchannel of the
// test cluster.
func (env *splitShardTestEnv) vchannelOn(pchannelIdx int, shardIdx int) string {
	pchannel := funcutil.ToPhysicalChannel(env.source)
	require.Contains(env.t, pchannel, "-rootcoord-dml_0_")
	pchannel = strings.Replace(pchannel, "-rootcoord-dml_0_", "-rootcoord-dml_"+string(rune('0'+pchannelIdx))+"_", 1)
	return funcutil.GetVirtualChannel(pchannel, env.collID, shardIdx)
}

func (env *splitShardTestEnv) collection() *model.Collection {
	coll, err := env.core.meta.GetCollectionByName(context.Background(), env.dbName, env.name, typeutil.MaxTimestamp, false)
	require.NoError(env.t, err)
	return coll
}

// newParts builds the header and the body of the SplitShard message of a
// split of source into targets, from the collection as it is now.
func (env *splitShardTestEnv) newParts(taskID int64, source string, targets [2]string) (*message.SplitShardMessageHeader, *message.SplitShardMessageBody) {
	plan, err := newSplitShardPlan(env.collection(), &SplitShardRequest{
		CollectionID: env.collID, SplitTaskID: taskID, Source: source, Targets: targets, ShardBy: "hash(pk)",
	})
	require.NoError(env.t, err)
	msg, err := shardsplit.NewBroadcastMessage(plan)
	require.NoError(env.t, err)
	split := message.MustAsBroadcastSplitShardMessageV2(msg)
	return split.Header(), split.MustBody()
}

// newResult builds the acknowledged SplitShard message of a split of source
// into targets, as the ack callback receives it.
func (env *splitShardTestEnv) newResult(taskID int64, source string, targets [2]string, ticks [4]uint64) message.BroadcastResultSplitShardMessageV2 {
	header, body := env.newParts(taskID, source, targets)
	return newSplitShardResult(header, body, ticks)
}

// newSplitShardResult builds the acknowledged message from a header and a
// body. ticks are the time ticks of the copies: source, first target, second
// target, control channel.
func newSplitShardResult(header *message.SplitShardMessageHeader, body *message.SplitShardMessageBody, ticks [4]uint64) message.BroadcastResultSplitShardMessageV2 {
	control := funcutil.GetControlChannel("by-dev-rootcoord-dml_0")
	vchannels := append([]string{header.GetSourceVchannel()}, header.GetTargetVchannels()...)
	msg := message.NewSplitShardMessageBuilderV2().WithHeader(header).WithBody(body).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(header.GetSourceVchannel())).
		MustBuildBroadcast().WithBroadcastID(1)
	msg = message.WithBroadcastControlChannel(msg, control)
	results := map[string]*message.AppendResult{}
	for idx, vchannel := range append(vchannels, control) {
		results[vchannel] = &message.AppendResult{
			MessageID: rmq.NewRmqID(int64(ticks[idx])), LastConfirmedMessageID: rmq.NewRmqID(int64(ticks[idx])), TimeTick: ticks[idx],
		}
	}
	return message.BroadcastResultSplitShardMessageV2{
		Message: message.MustAsBroadcastSplitShardMessageV2(msg),
		Results: results,
	}
}

func TestSplitShardAckCallbackAppliesTheSplitOnce(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	before := env.collection()
	generalBefore := env.core.meta.GetGeneralCount(ctx)
	stats := channel.StaticPChannelStatsManager.MustGet()
	sourcePChannel, targetPChannel := funcutil.ToPhysicalChannel(env.source), funcutil.ToPhysicalChannel(env.targets[0])
	countOn := func(pchannel string) int {
		return stats.GetPChannelStats(types.ChannelID{Name: pchannel}).VChannelCount()
	}
	sourceCountBefore, targetCountBefore := countOn(sourcePChannel), countOn(targetPChannel)

	origin := env.core.meta.(*MetaTable).catalog
	counting := &alterCountingCatalog{RootCoordCatalog: origin}
	env.core.meta.(*MetaTable).catalog = counting

	// The source at 1000, the two genesis ticks above it, the control copy last.
	result := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))

	after := env.collection()
	// One meta write. The source left the DML routing table and the DDL
	// broadcast set: both are taken from the vchannel list.
	require.Equal(t, 1, counting.alters)
	require.Equal(t, []string{env.targets[0], env.targets[1], env.other}, after.VirtualChannelNames)
	require.NotContains(t, after.VirtualChannelNames, env.source)
	require.NotContains(t, after.ShardInfos, env.source)
	require.Equal(t, int32(3), after.ShardsNum)
	// The targets are added with their residues and the modulus; the shard
	// that was not split keeps every key.
	require.Equal(t, uint64(4), after.RoutingModulus)
	require.Equal(t, "hash(pk)", after.ShardBy)
	require.Equal(t, []uint64{0}, after.ShardInfos[env.targets[0]].Buckets)
	require.Equal(t, []uint64{2}, after.ShardInfos[env.targets[1]].Buckets)
	require.Equal(t, []uint64{1, 3}, after.ShardInfos[env.other].Buckets)
	require.Equal(t, funcutil.ToPhysicalChannel(env.targets[0]), after.ShardInfos[env.targets[0]].PChannelName)
	// The source is recorded as split, with its lineage: T_switch is the time
	// tick of the copy on the source, and both genesis ticks are above it.
	wantLineage := &model.ShardSplitLineage{
		SourceVChannel:  env.source,
		SourcePChannel:  before.ShardInfos[env.source].PChannelName,
		TargetVChannels: env.targets[:],
		SwitchTimeTick:  1000,
		SplitTaskID:     7,
	}
	require.Equal(t, []*model.ShardSplitLineage{wantLineage}, after.SplitLineages)
	require.Less(t, wantLineage.SwitchTimeTick, result.Results[env.targets[0]].TimeTick)
	require.Less(t, wantLineage.SwitchTimeTick, result.Results[env.targets[1]].TimeTick)
	require.Equal(t, uint64(1030), after.UpdateTimestamp)
	// Then the proxy caches are invalidated.
	require.Equal(t, 1, env.expired)
	// The bookkeeping follows: one more shard, the targets counted on their
	// pchannels and the source not counted any more.
	require.Equal(t, generalBefore+after.GetPartitionNum(true), env.core.meta.GetGeneralCount(ctx))
	require.Equal(t, sourceCountBefore-1, countOn(sourcePChannel))
	require.Equal(t, targetCountBefore+1, countOn(targetPChannel))

	// DescribeCollection reports the routing: the three shards that take DML,
	// then the source in state ShardSplitting with the residues it owned.
	desc := convertModelToDesc(after, nil, env.dbName)
	require.Equal(t, after.VirtualChannelNames, desc.GetVirtualChannelNames())
	require.Equal(t, uint64(4), desc.GetRoutingModulus())
	require.Len(t, desc.GetShardInfos(), 4)
	require.Equal(t, schemapb.ShardState_ShardSplitting, desc.GetShardInfos()[3].GetState())
	require.Equal(t, env.source, desc.GetShardInfos()[3].GetVchannelName())

	// The record survives a reload of the collection from the catalog.
	reloaded, err := origin.GetCollectionByID(ctx, after.DBID, typeutil.MaxTimestamp, after.CollectionID)
	require.NoError(t, err)
	require.Equal(t, after.VirtualChannelNames, reloaded.VirtualChannelNames)
	require.Equal(t, after.SplitLineages, reloaded.SplitLineages)
	require.True(t, after.Routing().Equal(reloaded.Routing()))

	// The callback is entered again, as after a coordinator restart, and this
	// time the copies report other time ticks. Nothing is written, the lineage
	// of the source is unchanged, and the proxy caches are invalidated again.
	again := newSplitShardResult(result.Message.Header(), result.Message.MustBody(), [4]uint64{2000, 2010, 2020, 2030})
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, again))
	second := env.collection()
	require.Equal(t, 1, counting.alters)
	require.Equal(t, []*model.ShardSplitLineage{wantLineage}, second.SplitLineages)
	require.Equal(t, after.VirtualChannelNames, second.VirtualChannelNames)
	require.Equal(t, uint64(1030), second.UpdateTimestamp)
	require.True(t, after.Routing().Equal(second.Routing()))
	require.Equal(t, 2, env.expired)

	// A DDL of the collection after the split works on the new vchannel list.
	status, err := env.core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName: env.dbName, CollectionName: env.name, PartitionName: "p1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

// alterCountingCatalog counts the collection writes of a catalog.
type alterCountingCatalog struct {
	metastore.RootCoordCatalog
	alters int
}

func (c *alterCountingCatalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	c.alters++
	return c.RootCoordCatalog.AlterCollection(ctx, oldColl, newColl, alterType, ts, fieldModify)
}

func TestSplitShardAckCallbackRunsTheHookBeforeTheMetaWrite(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	result := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})

	hookErr := errors.New("datacoord is not ready")
	calls := 0
	RegisterSplitShardAckHook(func(_ context.Context, got message.BroadcastResultSplitShardMessageV2) error {
		calls++
		require.Equal(t, int64(7), got.Message.Header().GetSplitTaskId())
		// The routing is not published while the hook runs for the first time.
		if calls <= 2 {
			require.Contains(t, env.collection().VirtualChannelNames, env.source)
		}
		if calls == 1 {
			return hookErr
		}
		return nil
	})

	// A failing hook fails the callback, and nothing is written.
	require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, result), hookErr)
	require.Contains(t, env.collection().VirtualChannelNames, env.source)
	require.Empty(t, env.collection().SplitLineages)
	require.Zero(t, env.expired)

	// The retry runs the hook again and then applies the split.
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
	require.Equal(t, 2, calls)
	require.NotContains(t, env.collection().VirtualChannelNames, env.source)
	require.Equal(t, 1, env.expired)

	// A callback that is entered again runs the hook again: a restart may have
	// happened between the hook and the meta write, and the hook is idempotent.
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
	require.Equal(t, 3, calls)
	require.Len(t, env.collection().SplitLineages, 1)
}

func TestSplitShardAckCallbackRefusesAMessageItCannotApply(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	hooked := false
	RegisterSplitShardAckHook(func(context.Context, message.BroadcastResultSplitShardMessageV2) error {
		hooked = true
		return nil
	})
	valid := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})

	requireRefused := func(result message.BroadcastResultSplitShardMessageV2) {
		t.Helper()
		before := env.collection()
		err := env.callback.splitShardV2AckCallback(ctx, result)
		require.Error(t, err)
		// A refusal is never the fault of a request, and is retried.
		require.Equal(t, merr.SystemError, merr.GetErrorType(err), err.Error())
		after := env.collection()
		require.Equal(t, before.VirtualChannelNames, after.VirtualChannelNames)
		require.Empty(t, after.SplitLineages)
		require.False(t, hooked, "the other coordinators are not asked to act on a message that cannot be applied")
		require.Zero(t, env.expired)
	}

	// The source copy was acknowledged without a time tick.
	missing := newSplitShardResult(valid.Message.Header(), valid.Message.MustBody(), [4]uint64{0, 1010, 1020, 1030})
	requireRefused(missing)
	delete(missing.Results, env.source)
	requireRefused(missing)

	// The routing of the message moves a key of the shard that is not split.
	header, body := env.newParts(7, env.source, env.targets)
	body.Routing.Shards[0].Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 1}}}
	body.Routing.Shards[2].Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{3}}}
	requireRefused(newSplitShardResult(header, body, [4]uint64{1000, 1010, 1020, 1030}))

	// A target on the pchannel of the shard that is not split. The message
	// cannot be built by the issuer; a consumer refuses it all the same.
	header, body = env.newParts(7, env.source, env.targets)
	onUsedPChannel := funcutil.GetVirtualChannel(funcutil.ToPhysicalChannel(env.other), env.collID, 9)
	header.TargetVchannels[1] = onUsedPChannel
	body.Routing.Shards[1].VchannelName = onUsedPChannel
	requireRefused(newSplitShardResult(header, body, [4]uint64{1000, 1010, 1020, 1030}))

	// The valid message still applies afterwards.
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, valid))
	require.True(t, hooked)

	// Another split task that names the source that is already split.
	foreignHeader := proto.Clone(valid.Message.Header()).(*message.SplitShardMessageHeader)
	foreignHeader.SplitTaskId = 8
	foreign := newSplitShardResult(foreignHeader, valid.Message.MustBody(), [4]uint64{3000, 3010, 3020, 3030})
	err := env.callback.splitShardV2AckCallback(ctx, foreign)
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	require.Len(t, env.collection().SplitLineages, 1)
	require.Equal(t, int64(7), env.collection().SplitLineages[0].SplitTaskID)
}

func TestSplitShardAckCallbackIgnoresACollectionThatIsGone(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	hooked := false
	RegisterSplitShardAckHook(func(context.Context, message.BroadcastResultSplitShardMessageV2) error {
		hooked = true
		return nil
	})
	result := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})

	status, err := env.core.DropCollection(ctx, &milvuspb.DropCollectionRequest{DbName: env.dbName, CollectionName: env.name})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// The drop invalidated the proxy caches itself.
	env.expired = 0

	// There is nothing left to split. The callback ends, so that the broadcast
	// does not keep the resource key for ever.
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
	require.False(t, hooked)
	require.Zero(t, env.expired)
}

func TestSplitShardAckCallbackOnASecondaryReachesTheSameRouting(t *testing.T) {
	// The RootCoord of this test is the secondary cluster. The primary holds
	// the same collection on vchannels of its own names.
	secondary := newSplitShardTestEnv(t)
	ctx := context.Background()
	toPrimary := func(vchannel string) string { return strings.Replace(vchannel, "-rootcoord-dml_", "-primary-dml_", 1) }
	toSecondary := func(vchannel string) string { return strings.Replace(vchannel, "-primary-dml_", "-rootcoord-dml_", 1) }

	primaryColl := secondary.collection().Clone()
	primaryColl.ShardInfos = map[string]*model.ShardInfo{}
	for idx, vchannel := range primaryColl.VirtualChannelNames {
		renamed := toPrimary(vchannel)
		require.NotEqual(t, vchannel, renamed)
		primaryColl.VirtualChannelNames[idx] = renamed
		primaryColl.PhysicalChannelNames[idx] = funcutil.ToPhysicalChannel(renamed)
		primaryColl.ShardInfos[renamed] = &model.ShardInfo{VChannelName: renamed, PChannelName: funcutil.ToPhysicalChannel(renamed)}
	}
	primarySource := toPrimary(secondary.source)
	primaryTargets := [2]string{toPrimary(secondary.targets[0]), toPrimary(secondary.targets[1])}

	// The primary issues the split and applies it at the time ticks of its WAL.
	plan, err := newSplitShardPlan(primaryColl, &SplitShardRequest{
		CollectionID: secondary.collID, SplitTaskID: 7, Source: primarySource, Targets: primaryTargets, ShardBy: "hash(pk)",
	})
	require.NoError(t, err)
	msg, err := shardsplit.NewBroadcastMessage(plan)
	require.NoError(t, err)
	split := message.MustAsBroadcastSplitShardMessageV2(msg)
	header, body := split.Header(), split.MustBody()
	primaryNext, err := shardsplit.RoutingOf(body)
	require.NoError(t, err)
	primaryColl.ApplyShardSplit(primaryNext, &model.ShardSplitLineage{
		SourceVChannel: primarySource, TargetVChannels: primaryTargets[:], SwitchTimeTick: 1000, SplitTaskID: 7,
	})

	// The message is replicated: the replicate service renames every vchannel
	// it names.
	header.SourceVchannel = toSecondary(header.SourceVchannel)
	for i := range header.TargetVchannels {
		header.TargetVchannels[i] = toSecondary(header.TargetVchannels[i])
	}
	for _, shard := range body.Routing.Shards {
		shard.VchannelName = toSecondary(shard.VchannelName)
	}

	// The secondary consumes it, with the time ticks of its own WAL. Its
	// replication streams are independent, so a target genesis can even be
	// below the fence there; the callback does not depend on that order.
	result := newSplitShardResult(header, body, [4]uint64{5000, 4990, 5010, 5020})
	require.NoError(t, secondary.callback.splitShardV2AckCallback(ctx, result))

	// The secondary reaches the routing of the primary, under its own names.
	got := secondary.collection()
	require.Equal(t, primaryColl.RoutingModulus, got.RoutingModulus)
	require.Equal(t, primaryColl.ShardBy, got.ShardBy)
	require.Equal(t, primaryColl.ShardsNum, got.ShardsNum)
	require.Len(t, got.VirtualChannelNames, len(primaryColl.VirtualChannelNames))
	for idx, vchannel := range primaryColl.VirtualChannelNames {
		require.Equal(t, toSecondary(vchannel), got.VirtualChannelNames[idx])
		require.Equal(t, primaryColl.ShardInfos[vchannel].Buckets, got.ShardInfos[toSecondary(vchannel)].Buckets)
	}
	require.NotContains(t, got.VirtualChannelNames, secondary.source)
	// The lineage is the same relation between shards. T_switch is the time
	// tick of the copy on the source in the WAL of the secondary.
	require.Len(t, got.SplitLineages, 1)
	require.Equal(t, secondary.source, got.SplitLineages[0].SourceVChannel)
	require.Equal(t, secondary.targets[:], got.SplitLineages[0].TargetVChannels)
	require.Equal(t, primaryColl.SplitLineages[0].SplitTaskID, got.SplitLineages[0].SplitTaskID)
	require.Equal(t, uint64(5000), got.SplitLineages[0].SwitchTimeTick)
	require.Equal(t, 1, secondary.expired)
}

func TestBroadcastSplitShard(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()

	// A collection that does not exist.
	_, err := env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID + 1000, SplitTaskID: 7, Source: env.source, Targets: env.targets})
	require.Error(t, err)
	// A plan that is refused: no task id, a source that is not a shard, a
	// target on a pchannel that holds a shard of the collection.
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID, Source: env.source, Targets: env.targets})
	require.Error(t, err)
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID, SplitTaskID: 7, Source: env.targets[0], Targets: env.targets})
	require.Error(t, err)
	onUsedPChannel := [2]string{env.targets[0], funcutil.GetVirtualChannel(funcutil.ToPhysicalChannel(env.other), env.collID, 9)}
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID, SplitTaskID: 7, Source: env.source, Targets: onUsedPChannel})
	require.Error(t, err)
	require.Empty(t, env.collection().SplitLineages)

	// The split is issued, and its ack callback applies it.
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{
		CollectionID: env.collID, SplitTaskID: 7, Source: env.source, Targets: env.targets, ShardBy: "hash(pk)",
	})
	require.NoError(t, err)
	coll := env.collection()
	require.Equal(t, []string{env.targets[0], env.targets[1], env.other}, coll.VirtualChannelNames)
	require.Equal(t, uint64(4), coll.RoutingModulus)
	require.Equal(t, "hash(pk)", coll.ShardBy)
	require.Len(t, coll.SplitLineages, 1)
	require.NotZero(t, coll.SplitLineages[0].SwitchTimeTick)
	require.Equal(t, 1, env.expired)

	// The source cannot be split a second time: it is not a shard any more.
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{
		CollectionID: env.collID, SplitTaskID: 8, Source: env.source, Targets: [2]string{env.vchannelOn(7, 4), env.vchannelOn(8, 5)},
	})
	require.Error(t, err)
	// A new target cannot be placed on the pchannel of the shard that was
	// split: that vchannel may still exist there.
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{
		CollectionID: env.collID, SplitTaskID: 8, Source: env.other,
		Targets: [2]string{funcutil.GetVirtualChannel(funcutil.ToPhysicalChannel(env.source), env.collID, 4), env.vchannelOn(8, 5)},
	})
	require.Error(t, err)

	// A target is split again with an explicit plan. The first lineage record
	// is left as it is.
	first := coll.SplitLineages[0]
	second := [2]string{env.vchannelOn(7, 4), env.vchannelOn(8, 5)}
	split, err := routing.PlanSplit(coll.Routing(), env.targets[0], second)
	require.NoError(t, err)
	_, err = env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID, SplitTaskID: 9, Source: env.targets[0], Split: &split})
	require.NoError(t, err)
	coll = env.collection()
	require.Equal(t, []string{second[0], second[1], env.targets[1], env.other}, coll.VirtualChannelNames)
	require.Equal(t, uint64(8), coll.RoutingModulus)
	require.Len(t, coll.SplitLineages, 2)
	require.Equal(t, first, coll.SplitLineages[0])
	require.Equal(t, env.targets[0], coll.SplitLineages[1].SourceVChannel)
	_, err = coll.Routing().Table()
	require.NoError(t, err)
}

func TestBroadcastSplitShardRefusesANamespaceCollection(t *testing.T) {
	env := newSplitShardTestEnv(t)
	// The collection places rows by namespace.
	coll := env.collection()
	properties := []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "true"}}
	for _, property := range coll.Properties {
		// The property is written as false when a collection is created.
		if property.GetKey() != common.NamespaceShardingEnabledKey {
			properties = append(properties, property)
		}
	}
	coll.Properties = properties
	plan, err := newSplitShardPlan(coll, &SplitShardRequest{CollectionID: env.collID, SplitTaskID: 7, Source: env.source, Targets: env.targets})
	require.NoError(t, err)
	_, err = shardsplit.NewBroadcastMessage(plan)
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestSplitShardAckCallbackFailurePaths(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	result := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})
	failure := errors.New("etcd is down")

	t.Run("the meta write fails", func(t *testing.T) {
		patch := mockey.Mock((*MetaTable).ApplyShardSplit).Return(failure).Build()
		defer patch.UnPatch()
		// The failure is returned, so that the broadcaster runs the callback again.
		require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, result), failure)
		require.Zero(t, env.expired)
	})

	t.Run("the collection is dropped between the check and the write", func(t *testing.T) {
		for _, gone := range []error{errAlterCollectionNotFound, errShardSplitCollectionUnavailable} {
			patch := mockey.Mock((*MetaTable).ApplyShardSplit).Return(gone).Build()
			require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
			patch.UnPatch()
		}
		require.Zero(t, env.expired)
	})

	t.Run("the split is applied between the check and the write", func(t *testing.T) {
		patch := mockey.Mock((*MetaTable).ApplyShardSplit).Return(errShardSplitApplied).Build()
		defer patch.UnPatch()
		require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
		require.Equal(t, 1, env.expired)
		env.expired = 0
	})

	// Apply the split for real; the remaining cases fail after the write.
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
	env.expired = 0

	t.Run("the collection cannot be read to invalidate its caches", func(t *testing.T) {
		patch := mockey.Mock((*MetaTable).GetCollectionByID).Return(nil, failure).Build()
		require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, result), failure)
		patch.UnPatch()
		// A collection that is gone has its caches invalidated by its drop.
		patch = mockey.Mock((*MetaTable).GetCollectionByID).Return(nil, merr.WrapErrCollectionNotFound(env.collID)).Build()
		require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
		patch.UnPatch()
		require.Zero(t, env.expired)
	})

	t.Run("the caches of the collection cannot be collected", func(t *testing.T) {
		patch := mockey.Mock((*Core).getCacheExpireForCollection).Return(nil, failure).Build()
		require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, result), failure)
		patch.UnPatch()
		patch = mockey.Mock((*Core).getCacheExpireForCollection).Return(nil, merr.WrapErrCollectionNotFound(env.collID)).Build()
		require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))
		patch.UnPatch()
		require.Zero(t, env.expired)
	})

	t.Run("the cache invalidation fails", func(t *testing.T) {
		env.expireErr = failure
		defer func() { env.expireErr = nil }()
		require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, result), failure)
	})

	t.Run("the catalog write fails", func(t *testing.T) {
		other := [2]string{env.vchannelOn(7, 4), env.vchannelOn(8, 5)}
		next := env.newResult(9, env.targets[0], other, [4]uint64{2000, 2010, 2020, 2030})
		mt := env.core.meta.(*MetaTable)
		origin := mt.catalog
		mt.catalog = &failingAlterCatalog{RootCoordCatalog: origin, err: failure}
		before := env.collection()
		require.ErrorIs(t, env.callback.splitShardV2AckCallback(ctx, next), failure)
		// Nothing changed in memory.
		after := env.collection()
		require.Equal(t, before.VirtualChannelNames, after.VirtualChannelNames)
		require.Equal(t, before.SplitLineages, after.SplitLineages)
		mt.catalog = origin
		require.NoError(t, env.callback.splitShardV2AckCallback(ctx, next))
		require.Len(t, env.collection().SplitLineages, 2)
	})
}

// failingAlterCatalog fails every collection write.
type failingAlterCatalog struct {
	metastore.RootCoordCatalog
	err error
}

func (c *failingAlterCatalog) AlterCollection(context.Context, *model.Collection, *model.Collection, metastore.AlterType, typeutil.Timestamp, bool) error {
	return c.err
}

func TestBroadcastSplitShardRefusesARenameInFlight(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()
	// The collection is renamed after its name was read to take the resource
	// key and before the collection is read again under the key.
	reads := 0
	patch := mockey.Mock((*MetaTable).GetCollectionByID).Origin(&getCollectionByIDOrigin).
		To(func(mt *MetaTable, ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
			coll, err := getCollectionByIDOrigin(mt, ctx, dbName, collectionID, ts, allowUnavailable)
			reads++
			if err == nil && reads == 2 {
				coll = coll.Clone()
				coll.Name = "renamed"
			}
			return coll, err
		}).Build()
	defer patch.UnPatch()

	_, err := env.core.BroadcastSplitShard(ctx, &SplitShardRequest{CollectionID: env.collID, SplitTaskID: 7, Source: env.source, Targets: env.targets})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, env.collection().SplitLineages)
}

var getCollectionByIDOrigin func(mt *MetaTable, ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error)

func TestSplitShardAppliedResolver(t *testing.T) {
	env := newSplitShardTestEnv(t)
	ctx := context.Background()

	// Before the split is applied nothing records that the source copy was
	// acknowledged: a caller that holds no broadcast task keeps waiting.
	tick, applied, err := env.callback.splitShardAppliedResolver(ctx, []string{env.source})
	require.NoError(t, err)
	require.False(t, applied)
	require.Zero(t, tick)

	result := env.newResult(7, env.source, env.targets, [4]uint64{1000, 1010, 1020, 1030})
	require.NoError(t, env.callback.splitShardV2AckCallback(ctx, result))

	// Afterwards the lineage record answers, with the time tick the source
	// copy was acknowledged at, for as long as the collection exists.
	tick, applied, err = env.callback.splitShardAppliedResolver(ctx, []string{env.source})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(1000), tick)
	// The resolver is the one the broadcaster asks for this message type.
	tick, applied, err = registry.ResolveAppendFirstApplied(ctx, message.MessageTypeSplitShard, []string{env.source})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(1000), tick)

	// A vchannel that was not split, a collection that does not exist, and an
	// empty question are "not applied".
	for _, vchannels := range [][]string{{env.other}, {env.source, env.other}, {"by-dev-rootcoord-dml_0_999999v0"}, {"not-a-vchannel"}, nil} {
		_, applied, err = env.callback.splitShardAppliedResolver(ctx, vchannels)
		require.NoError(t, err)
		require.False(t, applied, vchannels)
	}

	// A failure to read the collection is returned; the caller asks again.
	failure := errors.New("etcd is down")
	patch := mockey.Mock((*MetaTable).GetCollectionByID).Return(nil, failure).Build()
	_, _, err = env.callback.splitShardAppliedResolver(ctx, []string{env.source})
	patch.UnPatch()
	require.ErrorIs(t, err, failure)
}
