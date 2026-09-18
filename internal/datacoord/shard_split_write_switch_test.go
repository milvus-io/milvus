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

package datacoord

import (
	"context"
	"slices"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func postImageStates(updates *messagespb.AlterCollectionMessageUpdates) map[string]schemapb.ShardState {
	states := make(map[string]schemapb.ShardState)
	for i, vchannel := range updates.GetVirtualChannelNames() {
		states[vchannel] = updates.GetShardInfos()[i].GetState()
	}
	return states
}

func postImageResidues(updates *messagespb.AlterCollectionMessageUpdates) map[string][]uint64 {
	residues := make(map[string][]uint64)
	for i, vchannel := range updates.GetVirtualChannelNames() {
		residues[vchannel] = updates.GetShardInfos()[i].GetHashRouting().GetBuckets()
	}
	return residues
}

// Every write switch the planner builds passes the checks the SplitShard ack
// callback and the routing apply would otherwise make after the fence: the
// builder's own (NewSplitShardBroadcastMessage) and the one run against the
// collection meta (CheckSplitShardAgainstCollection).
func TestBuildSplitShardParamPassesThePreFenceChecks(t *testing.T) {
	enableShardSplit(t)
	for _, tc := range []struct {
		name     string
		coll     *milvuspb.DescribeCollectionResponse
		source   string
		targets  []string
		modulus  uint64
		residues map[string][]uint64
	}{
		{
			name:     "the first split of a one-shard collection doubles the modulus",
			coll:     splitTestDescribe([]string{splitMgrV0}, nil, 0),
			source:   splitMgrV0,
			targets:  []string{splitMgrV1, splitMgrV2},
			modulus:  2,
			residues: map[string][]uint64{splitMgrV0: nil, splitMgrV1: {0}, splitMgrV2: {1}},
		},
		{
			name:     "a doubling re-expresses the untouched shard",
			coll:     splitTestDescribe([]string{splitMgrV0, splitMgrV1}, nil, 0),
			source:   splitMgrV1,
			targets:  []string{splitMgrV2, splitMgrV3},
			modulus:  4,
			residues: map[string][]uint64{splitMgrV0: {0, 2}, splitMgrV1: nil, splitMgrV2: {1}, splitMgrV3: {3}},
		},
		{
			name: "a shard owning several residues is halved at the same modulus",
			coll: splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
				hashInfo(splitMgrV0, schemapb.ShardState_ShardNormal, 0, 2),
				hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 1),
				hashInfo(splitMgrV2, schemapb.ShardState_ShardNormal, 3),
			}, 4),
			source:   splitMgrV0,
			targets:  []string{splitMgrV3, splitMgrV4},
			modulus:  4,
			residues: map[string][]uint64{splitMgrV0: nil, splitMgrV1: {1}, splitMgrV2: {3}, splitMgrV3: {0}, splitMgrV4: {2}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := splitCollectionFromDescribe(tc.coll, []int64{10, 11})
			residues, err := residuesOf(coll.Collection)
			require.NoError(t, err)
			own, err := residues.of(tc.source)
			require.NoError(t, err)
			left, right, after, err := planSplitResidues(residues.modulus, own)
			require.NoError(t, err)
			require.Equal(t, tc.modulus, after)
			task := &datapb.SplitShardTask{
				TaskId:         100,
				CollectionId:   splitMgrCollection,
				Sources:        []*datapb.SplitShardTaskSource{{Vchannel: tc.source}},
				Targets:        []*datapb.SplitShardTaskTarget{{Vchannel: tc.targets[0], Buckets: left}, {Vchannel: tc.targets[1], Buckets: right}},
				RoutingModulus: after,
			}

			param, err := buildSplitShardParam(task, coll, splitMgrControl)
			require.NoError(t, err)
			assert.Equal(t, tc.source, param.SourceVChannel)
			assert.Equal(t, tc.targets, param.TargetVChannels)
			assert.Equal(t, []int64{10, 11}, param.PartitionIDs)
			assert.Equal(t, splitMgrControl, param.ControlChannel)
			assert.Equal(t, tc.modulus, param.Routing.GetRoutingModulus())
			assert.Equal(t, "hash(pk)", param.Routing.GetShardBy())
			assert.Equal(t, schemapb.ShardState_ShardSplitting, postImageStates(param.Routing)[tc.source])
			for _, target := range tc.targets {
				assert.Equal(t, schemapb.ShardState_ShardCreating, postImageStates(param.Routing)[target])
			}
			assert.Equal(t, tc.residues, postImageResidues(param.Routing))

			msg, err := streaming.NewSplitShardBroadcastMessage(param)
			require.NoError(t, err)
			typed := message.MustAsSpecializedBroadcastMessage[*message.SplitShardMessageHeader, *message.SplitShardMessageBody](msg)
			require.NoError(t, streaming.CheckSplitShardAgainstCollection(coll.Collection, typed.Header(), typed.MustBody()))
		})
	}
}

func TestBuildSplitPostImageRefusals(t *testing.T) {
	coll := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10})
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)

	noModulus := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
	noModulus.RoutingModulus = 0
	_, err := buildSplitShardParam(noModulus, coll, splitMgrControl)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	// A modulus the untouched shards cannot be re-expressed at.
	twoShards := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV3}, nil, 0), []int64{10})
	odd := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
	odd.RoutingModulus = 3
	_, err = buildSplitPostImage(odd, twoShards)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	broken := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV0, schemapb.ShardState_ShardNormal, 0),
	}, 2), []int64{10})
	_, err = buildSplitPostImage(task, broken)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	// A collection with no primary key has nothing to route by.
	noPK := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	noPK.Schema.Fields[0].IsPrimaryKey = false
	_, err = buildSplitPostImage(task, splitCollectionFromDescribe(noPK, []int64{10}))
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	// A collection that declares its routing key keeps it.
	declared := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	declared.ShardBy = "hash(other)"
	updates, err := buildSplitPostImage(task, splitCollectionFromDescribe(declared, []int64{10}))
	require.NoError(t, err)
	assert.Equal(t, "hash(other)", updates.GetShardBy())

	// A split already committed builds the same post-image again.
	committed := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
		hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 1),
	}, 2), []int64{10})
	updates, err = buildSplitPostImage(task, committed)
	require.NoError(t, err)
	assert.Equal(t, []string{splitMgrV0, splitMgrV1, splitMgrV2}, updates.GetVirtualChannelNames())
}

func TestSplitCollectionFromDescribe(t *testing.T) {
	resp := splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
		hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 1),
	}, 2)
	resp.ShardBy = "hash(pk)"
	resp.Properties = []*commonpb.KeyValuePair{{Key: "k", Value: "v"}}
	coll := splitCollectionFromDescribe(resp, []int64{10, 11})
	assert.Equal(t, splitMgrCollection, coll.CollectionID)
	assert.Equal(t, "coll", coll.Name)
	assert.Equal(t, "db", coll.DBName)
	assert.True(t, coll.Available())
	assert.EqualValues(t, 2, coll.RoutingModulus)
	assert.Equal(t, "hash(pk)", coll.ShardBy)
	assert.Equal(t, schemapb.ShardState_ShardCreating, coll.ShardInfos[splitMgrV1].State)
	assert.Equal(t, []uint64{1}, coll.ShardInfos[splitMgrV2].Buckets)
	assert.Equal(t, "by-dev-rootcoord-dml_1", coll.ShardInfos[splitMgrV1].PChannelName)
	assert.Len(t, coll.Partitions, 2)
	assert.Equal(t, []int64{10, 11}, coll.partitionIDs)
	assert.Equal(t, "v", coll.Properties[0].GetValue())
	assert.Equal(t, "by-dev-rootcoord-dml_0", pchannelAt(coll.Collection, 0))
	coll.PhysicalChannelNames = nil
	assert.Equal(t, "by-dev-rootcoord-dml_2", pchannelAt(coll.Collection, 2), "falls back to the name's own pchannel")
}

// splitSwitchServer is a datacoord server whose broker describes the given
// collection and whose store records task.
func splitSwitchServer(t *testing.T, task *datapb.SplitShardTask, descs ...*milvuspb.DescribeCollectionResponse) *Server {
	svr := newShardSplitTestServer(t)
	b := broker.NewMockBroker(t)
	if len(descs) == 1 {
		b.EXPECT().DescribeCollectionInternal(mock.Anything, splitMgrCollection).Return(descs[0], nil).Maybe()
	} else {
		for _, desc := range descs {
			b.EXPECT().DescribeCollectionInternal(mock.Anything, splitMgrCollection).Return(desc, nil).Once()
		}
	}
	b.EXPECT().ShowPartitionsInternal(mock.Anything, splitMgrCollection).Return([]int64{10}, nil).Maybe()
	svr.broker = b
	if task != nil {
		require.NoError(t, svr.shardSplitTasks.create(context.Background(), svr.meta.catalog, task))
	}
	return svr
}

func mockSplitBroadcast(t *testing.T, bapi *mock_broadcaster.MockBroadcastAPI) *mockey.Mocker {
	var keys []message.ResourceKey
	mocker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(_ context.Context, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			keys = append(keys, resourceKeys...)
			return bapi, nil
		}).Build()
	t.Cleanup(func() {
		if len(keys) > 0 {
			assert.Contains(t, keys, message.NewSharedDBNameResourceKey("db"))
			assert.Contains(t, keys, message.NewExclusiveCollectionNameResourceKey("db", "coll"))
		}
	})
	return mocker
}

func TestIssueShardSplitBroadcastsUnderTheCollectionKeys(t *testing.T) {
	enableShardSplit(t)
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
	svr := splitSwitchServer(t, task, splitTestDescribe([]string{splitMgrV0}, nil, 0))

	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	var broadcasted []message.BroadcastMutableMessage
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			broadcasted = append(broadcasted, msg)
			return &types.BroadcastAppendResult{}, nil
		}).Once()
	bapi.EXPECT().Close().Once()
	mocker := mockSplitBroadcast(t, bapi)
	defer mocker.UnPatch()

	require.NoError(t, svr.issueShardSplit(context.Background(), task, splitMgrControl))
	require.Len(t, broadcasted, 1)
	typed := message.MustAsSpecializedBroadcastMessage[*message.SplitShardMessageHeader, *message.SplitShardMessageBody](broadcasted[0])
	assert.Equal(t, int64(100), typed.Header().GetSplitTaskId())
	assert.Equal(t, splitMgrV0, typed.Header().GetSourceVchannel())
	assert.Equal(t, []string{splitMgrV1, splitMgrV2}, typed.Header().GetTargetVchannels())
	vchannels := broadcasted[0].BroadcastHeader().VChannels
	assert.ElementsMatch(t, []string{splitMgrV0, splitMgrV1, splitMgrV2, splitMgrControl}, vchannels)
}

func TestIssueShardSplitRefusals(t *testing.T) {
	enableShardSplit(t)
	ctx := context.Background()

	newRefusalCase := func(t *testing.T, task *datapb.SplitShardTask, record *datapb.SplitShardTask, desc ...*milvuspb.DescribeCollectionResponse) (*Server, *mock_broadcaster.MockBroadcastAPI) {
		if len(desc) == 0 {
			desc = []*milvuspb.DescribeCollectionResponse{splitTestDescribe([]string{splitMgrV0}, nil, 0)}
		}
		svr := splitSwitchServer(t, record, desc...)
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Close().Maybe()
		mocker := mockSplitBroadcast(t, bapi)
		t.Cleanup(func() { mocker.UnPatch() })
		return svr, bapi
	}

	t.Run("a task the store does not record is not broadcast", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		svr, _ := newRefusalCase(t, task, nil)
		assert.ErrorIs(t, svr.issueShardSplit(ctx, task, splitMgrControl), merr.ErrServiceInternal)
	})

	t.Run("a task id recorded for another collection is not broadcast", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		record := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		record.CollectionId = 2
		svr, _ := newRefusalCase(t, task, record)
		err := svr.issueShardSplit(ctx, task, splitMgrControl)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.ErrorContains(t, err, "recorded on collection 2")
	})

	t.Run("a task id recorded with another source is not broadcast", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		record := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		record.Sources[0].Vchannel = splitMgrV3
		svr, _ := newRefusalCase(t, task, record)
		assert.ErrorContains(t, svr.issueShardSplit(ctx, task, splitMgrControl), "recorded with source")
	})

	t.Run("a task the callback already fenced needs no broadcast", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		record := fencedTask(datapb.SplitShardTaskState_SplitShardTaskRedistributing)
		svr, _ := newRefusalCase(t, task, record)
		assert.NoError(t, svr.issueShardSplit(ctx, task, splitMgrControl))
	})

	t.Run("a collection renamed while taking its keys is retried", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		renamed := splitTestDescribe([]string{splitMgrV0}, nil, 0)
		renamed.CollectionName = "renamed"
		svr, _ := newRefusalCase(t, task, task, splitTestDescribe([]string{splitMgrV0}, nil, 0), renamed)
		err := svr.issueShardSplit(ctx, task, splitMgrControl)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("a source that is not Normal in the meta is refused before the fence", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		task.TaskId = 101
		// Another split has already fenced v0 and created v3/v4.
		desc := splitTestDescribe([]string{splitMgrV0, splitMgrV3, splitMgrV4}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
			hashInfo(splitMgrV3, schemapb.ShardState_ShardCreating, 0),
			hashInfo(splitMgrV4, schemapb.ShardState_ShardCreating, 1),
		}, 2)
		svr, _ := newRefusalCase(t, task, task, desc)
		assert.Error(t, svr.issueShardSplit(ctx, task, splitMgrControl))
	})

	t.Run("the switch off refuses to build the broadcast", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		svr, _ := newRefusalCase(t, task, task)
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.ShardSplitEnable.Key, "false")
		defer paramtable.Get().Save(paramtable.Get().DataCoordCfg.ShardSplitEnable.Key, "true")
		assert.ErrorIs(t, svr.issueShardSplit(ctx, task, splitMgrControl), merr.ErrOperationNotSupported)
	})

	t.Run("a plan that no longer fits the collection is refused", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		task.RoutingModulus = 0
		svr, _ := newRefusalCase(t, task, task)
		assert.ErrorIs(t, svr.issueShardSplit(ctx, task, splitMgrControl), merr.ErrServiceInternal)
	})

	t.Run("a collection with a TEXT field is refused under the keys", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		desc := splitTestDescribe([]string{splitMgrV0}, nil, 0)
		desc.Schema = splitTestSchemaWithText()
		svr, _ := newRefusalCase(t, task, task, desc)
		err := svr.issueShardSplit(ctx, task, splitMgrControl)
		assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
		assert.ErrorContains(t, err, "TEXT")
	})

	t.Run("a failed broadcast is returned for the next tick", func(t *testing.T) {
		task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
		svr, bapi := newRefusalCase(t, task, task)
		bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnavailableMsg("wal not ready")).Once()
		assert.ErrorIs(t, svr.issueShardSplit(ctx, task, splitMgrControl), merr.ErrServiceUnavailable)
	})
}

func TestIssueShardSplitReadFailures(t *testing.T) {
	enableShardSplit(t)
	ctx := context.Background()
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)

	t.Run("a dropped collection says so", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		b := broker.NewMockBroker(t)
		b.EXPECT().DescribeCollectionInternal(mock.Anything, splitMgrCollection).Return(nil, merr.WrapErrCollectionNotFound(splitMgrCollection))
		svr.broker = b
		assert.ErrorIs(t, svr.issueShardSplit(ctx, task, splitMgrControl), merr.ErrCollectionNotFound)
	})

	t.Run("the keys cannot be taken", func(t *testing.T) {
		svr := splitSwitchServer(t, nil, splitTestDescribe([]string{splitMgrV0}, nil, 0))
		mocker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(nil, errors.New("not primary")).Build()
		defer mocker.UnPatch()
		assert.Error(t, svr.issueShardSplit(ctx, task, splitMgrControl))
	})

	t.Run("the partitions cannot be read under the keys", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		b := broker.NewMockBroker(t)
		b.EXPECT().DescribeCollectionInternal(mock.Anything, splitMgrCollection).Return(splitTestDescribe([]string{splitMgrV0}, nil, 0), nil)
		b.EXPECT().ShowPartitionsInternal(mock.Anything, splitMgrCollection).Return(nil, errors.New("rootcoord busy"))
		svr.broker = b
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Close().Once()
		mocker := mockSplitBroadcast(t, bapi)
		defer mocker.UnPatch()
		assert.Error(t, svr.issueShardSplit(ctx, task, splitMgrControl))
	})

	t.Run("the collection can be read to plan against", func(t *testing.T) {
		svr := splitSwitchServer(t, nil, splitTestDescribe([]string{splitMgrV0}, nil, 0))
		coll, err := svr.describeSplitCollection(ctx, splitMgrCollection)
		require.NoError(t, err)
		assert.True(t, slices.Equal([]string{splitMgrV0}, coll.VirtualChannelNames))
	})
}

// Only the check against the collection meta refuses this split: its message
// is self-consistent, but the source is Creating in the meta -- a target of an
// earlier split that has not been adopted yet -- and only a Normal shard may be
// split. Refused before the fence, nothing is broadcast.
func TestIssueShardSplitRefusedByTheCollectionMetaCheck(t *testing.T) {
	enableShardSplit(t)
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
	task.RoutingModulus = 4
	task.Targets[0].Buckets = []uint64{0}
	task.Targets[1].Buckets = []uint64{2}
	desc := splitTestDescribe([]string{splitMgrV0, splitMgrV3}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV0, schemapb.ShardState_ShardCreating, 0),
		hashInfo(splitMgrV3, schemapb.ShardState_ShardCreating, 1),
	}, 2)
	svr := splitSwitchServer(t, task, desc)

	// The message alone passes every check.
	param, err := buildSplitShardParam(task, splitCollectionFromDescribe(desc, []int64{10}), splitMgrControl)
	require.NoError(t, err)
	require.NoError(t, param.Validate())

	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	broadcasts := 0
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			broadcasts++
			return &types.BroadcastAppendResult{}, nil
		}).Maybe()
	bapi.EXPECT().Close().Once()
	mocker := mockSplitBroadcast(t, bapi)
	defer mocker.UnPatch()

	err = svr.issueShardSplit(context.Background(), task, splitMgrControl)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "only a Normal shard may be split")
	assert.Zero(t, broadcasts, "a split the meta check refuses must not be broadcast")
}

// issueThroughServer is a split coordinator whose write switch is the real
// datacoord one, over the server's own task store.
type issueThroughServer struct {
	*fakeSplitCoordinator
	server *Server
}

func (c *issueThroughServer) issueShardSplit(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	return c.server.issueShardSplit(ctx, task, controlChannel)
}

// newAllocatedPreparingCase is a manager over svr's task store holding a
// Preparing task whose targets are already allocated and persisted, as after
// the tick that allocated them.
func newAllocatedPreparingCase(t *testing.T, descs ...*milvuspb.DescribeCollectionResponse) (*shardSplitManager, *Server, *mock_broadcaster.MockBroadcastAPI) {
	enableShardSplit(t)
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskPreparing)
	svr := splitSwitchServer(t, task, descs...)
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Close().Maybe()
	mocker := mockSplitBroadcast(t, bapi)
	t.Cleanup(func() { mocker.UnPatch() })
	coordinator := &issueThroughServer{
		fakeSplitCoordinator: &fakeSplitCoordinator{coll: splitCollectionFromDescribe(descs[0], []int64{10})},
		server:               svr,
	}
	manager := newShardSplitManager(context.Background(), svr.meta, newMockAllocator(t), svr.shardSplitTasks, coordinator)
	manager.controlChannel = func() string { return splitMgrControl }
	return manager, svr, bapi
}

// A TEXT field added after the targets were allocated: the write switch is
// refused under the collection's keys, before anything is broadcast, and the
// source is not fenced anywhere. The task aborts and frees its slot, instead
// of retrying a refusal that cannot change.
func TestShardSplitAbortsAPreparingTaskWhoseWriteSwitchIsRefused(t *testing.T) {
	desc := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	desc.Schema = splitTestSchemaWithText()
	manager, _, _ := newAllocatedPreparingCase(t, desc)
	require.Equal(t, 1, manager.activeTaskCount())

	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, task.GetState())
	assert.Contains(t, task.GetFailReason(), "TEXT")
	assert.NotZero(t, task.GetEndTime())
	assert.Zero(t, manager.activeTaskCount(), "the concurrency slot is freed")
	assert.False(t, manager.IsVChannelSplitting(splitMgrV0), "the freeze lifts")
}

// A retriable refusal is the collection passing through a transient state:
// the task keeps its targets and retries next tick.
func TestShardSplitRetriesARetriableWriteSwitchRefusal(t *testing.T) {
	renamed := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	renamed.CollectionName = "renamed"
	manager, _, _ := newAllocatedPreparingCase(t, splitTestDescribe([]string{splitMgrV0}, nil, 0), renamed)

	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, task.GetState())
	assert.Equal(t, 1, manager.activeTaskCount())
}

// A failed broadcast may have landed: never abort on it, whatever its class.
func TestShardSplitNeverAbortsAfterABroadcastAttempt(t *testing.T) {
	manager, _, bapi := newAllocatedPreparingCase(t, splitTestDescribe([]string{splitMgrV0}, nil, 0))
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternalMsg("append failed")).Once()

	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mustTask(t, manager, 100).GetState())
}

// The abort re-checks the record under the task's lock: a record that shows the
// source fenced, or that has moved on, is never aborted.
func TestShardSplitRefusedAbortKeepsAFencedRecord(t *testing.T) {
	refused := markRefusedBeforeBroadcast(merr.WrapErrOperationNotSupportedMsg("refused"))
	for name, mutate := range map[string]func(*datapb.SplitShardTask){
		"fenced":   func(t *datapb.SplitShardTask) { t.Fenced = true },
		"T_switch": func(t *datapb.SplitShardTask) { t.Sources[0].SwitchTimeTick = 2000 },
		"moved on": func(t *datapb.SplitShardTask) { t.State = datapb.SplitShardTaskState_SplitShardTaskFencing },
	} {
		t.Run(name, func(t *testing.T) {
			manager, coordinator, _ := newPreparingCase(t)
			coordinator.issueErr = refused
			coordinator.onIssue = func(*datapb.SplitShardTask) {
				_, err := manager.store.modify(context.Background(), manager.catalog, 100, func(t *datapb.SplitShardTask) bool {
					mutate(t)
					return true
				})
				require.NoError(t, err)
			}
			manager.advanceTasks()
			assert.NotEqual(t, datapb.SplitShardTaskState_SplitShardTaskAborted, mustTask(t, manager, 100).GetState())
		})
	}
}

func TestIssueShardSplitMarksOnlyRefusalsBeforeTheBroadcast(t *testing.T) {
	ctx := context.Background()
	t.Run("a TEXT refusal is marked", func(t *testing.T) {
		desc := splitTestDescribe([]string{splitMgrV0}, nil, 0)
		desc.Schema = splitTestSchemaWithText()
		manager, svr, _ := newAllocatedPreparingCase(t, desc)
		err := svr.issueShardSplit(ctx, mustTask(t, manager, 100), splitMgrControl)
		assert.True(t, errors.Is(err, errSplitRefusedBeforeBroadcast))
		assert.ErrorIs(t, err, merr.ErrOperationNotSupported, "the mark keeps the code")
		assert.False(t, merr.IsRetryableErr(err))
	})

	t.Run("a refusal of a switch the meta already shows applied is not marked", func(t *testing.T) {
		// Targets listed: the write switch landed; only the callback's record
		// is missing. Never abort that.
		desc := splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
			hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
			hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 1),
		}, 2)
		desc.Schema = splitTestSchemaWithText()
		manager, svr, _ := newAllocatedPreparingCase(t, desc)
		err := svr.issueShardSplit(ctx, mustTask(t, manager, 100), splitMgrControl)
		assert.Error(t, err)
		assert.False(t, errors.Is(err, errSplitRefusedBeforeBroadcast))
	})
}
