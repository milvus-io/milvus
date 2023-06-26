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
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/common"
)

// waitAndStore simulates DataNode's action
func waitAndStore(t *testing.T, watchkv kv.MetaKv, key string, waitState, storeState datapb.ChannelWatchState) {
	for {
		v, err := watchkv.Load(key)
		if err == nil && len(v) > 0 {
			watchInfo, err := parseWatchInfo(key, []byte(v))
			require.NoError(t, err)
			require.Equal(t, waitState, watchInfo.GetState())

			watchInfo.State = storeState
			data, err := proto.Marshal(watchInfo)
			require.NoError(t, err)

			watchkv.Save(key, string(data))
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitAndCheckState checks if the DataCoord writes expected state into Etcd
func waitAndCheckState(t *testing.T, kv kv.MetaKv, expectedState datapb.ChannelWatchState, nodeID UniqueID, channelName string, collectionID UniqueID) {
	for {
		prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()
		v, err := kv.Load(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName))
		if err == nil && len(v) > 0 {
			watchInfo, err := parseWatchInfo("fake", []byte(v))
			require.NoError(t, err)

			if watchInfo.GetState() == expectedState {
				assert.Equal(t, watchInfo.Vchan.GetChannelName(), channelName)
				assert.Equal(t, watchInfo.Vchan.GetCollectionID(), collectionID)
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func getOpsWithWatchInfo(nodeID UniqueID, ch *channel) ChannelOpSet {
	var ops ChannelOpSet
	ops.Add(nodeID, []*channel{ch})

	for _, op := range ops {
		op.ChannelWatchInfos = []*datapb.ChannelWatchInfo{{}}
	}
	return ops
}

func TestChannelManager_StateTransfer(t *testing.T) {
	watchkv := getWatchKV(t)
	defer func() {
		watchkv.RemoveWithPrefix("")
		watchkv.Close()
	}()

	p := "/tmp/milvus_ut/rdb_data"
	t.Setenv("ROCKSMQ_PATH", p)

	prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()

	var (
		collectionID      = UniqueID(9)
		nodeID            = UniqueID(119)
		channelNamePrefix = t.Name()

		waitFor = time.Second
		tick    = time.Millisecond * 10
	)

	t.Run("ToWatch-WatchSuccess", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		cName := channelNamePrefix + "ToWatch-WatchSuccess"

		ctx, cancel := context.WithCancel(context.TODO())
		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.AddNode(nodeID)
		chManager.Watch(&channel{Name: cName, CollectionID: collectionID})

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_WatchSuccess, nodeID, cName, collectionID)

		assert.Eventually(t, func() bool {
			_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
			return !loaded
		}, waitFor, tick)

		cancel()
		wg.Wait()
	})

	t.Run("ToWatch-WatchFail-ToRelease", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		cName := channelNamePrefix + "ToWatch-WatchFail-ToRelase"
		ctx, cancel := context.WithCancel(context.TODO())
		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.AddNode(nodeID)
		chManager.Watch(&channel{Name: cName, CollectionID: collectionID})

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchFailure)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToRelease, nodeID, cName, collectionID)

		assert.Eventually(t, func() bool {
			_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
			return loaded
		}, waitFor, tick)

		cancel()
		wg.Wait()
		chManager.stateTimer.removeTimers([]string{cName})
	})

	t.Run("ToWatch-Timeout", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		cName := channelNamePrefix + "ToWatch-Timeout"
		ctx, cancel := context.WithCancel(context.TODO())
		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.AddNode(nodeID)
		chManager.Watch(&channel{Name: cName, CollectionID: collectionID})

		// simulating timeout behavior of startOne, cuz 20s is a long wait
		e := &ackEvent{
			ackType:     watchTimeoutAck,
			channelName: cName,
			nodeID:      nodeID,
		}
		chManager.stateTimer.notifyTimeoutWatcher(e)

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToRelease, nodeID, cName, collectionID)
		assert.Eventually(t, func() bool {
			_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
			return loaded
		}, waitFor, tick)

		cancel()
		wg.Wait()
		chManager.stateTimer.removeTimers([]string{cName})
	})

	t.Run("ToRelease-ReleaseSuccess-Reassign-ToWatch-2-DN", func(t *testing.T) {
		var oldNode = UniqueID(120)
		cName := channelNamePrefix + "ToRelease-ReleaseSuccess-Reassign-ToWatch-2-DN"

		watchkv.RemoveWithPrefix("")
		ctx, cancel := context.WithCancel(context.TODO())
		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{
					{Name: cName, CollectionID: collectionID},
				}},
				oldNode: {oldNode, []*channel{}},
			},
		}

		err = chManager.Release(nodeID, cName)
		assert.NoError(t, err)

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToRelease, datapb.ChannelWatchState_ReleaseSuccess)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, oldNode, cName, collectionID)

		cancel()
		wg.Wait()

		w, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(nodeID, 10)))
		assert.Error(t, err)
		assert.Empty(t, w)

		_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
		assert.True(t, loaded)
		chManager.stateTimer.removeTimers([]string{cName})
	})

	t.Run("ToRelease-ReleaseSuccess-Reassign-ToWatch-1-DN", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		ctx, cancel := context.WithCancel(context.TODO())
		cName := channelNamePrefix + "ToRelease-ReleaseSuccess-Reassign-ToWatch-1-DN"
		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{
					{Name: cName, CollectionID: collectionID},
				}},
			},
		}

		err = chManager.Release(nodeID, cName)
		assert.NoError(t, err)

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToRelease, datapb.ChannelWatchState_ReleaseSuccess)

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeID, cName, collectionID)

		assert.Eventually(t, func() bool {
			_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
			return loaded
		}, waitFor, tick)
		cancel()
		wg.Wait()

		chManager.stateTimer.removeTimers([]string{cName})
	})

	t.Run("ToRelease-ReleaseFail-CleanUpAndDelete-Reassign-ToWatch-2-DN", func(t *testing.T) {
		var oldNode = UniqueID(121)

		cName := channelNamePrefix + "ToRelease-ReleaseFail-CleanUpAndDelete-Reassign-ToWatch-2-DN"
		watchkv.RemoveWithPrefix("")
		ctx, cancel := context.WithCancel(context.TODO())
		factory := dependency.NewDefaultFactory(true)
		_, err := factory.NewMsgStream(context.TODO())
		require.NoError(t, err)
		chManager, err := NewChannelManager(watchkv, newMockHandler(), withMsgstreamFactory(factory))
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{
					{Name: cName, CollectionID: collectionID},
				}},
				oldNode: {oldNode, []*channel{}},
			},
		}

		err = chManager.Release(nodeID, cName)
		assert.NoError(t, err)

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToRelease, datapb.ChannelWatchState_ReleaseFailure)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, oldNode, cName, collectionID)

		cancel()
		wg.Wait()

		w, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(nodeID, 10)))
		assert.Error(t, err)
		assert.Empty(t, w)

		_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
		assert.True(t, loaded)
		chManager.stateTimer.removeTimers([]string{cName})
	})

	t.Run("ToRelease-ReleaseFail-CleanUpAndDelete-Reassign-ToWatch-1-DN", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		cName := channelNamePrefix + "ToRelease-ReleaseFail-CleanUpAndDelete-Reassign-ToWatch-1-DN"
		ctx, cancel := context.WithCancel(context.TODO())
		factory := dependency.NewDefaultFactory(true)
		_, err := factory.NewMsgStream(context.TODO())
		require.NoError(t, err)
		chManager, err := NewChannelManager(watchkv, newMockHandler(), withMsgstreamFactory(factory))
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chManager.watchChannelStatesLoop(ctx, common.LatestRevision)
			wg.Done()
		}()

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{
					{Name: cName, CollectionID: collectionID},
				}},
			},
		}

		err = chManager.Release(nodeID, cName)
		assert.NoError(t, err)

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToRelease, datapb.ChannelWatchState_ReleaseFailure)

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeID, cName, collectionID)
		assert.Eventually(t, func() bool {
			_, loaded := chManager.stateTimer.runningTimerStops.Load(cName)
			return loaded
		}, waitFor, tick)

		cancel()
		wg.Wait()
		chManager.stateTimer.removeTimers([]string{cName})
	})
}

func TestChannelManager(t *testing.T) {
	watchkv := getWatchKV(t)
	defer func() {
		watchkv.RemoveWithPrefix("")
		watchkv.Close()
	}()

	prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()
	t.Run("test AddNode with avalible node", func(t *testing.T) {
		// Note: this test is based on the default registerPolicy
		defer watchkv.RemoveWithPrefix("")
		var (
			collectionID       = UniqueID(8)
			nodeID, nodeToAdd  = UniqueID(118), UniqueID(811)
			channel1, channel2 = "channel1", "channel2"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)
		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{
					{Name: channel1, CollectionID: collectionID},
					{Name: channel2, CollectionID: collectionID},
				}},
			},
		}

		err = chManager.AddNode(nodeToAdd)
		assert.NoError(t, err)

		assert.True(t, chManager.Match(nodeID, channel1))
		assert.True(t, chManager.Match(nodeID, channel2))
		assert.False(t, chManager.Match(nodeToAdd, channel1))
		assert.False(t, chManager.Match(nodeToAdd, channel2))

		err = chManager.Watch(&channel{Name: "channel-3", CollectionID: collectionID})
		assert.NoError(t, err)

		assert.True(t, chManager.Match(nodeToAdd, "channel-3"))

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeToAdd, "channel-3", collectionID)
		chManager.stateTimer.removeTimers([]string{"channel-3"})
	})

	t.Run("test AddNode with no available node", func(t *testing.T) {
		// Note: this test is based on the default registerPolicy
		defer watchkv.RemoveWithPrefix("")
		var (
			collectionID       = UniqueID(8)
			nodeID             = UniqueID(119)
			channel1, channel2 = "channel1", "channel2"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)
		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				bufferID: {bufferID, []*channel{
					{Name: channel1, CollectionID: collectionID},
					{Name: channel2, CollectionID: collectionID},
				}},
			},
		}

		err = chManager.AddNode(nodeID)
		assert.NoError(t, err)

		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), channel1)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		key = path.Join(prefix, strconv.FormatInt(nodeID, 10), channel2)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		assert.True(t, chManager.Match(nodeID, channel1))
		assert.True(t, chManager.Match(nodeID, channel2))

		err = chManager.Watch(&channel{Name: "channel-3", CollectionID: collectionID})
		assert.NoError(t, err)

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeID, "channel-3", collectionID)
		chManager.stateTimer.removeTimers([]string{"channel-3"})
	})

	t.Run("test Watch", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var (
			collectionID = UniqueID(7)
			nodeID       = UniqueID(117)
			bufferCh     = "bufferID"
			chanToAdd    = "new-channel-watch"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		err = chManager.Watch(&channel{Name: bufferCh, CollectionID: collectionID})
		assert.NoError(t, err)

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, bufferID, bufferCh, collectionID)

		chManager.store.Add(nodeID)
		err = chManager.Watch(&channel{Name: chanToAdd, CollectionID: collectionID})
		assert.NoError(t, err)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeID, chanToAdd, collectionID)

		chManager.stateTimer.removeTimers([]string{chanToAdd})
	})

	t.Run("test Release", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var (
			collectionID               = UniqueID(4)
			nodeID, invalidNodeID      = UniqueID(114), UniqueID(999)
			channelName, invalidChName = "to-release", "invalid-to-release"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)
		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{{Name: channelName, CollectionID: collectionID}}},
			},
		}

		err = chManager.Release(invalidNodeID, invalidChName)
		assert.Error(t, err)

		err = chManager.Release(nodeID, channelName)
		assert.NoError(t, err)
		chManager.stateTimer.removeTimers([]string{channelName})

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToRelease, nodeID, channelName, collectionID)
	})

	t.Run("test Reassign", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var collectionID = UniqueID(5)

		tests := []struct {
			nodeID UniqueID
			chName string
		}{
			{UniqueID(125), "normal-chan"},
			{UniqueID(115), "to-delete-chan"},
		}

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		// prepare tests
		for _, test := range tests {
			chManager.store.Add(test.nodeID)
			ops := getOpsWithWatchInfo(test.nodeID, &channel{Name: test.chName, CollectionID: collectionID})
			err = chManager.store.Update(ops)
			require.NoError(t, err)

			info, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(test.nodeID, 10), test.chName))
			require.NoError(t, err)
			require.NotNil(t, info)
		}

		remainTest, reassignTest := tests[0], tests[1]
		err = chManager.Reassign(reassignTest.nodeID, reassignTest.chName)
		assert.NoError(t, err)
		chManager.stateTimer.stopIfExist(&ackEvent{releaseSuccessAck, reassignTest.chName, reassignTest.nodeID})

		// test nodes of reassignTest contains no channel
		// test all channels are assgined to node of remainTest
		assert.False(t, chManager.Match(reassignTest.nodeID, reassignTest.chName))
		assert.True(t, chManager.Match(remainTest.nodeID, reassignTest.chName))
		assert.True(t, chManager.Match(remainTest.nodeID, remainTest.chName))

		// Delete node of reassginTest and try to Reassign node in remainTest
		err = chManager.DeleteNode(reassignTest.nodeID)
		require.NoError(t, err)

		err = chManager.Reassign(remainTest.nodeID, remainTest.chName)
		assert.NoError(t, err)
		chManager.stateTimer.stopIfExist(&ackEvent{releaseSuccessAck, reassignTest.chName, reassignTest.nodeID})

		// channel is added to remainTest because there's only one node left
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, remainTest.nodeID, remainTest.chName, collectionID)
	})

	t.Run("test DeleteNode", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")

		var (
			collectionID = UniqueID(999)
		)
		chManager, err := NewChannelManager(watchkv, newMockHandler(), withStateChecker())
		require.NoError(t, err)
		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				1: {1, []*channel{
					{Name: "channel-1", CollectionID: collectionID},
					{Name: "channel-2", CollectionID: collectionID}}},
				bufferID: {bufferID, []*channel{}},
			},
		}
		chManager.stateTimer.startOne(datapb.ChannelWatchState_ToRelease, "channel-1", 1, Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second))

		err = chManager.DeleteNode(1)
		assert.NoError(t, err)

		chs := chManager.store.GetBufferChannelInfo()
		assert.Equal(t, 2, len(chs.Channels))
	})

	t.Run("test CleanupAndReassign", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var collectionID = UniqueID(6)

		tests := []struct {
			nodeID UniqueID
			chName string
		}{
			{UniqueID(126), "normal-chan"},
			{UniqueID(116), "to-delete-chan"},
		}

		factory := dependency.NewDefaultFactory(true)
		_, err := factory.NewMsgStream(context.TODO())
		require.NoError(t, err)
		chManager, err := NewChannelManager(watchkv, newMockHandler(), withMsgstreamFactory(factory))

		require.NoError(t, err)

		// prepare tests
		for _, test := range tests {
			chManager.store.Add(test.nodeID)
			ops := getOpsWithWatchInfo(test.nodeID, &channel{Name: test.chName, CollectionID: collectionID})
			err = chManager.store.Update(ops)
			require.NoError(t, err)

			info, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(test.nodeID, 10), test.chName))
			require.NoError(t, err)
			require.NotNil(t, info)
		}

		remainTest, reassignTest := tests[0], tests[1]
		err = chManager.CleanupAndReassign(reassignTest.nodeID, reassignTest.chName)
		assert.NoError(t, err)
		chManager.stateTimer.stopIfExist(&ackEvent{releaseSuccessAck, reassignTest.chName, reassignTest.nodeID})

		// test nodes of reassignTest contains no channel
		assert.False(t, chManager.Match(reassignTest.nodeID, reassignTest.chName))

		// test all channels are assgined to node of remainTest
		assert.True(t, chManager.Match(remainTest.nodeID, reassignTest.chName))
		assert.True(t, chManager.Match(remainTest.nodeID, remainTest.chName))

		// Delete node of reassginTest and try to CleanupAndReassign node in remainTest
		err = chManager.DeleteNode(reassignTest.nodeID)
		require.NoError(t, err)

		err = chManager.CleanupAndReassign(remainTest.nodeID, remainTest.chName)
		assert.NoError(t, err)
		chManager.stateTimer.stopIfExist(&ackEvent{releaseSuccessAck, reassignTest.chName, reassignTest.nodeID})

		// channel is added to remainTest because there's only one node left
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, remainTest.nodeID, remainTest.chName, collectionID)
	})

	t.Run("test getChannelByNodeAndName", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var (
			nodeID       = UniqueID(113)
			collectionID = UniqueID(3)
			channelName  = "get-channel-by-node-and-name"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		ch := chManager.getChannelByNodeAndName(nodeID, channelName)
		assert.Nil(t, ch)

		chManager.store.Add(nodeID)
		ch = chManager.getChannelByNodeAndName(nodeID, channelName)
		assert.Nil(t, ch)

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				nodeID: {nodeID, []*channel{{Name: channelName, CollectionID: collectionID}}},
			},
		}
		ch = chManager.getChannelByNodeAndName(nodeID, channelName)
		assert.NotNil(t, ch)
		assert.Equal(t, collectionID, ch.CollectionID)
		assert.Equal(t, channelName, ch.Name)
	})

	t.Run("test fillChannelWatchInfoWithState", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")
		var (
			nodeID       = UniqueID(111)
			collectionID = UniqueID(1)
			channelName  = "fill-channel-watchInfo-with-state"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)

		tests := []struct {
			inState datapb.ChannelWatchState

			description string
		}{
			{datapb.ChannelWatchState_ToWatch, "fill toWatch state"},
			{datapb.ChannelWatchState_ToRelease, "fill toRelase state"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ops := getReleaseOp(nodeID, &channel{Name: channelName, CollectionID: collectionID})
				for _, op := range ops {
					chs := chManager.fillChannelWatchInfoWithState(op, test.inState)
					assert.Equal(t, 1, len(chs))
					assert.Equal(t, channelName, chs[0])
					assert.Equal(t, 1, len(op.ChannelWatchInfos))
					assert.Equal(t, test.inState, op.ChannelWatchInfos[0].GetState())

					chManager.stateTimer.removeTimers(chs)
				}
			})
		}
	})

	t.Run("test updateWithTimer", func(t *testing.T) {
		var (
			nodeID       = UniqueID(112)
			collectionID = UniqueID(2)
			channelName  = "update-with-timer"
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler())
		require.NoError(t, err)
		chManager.store.Add(nodeID)

		opSet := getReleaseOp(nodeID, &channel{Name: channelName, CollectionID: collectionID})

		chManager.updateWithTimer(opSet, datapb.ChannelWatchState_ToWatch)
		chManager.stateTimer.removeTimers([]string{channelName})

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, nodeID, channelName, collectionID)
	})

	t.Run("test background check silent", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")
		defer watchkv.RemoveWithPrefix("")
		prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()
		var (
			collectionID      = UniqueID(9)
			channelNamePrefix = t.Name()
			nodeID            = UniqueID(111)
		)
		cName := channelNamePrefix + "TestBgChecker"

		//1. set up channel_manager
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		chManager, err := NewChannelManager(watchkv, newMockHandler(), withBgChecker())
		require.NoError(t, err)
		assert.NotNil(t, chManager.bgChecker)
		chManager.Startup(ctx, []int64{nodeID})

		//2. test isSilent function running correctly
		Params.Save(Params.DataCoordCfg.ChannelBalanceSilentDuration.Key, "3")
		assert.False(t, chManager.isSilent())
		assert.False(t, chManager.stateTimer.hasRunningTimers())

		//3. watch one channel
		chManager.Watch(&channel{Name: cName, CollectionID: collectionID})
		assert.False(t, chManager.isSilent())
		assert.True(t, chManager.stateTimer.hasRunningTimers())
		key := path.Join(prefix, strconv.FormatInt(nodeID, 10), cName)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_WatchSuccess, nodeID, cName, collectionID)

		//4. wait for duration and check silent again
		time.Sleep(Params.DataCoordCfg.ChannelBalanceSilentDuration.GetAsDuration(time.Second))
		chManager.stateTimer.removeTimers([]string{cName})
		assert.True(t, chManager.isSilent())
		assert.False(t, chManager.stateTimer.hasRunningTimers())
	})
}

func TestChannelManager_Reload(t *testing.T) {
	watchkv := getWatchKV(t)
	defer func() {
		watchkv.RemoveWithPrefix("")
		watchkv.Close()
	}()

	var (
		nodeID       = UniqueID(200)
		collectionID = UniqueID(2)
		channelName  = "channel-checkOldNodes"
	)
	prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()

	getWatchInfoWithState := func(state datapb.ChannelWatchState, collectionID UniqueID, channelName string) *datapb.ChannelWatchInfo {
		return &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  channelName,
			},
			State: state,
		}
	}

	t.Run("test checkOldNodes", func(t *testing.T) {
		watchkv.RemoveWithPrefix("")

		t.Run("ToWatch", func(t *testing.T) {
			defer watchkv.RemoveWithPrefix("")
			data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_ToWatch, collectionID, channelName))
			require.NoError(t, err)
			chManager, err := NewChannelManager(watchkv, newMockHandler())
			require.NoError(t, err)
			err = watchkv.Save(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName), string(data))
			require.NoError(t, err)

			chManager.checkOldNodes([]UniqueID{nodeID})
			_, ok := chManager.stateTimer.runningTimerStops.Load(channelName)
			assert.True(t, ok)
			chManager.stateTimer.removeTimers([]string{channelName})
		})

		t.Run("ToRelease", func(t *testing.T) {
			defer watchkv.RemoveWithPrefix("")
			data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_ToRelease, collectionID, channelName))
			require.NoError(t, err)
			chManager, err := NewChannelManager(watchkv, newMockHandler())
			require.NoError(t, err)
			err = watchkv.Save(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName), string(data))
			require.NoError(t, err)
			err = chManager.checkOldNodes([]UniqueID{nodeID})
			assert.NoError(t, err)

			_, ok := chManager.stateTimer.runningTimerStops.Load(channelName)
			assert.True(t, ok)
			chManager.stateTimer.removeTimers([]string{channelName})
		})

		t.Run("WatchFail", func(t *testing.T) {
			defer watchkv.RemoveWithPrefix("")
			chManager, err := NewChannelManager(watchkv, newMockHandler())
			require.NoError(t, err)
			chManager.store = &ChannelStore{
				store: watchkv,
				channelsInfo: map[int64]*NodeChannelInfo{
					nodeID: {nodeID, []*channel{{Name: channelName, CollectionID: collectionID}}}},
			}

			data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_WatchFailure, collectionID, channelName))
			require.NoError(t, err)
			err = watchkv.Save(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName), string(data))
			require.NoError(t, err)
			err = chManager.checkOldNodes([]UniqueID{nodeID})
			assert.NoError(t, err)

			waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToRelease, nodeID, channelName, collectionID)
			chManager.stateTimer.removeTimers([]string{channelName})
		})

		t.Run("ReleaseSuccess", func(t *testing.T) {
			defer watchkv.RemoveWithPrefix("")
			chManager, err := NewChannelManager(watchkv, newMockHandler())
			require.NoError(t, err)
			data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_ReleaseSuccess, collectionID, channelName))
			chManager.store = &ChannelStore{
				store: watchkv,
				channelsInfo: map[int64]*NodeChannelInfo{
					nodeID: {nodeID, []*channel{{Name: channelName, CollectionID: collectionID}}}},
			}

			require.NoError(t, err)
			chManager.AddNode(UniqueID(111))
			err = watchkv.Save(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName), string(data))
			require.NoError(t, err)
			err = chManager.checkOldNodes([]UniqueID{nodeID})
			assert.NoError(t, err)

			waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, 111, channelName, collectionID)
			chManager.stateTimer.removeTimers([]string{channelName})

			v, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(nodeID, 10)))
			assert.Error(t, err)
			assert.Empty(t, v)
		})

		t.Run("ReleaseFail", func(t *testing.T) {
			defer watchkv.RemoveWithPrefix("")
			chManager, err := NewChannelManager(watchkv, newMockHandler())
			require.NoError(t, err)
			data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_ReleaseFailure, collectionID, channelName))
			chManager.store = &ChannelStore{
				store: watchkv,
				channelsInfo: map[int64]*NodeChannelInfo{
					nodeID: {nodeID, []*channel{{Name: channelName, CollectionID: collectionID}}},
					999:    {999, []*channel{}},
				},
			}
			require.NoError(t, err)
			err = watchkv.Save(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName), string(data))
			require.NoError(t, err)
			err = chManager.checkOldNodes([]UniqueID{nodeID})
			assert.NoError(t, err)

			waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, 999, channelName, collectionID)

			v, err := watchkv.Load(path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName))
			assert.Error(t, err)
			assert.Empty(t, v)

		})
	})

	t.Run("test reload with data", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		cm, err := NewChannelManager(watchkv, newMockHandler())
		assert.NoError(t, err)
		assert.Nil(t, cm.AddNode(1))
		assert.Nil(t, cm.AddNode(2))
		cm.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				1: {1, []*channel{{Name: "channel1", CollectionID: 1}}},
				2: {2, []*channel{{Name: "channel2", CollectionID: 1}}},
			},
		}

		data, err := proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_WatchSuccess, 1, "channel1"))
		require.NoError(t, err)
		err = watchkv.Save(path.Join(prefix, strconv.FormatInt(1, 10), "channel1"), string(data))
		require.NoError(t, err)
		data, err = proto.Marshal(getWatchInfoWithState(datapb.ChannelWatchState_WatchSuccess, 1, "channel2"))
		require.NoError(t, err)
		err = watchkv.Save(path.Join(prefix, strconv.FormatInt(2, 10), "channel2"), string(data))
		require.NoError(t, err)

		cm2, err := NewChannelManager(watchkv, newMockHandler())
		assert.NoError(t, err)
		assert.Nil(t, cm2.Startup(ctx, []int64{3}))

		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, 3, "channel1", 1)
		waitAndCheckState(t, watchkv, datapb.ChannelWatchState_ToWatch, 3, "channel2", 1)
		assert.True(t, cm2.Match(3, "channel1"))
		assert.True(t, cm2.Match(3, "channel2"))

		cm2.stateTimer.removeTimers([]string{"channel1", "channel2"})
	})
}

func TestChannelManager_BalanceBehaviour(t *testing.T) {
	watchkv := getWatchKV(t)
	defer func() {
		watchkv.RemoveWithPrefix("")
		watchkv.Close()
	}()

	prefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()

	t.Run("one node with three channels add a new node", func(t *testing.T) {
		defer watchkv.RemoveWithPrefix("")

		var (
			collectionID = UniqueID(999)
		)

		chManager, err := NewChannelManager(watchkv, newMockHandler(), withStateChecker())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.TODO())
		chManager.stopChecker = cancel
		defer cancel()
		go chManager.stateChecker(ctx, common.LatestRevision)

		chManager.store = &ChannelStore{
			store: watchkv,
			channelsInfo: map[int64]*NodeChannelInfo{
				1: {1, []*channel{
					{Name: "channel-1", CollectionID: collectionID},
					{Name: "channel-2", CollectionID: collectionID},
					{Name: "channel-3", CollectionID: collectionID}}}},
		}

		var (
			channelBalanced string
		)

		chManager.AddNode(2)
		channelBalanced = "channel-1"

		// waitAndStore := func(waitState, storeState datapb.ChannelWatchState, nodeID UniqueID, channelName string) {
		//     for {
		//         key := path.Join(prefix, strconv.FormatInt(nodeID, 10), channelName)
		//         v, err := watchkv.Load(key)
		//         if err == nil && len(v) > 0 {
		//             watchInfo, err := parseWatchInfo(key, []byte(v))
		//             require.NoError(t, err)
		//             require.Equal(t, waitState, watchInfo.GetState())
		//
		//             watchInfo.State = storeState
		//             data, err := proto.Marshal(watchInfo)
		//             require.NoError(t, err)
		//
		//             watchkv.Save(key, string(data))
		//             break
		//         }
		//         time.Sleep(100 * time.Millisecond)
		//     }
		// }

		key := path.Join(prefix, "1", channelBalanced)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToRelease, datapb.ChannelWatchState_ReleaseSuccess)

		key = path.Join(prefix, "2", channelBalanced)
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		assert.True(t, chManager.Match(1, "channel-2"))
		assert.True(t, chManager.Match(1, "channel-3"))

		assert.True(t, chManager.Match(2, "channel-1"))

		chManager.AddNode(3)
		chManager.Watch(&channel{Name: "channel-4", CollectionID: collectionID})
		key = path.Join(prefix, "3", "channel-4")
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		assert.True(t, chManager.Match(1, "channel-2"))
		assert.True(t, chManager.Match(1, "channel-3"))
		assert.True(t, chManager.Match(2, "channel-1"))
		assert.True(t, chManager.Match(3, "channel-4"))

		chManager.DeleteNode(3)
		key = path.Join(prefix, "2", "channel-4")
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		assert.True(t, chManager.Match(1, "channel-2"))
		assert.True(t, chManager.Match(1, "channel-3"))
		assert.True(t, chManager.Match(2, "channel-1"))
		assert.True(t, chManager.Match(2, "channel-4"))

		chManager.DeleteNode(2)
		key = path.Join(prefix, "1", "channel-4")
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)
		key = path.Join(prefix, "1", "channel-1")
		waitAndStore(t, watchkv, key, datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_WatchSuccess)

		assert.True(t, chManager.Match(1, "channel-2"))
		assert.True(t, chManager.Match(1, "channel-3"))
		assert.True(t, chManager.Match(1, "channel-1"))
		assert.True(t, chManager.Match(1, "channel-4"))
	})

}

func TestChannelManager_RemoveChannel(t *testing.T) {
	watchkv := getWatchKV(t)
	defer func() {
		watchkv.RemoveWithPrefix("")
		watchkv.Close()
	}()

	type fields struct {
		store RWChannelStore
	}
	type args struct {
		channelName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test remove existed channel",
			fields{
				store: &ChannelStore{
					store: watchkv,
					channelsInfo: map[int64]*NodeChannelInfo{
						1: {
							NodeID: 1,
							Channels: []*channel{
								{Name: "ch1", CollectionID: 1},
							},
						},
					},
				},
			},
			args{
				"ch1",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ChannelManager{
				store: tt.fields.store,
			}
			err := c.RemoveChannel(tt.args.channelName)
			assert.Equal(t, tt.wantErr, err != nil)
			_, ch := c.findChannel(tt.args.channelName)
			assert.Nil(t, ch)
		})
	}
}

func TestChannelManager_HelperFunc(t *testing.T) {
	c := &ChannelManager{}
	t.Run("test getOldOnlines", func(t *testing.T) {
		tests := []struct {
			nodes  []int64
			oNodes []int64

			expectedOut []int64
			desription  string
		}{
			{[]int64{}, []int64{}, []int64{}, "empty both"},
			{[]int64{1}, []int64{}, []int64{}, "empty oNodes"},
			{[]int64{}, []int64{1}, []int64{}, "empty nodes"},
			{[]int64{1}, []int64{1}, []int64{1}, "same one"},
			{[]int64{1, 2}, []int64{1}, []int64{1}, "same one 2"},
			{[]int64{1}, []int64{1, 2}, []int64{1}, "same one 3"},
			{[]int64{1, 2}, []int64{1, 2}, []int64{1, 2}, "same two"},
		}

		for _, test := range tests {
			t.Run(test.desription, func(t *testing.T) {
				nodes := c.getOldOnlines(test.nodes, test.oNodes)
				assert.ElementsMatch(t, test.expectedOut, nodes)
			})
		}
	})

	t.Run("test getNewOnLines", func(t *testing.T) {
		tests := []struct {
			nodes  []int64
			oNodes []int64

			expectedOut []int64
			desription  string
		}{
			{[]int64{}, []int64{}, []int64{}, "empty both"},
			{[]int64{1}, []int64{}, []int64{1}, "empty oNodes"},
			{[]int64{}, []int64{1}, []int64{}, "empty nodes"},
			{[]int64{1}, []int64{1}, []int64{}, "same one"},
			{[]int64{1, 2}, []int64{1}, []int64{2}, "same one 2"},
			{[]int64{1}, []int64{1, 2}, []int64{}, "same one 3"},
			{[]int64{1, 2}, []int64{1, 2}, []int64{}, "same two"},
		}

		for _, test := range tests {
			t.Run(test.desription, func(t *testing.T) {
				nodes := c.getNewOnLines(test.nodes, test.oNodes)
				assert.ElementsMatch(t, test.expectedOut, nodes)
			})
		}

	})
}
