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
	"fmt"
	"time"

	"github.com/samber/lo"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lock"
<<<<<<< HEAD
	"github.com/milvus-io/milvus/pkg/util/logutil"
=======
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
>>>>>>> 1bcf3d9ee7 (fix: Fix consume blocked due to too many consumers)
)

// ChannelManagerImpl manages the allocation and the balance between channels and data nodes.
type ChannelManagerImpl struct {
	ctx              context.Context
	mu               lock.RWMutex
	h                Handler
	store            RWChannelStore
	factory          ChannelPolicyFactory
	registerPolicy   RegisterPolicy
	deregisterPolicy DeregisterPolicy
	assignPolicy     ChannelAssignPolicy
	reassignPolicy   ChannelReassignPolicy
	balancePolicy    BalanceChannelPolicy
	bgChecker        ChannelBGChecker
	msgstreamFactory msgstream.Factory

	stateChecker channelStateChecker
	stopChecker  context.CancelFunc
	stateTimer   *channelStateTimer

	lastActiveTimestamp time.Time
}

// ChannelManagerOpt is to set optional parameters in channel manager.
type ChannelManagerOpt func(c *ChannelManagerImpl)

func withFactory(f ChannelPolicyFactory) ChannelManagerOpt {
	return func(c *ChannelManagerImpl) { c.factory = f }
}

func withMsgstreamFactory(f msgstream.Factory) ChannelManagerOpt {
	return func(c *ChannelManagerImpl) { c.msgstreamFactory = f }
}

func withStateChecker() ChannelManagerOpt {
	return func(c *ChannelManagerImpl) { c.stateChecker = c.watchChannelStatesLoop }
}

func withBgChecker() ChannelManagerOpt {
	return func(c *ChannelManagerImpl) { c.bgChecker = c.bgCheckChannelsWork }
}

// NewChannelManager creates and returns a new ChannelManager instance.
func NewChannelManager(
	kv kv.WatchKV, // for TxnKv, MetaKv and WatchKV
	h Handler,
	options ...ChannelManagerOpt,
) (*ChannelManagerImpl, error) {
	c := &ChannelManagerImpl{
		ctx:        context.TODO(),
		h:          h,
		factory:    NewChannelPolicyFactoryV1(),
		store:      NewChannelStore(kv),
		stateTimer: newChannelStateTimer(kv),
	}

	if err := c.store.Reload(); err != nil {
		return nil, err
	}

	for _, opt := range options {
		opt(c)
	}

	c.registerPolicy = c.factory.NewRegisterPolicy()
	c.deregisterPolicy = c.factory.NewDeregisterPolicy()
	c.assignPolicy = c.factory.NewAssignPolicy()
	c.reassignPolicy = c.factory.NewReassignPolicy()
	c.balancePolicy = c.factory.NewBalancePolicy()
	c.lastActiveTimestamp = time.Now()
	return c, nil
}

// Startup adjusts the channel store according to current cluster states.
func (c *ChannelManagerImpl) Startup(ctx context.Context, legacyNodes, allNodes []int64) error {
	c.ctx = ctx
	channels := c.store.GetNodesChannels()
	// Retrieve the current old nodes.
	oNodes := make([]int64, 0, len(channels))
	for _, c := range channels {
		oNodes = append(oNodes, c.NodeID)
	}

	// Process watch states for old nodes.
	oldOnLines := c.getOldOnlines(allNodes, oNodes)
	if err := c.checkOldNodes(oldOnLines); err != nil {
		return err
	}

	// Add new online nodes to the cluster.
	newOnLines := c.getNewOnLines(allNodes, oNodes)
	for _, n := range newOnLines {
		if err := c.AddNode(n); err != nil {
			return err
		}
	}

	// Remove new offline nodes from the cluster.
	offLines := c.getOffLines(allNodes, oNodes)
	for _, n := range offLines {
		if err := c.DeleteNode(n); err != nil {
			return err
		}
	}

	// Unwatch and drop channel with drop flag.
	c.unwatchDroppedChannels()

	checkerContext, cancel := context.WithCancel(ctx)
	c.stopChecker = cancel
	if c.stateChecker != nil {
		// TODO get revision from reload logic
		go c.stateChecker(checkerContext, common.LatestRevision)
		log.Info("starting etcd states checker")
	}

	if c.bgChecker != nil {
		go c.bgChecker(checkerContext)
		log.Info("starting background balance checker")
	}

	log.Info("cluster start up",
		zap.Int64s("nodes", allNodes),
		zap.Int64s("oNodes", oNodes),
		zap.Int64s("old onlines", oldOnLines),
		zap.Int64s("new onlines", newOnLines),
		zap.Int64s("offLines", offLines))
	return nil
}

// Close notifies the running checker.
func (c *ChannelManagerImpl) Close() {
	if c.stopChecker != nil {
		c.stopChecker()
	}
}

// checkOldNodes processes the existing watch channels when starting up.
// ToWatch         get startTs and timeoutTs, start timer
// WatchSuccess    ignore
// WatchFail       ToRelease
// ToRelase        get startTs and timeoutTs, start timer
// ReleaseSuccess  remove
// ReleaseFail     clean up and remove
func (c *ChannelManagerImpl) checkOldNodes(nodes []UniqueID) error {
	// Load all the watch infos before processing
	nodeWatchInfos := make(map[UniqueID][]*datapb.ChannelWatchInfo)
	for _, nodeID := range nodes {
		watchInfos, err := c.stateTimer.loadAllChannels(nodeID)
		if err != nil {
			return err
		}
		nodeWatchInfos[nodeID] = watchInfos
	}

	for nodeID, watchInfos := range nodeWatchInfos {
		for _, info := range watchInfos {
			channelName := info.GetVchan().GetChannelName()
			checkInterval := Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second)

			log.Info("processing watch info",
				zap.String("watch state", info.GetState().String()),
				zap.String("channelName", channelName))

			switch info.GetState() {
			case datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_Uncomplete:
				c.stateTimer.startOne(datapb.ChannelWatchState_ToWatch, channelName, nodeID, checkInterval)

			case datapb.ChannelWatchState_WatchFailure:
				if err := c.Release(nodeID, channelName); err != nil {
					return err
				}

			case datapb.ChannelWatchState_ToRelease:
				c.stateTimer.startOne(datapb.ChannelWatchState_ToRelease, channelName, nodeID, checkInterval)

			case datapb.ChannelWatchState_ReleaseSuccess:
				if err := c.Reassign(nodeID, channelName); err != nil {
					return err
				}

			case datapb.ChannelWatchState_ReleaseFailure:
				if err := c.CleanupAndReassign(nodeID, channelName); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// unwatchDroppedChannels removes drops channel that are marked to drop.
func (c *ChannelManagerImpl) unwatchDroppedChannels() {
	nodeChannels := c.store.GetNodesChannels()
	for _, nodeChannel := range nodeChannels {
		for _, ch := range nodeChannel.Channels {
			if !c.isMarkedDrop(ch.GetName()) {
				continue
			}
			err := c.remove(nodeChannel.NodeID, ch)
			if err != nil {
				log.Warn("unable to remove channel", zap.String("channel", ch.GetName()), zap.Error(err))
				continue
			}
			err = c.h.FinishDropChannel(ch.GetName(), ch.GetCollectionID())
			if err != nil {
				log.Warn("FinishDropChannel failed when unwatchDroppedChannels", zap.String("channel", ch.GetName()), zap.Error(err))
			}
		}
	}
}

func (c *ChannelManagerImpl) bgCheckChannelsWork(ctx context.Context) {
	ticker := time.NewTicker(Params.DataCoordCfg.ChannelBalanceInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("background checking channels loop quit")
			return
		case <-ticker.C:
			if !Params.DataCoordCfg.AutoBalance.GetAsBool() {
				log.Info("auto balance disabled, skip auto bg check balance")
				continue
			}

			c.mu.Lock()
			if !c.isSilent() {
				log.Info("ChannelManager is not silent, skip channel balance this round")
			} else {
				currCluster := c.store.GetNodesChannels()
				updates := c.balancePolicy(currCluster)
				if updates == nil {
					continue
				}

				log.Info("channel manager bg check balance", zap.Array("toReleases", updates))
				if err := c.updateWithTimer(updates, datapb.ChannelWatchState_ToRelease); err != nil {
					log.Warn("channel store update error", zap.Error(err))
				}
			}
			c.mu.Unlock()
		}
	}
}

// getOldOnlines returns a list of old online node ids in `old` and in `curr`.
func (c *ChannelManagerImpl) getOldOnlines(curr []int64, old []int64) []int64 {
	mcurr := make(map[int64]struct{})
	ret := make([]int64, 0, len(old))
	for _, n := range curr {
		mcurr[n] = struct{}{}
	}
	for _, n := range old {
		if _, found := mcurr[n]; found {
			ret = append(ret, n)
		}
	}
	return ret
}

// getNewOnLines returns a list of new online node ids in `curr` but not in `old`.
func (c *ChannelManagerImpl) getNewOnLines(curr []int64, old []int64) []int64 {
	mold := make(map[int64]struct{})
	ret := make([]int64, 0, len(curr))
	for _, n := range old {
		mold[n] = struct{}{}
	}
	for _, n := range curr {
		if _, found := mold[n]; !found {
			ret = append(ret, n)
		}
	}
	return ret
}

// getOffLines returns a list of new offline node ids in `old` but not in `curr`.
func (c *ChannelManagerImpl) getOffLines(curr []int64, old []int64) []int64 {
	mcurr := make(map[int64]struct{})
	ret := make([]int64, 0, len(old))
	for _, n := range curr {
		mcurr[n] = struct{}{}
	}
	for _, n := range old {
		if _, found := mcurr[n]; !found {
			ret = append(ret, n)
		}
	}
	return ret
}

// AddNode adds a new node to cluster and reassigns the node - channel mapping.
func (c *ChannelManagerImpl) AddNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.AddNode(nodeID)

	bufferedUpdates, balanceUpdates := c.registerPolicy(c.store, nodeID)

	updates := bufferedUpdates
	// try bufferedUpdates first
	if updates == nil {
		if !Params.DataCoordCfg.AutoBalance.GetAsBool() {
			log.Info("auto balance disabled, skip reassignment for balance", zap.Int64("registered node", nodeID))
			return nil
		}
		updates = balanceUpdates
	}

	if updates == nil {
		log.Info("register node with no reassignment", zap.Int64("registered node", nodeID))
		return nil
	}

	log.Info("register node", zap.Int64("registered node", nodeID), zap.Array("updates", updates))

	state := datapb.ChannelWatchState_ToRelease

	for _, u := range updates.Collect() {
		if u.Type == Delete && u.NodeID == bufferID {
			state = datapb.ChannelWatchState_ToWatch
			break
		}
	}

	return c.updateWithTimer(updates, state)
}

// DeleteNode deletes the node from the cluster.
// DeleteNode deletes the nodeID's watchInfos in Etcd and reassign the channels to other Nodes
func (c *ChannelManagerImpl) DeleteNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeChannelInfo := c.store.GetNode(nodeID)
	if nodeChannelInfo == nil {
		c.store.RemoveNode(nodeID)
		return nil
	}

	c.unsubAttempt(nodeChannelInfo)

	updates := c.deregisterPolicy(c.store, nodeID)
	if updates == nil {
		c.store.RemoveNode(nodeID)
		return nil
	}
	log.Info("deregister node", zap.Int64("nodeID", nodeID), zap.Array("updates", updates))

	var channels []RWChannel
	for _, op := range updates.Collect() {
		if op.Type == Delete {
			channels = op.Channels
		}
	}

	chNames := make([]string, 0, len(channels))
	for _, ch := range channels {
		chNames = append(chNames, ch.GetName())
	}
	log.Info("remove timers for channel of the deregistered node",
		zap.Strings("channels", chNames), zap.Int64("nodeID", nodeID))
	c.stateTimer.removeTimers(chNames)

	if err := c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch); err != nil {
		return err
	}

	// No channels will be return
	c.store.RemoveNode(nodeID)
	return nil
}

// unsubAttempt attempts to unsubscribe node-channel info from the channel.
func (c *ChannelManagerImpl) unsubAttempt(ncInfo *NodeChannelInfo) {
	if ncInfo == nil {
		return
	}

	if c.msgstreamFactory == nil {
		log.Warn("msgstream factory is not set")
		return
	}

	nodeID := ncInfo.NodeID
	for _, ch := range ncInfo.Channels {
		// align to datanode subname, using vchannel name
		subName := fmt.Sprintf("%s-%d-%s", Params.CommonCfg.DataNodeSubName.GetValue(), nodeID, ch.GetName())
		pchannelName := funcutil.ToPhysicalChannel(ch.GetName())
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}
}

// Watch tries to add the channel to cluster. Watch is a no op if the channel already exists.
func (c *ChannelManagerImpl) Watch(ctx context.Context, ch RWChannel) error {
	log := log.Ctx(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.assignPolicy(c.store, []RWChannel{ch})
	if updates == nil {
		return nil
	}
	log.Info("try to update channel watch info with ToWatch state",
		zap.String("channel", ch.String()),
		zap.Array("updates", updates))

	err := c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)
	if err != nil {
		log.Warn("fail to update channel watch info with ToWatch state",
			zap.String("channel", ch.String()), zap.Array("updates", updates), zap.Error(err))
	}
	return err
}

// fillChannelWatchInfoWithState updates the channel op by filling in channel watch info.
func (c *ChannelManagerImpl) fillChannelWatchInfoWithState(op *ChannelOp, state datapb.ChannelWatchState) []string {
	channelsWithTimer := []string{}
	startTs := time.Now().Unix()
	checkInterval := Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second)
	for _, ch := range op.Channels {
		vcInfo := c.h.GetDataVChanPositions(ch, allPartitionID)
		info := &datapb.ChannelWatchInfo{
			Vchan:   vcInfo,
			StartTs: startTs,
			State:   state,
			Schema:  ch.GetSchema(),
		}

		// Only set timer for watchInfo not from bufferID
		if op.NodeID != bufferID {
			c.stateTimer.startOne(state, ch.GetName(), op.NodeID, checkInterval)
			channelsWithTimer = append(channelsWithTimer, ch.GetName())
		}

		ch.UpdateWatchInfo(info)
	}
	return channelsWithTimer
}

// GetAssignedChannels gets channels info of registered nodes.
func (c *ChannelManagerImpl) GetAssignedChannels() []*NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetNodesChannels()
}

// GetBufferChannels gets buffer channels.
func (c *ChannelManagerImpl) GetBufferChannels() *NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetBufferChannelInfo()
}

// GetNodeChannelsByCollectionID gets all node channels map of the collection
func (c *ChannelManagerImpl) GetNodeChannelsByCollectionID(collectionID UniqueID) map[UniqueID][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.GetNodeChannelsByCollectionID(collectionID)
}

// Get all channels belong to the collection
func (c *ChannelManagerImpl) GetChannelsByCollectionID(collectionID UniqueID) []RWChannel {
	channels := make([]RWChannel, 0)
	for _, nodeChannels := range c.GetAssignedChannels() {
		for _, ch := range nodeChannels.Channels {
			if ch.GetCollectionID() == collectionID {
				channels = append(channels, ch)
			}
		}
	}
	return channels
}

// Get all channel names belong to the collection
func (c *ChannelManagerImpl) GetChannelNamesByCollectionID(collectionID UniqueID) []string {
	channels := c.GetChannelsByCollectionID(collectionID)
	return lo.Map(channels, func(channel RWChannel, _ int) string {
		return channel.GetName()
	})
}

// Match checks and returns whether the node ID and channel match.
// use vchannel
func (c *ChannelManagerImpl) Match(nodeID int64, channel string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := c.store.GetNode(nodeID)
	if info == nil {
		return false
	}

	if _, ok := info.Channels[channel]; ok {
		return true
	}
	return false
}

// FindWatcher finds the datanode watching the provided channel.
func (c *ChannelManagerImpl) FindWatcher(channelName string) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	infos := c.store.GetNodesChannels()
	for _, info := range infos {
		if _, ok := info.Channels[channelName]; ok {
			return info.NodeID, nil
		}
	}

	// channel in buffer
	bufferInfo := c.store.GetBufferChannelInfo()
	if _, ok := bufferInfo.Channels[channelName]; ok {
		return bufferID, errChannelInBuffer
	}
	return 0, errChannelNotWatched
}

// RemoveChannel removes the channel from channel manager.
func (c *ChannelManagerImpl) RemoveChannel(channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, ch := c.findChannel(channelName)
	if ch == nil {
		return nil
	}

	return c.remove(nodeID, ch)
}

// remove deletes the nodeID-channel pair from data store.
func (c *ChannelManagerImpl) remove(nodeID int64, ch RWChannel) error {
	op := NewChannelOpSet(NewDeleteOp(nodeID, ch))
	log.Info("remove channel assignment",
		zap.Int64("nodeID to be removed", nodeID),
		zap.String("channel", ch.GetName()),
		zap.Int64("collectionID", ch.GetCollectionID()))
	if err := c.store.Update(op); err != nil {
		return err
	}
	return nil
}

func (c *ChannelManagerImpl) findChannel(channelName string) (int64, RWChannel) {
	infos := c.store.GetNodesChannels()
	for _, info := range infos {
		if channelInfo, ok := info.Channels[channelName]; ok {
			return info.NodeID, channelInfo
		}
	}
	return 0, nil
}

type ackType = int

const (
	invalidAck = iota
	watchSuccessAck
	watchFailAck
	watchTimeoutAck
	releaseSuccessAck
	releaseFailAck
	releaseTimeoutAck
)

type ackEvent struct {
	ackType     ackType
	channelName string
	nodeID      UniqueID
}

func (c *ChannelManagerImpl) updateWithTimer(updates *ChannelOpSet, state datapb.ChannelWatchState) error {
	channelsWithTimer := []string{}
	for _, op := range updates.Collect() {
		if op.Type != Delete {
			channelsWithTimer = append(channelsWithTimer, c.fillChannelWatchInfoWithState(op, state)...)
		}
	}

	err := c.store.Update(updates)
	if err != nil {
		log.Warn("fail to update", zap.Array("updates", updates), zap.Error(err))
		c.stateTimer.removeTimers(channelsWithTimer)
	}
	c.lastActiveTimestamp = time.Now()
	return err
}

func (c *ChannelManagerImpl) processAck(e *ackEvent) {
	c.stateTimer.stopIfExist(e)

	switch e.ackType {
	case invalidAck:
		log.Warn("detected invalid Ack", zap.String("channelName", e.channelName))

	case watchSuccessAck:
		log.Info("datanode successfully watched channel", zap.Int64("nodeID", e.nodeID), zap.String("channelName", e.channelName))
	case watchFailAck, watchTimeoutAck: // failure acks from toWatch
		log.Warn("datanode watch channel failed or timeout, will release", zap.Int64("nodeID", e.nodeID),
			zap.String("channel", e.channelName))
		err := c.Release(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to set channels to release for watch failure ACKs",
				zap.Int64("nodeID", e.nodeID), zap.String("channelName", e.channelName), zap.Error(err))
		}
	case releaseFailAck, releaseTimeoutAck: // failure acks from toRelease
		// Cleanup, Delete and Reassign
		log.Warn("datanode release channel failed or timeout, will cleanup and reassign", zap.Int64("nodeID", e.nodeID),
			zap.String("channel", e.channelName))
		err := c.CleanupAndReassign(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to clean and reassign channels for release failure ACKs",
				zap.Int64("nodeID", e.nodeID), zap.String("channelName", e.channelName), zap.Error(err))
		}

	case releaseSuccessAck:
		// Delete and Reassign
		log.Info("datanode release channel successfully, will reassign", zap.Int64("nodeID", e.nodeID),
			zap.String("channel", e.channelName))
		err := c.Reassign(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to response to release success ACK",
				zap.Int64("nodeID", e.nodeID), zap.String("channelName", e.channelName), zap.Error(err))
		}
	}
}

type channelStateChecker func(context.Context, int64)

func (c *ChannelManagerImpl) watchChannelStatesLoop(ctx context.Context, revision int64) {
	defer logutil.LogPanic()

	// REF MEP#7 watchInfo paths are orgnized as: [prefix]/channel/{node_id}/{channel_name}
	watchPrefix := Params.CommonCfg.DataCoordWatchSubPath.GetValue()
	// TODO, this is risky, we'd better watch etcd with revision rather simply a path
	var etcdWatcher clientv3.WatchChan
	var timeoutWatcher chan *ackEvent
	if revision == common.LatestRevision {
		etcdWatcher, timeoutWatcher = c.stateTimer.getWatchers(watchPrefix)
	} else {
		etcdWatcher, timeoutWatcher = c.stateTimer.getWatchersWithRevision(watchPrefix, revision)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("watch etcd loop quit")
			return
		case ackEvent := <-timeoutWatcher:
			log.Info("receive timeout acks from state watcher",
				zap.Int("state", ackEvent.ackType),
				zap.Int64("nodeID", ackEvent.nodeID), zap.String("channelName", ackEvent.channelName))
			c.processAck(ackEvent)
		case event, ok := <-etcdWatcher:
			if !ok {
				log.Warn("datacoord failed to watch channel, return")
				// rewatch for transient network error, session handles process quiting if connect is not recoverable
				go c.watchChannelStatesLoop(ctx, revision)
				return
			}

			if err := event.Err(); err != nil {
				log.Warn("datacoord watch channel hit error", zap.Error(event.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				// TODO add list and wathc with revision
				if event.Err() == v3rpc.ErrCompacted {
					go c.watchChannelStatesLoop(ctx, event.CompactRevision)
					return
				}
				// if watch loop return due to event canceled, the datacoord is not functional anymore
				log.Panic("datacoord is not functional for event canceled", zap.Error(err))
				return
			}

			revision = event.Header.GetRevision() + 1
			for _, evt := range event.Events {
				if evt.Type == clientv3.EventTypeDelete {
					continue
				}
				key := string(evt.Kv.Key)
				watchInfo, err := parseWatchInfo(key, evt.Kv.Value)
				if err != nil {
					log.Warn("fail to parse watch info", zap.Error(err))
					continue
				}

				// runnging states
				state := watchInfo.GetState()
				if state == datapb.ChannelWatchState_ToWatch ||
					state == datapb.ChannelWatchState_ToRelease ||
					state == datapb.ChannelWatchState_Uncomplete {
					c.stateTimer.resetIfExist(watchInfo.GetVchan().ChannelName, Params.DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second))
					log.Info("tickle update, timer delay", zap.String("channel", watchInfo.GetVchan().ChannelName), zap.Int32("progress", watchInfo.Progress))
					continue
				}

				nodeID, err := parseNodeKey(key)
				if err != nil {
					log.Warn("fail to parse node from key", zap.String("key", key), zap.Error(err))
					continue
				}

				ackEvent := parseAckEvent(nodeID, watchInfo)
				c.processAck(ackEvent)
			}
		}
	}
}

// Release writes ToRelease channel watch states for a channel
func (c *ChannelManagerImpl) Release(nodeID UniqueID, channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	toReleaseChannel := c.getChannelByNodeAndName(nodeID, channelName)
	if toReleaseChannel == nil {
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", nodeID, channelName)
	}

	toReleaseUpdates := NewChannelOpSet(NewAddOp(nodeID, toReleaseChannel))
	err := c.updateWithTimer(toReleaseUpdates, datapb.ChannelWatchState_ToRelease)
	if err != nil {
		log.Warn("fail to update to release with timer", zap.Array("to release updates", toReleaseUpdates))
	}

	return err
}

// Reassign reassigns a channel to another DataNode.
func (c *ChannelManagerImpl) Reassign(originNodeID UniqueID, channelName string) error {
	c.mu.RLock()
	ch := c.getChannelByNodeAndName(originNodeID, channelName)
	if ch == nil {
		c.mu.RUnlock()
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", originNodeID, channelName)
	}
	c.mu.RUnlock()

	reallocates := NewNodeChannelInfo(originNodeID, ch)
	isDropped := c.isMarkedDrop(channelName)

	if isDropped {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err := c.remove(originNodeID, ch); err != nil {
			return fmt.Errorf("failed to remove watch info: %v,%s", ch, err.Error())
		}
		if err := c.h.FinishDropChannel(channelName, ch.GetCollectionID()); err != nil {
			return fmt.Errorf("FinishDropChannel failed, err=%w", err)
		}
		log.Info("removed channel assignment", zap.String("channelName", channelName))
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Reassign policy won't choose the original node when a reassigning a channel.
	updates := c.reassignPolicy(c.store, []*NodeChannelInfo{reallocates})
	if updates == nil {
		// Skip the remove if reassign to the original node.
		log.Warn("failed to reassign channel to other nodes, assigning to the original DataNode",
			zap.Int64("nodeID", originNodeID),
			zap.String("channelName", channelName))
		updates = NewChannelOpSet(NewAddOp(originNodeID, ch))
	}

	log.Info("channel manager reassigning channels",
		zap.Int64("old node ID", originNodeID),
		zap.Array("updates", updates))
	return c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)
}

// CleanupAndReassign tries to clean up datanode's subscription, and then reassigns the channel to another DataNode.
func (c *ChannelManagerImpl) CleanupAndReassign(nodeID UniqueID, channelName string) error {
	c.mu.RLock()
	chToCleanUp := c.getChannelByNodeAndName(nodeID, channelName)
	if chToCleanUp == nil {
		c.mu.RUnlock()
		return fmt.Errorf("failed to find matching channel: %s and node: %d", channelName, nodeID)
	}
	c.mu.RUnlock()

	if c.msgstreamFactory == nil {
		log.Warn("msgstream factory is not set, unable to clean up topics")
	} else {
		subName := fmt.Sprintf("%s-%d-%d", Params.CommonCfg.DataNodeSubName.GetValue(), nodeID, chToCleanUp.GetCollectionID())
		pchannelName := funcutil.ToPhysicalChannel(channelName)
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}

	reallocates := NewNodeChannelInfo(nodeID, chToCleanUp)
	isDropped := c.isMarkedDrop(channelName)

	c.mu.Lock()
	defer c.mu.Unlock()
	if isDropped {
		if err := c.remove(nodeID, chToCleanUp); err != nil {
			return fmt.Errorf("failed to remove watch info: %v,%s", chToCleanUp, err.Error())
		}

		log.Info("try to cleanup removal flag ", zap.String("channelName", channelName))
		if err := c.h.FinishDropChannel(channelName, chToCleanUp.GetCollectionID()); err != nil {
			return fmt.Errorf("FinishDropChannel failed, err=%w", err)
		}

		log.Info("removed channel assignment", zap.Any("channel", chToCleanUp))
		return nil
	}

	// Reassign policy won't choose the original node when a reassigning a channel.
	updates := c.reassignPolicy(c.store, []*NodeChannelInfo{reallocates})
	if updates == nil {
		// Skip the remove if reassign to the original node.
		log.Warn("failed to reassign channel to other nodes, add channel to the original node",
			zap.Int64("node ID", nodeID),
			zap.String("channelName", channelName))
		updates = NewChannelOpSet(NewAddOp(nodeID, chToCleanUp))
	}

	log.Info("channel manager reassigning channels",
		zap.Int64("old nodeID", nodeID),
		zap.Array("updates", updates))
	return c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)
}

func (c *ChannelManagerImpl) getChannelByNodeAndName(nodeID UniqueID, channelName string) RWChannel {
	if nodeChannelInfo := c.store.GetNode(nodeID); nodeChannelInfo != nil {
		if ch, ok := nodeChannelInfo.Channels[channelName]; ok {
			return ch
		}
	}
	return nil
}

func (c *ChannelManagerImpl) GetCollectionIDByChannel(channelName string) (bool, UniqueID) {
	for _, nodeChannel := range c.GetAssignedChannels() {
		if ch, ok := nodeChannel.Channels[channelName]; ok {
			return true, ch.GetCollectionID()
		}
	}
	return false, 0
}

func (c *ChannelManagerImpl) GetChannel(nodeID int64, channelName string) (RWChannel, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch := c.getChannelByNodeAndName(nodeID, channelName)
	return ch, ch != nil
}

func (c *ChannelManagerImpl) isMarkedDrop(channel string) bool {
	return c.h.CheckShouldDropChannel(channel)
}

func (c *ChannelManagerImpl) isSilent() bool {
	if c.stateTimer.hasRunningTimers() {
		return false
	}
	return time.Since(c.lastActiveTimestamp) >= Params.DataCoordCfg.ChannelBalanceSilentDuration.GetAsDuration(time.Second)
}
