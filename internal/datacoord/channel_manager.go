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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"

	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"stathat.com/c/consistent"
)

const (
	bgCheckInterval  = 3 * time.Second
	maxWatchDuration = 20 * time.Second
)

// ChannelManager manages the allocation and the balance between channels and data nodes.
type ChannelManager struct {
	ctx              context.Context
	mu               sync.RWMutex
	h                Handler
	store            RWChannelStore
	factory          ChannelPolicyFactory
	registerPolicy   RegisterPolicy
	deregisterPolicy DeregisterPolicy
	assignPolicy     ChannelAssignPolicy
	reassignPolicy   ChannelReassignPolicy
	bgChecker        ChannelBGChecker
	msgstreamFactory msgstream.Factory

	stateChecker channelStateChecker
	stopChecker  context.CancelFunc
	stateTimer   *channelStateTimer
}

type channel struct {
	Name         string
	CollectionID UniqueID
}

// ChannelManagerOpt is to set optional parameters in channel manager.
type ChannelManagerOpt func(c *ChannelManager)

func withFactory(f ChannelPolicyFactory) ChannelManagerOpt {
	return func(c *ChannelManager) { c.factory = f }
}

func defaultFactory(hash *consistent.Consistent) ChannelPolicyFactory {
	return NewConsistentHashChannelPolicyFactory(hash)
}

func withMsgstreamFactory(f msgstream.Factory) ChannelManagerOpt {
	return func(c *ChannelManager) { c.msgstreamFactory = f }
}

func withStateChecker() ChannelManagerOpt {
	return func(c *ChannelManager) { c.stateChecker = c.watchChannelStatesLoop }
}

// NewChannelManager creates and returns a new ChannelManager instance.
func NewChannelManager(
	kv kv.MetaKv, // for TxnKv and MetaKv
	h Handler,
	options ...ChannelManagerOpt,
) (*ChannelManager, error) {
	c := &ChannelManager{
		ctx:        context.TODO(),
		h:          h,
		factory:    NewChannelPolicyFactoryV1(kv),
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
	c.bgChecker = c.factory.NewBgChecker()
	return c, nil
}

// Startup adjusts the channel store according to current cluster states.
func (c *ChannelManager) Startup(ctx context.Context, nodes []int64) error {
	c.ctx = ctx
	channels := c.store.GetNodesChannels()
	// Retrieve the current old nodes.
	oNodes := make([]int64, 0, len(channels))
	for _, c := range channels {
		oNodes = append(oNodes, c.NodeID)
	}

	// Process watch states for old nodes.
	oldOnLines := c.getOldOnlines(nodes, oNodes)
	if err := c.checkOldNodes(oldOnLines); err != nil {
		return err
	}

	// Add new online nodes to the cluster.
	newOnLines := c.getNewOnLines(nodes, oNodes)
	for _, n := range newOnLines {
		if err := c.AddNode(n); err != nil {
			return err
		}
	}

	// Remove new offline nodes from the cluster.
	offLines := c.getOffLines(nodes, oNodes)
	for _, n := range offLines {
		if err := c.DeleteNode(n); err != nil {
			return err
		}
	}

	// Unwatch and drop channel with drop flag.
	c.unwatchDroppedChannels()

	if c.stateChecker != nil {
		ctx1, cancel := context.WithCancel(ctx)
		c.stopChecker = cancel
		go c.stateChecker(ctx1)
		log.Debug("starting etcd states checker")
	}

	log.Info("cluster start up",
		zap.Int64s("nodes", nodes),
		zap.Int64s("oNodes", oNodes),
		zap.Int64s("old onlines", oldOnLines),
		zap.Int64s("new onlines", newOnLines),
		zap.Int64s("offLines", offLines))
	return nil
}

// checkOldNodes processes the existing watch channels when starting up.
// ToWatch         get startTs and timeoutTs, start timer
// WatchSuccess    ignore
// WatchFail       ToRelease
// ToRelase        get startTs and timeoutTs, start timer
// ReleaseSuccess  remove
// ReleaseFail     clean up and remove
func (c *ChannelManager) checkOldNodes(nodes []UniqueID) error {
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

			log.Debug("processing watch info",
				zap.String("watch state", info.GetState().String()),
				zap.String("channel name", channelName))

			switch info.GetState() {
			case datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_Uncomplete:
				c.stateTimer.startOne(datapb.ChannelWatchState_ToWatch, channelName, nodeID, info.GetTimeoutTs())

			case datapb.ChannelWatchState_WatchFailure:
				if err := c.Release(nodeID, channelName); err != nil {
					return err
				}

			case datapb.ChannelWatchState_ToRelease:
				c.stateTimer.startOne(datapb.ChannelWatchState_ToRelease, channelName, nodeID, info.GetTimeoutTs())

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
func (c *ChannelManager) unwatchDroppedChannels() {
	nodeChannels := c.store.GetNodesChannels()
	for _, nodeChannel := range nodeChannels {
		for _, ch := range nodeChannel.Channels {
			if !c.h.CheckShouldDropChannel(ch.Name) {
				continue
			}
			err := c.remove(nodeChannel.NodeID, ch)
			if err != nil {
				log.Warn("unable to remove channel", zap.String("channel", ch.Name), zap.Error(err))
				continue
			}
			c.h.FinishDropChannel(ch.Name)
		}
	}
}

// NOT USED.
func (c *ChannelManager) bgCheckChannelsWork(ctx context.Context) {
	timer := time.NewTicker(bgCheckInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			c.mu.Lock()

			channels := c.store.GetNodesChannels()
			reallocates, err := c.bgChecker(channels, time.Now())
			if err != nil {
				log.Warn("channel manager bg check failed", zap.Error(err))

				c.mu.Unlock()
				continue
			}

			updates := c.reassignPolicy(c.store, reallocates)
			log.Info("channel manager bg check reassign", zap.Array("updates", updates))
			for _, update := range updates {
				if update.Type == Add {
					c.fillChannelWatchInfo(update)
				}
			}

			if err := c.store.Update(updates); err != nil {
				log.Warn("channel store update error", zap.Error(err))
			}

			c.mu.Unlock()
		}
	}
}

// getOldOnlines returns a list of old online node ids in `old` and in `curr`.
func (c *ChannelManager) getOldOnlines(curr []int64, old []int64) []int64 {
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
func (c *ChannelManager) getNewOnLines(curr []int64, old []int64) []int64 {
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
func (c *ChannelManager) getOffLines(curr []int64, old []int64) []int64 {
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
func (c *ChannelManager) AddNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Add(nodeID)

	updates := c.registerPolicy(c.store, nodeID)
	if len(updates) <= 0 {
		log.Info("register node with no reassignment", zap.Int64("registered node", nodeID))
		return nil
	}

	log.Info("register node",
		zap.Int64("registered node", nodeID),
		zap.Array("updates", updates))

	state := datapb.ChannelWatchState_ToRelease

	for _, u := range updates {
		if u.Type == Delete && u.NodeID == bufferID {
			state = datapb.ChannelWatchState_ToWatch
			break
		}
	}

	return c.updateWithTimer(updates, state)
}

// DeleteNode deletes the node from the cluster.
// DeleteNode deletes the nodeID's watchInfos in Etcd and reassign the channels to other Nodes
func (c *ChannelManager) DeleteNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeChannelInfo := c.store.GetNode(nodeID)
	if nodeChannelInfo == nil {
		return nil
	}

	c.unsubAttempt(nodeChannelInfo)

	updates := c.deregisterPolicy(c.store, nodeID)
	log.Warn("deregister node",
		zap.Int64("unregistered node", nodeID),
		zap.Array("updates", updates))
	if len(updates) <= 0 {
		return nil
	}

	var channels []*channel
	for _, op := range updates {
		if op.Type == Delete {
			channels = op.Channels
		}
	}

	chNames := make([]string, 0, len(channels))
	for _, ch := range channels {
		chNames = append(chNames, ch.Name)
	}
	log.Debug("remove timers for channel of the deregistered node",
		zap.Any("channels", chNames), zap.Int64("nodeID", nodeID))
	c.stateTimer.removeTimers(chNames)

	if err := c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch); err != nil {
		return err
	}

	// No channels will be return
	_, err := c.store.Delete(nodeID)
	return err
}

// unsubAttempt attempts to unsubscribe node-channel info from the channel.
func (c *ChannelManager) unsubAttempt(ncInfo *NodeChannelInfo) {
	if ncInfo == nil {
		return
	}

	if c.msgstreamFactory == nil {
		log.Warn("msgstream factory is not set")
		return
	}

	nodeID := ncInfo.NodeID
	for _, ch := range ncInfo.Channels {
		subName := fmt.Sprintf("%s-%d-%d", Params.CommonCfg.DataNodeSubName, nodeID, ch.CollectionID)
		pchannelName := funcutil.ToPhysicalChannel(ch.Name)
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}
}

// Watch tries to add the channel to cluster. Watch is a no op if the channel already exists.
func (c *ChannelManager) Watch(ch *channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.assignPolicy(c.store, []*channel{ch})
	if len(updates) == 0 {
		return nil
	}
	log.Info("try to update channel watch info with ToWatch state",
		zap.Any("channel", ch),
		zap.Array("updates", updates))

	err := c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)
	if err != nil {
		log.Warn("fail to update channel watch info with ToWatch state",
			zap.Any("channel", ch), zap.Array("updates", updates), zap.Error(err))
	}
	return err
}

// fillChannelWatchInfo updates the channel op by filling in channel watch info.
func (c *ChannelManager) fillChannelWatchInfo(op *ChannelOp) {
	for _, ch := range op.Channels {
		vcInfo := c.h.GetVChanPositions(ch.Name, ch.CollectionID, allPartitionID)
		info := &datapb.ChannelWatchInfo{
			Vchan:     vcInfo,
			StartTs:   time.Now().Unix(),
			State:     datapb.ChannelWatchState_Uncomplete,
			TimeoutTs: time.Now().Add(maxWatchDuration).UnixNano(),
		}
		op.ChannelWatchInfos = append(op.ChannelWatchInfos, info)
	}
}

// fillChannelWatchInfoWithState updates the channel op by filling in channel watch info.
func (c *ChannelManager) fillChannelWatchInfoWithState(op *ChannelOp, state datapb.ChannelWatchState) []string {
	var channelsWithTimer = []string{}
	startTs := time.Now().Unix()
	timeoutTs := time.Now().Add(maxWatchDuration).UnixNano()
	for _, ch := range op.Channels {
		vcInfo := c.h.GetVChanPositions(ch.Name, ch.CollectionID, allPartitionID)
		info := &datapb.ChannelWatchInfo{
			Vchan:     vcInfo,
			StartTs:   startTs,
			State:     state,
			TimeoutTs: timeoutTs,
		}

		// Only set timer for watchInfo not from bufferID
		if op.NodeID != bufferID {
			c.stateTimer.startOne(state, ch.Name, op.NodeID, timeoutTs)
			channelsWithTimer = append(channelsWithTimer, ch.Name)
		}

		op.ChannelWatchInfos = append(op.ChannelWatchInfos, info)
	}
	return channelsWithTimer
}

// GetChannels gets channels info of registered nodes.
func (c *ChannelManager) GetChannels() []*NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetNodesChannels()
}

// GetBufferChannels gets buffer channels.
func (c *ChannelManager) GetBufferChannels() *NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetBufferChannelInfo()
}

// Match checks and returns whether the node ID and channel match.
func (c *ChannelManager) Match(nodeID int64, channel string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := c.store.GetNode(nodeID)
	if info == nil {
		return false
	}

	for _, ch := range info.Channels {
		if ch.Name == channel {
			return true
		}
	}
	return false
}

// FindWatcher finds the datanode watching the provided channel.
func (c *ChannelManager) FindWatcher(channel string) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	infos := c.store.GetNodesChannels()
	for _, info := range infos {
		for _, channelInfo := range info.Channels {
			if channelInfo.Name == channel {
				return info.NodeID, nil
			}
		}
	}

	// channel in buffer
	bufferInfo := c.store.GetBufferChannelInfo()
	for _, channelInfo := range bufferInfo.Channels {
		if channelInfo.Name == channel {
			return bufferID, errChannelInBuffer
		}
	}
	return 0, errChannelNotWatched
}

// RemoveChannel removes the channel from channel manager.
func (c *ChannelManager) RemoveChannel(channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, ch := c.findChannel(channelName)
	if ch == nil {
		return nil
	}

	return c.remove(nodeID, ch)
}

// remove deletes the nodeID-channel pair from data store.
func (c *ChannelManager) remove(nodeID int64, ch *channel) error {
	var op ChannelOpSet
	op.Delete(nodeID, []*channel{ch})
	if err := c.store.Update(op); err != nil {
		return err
	}
	return nil
}

func (c *ChannelManager) findChannel(channelName string) (int64, *channel) {
	infos := c.store.GetNodesChannels()
	for _, info := range infos {
		for _, channelInfo := range info.Channels {
			if channelInfo.Name == channelName {
				return info.NodeID, channelInfo
			}
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

func (c *ChannelManager) updateWithTimer(updates ChannelOpSet, state datapb.ChannelWatchState) error {
	var channelsWithTimer = []string{}
	for _, op := range updates {
		if op.Type == Add {
			channelsWithTimer = append(channelsWithTimer, c.fillChannelWatchInfoWithState(op, state)...)
		}
	}

	err := c.store.Update(updates)
	if err != nil {
		log.Warn("fail to update", zap.Array("updates", updates))
		c.stateTimer.removeTimers(channelsWithTimer)
	}
	return err
}

func (c *ChannelManager) processAck(e *ackEvent) {
	c.stateTimer.stopIfExsit(e)

	switch e.ackType {
	case invalidAck:
		log.Warn("detected invalid Ack", zap.String("channel name", e.channelName))

	case watchSuccessAck:
		log.Info("datanode successfully watched channel", zap.Int64("nodeID", e.nodeID), zap.String("channel name", e.channelName))
	case watchFailAck, watchTimeoutAck: // failure acks from toWatch
		err := c.Release(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to set channels to release for watch failure ACKs",
				zap.Int64("nodeID", e.nodeID), zap.String("channel name", e.channelName), zap.Error(err))
		}

	case releaseFailAck, releaseTimeoutAck: // failure acks from toRelease
		// Cleanup, Delete and Reassign
		err := c.CleanupAndReassign(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to clean and reassign channels for release failure ACKs",
				zap.Int64("nodeID", e.nodeID), zap.String("channel name", e.channelName), zap.Error(err))
		}

	case releaseSuccessAck:
		// Delete and Reassign
		err := c.Reassign(e.nodeID, e.channelName)
		if err != nil {
			log.Warn("fail to response to release success ACK",
				zap.Int64("nodeID", e.nodeID), zap.String("channel name", e.channelName), zap.Error(err))
		}
	}
}

// CleanupAndReassign tries to clean up datanode's subscription, and then delete channel watch info.
func (c *ChannelManager) CleanupAndReassign(nodeID UniqueID, channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	chToCleanUp := c.getChannelByNodeAndName(nodeID, channelName)
	if chToCleanUp == nil {
		return fmt.Errorf("failed to find matching channel: %s and node: %d", channelName, nodeID)
	}

	if c.msgstreamFactory == nil {
		log.Warn("msgstream factory is not set, unable to clean up topics")
	} else {
		subName := fmt.Sprintf("%s-%d-%d", Params.CommonCfg.DataNodeSubName, nodeID, chToCleanUp.CollectionID)
		pchannelName := funcutil.ToPhysicalChannel(channelName)
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}

	err := c.remove(nodeID, chToCleanUp)
	if err != nil {
		return fmt.Errorf("failed to remove watch info: %v,%s", chToCleanUp, err.Error())
	}

	if c.isMarkedDrop(channelName) {
		log.Debug("try to cleanup removal flag ", zap.String("channel name", channelName))
		c.h.FinishDropChannel(channelName)
		return nil
	}

	reallocates := &NodeChannelInfo{nodeID, []*channel{chToCleanUp}}

	// reassign policy won't choose the same Node for a ressignment of a channel
	updates := c.reassignPolicy(c.store, []*NodeChannelInfo{reallocates})
	if len(updates) <= 0 {
		log.Warn("fail to reassign channel to other nodes, add channel to the original node",
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName))
		updates.Add(nodeID, []*channel{chToCleanUp})
	}

	log.Info("channel manager reassign channels", zap.Int64("old nodeID", nodeID), zap.Array("updates", updates))

	return c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)
}

type channelStateChecker func(context.Context)

func (c *ChannelManager) watchChannelStatesLoop(ctx context.Context) {
	defer logutil.LogPanic()

	// REF MEP#7 watchInfo paths are orgnized as: [prefix]/channel/{node_id}/{channel_name}
	watchPrefix := Params.DataCoordCfg.ChannelWatchSubPath
	etcdWatcher, timeoutWatcher := c.stateTimer.getWatchers(watchPrefix)

	for {
		select {
		case <-ctx.Done():
			log.Info("watch etcd loop quit")
			return
		case ackEvent := <-timeoutWatcher:
			log.Debug("receive timeout acks from state watcher",
				zap.Int64("nodeID", ackEvent.nodeID), zap.String("channel name", ackEvent.channelName))
			c.processAck(ackEvent)
		case event := <-etcdWatcher:
			if event.Canceled {
				log.Warn("watch channel canceled", zap.Error(event.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				if event.Err() == v3rpc.ErrCompacted {
					go c.watchChannelStatesLoop(ctx)
					return
				}
				// if watch loop return due to event canceled, the datacoord is not functional anymore
				log.Panic("datacoord is not functional for event canceled")
			}

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

				// ignore these states
				state := watchInfo.GetState()
				if state == datapb.ChannelWatchState_ToWatch ||
					state == datapb.ChannelWatchState_ToRelease ||
					state == datapb.ChannelWatchState_Uncomplete {
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

// Release writes ToRlease channel watch states for a channel
func (c *ChannelManager) Release(nodeID UniqueID, channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	toReleaseChannel := c.getChannelByNodeAndName(nodeID, channelName)
	if toReleaseChannel == nil {
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", nodeID, channelName)
	}

	toReleaseUpdates := getReleaseOp(nodeID, toReleaseChannel)
	err := c.updateWithTimer(toReleaseUpdates, datapb.ChannelWatchState_ToRelease)
	if err != nil {
		log.Debug("fail to update to release with timer", zap.Array("to release updates", toReleaseUpdates))
	}

	return err
}

// Reassign removes channel assignment from a datanode
func (c *ChannelManager) Reassign(nodeID UniqueID, channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := c.getChannelByNodeAndName(nodeID, channelName)
	if ch == nil {
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", nodeID, channelName)
	}

	if err := c.remove(nodeID, ch); err != nil {
		return fmt.Errorf("failed to remove watch info: %v,%s", ch, err.Error())
	}

	if c.isMarkedDrop(channelName) {
		log.Debug("try to cleanup removal flag ", zap.String("channel name", channelName))
		c.h.FinishDropChannel(channelName)

		log.Info("removed channel assignment", zap.Any("channel", ch))
		return nil
	}

	reallocates := &NodeChannelInfo{nodeID, []*channel{ch}}

	// reassign policy won't choose the same Node for a ressignment of a channel
	updates := c.reassignPolicy(c.store, []*NodeChannelInfo{reallocates})
	if len(updates) <= 0 {
		log.Warn("fail to reassign channel to other nodes, assign to the original Node",
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName))
		updates.Add(nodeID, []*channel{ch})
	}

	log.Info("channel manager reassign channels", zap.Int64("old node ID", nodeID), zap.Array("updates", updates))

	return c.updateWithTimer(updates, datapb.ChannelWatchState_ToWatch)

}

func (c *ChannelManager) getChannelByNodeAndName(nodeID UniqueID, channelName string) *channel {
	var ret *channel

	nodeChannelInfo := c.store.GetNode(nodeID)
	if nodeChannelInfo == nil {
		return nil
	}

	for _, channel := range nodeChannelInfo.Channels {
		if channel.Name == channelName {
			ret = channel
			break
		}
	}
	return ret
}

func (c *ChannelManager) getNodeIDByChannelName(chName string) (bool, UniqueID) {
	for _, nodeChannel := range c.GetChannels() {
		for _, ch := range nodeChannel.Channels {
			if ch.Name == chName {
				return true, nodeChannel.NodeID
			}
		}
	}
	return false, 0
}

func (c *ChannelManager) isMarkedDrop(channelName string) bool {
	return c.h.CheckShouldDropChannel(channelName)
}

func getReleaseOp(nodeID UniqueID, ch *channel) ChannelOpSet {
	var op ChannelOpSet
	op.Add(nodeID, []*channel{ch})
	return op
}
