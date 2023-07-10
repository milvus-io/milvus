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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"stathat.com/c/consistent"
)

type ChannelManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	wg     sync.WaitGroup

	h         Handler
	store     RWChannelStore
	session   *SessionManager
	allocator allocator

	factory          ChannelPolicyFactory
	registerPolicy   RegisterPolicy
	deregisterPolicy DeregisterPolicy
	assignPolicy     ChannelAssignPolicy
	reassignPolicy   ChannelReassignPolicy
	balancePolicy    BalanceChannelPolicy
	msgstreamFactory msgstream.Factory
	channelChecker   *ChannelChecker

	balanceCheckLoop   ChannelBGChecker
	operationCheckLoop ChannelBGChecker

	lastActiveTimestamp time.Time
}

// ChannelBGChecker are goroutining running background
type ChannelBGChecker func(ctx context.Context)

type channel struct {
	Name            string
	CollectionID    UniqueID
	StartPositions  []*commonpb.KeyDataPair
	Schema          *schemapb.CollectionSchema
	CreateTimestamp uint64
}

// String implement Stringer.
func (ch *channel) String() string {
	// schema maybe too large to print
	return fmt.Sprintf("Name: %s, CollectionID: %d, StartPositions: %v", ch.Name, ch.CollectionID, ch.StartPositions)
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

func withOperationChecker() ChannelManagerOpt {
	return func(c *ChannelManager) { c.operationCheckLoop = c.checkChannelOperationLoop }
}

func withBalanceChecker() ChannelManagerOpt {
	return func(c *ChannelManager) { c.balanceCheckLoop = c.checkBalanceLoop }
}

// NewChannelManager creates and returns a new ChannelManager instance.
func NewChannelManager(
	kv kv.TxnKV,
	h Handler,
	session *SessionManager,
	alloc allocator,
	options ...ChannelManagerOpt,
) (*ChannelManager, error) {
	c := &ChannelManager{
		h:              h,
		ctx:            context.TODO(),
		factory:        NewChannelPolicyFactoryV1(kv),
		store:          NewChannelStore(kv),
		session:        session,
		allocator:      alloc,
		channelChecker: NewChannelChecker(session),
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

func (c *ChannelManager) Startup(ctx context.Context, nodes []int64) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	channels := c.store.GetNodesChannels()
	// Retrieve the current old nodes.
	oNodes := make([]int64, 0, len(channels))
	for _, c := range channels {
		oNodes = append(oNodes, c.NodeID)
	}

	// Process watch states for old nodes.
	oldOnLines := getOldOnlines(nodes, oNodes)
	if err := c.checkOldNodes(oldOnLines); err != nil {
		return err
	}

	// Add new online nodes to the cluster.
	newOnLines := getNewOnLines(nodes, oNodes)
	for _, n := range newOnLines {
		if err := c.RegisterNode(n); err != nil {
			return err
		}
	}

	// Remove new offline nodes from the cluster.
	offLines := getOffLines(nodes, oNodes)
	for _, n := range offLines {
		if err := c.UnregisterNode(n); err != nil {
			return err
		}
	}

	// Unwatch and drop channel with drop flag.
	c.unwatchDroppedChannels()

	if c.operationCheckLoop != nil {
		log.Info("starting channel operation loop")
		go c.checkChannelOperationLoop(c.ctx)
	}

	if c.balanceCheckLoop != nil {
		log.Info("starting channel balance loop")
		go c.checkBalanceLoop(c.ctx)
	}

	log.Info("cluster start up",
		zap.Int64s("nodes", nodes),
		zap.Int64s("oNodes", oNodes),
		zap.Int64s("old onlines", oldOnLines),
		zap.Int64s("new onlines", newOnLines),
		zap.Int64s("offLines", offLines))
	return nil
}

func (c *ChannelManager) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.channelChecker != nil {
		c.channelChecker.Close()
	}
	c.wg.Wait()
}

func (c *ChannelManager) RegisterNode(nodeID int64) error {
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

	return c.execute(updates, state)
}

func (c *ChannelManager) UnregisterNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	nodeChannelInfo := c.store.GetNode(nodeID)
	if nodeChannelInfo == nil {
		return nil
	}

	c.unsubAttempt(nodeChannelInfo)

	updates := c.deregisterPolicy(c.store, nodeID)
	log.Info("deregister node",
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

	if err := c.execute(updates, datapb.ChannelWatchState_ToWatch); err != nil {
		return err
	}
	_, err := c.store.Delete(nodeID)
	return err
}

func (c *ChannelManager) AddChannel(ch *channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.assignPolicy(c.store, []*channel{ch})
	if len(updates) == 0 {
		return nil
	}

	log.Info("add channel",
		zap.String("channel", ch.Name),
		zap.Array("updates", updates))

	return c.execute(updates, datapb.ChannelWatchState_ToWatch)
}

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

// Release writes ToRelease channel watch states for a channel
func (c *ChannelManager) Release(nodeID UniqueID, channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	toReleaseChannel := c.getChannelByNodeAndName(nodeID, channelName)
	if toReleaseChannel == nil {
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", nodeID, channelName)
	}

	toReleaseUpdates := getReleaseOp(nodeID, toReleaseChannel)
	err := c.execute(toReleaseUpdates, datapb.ChannelWatchState_ToRelease)
	if err != nil {
		log.Warn("fail to execute to release", zap.Array("to release updates", toReleaseUpdates))
	}

	return err
}

func (c *ChannelManager) Reassign(originNodeID UniqueID, channels []*channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var reallocatedCh []*channel
	for _, ch := range channels {
		if c.isMarkedDrop(ch.Name, ch.CollectionID) {
			c.finishDrop(originNodeID, ch)
		} else {
			reallocatedCh = append(reallocatedCh, ch)
		}
	}

	if len(reallocatedCh) == 0 {
		return nil
	}
	return c.executeReassign(originNodeID, reallocatedCh)
}

// CleanupAndReassign tries to clean up datanode's subscription, and then reassigns the channel to another DataNode.
func (c *ChannelManager) CleanupAndReassign(nodeID UniqueID, channels []*channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var reallocatedCh []*channel
	for _, ch := range channels {
		c.cleanChannelSub(nodeID, ch)

		if c.isMarkedDrop(ch.Name, ch.CollectionID) {
			c.finishDrop(nodeID, ch)
		} else {
			reallocatedCh = append(reallocatedCh, ch)
		}
	}

	if len(reallocatedCh) == 0 {
		return nil
	}

	return c.executeReassign(nodeID, reallocatedCh)
}

func (c *ChannelManager) executeReassign(nodeID UniqueID, channels []*channel) error {
	reallocates := &NodeChannelInfo{nodeID, channels}

	// Reassign policy won't choose the original node when a reassigning a channel.
	updates := c.reassignPolicy(c.store, []*NodeChannelInfo{reallocates})
	if len(updates) <= 0 {
		// Skip the remove if reassign to the original node.
		log.Warn("failed to reassign channel to other nodes, assigning to the original DataNode",
			zap.Int64("nodeID", nodeID),
			zap.Any("channels", channels),
		)
		updates.Add(nodeID, channels)
	}

	log.Info("channel manager reassigning channels",
		zap.Int64("old node ID", nodeID),
		zap.Array("updates", updates))
	return c.execute(updates, datapb.ChannelWatchState_ToWatch)
}

func (c *ChannelManager) cleanChannelSub(nodeID UniqueID, ch *channel) {
	if c.msgstreamFactory == nil {
		log.Warn("msgstream factory is not set, unable to clean up topics")
	} else {
		subName := fmt.Sprintf("%s-%d-%d", Params.CommonCfg.DataNodeSubName.GetValue(), nodeID, ch.CollectionID)
		pchannelName := funcutil.ToPhysicalChannel(ch.Name)
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}
}

// unwatchDroppedChannels removes drops channel that are marked to drop.
func (c *ChannelManager) unwatchDroppedChannels() {
	nodeChannels := c.store.GetChannels()
	for _, nodeChannel := range nodeChannels {
		for _, ch := range nodeChannel.Channels {
			if c.h.CheckShouldDropChannel(ch.Name, ch.CollectionID) {
				c.finishDrop(nodeChannel.NodeID, ch)
			}
		}
	}
}

func (c *ChannelManager) finishDrop(nodeID UniqueID, ch *channel) {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.String("channel", ch.Name),
	)
	if err := c.remove(nodeID, ch); err != nil {
		log.Warn("fail to remove watch info, will try later", zap.Error(err))
	}
	if err := c.h.FinishDropChannel(ch.Name); err != nil {
		log.Warn("fail to FinishDropChannel, will try later", zap.Error(err))
	}
	log.Info("success to remove channel assignment")
}

func (c *ChannelManager) checkChannelOperationLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("checking channel progress loop quit")
			return
		case checkResult := <-c.channelChecker.Watcher():
			op := checkResult.singleOp.ToChannelOp()
			switch checkResult.state {
			case datapb.ChannelWatchState_WatchSuccess:
				update := NewChannelOpSet(op)
				if err := c.store.Update(update); err != nil {
					log.Warn("fail to update success state into meta")
				}
			case datapb.ChannelWatchState_WatchFailure, datapb.ChannelWatchState_ReleaseSuccess:
				c.Reassign(op.NodeID, op.Channels)

			case datapb.ChannelWatchState_ReleaseFailure:
				c.CleanupAndReassign(op.NodeID, op.Channels)
				continue
			}
		}
	}
}

func (c *ChannelManager) checkBalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(Params.DataCoordCfg.ChannelBalanceInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("background checking channels loop quit")
			return
		case <-ticker.C:
			c.mu.Lock()
			if !c.isSilent() {
				log.Info("ChannelManager is not silent, skip channel balance this round")
			} else {
				toReleases := c.balancePolicy(c.store, time.Now())
				if len(toReleases) > 0 {
					log.Info("Channel Balancer got new reAllocations:", zap.Array("reAllocations", toReleases))
					err := c.execute(toReleases, datapb.ChannelWatchState_ToRelease)
					if err != nil {
						log.Warn("fail to execute balance", zap.Error(err))
					}
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *ChannelManager) execute(updates ChannelOpSet, state datapb.ChannelWatchState) error {
	log := log.With(zap.String("state", state.String()), zap.Array("updates", updates))
	for _, op := range updates {
		if op.Type == Add {
			if err := c.fillChannelWatchInfoWithState(op, state); err != nil {
				return err
			}
		}
	}
	log.Info("update meta of channel operations")
	if err := c.store.Update(updates); err != nil {
		log.Warn("fail to update channel operation updates into meta", zap.Error(err))
		return err
	}

	for _, op := range updates {
		if op.Type == Delete || op.NodeID == bufferID {
			continue
		}

		for _, ch := range op.Channels {
			c.channelChecker.Remove(ch.Name)
		}
		c.notifyAndCheck(op)
	}

	return nil
}

func (c *ChannelManager) notifyAndCheck(op *ChannelOp) {
	var skipCheck bool = false
	channels := lo.Map(op.Channels, func(x *channel, _ int) string {
		return x.Name
	})
	log := log.With(zap.Int64("nodeID", op.NodeID), zap.Any("channels", channels))

	log.Info("notify datanode channel operations")
	err := c.session.NotifyChannelOperation(c.ctx, op.NodeID, &datapb.ChannelOperations{Infos: op.ChannelWatchInfos})
	if err != nil {
		log.Warn("fail to notify datanode node channel operation, reassigning channel", zap.Error(err))
		skipCheck = true
	}

	for _, ch := range op.Channels {
		c.channelChecker.Submit(c.ctx, op.GetSingle(ch.Name), skipCheck)
	}
}

// fillChannelWatchInfoWithState updates the channel op by filling in channel watch info.
func (c *ChannelManager) fillChannelWatchInfoWithState(op *ChannelOp, state datapb.ChannelWatchState) error {
	startTs := time.Now().Unix()
	for _, ch := range op.Channels {
		vcInfo := c.h.GetDataVChanPositions(ch, allPartitionID)
		opID, err := c.allocator.allocID(context.Background())
		if err != nil {
			return err
		}

		info := &datapb.ChannelWatchInfo{
			Vchan:   vcInfo,
			StartTs: startTs,
			State:   state,
			Schema:  ch.Schema,
			OpID:    opID,
		}

		op.ChannelWatchInfos = append(op.ChannelWatchInfos, info)
	}
	return nil
}

func (c *ChannelManager) isSilent() bool {
	if !c.channelChecker.Empty() {
		return false
	}

	return time.Since(c.lastActiveTimestamp) >= Params.DataCoordCfg.ChannelBalanceSilentDuration.GetAsDuration(time.Second)
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
		// align to datanode subname, using vchannel name
		subName := fmt.Sprintf("%s-%d-%s", Params.CommonCfg.DataNodeSubName.GetValue(), nodeID, ch.Name)
		pchannelName := funcutil.ToPhysicalChannel(ch.Name)
		msgstream.UnsubscribeChannels(c.ctx, c.msgstreamFactory, subName, []string{pchannelName})
	}
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

// checkOldNodes processes the existing watch channels when starting up.
// ToWatch         re-notify and start check loop
// WatchSuccess    ignore
// WatchFail       set to ToRelease
// ToRelase        re-notify and start check loop
// ReleaseSuccess  Reassign
// ReleaseFail     CleanupAndReassign
func (c *ChannelManager) checkOldNodes(nodes []UniqueID) error {
	// Load all the watch infos before processing
	nodeWatchInfos := make(map[UniqueID][]*datapb.ChannelWatchInfo)
	for _, nodeID := range nodes {
		watchInfos, err := c.store.GetNodeWatchInfos(nodeID)
		if err != nil {
			return err
		}
		nodeWatchInfos[nodeID] = watchInfos
	}

	for nodeID, watchInfos := range nodeWatchInfos {
		for _, info := range watchInfos {
			channelName := info.GetVchan().GetChannelName()

			log := log.With(
				zap.Int64("nodeID", nodeID),
				zap.String("watch state", info.GetState().String()),
				zap.String("channel", channelName),
			)
			ch := c.getChannelByNodeAndName(nodeID, channelName)
			if ch == nil {
				log.Warn("fail to find matching nodeID with channel")
				continue
			}

			log.Info("processing watch info")

			switch info.GetState() {
			case datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_Uncomplete:
				c.notifyAndCheck(&ChannelOp{
					Type:              Add,
					NodeID:            nodeID,
					Channels:          []*channel{ch},
					ChannelWatchInfos: []*datapb.ChannelWatchInfo{info},
				})

			case datapb.ChannelWatchState_WatchFailure:
				if err := c.Release(nodeID, channelName); err != nil {
					log.Warn("fail to release", zap.Error(err))
					return err
				}

			case datapb.ChannelWatchState_ToRelease:
				c.notifyAndCheck(&ChannelOp{
					Type:              Add,
					NodeID:            nodeID,
					Channels:          []*channel{ch},
					ChannelWatchInfos: []*datapb.ChannelWatchInfo{info},
				})

			case datapb.ChannelWatchState_ReleaseSuccess:
				if err := c.Reassign(nodeID, []*channel{ch}); err != nil {
					log.Warn("fail to reassign", zap.Error(err))
					return err
				}

			case datapb.ChannelWatchState_ReleaseFailure:
				if err := c.CleanupAndReassign(nodeID, []*channel{ch}); err != nil {
					log.Warn("fail to cleanup and reassign", zap.Error(err))
					return err
				}
			}
		}
	}
	return nil
}

func (c *ChannelManager) getNodeIDByChannelName(channel string) (bool, UniqueID) {
	for _, nodeChannel := range c.GetChannels() {
		for _, ch := range nodeChannel.Channels {
			if ch.Name == channel {
				return true, nodeChannel.NodeID
			}
		}
	}
	return false, 0
}

// GetChannels gets channels info of registered nodes.
func (c *ChannelManager) GetChannels() []*NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetNodesChannels()
}

func (c *ChannelManager) isMarkedDrop(channelName string, collectionID UniqueID) bool {
	return c.h.CheckShouldDropChannel(channelName, collectionID)
}

// remove deletes the nodeID-channel pair from data store.
func (c *ChannelManager) remove(nodeID int64, ch *channel) error {
	var op ChannelOpSet
	op.Delete(nodeID, []*channel{ch})
	log.Info("remove channel assignment",
		zap.Int64("nodeID", nodeID),
		zap.String("channel", ch.Name),
		zap.Int64("collectionID", ch.CollectionID))
	err := c.store.Update(op)
	return err
}

// GetBufferChannels gets buffer channels.
func (c *ChannelManager) GetBufferChannels() *NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetBufferChannelInfo()
}

func getReleaseOp(nodeID UniqueID, ch *channel) ChannelOpSet {
	var op ChannelOpSet
	op.Add(nodeID, []*channel{ch})
	return op
}

// getOldOnlines returns a list of old online node ids in `old` and in `curr`.
func getOldOnlines(curr []int64, old []int64) []int64 {
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
func getNewOnLines(curr []int64, old []int64) []int64 {
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
func getOffLines(curr []int64, old []int64) []int64 {
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
