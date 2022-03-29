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
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
	"stathat.com/c/consistent"
)

const (
	bgCheckInterval  = 3 * time.Second
	maxWatchDuration = 20 * time.Second
)

// ChannelManager manages the allocation and the balance between channels and data nodes.
type ChannelManager struct {
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

// NewChannelManager creates and returns a new ChannelManager instance.
func NewChannelManager(
	kv kv.TxnKV,
	h Handler,
	options ...ChannelManagerOpt,
) (*ChannelManager, error) {
	c := &ChannelManager{
		h:       h,
		factory: NewChannelPolicyFactoryV1(kv),
		store:   NewChannelStore(kv),
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
func (c *ChannelManager) Startup(nodes []int64) error {
	channels := c.store.GetNodesChannels()
	// Retrieve the current old nodes.
	oNodes := make([]int64, 0, len(channels))
	for _, c := range channels {
		oNodes = append(oNodes, c.NodeID)
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

	log.Info("cluster start up",
		zap.Any("nodes", nodes),
		zap.Any("oNodes", oNodes),
		zap.Int64s("new onlines", newOnLines),
		zap.Int64s("offLines", offLines))
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
	log.Info("register node",
		zap.Int64("registered node", nodeID),
		zap.Array("updates", updates))

	for _, op := range updates {
		if op.Type == Add {
			c.fillChannelWatchInfo(op)
		}
	}
	return c.store.Update(updates)
}

// DeleteNode deletes the node from the cluster.
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

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelWatchInfo(v)
		}
	}
	if err := c.store.Update(updates); err != nil {
		return err
	}
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
		subName := buildSubName(ch.CollectionID, nodeID)
		err := c.unsubscribe(subName, ch.Name)
		if err != nil {
			log.Warn("failed to unsubscribe topic", zap.String("subscription name", subName), zap.String("channel name", ch.Name))
		}
	}
}

// buildSubName generates a subscription name by concatenating DataNodeSubName, node ID and collection ID.
func buildSubName(collectionID int64, nodeID int64) string {
	return fmt.Sprintf("%s-%d-%d", Params.MsgChannelCfg.DataNodeSubName, nodeID, collectionID)
}

func (c *ChannelManager) unsubscribe(subName string, channel string) error {
	msgStream, err := c.msgstreamFactory.NewMsgStream(context.TODO())
	if err != nil {
		return err
	}

	msgStream.AsConsumer([]string{channel}, subName)
	msgStream.Close()
	return nil
}

// Watch tries to add the channel to cluster. Watch is a no op if the channel already exists.
func (c *ChannelManager) Watch(ch *channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.assignPolicy(c.store, []*channel{ch})
	if len(updates) == 0 {
		return nil
	}
	log.Info("watch channel",
		zap.Any("channel", ch),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelWatchInfo(v)
		}
	}
	err := c.store.Update(updates)
	if err != nil {
		log.Error("ChannelManager RWChannelStore update failed", zap.Int64("collectionID", ch.CollectionID),
			zap.String("channelName", ch.Name), zap.Error(err))
		return err
	}
	log.Info("ChannelManager RWChannelStore update success", zap.Int64("collectionID", ch.CollectionID),
		zap.String("channelName", ch.Name))
	return nil
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
