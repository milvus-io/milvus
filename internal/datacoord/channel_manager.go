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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
	"stathat.com/c/consistent"
)

const (
	bgCheckInterval  = 3 * time.Second
	maxWatchDuration = 20 * time.Second
)

// ChannelManager manages the allocation and the balance of channels between datanodes
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
}

type channel struct {
	Name         string
	CollectionID UniqueID
}

// ChannelManagerOpt is to set optional parameters in channel manager
type ChannelManagerOpt func(c *ChannelManager)

func withFactory(f ChannelPolicyFactory) ChannelManagerOpt {
	return func(c *ChannelManager) { c.factory = f }
}
func defaultFactory(hash *consistent.Consistent) ChannelPolicyFactory {
	return NewConsistentHashChannelPolicyFactory(hash)
}

// NewChannelManager returns a new ChannelManager
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

// Startup adjusts the channel store according to current cluster states
func (c *ChannelManager) Startup(nodes []int64) error {
	channels := c.store.GetNodesChannels()
	olds := make([]int64, 0, len(channels))
	for _, c := range channels {
		olds = append(olds, c.NodeID)
	}

	newOnLines := c.getNewOnLines(nodes, olds)
	for _, n := range newOnLines {
		if err := c.AddNode(n); err != nil {
			return err
		}
	}

	offlines := c.getOffLines(nodes, olds)
	for _, n := range offlines {
		if err := c.DeleteNode(n); err != nil {
			return err
		}
	}

	c.unwatchDroppedChannels()

	log.Debug("cluster start up",
		zap.Any("nodes", nodes),
		zap.Any("olds", olds),
		zap.Int64s("new onlines", newOnLines),
		zap.Int64s("offLines", offlines))
	return nil
}

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
			log.Debug("channel manager bg check reassign", zap.Array("updates", updates))
			for _, update := range updates {
				if update.Type == Add {
					c.fillChannelPosition(update)
				}
			}

			if err := c.store.Update(updates); err != nil {
				log.Warn("channel store update error", zap.Error(err))
			}

			c.mu.Unlock()
		}
	}
}

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

// AddNode adds a new node in cluster
func (c *ChannelManager) AddNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Add(nodeID)

	updates := c.registerPolicy(c.store, nodeID)
	log.Debug("register node",
		zap.Int64("registered node", nodeID),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelPosition(v)
		}
	}
	return c.store.Update(updates)
}

// DeleteNode deletes the node from the cluster
func (c *ChannelManager) DeleteNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.deregisterPolicy(c.store, nodeID)
	log.Debug("deregister node",
		zap.Int64("unregistered node", nodeID),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelPosition(v)
		}
	}
	if err := c.store.Update(updates); err != nil {
		return err
	}
	_, err := c.store.Delete(nodeID)
	return err
}

// Watch try to add the channel to cluster. If the channel already exists, do nothing
func (c *ChannelManager) Watch(ch *channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updates := c.assignPolicy(c.store, []*channel{ch})
	if len(updates) == 0 {
		return nil
	}
	log.Debug("watch channel",
		zap.Any("channel", ch),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelPosition(v)
		}
	}
	err := c.store.Update(updates)
	if err != nil {
		log.Error("ChannelManager RWChannelStore update failed", zap.Int64("collectionID", ch.CollectionID),
			zap.String("channelName", ch.Name), zap.Error(err))
		return err
	}
	log.Debug("ChannelManager RWChannelStore update success", zap.Int64("collectionID", ch.CollectionID),
		zap.String("channelName", ch.Name))
	return nil
}

func (c *ChannelManager) fillChannelPosition(update *ChannelOp) {
	for _, ch := range update.Channels {
		vchan := c.h.GetVChanPositions(ch.Name, ch.CollectionID, allPartitionID)
		info := &datapb.ChannelWatchInfo{
			Vchan:   vchan,
			StartTs: time.Now().Unix(),
			State:   datapb.ChannelWatchState_Uncomplete,
		}
		update.ChannelWatchInfos = append(update.ChannelWatchInfos, info)
	}
}

// GetChannels gets channels info of registered nodes
func (c *ChannelManager) GetChannels() []*NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetNodesChannels()
}

// GetBuffer gets buffer channels
func (c *ChannelManager) GetBuffer() *NodeChannelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.GetBufferChannelInfo()
}

// Match checks whether nodeID and channel match
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

// FindWatcher finds the datanode watching the provided channel
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

// RemoveChannel removes the channel from channel manager
func (c *ChannelManager) RemoveChannel(channelName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, ch := c.findChannel(channelName)
	if ch == nil {
		return nil
	}

	return c.remove(nodeID, ch)
}

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
