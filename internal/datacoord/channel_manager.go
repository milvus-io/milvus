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
	posProvider      positionProvider
	store            RWChannelStore
	factory          ChannelPolicyFactory
	registerPolicy   RegisterPolicy
	deregisterPolicy DeregisterPolicy
	assignPolicy     ChannelAssignPolicy
	reassignPolicy   ChannelReassignPolicy
	bgChecker        ChannelBGChecker
}

type channel struct {
	name         string
	collectionID UniqueID
}

// ChannelManagerOpt is to set optional parameters in channel manager
type ChannelManagerOpt func(c *ChannelManager)

func withFactory(f ChannelPolicyFactory) ChannelManagerOpt {
	return func(c *ChannelManager) { c.factory = f }
}
func defaultFactory(hash *consistent.Consistent) ChannelPolicyFactory {
	return NewConsistentHashChannelPolicyFactory(hash)
}

// NewChannelManager return a new ChannelManager
func NewChannelManager(kv kv.TxnKV, posProvider positionProvider, options ...ChannelManagerOpt) (*ChannelManager, error) {
	hashring := consistent.New()
	c := &ChannelManager{
		posProvider: posProvider,
		factory:     defaultFactory(hashring),
		store:       NewChannelStore(kv),
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

// Startup adjust the channel store according to current cluster states
func (c *ChannelManager) Startup(nodes []int64) error {
	channels := c.store.GetNodesChannels()
	olds := make([]int64, 0, len(channels))
	for _, c := range channels {
		olds = append(olds, c.NodeID)
	}

	newOnlines := c.getNewOnlines(nodes, olds)
	for _, n := range newOnlines {
		if err := c.AddNode(n); err != nil {
			return err
		}
	}

	offlines := c.getOfflines(nodes, olds)
	for _, n := range offlines {
		if err := c.DeleteNode(n); err != nil {
			return err
		}
	}
	log.Debug("cluster start up",
		zap.Any("nodes", nodes),
		zap.Any("olds", olds),
		zap.Int64s("new onlines", newOnlines),
		zap.Int64s("offlines", offlines))
	return nil
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
			reallocs, err := c.bgChecker(channels, time.Now())
			if err != nil {
				log.Warn("channel manager bg check failed", zap.Error(err))

				c.mu.Unlock()
				continue
			}

			updates := c.reassignPolicy(c.store, reallocs)
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

func (c *ChannelManager) getNewOnlines(curr []int64, old []int64) []int64 {
	mold := make(map[int64]struct{})
	ret := make([]int64, 0)
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

func (c *ChannelManager) getOfflines(curr []int64, old []int64) []int64 {
	mcurr := make(map[int64]struct{})
	ret := make([]int64, 0)
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

	channels := c.store.GetChannels()
	c.store.Add(nodeID)

	updates := c.registerPolicy(c.store, nodeID)
	log.Debug("register node",
		zap.Int64("registered node", nodeID),
		zap.Any("channels before registry", channels),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelPosition(v)
		}
	}
	return c.store.Update(updates)
}

// DeleteNode delete the node whose id is nodeID
func (c *ChannelManager) DeleteNode(nodeID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.store.GetChannels()

	updates := c.deregisterPolicy(c.store, nodeID)
	log.Debug("deregister node",
		zap.Int64("unregistered node", nodeID),
		zap.Any("channels before deregistery", channels),
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

// Watch try to add the chanel to cluster. If the channel already exists, do nothing
func (c *ChannelManager) Watch(ch *channel) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.store.GetChannels()

	updates := c.assignPolicy(c.store, []*channel{ch})
	if len(updates) == 0 {
		return nil
	}
	log.Debug("watch channel",
		zap.Any("channel", ch),
		zap.Any("channels before watching", channels),
		zap.Array("updates", updates))

	for _, v := range updates {
		if v.Type == Add {
			c.fillChannelPosition(v)
		}
	}
	return c.store.Update(updates)
}

func (c *ChannelManager) fillChannelPosition(update *ChannelOp) {
	for _, ch := range update.Channels {
		vchan := c.posProvider.GetVChanPositions(ch.name, ch.collectionID, true)
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
		if ch.name == channel {
			return true
		}
	}
	return false
}
