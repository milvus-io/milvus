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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type ChannelManager interface {
	Startup(ctx context.Context, legacyNodes, allNodes []int64) error
	Close()

	AddNode(nodeID UniqueID) error
	DeleteNode(nodeID UniqueID) error
	Watch(ctx context.Context, ch RWChannel) error
	Release(nodeID UniqueID, channelName string) error

	Match(nodeID UniqueID, channel string) bool
	FindWatcher(channel string) (UniqueID, error)
	GetChannel(nodeID int64, channel string) (RWChannel, bool)
	GetNodeIDByChannelName(channel string) (int64, bool)
	GetNodeChannelsByCollectionID(collectionID int64) map[int64][]string
	GetChannelsByCollectionID(collectionID int64) []RWChannel
	GetChannelNamesByCollectionID(collectionID int64) []string
}

// An interface sessionManager implments
type SubCluster interface {
	NotifyChannelOperation(ctx context.Context, nodeID int64, req *datapb.ChannelOperationsRequest) error
	CheckChannelOperationProgress(ctx context.Context, nodeID int64, info *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error)
}

type ChannelManagerImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	wg     sync.WaitGroup

	h          Handler
	store      RWChannelStore
	subCluster SubCluster // sessionManager
	allocator  allocator

	factory          ChannelPolicyFactory
	registerPolicy   RegisterPolicy
	deregisterPolicy DeregisterPolicy
	assignPolicy     ChannelAssignPolicy
	reassignPolicy   ChannelReassignPolicy
	balancePolicy    BalanceChannelPolicy
	msgstreamFactory msgstream.Factory

	balanceCheckLoop ChannelBGChecker

	lastActiveTimestamp time.Time
}

// ChannelBGChecker are goroutining running background
type ChannelBGChecker func(ctx context.Context)

// ChannelmanagerOpt is to set optional parameters in channel manager.
type ChannelmanagerOpt func(c *ChannelManagerImpl)

func withFactory(f ChannelPolicyFactory) ChannelmanagerOpt {
	return func(c *ChannelManagerImpl) { c.factory = f }
}

func withMsgstreamFactory(f msgstream.Factory) ChannelmanagerOpt {
	return func(c *ChannelManagerImpl) { c.msgstreamFactory = f }
}

func withChecker() ChannelmanagerOpt {
	return func(c *ChannelManagerImpl) { c.balanceCheckLoop = c.CheckLoop }
}

func NewChannelManager(
	kv kv.TxnKV,
	h Handler,
	subCluster SubCluster, // sessionManager
	alloc allocator,
	options ...ChannelmanagerOpt,
) (*ChannelManagerImpl, error) {
	m := &ChannelManagerImpl{
		h:          h,
		ctx:        context.TODO(), // TODO
		factory:    NewChannelPolicyFactoryV1(),
		store:      NewChannelStore(kv),
		subCluster: subCluster,
		allocator:  alloc,
	}

	if err := m.store.Reload(); err != nil {
		return nil, err
	}

	for _, opt := range options {
		opt(m)
	}

	m.registerPolicy = m.factory.NewRegisterPolicy()
	m.deregisterPolicy = m.factory.NewDeregisterPolicy()
	m.assignPolicy = m.factory.NewAssignPolicy()
	m.reassignPolicy = m.factory.NewReassignPolicy()
	m.balancePolicy = m.factory.NewBalancePolicy()
	m.lastActiveTimestamp = time.Now()
	return m, nil
}

func (m *ChannelManagerImpl) Startup(ctx context.Context, legacyNodes, allNodes []int64) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	m.mu.Lock()
	m.store.SetLegacyChannelByNode(legacyNodes...)
	oNodes := m.store.GetNodes()
	m.mu.Unlock()

	// Add new online nodes to the cluster.
	offLines, newOnLines := lo.Difference(oNodes, allNodes)
	lo.ForEach(newOnLines, func(nodeID int64, _ int) {
		m.AddNode(nodeID)
	})

	// Delete offlines from the cluster
	lo.ForEach(offLines, func(nodeID int64, _ int) {
		m.DeleteNode(nodeID)
	})

	m.mu.Lock()
	nodeChannels := m.store.GetNodeChannelsBy(
		WithAllNodes(),
		func(ch *StateChannel) bool {
			return m.h.CheckShouldDropChannel(ch.GetName())
		})
	m.mu.Unlock()

	for _, info := range nodeChannels {
		m.finishRemoveChannel(info.NodeID, info.Channels...)
	}

	if m.balanceCheckLoop != nil {
		log.Info("starting channel balance loop")
		go m.balanceCheckLoop(m.ctx)
	}

	log.Info("cluster start up",
		zap.Int64s("allNodes", allNodes),
		zap.Int64s("legacyNodes", legacyNodes),
		zap.Int64s("oldNodes", oNodes),
		zap.Int64s("newOnlines", newOnLines),
		zap.Int64s("offLines", offLines))
	return nil
}

func (m *ChannelManagerImpl) Close() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *ChannelManagerImpl) AddNode(nodeID UniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store.AddNode(nodeID)

	bufferedUpdates, balanceUpdates := m.registerPolicy(m.store, nodeID)
	updates := bufferedUpdates
	// try bufferedUpdates first
	if updates == nil {
		if !Params.DataCoordCfg.AutoBalance.GetAsBool() {
			log.Info("auto balance disabled, register node with no assignment", zap.Int64("registered node", nodeID))
			return nil
		}
		updates = balanceUpdates
	}

	if updates == nil {
		log.Info("register node with no reassignment", zap.Int64("registered node", nodeID))
		return nil
	}

	log.Info("register node", zap.Int64("registered node", nodeID), zap.Array("updates", updates))

	err := m.execute(updates)
	if err != nil {
		log.Warn("fail to update channel operation updates into meta", zap.Error(err))
	}
	return err
}

// Release writes ToRelease channel watch states for a channel
func (m *ChannelManagerImpl) Release(nodeID UniqueID, channelName string) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channelName),
	)

	// channel in bufferID are released already
	if nodeID == bufferID {
		return nil
	}

	log.Info("Releasing channel from watched node")
	ch, found := m.GetChannel(nodeID, channelName)
	if !found {
		return fmt.Errorf("fail to find matching nodeID: %d with channelName: %s", nodeID, channelName)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updates := NewChannelOpSet(NewChannelOp(nodeID, Release, ch))
	return m.execute(updates)
}

func (m *ChannelManagerImpl) Watch(ctx context.Context, ch RWChannel) error {
	log := log.Ctx(ctx).With(zap.String("channel", ch.String()))
	m.mu.Lock()
	defer m.mu.Unlock()

	updates := m.assignPolicy(m.store, []RWChannel{ch})
	if updates == nil {
		return nil
	}
	log.Info("Add a new channel and update watch info with ToWatch state", zap.Array("updates", updates))

	err := m.execute(updates)
	if err != nil {
		log.Warn("fail to update new channel updates into meta",
			zap.Array("updates", updates), zap.Error(err))
	}
	return err
}

func (m *ChannelManagerImpl) DeleteNode(nodeID UniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	updates := m.deregisterPolicy(m.store, nodeID)
	if updates != nil {
		log.Info("deregister node", zap.Int64("nodeID", nodeID), zap.Array("updates", updates))

		err := m.execute(updates)
		if err != nil {
			log.Warn("fail to update channel operation updates into meta", zap.Error(err))
			return err
		}
	}

	if nodeID != bufferID {
		m.store.RemoveNode(nodeID)
	}
	return nil
}

// reassign reassigns a channel to another DataNode.
func (m *ChannelManagerImpl) reassign(original *NodeChannelInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Reassign policy won't choose the original node when a reassigning a channel.
	if updates := m.reassignPolicy(m.store, []*NodeChannelInfo{original}); updates != nil {
		log.Info("channel manager reassigning channels",
			zap.Int64("old nodeID", original.NodeID),
			zap.Any("updates", updates))
		return m.execute(updates)
	}

	log.RatedWarn(5.0, "Failed to reassign channel to other nodes, assign to the original nodes",
		zap.Any("original node", original.NodeID),
		zap.Strings("channels", lo.Map(original.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})),
	)

	if original.NodeID != bufferID {
		updates := NewChannelOpSet(NewChannelOp(original.NodeID, Watch, original.Channels...))
		return m.execute(updates)
	}

	return nil
}

func (m *ChannelManagerImpl) Balance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	toReleases := m.balancePolicy(m.store)
	if toReleases == nil {
		return
	}

	log.Info("Channel balancer got new reAllocations:", zap.Array("assignment", toReleases))
	if err := m.execute(toReleases); err != nil {
		log.Warn("Channel balancer fail to execute", zap.Array("assignment", toReleases), zap.Error(err))
	}
}

func (m *ChannelManagerImpl) Match(nodeID UniqueID, channel string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info := m.store.GetNode(nodeID)
	if info == nil {
		return false
	}

	return lo.ContainsBy(info.Channels, func(ch RWChannel) bool {
		return ch.GetName() == channel
	})
}

func (m *ChannelManagerImpl) GetChannel(nodeID int64, channel string) (RWChannel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info := m.store.GetNode(nodeID)
	if info == nil {
		return nil, false
	}

	return lo.Find(info.Channels, func(ch RWChannel) bool {
		return ch.GetName() == channel
	})
}

func (m *ChannelManagerImpl) GetNodeIDByChannelName(channel string) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeChannels := m.store.GetNodeChannelsBy(
		WithoutBufferNode(),
		WithChannelName(channel))

	if len(nodeChannels) > 0 {
		return nodeChannels[0].NodeID, true
	}

	return 0, false
}

func (m *ChannelManagerImpl) GetNodeChannelsByCollectionID(collectionID int64) map[int64][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeChs := make(map[UniqueID][]string)
	nodeChannels := m.store.GetNodeChannelsBy(
		WithoutBufferNode(),
		WithCollectionID(collectionID))
	lo.ForEach(nodeChannels, func(info *NodeChannelInfo, _ int) {
		nodeChs[info.NodeID] = lo.Map(info.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})
	})
	return nodeChs
}

func (m *ChannelManagerImpl) GetChannelsByCollectionID(collectionID int64) []RWChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channels := []RWChannel{}

	nodeChannels := m.store.GetNodeChannelsBy(
		WithAllNodes(),
		WithCollectionID(collectionID))
	lo.ForEach(nodeChannels, func(info *NodeChannelInfo, _ int) {
		channels = append(channels, info.Channels...)
	})
	return channels
}

func (m *ChannelManagerImpl) GetChannelNamesByCollectionID(collectionID int64) []string {
	channels := m.GetChannelsByCollectionID(collectionID)
	return lo.Map(channels, func(ch RWChannel, _ int) string {
		return ch.GetName()
	})
}

func (m *ChannelManagerImpl) FindWatcher(channel string) (UniqueID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := m.store.GetNodesChannels()
	for _, info := range infos {
		for _, channelInfo := range info.Channels {
			if channelInfo.GetName() == channel {
				return info.NodeID, nil
			}
		}
	}

	// channel in buffer
	bufferInfo := m.store.GetBufferChannelInfo()
	for _, channelInfo := range bufferInfo.Channels {
		if channelInfo.GetName() == channel {
			return bufferID, errChannelInBuffer
		}
	}
	return 0, errChannelNotWatched
}

// unsafe innter func
func (m *ChannelManagerImpl) removeChannel(nodeID int64, ch RWChannel) error {
	op := NewChannelOpSet(NewChannelOp(nodeID, Delete, ch))
	log.Info("remove channel assignment",
		zap.String("channel", ch.GetName()),
		zap.Int64("assignment", nodeID),
		zap.Int64("collectionID", ch.GetCollectionID()))
	return m.store.Update(op)
}

func (m *ChannelManagerImpl) CheckLoop(ctx context.Context) {
	balanceTicker := time.NewTicker(Params.DataCoordCfg.ChannelBalanceInterval.GetAsDuration(time.Second))
	defer balanceTicker.Stop()
	checkTicker := time.NewTicker(Params.DataCoordCfg.ChannelCheckInterval.GetAsDuration(time.Second))
	defer checkTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("background checking channels loop quit")
			return
		case <-balanceTicker.C:
			// balance
			if time.Since(m.lastActiveTimestamp) >= Params.DataCoordCfg.ChannelBalanceSilentDuration.GetAsDuration(time.Second) {
				m.Balance()
			}
		case <-checkTicker.C:
			m.AdvanceChannelState()
		}
	}
}

func (m *ChannelManagerImpl) AdvanceChannelState() {
	m.mu.RLock()
	standbys := m.store.GetNodeChannelsBy(WithAllNodes(), WithChannelStates(Standby))
	toNotifies := m.store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(ToWatch, ToRelease))
	toChecks := m.store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(Watching, Releasing))
	m.mu.RUnlock()

	var advanced bool = false
	// Processing standby channels
	advanced = advanced || m.advanceStandbys(standbys)
	advanced = advanced || m.advanceToNotifies(toNotifies)
	advanced = advanced || m.advanceToChecks(toChecks)

	if advanced {
		m.lastActiveTimestamp = time.Now()
	}
}

func (m *ChannelManagerImpl) finishRemoveChannel(nodeID int64, channels ...RWChannel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range channels {
		if err := m.removeChannel(nodeID, ch); err != nil {
			log.Warn("Failed to remove channel", zap.Any("channel", ch), zap.Error(err))
			continue
		}

		if err := m.h.FinishDropChannel(ch.GetName()); err != nil {
			log.Warn("Failed to finish drop channel", zap.Any("channel", ch), zap.Error(err))
			continue
		}
	}
}

func (m *ChannelManagerImpl) advanceStandbys(standbys []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range standbys {
		validChannels := []RWChannel{}
		lo.ForEach(nodeAssign.Channels, func(ch RWChannel, _ int) {
			// drop marked-drop channels
			if m.h.CheckShouldDropChannel(ch.GetName()) {
				m.finishRemoveChannel(nodeAssign.NodeID, ch)
				return
			}

			validChannels = append(validChannels, ch)
		})
		nodeAssign.Channels = validChannels

		if len(nodeAssign.Channels) == 0 {
			continue
		}

		chNames := lo.Map(nodeAssign.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})
		if err := m.reassign(nodeAssign); err != nil {
			log.Warn("Reassign channels fail",
				zap.Int64("nodeID", nodeAssign.NodeID),
				zap.Strings("channels", chNames),
			)
		}

		log.Info("Reassign standby channels to node",
			zap.Int64("nodeID", nodeAssign.NodeID),
			zap.Strings("channels", chNames),
		)
		advanced = true
	}

	return advanced
}

func (m *ChannelManagerImpl) advanceToNotifies(toNotifies []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range toNotifies {
		if len(nodeAssign.Channels) == 0 {
			continue
		}

		var (
			succeededChannels []RWChannel
			failedChannels    []RWChannel
		)

		chNames := lo.Map(nodeAssign.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})
		log.Info("Notify channel operations to datanode",
			zap.Int64("assignment", nodeAssign.NodeID),
			zap.Int("total operation count", len(nodeAssign.Channels)),
			zap.Strings("channel names", chNames),
		)
		for _, ch := range nodeAssign.Channels {
			info := ch.GetWatchInfo()
			err := m.Notify(nodeAssign.NodeID, info)
			if err != nil {
				failedChannels = append(failedChannels, ch)
				log.Warn("Fail to notify channel operations",
					zap.String("channel", ch.GetName()),
					zap.Int64("assignment", nodeAssign.NodeID),
					zap.String("operation", info.GetState().String()),
				)
			} else {
				succeededChannels = append(succeededChannels, ch)
				advanced = true
				log.Debug("Success to notify channel operations",
					zap.String("channel", ch.GetName()),
					zap.Int64("assignment", nodeAssign.NodeID),
					zap.String("operation", info.GetState().String()),
				)
			}
		}

		log.Info("Finish to notify channel operations to datanode",
			zap.Int64("assignment", nodeAssign.NodeID),
			zap.Int("operation count", len(chNames)),
			zap.Int("success count", len(succeededChannels)),
			zap.Int("failure count", len(failedChannels)),
		)
		m.mu.Lock()
		m.store.UpdateState(false, failedChannels...)
		m.store.UpdateState(true, succeededChannels...)
		m.mu.Unlock()
	}

	return advanced
}

func (m *ChannelManagerImpl) advanceToChecks(toChecks []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range toChecks {
		if len(nodeAssign.Channels) == 0 {
			continue
		}

		chNames := lo.Map(nodeAssign.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})
		log.Info("Check ToWatch/ToRelease channel operations progress",
			zap.Int("channel count", len(nodeAssign.Channels)),
			zap.Strings("channel names", chNames),
		)

		for _, ch := range nodeAssign.Channels {
			info := ch.GetWatchInfo()
			if successful, got := m.Check(nodeAssign.NodeID, info); got {
				log.Info("Success to check channel operations",
					zap.Int64("opID", info.GetOpID()),
					zap.String("check operation", info.GetState().String()),
					zap.String("channel", info.GetVchan().GetChannelName()),
					zap.Bool("operation finishing state", successful))
				m.mu.Lock()
				m.store.UpdateState(successful, ch)
				m.mu.Unlock()
			}
		}
		advanced = true
	}
	return advanced
}

func (m *ChannelManagerImpl) Notify(nodeID int64, info *datapb.ChannelWatchInfo) error {
	return m.subCluster.NotifyChannelOperation(m.ctx, nodeID, &datapb.ChannelOperationsRequest{Infos: []*datapb.ChannelWatchInfo{info}})
}

func (m *ChannelManagerImpl) Check(nodeID int64, info *datapb.ChannelWatchInfo) (successful bool, got bool) {
	log.With(
		zap.Int64("opID", info.GetOpID()),
		zap.Int64("nodeID", nodeID),
		zap.String("check operation", info.GetState().String()),
		zap.String("channel", info.GetVchan().GetChannelName()),
	)
	resp, err := m.subCluster.CheckChannelOperationProgress(m.ctx, nodeID, info)
	if err != nil {
		log.Warn("Fail to check channel operation progress")
		return false, false
	}

	log.Info("Got channel operation progress",
		zap.String("got state", resp.GetState().String()),
		zap.Int32("got progress", resp.GetProgress()),
	)
	switch info.GetState() {
	case datapb.ChannelWatchState_ToWatch:
		if resp.GetState() == datapb.ChannelWatchState_ToWatch {
			return false, false
		}
		if resp.GetState() == datapb.ChannelWatchState_WatchSuccess {
			return true, true
		}
		if resp.GetState() == datapb.ChannelWatchState_WatchFailure {
			return false, true
		}
	case datapb.ChannelWatchState_ToRelease:
		if resp.GetState() == datapb.ChannelWatchState_ToRelease {
			return false, false
		}
		if resp.GetState() == datapb.ChannelWatchState_ReleaseSuccess {
			return true, true
		}
		if resp.GetState() == datapb.ChannelWatchState_ReleaseFailure {
			return false, true
		}
	}
	return false, false
}

func (m *ChannelManagerImpl) execute(updates *ChannelOpSet) error {
	for _, op := range updates.ops {
		if op.Type != Delete {
			if err := m.fillChannelWatchInfo(op); err != nil {
				log.Warn("fail to fill channel watch info", zap.Error(err))
				return err
			}
		}
	}

	return m.store.Update(updates)
}

// fillChannelWatchInfoWithState updates the channel op by filling in channel watch info.
func (m *ChannelManagerImpl) fillChannelWatchInfo(op *ChannelOp) error {
	startTs := time.Now().Unix()
	for _, ch := range op.Channels {
		vcInfo := m.h.GetDataVChanPositions(ch, allPartitionID)
		opID, err := m.allocator.allocID(context.Background())
		if err != nil {
			return err
		}

		info := &datapb.ChannelWatchInfo{
			Vchan:   vcInfo,
			StartTs: startTs,
			State:   inferStateByOpType(op.Type),
			Schema:  ch.GetSchema(),
			OpID:    opID,
		}
		ch.UpdateWatchInfo(info)
	}
	return nil
}

func inferStateByOpType(opType ChannelOpType) datapb.ChannelWatchState {
	switch opType {
	case Watch:
		return datapb.ChannelWatchState_ToWatch
	case Release:
		return datapb.ChannelWatchState_ToRelease
	default:
		return datapb.ChannelWatchState_ToWatch
	}
}
