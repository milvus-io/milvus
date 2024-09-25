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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	cancel context.CancelFunc
	mu     lock.RWMutex
	wg     sync.WaitGroup

	h          Handler
	store      RWChannelStore
	subCluster SubCluster // sessionManager
	allocator  allocator.Allocator

	factory       ChannelPolicyFactory
	balancePolicy BalanceChannelPolicy
	assignPolicy  AssignPolicy

	balanceCheckLoop ChannelBGChecker

	legacyNodes typeutil.UniqueSet

	lastActiveTimestamp time.Time
}

// ChannelBGChecker are goroutining running background
type ChannelBGChecker func(ctx context.Context)

// ChannelmanagerOpt is to set optional parameters in channel manager.
type ChannelmanagerOpt func(c *ChannelManagerImpl)

func withFactoryV2(f ChannelPolicyFactory) ChannelmanagerOpt {
	return func(c *ChannelManagerImpl) { c.factory = f }
}

func withCheckerV2() ChannelmanagerOpt {
	return func(c *ChannelManagerImpl) { c.balanceCheckLoop = c.CheckLoop }
}

func NewChannelManager(
	kv kv.TxnKV,
	h Handler,
	subCluster SubCluster, // sessionManager
	alloc allocator.Allocator,
	options ...ChannelmanagerOpt,
) (*ChannelManagerImpl, error) {
	m := &ChannelManagerImpl{
		h:          h,
		factory:    NewChannelPolicyFactoryV1(),
		store:      NewChannelStoreV2(kv),
		subCluster: subCluster,
		allocator:  alloc,
	}

	if err := m.store.Reload(); err != nil {
		return nil, err
	}

	for _, opt := range options {
		opt(m)
	}

	m.balancePolicy = m.factory.NewBalancePolicy()
	m.assignPolicy = m.factory.NewAssignPolicy()
	m.lastActiveTimestamp = time.Now()
	return m, nil
}

func (m *ChannelManagerImpl) Startup(ctx context.Context, legacyNodes, allNodes []int64) error {
	ctx, m.cancel = context.WithCancel(ctx)

	m.legacyNodes = typeutil.NewUniqueSet(legacyNodes...)

	m.mu.Lock()
	m.store.SetLegacyChannelByNode(legacyNodes...)
	oNodes := m.store.GetNodes()
	m.mu.Unlock()

	offLines, newOnLines := lo.Difference(oNodes, allNodes)
	// Delete offlines from the cluster
	for _, nodeID := range offLines {
		if err := m.DeleteNode(nodeID); err != nil {
			return err
		}
	}
	// Add new online nodes to the cluster.
	for _, nodeID := range newOnLines {
		if err := m.AddNode(nodeID); err != nil {
			return err
		}
	}

	m.mu.Lock()
	nodeChannels := m.store.GetNodeChannelsBy(
		WithAllNodes(),
		func(ch *StateChannel) bool {
			return m.h.CheckShouldDropChannel(ch.GetName())
		})
	m.mu.Unlock()

	for _, info := range nodeChannels {
		m.finishRemoveChannel(info.NodeID, lo.Values(info.Channels)...)
	}

	if m.balanceCheckLoop != nil && !streamingutil.IsStreamingServiceEnabled() {
		log.Info("starting channel balance loop")
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.balanceCheckLoop(ctx)
		}()
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
		m.wg.Wait()
	}
}

func (m *ChannelManagerImpl) AddNode(nodeID UniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Info("register node", zap.Int64("registered node", nodeID))

	m.store.AddNode(nodeID)
	updates := m.assignPolicy(m.store.GetNodesChannels(), m.store.GetBufferChannelInfo(), m.legacyNodes.Collect())

	if updates == nil {
		log.Info("register node with no reassignment", zap.Int64("registered node", nodeID))
		return nil
	}

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
	log := log.Ctx(ctx).With(zap.String("channel", ch.GetName()))
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Info("Add channel")
	updates := NewChannelOpSet(NewChannelOp(bufferID, Watch, ch))
	err := m.execute(updates)
	if err != nil {
		log.Warn("fail to update new channel updates into meta",
			zap.Array("updates", updates), zap.Error(err))
	}

	// channel already written into meta, try to assign it to the cluster
	// not error is returned if failed, the assignment will retry later
	updates = m.assignPolicy(m.store.GetNodesChannels(), m.store.GetBufferChannelInfo(), m.legacyNodes.Collect())
	if updates == nil {
		return nil
	}

	if err := m.execute(updates); err != nil {
		log.Warn("fail to assign channel, will retry later", zap.Array("updates", updates), zap.Error(err))
		return nil
	}

	log.Info("Assign channel", zap.Array("updates", updates))
	return nil
}

func (m *ChannelManagerImpl) DeleteNode(nodeID UniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.legacyNodes.Remove(nodeID)
	info := m.store.GetNode(nodeID)
	if info == nil || len(info.Channels) == 0 {
		if nodeID != bufferID {
			m.store.RemoveNode(nodeID)
		}
		return nil
	}

	updates := NewChannelOpSet(
		NewChannelOp(info.NodeID, Delete, lo.Values(info.Channels)...),
		NewChannelOp(bufferID, Watch, lo.Values(info.Channels)...),
	)
	log.Info("deregister node", zap.Int64("nodeID", nodeID), zap.Array("updates", updates))

	err := m.execute(updates)
	if err != nil {
		log.Warn("fail to update channel operation updates into meta", zap.Error(err))
		return err
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

	updates := m.assignPolicy(m.store.GetNodesChannels(), original, m.legacyNodes.Collect())
	if updates != nil {
		return m.execute(updates)
	}

	if original.NodeID != bufferID {
		log.RatedWarn(5.0, "Failed to reassign channel to other nodes, assign to the original nodes",
			zap.Any("original node", original.NodeID),
			zap.Strings("channels", lo.Keys(original.Channels)),
		)
		updates := NewChannelOpSet(NewChannelOp(original.NodeID, Watch, lo.Values(original.Channels)...))
		return m.execute(updates)
	}

	return nil
}

func (m *ChannelManagerImpl) Balance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	watchedCluster := m.store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(Watched))
	updates := m.balancePolicy(watchedCluster)
	if updates == nil {
		return
	}

	log.Info("Channel balancer got new reAllocations:", zap.Array("assignment", updates))
	if err := m.execute(updates); err != nil {
		log.Warn("Channel balancer fail to execute", zap.Array("assignment", updates), zap.Error(err))
	}
}

func (m *ChannelManagerImpl) Match(nodeID UniqueID, channel string) bool {
	if streamingutil.IsStreamingServiceEnabled() {
		// Skip the channel matching check since the
		// channel manager no longer manages channels in streaming mode.
		return true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	info := m.store.GetNode(nodeID)
	if info == nil {
		return false
	}

	_, ok := info.Channels[channel]
	return ok
}

func (m *ChannelManagerImpl) GetChannel(nodeID int64, channelName string) (RWChannel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if nodeChannelInfo := m.store.GetNode(nodeID); nodeChannelInfo != nil {
		if ch, ok := nodeChannelInfo.Channels[channelName]; ok {
			return ch, true
		}
	}
	return nil, false
}

func (m *ChannelManagerImpl) GetNodeChannelsByCollectionID(collectionID int64) map[int64][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.GetNodeChannelsByCollectionID(collectionID)
}

func (m *ChannelManagerImpl) GetChannelsByCollectionID(collectionID int64) []RWChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channels := []RWChannel{}

	nodeChannels := m.store.GetNodeChannelsBy(
		WithAllNodes(),
		WithCollectionIDV2(collectionID))
	lo.ForEach(nodeChannels, func(info *NodeChannelInfo, _ int) {
		channels = append(channels, lo.Values(info.Channels)...)
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
		_, ok := info.Channels[channel]
		if ok {
			return info.NodeID, nil
		}
	}

	// channel in buffer
	bufferInfo := m.store.GetBufferChannelInfo()
	_, ok := bufferInfo.Channels[channel]
	if ok {
		return bufferID, errChannelInBuffer
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
			m.AdvanceChannelState(ctx)
		}
	}
}

func (m *ChannelManagerImpl) AdvanceChannelState(ctx context.Context) {
	m.mu.RLock()
	standbys := m.store.GetNodeChannelsBy(WithAllNodes(), WithChannelStates(Standby))
	toNotifies := m.store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(ToWatch, ToRelease))
	toChecks := m.store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(Watching, Releasing))
	m.mu.RUnlock()

	// Processing standby channels
	updatedStandbys := m.advanceStandbys(ctx, standbys)
	updatedToCheckes := m.advanceToChecks(ctx, toChecks)
	updatedToNotifies := m.advanceToNotifies(ctx, toNotifies)

	if updatedStandbys || updatedToCheckes || updatedToNotifies {
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

		if err := m.h.FinishDropChannel(ch.GetName(), ch.GetCollectionID()); err != nil {
			log.Warn("Failed to finish drop channel", zap.Any("channel", ch), zap.Error(err))
			continue
		}
	}
}

func (m *ChannelManagerImpl) advanceStandbys(_ context.Context, standbys []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range standbys {
		validChannels := make(map[string]RWChannel)
		for chName, ch := range nodeAssign.Channels {
			// drop marked-drop channels
			if m.h.CheckShouldDropChannel(chName) {
				m.finishRemoveChannel(nodeAssign.NodeID, ch)
				continue
			}
			validChannels[chName] = ch
		}
		nodeAssign.Channels = validChannels

		if len(nodeAssign.Channels) == 0 {
			continue
		}

		chNames := lo.Keys(validChannels)
		if err := m.reassign(nodeAssign); err != nil {
			log.Warn("Reassign channels fail",
				zap.Int64("nodeID", nodeAssign.NodeID),
				zap.Strings("channels", chNames),
			)
			continue
		}

		log.Info("Reassign standby channels to node",
			zap.Int64("nodeID", nodeAssign.NodeID),
			zap.Strings("channels", chNames),
		)
		advanced = true
	}

	return advanced
}

func (m *ChannelManagerImpl) advanceToNotifies(ctx context.Context, toNotifies []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range toNotifies {
		channelCount := len(nodeAssign.Channels)
		if channelCount == 0 {
			continue
		}
		nodeID := nodeAssign.NodeID

		var (
			succeededChannels = make([]RWChannel, 0, channelCount)
			failedChannels    = make([]RWChannel, 0, channelCount)
			futures           = make([]*conc.Future[any], 0, channelCount)
		)

		chNames := lo.Keys(nodeAssign.Channels)
		log.Info("Notify channel operations to datanode",
			zap.Int64("assignment", nodeAssign.NodeID),
			zap.Int("total operation count", len(nodeAssign.Channels)),
			zap.Strings("channel names", chNames),
		)
		for _, ch := range nodeAssign.Channels {
			innerCh := ch
			tmpWatchInfo := typeutil.Clone(innerCh.GetWatchInfo())
			tmpWatchInfo.Vchan = m.h.GetDataVChanPositions(innerCh, allPartitionID)

			future := getOrCreateIOPool().Submit(func() (any, error) {
				err := m.Notify(ctx, nodeID, tmpWatchInfo)
				return innerCh, err
			})
			futures = append(futures, future)
		}

		for _, f := range futures {
			ch, err := f.Await()
			if err != nil {
				failedChannels = append(failedChannels, ch.(RWChannel))
			} else {
				succeededChannels = append(succeededChannels, ch.(RWChannel))
				advanced = true
			}
		}

		log.Info("Finish to notify channel operations to datanode",
			zap.Int64("assignment", nodeAssign.NodeID),
			zap.Int("operation count", channelCount),
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

type poolResult struct {
	successful bool
	ch         RWChannel
}

func (m *ChannelManagerImpl) advanceToChecks(ctx context.Context, toChecks []*NodeChannelInfo) bool {
	var advanced bool = false
	for _, nodeAssign := range toChecks {
		if len(nodeAssign.Channels) == 0 {
			continue
		}

		nodeID := nodeAssign.NodeID
		futures := make([]*conc.Future[any], 0, len(nodeAssign.Channels))

		chNames := lo.Keys(nodeAssign.Channels)
		log.Info("Check ToWatch/ToRelease channel operations progress",
			zap.Int("channel count", len(nodeAssign.Channels)),
			zap.Strings("channel names", chNames),
		)

		for _, ch := range nodeAssign.Channels {
			innerCh := ch

			future := getOrCreateIOPool().Submit(func() (any, error) {
				successful, got := m.Check(ctx, nodeID, innerCh.GetWatchInfo())
				if got {
					return poolResult{
						successful: successful,
						ch:         innerCh,
					}, nil
				}
				return nil, errors.New("Got results with no progress")
			})
			futures = append(futures, future)
		}

		for _, f := range futures {
			got, err := f.Await()
			if err == nil {
				m.mu.Lock()
				result := got.(poolResult)
				m.store.UpdateState(result.successful, result.ch)
				m.mu.Unlock()

				advanced = true
			}
		}

		log.Info("Finish to Check ToWatch/ToRelease channel operations progress",
			zap.Int("channel count", len(nodeAssign.Channels)),
			zap.Strings("channel names", chNames),
		)
	}
	return advanced
}

func (m *ChannelManagerImpl) Notify(ctx context.Context, nodeID int64, info *datapb.ChannelWatchInfo) error {
	log := log.With(
		zap.String("channel", info.GetVchan().GetChannelName()),
		zap.Int64("assignment", nodeID),
		zap.String("operation", info.GetState().String()),
	)
	log.Info("Notify channel operation")
	err := m.subCluster.NotifyChannelOperation(ctx, nodeID, &datapb.ChannelOperationsRequest{Infos: []*datapb.ChannelWatchInfo{info}})
	if err != nil {
		log.Warn("Fail to notify channel operations", zap.Error(err))
		return err
	}
	log.Debug("Success to notify channel operations")
	return nil
}

func (m *ChannelManagerImpl) Check(ctx context.Context, nodeID int64, info *datapb.ChannelWatchInfo) (successful bool, got bool) {
	log := log.With(
		zap.Int64("opID", info.GetOpID()),
		zap.Int64("nodeID", nodeID),
		zap.String("check operation", info.GetState().String()),
		zap.String("channel", info.GetVchan().GetChannelName()),
	)
	resp, err := m.subCluster.CheckChannelOperationProgress(ctx, nodeID, info)
	if err != nil {
		log.Warn("Fail to check channel operation progress", zap.Error(err))
		if errors.Is(err, merr.ErrNodeNotFound) {
			return false, true
		}
		return false, false
	}
	log.Info("Got channel operation progress",
		zap.String("got state", resp.GetState().String()),
		zap.Int32("progress", resp.GetProgress()))
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
		opID, err := m.allocator.AllocID(context.Background())
		if err != nil {
			return err
		}

		schema := ch.GetSchema()
		if schema == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			collInfo, err := m.h.GetCollection(ctx, ch.GetCollectionID())
			if err != nil {
				return err
			}
			schema = collInfo.Schema
		}

		info := &datapb.ChannelWatchInfo{
			Vchan:   reduceVChanSize(vcInfo),
			StartTs: startTs,
			State:   inferStateByOpType(op.Type),
			Schema:  schema,
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

// Clear segmentID in vChannelInfo to reduce meta size.
// About 200k segments will exceed default meta size limit,
// clear it would make meta size way smaller and support infinite segments count
//
// NOTE: all the meta and in-mem watchInfo contains partial VChanInfo that dones't include segmentIDs
// Need to recalulate and fill-in segmentIDs before notify to DataNode
func reduceVChanSize(vChan *datapb.VchannelInfo) *datapb.VchannelInfo {
	vChan.DroppedSegmentIds = nil
	vChan.FlushedSegmentIds = nil
	vChan.UnflushedSegmentIds = nil
	return vChan
}
