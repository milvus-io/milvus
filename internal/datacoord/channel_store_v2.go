package datacoord

import (
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type StateChannelStore struct {
	store        kv.TxnKV
	channelsInfo map[int64]*NodeChannelInfo // A map of (nodeID) -> (NodeChannelInfo).
}

var _ RWChannelStore = (*StateChannelStore)(nil)

var errChannelNotExistInNode = errors.New("channel doesn't exist in given node")

func NewChannelStoreV2(kv kv.TxnKV) RWChannelStore {
	return NewStateChannelStore(kv)
}

func NewStateChannelStore(kv kv.TxnKV) *StateChannelStore {
	c := StateChannelStore{
		store:        kv,
		channelsInfo: make(map[int64]*NodeChannelInfo),
	}
	c.channelsInfo[bufferID] = &NodeChannelInfo{
		NodeID:   bufferID,
		Channels: make(map[string]RWChannel),
	}
	return &c
}

func (c *StateChannelStore) Reload() error {
	record := timerecord.NewTimeRecorder("datacoord")
	keys, values, err := c.store.LoadWithPrefix(Params.CommonCfg.DataCoordWatchSubPath.GetValue())
	if err != nil {
		return err
	}
	for i := 0; i < len(keys); i++ {
		k := keys[i]
		v := values[i]
		nodeID, err := parseNodeKey(k)
		if err != nil {
			return err
		}

		info := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal([]byte(v), info); err != nil {
			return err
		}
		reviseVChannelInfo(info.GetVchan())

		c.AddNode(nodeID)

		channel := NewStateChannelByWatchInfo(nodeID, info)
		c.channelsInfo[nodeID].AddChannel(channel)
		log.Info("channel store reload channel",
			zap.Int64("nodeID", nodeID), zap.String("channel", channel.Name))
		metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)).Set(float64(len(c.channelsInfo[nodeID].Channels)))
	}
	log.Info("channel store reload done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (c *StateChannelStore) AddNode(nodeID int64) {
	if _, ok := c.channelsInfo[nodeID]; ok {
		return
	}
	c.channelsInfo[nodeID] = &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: make(map[string]RWChannel),
	}
}

func (c *StateChannelStore) UpdateState(isSuccessful bool, nodeID int64, channel RWChannel, opID int64) {
	channelName := channel.GetName()
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		if stateChannel, ok := cInfo.Channels[channelName]; ok {
			if isSuccessful {
				stateChannel.(*StateChannel).TransitionOnSuccess(opID)
			} else {
				stateChannel.(*StateChannel).TransitionOnFailure(opID)
			}
		}
	}
}

func (c *StateChannelStore) SetLegacyChannelByNode(nodeIDs ...int64) {
	lo.ForEach(nodeIDs, func(nodeID int64, _ int) {
		if cInfo, ok := c.channelsInfo[nodeID]; ok {
			for _, ch := range cInfo.Channels {
				ch.(*StateChannel).setState(Legacy)
			}
		}
	})
}

func (c *StateChannelStore) Update(opSet *ChannelOpSet) error {
	// Split opset into multiple txn. Operations on the same channel must be executed in one txn.
	perChOps := opSet.SplitByChannel()

	// Execute a txn for every 64 operations.
	count := 0
	operations := make([]*ChannelOp, 0, maxOperationsPerTxn)
	for _, opset := range perChOps {
		if !c.sanityCheckPerChannelOpSet(opset) {
			log.Error("unsupported ChannelOpSet", zap.Any("OpSet", opset))
			continue
		}
		if opset.Len() > maxOperationsPerTxn {
			log.Error("Operations for one channel exceeds maxOperationsPerTxn",
				zap.Any("opset size", opset.Len()),
				zap.Int("limit", maxOperationsPerTxn))
		}
		if count+opset.Len() > maxOperationsPerTxn {
			if err := c.updateMeta(NewChannelOpSet(operations...)); err != nil {
				return err
			}
			count = 0
			operations = make([]*ChannelOp, 0, maxOperationsPerTxn)
		}
		count += opset.Len()
		operations = append(operations, opset.Collect()...)
	}
	if count == 0 {
		return nil
	}

	return c.updateMeta(NewChannelOpSet(operations...))
}

// remove from the assignments
func (c *StateChannelStore) removeAssignment(nodeID int64, channelName string) {
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		delete(cInfo.Channels, channelName)
	}
}

func (c *StateChannelStore) addAssignment(nodeID int64, channel RWChannel) {
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		cInfo.Channels[channel.GetName()] = channel
	} else {
		c.channelsInfo[nodeID] = &NodeChannelInfo{
			NodeID: nodeID,
			Channels: map[string]RWChannel{
				channel.GetName(): channel,
			},
		}
	}
}

// updateMeta applies the WATCH/RELEASE/DELETE operations to the current channel store.
// DELETE + WATCH ---> from bufferID    to nodeID
// DELETE + WATCH ---> from lagecyID    to nodeID
// DELETE + WATCH ---> from deletedNode to nodeID/bufferID
// DELETE + WATCH ---> from releasedNode to nodeID/bufferID
// RELEASE        ---> release from nodeID
// WATCH          ---> watch to a new channel
// DELETE         ---> remove the channel
func (c *StateChannelStore) sanityCheckPerChannelOpSet(opSet *ChannelOpSet) bool {
	if opSet.Len() == 2 {
		ops := opSet.Collect()
		return (ops[0].Type == Delete && ops[1].Type == Watch) || (ops[1].Type == Delete && ops[0].Type == Watch)
	} else if opSet.Len() == 1 {
		t := opSet.Collect()[0].Type
		return t == Delete || t == Watch || t == Release
	}
	return false
}

// DELETE + WATCH
func (c *StateChannelStore) updateMetaMemoryForPairOp(chName string, opSet *ChannelOpSet) error {
	if !c.sanityCheckPerChannelOpSet(opSet) {
		return errUnknownOpType
	}
	ops := opSet.Collect()
	op1 := ops[1]
	op2 := ops[0]
	if ops[0].Type == Delete {
		op1 = ops[0]
		op2 = ops[1]
	}
	cInfo, ok := c.channelsInfo[op1.NodeID]
	if !ok {
		return errChannelNotExistInNode
	}
	var ch *StateChannel
	if channel, ok := cInfo.Channels[chName]; ok {
		ch = channel.(*StateChannel)
		c.addAssignment(op2.NodeID, ch)
		c.removeAssignment(op1.NodeID, chName)
	} else {
		if cInfo, ok = c.channelsInfo[op2.NodeID]; ok {
			if channel2, ok := cInfo.Channels[chName]; ok {
				ch = channel2.(*StateChannel)
			}
		}
	}
	// update channel
	if ch != nil {
		ch.Assign(op2.NodeID)
		if op2.NodeID == bufferID {
			ch.setState(Standby)
		} else {
			ch.setState(ToWatch)
		}
	}
	return nil
}

func (c *StateChannelStore) getChannel(nodeID int64, channelName string) *StateChannel {
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		if storedChannel, ok := cInfo.Channels[channelName]; ok {
			return storedChannel.(*StateChannel)
		}
		log.Debug("Channel doesn't exist in Node", zap.String("channel", channelName), zap.Int64("nodeID", nodeID))
	} else {
		log.Error("Node doesn't exist", zap.Int64("NodeID", nodeID))
	}
	return nil
}

func (c *StateChannelStore) updateMetaMemoryForSingleOp(op *ChannelOp) error {
	lo.ForEach(op.Channels, func(ch RWChannel, _ int) {
		switch op.Type {
		case Release: // release an already exsits storedChannel-node pair
			if channel := c.getChannel(op.NodeID, ch.GetName()); channel != nil {
				channel.setState(ToRelease)
			}
		case Watch:
			storedChannel := c.getChannel(op.NodeID, ch.GetName())
			if storedChannel == nil { // New Channel
				//  set the correct assigment and state for NEW stateChannel
				newChannel := NewStateChannel(ch)
				newChannel.Assign(op.NodeID)

				if op.NodeID != bufferID {
					newChannel.setState(ToWatch)
				}

				// add channel to memory
				c.addAssignment(op.NodeID, newChannel)
			} else { // assign to the original nodes
				storedChannel.setState(ToWatch)
			}
		case Delete: // Remove Channel
			// if not Delete from bufferID, remove from channel
			if op.NodeID != bufferID {
				c.removeAssignment(op.NodeID, ch.GetName())
			}
		default:
			log.Error("unknown opType in updateMetaMemoryForSingleOp", zap.Any("type", op.Type))
		}
	})
	return nil
}

func (c *StateChannelStore) updateMeta(opSet *ChannelOpSet) error {
	// Update ChannelStore's kv store.
	if err := c.txn(opSet); err != nil {
		return err
	}

	// Update memory
	chOpSet := opSet.SplitByChannel()
	for chName, ops := range chOpSet {
		// DELETE + WATCH
		if ops.Len() == 2 {
			c.updateMetaMemoryForPairOp(chName, ops)
			// RELEASE, DELETE, WATCH
		} else if ops.Len() == 1 {
			c.updateMetaMemoryForSingleOp(ops.Collect()[0])
		} else {
			log.Error("unsupported ChannelOpSet", zap.Any("OpSet", ops))
		}
	}
	return nil
}

// txn updates the channelStore's kv store with the given channel ops.
func (c *StateChannelStore) txn(opSet *ChannelOpSet) error {
	var (
		saves    = make(map[string]string)
		removals []string
	)
	for _, op := range opSet.Collect() {
		opSaves, opRemovals, err := op.BuildKV()
		if err != nil {
			return err
		}

		saves = lo.Assign(opSaves, saves)
		removals = append(removals, opRemovals...)
	}
	return c.store.MultiSaveAndRemove(saves, removals)
}

func (c *StateChannelStore) RemoveNode(nodeID int64) {
	delete(c.channelsInfo, nodeID)
}

func (c *StateChannelStore) HasChannel(channel string) bool {
	for _, info := range c.channelsInfo {
		if _, ok := info.Channels[channel]; ok {
			return true
		}
	}
	return false
}

type (
	ChannelSelector func(ch *StateChannel) bool
	NodeSelector    func(ID int64) bool
)

func WithAllNodes() NodeSelector {
	return func(ID int64) bool {
		return true
	}
}

func WithoutBufferNode() NodeSelector {
	return func(ID int64) bool {
		return ID != int64(bufferID)
	}
}

func WithNodeIDs(IDs ...int64) NodeSelector {
	return func(ID int64) bool {
		return lo.Contains(IDs, ID)
	}
}

func WithoutNodeIDs(IDs ...int64) NodeSelector {
	return func(ID int64) bool {
		return !lo.Contains(IDs, ID)
	}
}

func WithChannelName(channel string) ChannelSelector {
	return func(ch *StateChannel) bool {
		return ch.GetName() == channel
	}
}

func WithCollectionIDV2(collectionID int64) ChannelSelector {
	return func(ch *StateChannel) bool {
		return ch.GetCollectionID() == collectionID
	}
}

func WithChannelStates(states ...ChannelState) ChannelSelector {
	return func(ch *StateChannel) bool {
		return lo.Contains(states, ch.currentState)
	}
}

func (c *StateChannelStore) GetNodeChannelsBy(nodeSelector NodeSelector, channelSelectors ...ChannelSelector) []*NodeChannelInfo {
	var nodeChannels []*NodeChannelInfo
	for nodeID, cInfo := range c.channelsInfo {
		if nodeSelector(nodeID) {
			selected := make(map[string]RWChannel)
			for chName, channel := range cInfo.Channels {
				var sel bool = true
				for _, selector := range channelSelectors {
					if !selector(channel.(*StateChannel)) {
						sel = false
						break
					}
				}
				if sel {
					selected[chName] = channel
				}
			}
			nodeChannels = append(nodeChannels, &NodeChannelInfo{
				NodeID:   nodeID,
				Channels: selected,
			})
		}
	}
	return nodeChannels
}

func (c *StateChannelStore) GetNodesChannels() []*NodeChannelInfo {
	ret := make([]*NodeChannelInfo, 0, len(c.channelsInfo))
	for id, info := range c.channelsInfo {
		if id != bufferID {
			ret = append(ret, info)
		}
	}
	return ret
}

func (c *StateChannelStore) GetNodeChannelsByCollectionID(collectionID UniqueID) map[UniqueID][]string {
	nodeChs := make(map[UniqueID][]string)
	for id, info := range c.channelsInfo {
		if id == bufferID {
			continue
		}
		var channelNames []string
		for name, ch := range info.Channels {
			if ch.GetCollectionID() == collectionID {
				channelNames = append(channelNames, name)
			}
		}
		nodeChs[id] = channelNames
	}
	return nodeChs
}

func (c *StateChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	return c.GetNode(bufferID)
}

func (c *StateChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	if info, ok := c.channelsInfo[nodeID]; ok {
		return info
	}
	return nil
}

func (c *StateChannelStore) GetNodeChannelCount(nodeID int64) int {
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		return len(cInfo.Channels)
	}
	return 0
}

func (c *StateChannelStore) GetNodes() []int64 {
	return lo.Filter(lo.Keys(c.channelsInfo), func(ID int64, _ int) bool {
		return ID != bufferID
	})
}

// remove deletes kv pairs from the kv store where keys have given nodeID as prefix.
func (c *StateChannelStore) remove(nodeID int64) error {
	k := buildKeyPrefix(nodeID)
	return c.store.RemoveWithPrefix(k)
}
