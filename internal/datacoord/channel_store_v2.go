package datacoord

import (
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type StateChannelStore struct {
	store kv.TxnKV

	channels    map[string]*StateChannel
	assignments map[int64][]string // node to channel name
}

var _ RWChannelStore = (*StateChannelStore)(nil)

func NewChannelStoreV2(kv kv.TxnKV) RWChannelStore {
	return NewStateChannelStore(kv)
}

func NewStateChannelStore(kv kv.TxnKV) *StateChannelStore {
	c := StateChannelStore{
		store:    kv,
		channels: make(map[string]*StateChannel),
	}

	assignments := map[int64][]string{
		bufferID: {},
	}

	c.assignments = assignments
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
		c.assignments[nodeID] = append(c.assignments[nodeID], channel.GetName())
		c.channels[channel.GetName()] = channel
		log.Info("channel store reload channel",
			zap.Int64("nodeID", nodeID), zap.String("channel", channel.Name))
		metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)).Set(float64(len(c.assignments[nodeID])))
	}
	log.Info("channel store reload done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (c *StateChannelStore) AddNode(nodeID int64) {
	if _, ok := c.assignments[nodeID]; ok {
		return
	}
	c.assignments[nodeID] = []string{}
}

func (c *StateChannelStore) UpdateState(isSuccessful bool, channels ...RWChannel) {
	lo.ForEach(channels, func(ch RWChannel, _ int) {
		if stateChannel, ok := c.channels[ch.GetName()]; ok {
			if isSuccessful {
				stateChannel.TransitionOnSuccess()
			} else {
				stateChannel.TransitionOnFailure()
			}
		}
	})
}

func (c *StateChannelStore) SetLegacyChannelByNode(nodeIDs ...int64) {
	lo.ForEach(nodeIDs, func(nodeID int64, _ int) {
		channels := c.assignments[nodeID]
		for _, channel := range channels {
			if ch, ok := c.channels[channel]; ok {
				ch.setState(Legacy)
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
func (c *StateChannelStore) removeAssignment(nodeID int64, channel string) {
	filtered := lo.Filter(c.assignments[nodeID], func(ch string, _ int) bool {
		return ch != channel
	})
	c.assignments[nodeID] = filtered
}

func (c *StateChannelStore) addAssignment(nodeID int64, channel string) {
	if !lo.Contains(c.assignments[nodeID], channel) {
		c.assignments[nodeID] = append(c.assignments[nodeID], channel)
	}
}

// updateMeta applies the WATCH/RELEASE/DELETE operations to the current channel store.
// DELETE + WATCH ---> from bufferID    to nodeID
// DELETE + WATCH ---> from lagecyID    to nodeID
// DELETE + WATCH ---> from deletedNode to nodeID/bufferID
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
	ch := c.channels[chName]
	c.removeAssignment(op1.NodeID, chName)
	c.addAssignment(op2.NodeID, chName)
	ch.Assign(op2.NodeID)
	if op2.NodeID == bufferID {
		ch.setState(Standby)
	} else {
		ch.setState(ToWatch)
	}
	return nil
}

func (c *StateChannelStore) updateMetaMemoryForSingleOp(op *ChannelOp) error {
	lo.ForEach(op.Channels, func(ch RWChannel, _ int) {
		switch op.Type {
		case Release: // release an already exsits storedChannel-node pair
			storedChannel := c.channels[ch.GetName()]
			storedChannel.setState(ToRelease)
		case Watch:
			storedChannel, ok := c.channels[ch.GetName()]
			// New Channel
			if !ok {
				//  set the correct assigment and state for NEW stateChannel
				newChannel := NewStateChannel(ch)
				newChannel.Assign(op.NodeID)

				if op.NodeID != bufferID {
					newChannel.setState(ToWatch)
				}

				// add channel to memory
				c.channels[ch.GetName()] = newChannel
				c.addAssignment(op.NodeID, ch.GetName())
			} else { // assign to the original nodes
				storedChannel.setState(ToWatch)
			}
		case Delete: // Remove Channel
			// if not Delete from bufferID, remove from channel
			if op.NodeID != bufferID {
				delete(c.channels, ch.GetName())
			}
			c.removeAssignment(op.NodeID, ch.GetName())
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
	delete(c.assignments, nodeID)
	for _, ch := range c.channels {
		ch.RemoveNode(nodeID)
	}
}

func (c *StateChannelStore) HasChannel(channel string) bool {
	_, has := c.channels[channel]
	return has
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
	nodeChannels := make(map[int64]*NodeChannelInfo)

	for nodeID, channels := range c.assignments {
		if nodeSelector(nodeID) {
			selected := make(map[string]RWChannel)
			for _, chName := range channels {
				channel := c.channels[chName]
				var sel bool = true
				for _, selector := range channelSelectors {
					if !selector(channel) {
						sel = false
						break
					}
				}
				if sel {
					selected[chName] = channel
				}
			}
			nodeChannels[nodeID] = &NodeChannelInfo{
				NodeID:   nodeID,
				Channels: selected,
			}
		}
	}
	return lo.Values(nodeChannels)
}

func (c *StateChannelStore) GetNodesChannels() []*NodeChannelInfo {
	nodeChannels := make(map[int64]*NodeChannelInfo)

	for nodeID, channels := range c.assignments {
		if nodeID == bufferID {
			continue
		}

		dup := make(map[string]RWChannel)
		for _, chName := range channels {
			dup[chName] = c.channels[chName].Clone()
		}
		nodeChannels[nodeID] = &NodeChannelInfo{
			NodeID:   nodeID,
			Channels: dup,
		}
	}
	return lo.Values(nodeChannels)
}

func (c *StateChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	return c.GetNode(bufferID)
}

func (c *StateChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	channels := make(map[string]RWChannel)
	for _, channelName := range c.assignments[nodeID] {
		channels[channelName] = c.channels[channelName]
	}
	return &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: channels,
	}
}

func (c *StateChannelStore) GetNodeChannelCount(nodeID int64) int {
	for id, channels := range c.assignments {
		if nodeID == id {
			return len(channels)
		}
	}

	return 0
}

func (c *StateChannelStore) GetNodes() []int64 {
	return lo.Filter(lo.Keys(c.assignments), func(ID int64, _ int) bool {
		return ID != bufferID
	})
}

// remove deletes kv pairs from the kv store where keys have given nodeID as prefix.
func (c *StateChannelStore) remove(nodeID int64) error {
	k := buildKeyPrefix(nodeID)
	return c.store.RemoveWithPrefix(k)
}
