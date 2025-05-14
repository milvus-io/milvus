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
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ROChannelStore is a read only channel store for channels and nodes.
//
//go:generate mockery --name=ROChannelStore --structname=ROChannelStore --output=./ --filename=mock_ro_channel_store.go --with-expecter
type ROChannelStore interface {
	// GetNode returns the channel info of a specific node.
	// Returns nil if the node doesn't belong to the cluster
	GetNode(nodeID int64) *NodeChannelInfo
	// GetNodesChannels returns the channels that are assigned to nodes.
	// without bufferID node
	GetNodesChannels() []*NodeChannelInfo
	// GetBufferChannelInfo gets the unassigned channels.
	GetBufferChannelInfo() *NodeChannelInfo
	// GetNodes gets all node ids in store.
	GetNodes() []int64
	// GetNodeChannels for given collection
	GetNodeChannelsByCollectionID(collectionID UniqueID) map[UniqueID][]string

	GetNodeChannelsBy(nodeSelector NodeSelector, channelSelectors ...ChannelSelector) []*NodeChannelInfo
}

// RWChannelStore is the read write channel store for channels and nodes.
//
//go:generate mockery --name=RWChannelStore --structname=RWChannelStore --output=./ --filename=mock_channel_store.go --with-expecter
type RWChannelStore interface {
	ROChannelStore
	// Reload restores the buffer channels and node-channels mapping form kv.
	Reload() error
	// Add creates a new node-channels mapping, with no channels assigned to the node.
	AddNode(nodeID int64)
	// Delete removes nodeID and returns its channels.
	RemoveNode(nodeID int64)
	// Update applies the operations in ChannelOpSet.
	Update(op *ChannelOpSet) error

	// UpdateState is used by StateChannelStore only
	UpdateState(action Action, nodeID int64, channel RWChannel, opID int64)
	// SegLegacyChannelByNode is used by StateChannelStore only
	SetLegacyChannelByNode(nodeIDs ...int64)

	HasChannel(channel string) bool
}

// ChannelOpTypeNames implements zap log marshaller for ChannelOpSet.
var ChannelOpTypeNames = []string{"Add", "Delete", "Watch", "Release"}

const (
	bufferID            = math.MinInt64
	delimiter           = "/"
	maxOperationsPerTxn = 64
	maxBytesPerTxn      = 1024 * 1024
)

var errUnknownOpType = errors.New("unknown operation type")

type ChannelOpType int8

const (
	Add ChannelOpType = iota
	Delete
	Watch
	Release
)

// ChannelOp is an individual ADD or DELETE operation to the channel store.
type ChannelOp struct {
	Type     ChannelOpType
	NodeID   int64
	Channels []RWChannel
}

func NewChannelOp(ID int64, opType ChannelOpType, channels ...RWChannel) *ChannelOp {
	return &ChannelOp{
		Type:     opType,
		NodeID:   ID,
		Channels: channels,
	}
}

func (op *ChannelOp) Append(channels ...RWChannel) {
	op.Channels = append(op.Channels, channels...)
}

func (op *ChannelOp) GetChannelNames() []string {
	return lo.Map(op.Channels, func(c RWChannel, _ int) string {
		return c.GetName()
	})
}

func (op *ChannelOp) BuildKV() (map[string]string, []string, error) {
	var (
		saves    = make(map[string]string)
		removals = []string{}
	)
	for _, ch := range op.Channels {
		k := buildNodeChannelKey(op.NodeID, ch.GetName())
		switch op.Type {
		case Add, Watch, Release:
			tmpWatchInfo := proto.Clone(ch.GetWatchInfo()).(*datapb.ChannelWatchInfo)
			tmpWatchInfo.Vchan = reduceVChanSize(tmpWatchInfo.GetVchan())
			info, err := proto.Marshal(tmpWatchInfo)
			if err != nil {
				return saves, removals, err
			}
			saves[k] = string(info)
		case Delete:
			removals = append(removals, k)
		default:
			return saves, removals, errUnknownOpType
		}
	}
	return saves, removals, nil
}

// TODO: NIT: ObjectMarshaler -> ObjectMarshaller
// MarshalLogObject implements the interface ObjectMarshaler.
func (op *ChannelOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", ChannelOpTypeNames[op.Type])
	enc.AddInt64("nodeID", op.NodeID)
	cstr := "["
	if len(op.Channels) > 0 {
		for _, s := range op.Channels {
			cstr += s.GetName()
			cstr += ", "
		}
		cstr = cstr[:len(cstr)-2]
	}
	cstr += "]"
	enc.AddString("channels", cstr)
	return nil
}

// ChannelOpSet is a set of channel operations.
type ChannelOpSet struct {
	ops []*ChannelOp
}

func NewChannelOpSet(ops ...*ChannelOp) *ChannelOpSet {
	if ops == nil {
		ops = []*ChannelOp{}
	}
	return &ChannelOpSet{ops}
}

func (c *ChannelOpSet) Insert(ops ...*ChannelOp) {
	c.ops = append(c.ops, ops...)
}

func (c *ChannelOpSet) Collect() []*ChannelOp {
	if c == nil {
		return []*ChannelOp{}
	}
	return c.ops
}

func (c *ChannelOpSet) Len() int {
	if c == nil {
		return 0
	}

	return len(c.ops)
}

// Add a new Add channel op, for ToWatch and ToRelease
func (c *ChannelOpSet) Add(ID int64, channels ...RWChannel) {
	c.Append(ID, Add, channels...)
}

func (c *ChannelOpSet) Delete(ID int64, channels ...RWChannel) {
	c.Append(ID, Delete, channels...)
}

func (c *ChannelOpSet) Append(ID int64, opType ChannelOpType, channels ...RWChannel) {
	c.ops = append(c.ops, NewChannelOp(ID, opType, channels...))
}

func (c *ChannelOpSet) GetChannelNumber() int {
	if c == nil {
		return 0
	}

	uniqChannels := typeutil.NewSet[string]()
	for _, op := range c.ops {
		uniqChannels.Insert(lo.Map(op.Channels, func(ch RWChannel, _ int) string {
			return ch.GetName()
		})...)
	}

	return uniqChannels.Len()
}

func (c *ChannelOpSet) SplitByChannel() map[string]*ChannelOpSet {
	perChOps := make(map[string]*ChannelOpSet)

	for _, op := range c.Collect() {
		for _, ch := range op.Channels {
			if _, ok := perChOps[ch.GetName()]; !ok {
				perChOps[ch.GetName()] = NewChannelOpSet()
			}

			perChOps[ch.GetName()].Append(op.NodeID, op.Type, ch)
		}
	}
	return perChOps
}

// TODO: NIT: ArrayMarshaler -> ArrayMarshaller
// MarshalLogArray implements the interface of ArrayMarshaler of zap.
func (c *ChannelOpSet) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range c.Collect() {
		enc.AppendObject(o)
	}
	return nil
}

// NodeChannelInfo stores the nodeID and its channels.
type NodeChannelInfo struct {
	NodeID   int64
	Channels map[string]RWChannel
	// ChannelsSet typeutil.Set[string] // map for fast channel check
}

// AddChannel appends channel info node channel list.
func (info *NodeChannelInfo) AddChannel(ch RWChannel) {
	info.Channels[ch.GetName()] = ch
}

// RemoveChannel removes channel from Channels.
func (info *NodeChannelInfo) RemoveChannel(channelName string) {
	delete(info.Channels, channelName)
}

func NewNodeChannelInfo(nodeID int64, channels ...RWChannel) *NodeChannelInfo {
	info := &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: make(map[string]RWChannel),
	}

	for _, channel := range channels {
		info.Channels[channel.GetName()] = channel
	}

	return info
}

func (info *NodeChannelInfo) GetChannels() []RWChannel {
	if info == nil {
		return nil
	}
	return lo.Values(info.Channels)
}

// buildNodeChannelKey generates a key for kv store, where the key is a concatenation of ChannelWatchSubPath, nodeID and channel name.
// ${WatchSubPath}/${nodeID}/${channelName}
func buildNodeChannelKey(nodeID int64, chName string) string {
	return fmt.Sprintf("%s%s%d%s%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), delimiter, nodeID, delimiter, chName)
}

// buildKeyPrefix generates a key *prefix* for kv store, where the key prefix is a concatenation of ChannelWatchSubPath and nodeID.
func buildKeyPrefix(nodeID int64) string {
	return fmt.Sprintf("%s%s%d", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), delimiter, nodeID)
}

// parseNodeKey validates a given node key, then extracts and returns the corresponding node id on success.
func parseNodeKey(key string) (int64, error) {
	s := strings.Split(key, delimiter)
	if len(s) < 2 {
		return -1, fmt.Errorf("wrong node key in etcd %s", key)
	}
	return strconv.ParseInt(s[len(s)-2], 10, 64)
}

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
	keys, values, err := c.store.LoadWithPrefix(context.TODO(), Params.CommonCfg.DataCoordWatchSubPath.GetValue())
	if err != nil {
		return err
	}

	dupChannel := []*StateChannel{}
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

		channelName := info.GetVchan().GetChannelName()
		channel := NewStateChannelByWatchInfo(nodeID, info)

		if c.HasChannel(channelName) {
			dupChannel = append(dupChannel, channel)
			log.Warn("channel store detects duplicated channel, skip recovering it",
				zap.Int64("nodeID", nodeID),
				zap.String("channel", channelName))
			continue
		}

		c.AddNode(nodeID)
		c.channelsInfo[nodeID].AddChannel(channel)
		log.Info("channel store reloads channel from meta",
			zap.Int64("nodeID", nodeID),
			zap.String("channel", channelName))
		metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)).Set(float64(len(c.channelsInfo[nodeID].Channels)))
	}

	for _, channel := range dupChannel {
		log.Warn("channel store clearing duplicated channel",
			zap.String("channel", channel.GetName()), zap.Int64("nodeID", channel.assignedNode))
		chOp := NewChannelOpSet(NewChannelOp(channel.assignedNode, Delete, channel))
		if err := c.Update(chOp); err != nil {
			log.Warn("channel store failed to remove duplicated channel, will retry later",
				zap.String("channel", channel.GetName()),
				zap.Int64("nodeID", channel.assignedNode),
				zap.Error(err))
		}
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

func (c *StateChannelStore) SetState(targetState ChannelState, nodeID int64, channel RWChannel, opID int64) {
	channelName := channel.GetName()
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		if stateChannel, ok := cInfo.Channels[channelName]; ok {
			stateChannel.(*StateChannel).setState(targetState)
		}
	}
}

type Action string

const (
	OnSuccess         Action = "OnSuccess"
	OnFailure         Action = "OnFailure"
	OnNotifyDuplicate Action = "OnNotifyDuplicate" // notify ToWatch to DataNode already subscribed to the channel
)

func (c *StateChannelStore) UpdateState(action Action, nodeID int64, channel RWChannel, opID int64) {
	channelName := channel.GetName()
	if cInfo, ok := c.channelsInfo[nodeID]; ok {
		if stateChannel, ok := cInfo.Channels[channelName]; ok {
			switch action {
			case OnSuccess:
				stateChannel.(*StateChannel).TransitionOnSuccess(opID)
			case OnFailure:
				stateChannel.(*StateChannel).TransitionOnFailure(opID)
			case OnNotifyDuplicate:
				stateChannel.(*StateChannel).setState(ToRelease)
			default:
				log.Warn("unknown action", zap.Any("action", action),
					zap.Int64("nodeID", nodeID),
					zap.String("channel", channelName),
					zap.Int64("opID", opID))
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
		log.Ctx(context.TODO()).Debug("Channel doesn't exist in Node", zap.String("channel", channelName), zap.Int64("nodeID", nodeID))
	} else {
		log.Ctx(context.TODO()).Error("Node doesn't exist", zap.Int64("NodeID", nodeID))
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
			c.removeAssignment(op.NodeID, ch.GetName())
		default:
			log.Ctx(context.TODO()).Error("unknown opType in updateMetaMemoryForSingleOp", zap.Any("type", op.Type))
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
			log.Ctx(context.TODO()).Error("unsupported ChannelOpSet", zap.Any("OpSet", ops))
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
	return c.store.MultiSaveAndRemove(context.TODO(), saves, removals)
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
	return c.store.RemoveWithPrefix(context.TODO(), k)
}
