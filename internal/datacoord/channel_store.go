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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ROChannelStore is a read only channel store for channels and nodes.
//
//go:generate mockery --name=ROChannelStore --structname=ROChannelStore --output=./ --filename=mock_ro_channel_store.go --with-expecter
type ROChannelStore interface {
	// GetNode returns the channel info of a specific node.
	// Returns nil if the node doesn't belong to the cluster
	GetNode(nodeID int64) *NodeChannelInfo
	// HasChannel checks if store already has the channel
	HasChannel(channel string) bool
	// GetNodesChannels returns the channels that are assigned to nodes.
	// without bufferID node
	GetNodesChannels() []*NodeChannelInfo
	// GetBufferChannelInfo gets the unassigned channels.
	GetBufferChannelInfo() *NodeChannelInfo
	// GetNodes gets all node ids in store.
	GetNodes() []int64
	// GetNodeChannelCount
	GetNodeChannelCount(nodeID int64) int
	// GetNodeChannels for given collection
	GetNodeChannelsByCollectionID(collectionID UniqueID) map[UniqueID][]string

	// GetNodeChannelsBy used by channel_store_v2 and channel_manager_v2 only
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
	UpdateState(isSuccessful bool, nodeID int64, channel RWChannel, opID int64)
	// SegLegacyChannelByNode is used by StateChannelStore only
	SetLegacyChannelByNode(nodeIDs ...int64)
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

func NewAddOp(id int64, channels ...RWChannel) *ChannelOp {
	return &ChannelOp{
		NodeID:   id,
		Type:     Add,
		Channels: channels,
	}
}

func NewDeleteOp(id int64, channels ...RWChannel) *ChannelOp {
	return &ChannelOp{
		NodeID:   id,
		Type:     Delete,
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
			if paramtable.Get().DataCoordCfg.EnableBalanceChannelWithRPC.GetAsBool() {
				tmpWatchInfo.Vchan = reduceVChanSize(tmpWatchInfo.GetVchan())
			}
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

// ChannelStore must satisfy RWChannelStore.
var _ RWChannelStore = (*ChannelStore)(nil)

// ChannelStore maintains a mapping between channels and data nodes.
type ChannelStore struct {
	store        kv.TxnKV                   // A kv store with (NodeChannelKey) -> (ChannelWatchInfos) information.
	channelsInfo map[int64]*NodeChannelInfo // A map of (nodeID) -> (NodeChannelInfo).
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

// NewChannelStore creates and returns a new ChannelStore.
func NewChannelStore(kv kv.TxnKV) *ChannelStore {
	c := &ChannelStore{
		store:        kv,
		channelsInfo: make(map[int64]*NodeChannelInfo),
	}
	c.channelsInfo[bufferID] = &NodeChannelInfo{
		NodeID:   bufferID,
		Channels: make(map[string]RWChannel),
	}
	return c
}

// Reload restores the buffer channels and node-channels mapping from kv.
func (c *ChannelStore) Reload() error {
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

		cw := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal([]byte(v), cw); err != nil {
			return err
		}
		reviseVChannelInfo(cw.GetVchan())

		c.AddNode(nodeID)
		channel := &channelMeta{
			Name:         cw.GetVchan().GetChannelName(),
			CollectionID: cw.GetVchan().GetCollectionID(),
			Schema:       cw.GetSchema(),
			WatchInfo:    cw,
		}
		c.channelsInfo[nodeID].AddChannel(channel)

		log.Info("channel store reload channel",
			zap.Int64("nodeID", nodeID), zap.String("channel", channel.Name))
		metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)).Set(float64(len(c.channelsInfo[nodeID].Channels)))
	}
	log.Info("channel store reload done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// AddNode creates a new node-channels mapping for the given node, and assigns no channels to it.
// Returns immediately if the node's already in the channel.
func (c *ChannelStore) AddNode(nodeID int64) {
	if _, ok := c.channelsInfo[nodeID]; ok {
		return
	}

	c.channelsInfo[nodeID] = NewNodeChannelInfo(nodeID)
}

// Update applies the channel operations in opSet.
func (c *ChannelStore) Update(opSet *ChannelOpSet) error {
	totalChannelNum := opSet.GetChannelNumber()
	if totalChannelNum <= maxOperationsPerTxn {
		return c.update(opSet)
	}

	// Split opset into multiple txn. Operations on the same channel must be executed in one txn.
	perChOps := opSet.SplitByChannel()

	// Execute a txn for every 64 operations.
	count := 0
	operations := make([]*ChannelOp, 0, maxOperationsPerTxn)
	for _, opset := range perChOps {
		if count+opset.Len() > maxOperationsPerTxn {
			if err := c.update(NewChannelOpSet(operations...)); err != nil {
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
	return c.update(NewChannelOpSet(operations...))
}

func (c *ChannelStore) checkIfExist(nodeID int64, channel RWChannel) bool {
	if info, ok := c.channelsInfo[nodeID]; ok {
		if ch, ok := info.Channels[channel.GetName()]; ok {
			return ch.GetCollectionID() == channel.GetCollectionID()
		}
	}
	return false
}

// update applies the ADD/DELETE operations to the current channel store.
func (c *ChannelStore) update(opSet *ChannelOpSet) error {
	// Update ChannelStore's kv store.
	if err := c.txn(opSet); err != nil {
		return err
	}

	// Update node id -> channel mapping.
	for _, op := range opSet.Collect() {
		switch op.Type {
		case Add, Watch, Release:
			for _, ch := range op.Channels {
				if c.checkIfExist(op.NodeID, ch) {
					continue // prevent adding duplicated channel info
				}
				// Append target channels to channel store.
				c.channelsInfo[op.NodeID].AddChannel(ch)
			}
		case Delete:
			info := c.channelsInfo[op.NodeID]
			for _, channelName := range op.GetChannelNames() {
				info.RemoveChannel(channelName)
			}
		default:
			return errUnknownOpType
		}
		metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(op.NodeID, 10)).Set(float64(len(c.channelsInfo[op.NodeID].Channels)))
	}
	return nil
}

// GetChannels returns information of all channels.
func (c *ChannelStore) GetChannels() []*NodeChannelInfo {
	ret := make([]*NodeChannelInfo, 0, len(c.channelsInfo))
	for _, info := range c.channelsInfo {
		ret = append(ret, info)
	}
	return ret
}

// GetNodesChannels returns the channels assigned to real nodes.
func (c *ChannelStore) GetNodesChannels() []*NodeChannelInfo {
	ret := make([]*NodeChannelInfo, 0, len(c.channelsInfo))
	for id, info := range c.channelsInfo {
		if id != bufferID {
			ret = append(ret, info)
		}
	}
	return ret
}

func (c *ChannelStore) GetNodeChannelsByCollectionID(collectionID UniqueID) map[UniqueID][]string {
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

// GetBufferChannelInfo returns all unassigned channels.
func (c *ChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	if info, ok := c.channelsInfo[bufferID]; ok {
		return info
	}
	return nil
}

// GetNode returns the channel info of a given node.
func (c *ChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	if info, ok := c.channelsInfo[nodeID]; ok {
		return info
	}
	return nil
}

func (c *ChannelStore) GetNodeChannelCount(nodeID int64) int {
	if info, ok := c.channelsInfo[nodeID]; ok {
		return len(info.Channels)
	}
	return 0
}

// RemoveNode removes the given node from the channel store and returns its channels.
func (c *ChannelStore) RemoveNode(nodeID int64) {
	delete(c.channelsInfo, nodeID)
}

// GetNodes returns a slice of all nodes ids in the current channel store.
func (c *ChannelStore) GetNodes() []int64 {
	ids := make([]int64, 0, len(c.channelsInfo))
	for id := range c.channelsInfo {
		if id != bufferID {
			ids = append(ids, id)
		}
	}
	return ids
}

// remove deletes kv pairs from the kv store where keys have given nodeID as prefix.
func (c *ChannelStore) remove(nodeID int64) error {
	k := buildKeyPrefix(nodeID)
	return c.store.RemoveWithPrefix(k)
}

// txn updates the channelStore's kv store with the given channel ops.
func (c *ChannelStore) txn(opSet *ChannelOpSet) error {
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

func (c *ChannelStore) HasChannel(channel string) bool {
	for _, info := range c.channelsInfo {
		for _, ch := range info.Channels {
			if ch.GetName() == channel {
				return true
			}
		}
	}
	return false
}

func (c *ChannelStore) GetNodeChannelsBy(nodeSelector NodeSelector, channelSelectors ...ChannelSelector) []*NodeChannelInfo {
	log.Error("ChannelStore doesn't implement GetNodeChannelsBy")
	return nil
}

func (c *ChannelStore) UpdateState(isSuccessful bool, nodeID int64, channel RWChannel, opID int64) {
	log.Error("ChannelStore doesn't implement UpdateState")
}

func (c *ChannelStore) SetLegacyChannelByNode(nodeIDs ...int64) {
	log.Error("ChannelStore doesn't implement SetLegacyChannelByNode")
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
