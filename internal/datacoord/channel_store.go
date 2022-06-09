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
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
)

// ChannelOp is an individual ADD or DELETE operation to the channel store.
type ChannelOp struct {
	Type              ChannelOpType
	NodeID            int64
	Channels          []*channel
	ChannelWatchInfos []*datapb.ChannelWatchInfo
}

// ChannelOpSet is a set of channel operations.
type ChannelOpSet []*ChannelOp

// Add appends a single operation to add the mapping between a node and channels.
func (cos *ChannelOpSet) Add(id int64, channels []*channel) {
	*cos = append(*cos, &ChannelOp{
		NodeID:   id,
		Type:     Add,
		Channels: channels,
	})
}

// Delete appends a single operation to remove the mapping between a node and channels.
func (cos *ChannelOpSet) Delete(id int64, channels []*channel) {
	*cos = append(*cos, &ChannelOp{
		NodeID:   id,
		Type:     Delete,
		Channels: channels,
	})
}

// ROChannelStore is a read only channel store for channels and nodes.
type ROChannelStore interface {
	// GetNode returns the channel info of a specific node.
	GetNode(nodeID int64) *NodeChannelInfo
	// GetChannels returns info of all channels.
	GetChannels() []*NodeChannelInfo
	// GetNodesChannels returns the channels that are assigned to nodes.
	GetNodesChannels() []*NodeChannelInfo
	// GetBufferChannelInfo gets the unassigned channels.
	GetBufferChannelInfo() *NodeChannelInfo
	// GetNodes gets all node ids in store.
	GetNodes() []int64
	// LoadAllChannels returns all channels of the given nodeId
	LoadAllChannels(nodeID UniqueID) ([]*datapb.ChannelWatchInfo, error)
}

// RWChannelStore is the read write channel store for channels and nodes.
type RWChannelStore interface {
	ROChannelStore
	// Reload restores the buffer channels and node-channels mapping form kv.
	Reload() error
	// Add creates a new node-channels mapping, with no channels assigned to the node.
	Add(nodeID int64)
	// Delete removes nodeID and returns its channels.
	Delete(nodeID int64) ([]*channel, error)
	// Update applies the operations in ChannelOpSet.
	Update(op ChannelOpSet) error
}

// ChannelStore must satisfy RWChannelStore.
var _ RWChannelStore = (*ChannelStore)(nil)

// TODO: merge ChannelStore into meta, meta should tak charge of all the operations with kv
// ChannelStore maintains a mapping between channels and data nodes.
type ChannelStore struct {
	meta         meta
	channelsInfo map[int64]*NodeChannelInfo // A map of (nodeID) -> (NodeChannelInfo).
}

// NodeChannelInfo stores the nodeID and its channels.
type NodeChannelInfo struct {
	NodeID   int64
	Channels []*channel
}

// NewChannelStore creates and returns a new ChannelStore.
func NewChannelStore(meta meta) *ChannelStore {
	c := &ChannelStore{
		meta:         meta,
		channelsInfo: make(map[int64]*NodeChannelInfo),
	}
	c.channelsInfo[bufferID] = &NodeChannelInfo{
		NodeID:   bufferID,
		Channels: make([]*channel, 0),
	}
	return c
}

// Reload restores the buffer channels and node-channels mapping from kv.
func (c *ChannelStore) Reload() error {
	keys, values, err := c.meta.client.LoadWithPrefix(Params.DataCoordCfg.ChannelWatchSubPath)
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

		c.Add(nodeID)
		channel := &channel{
			Name:         cw.GetVchan().GetChannelName(),
			CollectionID: cw.GetVchan().GetCollectionID(),
		}
		c.channelsInfo[nodeID].Channels = append(c.channelsInfo[nodeID].Channels, channel)
	}
	return nil
}

// Add creates a new node-channels mapping for the given node, and assigns no channels to it.
// Returns immediately if the node's already in the channel.
func (c *ChannelStore) Add(nodeID int64) {
	if _, ok := c.channelsInfo[nodeID]; ok {
		return
	}

	c.channelsInfo[nodeID] = &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: make([]*channel, 0),
	}
}

// Update applies the channel operations in opSet.
func (c *ChannelStore) Update(opSet ChannelOpSet) error {
	totalChannelNum := 0
	for _, op := range opSet {
		totalChannelNum += len(op.Channels)
	}
	if totalChannelNum <= maxOperationsPerTxn {
		return c.update(opSet)
	}
	// Split opset into multiple txn. Operations on the same channel must be executed in one txn.
	perChOps := make(map[string]ChannelOpSet)
	for _, op := range opSet {
		for i, ch := range op.Channels {
			chOp := &ChannelOp{
				Type:     op.Type,
				NodeID:   op.NodeID,
				Channels: []*channel{ch},
			}
			if op.Type == Add {
				chOp.ChannelWatchInfos = []*datapb.ChannelWatchInfo{c.meta.compactChannelWatchInfo(op.ChannelWatchInfos[i])}
			}
			perChOps[ch.Name] = append(perChOps[ch.Name], chOp)
		}
	}

	// Execute a txn for every 128 operations.
	count := 0
	operations := make([]*ChannelOp, 0, maxOperationsPerTxn)
	for _, opset := range perChOps {
		if count+len(opset) > maxOperationsPerTxn {
			if err := c.update(operations); err != nil {
				return err
			}
			count = 0
			operations = make([]*ChannelOp, 0, maxOperationsPerTxn)
		}
		count += len(opset)
		operations = append(operations, opset...)
	}
	if count == 0 {
		return nil
	}
	return c.update(operations)
}

// update applies the ADD/DELETE operations to the current channel store.
func (c *ChannelStore) update(opSet ChannelOpSet) error {
	// Update ChannelStore's kv store.
	if err := c.txn(opSet); err != nil {
		return err
	}

	// Update node id -> channel mapping.
	for _, op := range opSet {
		switch op.Type {
		case Add:
			// Append target channels to channel store.
			c.channelsInfo[op.NodeID].Channels = append(c.channelsInfo[op.NodeID].Channels, op.Channels...)
		case Delete:
			// Remove target channels from channel store.
			del := make(map[string]struct{})
			for _, ch := range op.Channels {
				del[ch.Name] = struct{}{}
			}
			prev := c.channelsInfo[op.NodeID].Channels
			curr := make([]*channel, 0, len(prev))
			for _, ch := range prev {
				if _, ok := del[ch.Name]; !ok {
					curr = append(curr, ch)
				}
			}
			c.channelsInfo[op.NodeID].Channels = curr
		default:
			return errUnknownOpType
		}
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

// GetBufferChannelInfo returns all unassigned channels.
func (c *ChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	for id, info := range c.channelsInfo {
		if id == bufferID {
			return info
		}
	}
	return nil
}

// GetNode returns the channel info of a given node.
func (c *ChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	for id, info := range c.channelsInfo {
		if id == nodeID {
			return info
		}
	}
	return nil
}

// Delete removes the given node from the channel store and returns its channels.
func (c *ChannelStore) Delete(nodeID int64) ([]*channel, error) {
	for id, info := range c.channelsInfo {
		if id == nodeID {
			delete(c.channelsInfo, id)
			if err := c.remove(nodeID); err != nil {
				return nil, err
			}
			return info.Channels, nil
		}
	}
	return nil, nil
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
	return c.meta.client.RemoveWithPrefix(k)
}

func (c *ChannelStore) LoadAllChannels(nodeID UniqueID) ([]*datapb.ChannelWatchInfo, error) {
	prefix := path.Join(Params.DataCoordCfg.ChannelWatchSubPath, strconv.FormatInt(nodeID, 10))

	// TODO: change to LoadWithPrefixBytes
	keys, values, err := c.meta.client.LoadWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	ret := []*datapb.ChannelWatchInfo{}

	for i, k := range keys {
		watchInfo, err := parseWatchInfo(k, []byte(values[i]))
		watchInfo = c.meta.deCompactChannelWatchInfo(watchInfo)
		if err != nil {
			// TODO: delete this kv later
			log.Warn("invalid watchInfo loaded", zap.Error(err))
			continue
		}

		ret = append(ret, watchInfo)
	}

	return ret, nil
}

// txn updates the channelStore's kv store with the given channel ops.
func (c *ChannelStore) txn(opSet ChannelOpSet) error {
	saves := make(map[string]string)
	var removals []string
	for _, op := range opSet {
		for i, ch := range op.Channels {
			k := buildNodeChannelKey(op.NodeID, ch.Name)
			switch op.Type {
			case Add:
				info, err := proto.Marshal(c.meta.compactChannelWatchInfo(op.ChannelWatchInfos[i]))
				if err != nil {
					return err
				}
				log.Info("wayblink ChannelStore add ChannelWatchInfo",
					zap.String("key", k),
					zap.String("value", string(info)),
					zap.Int("length", len(string(info))))
				saves[k] = string(info)
			case Delete:
				log.Info("wayblink ChannelStore delete ChannelWatchInfo",
					zap.String("key", k))
				removals = append(removals, k)
			default:
				return errUnknownOpType
			}
		}
	}
	return c.meta.client.MultiSaveAndRemove(saves, removals)
}

// buildNodeChannelKey generates a key for kv store, where the key is a concatenation of ChannelWatchSubPath, nodeID and channel name.
func buildNodeChannelKey(nodeID int64, chName string) string {
	return fmt.Sprintf("%s%s%d%s%s", Params.DataCoordCfg.ChannelWatchSubPath, delimiter, nodeID, delimiter, chName)
}

// buildKeyPrefix generates a key *prefix* for kv store, where the key prefix is a concatenation of ChannelWatchSubPath and nodeID.
func buildKeyPrefix(nodeID int64) string {
	return fmt.Sprintf("%s%s%d", Params.DataCoordCfg.ChannelWatchSubPath, delimiter, nodeID)
}

// parseNodeKey validates a given node key, then extracts and returns the corresponding node id on success.
func parseNodeKey(key string) (int64, error) {
	s := strings.Split(key, delimiter)
	if len(s) < 2 {
		return -1, fmt.Errorf("wrong node key in etcd %s", key)
	}
	return strconv.ParseInt(s[len(s)-2], 10, 64)
}

// ChannelOpTypeNames implements zap log marshaller for ChannelOpSet.
var ChannelOpTypeNames = []string{"Add", "Delete"}

// TODO: NIT: ObjectMarshaler -> ObjectMarshaller
// MarshalLogObject implements the interface ObjectMarshaler.
func (cu *ChannelOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", ChannelOpTypeNames[cu.Type])
	enc.AddInt64("nodeID", cu.NodeID)
	cstr := "["
	if len(cu.Channels) > 0 {
		for _, s := range cu.Channels {
			cstr += s.Name
			cstr += ", "
		}
		cstr = cstr[:len(cstr)-2]
	}
	cstr += "]"
	enc.AddString("channels", cstr)
	return nil
}

// TODO: NIT: ArrayMarshaler -> ArrayMarshaller
// MarshalLogArray implements the interface of ArrayMarshaler of zap.
func (cos ChannelOpSet) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range cos {
		enc.AppendObject(o)
	}
	return nil
}
