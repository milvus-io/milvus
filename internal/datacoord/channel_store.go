package datacoord

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap/zapcore"
)

const (
	bufferID  = math.MinInt64
	delimeter = "/"
)

var errUnknownOpType error = errors.New("unknown operation type")

// ChannelOpType type alias uses int8 stands for Channel operation type
type ChannelOpType int8

const (
	// Add const value for Add Channel operation type
	Add ChannelOpType = iota
	// Delete const value for Delete Channel operation type
	Delete
)

//ChannelOp is the operation to update the channel store
type ChannelOp struct {
	Type              ChannelOpType
	NodeID            int64
	Channels          []*channel
	ChannelWatchInfos []*datapb.ChannelWatchInfo
}

// ChannelOpSet contains some channel update operations
type ChannelOpSet []*ChannelOp

// Add adds the operation which maps channels to node
func (cos *ChannelOpSet) Add(id int64, channels []*channel) {
	*cos = append(*cos, &ChannelOp{
		NodeID:   id,
		Type:     Add,
		Channels: channels,
	})
}

// Delete remove the mapping between channels and node
func (cos *ChannelOpSet) Delete(id int64, channels []*channel) {
	*cos = append(*cos, &ChannelOp{
		NodeID:   id,
		Type:     Delete,
		Channels: channels,
	})
}

// ROChannelStore is the read only channel store from which user can read the mapping between channels and node
type ROChannelStore interface {
	// GetNode gets the channel info of node
	GetNode(nodeID int64) *NodeChannelInfo
	// GetChannels gets all channel infos
	GetChannels() []*NodeChannelInfo
	// GetNodesChannels gets the channels assigned to real nodes
	GetNodesChannels() []*NodeChannelInfo
	// GetBufferChannelInfo gets the unassigned channels
	GetBufferChannelInfo() *NodeChannelInfo
	// GetNodes gets all nodes id in store
	GetNodes() []int64
}

// RWChannelStore is the read write channel store which matains the mapping between channels and node
type RWChannelStore interface {
	ROChannelStore
	// Reload restores the buffer channels and node-channels mapping form kv
	Reload() error
	// Add creates a new node-channels mapping, but no channels are assigned to this node
	Add(nodeID int64)
	// Delete removes nodeID and returns the channels
	Delete(nodeID int64) ([]*channel, error)
	// Update applies the operations in ChannelOpSet
	Update(op ChannelOpSet) error
}

var _ RWChannelStore = (*ChannelStore)(nil)

// ChannelStore maintains the mapping relationship between channel and datanode
type ChannelStore struct {
	store        kv.TxnKV
	channelsInfo map[int64]*NodeChannelInfo
}

// NodeChannelInfo is the mapping between channels and node
type NodeChannelInfo struct {
	NodeID   int64
	Channels []*channel
}

// NewChannelStore creates a new ChannelStore
func NewChannelStore(kv kv.TxnKV) *ChannelStore {
	c := &ChannelStore{
		store:        kv,
		channelsInfo: make(map[int64]*NodeChannelInfo),
	}
	c.channelsInfo[bufferID] = &NodeChannelInfo{
		NodeID:   bufferID,
		Channels: make([]*channel, 0),
	}
	return c
}

// Reload restores the buffer channels and node-channels mapping form kv
func (c *ChannelStore) Reload() error {
	keys, values, err := c.store.LoadWithPrefix(Params.ChannelWatchSubPath)
	if err != nil {
		return err
	}
	for i := 0; i < len(keys); i++ {
		k := keys[i]
		v := values[i]
		nodeID, err := parseNodeID(k)
		if err != nil {
			return err
		}

		temp := &datapb.ChannelWatchInfo{}
		if err := proto.Unmarshal([]byte(v), temp); err != nil {
			return err
		}

		c.Add(nodeID)
		channel := &channel{
			name:         temp.GetVchan().GetChannelName(),
			collectionID: temp.GetVchan().GetCollectionID(),
		}
		c.channelsInfo[nodeID].Channels = append(c.channelsInfo[nodeID].Channels, channel)
	}
	return nil
}

// Add create a new node-channels mapping, but no channels are assigned to this node
func (c *ChannelStore) Add(nodeID int64) {
	if _, ok := c.channelsInfo[nodeID]; ok {
		return
	}

	c.channelsInfo[nodeID] = &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: make([]*channel, 0),
	}
}

// Update applies the operations in opSet
func (c *ChannelStore) Update(opSet ChannelOpSet) error {
	if err := c.txn(opSet); err != nil {
		return err
	}

	for _, v := range opSet {
		switch v.Type {
		case Add:
			c.channelsInfo[v.NodeID].Channels = append(c.channelsInfo[v.NodeID].Channels, v.Channels...)
		case Delete:
			filter := make(map[string]struct{})
			for _, ch := range v.Channels {
				filter[ch.name] = struct{}{}
			}
			origin := c.channelsInfo[v.NodeID].Channels
			res := make([]*channel, 0, len(origin))
			for _, ch := range origin {
				if _, ok := filter[ch.name]; !ok {
					res = append(res, ch)
				}
			}
			c.channelsInfo[v.NodeID].Channels = res
		default:
			return errUnknownOpType
		}
	}
	return nil
}

// GetChannels gets all channel infos
func (c *ChannelStore) GetChannels() []*NodeChannelInfo {
	ret := make([]*NodeChannelInfo, 0, len(c.channelsInfo))
	for _, info := range c.channelsInfo {
		ret = append(ret, info)
	}
	return ret
}

// GetNodesChannels gets the channels assigned to real nodes
func (c *ChannelStore) GetNodesChannels() []*NodeChannelInfo {
	ret := make([]*NodeChannelInfo, 0, len(c.channelsInfo))
	for id, info := range c.channelsInfo {
		if id == bufferID {
			continue
		}
		ret = append(ret, info)
	}
	return ret
}

// GetBufferChannelInfo gets the unassigned channels
func (c *ChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	for id, info := range c.channelsInfo {
		if id == bufferID {
			return info
		}
	}
	return nil
}

// GetNode gets the channel info of node
func (c *ChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	for id, info := range c.channelsInfo {
		if id == nodeID {
			return info
		}
	}
	return nil
}

// Delete remove the nodeID and returns its channels
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

// GetNodes gets all nodes id in store
func (c *ChannelStore) GetNodes() []int64 {
	ids := make([]int64, 0, len(c.channelsInfo))
	for id := range c.channelsInfo {
		if id == bufferID {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func (c *ChannelStore) remove(nodeID int64) error {
	k := buildNodeKey(nodeID)
	return c.store.RemoveWithPrefix(k)
}

func (c *ChannelStore) txn(opSet ChannelOpSet) error {
	saves := make(map[string]string)
	removals := make([]string, 0)
	for _, update := range opSet {
		for i, c := range update.Channels {
			k := buildChannelKey(update.NodeID, c.name)
			switch update.Type {
			case Add:
				val, err := proto.Marshal(update.ChannelWatchInfos[i])
				if err != nil {
					return err
				}
				saves[k] = string(val)
			case Delete:
				removals = append(removals, k)
			default:
				return errUnknownOpType
			}
		}
	}
	return c.store.MultiSaveAndRemove(saves, removals)
}

func buildChannelKey(nodeID int64, channel string) string {
	return fmt.Sprintf("%s%s%d%s%s", Params.ChannelWatchSubPath, delimeter, nodeID, delimeter, channel)
}

func buildNodeKey(nodeID int64) string {
	return fmt.Sprintf("%s%s%d", Params.ChannelWatchSubPath, delimeter, nodeID)
}

func parseNodeID(key string) (int64, error) {
	s := strings.Split(key, delimeter)
	if len(s) < 2 {
		return -1, fmt.Errorf("wrong channel key in etcd %s", key)
	}
	return strconv.ParseInt(s[len(s)-2], 10, 64)
}

// ChannelOpTypeNames implements zap log marshaler for ChannelOpSet
var ChannelOpTypeNames = []string{"Add", "Delete"}

// MarshalLogObject implements the interface ObjectMarshaler
func (cu *ChannelOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", ChannelOpTypeNames[cu.Type])
	enc.AddInt64("nodeID", cu.NodeID)
	cstr := "["
	if len(cu.Channels) > 0 {
		for _, s := range cu.Channels {
			cstr += s.name
			cstr += ", "
		}
		cstr = cstr[:len(cstr)-2]
	}
	cstr += "]"
	enc.AddString("channels", cstr)
	return nil
}

func (cos ChannelOpSet) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range cos {
		enc.AppendObject(o)
	}
	return nil
}
