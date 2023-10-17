package datacoord

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ChannelOpTypeNames implements zap log marshaller for ChannelOpSet.
var ChannelOpTypeNames = []string{"Add", "Delete", "Watch", "Release"}

// ROChannelStore is a read only channel store for channels and nodes.
type ROChannelStore interface {
	// GetNode returns the channel info of a specific node.
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
	GetNodeChannelsBy(nodeSelector NodeSelector, channelSelectors ...ChannelSelector) []*NodeChannelInfo
}

// RWChannelStore is the read write channel store for channels and nodes.
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
	UpdateState(isSuccessful bool, channels ...RWChannel)
	SetLegacyChannelByNode(nodeIDs ...int64)
}

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
		case Watch, Release:
			info, err := proto.Marshal(ch.GetWatchInfo())
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

func (c *ChannelOpSet) Append(ID int64, opType ChannelOpType, channels ...RWChannel) {
	c.ops = append(c.ops, NewChannelOp(ID, opType, channels...))
}

// append a new Add channel op
func (c *ChannelOpSet) Add(ID int64, channels ...RWChannel) {
	c.Append(ID, Add, channels...)
}

func (c *ChannelOpSet) Delete(ID int64, channels ...RWChannel) {
	c.Append(ID, Delete, channels...)
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

// NodeChannelInfo stores the nodeID and its channels.
type NodeChannelInfo struct {
	NodeID   int64
	Channels []RWChannel
}

type StateChannelStore struct {
	store kv.TxnKV

	channels    map[string]*StateChannel
	assignments map[int64][]string // node to channel name
}

var _ RWChannelStore = (*StateChannelStore)(nil)

func NewChannelStore(kv kv.TxnKV) RWChannelStore {
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
	totalChannelNum := opSet.GetChannelNumber()
	if totalChannelNum <= maxOperationsPerTxn {
		return c.updateMeta(opSet)
	}

	// Split opset into multiple txn. Operations on the same channel must be executed in one txn.
	perChOps := opSet.SplitByChannel()

	// Execute a txn for every 64 operations.
	count := 0
	operations := make([]*ChannelOp, 0, maxOperationsPerTxn)
	for _, opset := range perChOps {
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
func (c *StateChannelStore) updateMeta(opSet *ChannelOpSet) error {
	// Update ChannelStore's kv store.
	if err := c.txn(opSet); err != nil {
		return err
	}

	// Update memory
	chOpSet := opSet.SplitByChannel()
	for chName, ops := range chOpSet {
		// DELETE + WATCH
		if ops.Len() >= 2 {
			for _, op := range opSet.Collect() {
				ch := c.channels[chName]
				switch op.Type {
				case Delete:
					c.removeAssignment(op.NodeID, chName)
				case Watch:
					c.addAssignment(op.NodeID, chName)
					ch.Assign(op.NodeID)
					ch.setState(ToWatch)
				default:
					return errUnknownOpType
				}
			}
		} else if ops.Len() > 0 {
			op := opSet.Collect()[0]
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
					log.Warn("unknown opType", zap.Any("type", op.Type))
				}
			})
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

func WithCollectionID(collectionID int64) ChannelSelector {
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
			selected := lo.FilterMap(channels, func(chName string, _ int) (RWChannel, bool) {
				channel := c.channels[chName].Clone()

				if len(channelSelectors) == 0 {
					return channel, true
				}

				var sel bool = true
				for _, selector := range channelSelectors {
					if !selector(channel) {
						sel = false
						break
					}
				}
				return channel, sel
			})

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

		nodeChannels[nodeID] = &NodeChannelInfo{
			NodeID: nodeID,
			Channels: lo.Map(channels, func(ch string, _ int) RWChannel {
				return c.channels[ch].Clone()
			}),
		}
	}
	return lo.Values(nodeChannels)
}

func (c *StateChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	return c.GetNode(bufferID)
}

func (c *StateChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	infos := c.GetNodeChannelsBy(WithNodeIDs(nodeID))
	if len(infos) == 0 {
		return nil
	}

	return infos[0]
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

// TODO: NIT: ArrayMarshaler -> ArrayMarshaller
// MarshalLogArray implements the interface of ArrayMarshaler of zap.
func (c *ChannelOpSet) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range c.Collect() {
		enc.AppendObject(o)
	}
	return nil
}

func parseWatchInfo(key string, data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	if err := proto.Unmarshal(data, &watchInfo); err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, key: %s, err: %v", key, err)
	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo, key: %s", key)
	}
	reviseVChannelInfo(watchInfo.GetVchan())

	return &watchInfo, nil
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
