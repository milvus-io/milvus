// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

// clusterPrefix const for kv prefix storing DataNodeInfo
const clusterPrefix = "cluster-prefix/"

// clusterBuffer const for kv key storing buffer channels(no assigned ones)
const clusterBuffer = "cluster-buffer"

// nodeEventChBufferSize magic number for Event Channel buffer size
const nodeEventChBufferSize = 1024

// eventTimeout magic number for event timeout
const eventTimeout = 5 * time.Second

// EventType enum for events
type EventType int

const (
	// Register EventType const for data node registration
	Register EventType = 1
	// UnRegister EventType const for data node unRegistration
	UnRegister EventType = 2
	// WatchChannel EventType const for a channel needs to be watched
	WatchChannel EventType = 3
	// FlushSegments EventType const for flush specified segments
	FlushSegments EventType = 4
)

// NodeEventType enum for node events
type NodeEventType int

const (
	// Watch NodeEventType const for assign channel to datanode for watching
	Watch NodeEventType = 1
	// Flush NodeEventTYpe const for flush specified segments
	Flush NodeEventType = 2
)

// Event event wrapper contains EventType and related parameter
type Event struct {
	Type EventType
	Data interface{}
}

// WatchChannelParams Watch Event related parameter struct
type WatchChannelParams struct {
	Channel      string
	CollectionID UniqueID
}

// Cluster handles all DataNode life-cycle and functional events
type Cluster struct {
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.Mutex
	wg               sync.WaitGroup
	nodes            ClusterStore
	posProvider      positionProvider
	chanBuffer       []*datapb.ChannelStatus //Unwatched channels buffer
	kv               kv.TxnKV
	registerPolicy   dataNodeRegisterPolicy
	unregisterPolicy dataNodeUnregisterPolicy
	assignPolicy     channelAssignPolicy
	eventCh          chan *Event
}

// ClusterOption helper function used when creating a Cluster
type ClusterOption func(c *Cluster)

// withRegisterPolicy helper function setting registerPolicy
func withRegisterPolicy(p dataNodeRegisterPolicy) ClusterOption {
	return func(c *Cluster) { c.registerPolicy = p }
}

// withUnregistorPolicy helper function setting unregisterPolicy
func withUnregistorPolicy(p dataNodeUnregisterPolicy) ClusterOption {
	return func(c *Cluster) { c.unregisterPolicy = p }
}

// withAssignPolicy helper function setting assignPolicy
func withAssignPolicy(p channelAssignPolicy) ClusterOption {
	return func(c *Cluster) { c.assignPolicy = p }
}

// defaultRegisterPolicy returns default registerPolicy
func defaultRegisterPolicy() dataNodeRegisterPolicy {
	return newAssignBufferRegisterPolicy()
}

// defaultUnregisterPolicy returns default unregisterPolicy
func defaultUnregisterPolicy() dataNodeUnregisterPolicy {
	return randomAssignRegisterFunc
}

// defaultAssignPolicy returns default assignPolicy
func defaultAssignPolicy() channelAssignPolicy {
	return newBalancedAssignPolicy()
}

// NewCluster creates a cluster with provided components
// triggers loadFromKV to load previous meta from KV if exists
// returns error when loadFromKV fails
func NewCluster(ctx context.Context, kv kv.TxnKV, store ClusterStore,
	posProvider positionProvider, opts ...ClusterOption) (*Cluster, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cluster{
		ctx:              ctx,
		cancel:           cancel,
		kv:               kv,
		nodes:            store,
		posProvider:      posProvider,
		chanBuffer:       []*datapb.ChannelStatus{},
		registerPolicy:   defaultRegisterPolicy(),
		unregisterPolicy: defaultUnregisterPolicy(),
		assignPolicy:     defaultAssignPolicy(),
		eventCh:          make(chan *Event, nodeEventChBufferSize),
	}

	for _, opt := range opts {
		opt(c)
	}

	if err := c.loadFromKV(); err != nil {
		return nil, err
	}
	return c, nil
}

// loadFromKV load pre-stored kv meta
// keys start with clusterPrefix stands for DataNodeInfos
// value bind to key clusterBuffer stands for Channels not assigned yet
func (c *Cluster) loadFromKV() error {
	_, values, err := c.kv.LoadWithPrefix(clusterPrefix)
	if err != nil {
		return err
	}

	for _, v := range values {
		info := &datapb.DataNodeInfo{}
		if err := proto.Unmarshal([]byte(v), info); err != nil {
			return err
		}

		node := NewNodeInfo(c.ctx, info)
		c.nodes.SetNode(info.GetVersion(), node)
		go c.handleEvent(node)
	}
	dn, _ := c.kv.Load(clusterBuffer)
	//TODO add not value error check
	if dn != "" {
		info := &datapb.DataNodeInfo{}
		if err := proto.Unmarshal([]byte(dn), info); err != nil {
			return err
		}
		c.chanBuffer = info.Channels
	}

	return nil
}

// Flush triggers Flush event
// puts Event into buffered channel
// function returns not guarantee event processed
func (c *Cluster) Flush(segments []*datapb.SegmentInfo) {
	c.eventCh <- &Event{
		Type: FlushSegments,
		Data: segments,
	}
}

// Register triggers Register event
// put Event into buffered channel
// function returns not guarantee event processed
func (c *Cluster) Register(node *NodeInfo) {
	c.eventCh <- &Event{
		Type: Register,
		Data: node,
	}
}

// UnRegister triggers UnRegister event
// put Event into buffered channel
// function returns not guarantee event processed
func (c *Cluster) UnRegister(node *NodeInfo) {
	c.eventCh <- &Event{
		Type: UnRegister,
		Data: node,
	}
}

// Watch triggers Watch event
// put Event into buffered channel
// function returns not guarantee event processed
func (c *Cluster) Watch(channel string, collectionID UniqueID) {
	c.eventCh <- &Event{
		Type: WatchChannel,
		Data: &WatchChannelParams{
			Channel:      channel,
			CollectionID: collectionID,
		},
	}
}

// handleNodeEvent worker loop handles all node events
func (c *Cluster) handleNodeEvent() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case e := <-c.eventCh:
			switch e.Type {
			case Register:
				c.handleRegister(e.Data.(*NodeInfo))
			case UnRegister:
				c.handleUnRegister(e.Data.(*NodeInfo))
			case WatchChannel:
				params := e.Data.(*WatchChannelParams)
				c.handleWatchChannel(params.Channel, params.CollectionID)
			case FlushSegments:
				c.handleFlush(e.Data.([]*datapb.SegmentInfo))
			default:
				log.Warn("Unknown node event type")
			}
		}
	}
}

// handleEvent worker loop handles all events belongs to specified DataNode
func (c *Cluster) handleEvent(node *NodeInfo) {
	log.Debug("start handle event", zap.Any("node", node))
	ctx := node.ctx
	ch := node.GetEventChannel()
	version := node.Info.GetVersion()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			cli, err := c.getOrCreateClient(ctx, version)
			if err != nil {
				log.Warn("failed to get client", zap.Int64("nodeID", version), zap.Error(err))
				continue
			}
			switch event.Type {
			case Watch:
				req, ok := event.Req.(*datapb.WatchDmChannelsRequest)
				if !ok {
					log.Warn("request type is not Watch")
					continue
				}
				log.Debug("receive watch event", zap.Any("event", event), zap.Any("node", node))
				tCtx, cancel := context.WithTimeout(ctx, eventTimeout)
				resp, err := cli.WatchDmChannels(tCtx, req)
				cancel()
				if err = VerifyResponse(resp, err); err != nil {
					log.Warn("failed to watch dm channels", zap.String("addr", node.Info.GetAddress()))
				}
				c.mu.Lock()
				c.nodes.SetWatched(node.Info.GetVersion(), parseChannelsFromReq(req))
				c.mu.Unlock()
				if err = c.saveNode(node); err != nil {
					log.Warn("failed to save node info", zap.Any("node", node), zap.Error(err))
					continue
				}
			case Flush:
				req, ok := event.Req.(*datapb.FlushSegmentsRequest)
				if !ok {
					log.Warn("request type is not Flush")
					continue
				}
				tCtx, cancel := context.WithTimeout(ctx, eventTimeout)
				resp, err := cli.FlushSegments(tCtx, req)
				cancel()
				if err = VerifyResponse(resp, err); err != nil {
					log.Warn("failed to flush segments", zap.String("addr", node.Info.GetAddress()), zap.Error(err))
				}
			default:
				log.Warn("unknown event type", zap.Any("type", event.Type))
			}
		}
	}
}

// getOrCreateClient get type.DataNode for specified data node
// if not connected yet, try to connect
func (c *Cluster) getOrCreateClient(ctx context.Context, id UniqueID) (types.DataNode, error) {
	c.mu.Lock()
	node := c.nodes.GetNode(id)
	c.mu.Unlock()
	if node == nil {
		return nil, fmt.Errorf("node %d is not alive", id)
	}
	cli := node.GetClient()
	if cli != nil {
		return cli, nil
	}
	var err error
	cli, err = createClient(ctx, node.Info.GetAddress())
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes.SetClient(node.Info.GetVersion(), cli)
	return cli, nil
}

// parseChannelsFromReq map-reduce to fetch channel names
func parseChannelsFromReq(req *datapb.WatchDmChannelsRequest) []string {
	channels := make([]string, 0, len(req.GetVchannels()))
	for _, vc := range req.GetVchannels() {
		channels = append(channels, vc.ChannelName)
	}
	return channels
}

// createClient create type.DataNode from specified address
// needs to be deprecated, since this function hard-corded DataNode to be grpc client
func createClient(ctx context.Context, addr string) (types.DataNode, error) {
	cli, err := grpcdatanodeclient.NewClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := cli.Init(); err != nil {
		return nil, err
	}
	if err := cli.Start(); err != nil {
		return nil, err
	}
	return cli, nil
}

// Startup applies startup policy
func (c *Cluster) Startup(nodes []*NodeInfo) {
	c.wg.Add(1)
	go c.handleNodeEvent()
	// before startup, we have restore all nodes recorded last time. We should
	// find new created/offlined/restarted nodes and adjust channels allocation.
	addNodes, deleteNodes := c.updateCluster(nodes)
	for _, node := range addNodes {
		c.Register(node)
	}

	for _, node := range deleteNodes {
		c.UnRegister(node)
	}
}

// updateCluster update nodes list
// separates them into new nodes list and offline nodes list
func (c *Cluster) updateCluster(nodes []*NodeInfo) (newNodes []*NodeInfo, offlines []*NodeInfo) {
	var onCnt, offCnt float64
	currentOnline := make(map[int64]struct{})
	for _, n := range nodes {
		currentOnline[n.Info.GetVersion()] = struct{}{}
		node := c.nodes.GetNode(n.Info.GetVersion())
		if node == nil {
			newNodes = append(newNodes, n)
		}
		onCnt++
	}

	currNodes := c.nodes.GetNodes()
	for _, node := range currNodes {
		_, has := currentOnline[node.Info.GetVersion()]
		if !has {
			offlines = append(offlines, node)
			offCnt++
		}
	}
	metrics.DataCoordDataNodeList.WithLabelValues("online").Set(onCnt)
	metrics.DataCoordDataNodeList.WithLabelValues("offline").Set(offCnt)
	return
}

// handleRegister handle register logic
// applies register policy and save result into kv store
func (c *Cluster) handleRegister(n *NodeInfo) {
	c.mu.Lock()
	cNodes := c.nodes.GetNodes()
	var nodes []*NodeInfo
	log.Debug("channels info before register policy applied",
		zap.Any("n.Channels", n.Info.GetChannels()),
		zap.Any("buffer", c.chanBuffer))
	nodes, c.chanBuffer = c.registerPolicy(cNodes, n, c.chanBuffer)
	log.Debug("delta changes after register policy applied",
		zap.Any("nodes", nodes),
		zap.Any("buffer", c.chanBuffer))
	go c.handleEvent(n)
	err := c.txnSaveNodesAndBuffer(nodes, c.chanBuffer)
	if err != nil {
		log.Warn("DataCoord Cluster handleRegister txnSaveNodesAndBuffer", zap.Error(err))
	}
	for _, node := range nodes {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range nodes {
		c.watch(node)
	}
}

// handleUnRegister handles datanode unregister logic
// applies unregisterPolicy and stores results into kv store
func (c *Cluster) handleUnRegister(n *NodeInfo) {
	c.mu.Lock()
	node := c.nodes.GetNode(n.Info.GetVersion())
	if node == nil {
		c.mu.Unlock()
		return
	}
	node.Dispose()
	// save deleted node to kv
	deleted := node.Clone(SetChannels(nil))
	c.saveNode(deleted)
	c.nodes.DeleteNode(n.Info.GetVersion())

	cNodes := c.nodes.GetNodes()
	log.Debug("channels info before unregister policy applied", zap.Any("node.Channels", node.Info.GetChannels()), zap.Any("buffer", c.chanBuffer), zap.Any("nodes", cNodes))
	var rets []*NodeInfo
	if len(cNodes) == 0 {
		for _, chStat := range node.Info.GetChannels() {
			chStat.State = datapb.ChannelWatchState_Uncomplete
			c.chanBuffer = append(c.chanBuffer, chStat)
		}
	} else {
		rets = c.unregisterPolicy(cNodes, node)
	}
	log.Debug("delta changes after unregister policy", zap.Any("nodes", rets), zap.Any("buffer", c.chanBuffer))
	err := c.txnSaveNodesAndBuffer(rets, c.chanBuffer)
	if err != nil {
		log.Warn("DataCoord Cluster handleUnRegister txnSaveNodesAndBuffer", zap.Error(err))
	}
	for _, node := range rets {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range rets {
		c.watch(node)
	}
}

// handleWatchChannel handle watch channel logic
// applies assignPolicy and saves results into kv store
func (c *Cluster) handleWatchChannel(channel string, collectionID UniqueID) {
	c.mu.Lock()
	cNodes := c.nodes.GetNodes()
	var rets []*NodeInfo
	if len(cNodes) == 0 { // no nodes to assign, put into buffer
		c.chanBuffer = append(c.chanBuffer, &datapb.ChannelStatus{
			Name:         channel,
			CollectionID: collectionID,
			State:        datapb.ChannelWatchState_Uncomplete,
		})
	} else {
		rets = c.assignPolicy(cNodes, channel, collectionID)
	}
	err := c.txnSaveNodesAndBuffer(rets, c.chanBuffer)
	if err != nil {
		log.Warn("DataCoord Cluster handleWatchChannel txnSaveNodesAndBuffer", zap.Error(err))
	}
	for _, node := range rets {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range rets {
		c.watch(node)
	}
}

// handleFlush handles flush logic
// finds corresponding data nodes and trigger Node Events
func (c *Cluster) handleFlush(segments []*datapb.SegmentInfo) {
	m := make(map[string]map[UniqueID][]UniqueID) // channel-> map[collectionID]segmentIDs
	for _, seg := range segments {
		if _, ok := m[seg.InsertChannel]; !ok {
			m[seg.InsertChannel] = make(map[UniqueID][]UniqueID)
		}

		m[seg.InsertChannel][seg.CollectionID] = append(m[seg.InsertChannel][seg.CollectionID], seg.ID)
	}

	c.mu.Lock()
	dataNodes := c.nodes.GetNodes()
	c.mu.Unlock()

	channel2Node := make(map[string]*NodeInfo)
	for _, node := range dataNodes {
		for _, chstatus := range node.Info.GetChannels() {
			channel2Node[chstatus.Name] = node
		}
	}

	for ch, coll2seg := range m {
		node, ok := channel2Node[ch]
		if !ok {
			continue
		}
		for coll, segs := range coll2seg {
			req := &datapb.FlushSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Flush,
					SourceID: Params.NodeID,
				},
				CollectionID: coll,
				SegmentIDs:   segs,
			}
			ch := node.GetEventChannel()
			e := &NodeEvent{
				Type: Flush,
				Req:  req,
			}
			ch <- e
		}
	}
}

// watch handles watch logic
// finds corresponding data nodes and trigger Node Events
func (c *Cluster) watch(n *NodeInfo) {
	channelNames := make([]string, 0)
	uncompletes := make([]vchannel, 0, len(n.Info.Channels))
	for _, ch := range n.Info.GetChannels() {
		if ch.State == datapb.ChannelWatchState_Uncomplete {
			channelNames = append(channelNames, ch.GetName())
			uncompletes = append(uncompletes, vchannel{
				CollectionID: ch.CollectionID,
				DmlChannel:   ch.Name,
			})
		}
	}

	if len(uncompletes) == 0 {
		return // all set, just return
	}
	log.Debug("plan to watch channel",
		zap.String("node", n.Info.GetAddress()),
		zap.Int64("version", n.Info.GetVersion()),
		zap.Strings("channels", channelNames))

	vchanInfos, err := c.posProvider.GetVChanPositions(uncompletes, true)
	if err != nil {
		log.Warn("get vchannel position failed", zap.Error(err))
		return
	}
	req := &datapb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			SourceID: Params.NodeID,
		},
		Vchannels: vchanInfos,
	}
	e := &NodeEvent{
		Type: Watch,
		Req:  req,
	}
	ch := n.GetEventChannel()
	log.Debug("put watch event to node channel",
		zap.Any("event", e),
		zap.Any("node.version", n.Info.GetVersion()),
		zap.String("node.address", n.Info.GetAddress()))
	ch <- e
}

func (c *Cluster) saveNode(n *NodeInfo) error {
	key := fmt.Sprintf("%s%d", clusterPrefix, n.Info.GetVersion())
	value, err := proto.Marshal(n.Info)
	if err != nil {
		return err
	}
	return c.kv.Save(key, string(value))
}

func (c *Cluster) txnSaveNodesAndBuffer(nodes []*NodeInfo, buffer []*datapb.ChannelStatus) error {
	if len(nodes) == 0 && len(buffer) == 0 {
		return nil
	}
	data := make(map[string]string)
	for _, n := range nodes {
		key := fmt.Sprintf("%s%d", clusterPrefix, n.Info.GetVersion())
		value, err := proto.Marshal(n.Info)
		if err != nil {
			return fmt.Errorf("marshal failed key:%s, err:%w", key, err)
		}
		data[key] = string(value)
	}

	// short cut, reusing dataInfo to store array of channel status
	bufNode := &datapb.DataNodeInfo{
		Channels: buffer,
	}
	buffData, err := proto.Marshal(bufNode)
	if err != nil {
		return fmt.Errorf("marshal bufNode failed:%w", err)
	}
	data[clusterBuffer] = string(buffData)
	return c.kv.MultiSave(data)
}

// GetNodes returns all nodes info in Cluster
func (c *Cluster) GetNodes() []*NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes.GetNodes()
}

// Close dispose all nodes resources
func (c *Cluster) Close() {
	c.cancel()
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	nodes := c.nodes.GetNodes()
	for _, node := range nodes {
		node.Dispose()
	}
}
