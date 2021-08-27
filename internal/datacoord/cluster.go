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

const clusterPrefix = "cluster-prefix/"
const clusterBuffer = "cluster-buffer"
const nodeEventChBufferSize = 1024

const eventTimeout = 5 * time.Second

type EventType int

const (
	Register      EventType = 1
	UnRegister    EventType = 2
	WatchChannel  EventType = 3
	FlushSegments EventType = 4
)

type NodeEventType int

const (
	Watch NodeEventType = 0
	Flush NodeEventType = 1
)

type Event struct {
	Type EventType
	Data interface{}
}

type WatchChannelParams struct {
	Channel      string
	CollectionID UniqueID
}

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

type ClusterOption func(c *Cluster)

func withRegisterPolicy(p dataNodeRegisterPolicy) ClusterOption {
	return func(c *Cluster) { c.registerPolicy = p }
}

func withUnregistorPolicy(p dataNodeUnregisterPolicy) ClusterOption {
	return func(c *Cluster) { c.unregisterPolicy = p }
}

func withAssignPolicy(p channelAssignPolicy) ClusterOption {
	return func(c *Cluster) { c.assignPolicy = p }
}

func defaultRegisterPolicy() dataNodeRegisterPolicy {
	return newAssignBufferRegisterPolicy()
}

func defaultUnregisterPolicy() dataNodeUnregisterPolicy {
	return randomAssignRegisterFunc
}

func defaultAssignPolicy() channelAssignPolicy {
	return newBalancedAssignPolicy()
}

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

	if err := c.loadFromKv(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) loadFromKv() error {
	_, values, err := c.kv.LoadWithPrefix(clusterPrefix)
	if err != nil {
		return err
	}

	for _, v := range values {
		info := &datapb.DataNodeInfo{}
		if err := proto.UnmarshalText(v, info); err != nil {
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
		if err := proto.UnmarshalText(dn, info); err != nil {
			return err
		}
		c.chanBuffer = info.Channels
	}

	return nil
}

func (c *Cluster) Flush(segments []*datapb.SegmentInfo) {
	c.eventCh <- &Event{
		Type: FlushSegments,
		Data: segments,
	}
}

func (c *Cluster) Register(node *NodeInfo) {
	c.eventCh <- &Event{
		Type: Register,
		Data: node,
	}
}

func (c *Cluster) UnRegister(node *NodeInfo) {
	c.eventCh <- &Event{
		Type: UnRegister,
		Data: node,
	}
}

func (c *Cluster) Watch(channel string, collectionID UniqueID) {
	c.eventCh <- &Event{
		Type: WatchChannel,
		Data: &WatchChannelParams{
			Channel:      channel,
			CollectionID: collectionID,
		},
	}
}

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
				log.Warn("Unknow node event type")
			}
		}
	}
}

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
					log.Warn("failed to save node info", zap.Any("node", node))
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

func parseChannelsFromReq(req *datapb.WatchDmChannelsRequest) []string {
	channels := make([]string, 0, len(req.GetVchannels()))
	for _, vc := range req.GetVchannels() {
		channels = append(channels, vc.ChannelName)
	}
	return channels
}

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

// Startup applies statup policy
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
	c.txnSaveNodesAndBuffer(nodes, c.chanBuffer)
	for _, node := range nodes {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range nodes {
		c.watch(node)
	}
}

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
	c.txnSaveNodesAndBuffer(rets, c.chanBuffer)
	for _, node := range rets {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range rets {
		c.watch(node)
	}
}

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
	c.txnSaveNodesAndBuffer(rets, c.chanBuffer)
	for _, node := range rets {
		c.nodes.SetNode(node.Info.GetVersion(), node)
	}
	c.mu.Unlock()
	for _, node := range rets {
		c.watch(node)
	}
}

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
	value := proto.MarshalTextString(n.Info)
	return c.kv.Save(key, value)
}

func (c *Cluster) txnSaveNodesAndBuffer(nodes []*NodeInfo, buffer []*datapb.ChannelStatus) error {
	if len(nodes) == 0 && len(buffer) == 0 {
		return nil
	}
	data := make(map[string]string)
	for _, n := range nodes {
		key := fmt.Sprintf("%s%d", clusterPrefix, n.Info.GetVersion())
		value := proto.MarshalTextString(n.Info)
		data[key] = value
	}

	// short cut, reusing datainfo to store array of channel status
	bufNode := &datapb.DataNodeInfo{
		Channels: buffer,
	}

	data[clusterBuffer] = proto.MarshalTextString(bufNode)
	return c.kv.MultiSave(data)
}

func (c *Cluster) GetNodes() []*NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes.GetNodes()
}

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
