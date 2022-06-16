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

package querycoord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

// Cluster manages all query node connections and grpc requests
type Cluster interface {
	// Collection/Parition
	ReleaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error
	ReleasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error

	// Segment
	LoadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error
	ReleaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) error
	GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error)
	GetSegmentInfoByNode(ctx context.Context, nodeID int64, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error)
	GetSegmentInfoByID(ctx context.Context, segmentID UniqueID) (*querypb.SegmentInfo, error)
	SyncReplicaSegments(ctx context.Context, leaderID UniqueID, in *querypb.SyncReplicaSegmentsRequest) error

	// Channel
	WatchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error
	WatchDeltaChannels(ctx context.Context, nodeID int64, in *querypb.WatchDeltaChannelsRequest) error
	HasWatchedDeltaChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool

	// Node
	RegisterNode(ctx context.Context, session *sessionutil.Session, id UniqueID, state nodeState) error
	GetNodeInfoByID(nodeID int64) (Node, error)
	RemoveNodeInfo(nodeID int64) error
	StopNode(nodeID int64)
	OnlineNodeIDs() []int64
	IsOnline(nodeID int64) (bool, error)
	OfflineNodeIDs() []int64
	HasNode(nodeID int64) bool
	GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse

	AllocateSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error
	AllocateChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error
	AssignNodesToReplicas(ctx context.Context, replicas []*milvuspb.ReplicaInfo, collectionSize uint64) error

	GetSessionVersion() int64

	// Inner
	reloadFromKV() error
	getComponentInfos(ctx context.Context) []*internalpb.ComponentInfo
}

type newQueryNodeFn func(ctx context.Context, address string, id UniqueID, kv *etcdkv.EtcdKV) (Node, error)

type nodeState int

const (
	disConnect nodeState = 0
	online     nodeState = 1
	offline    nodeState = 2
)

type queryNodeCluster struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *etcdkv.EtcdKV

	session        *sessionutil.Session
	sessionVersion int64

	sync.RWMutex
	clusterMeta      Meta
	handler          *channelUnsubscribeHandler
	nodes            map[int64]Node
	newNodeFn        newQueryNodeFn
	segmentAllocator SegmentAllocatePolicy
	channelAllocator ChannelAllocatePolicy
}

func newQueryNodeCluster(ctx context.Context, clusterMeta Meta, kv *etcdkv.EtcdKV, newNodeFn newQueryNodeFn, session *sessionutil.Session, handler *channelUnsubscribeHandler) (Cluster, error) {
	childCtx, cancel := context.WithCancel(ctx)
	nodes := make(map[int64]Node)
	c := &queryNodeCluster{
		ctx:              childCtx,
		cancel:           cancel,
		client:           kv,
		session:          session,
		clusterMeta:      clusterMeta,
		handler:          handler,
		nodes:            nodes,
		newNodeFn:        newNodeFn,
		segmentAllocator: defaultSegAllocatePolicy(),
		channelAllocator: defaultChannelAllocatePolicy(),
	}
	err := c.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Reload trigger task, trigger task states, internal task, internal task state from etcd
// Assign the internal task to the corresponding trigger task as a child task
func (c *queryNodeCluster) reloadFromKV() error {
	// get current online session
	onlineNodeSessions, version, _ := c.session.GetSessions(typeutil.QueryNodeRole)
	onlineSessionMap := make(map[int64]*sessionutil.Session)
	for _, session := range onlineNodeSessions {
		nodeID := session.ServerID
		onlineSessionMap[nodeID] = session
	}
	for nodeID, session := range onlineSessionMap {
		log.Info("reloadFromKV: register a queryNode to cluster", zap.Any("nodeID", nodeID))
		err := c.RegisterNode(c.ctx, session, nodeID, disConnect)
		if err != nil {
			log.Warn("QueryNode failed to register", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}
	}
	c.sessionVersion = version

	// load node information before power off from etcd
	oldStringNodeIDs, oldNodeSessions, err := c.client.LoadWithPrefix(queryNodeInfoPrefix)
	if err != nil {
		log.Warn("reloadFromKV: get previous node info from etcd error", zap.Error(err))
		return err
	}
	for index := range oldStringNodeIDs {
		nodeID, err := strconv.ParseInt(filepath.Base(oldStringNodeIDs[index]), 10, 64)
		if err != nil {
			log.Warn("watchNodeLoop: parse nodeID error", zap.Error(err))
			return err
		}
		if _, ok := onlineSessionMap[nodeID]; !ok {
			session := &sessionutil.Session{}
			err = json.Unmarshal([]byte(oldNodeSessions[index]), session)
			if err != nil {
				log.Warn("watchNodeLoop: unmarshal session error", zap.Error(err))
				return err
			}
			err = c.RegisterNode(context.Background(), session, nodeID, offline)
			if err != nil {
				log.Warn("reloadFromKV: failed to add queryNode to cluster", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
				return err
			}
		}
	}

	return nil
}

func (c *queryNodeCluster) GetSessionVersion() int64 {
	return c.sessionVersion
}

func (c *queryNodeCluster) getComponentInfos(ctx context.Context) []*internalpb.ComponentInfo {
	c.RLock()
	defer c.RUnlock()
	var subComponentInfos []*internalpb.ComponentInfo
	for _, node := range c.nodes {
		componentState := node.getComponentInfo(ctx)
		subComponentInfos = append(subComponentInfos, componentState)
	}

	return subComponentInfos
}

func (c *queryNodeCluster) LoadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		collectionID := in.CollectionID
		// if node has watched the collection's deltaChannel
		// then the node should recover part delete log from dmChanel
		if c.HasWatchedDeltaChannel(ctx, nodeID, collectionID) {
			// get all deltaChannelInfo of the collection from meta
			deltaChannelInfos, err := c.clusterMeta.getDeltaChannelsByCollectionID(collectionID)
			if err != nil {
				// this case should not happen
				// deltaChannelInfos should have been set to meta before executing child tasks
				log.Error("loadSegments: failed to get deltaChannelInfo from meta", zap.Error(err))
				return err
			}
			deltaChannel2Info := make(map[string]*datapb.VchannelInfo, len(deltaChannelInfos))
			for _, info := range deltaChannelInfos {
				deltaChannel2Info[info.ChannelName] = info
			}

			// check delta channel which should be reloaded
			reloadDeltaChannels := make(map[string]struct{})
			for _, segment := range in.Infos {
				// convert vChannel to deltaChannel
				deltaChannelName, err := funcutil.ConvertChannelName(segment.InsertChannel, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
				if err != nil {
					return err
				}

				reloadDeltaChannels[deltaChannelName] = struct{}{}
			}

			in.DeltaPositions = make([]*internalpb.MsgPosition, 0)
			for deltaChannelName := range reloadDeltaChannels {
				if info, ok := deltaChannel2Info[deltaChannelName]; ok {
					in.DeltaPositions = append(in.DeltaPositions, info.SeekPosition)
				} else {
					// this case should not happen
					err = fmt.Errorf("loadSegments: can't find deltaChannelInfo, channel name = %s", deltaChannelName)
					log.Error(err.Error())
					return err
				}
			}
		}

		err := targetNode.loadSegments(ctx, in)
		if err != nil {
			log.Warn("loadSegments: queryNode load segments error", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}

		return nil
	}
	return fmt.Errorf("loadSegments: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) ReleaseSegments(ctx context.Context, leaderID int64, in *querypb.ReleaseSegmentsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[leaderID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		if !targetNode.isOnline() {
			return errors.New("node offline")
		}

		err := targetNode.releaseSegments(ctx, in)
		if err != nil {
			log.Warn("releaseSegments: queryNode release segments error", zap.Int64("leaderID", leaderID), zap.Int64("nodeID", in.NodeID), zap.String("error info", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("releaseSegments: can't find QueryNode by nodeID, nodeID = %d", leaderID)
}

func (c *queryNodeCluster) WatchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.watchDmChannels(ctx, in)
		if err != nil {
			return err
		}

		dmChannelWatchInfo := make([]*querypb.DmChannelWatchInfo, len(in.Infos))
		for index, info := range in.Infos {
			nodes := []UniqueID{nodeID}

			old, ok := c.clusterMeta.getDmChannel(info.ChannelName)
			if ok {
				nodes = append(nodes, old.NodeIds...)
			}

			dmChannelWatchInfo[index] = &querypb.DmChannelWatchInfo{
				CollectionID: info.CollectionID,
				DmChannel:    info.ChannelName,
				ReplicaID:    in.ReplicaID,
				NodeIds:      nodes,
			}
		}

		err = c.clusterMeta.setDmChannelInfos(dmChannelWatchInfo...)
		if err != nil {
			// TODO DML channel maybe leaked, need to release dml if no related segment
			return err
		}

		return nil
	}
	return fmt.Errorf("watchDmChannels: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) WatchDeltaChannels(ctx context.Context, nodeID int64, in *querypb.WatchDeltaChannelsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.watchDeltaChannels(ctx, in)
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("watchDeltaChannels: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) HasWatchedDeltaChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool {
	c.RLock()
	defer c.RUnlock()

	return c.nodes[nodeID].hasWatchedDeltaChannel(collectionID)
}

func (c *queryNodeCluster) ReleaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.releaseCollection(ctx, in)
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("releaseCollection: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) ReleasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.releasePartitions(ctx, in)
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("releasePartitions: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) GetSegmentInfoByID(ctx context.Context, segmentID UniqueID) (*querypb.SegmentInfo, error) {
	segmentInfo, err := c.clusterMeta.getSegmentInfoByID(segmentID)
	if err != nil {
		return nil, err
	}

	c.RLock()
	targetNode, ok := c.nodes[segmentInfo.NodeID]
	c.RUnlock()
	if !ok {
		return nil, fmt.Errorf("updateSegmentInfo: can't find query node by nodeID, nodeID = %d", segmentInfo.NodeID)
	}

	res, err := targetNode.getSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SegmentInfo,
		},
		CollectionID: segmentInfo.CollectionID,
	})
	if err != nil {
		return nil, err
	}

	// protobuf convention, it's ok to call GetXXX on nil
	for _, info := range res.GetInfos() {
		if info.GetSegmentID() == segmentID {
			return info, nil
		}
	}

	return nil, fmt.Errorf("updateSegmentInfo: can't find segment %d on QueryNode %d", segmentID, segmentInfo.NodeID)
}

func (c *queryNodeCluster) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	type respTuple struct {
		res *querypb.GetSegmentInfoResponse
		err error
	}

	var (
		segmentInfos []*querypb.SegmentInfo
	)

	// Fetch sealed segments from Meta
	if len(in.SegmentIDs) > 0 {
		for _, segmentID := range in.SegmentIDs {
			segment, err := c.clusterMeta.getSegmentInfoByID(segmentID)
			if err != nil {
				return nil, err
			}

			segmentInfos = append(segmentInfos, segment)
		}
	} else {
		allSegments := c.clusterMeta.showSegmentInfos(in.CollectionID, nil)
		for _, segment := range allSegments {
			if in.CollectionID == 0 || segment.CollectionID == in.CollectionID {
				segmentInfos = append(segmentInfos, segment)
			}
		}
	}

	// Fetch growing segments
	c.RLock()
	var wg sync.WaitGroup
	cnt := len(c.nodes)
	resChan := make(chan respTuple, cnt)
	wg.Add(cnt)
	for _, node := range c.nodes {
		go func(node Node) {
			defer wg.Done()
			res, err := node.getSegmentInfo(ctx, in)
			resChan <- respTuple{
				res: res,
				err: err,
			}
		}(node)
	}
	c.RUnlock()
	wg.Wait()
	close(resChan)

	for tuple := range resChan {
		if tuple.err != nil {
			return nil, tuple.err
		}

		segments := tuple.res.GetInfos()
		for _, segment := range segments {
			if segment.SegmentState != commonpb.SegmentState_Sealed {
				segmentInfos = append(segmentInfos, segment)
			}
		}
	}

	//TODO::update meta
	return segmentInfos, nil
}

func (c *queryNodeCluster) GetSegmentInfoByNode(ctx context.Context, nodeID int64, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	c.RLock()
	node, ok := c.nodes[nodeID]
	c.RUnlock()

	if !ok {
		return nil, fmt.Errorf("getSegmentInfoByNode: can't find query node by nodeID, nodeID = %d", nodeID)
	}
	res, err := node.getSegmentInfo(ctx, in)
	if err != nil {
		return nil, err
	}
	return res.GetInfos(), nil
}

func (c *queryNodeCluster) SyncReplicaSegments(ctx context.Context, leaderID UniqueID, in *querypb.SyncReplicaSegmentsRequest) error {
	c.RLock()
	leader, ok := c.nodes[leaderID]
	c.RUnlock()

	if !ok {
		return fmt.Errorf("syncReplicaSegments: can't find leader query node, leaderID = %d", leaderID)
	}
	return leader.syncReplicaSegments(ctx, in)
}

type queryNodeGetMetricsResponse struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

func (c *queryNodeCluster) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse {
	c.RLock()
	var wg sync.WaitGroup
	cnt := len(c.nodes)
	wg.Add(cnt)
	respChan := make(chan queryNodeGetMetricsResponse, cnt)
	for _, node := range c.nodes {
		go func(node Node) {
			defer wg.Done()
			resp, err := node.getMetrics(ctx, in)
			respChan <- queryNodeGetMetricsResponse{
				resp: resp,
				err:  err,
			}
		}(node)
	}
	c.RUnlock()

	wg.Wait()
	close(respChan)

	ret := make([]queryNodeGetMetricsResponse, 0, cnt)
	for res := range respChan {
		ret = append(ret, res)
	}

	return ret
}

// setNodeState update queryNode state, which may be offline, disconnect, online
// when queryCoord restart, it will call setNodeState via the registerNode function
// when the new queryNode starts, queryCoord calls setNodeState via the registerNode function
// when the new queryNode down, queryCoord calls setNodeState via the stopNode function
func (c *queryNodeCluster) setNodeState(nodeID int64, node Node, state nodeState) {
	// if query node down, should unsubscribe all channel the node has watched
	// if not unsubscribe channel, may result in pulsar having too many backlogs
	if state == offline {
		// 1. find all the search/dmChannel/deltaChannel the node has watched
		unsubscribeChannelInfo := c.clusterMeta.getWatchedChannelsByNodeID(nodeID)

		// 2.add unsubscribed channels to handler, handler will auto unsubscribe channel
		if len(unsubscribeChannelInfo.CollectionChannels) != 0 {
			c.handler.addUnsubscribeChannelInfo(unsubscribeChannelInfo)
		}
	}

	node.setState(state)
}

func (c *queryNodeCluster) RegisterNode(ctx context.Context, session *sessionutil.Session, id UniqueID, state nodeState) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[id]; !ok {
		sessionJSON, err := json.Marshal(session)
		if err != nil {
			log.Warn("registerNode: marshal session error", zap.Int64("nodeID", id), zap.Any("address", session))
			return err
		}
		key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, id)
		err = c.client.Save(key, string(sessionJSON))
		if err != nil {
			return err
		}
		node, err := c.newNodeFn(ctx, session.Address, id, c.client)
		if err != nil {
			log.Warn("registerNode: create a new QueryNode failed", zap.Int64("nodeID", id), zap.Error(err))
			return err
		}
		c.setNodeState(id, node, state)
		if state < online {
			go node.start()
		}
		c.nodes[id] = node
		metrics.QueryCoordNumQueryNodes.WithLabelValues().Inc()
		log.Info("registerNode: create a new QueryNode", zap.Int64("nodeID", id), zap.String("address", session.Address), zap.Any("state", state))
		return nil
	}
	return fmt.Errorf("registerNode: QueryNode %d alredy exists in cluster", id)
}

func (c *queryNodeCluster) GetNodeInfoByID(nodeID int64) (Node, error) {
	c.RLock()
	node, ok := c.nodes[nodeID]
	c.RUnlock()
	if !ok {
		return nil, fmt.Errorf("getNodeInfoByID: QueryNode %d not exist", nodeID)
	}

	nodeInfo, err := node.getNodeInfo()
	if err != nil {
		return nil, err
	}
	return nodeInfo, nil
}

func (c *queryNodeCluster) RemoveNodeInfo(nodeID int64) error {
	c.Lock()
	defer c.Unlock()

	key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, nodeID)
	err := c.client.Remove(key)
	if err != nil {
		return err
	}

	delete(c.nodes, nodeID)
	metrics.QueryCoordNumQueryNodes.WithLabelValues().Dec()
	log.Info("removeNodeInfo: delete nodeInfo in cluster MetaReplica", zap.Int64("nodeID", nodeID))

	return nil
}

func (c *queryNodeCluster) StopNode(nodeID int64) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		c.setNodeState(nodeID, node, offline)
		node.stop()
		log.Info("stopNode: queryNode offline", zap.Int64("nodeID", nodeID))
	}
}

func (c *queryNodeCluster) OnlineNodeIDs() []int64 {
	c.RLock()
	defer c.RUnlock()

	var onlineNodeIDs []int64
	for nodeID, node := range c.nodes {
		if node.isOnline() {
			onlineNodeIDs = append(onlineNodeIDs, nodeID)
		}
	}

	return onlineNodeIDs
}

func (c *queryNodeCluster) OfflineNodeIDs() []int64 {
	c.RLock()
	defer c.RUnlock()

	var offlineNodeIDs []int64
	for nodeID, node := range c.nodes {
		if node.isOffline() {
			offlineNodeIDs = append(offlineNodeIDs, nodeID)
		}
	}

	return offlineNodeIDs
}

func (c *queryNodeCluster) HasNode(nodeID int64) bool {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.nodes[nodeID]; ok {
		return true
	}

	return false
}

func (c *queryNodeCluster) IsOnline(nodeID int64) (bool, error) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		return node.isOnline(), nil
	}

	return false, fmt.Errorf("isOnline: QueryNode %d not exist", nodeID)
}

//func (c *queryNodeCluster) printMeta() {
//	c.RLock()
//	defer c.RUnlock()
//
//	for id, node := range c.nodes {
//		if node.isOnline() {
//			collectionInfos := node.showCollections()
//			for _, info := range collectionInfos {
//				log.Debug("PrintMeta: query coordinator cluster info: collectionInfo", zap.Int64("nodeID", id), zap.Int64("collectionID", info.CollectionID), zap.Any("info", info))
//			}
//
//		}
//	}
//}

func (c *queryNodeCluster) AllocateSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error {
	return c.segmentAllocator(ctx, reqs, c, c.clusterMeta, wait, excludeNodeIDs, includeNodeIDs, replicaID)
}

func (c *queryNodeCluster) AllocateChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error {
	return c.channelAllocator(ctx, reqs, c, c.clusterMeta, wait, excludeNodeIDs, includeNodeIDs, replicaID)
}

// Return error if no enough nodes/resources to create replicas
func (c *queryNodeCluster) AssignNodesToReplicas(ctx context.Context, replicas []*milvuspb.ReplicaInfo, collectionSize uint64) error {
	nodeIds := c.OnlineNodeIDs()
	if len(nodeIds) < len(replicas) {
		return fmt.Errorf("no enough nodes to create replicas, node_num=%d replica_num=%d", len(nodeIds), len(replicas))
	}

	nodeInfos := getNodeInfos(c, nodeIds)
	if len(nodeInfos) < len(replicas) {
		return fmt.Errorf("no enough nodes to create replicas, node_num=%d replica_num=%d", len(nodeInfos), len(replicas))
	}

	sort.Slice(nodeInfos, func(i, j int) bool {
		return nodeInfos[i].totalMem-nodeInfos[i].memUsage > nodeInfos[j].totalMem-nodeInfos[j].memUsage
	})

	memCapCount := make([]uint64, len(replicas))
	for _, info := range nodeInfos {
		i := 0
		minMemCap := uint64(math.MaxUint64)
		for j, memCap := range memCapCount {
			if memCap < minMemCap {
				minMemCap = memCap
				i = j
			}
		}

		replicas[i].NodeIds = append(replicas[i].NodeIds, info.id)
		memCapCount[i] += info.totalMem - info.memUsage
	}

	for _, memCap := range memCapCount {
		if memCap < collectionSize {
			return fmt.Errorf("no enough memory to load collection/partitions, collectionSize=%v, replicasNum=%v", collectionSize, len(replicas))
		}
	}

	return nil
}

// It's a helper method to concurrently get nodes' info
// Remove nodes that it can't connect to
func getNodeInfos(cluster Cluster, nodeIds []UniqueID) []*queryNode {
	nodeCh := make(chan *queryNode, len(nodeIds))

	wg := sync.WaitGroup{}
	for _, id := range nodeIds {
		wg.Add(1)
		go func(id UniqueID) {
			defer wg.Done()
			info, err := cluster.GetNodeInfoByID(id)
			if err != nil {
				return
			}
			nodeCh <- info.(*queryNode)
		}(id)
	}
	wg.Wait()
	close(nodeCh)

	nodes := make([]*queryNode, 0, len(nodeCh))
	for node := range nodeCh {
		nodes = append(nodes, node)
	}

	return nodes
}
