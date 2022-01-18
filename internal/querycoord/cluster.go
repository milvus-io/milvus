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
	"path/filepath"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

// Cluster manages all query node connections and grpc requests
type Cluster interface {
	reloadFromKV() error
	getComponentInfos(ctx context.Context) []*internalpb.ComponentInfo

	loadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error
	releaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) error

	watchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error
	watchDeltaChannels(ctx context.Context, nodeID int64, in *querypb.WatchDeltaChannelsRequest) error

	hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool
	hasWatchedDeltaChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool
	addQueryChannel(ctx context.Context, nodeID int64, in *querypb.AddQueryChannelRequest) error
	removeQueryChannel(ctx context.Context, nodeID int64, in *querypb.RemoveQueryChannelRequest) error
	releaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error
	releasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error
	getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error)
	getSegmentInfoByNode(ctx context.Context, nodeID int64, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error)
	getSegmentInfoByID(ctx context.Context, segmentID UniqueID) (*querypb.SegmentInfo, error)

	registerNode(ctx context.Context, session *sessionutil.Session, id UniqueID, state nodeState) error
	getNodeInfoByID(nodeID int64) (Node, error)
	removeNodeInfo(nodeID int64) error
	stopNode(nodeID int64)
	onlineNodeIDs() []int64
	isOnline(nodeID int64) (bool, error)
	offlineNodeIDs() []int64
	hasNode(nodeID int64) bool

	allocateSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error
	allocateChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, wait bool, excludeNodeIDs []int64) error

	getSessionVersion() int64

	getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse
	estimateSegmentsSize(segments *querypb.LoadSegmentsRequest) (int64, error)
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
	dataKV kv.DataKV

	session        *sessionutil.Session
	sessionVersion int64

	sync.RWMutex
	clusterMeta      Meta
	handler          *channelUnsubscribeHandler
	nodes            map[int64]Node
	newNodeFn        newQueryNodeFn
	segmentAllocator SegmentAllocatePolicy
	channelAllocator ChannelAllocatePolicy
	segSizeEstimator func(request *querypb.LoadSegmentsRequest, dataKV kv.DataKV) (int64, error)
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
		segSizeEstimator: defaultSegEstimatePolicy(),
	}
	err := c.reloadFromKV()
	if err != nil {
		return nil, err
	}

	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	c.dataKV, err = minioKV.NewMinIOKV(ctx, option)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Reload trigger task, trigger task states, internal task, internal task state from etcd
// Assign the internal task to the corresponding trigger task as a child task
func (c *queryNodeCluster) reloadFromKV() error {
	toLoadMetaNodeIDs := make([]int64, 0)
	// get current online session
	onlineNodeSessions, version, _ := c.session.GetSessions(typeutil.QueryNodeRole)
	onlineSessionMap := make(map[int64]*sessionutil.Session)
	for _, session := range onlineNodeSessions {
		nodeID := session.ServerID
		onlineSessionMap[nodeID] = session
	}
	for nodeID, session := range onlineSessionMap {
		log.Debug("reloadFromKV: register a queryNode to cluster", zap.Any("nodeID", nodeID))
		err := c.registerNode(c.ctx, session, nodeID, disConnect)
		if err != nil {
			log.Error("QueryNode failed to register", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}
		toLoadMetaNodeIDs = append(toLoadMetaNodeIDs, nodeID)
	}
	c.sessionVersion = version

	// load node information before power off from etcd
	oldStringNodeIDs, oldNodeSessions, err := c.client.LoadWithPrefix(queryNodeInfoPrefix)
	if err != nil {
		log.Error("reloadFromKV: get previous node info from etcd error", zap.Error(err))
		return err
	}
	for index := range oldStringNodeIDs {
		nodeID, err := strconv.ParseInt(filepath.Base(oldStringNodeIDs[index]), 10, 64)
		if err != nil {
			log.Error("watchNodeLoop: parse nodeID error", zap.Error(err))
			return err
		}
		if _, ok := onlineSessionMap[nodeID]; !ok {
			session := &sessionutil.Session{}
			err = json.Unmarshal([]byte(oldNodeSessions[index]), session)
			if err != nil {
				log.Error("watchNodeLoop: unmarshal session error", zap.Error(err))
				return err
			}
			err = c.registerNode(context.Background(), session, nodeID, offline)
			if err != nil {
				log.Debug("reloadFromKV: failed to add queryNode to cluster", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
				return err
			}
			toLoadMetaNodeIDs = append(toLoadMetaNodeIDs, nodeID)
		}
	}

	return nil
}

func (c *queryNodeCluster) getSessionVersion() int64 {
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

func (c *queryNodeCluster) loadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.loadSegments(ctx, in)
		if err != nil {
			log.Debug("loadSegments: queryNode load segments error", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}

		return nil
	}
	return fmt.Errorf("loadSegments: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) releaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		if !targetNode.isOnline() {
			return errors.New("node offline")
		}

		err := targetNode.releaseSegments(ctx, in)
		if err != nil {
			log.Debug("releaseSegments: queryNode release segments error", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("releaseSegments: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) watchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.watchDmChannels(ctx, in)
		if err != nil {
			log.Debug("watchDmChannels: queryNode watch dm channel error", zap.String("error", err.Error()))
			return err
		}
		dmChannelWatchInfo := make([]*querypb.DmChannelWatchInfo, len(in.Infos))
		for index, info := range in.Infos {
			dmChannelWatchInfo[index] = &querypb.DmChannelWatchInfo{
				CollectionID: info.CollectionID,
				DmChannel:    info.ChannelName,
				NodeIDLoaded: nodeID,
			}
		}

		err = c.clusterMeta.setDmChannelInfos(dmChannelWatchInfo)
		if err != nil {
			log.Debug("watchDmChannels: update dmChannelWatchInfos to meta failed", zap.String("error", err.Error()))
			return err
		}

		return nil
	}
	return fmt.Errorf("watchDmChannels: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) watchDeltaChannels(ctx context.Context, nodeID int64, in *querypb.WatchDeltaChannelsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.watchDeltaChannels(ctx, in)
		if err != nil {
			log.Debug("watchDeltaChannels: queryNode watch delta channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("watchDeltaChannels: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) hasWatchedDeltaChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool {
	c.RLock()
	defer c.RUnlock()

	return c.nodes[nodeID].hasWatchedDeltaChannel(collectionID)
}

func (c *queryNodeCluster) hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool {
	c.RLock()
	defer c.RUnlock()

	return c.nodes[nodeID].hasWatchedQueryChannel(collectionID)
}

func (c *queryNodeCluster) addQueryChannel(ctx context.Context, nodeID int64, in *querypb.AddQueryChannelRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		emptyChangeInfo := &querypb.SealedSegmentsChangeInfo{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SealedSegmentsChangeInfo,
			},
		}
		msgPosition, err := c.clusterMeta.sendSealedSegmentChangeInfos(in.CollectionID, in.QueryChannel, emptyChangeInfo)
		if err != nil {
			log.Error("addQueryChannel: get latest messageID of query channel error", zap.String("queryChannel", in.QueryChannel), zap.Error(err))
			return err
		}

		// update watch position to latest
		in.SeekPosition = msgPosition
		err = targetNode.addQueryChannel(ctx, in)
		if err != nil {
			log.Error("addQueryChannel: queryNode add query channel error", zap.String("queryChannel", in.QueryChannel), zap.Error(err))
			return err
		}
		return nil
	}

	return fmt.Errorf("addQueryChannel: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}
func (c *queryNodeCluster) removeQueryChannel(ctx context.Context, nodeID int64, in *querypb.RemoveQueryChannelRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.removeQueryChannel(ctx, in)
		if err != nil {
			log.Debug("removeQueryChannel: queryNode remove query channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("removeQueryChannel: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) releaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.releaseCollection(ctx, in)
		if err != nil {
			log.Debug("releaseCollection: queryNode release collection error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("releaseCollection: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) releasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error {
	c.RLock()
	var targetNode Node
	if node, ok := c.nodes[nodeID]; ok {
		targetNode = node
	}
	c.RUnlock()

	if targetNode != nil {
		err := targetNode.releasePartitions(ctx, in)
		if err != nil {
			log.Debug("releasePartitions: queryNode release partitions error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("releasePartitions: can't find QueryNode by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) getSegmentInfoByID(ctx context.Context, segmentID UniqueID) (*querypb.SegmentInfo, error) {
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

func (c *queryNodeCluster) getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {

	type respTuple struct {
		res *querypb.GetSegmentInfoResponse
		err error
	}

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
	var segmentInfos []*querypb.SegmentInfo

	for tuple := range resChan {
		if tuple.err != nil {
			return nil, tuple.err
		}
		segmentInfos = append(segmentInfos, tuple.res.GetInfos()...)
	}

	//TODO::update meta
	return segmentInfos, nil
}

func (c *queryNodeCluster) getSegmentInfoByNode(ctx context.Context, nodeID int64, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
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

type queryNodeGetMetricsResponse struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

func (c *queryNodeCluster) getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse {
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

func (c *queryNodeCluster) registerNode(ctx context.Context, session *sessionutil.Session, id UniqueID, state nodeState) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[id]; !ok {
		sessionJSON, err := json.Marshal(session)
		if err != nil {
			log.Debug("registerNode: marshal session error", zap.Int64("nodeID", id), zap.Any("address", session))
			return err
		}
		key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, id)
		err = c.client.Save(key, string(sessionJSON))
		if err != nil {
			return err
		}
		node, err := c.newNodeFn(ctx, session.Address, id, c.client)
		if err != nil {
			log.Debug("registerNode: create a new QueryNode failed", zap.Int64("nodeID", id), zap.Error(err))
			return err
		}
		c.setNodeState(id, node, state)
		if state < online {
			go node.start()
		}
		c.nodes[id] = node
		log.Debug("registerNode: create a new QueryNode", zap.Int64("nodeID", id), zap.String("address", session.Address), zap.Any("state", state))
		return nil
	}
	return fmt.Errorf("registerNode: QueryNode %d alredy exists in cluster", id)
}

func (c *queryNodeCluster) getNodeInfoByID(nodeID int64) (Node, error) {
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

func (c *queryNodeCluster) removeNodeInfo(nodeID int64) error {
	c.Lock()
	defer c.Unlock()

	key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, nodeID)
	err := c.client.Remove(key)
	if err != nil {
		return err
	}

	delete(c.nodes, nodeID)
	log.Debug("removeNodeInfo: delete nodeInfo in cluster MetaReplica", zap.Int64("nodeID", nodeID))

	return nil
}

func (c *queryNodeCluster) stopNode(nodeID int64) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		node.stop()
		c.setNodeState(nodeID, node, offline)
		log.Debug("stopNode: queryNode offline", zap.Int64("nodeID", nodeID))
	}
}

func (c *queryNodeCluster) onlineNodeIDs() []int64 {
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

func (c *queryNodeCluster) offlineNodeIDs() []int64 {
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

func (c *queryNodeCluster) hasNode(nodeID int64) bool {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.nodes[nodeID]; ok {
		return true
	}

	return false
}

func (c *queryNodeCluster) isOnline(nodeID int64) (bool, error) {
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
//			queryChannelInfos := node.showWatchedQueryChannels()
//			for _, info := range queryChannelInfos {
//				log.Debug("PrintMeta: query coordinator cluster info: watchedQueryChannelInfo", zap.Int64("nodeID", id), zap.Int64("collectionID", info.CollectionID), zap.Any("info", info))
//			}
//		}
//	}
//}

func (c *queryNodeCluster) allocateSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error {
	return c.segmentAllocator(ctx, reqs, c, c.clusterMeta, wait, excludeNodeIDs, includeNodeIDs)
}

func (c *queryNodeCluster) allocateChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, wait bool, excludeNodeIDs []int64) error {
	return c.channelAllocator(ctx, reqs, c, c.clusterMeta, wait, excludeNodeIDs)
}

func (c *queryNodeCluster) estimateSegmentsSize(segments *querypb.LoadSegmentsRequest) (int64, error) {
	return c.segSizeEstimator(segments, c.dataKV)
}

func defaultSegEstimatePolicy() segEstimatePolicy {
	return estimateSegmentsSize
}

type segEstimatePolicy func(request *querypb.LoadSegmentsRequest, dataKv kv.DataKV) (int64, error)

func estimateSegmentsSize(segments *querypb.LoadSegmentsRequest, kvClient kv.DataKV) (int64, error) {
	requestSize := int64(0)
	for _, loadInfo := range segments.Infos {
		segmentSize := int64(0)
		// get which field has index file
		vecFieldIndexInfo := make(map[int64]*querypb.VecFieldIndexInfo)
		for _, indexInfo := range loadInfo.IndexInfos {
			if indexInfo.EnableIndex {
				fieldID := indexInfo.FieldID
				vecFieldIndexInfo[fieldID] = indexInfo
			}
		}

		for _, binlogPath := range loadInfo.BinlogPaths {
			fieldID := binlogPath.FieldID
			// if index node has built index, cal segment size by index file size, or use raw data's binlog size
			if indexInfo, ok := vecFieldIndexInfo[fieldID]; ok {
				segmentSize += indexInfo.IndexSize
			} else {
				for _, binlog := range binlogPath.Binlogs {
					segmentSize += binlog.GetLogSize()
				}
			}
		}
		loadInfo.SegmentSize = segmentSize
		requestSize += segmentSize
	}

	return requestSize, nil
}
