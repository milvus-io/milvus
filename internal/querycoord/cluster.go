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

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	queryNodeMetaPrefix = "queryCoord-queryNodeMeta"
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

// Cluster manages all query node connections and grpc requests
type Cluster interface {
	reloadFromKV() error
	getComponentInfos(ctx context.Context) ([]*internalpb.ComponentInfo, error)

	loadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error
	releaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) error
	getNumSegments(nodeID int64) (int, error)

	watchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error
	watchDeltaChannels(ctx context.Context, nodeID int64, in *querypb.WatchDeltaChannelsRequest) error
	//TODO:: removeDmChannel
	getNumDmChannels(nodeID int64) (int, error)

	hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool
	hasWatchedDeltaChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool
	getCollectionInfosByID(ctx context.Context, nodeID int64) []*querypb.CollectionInfo
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
	onlineNodes() (map[int64]Node, error)
	isOnline(nodeID int64) (bool, error)
	offlineNodes() (map[int64]Node, error)
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
	nodes            map[int64]Node
	newNodeFn        newQueryNodeFn
	segmentAllocator SegmentAllocatePolicy
	channelAllocator ChannelAllocatePolicy
	segSizeEstimator func(request *querypb.LoadSegmentsRequest, dataKV kv.DataKV) (int64, error)
}

func newQueryNodeCluster(ctx context.Context, clusterMeta Meta, kv *etcdkv.EtcdKV, newNodeFn newQueryNodeFn, session *sessionutil.Session) (Cluster, error) {
	childCtx, cancel := context.WithCancel(ctx)
	nodes := make(map[int64]Node)
	c := &queryNodeCluster{
		ctx:              childCtx,
		cancel:           cancel,
		client:           kv,
		session:          session,
		clusterMeta:      clusterMeta,
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
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
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
			log.Error("query node failed to register", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
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

	// load collection meta of queryNode from etcd
	for _, nodeID := range toLoadMetaNodeIDs {
		infoPrefix := fmt.Sprintf("%s/%d", queryNodeMetaPrefix, nodeID)
		_, collectionValues, err := c.client.LoadWithPrefix(infoPrefix)
		if err != nil {
			return err
		}
		for _, value := range collectionValues {
			collectionInfo := &querypb.CollectionInfo{}
			err = proto.Unmarshal([]byte(value), collectionInfo)
			if err != nil {
				return err
			}
			err = c.nodes[nodeID].setCollectionInfo(collectionInfo)
			if err != nil {
				log.Debug("reloadFromKV: failed to add queryNode meta to cluster", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
				return err
			}
			log.Debug("reloadFromKV: reload collection info from etcd", zap.Any("info", collectionInfo))
		}
	}
	return nil
}

func (c *queryNodeCluster) getSessionVersion() int64 {
	return c.sessionVersion
}

func (c *queryNodeCluster) getComponentInfos(ctx context.Context) ([]*internalpb.ComponentInfo, error) {
	c.RLock()
	defer c.RUnlock()
	subComponentInfos := make([]*internalpb.ComponentInfo, 0)
	nodes, err := c.getOnlineNodes()
	if err != nil {
		log.Debug("getComponentInfos: failed get on service nodes", zap.String("error info", err.Error()))
		return nil, err
	}
	for _, node := range nodes {
		componentState := node.getComponentInfo(ctx)
		subComponentInfos = append(subComponentInfos, componentState)
	}

	return subComponentInfos, nil
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
	return fmt.Errorf("loadSegments: can't find query node by nodeID, nodeID = %d", nodeID)
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

	return fmt.Errorf("releaseSegments: can't find query node by nodeID, nodeID = %d", nodeID)
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
		channels := make([]string, 0)
		for _, info := range in.Infos {
			channels = append(channels, info.ChannelName)
		}

		collectionID := in.CollectionID
		err = c.clusterMeta.addDmChannel(collectionID, nodeID, channels)
		if err != nil {
			log.Debug("watchDmChannels: queryNode watch dm channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}
	return fmt.Errorf("watchDmChannels: can't find query node by nodeID, nodeID = %d", nodeID)
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
		err = c.clusterMeta.setDeltaChannel(in.CollectionID, in.Infos)
		if err != nil {
			log.Debug("watchDeltaChannels: queryNode watch delta channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return fmt.Errorf("watchDeltaChannels: can't find query node by nodeID, nodeID = %d", nodeID)
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
		err := targetNode.addQueryChannel(ctx, in)
		if err != nil {
			log.Debug("addQueryChannel: queryNode add query channel error", zap.String("error", err.Error()))
			return err
		}
		return nil
	}

	return fmt.Errorf("addQueryChannel: can't find query node by nodeID, nodeID = %d", nodeID)
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

	return fmt.Errorf("removeQueryChannel: can't find query node by nodeID, nodeID = %d", nodeID)
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
		err = c.clusterMeta.releaseCollection(in.CollectionID)
		if err != nil {
			log.Debug("releaseCollection: meta release collection error", zap.String("error", err.Error()))
			return err
		}
		return nil
	}

	return fmt.Errorf("releaseCollection: can't find query node by nodeID, nodeID = %d", nodeID)
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

		for _, partitionID := range in.PartitionIDs {
			err = c.clusterMeta.releasePartition(in.CollectionID, partitionID)
			if err != nil {
				log.Debug("releasePartitions: meta release partitions error", zap.String("error", err.Error()))
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("releasePartitions: can't find query node by nodeID, nodeID = %d", nodeID)
}

func (c *queryNodeCluster) getSegmentInfoByID(ctx context.Context, segmentID UniqueID) (*querypb.SegmentInfo, error) {
	c.RLock()
	defer c.RUnlock()

	segmentInfo, err := c.clusterMeta.getSegmentInfoByID(segmentID)
	if err != nil {
		return nil, err
	}
	if node, ok := c.nodes[segmentInfo.NodeID]; ok {
		res, err := node.getSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: segmentInfo.CollectionID,
		})
		if err != nil {
			return nil, err
		}
		if res != nil {
			for _, info := range res.Infos {
				if info.SegmentID == segmentID {
					return info, nil
				}
			}
		}
		return nil, fmt.Errorf("updateSegmentInfo: can't find segment %d on query node %d", segmentID, segmentInfo.NodeID)
	}

	return nil, fmt.Errorf("updateSegmentInfo: can't find query node by nodeID, nodeID = %d", segmentInfo.NodeID)
}

func (c *queryNodeCluster) getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	c.RLock()
	defer c.RUnlock()

	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, node := range c.nodes {
		res, err := node.getSegmentInfo(ctx, in)
		if err != nil {
			return nil, err
		}
		if res != nil {
			segmentInfos = append(segmentInfos, res.Infos...)
		}
	}

	//TODO::update meta
	return segmentInfos, nil
}

func (c *queryNodeCluster) getSegmentInfoByNode(ctx context.Context, nodeID int64, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		res, err := node.getSegmentInfo(ctx, in)
		if err != nil {
			return nil, err
		}
		return res.Infos, nil
	}

	return nil, fmt.Errorf("getSegmentInfoByNode: can't find query node by nodeID, nodeID = %d", nodeID)
}

type queryNodeGetMetricsResponse struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

func (c *queryNodeCluster) getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse {
	c.RLock()
	defer c.RUnlock()

	ret := make([]queryNodeGetMetricsResponse, 0, len(c.nodes))
	for _, node := range c.nodes {
		resp, err := node.getMetrics(ctx, in)
		ret = append(ret, queryNodeGetMetricsResponse{
			resp: resp,
			err:  err,
		})
	}

	return ret
}

func (c *queryNodeCluster) getNumDmChannels(nodeID int64) (int, error) {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.nodes[nodeID]; !ok {
		return 0, fmt.Errorf("getNumDmChannels: can't find query node by nodeID, nodeID = %d", nodeID)
	}

	numChannel := 0
	collectionInfos := c.clusterMeta.showCollections()
	for _, info := range collectionInfos {
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
				numChannel++
			}
		}
	}
	return numChannel, nil
}

func (c *queryNodeCluster) getNumSegments(nodeID int64) (int, error) {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.nodes[nodeID]; !ok {
		return 0, fmt.Errorf("getNumSegments: can't find query node by nodeID, nodeID = %d", nodeID)
	}

	numSegment := 0
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	collectionInfos := c.clusterMeta.showCollections()
	for _, info := range collectionInfos {
		res := c.clusterMeta.showSegmentInfos(info.CollectionID, nil)
		segmentInfos = append(segmentInfos, res...)
	}
	for _, info := range segmentInfos {
		if info.NodeID == nodeID {
			numSegment++
		}
	}
	return numSegment, nil
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
			log.Debug("registerNode: create a new query node failed", zap.Int64("nodeID", id), zap.Error(err))
			return err
		}
		node.setState(state)
		if state < online {
			go node.start()
		}
		c.nodes[id] = node
		log.Debug("registerNode: create a new query node", zap.Int64("nodeID", id), zap.String("address", session.Address))
		return nil
	}
	return fmt.Errorf("registerNode: node %d alredy exists in cluster", id)
}

func (c *queryNodeCluster) getNodeInfoByID(nodeID int64) (Node, error) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		nodeInfo, err := node.getNodeInfo()
		if err != nil {
			return nil, err
		}
		return nodeInfo, nil
	}

	return nil, fmt.Errorf("getNodeInfoByID: query node %d not exist", nodeID)
}

func (c *queryNodeCluster) removeNodeInfo(nodeID int64) error {
	c.Lock()
	defer c.Unlock()

	key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, nodeID)
	err := c.client.Remove(key)
	if err != nil {
		return err
	}

	if _, ok := c.nodes[nodeID]; ok {
		err = c.nodes[nodeID].clearNodeInfo()
		if err != nil {
			return err
		}
		delete(c.nodes, nodeID)
		log.Debug("removeNodeInfo: delete nodeInfo in cluster MetaReplica and etcd", zap.Int64("nodeID", nodeID))
	}

	return nil
}

func (c *queryNodeCluster) stopNode(nodeID int64) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		node.stop()
		log.Debug("stopNode: queryNode offline", zap.Int64("nodeID", nodeID))
	}
}

func (c *queryNodeCluster) onlineNodes() (map[int64]Node, error) {
	c.RLock()
	defer c.RUnlock()

	return c.getOnlineNodes()
}

func (c *queryNodeCluster) getOnlineNodes() (map[int64]Node, error) {
	nodes := make(map[int64]Node)
	for nodeID, node := range c.nodes {
		if node.isOnline() {
			nodes[nodeID] = node
		}
	}
	if len(nodes) == 0 {
		return nil, errors.New("getOnlineNodes: no queryNode is alive")
	}

	return nodes, nil
}

func (c *queryNodeCluster) offlineNodes() (map[int64]Node, error) {
	c.RLock()
	defer c.RUnlock()

	return c.getOfflineNodes()
}

func (c *queryNodeCluster) hasNode(nodeID int64) bool {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.nodes[nodeID]; ok {
		return true
	}

	return false
}

func (c *queryNodeCluster) getOfflineNodes() (map[int64]Node, error) {
	nodes := make(map[int64]Node)
	for nodeID, node := range c.nodes {
		if node.isOffline() {
			nodes[nodeID] = node
		}
	}
	if len(nodes) == 0 {
		return nil, errors.New("getOfflineNodes: no queryNode is offline")
	}

	return nodes, nil
}

func (c *queryNodeCluster) isOnline(nodeID int64) (bool, error) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		return node.isOnline(), nil
	}

	return false, fmt.Errorf("isOnline: query node %d not exist", nodeID)
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

func (c *queryNodeCluster) getCollectionInfosByID(ctx context.Context, nodeID int64) []*querypb.CollectionInfo {
	c.RLock()
	defer c.RUnlock()
	if node, ok := c.nodes[nodeID]; ok {
		return node.showCollections()
	}

	return nil
}

func (c *queryNodeCluster) allocateSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error {
	return c.segmentAllocator(ctx, reqs, c, wait, excludeNodeIDs, includeNodeIDs)
}

func (c *queryNodeCluster) allocateChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, wait bool, excludeNodeIDs []int64) error {
	return c.channelAllocator(ctx, reqs, c, wait, excludeNodeIDs)
}

func (c *queryNodeCluster) estimateSegmentsSize(segments *querypb.LoadSegmentsRequest) (int64, error) {
	return c.segSizeEstimator(segments, c.dataKV)
}

func defaultSegEstimatePolicy() segEstimatePolicy {
	return estimateSegmentsSize
}

type segEstimatePolicy func(request *querypb.LoadSegmentsRequest, dataKv kv.DataKV) (int64, error)

func estimateSegmentsSize(segments *querypb.LoadSegmentsRequest, kvClient kv.DataKV) (int64, error) {
	segmentSize := int64(0)

	//TODO:: collection has multi vector field
	//vecFields := make([]int64, 0)
	//for _, field := range segments.Schema.Fields {
	//	if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
	//		vecFields = append(vecFields, field.FieldID)
	//	}
	//}
	// get fields data size, if len(indexFieldIDs) == 0, vector field would be involved in fieldBinLogs
	for _, loadInfo := range segments.Infos {
		// get index size
		if loadInfo.EnableIndex {
			for _, pathInfo := range loadInfo.IndexPathInfos {
				for _, path := range pathInfo.IndexFilePaths {
					indexSize, err := storage.EstimateMemorySize(kvClient, path)
					if err != nil {
						indexSize, err = storage.GetBinlogSize(kvClient, path)
						if err != nil {
							return 0, err
						}
					}
					segmentSize += indexSize
				}
			}
			continue
		}

		// get binlog size
		for _, binlogPath := range loadInfo.BinlogPaths {
			for _, path := range binlogPath.Binlogs {
				binlogSize, err := storage.EstimateMemorySize(kvClient, path)
				if err != nil {
					binlogSize, err = storage.GetBinlogSize(kvClient, path)
					if err != nil {
						return 0, err
					}
				}
				segmentSize += binlogSize
			}
		}
	}

	return segmentSize, nil
}
