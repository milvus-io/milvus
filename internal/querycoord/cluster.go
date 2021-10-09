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

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	//TODO:: removeDmChannel
	getNumDmChannels(nodeID int64) (int, error)

	hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool
	getCollectionInfosByID(ctx context.Context, nodeID int64) []*querypb.CollectionInfo
	addQueryChannel(ctx context.Context, nodeID int64, in *querypb.AddQueryChannelRequest) error
	removeQueryChannel(ctx context.Context, nodeID int64, in *querypb.RemoveQueryChannelRequest) error
	releaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error
	releasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error
	getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error)

	registerNode(ctx context.Context, session *sessionutil.Session, id UniqueID, state nodeState) error
	getNodeByID(nodeID int64) (Node, error)
	removeNodeInfo(nodeID int64) error
	stopNode(nodeID int64)
	onlineNodes() (map[int64]Node, error)
	isOnline(nodeID int64) (bool, error)
	offlineNodes() (map[int64]Node, error)

	getSessionVersion() int64

	getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) []queryNodeGetMetricsResponse
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
	clusterMeta Meta
	nodes       map[int64]Node
	newNodeFn   newQueryNodeFn
}

func newQueryNodeCluster(ctx context.Context, clusterMeta Meta, kv *etcdkv.EtcdKV, newNodeFn newQueryNodeFn, session *sessionutil.Session) (*queryNodeCluster, error) {
	childCtx, cancel := context.WithCancel(ctx)
	nodes := make(map[int64]Node)
	c := &queryNodeCluster{
		ctx:         childCtx,
		cancel:      cancel,
		client:      kv,
		session:     session,
		clusterMeta: clusterMeta,
		nodes:       nodes,
		newNodeFn:   newNodeFn,
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
	toLoadMetaNodeIDs := make([]int64, 0)
	// get current online session
	onlineNodeSessions, version, _ := c.session.GetSessions(typeutil.QueryNodeRole)
	onlineSessionMap := make(map[int64]*sessionutil.Session)
	for _, session := range onlineNodeSessions {
		nodeID := session.ServerID
		onlineSessionMap[nodeID] = session
	}
	for nodeID, session := range onlineSessionMap {
		log.Debug("ReloadFromKV: register a queryNode to cluster", zap.Any("nodeID", nodeID))
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
			log.Error("WatchNodeLoop: parse nodeID error", zap.Error(err))
			return err
		}
		if _, ok := onlineSessionMap[nodeID]; !ok {
			session := &sessionutil.Session{}
			err = json.Unmarshal([]byte(oldNodeSessions[index]), session)
			if err != nil {
				log.Error("WatchNodeLoop: unmarshal session error", zap.Error(err))
				return err
			}
			err = c.registerNode(context.Background(), session, nodeID, offline)
			if err != nil {
				log.Debug("ReloadFromKV: failed to add queryNode to cluster", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
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
				log.Debug("ReloadFromKV: failed to add queryNode meta to cluster", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
				return err
			}
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
		log.Debug("GetComponentInfos: failed get on service nodes", zap.String("error info", err.Error()))
		return nil, err
	}
	for _, node := range nodes {
		componentState := node.getComponentInfo(ctx)
		subComponentInfos = append(subComponentInfos, componentState)
	}

	return subComponentInfos, nil
}

func (c *queryNodeCluster) loadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
		for _, info := range in.Infos {
			segmentID := info.SegmentID
			segmentInfo, err := c.clusterMeta.getSegmentInfoByID(segmentID)
			if err == nil {
				segmentInfos[segmentID] = proto.Clone(segmentInfo).(*querypb.SegmentInfo)
				if in.LoadCondition != querypb.TriggerCondition_loadBalance {
					segmentInfo.SegmentState = querypb.SegmentState_sealing
					segmentInfo.NodeID = nodeID
				}
			} else {
				segmentInfo = &querypb.SegmentInfo{
					SegmentID:    segmentID,
					CollectionID: info.CollectionID,
					PartitionID:  info.PartitionID,
					NodeID:       nodeID,
					SegmentState: querypb.SegmentState_sealing,
				}
			}
			c.clusterMeta.setSegmentInfo(segmentID, segmentInfo)
		}
		err := node.loadSegments(ctx, in)
		if err != nil {
			for _, info := range in.Infos {
				segmentID := info.SegmentID
				if _, ok = segmentInfos[segmentID]; ok {
					c.clusterMeta.setSegmentInfo(segmentID, segmentInfos[segmentID])
					continue
				}
				c.clusterMeta.deleteSegmentInfoByID(segmentID)
			}
			log.Debug("LoadSegments: queryNode load segments error", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}
		return nil
	}
	return errors.New("LoadSegments: Can't find query node by nodeID ")
}

func (c *queryNodeCluster) releaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		if !node.isOnline() {
			return errors.New("node offline")
		}

		err := node.releaseSegments(ctx, in)
		if err != nil {
			log.Debug("ReleaseSegments: queryNode release segments error", zap.Int64("nodeID", nodeID), zap.String("error info", err.Error()))
			return err
		}

		for _, segmentID := range in.SegmentIDs {
			c.clusterMeta.deleteSegmentInfoByID(segmentID)
		}
		return nil
	}

	return errors.New("ReleaseSegments: Can't find query node by nodeID ")
}

func (c *queryNodeCluster) watchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		err := node.watchDmChannels(ctx, in)
		if err != nil {
			log.Debug("WatchDmChannels: queryNode watch dm channel error", zap.String("error", err.Error()))
			return err
		}
		channels := make([]string, 0)
		for _, info := range in.Infos {
			channels = append(channels, info.ChannelName)
		}

		collectionID := in.CollectionID
		//c.clusterMeta.addCollection(collectionID, in.Schema)
		err = c.clusterMeta.addDmChannel(collectionID, nodeID, channels)
		if err != nil {
			log.Debug("WatchDmChannels: queryNode watch dm channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}
	return errors.New("WatchDmChannels: Can't find query node by nodeID ")
}

func (c *queryNodeCluster) hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool {
	c.Lock()
	defer c.Unlock()

	return c.nodes[nodeID].hasWatchedQueryChannel(collectionID)
}

func (c *queryNodeCluster) addQueryChannel(ctx context.Context, nodeID int64, in *querypb.AddQueryChannelRequest) error {
	c.Lock()
	defer c.Unlock()
	if node, ok := c.nodes[nodeID]; ok {
		err := node.addQueryChannel(ctx, in)
		if err != nil {
			log.Debug("AddQueryChannel: queryNode add query channel error", zap.String("error", err.Error()))
			return err
		}
		return nil
	}

	return errors.New("AddQueryChannel: can't find query node by nodeID")
}
func (c *queryNodeCluster) removeQueryChannel(ctx context.Context, nodeID int64, in *querypb.RemoveQueryChannelRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		err := node.removeQueryChannel(ctx, in)
		if err != nil {
			log.Debug("RemoveQueryChannel: queryNode remove query channel error", zap.String("error", err.Error()))
			return err
		}

		return nil
	}

	return errors.New("RemoveQueryChannel: can't find query node by nodeID")
}

func (c *queryNodeCluster) releaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		err := node.releaseCollection(ctx, in)
		if err != nil {
			log.Debug("ReleaseCollection: queryNode release collection error", zap.String("error", err.Error()))
			return err
		}
		err = c.clusterMeta.releaseCollection(in.CollectionID)
		if err != nil {
			log.Debug("ReleaseCollection: meta release collection error", zap.String("error", err.Error()))
			return err
		}
		return nil
	}

	return errors.New("ReleaseCollection: can't find query node by nodeID")
}

func (c *queryNodeCluster) releasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) error {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		err := node.releasePartitions(ctx, in)
		if err != nil {
			log.Debug("ReleasePartitions: queryNode release partitions error", zap.String("error", err.Error()))
			return err
		}

		for _, partitionID := range in.PartitionIDs {
			err = c.clusterMeta.releasePartition(in.CollectionID, partitionID)
			if err != nil {
				log.Debug("ReleasePartitions: meta release partitions error", zap.String("error", err.Error()))
				return err
			}
		}
		return nil
	}

	return errors.New("ReleasePartitions: can't find query node by nodeID")
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
		return 0, errors.New("GetNumDmChannels: Can't find query node by nodeID ")
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
		return 0, errors.New("getNumSegments: Can't find query node by nodeID ")
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
			log.Debug("RegisterNode: marshal session error", zap.Int64("nodeID", id), zap.Any("address", session))
			return err
		}
		key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, id)
		err = c.client.Save(key, string(sessionJSON))
		if err != nil {
			return err
		}
		node, err := c.newNodeFn(ctx, session.Address, id, c.client)
		if err != nil {
			log.Debug("RegisterNode: create a new query node failed", zap.Int64("nodeID", id), zap.Error(err))
			return err
		}
		node.setState(state)
		if state < online {
			go node.start()
		}
		c.nodes[id] = node
		log.Debug("RegisterNode: create a new query node", zap.Int64("nodeID", id), zap.String("address", session.Address))
		return nil
	}
	return fmt.Errorf("RegisterNode: node %d alredy exists in cluster", id)
}

func (c *queryNodeCluster) getNodeByID(nodeID int64) (Node, error) {
	c.RLock()
	defer c.RUnlock()

	if node, ok := c.nodes[nodeID]; ok {
		return node, nil
	}

	return nil, fmt.Errorf("GetNodeByID: query node %d not exist", nodeID)
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
		log.Debug("RemoveNodeInfo: delete nodeInfo in cluster MetaReplica and etcd", zap.Int64("nodeID", nodeID))
	}

	return nil
}

func (c *queryNodeCluster) stopNode(nodeID int64) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		node.stop()
		log.Debug("StopNode: queryNode offline", zap.Int64("nodeID", nodeID))
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
		return nil, errors.New("GetOnlineNodes: no queryNode is alive")
	}

	return nodes, nil
}

func (c *queryNodeCluster) offlineNodes() (map[int64]Node, error) {
	c.RLock()
	defer c.RUnlock()

	return c.getOfflineNodes()
}

func (c *queryNodeCluster) getOfflineNodes() (map[int64]Node, error) {
	nodes := make(map[int64]Node)
	for nodeID, node := range c.nodes {
		if node.isOffline() {
			nodes[nodeID] = node
		}
	}
	if len(nodes) == 0 {
		return nil, errors.New("GetOfflineNodes: no queryNode is offline")
	}

	return nodes, nil
}

func (c *queryNodeCluster) isOnline(nodeID int64) (bool, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		return node.isOnline(), nil
	}

	return false, fmt.Errorf("IsOnService: query node %d not exist", nodeID)
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
