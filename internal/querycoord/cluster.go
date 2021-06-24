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
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

const (
	queryNodeMetaPrefix = "queryCoord-queryNodeMeta"
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

type queryNodeCluster struct {
	client *etcdkv.EtcdKV

	sync.RWMutex
	clusterMeta *meta
	nodes       map[int64]*queryNode
}

func newQueryNodeCluster(clusterMeta *meta, kv *etcdkv.EtcdKV) (*queryNodeCluster, error) {
	nodes := make(map[int64]*queryNode)
	c := &queryNodeCluster{
		client:      kv,
		clusterMeta: clusterMeta,
		nodes:       nodes,
	}
	err := c.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *queryNodeCluster) reloadFromKV() error {
	nodeIDs := make([]UniqueID, 0)
	keys, values, err := c.client.LoadWithPrefix(queryNodeInfoPrefix)
	if err != nil {
		return err
	}
	for index := range keys {
		nodeID, err := strconv.ParseInt(filepath.Base(keys[index]), 10, 64)
		if err != nil {
			return err
		}
		nodeIDs = append(nodeIDs, nodeID)
		session := &sessionutil.Session{}
		err = json.Unmarshal([]byte(values[index]), session)
		if err != nil {
			return err
		}
		err = c.RegisterNode(context.Background(), session, nodeID)
		if err != nil {
			return err
		}
	}
	for _, nodeID := range nodeIDs {
		infoPrefix := fmt.Sprintf("%s/%d", queryNodeMetaPrefix, nodeID)
		collectionKeys, collectionValues, err := c.client.LoadWithPrefix(infoPrefix)
		if err != nil {
			return err
		}
		for index := range collectionKeys {
			collectionID, err := strconv.ParseInt(filepath.Base(collectionKeys[index]), 10, 64)
			if err != nil {
				return err
			}
			collectionInfo := &querypb.CollectionInfo{}
			err = proto.UnmarshalText(collectionValues[index], collectionInfo)
			if err != nil {
				return err
			}
			c.nodes[nodeID].collectionInfos[collectionID] = collectionInfo
		}
	}
	return nil
}

func (c *queryNodeCluster) GetComponentInfos(ctx context.Context) ([]*internalpb.ComponentInfo, error) {
	c.RLock()
	defer c.RUnlock()
	subComponentInfos := make([]*internalpb.ComponentInfo, 0)
	nodeIDs, err := c.getOnServiceNodeIDs()
	if err != nil {
		return nil, err
	}
	for _, nodeID := range nodeIDs {
		node := c.nodes[nodeID]
		componentStates, err := node.client.GetComponentStates(ctx)
		if err != nil {
			subComponentInfos = append(subComponentInfos, &internalpb.ComponentInfo{
				NodeID:    nodeID,
				StateCode: internalpb.StateCode_Abnormal,
			})
			continue
		}
		subComponentInfos = append(subComponentInfos, componentStates.State)
	}

	return subComponentInfos, nil
}

func (c *queryNodeCluster) LoadSegments(ctx context.Context, nodeID int64, in *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		if !node.isOnService() {
			return nil, errors.New("node offline")
		}
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
		status, err := node.client.LoadSegments(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			for _, info := range in.Infos {
				//c.clusterMeta.addCollection(info.CollectionID, in.Schema)
				//c.clusterMeta.addPartition(info.CollectionID, info.PartitionID)

				node.addCollection(info.CollectionID, in.Schema)
				node.addPartition(info.CollectionID, info.PartitionID)
			}
		} else {
			for _, info := range in.Infos {
				segmentID := info.SegmentID
				if _, ok = segmentInfos[segmentID]; ok {
					c.clusterMeta.setSegmentInfo(segmentID, segmentInfos[segmentID])
					continue
				}
				c.clusterMeta.removeSegmentInfo(segmentID)
				c.clusterMeta.deleteSegmentInfoByID(segmentID)
			}
		}

		return status, err
	}
	return nil, errors.New("Can't find query node by nodeID ")
}

func (c *queryNodeCluster) ReleaseSegments(ctx context.Context, nodeID int64, in *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		if !node.isOnService() {
			return nil, errors.New("node offline")
		}
		for _, segmentID := range in.SegmentIDs {
			err := c.clusterMeta.removeSegmentInfo(segmentID)
			if err != nil {
				log.Error("remove segmentInfo Error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
			}
		}
		status, err := node.client.ReleaseSegments(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			for _, segmentID := range in.SegmentIDs {
				c.clusterMeta.deleteSegmentInfoByID(segmentID)
			}
		}
		return status, err
	}

	return nil, errors.New("Can't find query node by nodeID ")
}

func (c *queryNodeCluster) WatchDmChannels(ctx context.Context, nodeID int64, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		if !node.isOnService() {
			return nil, errors.New("node offline")
		}
		channels := make([]string, 0)
		for _, info := range in.Infos {
			channels = append(channels, info.ChannelName)
		}
		log.Debug("wait queryNode watch dm channel")
		status, err := node.client.WatchDmChannels(ctx, in)
		log.Debug("queryNode watch dm channel done")
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			collectionID := in.CollectionID
			//c.clusterMeta.addCollection(collectionID, in.Schema)
			//c.clusterMeta.addDmChannel(collectionID, nodeID, channels)

			node.addCollection(collectionID, in.Schema)
			node.addDmChannel(collectionID, channels)
		} else {

		}
		return status, err
	}
	return nil, errors.New("Can't find query node by nodeID ")
}

func (c *queryNodeCluster) hasWatchedQueryChannel(ctx context.Context, nodeID int64, collectionID UniqueID) bool {
	c.Lock()
	defer c.Unlock()

	return c.nodes[nodeID].hasWatchedQueryChannel(collectionID)
}

func (c *queryNodeCluster) AddQueryChannel(ctx context.Context, nodeID int64, in *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()
	if node, ok := c.nodes[nodeID]; ok {
		status, err := node.client.AddQueryChannel(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			//TODO::should reopen
			collectionID := in.CollectionID
			//collectionID := int64(0)
			if queryChannelInfo, ok := c.clusterMeta.queryChannelInfos[0]; ok {
				node.addQueryChannel(collectionID, queryChannelInfo)
				return status, err
			}
			log.Error("queryChannel for collection not assigned", zap.Int64("collectionID", collectionID))
		}
		return status, err
	}

	return nil, errors.New("can't find query node by nodeID")
}
func (c *queryNodeCluster) removeQueryChannel(ctx context.Context, nodeID int64, in *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		status, err := node.client.RemoveQueryChannel(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			//TODO::should reopen
			//collectionID := in.CollectionID
			collectionID := int64(0)
			if _, ok = node.watchedQueryChannels[collectionID]; ok {
				node.removeQueryChannel(collectionID)
				return status, err
			}
			log.Error("queryChannel for collection not watched", zap.Int64("collectionID", collectionID))
		}
		return status, err
	}

	return nil, errors.New("can't find query node by nodeID")
}

func (c *queryNodeCluster) releaseCollection(ctx context.Context, nodeID int64, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		status, err := node.client.ReleaseCollection(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			node.releaseCollection(in.CollectionID)
			c.clusterMeta.releaseCollection(in.CollectionID)
		}
		return status, err
	}

	return nil, errors.New("can't find query node by nodeID")
}

func (c *queryNodeCluster) releasePartitions(ctx context.Context, nodeID int64, in *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	c.Lock()
	defer c.Unlock()

	if node, ok := c.nodes[nodeID]; ok {
		status, err := node.client.ReleasePartitions(ctx, in)
		if err == nil && status.ErrorCode == commonpb.ErrorCode_Success {
			for _, partitionID := range in.PartitionIDs {
				node.releasePartition(in.CollectionID, partitionID)
				c.clusterMeta.releasePartition(in.CollectionID, partitionID)
			}
		}
		return status, err
	}

	return nil, errors.New("can't find query node by nodeID")
}

func (c *queryNodeCluster) getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	c.Lock()
	defer c.Unlock()

	segmentInfos := make([]*querypb.SegmentInfo, 0)
	nodes, err := c.getOnServiceNodeIDs()
	if err != nil {
		log.Warn(err.Error())
		return segmentInfos, nil
	}
	for _, nodeID := range nodes {
		res, err := c.nodes[nodeID].client.GetSegmentInfo(ctx, in)
		if err != nil {
			return nil, err
		}
		segmentInfos = append(segmentInfos, res.Infos...)
	}

	return segmentInfos, nil
}

func (c *queryNodeCluster) getNumDmChannels(nodeID int64) (int, error) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[nodeID]; !ok {
		return 0, errors.New("Can't find query node by nodeID ")
	}

	numChannel := 0
	for _, info := range c.clusterMeta.collectionInfos {
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
				numChannel++
			}
		}
	}
	return numChannel, nil
}

func (c *queryNodeCluster) getNumSegments(nodeID int64) (int, error) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[nodeID]; !ok {
		return 0, errors.New("Can't find query node by nodeID ")
	}

	numSegment := 0
	for _, info := range c.clusterMeta.segmentInfos {
		if info.NodeID == nodeID {
			numSegment++
		}
	}
	return numSegment, nil
}

func (c *queryNodeCluster) RegisterNode(ctx context.Context, session *sessionutil.Session, id UniqueID) error {
	c.Lock()
	defer c.Unlock()

	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, id)
	err = c.client.Save(key, string(sessionJSON))
	if err != nil {
		return err
	}
	node, err := newQueryNode(ctx, session.Address, id, c.client)
	if err != nil {
		return err
	}
	log.Debug("register a new query node", zap.Int64("nodeID", id), zap.String("address", session.Address))

	if _, ok := c.nodes[id]; !ok {
		c.nodes[id] = node
		return nil
	}

	return fmt.Errorf("node %d alredy exists in cluster", id)
}

func (c *queryNodeCluster) removeNodeInfo(nodeID int64) error {
	c.Lock()
	defer c.Unlock()

	key := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, nodeID)
	err := c.client.Remove(key)
	if err != nil {
		return err
	}

	err = c.nodes[nodeID].clearNodeInfo()
	if err != nil {
		return err
	}
	delete(c.nodes, nodeID)
	log.Debug("delete nodeInfo in cluster meta and etcd", zap.Int64("nodeID", nodeID))

	return nil
}

func (c *queryNodeCluster) onServiceNodeIDs() ([]int64, error) {
	c.Lock()
	defer c.Unlock()

	return c.getOnServiceNodeIDs()
}

func (c *queryNodeCluster) getOnServiceNodeIDs() ([]int64, error) {
	nodeIDs := make([]int64, 0)
	for nodeID, node := range c.nodes {
		if node.isOnService() {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	if len(nodeIDs) == 0 {
		return nil, errors.New("no queryNode is alive")
	}

	return nodeIDs, nil
}

func (c *queryNodeCluster) printMeta() {
	for id, node := range c.nodes {
		if node.isOnService() {
			for collectionID, info := range node.collectionInfos {
				log.Debug("query coordinator cluster info: collectionInfo", zap.Int64("nodeID", id), zap.Int64("collectionID", collectionID), zap.Any("info", info))
			}

			for collectionID, info := range node.watchedQueryChannels {
				log.Debug("query coordinator cluster info: watchedQueryChannelInfo", zap.Int64("nodeID", id), zap.Int64("collectionID", collectionID), zap.Any("info", info))
			}
		}
	}
}
