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
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	nodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// Node provides many interfaces to access querynode via grpc
type Node interface {
	start() error
	stop()
	getNodeInfo() (Node, error)
	clearNodeInfo() error

	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	setCollectionInfo(info *querypb.CollectionInfo) error
	showCollections() []*querypb.CollectionInfo
	releaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) error

	addPartition(collectionID UniqueID, partitionID UniqueID) error
	releasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) error

	watchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) error
	watchDeltaChannels(ctx context.Context, in *querypb.WatchDeltaChannelsRequest) error
	//removeDmChannel(collectionID UniqueID, channels []string) error

	hasWatchedDeltaChannel(collectionID UniqueID) bool
	hasWatchedQueryChannel(collectionID UniqueID) bool
	//showWatchedQueryChannels() []*querypb.QueryChannelInfo
	addQueryChannel(ctx context.Context, in *querypb.AddQueryChannelRequest) error
	removeQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelRequest) error

	setState(state nodeState)
	getState() nodeState
	isOnline() bool
	isOffline() bool

	getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	loadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest) error
	releaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest) error
	getComponentInfo(ctx context.Context) *internalpb.ComponentInfo

	getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type queryNode struct {
	ctx      context.Context
	cancel   context.CancelFunc
	id       int64
	address  string
	client   types.QueryNode
	kvClient *etcdkv.EtcdKV

	sync.RWMutex
	collectionInfos      map[UniqueID]*querypb.CollectionInfo
	watchedQueryChannels map[UniqueID]*querypb.QueryChannelInfo
	watchedDeltaChannels map[UniqueID][]*datapb.VchannelInfo
	state                nodeState
	stateLock            sync.RWMutex

	totalMem     uint64
	memUsage     uint64
	memUsageRate float64
	cpuUsage     float64
}

func newQueryNode(ctx context.Context, address string, id UniqueID, kv *etcdkv.EtcdKV) (Node, error) {
	collectionInfo := make(map[UniqueID]*querypb.CollectionInfo)
	watchedChannels := make(map[UniqueID]*querypb.QueryChannelInfo)
	watchedDeltaChannels := make(map[UniqueID][]*datapb.VchannelInfo)
	childCtx, cancel := context.WithCancel(ctx)
	client, err := nodeclient.NewClient(childCtx, address)
	if err != nil {
		cancel()
		return nil, err
	}
	node := &queryNode{
		ctx:                  childCtx,
		cancel:               cancel,
		id:                   id,
		address:              address,
		client:               client,
		kvClient:             kv,
		collectionInfos:      collectionInfo,
		watchedQueryChannels: watchedChannels,
		watchedDeltaChannels: watchedDeltaChannels,
		state:                disConnect,
	}

	return node, nil
}

func (qn *queryNode) start() error {
	if err := qn.client.Init(); err != nil {
		log.Error("start: init queryNode client failed", zap.Int64("nodeID", qn.id), zap.String("error", err.Error()))
		return err
	}
	if err := qn.client.Start(); err != nil {
		log.Error("start: start queryNode client failed", zap.Int64("nodeID", qn.id), zap.String("error", err.Error()))
		return err
	}

	qn.stateLock.Lock()
	if qn.state < online {
		qn.state = online
	}
	qn.stateLock.Unlock()
	log.Debug("start: queryNode client start success", zap.Int64("nodeID", qn.id), zap.String("address", qn.address))
	return nil
}

func (qn *queryNode) stop() {
	qn.stateLock.Lock()
	defer qn.stateLock.Unlock()
	qn.state = offline
	if qn.client != nil {
		qn.client.Stop()
	}
	qn.cancel()
}

func (qn *queryNode) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	qn.Lock()
	defer qn.Unlock()

	if _, ok := qn.collectionInfos[collectionID]; !ok {
		partitions := make([]UniqueID, 0)
		channels := make([]*querypb.DmChannelInfo, 0)
		newCollection := &querypb.CollectionInfo{
			CollectionID: collectionID,
			PartitionIDs: partitions,
			ChannelInfos: channels,
			Schema:       schema,
		}
		qn.collectionInfos[collectionID] = newCollection
		err := saveNodeCollectionInfo(collectionID, newCollection, qn.id, qn.kvClient)
		if err != nil {
			log.Error("addCollection: save collectionInfo error", zap.Int64("nodeID", qn.id), zap.Int64("collectionID", collectionID), zap.Any("error", err.Error()))
			return err
		}
		log.Debug("queryNode addCollection", zap.Int64("nodeID", qn.id), zap.Any("collectionInfo", newCollection))
	}

	return nil
}

func (qn *queryNode) setCollectionInfo(info *querypb.CollectionInfo) error {
	qn.Lock()
	defer qn.Unlock()

	qn.collectionInfos[info.CollectionID] = info
	err := saveNodeCollectionInfo(info.CollectionID, info, qn.id, qn.kvClient)
	if err != nil {
		log.Error("setCollectionInfo: save collectionInfo error", zap.Int64("nodeID", qn.id), zap.Int64("collectionID", info.CollectionID), zap.Any("error", err.Error()))
		return err
	}
	return nil
}

func (qn *queryNode) showCollections() []*querypb.CollectionInfo {
	qn.RLock()
	defer qn.RUnlock()

	results := make([]*querypb.CollectionInfo, 0)
	for _, info := range qn.collectionInfos {
		results = append(results, proto.Clone(info).(*querypb.CollectionInfo))
	}

	return results
}

func (qn *queryNode) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	qn.Lock()
	defer qn.Unlock()
	if col, ok := qn.collectionInfos[collectionID]; ok {
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return nil
			}
		}
		col.PartitionIDs = append(col.PartitionIDs, partitionID)
		err := saveNodeCollectionInfo(collectionID, col, qn.id, qn.kvClient)
		if err != nil {
			log.Error("AddPartition: save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		log.Debug("queryNode add partition", zap.Int64("nodeID", qn.id), zap.Any("collectionInfo", col))
		return nil
	}
	return errors.New("AddPartition: can't find collection when add partition")
}

func (qn *queryNode) releaseCollectionInfo(collectionID UniqueID) error {
	qn.Lock()
	defer qn.Unlock()
	if _, ok := qn.collectionInfos[collectionID]; ok {
		err := removeNodeCollectionInfo(collectionID, qn.id, qn.kvClient)
		if err != nil {
			log.Error("ReleaseCollectionInfo: remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		delete(qn.collectionInfos, collectionID)
	}

	delete(qn.watchedQueryChannels, collectionID)
	return nil
}

func (qn *queryNode) releasePartitionsInfo(collectionID UniqueID, partitionIDs []UniqueID) error {
	qn.Lock()
	defer qn.Unlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		newPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.PartitionIDs {
			match := false
			for _, partitionID := range partitionIDs {
				if id == partitionID {
					match = true
					break
				}
			}
			if !match {
				newPartitionIDs = append(newPartitionIDs, id)
			}
		}
		info.PartitionIDs = newPartitionIDs
		err := saveNodeCollectionInfo(collectionID, info, qn.id, qn.kvClient)
		if err != nil {
			log.Error("ReleasePartitionsInfo: remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		log.Debug("queryNode release partition info", zap.Int64("nodeID", qn.id), zap.Any("info", info))
	}

	return nil
}

func (qn *queryNode) addDmChannel(collectionID UniqueID, channels []string) error {
	qn.Lock()
	defer qn.Unlock()

	//before add channel, should ensure toAddedChannels not in MetaReplica
	if info, ok := qn.collectionInfos[collectionID]; ok {
		findNodeID := false
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == qn.id {
				findNodeID = true
				channelInfo.ChannelIDs = append(channelInfo.ChannelIDs, channels...)
			}
		}
		if !findNodeID {
			newChannelInfo := &querypb.DmChannelInfo{
				NodeIDLoaded: qn.id,
				ChannelIDs:   channels,
			}
			info.ChannelInfos = append(info.ChannelInfos, newChannelInfo)
		}
		err := saveNodeCollectionInfo(collectionID, info, qn.id, qn.kvClient)
		if err != nil {
			log.Error("AddDmChannel: save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		return nil
	}

	return errors.New("AddDmChannels: can't find collection in watchedQueryChannel")
}

//func (qn *queryNode) removeDmChannel(collectionID UniqueID, channels []string) error {
//	qn.Lock()
//	defer qn.Unlock()
//
//	if info, ok := qn.collectionInfos[collectionID]; ok {
//		for _, channelInfo := range info.ChannelInfos {
//			if channelInfo.NodeIDLoaded == qn.id {
//				newChannelIDs := make([]string, 0)
//				for _, channelID := range channelInfo.ChannelIDs {
//					findChannel := false
//					for _, channel := range channels {
//						if channelID == channel {
//							findChannel = true
//						}
//					}
//					if !findChannel {
//						newChannelIDs = append(newChannelIDs, channelID)
//					}
//				}
//				channelInfo.ChannelIDs = newChannelIDs
//			}
//		}
//
//		err := saveNodeCollectionInfo(collectionID, info, qn.id, qn.kvClient)
//		if err != nil {
//			log.Error("RemoveDmChannel: save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
//		}
//	}
//
//	return errors.New("RemoveDmChannel: can't find collection in watchedQueryChannel")
//}

func (qn *queryNode) hasWatchedQueryChannel(collectionID UniqueID) bool {
	qn.RLock()
	defer qn.RUnlock()

	if _, ok := qn.watchedQueryChannels[collectionID]; ok {
		return true
	}

	return false
}

func (qn *queryNode) hasWatchedDeltaChannel(collectionID UniqueID) bool {
	qn.RLock()
	defer qn.RUnlock()

	_, ok := qn.watchedDeltaChannels[collectionID]
	return ok
}

//func (qn *queryNode) showWatchedQueryChannels() []*querypb.QueryChannelInfo {
//	qn.RLock()
//	defer qn.RUnlock()
//
//	results := make([]*querypb.QueryChannelInfo, 0)
//	for _, info := range qn.watchedQueryChannels {
//		results = append(results, proto.Clone(info).(*querypb.QueryChannelInfo))
//	}
//
//	return results
//}

func (qn *queryNode) setDeltaChannelInfo(collectionID int64, infos []*datapb.VchannelInfo) {
	qn.Lock()
	defer qn.Unlock()

	qn.watchedDeltaChannels[collectionID] = infos
}

func (qn *queryNode) setQueryChannelInfo(info *querypb.QueryChannelInfo) {
	qn.Lock()
	defer qn.Unlock()

	qn.watchedQueryChannels[info.CollectionID] = info
}

func (qn *queryNode) removeQueryChannelInfo(collectionID UniqueID) {
	qn.Lock()
	defer qn.Unlock()

	delete(qn.watchedQueryChannels, collectionID)
}

func (qn *queryNode) clearNodeInfo() error {
	qn.RLock()
	defer qn.RUnlock()
	for collectionID := range qn.collectionInfos {
		err := removeNodeCollectionInfo(collectionID, qn.id, qn.kvClient)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qn *queryNode) setState(state nodeState) {
	qn.stateLock.Lock()
	defer qn.stateLock.Unlock()

	qn.state = state
}

func (qn *queryNode) getState() nodeState {
	qn.stateLock.RLock()
	defer qn.stateLock.RUnlock()

	return qn.state
}

func (qn *queryNode) isOnline() bool {
	qn.stateLock.RLock()
	defer qn.stateLock.RUnlock()

	return qn.state == online
}

func (qn *queryNode) isOffline() bool {
	qn.stateLock.RLock()
	defer qn.stateLock.RUnlock()

	return qn.state == offline
}

//***********************grpc req*************************//
func (qn *queryNode) watchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) error {
	if !qn.isOnline() {
		return errors.New("WatchDmChannels: queryNode is offline")
	}

	status, err := qn.client.WatchDmChannels(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}
	channels := make([]string, 0)
	for _, info := range in.Infos {
		channels = append(channels, info.ChannelName)
	}
	err = qn.addCollection(in.CollectionID, in.Schema)
	if err != nil {
		return err
	}
	err = qn.addDmChannel(in.CollectionID, channels)
	return err
}

func (qn *queryNode) watchDeltaChannels(ctx context.Context, in *querypb.WatchDeltaChannelsRequest) error {
	if !qn.isOnline() {
		return errors.New("WatchDmChannels: queryNode is offline")
	}

	status, err := qn.client.WatchDeltaChannels(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}
	qn.setDeltaChannelInfo(in.CollectionID, in.Infos)
	return err
}

func (qn *queryNode) addQueryChannel(ctx context.Context, in *querypb.AddQueryChannelRequest) error {
	if !qn.isOnline() {
		return errors.New("AddQueryChannel: queryNode is offline")
	}

	status, err := qn.client.AddQueryChannel(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	queryChannelInfo := &querypb.QueryChannelInfo{
		CollectionID:         in.CollectionID,
		QueryChannelID:       in.RequestChannelID,
		QueryResultChannelID: in.ResultChannelID,
	}
	qn.setQueryChannelInfo(queryChannelInfo)
	return nil
}

func (qn *queryNode) removeQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelRequest) error {
	if !qn.isOnline() {
		return nil
	}

	status, err := qn.client.RemoveQueryChannel(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	qn.removeQueryChannelInfo(in.CollectionID)
	return nil
}

func (qn *queryNode) releaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) error {
	if !qn.isOnline() {
		log.Debug("ReleaseCollection: the query node has been offline, the release request is no longer needed", zap.Int64("nodeID", qn.id))
		return nil
	}

	status, err := qn.client.ReleaseCollection(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	err = qn.releaseCollectionInfo(in.CollectionID)
	if err != nil {
		return err
	}

	return nil
}

func (qn *queryNode) releasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) error {
	if !qn.isOnline() {
		return nil
	}

	status, err := qn.client.ReleasePartitions(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}
	err = qn.releasePartitionsInfo(in.CollectionID, in.PartitionIDs)
	if err != nil {
		return err
	}

	return nil
}

func (qn *queryNode) getSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	if !qn.isOnline() {
		return nil, fmt.Errorf("getSegmentInfo: queryNode %d is offline", qn.id)
	}

	res, err := qn.client.GetSegmentInfo(qn.ctx, in)
	if err != nil {
		return nil, err
	}
	if res.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(res.Status.Reason)
	}

	return res, nil
}

func (qn *queryNode) getComponentInfo(ctx context.Context) *internalpb.ComponentInfo {
	if !qn.isOnline() {
		return &internalpb.ComponentInfo{
			NodeID:    qn.id,
			StateCode: internalpb.StateCode_Abnormal,
		}
	}

	res, err := qn.client.GetComponentStates(qn.ctx)
	if err != nil || res.Status.ErrorCode != commonpb.ErrorCode_Success {
		return &internalpb.ComponentInfo{
			NodeID:    qn.id,
			StateCode: internalpb.StateCode_Abnormal,
		}
	}

	return res.State
}

func (qn *queryNode) getMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !qn.isOnline() {
		return nil, errQueryNodeIsNotOnService(qn.id)
	}

	return qn.client.GetMetrics(qn.ctx, in)
}

func (qn *queryNode) loadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest) error {
	if !qn.isOnline() {
		return errors.New("LoadSegments: queryNode is offline")
	}

	status, err := qn.client.LoadSegments(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	for _, info := range in.Infos {
		err = qn.addCollection(info.CollectionID, in.Schema)
		if err != nil {
			return err
		}
		err = qn.addPartition(info.CollectionID, info.PartitionID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qn *queryNode) releaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest) error {
	if !qn.isOnline() {
		return errors.New("ReleaseSegments: queryNode is offline")
	}

	status, err := qn.client.ReleaseSegments(qn.ctx, in)
	if err != nil {
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	return nil
}

func (qn *queryNode) getNodeInfo() (Node, error) {
	qn.RLock()
	defer qn.RUnlock()

	if !qn.isOnline() {
		return nil, errors.New("getNodeInfo: queryNode is offline")
	}

	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, err
	}
	resp, err := qn.client.GetMetrics(qn.ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(resp.Status.Reason)
	}
	infos := metricsinfo.QueryNodeInfos{}
	err = metricsinfo.UnmarshalComponentInfos(resp.Response, &infos)
	if err != nil {
		return nil, err
	}
	qn.cpuUsage = infos.HardwareInfos.CPUCoreUsage
	qn.totalMem = infos.HardwareInfos.Memory
	qn.memUsage = infos.HardwareInfos.MemoryUsage
	qn.memUsageRate = float64(qn.memUsage) / float64(qn.totalMem)
	return &queryNode{
		id:      qn.id,
		address: qn.address,
		state:   qn.state,

		totalMem:     qn.totalMem,
		memUsage:     qn.memUsage,
		memUsageRate: qn.memUsageRate,
		cpuUsage:     qn.cpuUsage,
	}, nil
}

//****************************************************//

func saveNodeCollectionInfo(collectionID UniqueID, info *querypb.CollectionInfo, nodeID int64, kv *etcdkv.EtcdKV) error {
	infoBytes, err := proto.Marshal(info)
	if err != nil {
		log.Error("QueryNode::saveNodeCollectionInfo ", zap.Error(err))
		return err
	}

	key := fmt.Sprintf("%s/%d/%d", queryNodeMetaPrefix, nodeID, collectionID)
	return kv.Save(key, string(infoBytes))
}

func removeNodeCollectionInfo(collectionID UniqueID, nodeID int64, kv *etcdkv.EtcdKV) error {
	key := fmt.Sprintf("%s/%d/%d", queryNodeMetaPrefix, nodeID, collectionID)
	return kv.Remove(key)
}
