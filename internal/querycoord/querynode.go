//// Copyright (C) 2019-2020 Zilliz. All rights reserved.
////
//// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
//// with the License. You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software distributed under the License
//// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//// or implied. See the License for the specific language governing permissions and limitations under the License.
//
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
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
)

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
	onService            bool
	serviceLock          sync.Mutex
}

func newQueryNode(ctx context.Context, address string, id UniqueID, kv *etcdkv.EtcdKV) *queryNode {
	collectionInfo := make(map[UniqueID]*querypb.CollectionInfo)
	watchedChannels := make(map[UniqueID]*querypb.QueryChannelInfo)
	childCtx, cancel := context.WithCancel(ctx)
	node := &queryNode{
		ctx:                  childCtx,
		cancel:               cancel,
		id:                   id,
		address:              address,
		kvClient:             kv,
		collectionInfos:      collectionInfo,
		watchedQueryChannels: watchedChannels,
		onService:            false,
	}

	return node
}

func (qn *queryNode) start() error {
	client, err := nodeclient.NewClient(qn.ctx, qn.address)
	if err != nil {
		return err
	}
	if err = client.Init(); err != nil {
		return err
	}
	if err = client.Start(); err != nil {
		return err
	}

	qn.client = client
	qn.serviceLock.Lock()
	qn.onService = true
	qn.serviceLock.Unlock()
	log.Debug("queryNode client start success", zap.Int64("nodeID", qn.id), zap.String("address", qn.address))
	return nil
}

func (qn *queryNode) stop() {
	qn.serviceLock.Lock()
	defer qn.serviceLock.Unlock()
	qn.onService = false
	if qn.client != nil {
		qn.client.Stop()
	}
	qn.cancel()
}

func (qn *queryNode) hasCollection(collectionID UniqueID) bool {
	qn.RLock()
	defer qn.RUnlock()

	if _, ok := qn.collectionInfos[collectionID]; ok {
		return true
	}

	return false
}

func (qn *queryNode) hasPartition(collectionID UniqueID, partitionID UniqueID) bool {
	qn.RLock()
	defer qn.RUnlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		for _, id := range info.PartitionIDs {
			if partitionID == id {
				return true
			}
		}
	}

	return false
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
		err := qn.saveCollectionInfo(collectionID, newCollection)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		return nil
	}

	return errors.New("addCollection: collection already exists")
}

func (qn *queryNode) getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	qn.Lock()
	defer qn.Lock()

	if _, ok := qn.collectionInfos[collectionID]; ok {
		return proto.Clone(qn.collectionInfos[collectionID]).(*querypb.CollectionInfo), nil
	}
	return nil, errors.New("addPartition: can't find collection")
}

func (qn *queryNode) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	qn.Lock()
	defer qn.Unlock()
	if col, ok := qn.collectionInfos[collectionID]; ok {
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return errors.New("addPartition: partition already exists in collectionInfos")
			}
		}
		col.PartitionIDs = append(col.PartitionIDs, partitionID)
		err := qn.saveCollectionInfo(collectionID, col)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (qn *queryNode) releaseCollection(collectionID UniqueID) {
	qn.Lock()
	defer qn.Unlock()
	if _, ok := qn.collectionInfos[collectionID]; ok {
		err := qn.removeCollectionInfo(collectionID)
		if err != nil {
			log.Error("remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		delete(qn.collectionInfos, collectionID)
	}

	delete(qn.watchedQueryChannels, collectionID)
}

func (qn *queryNode) releasePartition(collectionID UniqueID, partitionID UniqueID) {
	qn.Lock()
	defer qn.Unlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		newPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.PartitionIDs {
			if id != partitionID {
				newPartitionIDs = append(newPartitionIDs, id)
			}
		}
		info.PartitionIDs = newPartitionIDs
		err := qn.removeCollectionInfo(collectionID)
		if err != nil {
			log.Error("remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}
}

func (qn *queryNode) hasWatchedDmChannel(collectionID UniqueID, channelID string) (bool, error) {
	qn.RLock()
	defer qn.RUnlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		channelInfos := info.ChannelInfos
		for _, channelInfo := range channelInfos {
			for _, channel := range channelInfo.ChannelIDs {
				if channel == channelID {
					return true, nil
				}
			}
		}
		return false, nil
	}

	return false, errors.New("hasWatchedDmChannel: can't find collection in collectionInfos")
}

func (qn *queryNode) getDmChannelsByCollectionID(collectionID UniqueID) ([]string, error) {
	qn.RLock()
	defer qn.RUnlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		channels := make([]string, 0)
		for _, channelsInfo := range info.ChannelInfos {
			channels = append(channels, channelsInfo.ChannelIDs...)
		}
		return channels, nil
	}

	return nil, errors.New("getDmChannelsByCollectionID: can't find collection in collectionInfos")
}

func (qn *queryNode) addDmChannel(collectionID UniqueID, channels []string) error {
	qn.Lock()
	defer qn.Unlock()

	//before add channel, should ensure toAddedChannels not in meta
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
		err := qn.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}

	return errors.New("addDmChannels: can't find collection in watchedQueryChannel")
}

func (qn *queryNode) removeDmChannel(collectionID UniqueID, channels []string) error {
	qn.Lock()
	defer qn.Unlock()

	if info, ok := qn.collectionInfos[collectionID]; ok {
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == qn.id {
				newChannelIDs := make([]string, 0)
				for _, channelID := range channelInfo.ChannelIDs {
					findChannel := false
					for _, channel := range channels {
						if channelID == channel {
							findChannel = true
						}
					}
					if !findChannel {
						newChannelIDs = append(newChannelIDs, channelID)
					}
				}
				channelInfo.ChannelIDs = newChannelIDs
			}
		}

		err := qn.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}

	return errors.New("addDmChannels: can't find collection in watchedQueryChannel")
}

func (qn *queryNode) hasWatchedQueryChannel(collectionID UniqueID) bool {
	qn.RLock()
	defer qn.RUnlock()

	if _, ok := qn.watchedQueryChannels[collectionID]; ok {
		return true
	}

	return false
}

func (qn *queryNode) addQueryChannel(collectionID UniqueID, queryChannel *querypb.QueryChannelInfo) {
	qn.Lock()
	defer qn.Unlock()

	qn.watchedQueryChannels[collectionID] = queryChannel
}

func (qn *queryNode) removeQueryChannel(collectionID UniqueID) error {
	qn.Lock()
	defer qn.Unlock()

	delete(qn.watchedQueryChannels, collectionID)

	return errors.New("removeQueryChannel: can't find collection in watchedQueryChannel")
}

func (qn *queryNode) saveCollectionInfo(collectionID UniqueID, info *querypb.CollectionInfo) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d/%d", queryNodeMetaPrefix, qn.id, collectionID)
	return qn.kvClient.Save(key, infoBytes)
}

func (qn *queryNode) removeCollectionInfo(collectionID UniqueID) error {
	key := fmt.Sprintf("%s/%d/%d", queryNodeMetaPrefix, qn.id, collectionID)
	return qn.kvClient.Remove(key)
}

func (qn *queryNode) clearNodeInfo() error {
	for collectionID := range qn.collectionInfos {
		err := qn.removeCollectionInfo(collectionID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qn *queryNode) setNodeState(onService bool) {
	qn.serviceLock.Lock()
	defer qn.serviceLock.Unlock()

	qn.onService = onService
}

func (qn *queryNode) isOnService() bool {
	qn.serviceLock.Lock()
	defer qn.serviceLock.Unlock()

	return qn.onService
}
