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
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

const (
	collectionMetaPrefix   = "queryCoord-collectionMeta"
	segmentMetaPrefix      = "queryCoord-segmentMeta"
	queryChannelMetaPrefix = "queryCoord-queryChannel"
)

type meta struct {
	client *etcdkv.EtcdKV // client of a reliable kv service, i.e. etcd client

	sync.RWMutex
	collectionInfos   map[UniqueID]*querypb.CollectionInfo
	segmentInfos      map[UniqueID]*querypb.SegmentInfo
	queryChannelInfos map[UniqueID]*querypb.QueryChannelInfo

	partitionStates map[UniqueID]querypb.PartitionState
}

func newMeta(kv *etcdkv.EtcdKV) (*meta, error) {
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	queryChannelInfos := make(map[UniqueID]*querypb.QueryChannelInfo)
	partitionStates := make(map[UniqueID]querypb.PartitionState)

	m := &meta{
		client:            kv,
		collectionInfos:   collectionInfos,
		segmentInfos:      segmentInfos,
		queryChannelInfos: queryChannelInfos,
		partitionStates:   partitionStates,
	}

	err := m.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *meta) reloadFromKV() error {
	collectionKeys, collectionValues, err := m.client.LoadWithPrefix(collectionMetaPrefix)
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
		m.collectionInfos[collectionID] = collectionInfo
	}

	segmentKeys, segmentValues, err := m.client.LoadWithPrefix(segmentMetaPrefix)
	if err != nil {
		return err
	}
	for index := range segmentKeys {
		segmentID, err := strconv.ParseInt(filepath.Base(segmentKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		segmentInfo := &querypb.SegmentInfo{}
		err = proto.UnmarshalText(segmentValues[index], segmentInfo)
		if err != nil {
			return err
		}
		m.segmentInfos[segmentID] = segmentInfo
	}

	queryChannelKeys, queryChannelValues, err := m.client.LoadWithPrefix(queryChannelMetaPrefix)
	if err != nil {
		return nil
	}
	for index := range queryChannelKeys {
		collectionID, err := strconv.ParseInt(filepath.Base(queryChannelKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		queryChannelInfo := &querypb.QueryChannelInfo{}
		err = proto.UnmarshalText(queryChannelValues[index], queryChannelInfo)
		if err != nil {
			return err
		}
		m.queryChannelInfos[collectionID] = queryChannelInfo
	}
	//TODO::update partition states

	return nil
}

func (m *meta) showCollections() []UniqueID {
	m.RLock()
	defer m.RUnlock()

	collections := make([]UniqueID, 0)
	for id := range m.collectionInfos {
		collections = append(collections, id)
	}
	return collections
}

func (m *meta) showPartitions(collectionID UniqueID) ([]UniqueID, error) {
	m.RLock()
	defer m.RUnlock()

	//TODO::should update after load collection
	if info, ok := m.collectionInfos[collectionID]; ok {
		return info.PartitionIDs, nil
	}

	return nil, errors.New("showPartitions: can't find collection in collectionInfos")
}

func (m *meta) hasCollection(collectionID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.collectionInfos[collectionID]; ok {
		return true
	}

	return false
}

func (m *meta) hasPartition(collectionID UniqueID, partitionID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, id := range info.PartitionIDs {
			if partitionID == id {
				return true
			}
		}
	}

	return false
}

func (m *meta) hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, id := range info.ReleasedPartitionIDs {
			if partitionID == id {
				return true
			}
		}
	}

	return false
}

func (m *meta) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.collectionInfos[collectionID]; !ok {
		partitions := make([]UniqueID, 0)
		channels := make([]*querypb.DmChannelInfo, 0)
		newCollection := &querypb.CollectionInfo{
			CollectionID: collectionID,
			PartitionIDs: partitions,
			ChannelInfos: channels,
			Schema:       schema,
		}
		m.collectionInfos[collectionID] = newCollection
		err := m.saveCollectionInfo(collectionID, newCollection)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		log.Debug("add collection",
			zap.Any("collectionID", collectionID),
		)
		return nil
	}

	return errors.New("addCollection: collection already exists")
}

func (m *meta) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	if col, ok := m.collectionInfos[collectionID]; ok {
		log.Debug("add a  partition to meta...", zap.Int64s("partitionIDs", col.PartitionIDs))
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return errors.New("addPartition: partition already exists in collectionInfos")
			}
		}
		col.PartitionIDs = append(col.PartitionIDs, partitionID)
		releasedPartitionIDs := make([]UniqueID, 0)
		for _, id := range col.ReleasedPartitionIDs {
			if id != partitionID {
				releasedPartitionIDs = append(releasedPartitionIDs, id)
			}
		}
		col.ReleasedPartitionIDs = releasedPartitionIDs
		m.partitionStates[partitionID] = querypb.PartitionState_NotPresent
		log.Debug("add a  partition to meta", zap.Int64s("partitionIDs", col.PartitionIDs))
		err := m.saveCollectionInfo(collectionID, col)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (m *meta) deleteSegmentInfoByID(segmentID UniqueID) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.segmentInfos[segmentID]; ok {
		err := m.removeSegmentInfo(segmentID)
		if err != nil {
			log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
		}
		delete(m.segmentInfos, segmentID)
	}
}

func (m *meta) deleteSegmentInfoByNodeID(nodeID UniqueID) {
	m.Lock()
	defer m.Unlock()

	for segmentID, info := range m.segmentInfos {
		if info.NodeID == nodeID {
			err := m.removeSegmentInfo(segmentID)
			if err != nil {
				log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
			}
			delete(m.segmentInfos, segmentID)
		}
	}
}

func (m *meta) setSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) {
	m.Lock()
	defer m.Unlock()

	err := m.saveSegmentInfo(segmentID, info)
	if err != nil {
		log.Error("save segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
	}
	m.segmentInfos[segmentID] = info
}

func (m *meta) getSegmentInfos(segmentIDs []UniqueID) ([]*querypb.SegmentInfo, error) {
	m.Lock()
	defer m.Unlock()

	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, segmentID := range segmentIDs {
		if info, ok := m.segmentInfos[segmentID]; ok {
			segmentInfos = append(segmentInfos, proto.Clone(info).(*querypb.SegmentInfo))
			continue
		}
		return nil, errors.New("segment not exist")
	}
	return segmentInfos, nil
}

func (m *meta) hasSegmentInfo(segmentID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.segmentInfos[segmentID]; ok {
		return true
	}

	return false
}

func (m *meta) getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.segmentInfos[segmentID]; ok {

		return proto.Clone(info).(*querypb.SegmentInfo), nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
}

func (m *meta) getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.CollectionInfo), nil
	}

	return nil, errors.New("getCollectionInfoByID: can't find collectionID in collectionInfo")
}

func (m *meta) getQueryChannelInfoByID(collectionID UniqueID) (*querypb.QueryChannelInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.queryChannelInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.QueryChannelInfo), nil
	}

	return nil, errors.New("getQueryChannelInfoByID: can't find collectionID in queryChannelInfo")
}

func (m *meta) updatePartitionState(partitionID UniqueID, state querypb.PartitionState) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.partitionStates[partitionID]; ok {
		m.partitionStates[partitionID] = state
	}

	return errors.New("updatePartitionState: can't find partition in partitionStates")
}

func (m *meta) getPartitionStateByID(partitionID UniqueID) (querypb.PartitionState, error) {
	m.RLock()
	defer m.RUnlock()

	if state, ok := m.partitionStates[partitionID]; ok {
		return state, nil
	}

	return 0, errors.New("getPartitionStateByID: can't find partition in partitionStates")
}

func (m *meta) releaseCollection(collectionID UniqueID) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, partitionID := range info.PartitionIDs {
			delete(m.partitionStates, partitionID)
		}
		delete(m.collectionInfos, collectionID)
	}
	for id, info := range m.segmentInfos {
		if info.CollectionID == collectionID {
			err := m.removeSegmentInfo(id)
			if err != nil {
				log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", id))
			}
			delete(m.segmentInfos, id)
		}
	}

	delete(m.queryChannelInfos, collectionID)
	err := m.removeCollectionInfo(collectionID)
	if err != nil {
		log.Error("remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
	}
}

func (m *meta) releasePartition(collectionID UniqueID, partitionID UniqueID) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		newPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.PartitionIDs {
			if id == partitionID {
				delete(m.partitionStates, partitionID)
			} else {
				newPartitionIDs = append(newPartitionIDs, id)
			}
		}
		info.PartitionIDs = newPartitionIDs

		releasedPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.ReleasedPartitionIDs {
			if id != partitionID {
				releasedPartitionIDs = append(releasedPartitionIDs, id)
			}
		}
		releasedPartitionIDs = append(releasedPartitionIDs, partitionID)
		info.ReleasedPartitionIDs = releasedPartitionIDs
		err := m.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}
	for id, info := range m.segmentInfos {
		if info.PartitionID == partitionID {
			err := m.removeSegmentInfo(id)
			if err != nil {
				log.Error("delete segmentInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID), zap.Int64("segmentID", id))
			}
			delete(m.segmentInfos, id)
		}
	}
}

func (m *meta) hasWatchedDmChannel(collectionID UniqueID, channelID string) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
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

func (m *meta) getDmChannelsByCollectionID(collectionID UniqueID) ([]string, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		channels := make([]string, 0)
		for _, channelsInfo := range info.ChannelInfos {
			channels = append(channels, channelsInfo.ChannelIDs...)
		}
		return channels, nil
	}

	return nil, errors.New("getDmChannelsByCollectionID: can't find collection in collectionInfos")
}

func (m *meta) getDmChannelsByNodeID(collectionID UniqueID, nodeID int64) ([]string, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		channels := make([]string, 0)
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
				channels = append(channels, channelInfo.ChannelIDs...)
			}
		}
		return channels, nil
	}

	return nil, errors.New("getDmChannelsByNodeID: can't find collection in collectionInfos")
}

func (m *meta) addDmChannel(collectionID UniqueID, nodeID int64, channels []string) error {
	m.Lock()
	defer m.Unlock()

	//before add channel, should ensure toAddedChannels not in meta
	if info, ok := m.collectionInfos[collectionID]; ok {
		findNodeID := false
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
				findNodeID = true
				channelInfo.ChannelIDs = append(channelInfo.ChannelIDs, channels...)
			}
		}
		if !findNodeID {
			newChannelInfo := &querypb.DmChannelInfo{
				NodeIDLoaded: nodeID,
				ChannelIDs:   channels,
			}
			info.ChannelInfos = append(info.ChannelInfos, newChannelInfo)
		}

		err := m.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}

	return errors.New("addDmChannels: can't find collection in collectionInfos")
}

func (m *meta) removeDmChannel(collectionID UniqueID, nodeID int64, channels []string) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, channelInfo := range info.ChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
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

		err := m.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}

	return errors.New("addDmChannels: can't find collection in collectionInfos")
}

func (m *meta) GetQueryChannel(collectionID UniqueID) (string, string) {
	m.Lock()
	defer m.Unlock()

	//TODO::to remove
	collectionID = 0
	if info, ok := m.queryChannelInfos[collectionID]; ok {
		return info.QueryChannelID, info.QueryResultChannelID
	}

	searchPrefix := Params.SearchChannelPrefix
	searchResultPrefix := Params.SearchResultChannelPrefix
	allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(collectionID, 10)
	allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(collectionID, 10)
	log.Debug("query coordinator create query channel", zap.String("queryChannelName", allocatedQueryChannel), zap.String("queryResultChannelName", allocatedQueryResultChannel))

	queryChannelInfo := &querypb.QueryChannelInfo{
		CollectionID:         collectionID,
		QueryChannelID:       allocatedQueryChannel,
		QueryResultChannelID: allocatedQueryResultChannel,
	}
	m.queryChannelInfos[collectionID] = queryChannelInfo
	//TODO::return channel according collectionID
	return allocatedQueryChannel, allocatedQueryResultChannel
}

func (m *meta) saveCollectionInfo(collectionID UniqueID, info *querypb.CollectionInfo) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return m.client.Save(key, infoBytes)
}

func (m *meta) removeCollectionInfo(collectionID UniqueID) error {
	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return m.client.Remove(key)
}

func (m *meta) saveSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", segmentMetaPrefix, segmentID)
	return m.client.Save(key, infoBytes)
}

func (m *meta) removeSegmentInfo(segmentID UniqueID) error {
	key := fmt.Sprintf("%s/%d", segmentMetaPrefix, segmentID)
	return m.client.Remove(key)
}

func (m *meta) saveQueryChannelInfo(collectionID UniqueID, info *querypb.QueryChannelInfo) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
	return m.client.Save(key, infoBytes)
}

func (m *meta) removeQueryChannelInfo(collectionID UniqueID) error {
	key := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
	return m.client.Remove(key)
}

func (m *meta) setLoadCollection(collectionID UniqueID, state bool) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		info.LoadCollection = state
		err := m.saveCollectionInfo(collectionID, info)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		}
	}

	return errors.New("setLoadCollection: can't find collection in collectionInfos")
}

func (m *meta) getLoadCollection(collectionID UniqueID) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return info.LoadCollection, nil
	}

	return false, errors.New("getLoadCollection: can't find collection in collectionInfos")
}

func (m *meta) printMeta() {
	for id, info := range m.collectionInfos {
		log.Debug("query coordinator meta: collectionInfo", zap.Int64("collectionID", id), zap.Any("info", info))
	}

	for id, info := range m.segmentInfos {
		log.Debug("query coordinator meta: segmentInfo", zap.Int64("segmentID", id), zap.Any("info", info))
	}

	for id, info := range m.queryChannelInfos {
		log.Debug("query coordinator meta: queryChannelInfo", zap.Int64("collectionID", id), zap.Any("info", info))
	}
}
