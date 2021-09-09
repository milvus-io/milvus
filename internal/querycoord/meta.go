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

type Meta interface {
	reloadFromKV() error

	showCollections() []*querypb.CollectionInfo
	hasCollection(collectionID UniqueID) bool
	getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error)
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	releaseCollection(collectionID UniqueID) error

	addPartition(collectionID UniqueID, partitionID UniqueID) error
	showPartitions(collectionID UniqueID) ([]*querypb.PartitionStates, error)
	hasPartition(collectionID UniqueID, partitionID UniqueID) bool
	hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool
	releasePartition(collectionID UniqueID, partitionID UniqueID) error

	deleteSegmentInfoByID(segmentID UniqueID) error
	deleteSegmentInfoByNodeID(nodeID UniqueID) error
	setSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) error
	hasSegmentInfo(segmentID UniqueID) bool
	showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo
	getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error)

	getPartitionStatesByID(collectionID UniqueID, partitionID UniqueID) (*querypb.PartitionStates, error)

	getDmChannelsByNodeID(collectionID UniqueID, nodeID int64) ([]string, error)
	addDmChannel(collectionID UniqueID, nodeID int64, channels []string) error
	removeDmChannel(collectionID UniqueID, nodeID int64, channels []string) error

	getQueryChannelInfoByID(collectionID UniqueID) (*querypb.QueryChannelInfo, error)
	GetQueryChannel(collectionID UniqueID) (string, string)

	setLoadType(collectionID UniqueID, loadType querypb.LoadType) error
	getLoadType(collectionID UniqueID) (querypb.LoadType, error)
	setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error
	printMeta()
}

type MetaReplica struct {
	client *etcdkv.EtcdKV // client of a reliable kv service, i.e. etcd client

	sync.RWMutex
	collectionInfos   map[UniqueID]*querypb.CollectionInfo
	segmentInfos      map[UniqueID]*querypb.SegmentInfo
	queryChannelInfos map[UniqueID]*querypb.QueryChannelInfo

	//partitionStates map[UniqueID]*querypb.PartitionStates
}

func newMeta(kv *etcdkv.EtcdKV) (Meta, error) {
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	queryChannelInfos := make(map[UniqueID]*querypb.QueryChannelInfo)

	m := &MetaReplica{
		client:            kv,
		collectionInfos:   collectionInfos,
		segmentInfos:      segmentInfos,
		queryChannelInfos: queryChannelInfos,
	}

	err := m.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MetaReplica) reloadFromKV() error {
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

func (m *MetaReplica) showCollections() []*querypb.CollectionInfo {
	m.RLock()
	defer m.RUnlock()

	collections := make([]*querypb.CollectionInfo, 0)
	for _, info := range m.collectionInfos {
		collections = append(collections, proto.Clone(info).(*querypb.CollectionInfo))
	}
	return collections
}

func (m *MetaReplica) showPartitions(collectionID UniqueID) ([]*querypb.PartitionStates, error) {
	m.RLock()
	defer m.RUnlock()

	//TODO::should update after load collection
	results := make([]*querypb.PartitionStates, 0)
	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, state := range info.PartitionStates {
			results = append(results, proto.Clone(state).(*querypb.PartitionStates))
		}
		return results, nil
	}

	return nil, errors.New("showPartitions: can't find collection in collectionInfos")
}

func (m *MetaReplica) hasCollection(collectionID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.collectionInfos[collectionID]; ok {
		return true
	}

	return false
}

func (m *MetaReplica) hasPartition(collectionID UniqueID, partitionID UniqueID) bool {
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

func (m *MetaReplica) hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool {
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

func (m *MetaReplica) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.collectionInfos[collectionID]; !ok {
		partitions := make([]UniqueID, 0)
		partitionStates := make([]*querypb.PartitionStates, 0)
		channels := make([]*querypb.DmChannelInfo, 0)
		newCollection := &querypb.CollectionInfo{
			CollectionID:    collectionID,
			PartitionIDs:    partitions,
			PartitionStates: partitionStates,
			ChannelInfos:    channels,
			Schema:          schema,
		}
		m.collectionInfos[collectionID] = newCollection
		err := saveGlobalCollectionInfo(collectionID, newCollection, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	}

	return nil
}

func (m *MetaReplica) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	if col, ok := m.collectionInfos[collectionID]; ok {
		log.Debug("add a  partition to MetaReplica...", zap.Int64s("partitionIDs", col.PartitionIDs))
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return nil
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
		col.PartitionStates = append(col.PartitionStates, &querypb.PartitionStates{
			PartitionID: partitionID,
			State:       querypb.PartitionState_NotPresent,
		})

		log.Debug("add a  partition to MetaReplica", zap.Int64s("partitionIDs", col.PartitionIDs))
		err := saveGlobalCollectionInfo(collectionID, col, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (m *MetaReplica) deleteSegmentInfoByID(segmentID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.segmentInfos[segmentID]; ok {
		err := removeSegmentInfo(segmentID, m.client)
		if err != nil {
			log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
			return err
		}
		delete(m.segmentInfos, segmentID)
	}

	return nil
}

func (m *MetaReplica) deleteSegmentInfoByNodeID(nodeID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	for segmentID, info := range m.segmentInfos {
		if info.NodeID == nodeID {
			err := removeSegmentInfo(segmentID, m.client)
			if err != nil {
				log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
				return err
			}
			delete(m.segmentInfos, segmentID)
		}
	}

	return nil
}

func (m *MetaReplica) setSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) error {
	m.Lock()
	defer m.Unlock()

	err := saveSegmentInfo(segmentID, info, m.client)
	if err != nil {
		log.Error("save segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", segmentID))
		return err
	}
	m.segmentInfos[segmentID] = info
	return nil
}

func (m *MetaReplica) showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo {
	m.RLock()
	defer m.RUnlock()

	results := make([]*querypb.SegmentInfo, 0)
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, info := range m.segmentInfos {
		if info.CollectionID == collectionID {
			segmentInfos = append(segmentInfos, proto.Clone(info).(*querypb.SegmentInfo))
		}
	}
	if len(partitionIDs) == 0 {
		return segmentInfos
	}
	for _, info := range segmentInfos {
		for _, partitionID := range partitionIDs {
			if info.PartitionID == partitionID {
				results = append(results, info)
			}
		}
	}
	return results
}

func (m *MetaReplica) hasSegmentInfo(segmentID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.segmentInfos[segmentID]; ok {
		return true
	}

	return false
}

func (m *MetaReplica) getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.segmentInfos[segmentID]; ok {
		return proto.Clone(info).(*querypb.SegmentInfo), nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
}

func (m *MetaReplica) getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.CollectionInfo), nil
	}

	return nil, errors.New("getCollectionInfoByID: can't find collectionID in collectionInfo")
}

func (m *MetaReplica) getQueryChannelInfoByID(collectionID UniqueID) (*querypb.QueryChannelInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.queryChannelInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.QueryChannelInfo), nil
	}

	return nil, errors.New("getQueryChannelInfoByID: can't find collectionID in queryChannelInfo")
}

func (m *MetaReplica) getPartitionStatesByID(collectionID UniqueID, partitionID UniqueID) (*querypb.PartitionStates, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for offset, id := range info.PartitionIDs {
			if id == partitionID {
				return proto.Clone(info.PartitionStates[offset]).(*querypb.PartitionStates), nil
			}
		}
		return nil, errors.New("getPartitionStateByID: can't find partitionID in partitionStates")
	}

	return nil, errors.New("getPartitionStateByID: can't find collectionID in collectionInfo")
}

func (m *MetaReplica) releaseCollection(collectionID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	delete(m.collectionInfos, collectionID)
	for id, info := range m.segmentInfos {
		if info.CollectionID == collectionID {
			err := removeSegmentInfo(id, m.client)
			if err != nil {
				log.Error("remove segmentInfo error", zap.Any("error", err.Error()), zap.Int64("segmentID", id))
				return err
			}
			delete(m.segmentInfos, id)
		}
	}

	delete(m.queryChannelInfos, collectionID)
	err := removeGlobalCollectionInfo(collectionID, m.client)
	if err != nil {
		log.Error("remove collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		return err
	}

	return nil
}

func (m *MetaReplica) releasePartition(collectionID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		newPartitionIDs := make([]UniqueID, 0)
		newPartitionStates := make([]*querypb.PartitionStates, 0)
		for offset, id := range info.PartitionIDs {
			if id != partitionID {
				newPartitionIDs = append(newPartitionIDs, id)
				newPartitionStates = append(newPartitionStates, info.PartitionStates[offset])
			}
		}
		info.PartitionIDs = newPartitionIDs
		info.PartitionStates = newPartitionStates

		releasedPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.ReleasedPartitionIDs {
			if id != partitionID {
				releasedPartitionIDs = append(releasedPartitionIDs, id)
			}
		}
		releasedPartitionIDs = append(releasedPartitionIDs, partitionID)
		info.ReleasedPartitionIDs = releasedPartitionIDs

		// If user loaded a collectionA, and release a partitionB which belongs to collectionA,
		// and then load collectionA again, if we don't set the inMemoryPercentage to 0 when releasing
		// partitionB, the second loading of collectionA would directly return because
		// the inMemoryPercentage in ShowCollection response is still the old value -- 100.
		// So if releasing partition, inMemoryPercentage should be set to 0.
		info.InMemoryPercentage = 0

		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	}
	for id, info := range m.segmentInfos {
		if info.PartitionID == partitionID {
			err := removeSegmentInfo(id, m.client)
			if err != nil {
				log.Error("delete segmentInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID), zap.Int64("segmentID", id))
				return err
			}
			delete(m.segmentInfos, id)
		}
	}

	return nil
}

func (m *MetaReplica) getDmChannelsByNodeID(collectionID UniqueID, nodeID int64) ([]string, error) {
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

func (m *MetaReplica) addDmChannel(collectionID UniqueID, nodeID int64, channels []string) error {
	m.Lock()
	defer m.Unlock()

	//before add channel, should ensure toAddedChannels not in MetaReplica
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

		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		return nil
	}

	return errors.New("addDmChannels: can't find collection in collectionInfos")
}

func (m *MetaReplica) removeDmChannel(collectionID UniqueID, nodeID int64, channels []string) error {
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

		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}

		return nil
	}

	return errors.New("addDmChannels: can't find collection in collectionInfos")
}

func (m *MetaReplica) GetQueryChannel(collectionID UniqueID) (string, string) {
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

func (m *MetaReplica) setLoadType(collectionID UniqueID, loadType querypb.LoadType) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		info.LoadType = loadType
		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		return nil
	}

	return errors.New("setLoadType: can't find collection in collectionInfos")
}

func (m *MetaReplica) getLoadType(collectionID UniqueID) (querypb.LoadType, error) {
	m.RLock()
	defer m.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return info.LoadType, nil
	}

	return 0, errors.New("getLoadType: can't find collection in collectionInfos")
}

func (m *MetaReplica) setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error {
	m.Lock()
	defer m.Unlock()

	info, ok := m.collectionInfos[collectionID]
	if !ok {
		return errors.New("setLoadPercentage: can't find collection in collectionInfos")
	}

	if loadType == querypb.LoadType_loadCollection {
		info.InMemoryPercentage = percentage
		for _, partitionState := range info.PartitionStates {
			if percentage >= 100 {
				partitionState.State = querypb.PartitionState_InMemory
			} else {
				partitionState.State = querypb.PartitionState_PartialInMemory
			}
			partitionState.InMemoryPercentage = percentage
		}
		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	} else {
		for _, partitionState := range info.PartitionStates {
			if partitionState.PartitionID == partitionID {
				if percentage >= 100 {
					partitionState.State = querypb.PartitionState_InMemory
				} else {
					partitionState.State = querypb.PartitionState_PartialInMemory
				}
				partitionState.InMemoryPercentage = percentage
				err := saveGlobalCollectionInfo(collectionID, info, m.client)
				if err != nil {
					log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
					return err
				}
				return nil
			}
		}
		return errors.New("setLoadPercentage: can't find partitionID in collectionInfos")
	}

	return nil
}

func (m *MetaReplica) printMeta() {
	m.RLock()
	defer m.RUnlock()
	for id, info := range m.collectionInfos {
		log.Debug("query coordinator MetaReplica: collectionInfo", zap.Int64("collectionID", id), zap.Any("info", info))
	}

	for id, info := range m.segmentInfos {
		log.Debug("query coordinator MetaReplica: segmentInfo", zap.Int64("segmentID", id), zap.Any("info", info))
	}

	for id, info := range m.queryChannelInfos {
		log.Debug("query coordinator MetaReplica: queryChannelInfo", zap.Int64("collectionID", id), zap.Any("info", info))
	}
}

func saveGlobalCollectionInfo(collectionID UniqueID, info *querypb.CollectionInfo, kv *etcdkv.EtcdKV) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return kv.Save(key, infoBytes)
}

func removeGlobalCollectionInfo(collectionID UniqueID, kv *etcdkv.EtcdKV) error {
	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return kv.Remove(key)
}

func saveSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo, kv *etcdkv.EtcdKV) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", segmentMetaPrefix, segmentID)
	return kv.Save(key, infoBytes)
}

func removeSegmentInfo(segmentID UniqueID, kv *etcdkv.EtcdKV) error {
	key := fmt.Sprintf("%s/%d", segmentMetaPrefix, segmentID)
	return kv.Remove(key)
}

func saveQueryChannelInfo(collectionID UniqueID, info *querypb.QueryChannelInfo, kv *etcdkv.EtcdKV) error {
	infoBytes := proto.MarshalTextString(info)

	key := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
	return kv.Save(key, infoBytes)
}

func removeQueryChannelInfo(collectionID UniqueID, kv *etcdkv.EtcdKV) error {
	key := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
	return kv.Remove(key)
}
