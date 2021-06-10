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

package queryservice

import (
	"errors"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type meta struct {
	sync.RWMutex
	collectionInfos   map[UniqueID]*querypb.CollectionInfo
	segmentInfos      map[UniqueID]*querypb.SegmentInfo
	queryChannelInfos map[UniqueID]*querypb.QueryChannelInfo

	partitionStates map[UniqueID]querypb.PartitionState
}

func newMeta() *meta {
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	queryChannelInfos := make(map[UniqueID]*querypb.QueryChannelInfo)
	partitionStates := make(map[UniqueID]querypb.PartitionState)
	return &meta{
		collectionInfos:   collectionInfos,
		segmentInfos:      segmentInfos,
		queryChannelInfos: queryChannelInfos,
		partitionStates:   partitionStates,
	}
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
		m.partitionStates[partitionID] = querypb.PartitionState_NotPresent
		log.Debug("add a  partition to meta", zap.Int64s("partitionIDs", col.PartitionIDs))
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (m *meta) deleteSegmentInfoByID(segmentID UniqueID) {
	m.Lock()
	m.Unlock()
	delete(m.segmentInfos, segmentID)
}

func (m *meta) setSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) {
	m.Lock()
	defer m.Unlock()

	m.segmentInfos[segmentID] = info
}

func (m *meta) getSegmentInfos(segmentIDs []UniqueID) ([]*querypb.SegmentInfo, error) {
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, segmentID := range segmentIDs {
		if info, ok := m.segmentInfos[segmentID]; ok {
			segmentInfos = append(segmentInfos, info)
			continue
		}
		return nil, errors.New("segment not exist")
	}
	return segmentInfos, nil
}

func (m *meta) getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.segmentInfos[segmentID]; ok {
		return info, nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
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
			delete(m.segmentInfos, id)
		}
	}
	delete(m.queryChannelInfos, collectionID)
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
	}
	for id, info := range m.segmentInfos {
		if info.PartitionID == partitionID {
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
	log.Debug("query service create query channel", zap.String("queryChannelName", allocatedQueryChannel), zap.String("queryResultChannelName", allocatedQueryResultChannel))

	queryChannelInfo := &querypb.QueryChannelInfo{
		CollectionID:         collectionID,
		QueryChannelID:       allocatedQueryChannel,
		QueryResultChannelID: allocatedQueryResultChannel,
	}
	m.queryChannelInfos[collectionID] = queryChannelInfo
	//TODO::return channel according collectionID
	return allocatedQueryChannel, allocatedQueryResultChannel
}
