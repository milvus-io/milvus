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
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type meta struct {
	sync.RWMutex
	collectionInfos map[UniqueID]*querypb.CollectionInfo
	segmentInfos map[UniqueID]*querypb.SegmentInfo
	queryChannelInfo

	partitionStates map[UniqueID]querypb.PartitionState
}

func newMeta() *meta {
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	partitionStates := make(map[UniqueID]querypb.PartitionState)
	return &meta{
		collectionInfos: collectionInfos,
		segmentInfos: segmentInfos,
		partitionStates: partitionStates,
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
			Id:                collectionID,
			PartitionIDs:        partitions,
			ChannelInfos:            channels,
			Schema:        schema,
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
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return errors.New("addPartition: partition already exists in collectionInfos")
			}
		}
		col.PartitionIDs = append(col.PartitionIDs, partitionID)
		m.partitionStates[partitionID] = querypb.PartitionState_NotPresent
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (m *meta) deleteSegmentInfoByID(segmentID UniqueID) error {
	m.Lock()
	m.Unlock()
	if _, ok := m.segmentInfos[segmentID]; ok {
		delete(m.segmentInfos, segmentID)
		return nil
	}

	return errors.New("deleteSegmentInfoByID: can't find segmentID in segmentInfos")
}

func (m *meta)setSegmentInfo(segmentID UniqueID, info *querypb.SegmentInfo) {
	m.Lock()
	defer m.Unlock()

	m.segmentInfos[segmentID] = info
}

func (m *meta) getSegmentInfos(segmentIDs []UniqueID) ([]*querypb.SegmentInfo, error) {
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _,segmentID := range segmentIDs {
		if info, ok := m.segmentInfos[segmentID]; ok {
			segmentInfos = append(segmentInfos, info)
			continue
		}
		return nil, errors.New("segment not exist")
	}
	return segmentInfos, nil
}

func (m *meta)getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.segmentInfos[segmentID]; ok {
		return info, nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
}


//func (m *meta) getCollections(dbID UniqueID) ([]*collection, error) {
//	m.RLock()
//	defer m.RUnlock()
//	if collections, ok := m.db2collections[dbID]; ok {
//		return collections, nil
//	}
//
//	return nil, errors.New("getCollections: can't find collectionID")
//}
//
//func (m *meta) getPartitions(dbID UniqueID, collectionID UniqueID) ([]*partition, error) {
//	m.RLock()
//	defer m.RUnlock()
//	if collections, ok := m.db2collections[dbID]; ok {
//		for _, collection := range collections {
//			if collectionID == collection.id {
//				partitions := make([]*partition, 0)
//				for _, partition := range collection.partitions {
//					partitions = append(partitions, partition)
//				}
//				return partitions, nil
//			}
//		}
//	}
//
//	return nil, errors.New("getPartitions: can't find partitionIDs")
//}
//
//func (m *meta) getSegments(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]UniqueID, error) {
//	m.RLock()
//	defer m.RUnlock()
//	if collections, ok := m.db2collections[dbID]; ok {
//		for _, collection := range collections {
//			if collectionID == collection.id {
//				if partition, ok := collection.partitions[partitionID]; ok {
//					segments := make([]UniqueID, 0)
//					segments = append(segments, partition.sealedSegments...)
//					segments = append(segments, partition.growingSegments...)
//					//for _, segment := range partition.segments {
//					//	segments = append(segments, segment)
//					//}
//					return segments, nil
//				}
//			}
//		}
//	}
//	return nil, errors.New("getSegments: can't find segmentID")
//}
//
//func (m *meta) getCollectionByID(dbID UniqueID, collectionID UniqueID) (*collection, error) {
//	m.RLock()
//	defer m.RUnlock()
//	if collections, ok := m.db2collections[dbID]; ok {
//		for _, collection := range collections {
//			if collectionID == collection.id {
//				return collection, nil
//			}
//		}
//	}
//
//	return nil, errors.New("getCollectionByID: can't find collectionID")
//}
//
//func (m *meta) getPartitionByID(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error) {
//	m.RLock()
//	defer m.RUnlock()
//	if collections, ok := m.db2collections[dbID]; ok {
//		for _, collection := range collections {
//			if collectionID == collection.id {
//				partitions := collection.partitions
//				if partition, ok := partitions[partitionID]; ok {
//					return partition, nil
//				}
//			}
//		}
//	}
//
//	return nil, errors.New("getPartitionByID: can't find partitionID")
//}

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

func (m *meta) deleteCollection(collectionID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, partitionID := range info.PartitionIDs {
			delete(m.partitionStates, partitionID)
		}

		delete(m.collectionInfos, collectionID)
		for id, info := range m.segmentInfos {
			if info.CollectionID == collectionID {
				delete(m.segmentInfos, id)
			}
		}
	}

	return errors.New("releaseCollection: can't find collection in collectionInfos")
}

func (m *meta) deletePartition(collectionID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		findPartition := false
		for _, id := range info.PartitionIDs {
			if id == partitionID {
				findPartition = true
				delete(m.partitionStates, partitionID)
			}
		}
		if !findPartition {
			return errors.New("releasePartition: can't find partition in partitionInfos")
		}
		for id, info := range m.segmentInfos {
			if info.PartitionID == partitionID {
				delete(m.segmentInfos, id)
			}
		}
	}

	return errors.New("releasePartition: can't find collection in collectionInfos")
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

func (m *meta) setQueryChannel(collectionID UniqueID, queryChannel string, queryResultChannel string) error {
	m.Lock()
	defer m.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		info.QueryChannelID = queryChannel
		info.QueryResultChannelID = queryResultChannel
	}

	return errors.New("setQueryChannel: can't find collection in collectionInfos")
}

//func (mp *meta) addExcludeSegmentIDs(dbID UniqueID, collectionID UniqueID, excludeSegments []UniqueID) error {
//	if collections, ok := mp.db2collections[dbID]; ok {
//		for _, collection := range collections {
//			if collectionID == collection.id {
//				collection.excludeSegmentIds = append(collection.excludeSegmentIds, excludeSegments...)
//				return nil
//			}
//		}
//	}
//	return errors.New("addExcludeSegmentIDs: can't find dbID or collectionID")
//}
