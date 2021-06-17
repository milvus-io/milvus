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
package queryservice

import (
	"errors"
	"fmt"
	"sync"
	"time"

	nodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
)

type queryNode struct {
	id      int64
	address struct {
		ip   string
		port int64
	}
	client types.QueryNode

	//mu       sync.Mutex // guards segments and channels2Col
	//nodeMeta *meta
	sync.RWMutex
	collectionInfos      map[UniqueID]*querypb.CollectionInfo
	watchedQueryChannels map[UniqueID]*querypb.QueryChannelInfo
	//segments     map[UniqueID][]UniqueID
	//channels2Col map[UniqueID][]string
}

func newQueryNode(ip string, port int64, id UniqueID) (*queryNode, error) {
	client, err := nodeclient.NewClient(fmt.Sprintf("%s:%d", ip, port), 3*time.Second)
	if err != nil {
		return nil, err
	}
	if err := client.Init(); err != nil {
		return nil, err
	}

	if err := client.Start(); err != nil {
		return nil, err
	}
	collectionInfo := make(map[UniqueID]*querypb.CollectionInfo)
	watchedChannels := make(map[UniqueID]*querypb.QueryChannelInfo)
	return &queryNode{
		id: id,
		address: struct {
			ip   string
			port int64
		}{ip: ip, port: port},
		client:               client,
		collectionInfos:      collectionInfo,
		watchedQueryChannels: watchedChannels,
		//nodeMeta: newMetaReplica(),
	}, nil
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
		return nil
	}

	return errors.New("addCollection: collection already exists")
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
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (qn *queryNode) releaseCollection(collectionID UniqueID) {
	qn.Lock()
	defer qn.Unlock()
	delete(qn.collectionInfos, collectionID)
	//TODO::should reopen
	//collectionID = 0
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
	}

	return errors.New("addDmChannels: can't find collection in watchedQueryChannel")
}

//TODO::removeDmChannels

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
	return nil
}
