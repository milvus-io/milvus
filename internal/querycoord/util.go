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
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func addDmChannel(nodeID int64, collectionInfo *querypb.CollectionInfo, watchInfo *querypb.DmChannelWatchInfo) {
	nodeExistInWatchInfo := false
	for _, dmChannelInfo := range collectionInfo.ChannelInfos {
		if dmChannelInfo.NodeIDLoaded == nodeID {
			nodeExistInWatchInfo = true
			var newWatchedChannelInfo []*querypb.DmChannelWatchInfo
			hasWatchedChannel := false
			for _, watchedChannelInfo := range dmChannelInfo.WatchInfos {
				if watchedChannelInfo.ChannelName == watchInfo.ChannelName {
					hasWatchedChannel = true
					// watchedChannelInfo.WatchType == watchCollection
					// query node's watch don't change, so skip change watchInfo
					if watchedChannelInfo.WatchType == querypb.WatchType_WatchCollection {
						newWatchedChannelInfo = append(newWatchedChannelInfo, watchedChannelInfo)
						continue
					}

					// watchedChannelInfo.WatchType == watchPartition && watchInfo.WatchType == watchCollection
					if watchInfo.WatchType == querypb.WatchType_WatchCollection {
						newWatchedChannelInfo = append(newWatchedChannelInfo, watchInfo)
						continue
					}

					// watchedChannelInfo.WatchType == watchPartition && watchInfo.WatchType == watchPartition
					// query node add watch some partition's dmChannels
					for _, partitionID := range watchInfo.PartitionIDs {
						toAdd := true
						for _, watchedPartitionID := range watchedChannelInfo.PartitionIDs {
							if watchedPartitionID == partitionID {
								toAdd = false
								break
							}
						}
						if toAdd {
							watchedChannelInfo.PartitionIDs = append(watchedChannelInfo.PartitionIDs, partitionID)
						}
					}
					newWatchedChannelInfo = append(newWatchedChannelInfo, watchedChannelInfo)
				} else {
					newWatchedChannelInfo = append(newWatchedChannelInfo, watchedChannelInfo)
				}
			}

			if !hasWatchedChannel {
				newWatchedChannelInfo = append(newWatchedChannelInfo, watchInfo)
			}

			dmChannelInfo.WatchInfos = newWatchedChannelInfo
		}
	}

	if !nodeExistInWatchInfo {
		collectionInfo.ChannelInfos = append(collectionInfo.ChannelInfos, &querypb.DmChannelInfo{
			NodeIDLoaded: nodeID,
			WatchInfos:   []*querypb.DmChannelWatchInfo{watchInfo},
		})
	}
}

func removeDmChannel(nodeID int64, collectionInfo *querypb.CollectionInfo, watchInfo *querypb.DmChannelWatchInfo) {
	var newDmChannelInfos []*querypb.DmChannelInfo
	for _, dmChannelInfo := range collectionInfo.ChannelInfos {
		var newWatchedInfos []*querypb.DmChannelWatchInfo
		for _, watchedChannelInfo := range dmChannelInfo.WatchInfos {
			if dmChannelInfo.NodeIDLoaded == nodeID && watchInfo.ChannelName == watchedChannelInfo.ChannelName {
				// query node remove watch collection's dmChannels
				if watchInfo.WatchType == querypb.WatchType_WatchCollection {
					continue
				}

				// watchedChannelInfo.WatchType == watchCollection && watchInfo.WatchType == watchPartition
				// query node's watch don't change, so skip change watchInfo
				if watchedChannelInfo.WatchType == querypb.WatchType_WatchCollection {
					newWatchedInfos = append(newWatchedInfos, watchedChannelInfo)
					continue
				}

				// watchedChannelInfo.WatchType == watchPartition && watchInfo.WatchType == watchPartition
				// query node remove watch some partition's dmChannels,
				var newPartitionIDs []UniqueID
				for _, watchedPartitionID := range watchedChannelInfo.PartitionIDs {
					toRemove := false
					for _, partitionID := range watchInfo.PartitionIDs {
						if watchedPartitionID == partitionID {
							toRemove = true
							break
						}
					}
					if !toRemove {
						newPartitionIDs = append(newPartitionIDs, watchedPartitionID)
					}
				}
				if len(newPartitionIDs) != 0 {
					watchedChannelInfo.PartitionIDs = newPartitionIDs
					newWatchedInfos = append(newWatchedInfos, watchedChannelInfo)
				}
			} else {
				newWatchedInfos = append(newWatchedInfos, watchedChannelInfo)
			}
		}
		if len(newWatchedInfos) != 0 {
			dmChannelInfo.WatchInfos = newWatchedInfos
			newDmChannelInfos = append(newDmChannelInfos, dmChannelInfo)
		}
	}

	collectionInfo.ChannelInfos = newDmChannelInfos
}

func removeDmChannelByPartitionID(collectionInfo *querypb.CollectionInfo, partitionID UniqueID) {
	var newDmChannelInfos []*querypb.DmChannelInfo
	for _, dmChannelInfo := range collectionInfo.ChannelInfos {
		var newWatchedInfos []*querypb.DmChannelWatchInfo
		for _, watchedChannelInfo := range dmChannelInfo.WatchInfos {
			if watchedChannelInfo.WatchType == querypb.WatchType_WatchPartition {
				// query node remove watch some partition's dmChannels,
				var newPartitionIDs []UniqueID
				for _, watchedPartitionID := range watchedChannelInfo.PartitionIDs {
					if watchedPartitionID != partitionID {
						newPartitionIDs = append(newPartitionIDs, watchedPartitionID)
					}
				}
				// if len(newPartitionIDs) == 0, the watchedChannelInfo is invalid
				if len(newPartitionIDs) != 0 {
					watchedChannelInfo.PartitionIDs = newPartitionIDs
					newWatchedInfos = append(newWatchedInfos, watchedChannelInfo)
				}
			} else {
				newWatchedInfos = append(newWatchedInfos, watchedChannelInfo)
			}
		}
		// keep the dmChannelInfo if watchInfo is not empty
		// if len(newWatchedInfos) == 0, the dmChannelInfo is invalid
		if len(newWatchedInfos) != 0 {
			dmChannelInfo.WatchInfos = newWatchedInfos
			newDmChannelInfos = append(newDmChannelInfos, dmChannelInfo)
		}
	}
	collectionInfo.ChannelInfos = newDmChannelInfos
}
