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

package utils

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func CreateTestLeaderView(id, collection int64, channel string, segments map[int64]int64, growings map[int64]*meta.Segment) *meta.LeaderView {
	segmentVersions := make(map[int64]*querypb.SegmentDist)
	for segment, node := range segments {
		segmentVersions[segment] = &querypb.SegmentDist{
			NodeID:  node,
			Version: 0,
		}
	}
	return &meta.LeaderView{
		ID:              id,
		CollectionID:    collection,
		Channel:         channel,
		Segments:        segmentVersions,
		GrowingSegments: growings,
	}
}

func CreateTestChannel(collection, node, version int64, channel string) *meta.DmChannel {
	return &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel,
		},
		Node:    node,
		Version: version,
	}
}

func CreateTestReplica(id, collectionID int64, nodes []int64) *meta.Replica {
	return &meta.Replica{
		Replica: &querypb.Replica{
			ID:           id,
			CollectionID: collectionID,
			Nodes:        nodes,
		},
		Nodes: typeutil.NewUniqueSet(nodes...),
	}
}

func CreateTestCollection(collection int64, replica int32) *meta.Collection {
	return &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collection,
			ReplicaNumber: replica,
		},
	}
}

func CreateTestPartition(collection int64, partitionID int64) *meta.Partition {
	return &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collection,
			PartitionID:  partitionID,
		},
	}
}

func CreateTestSegmentInfo(collection, partition, segment int64, channel string) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            segment,
		CollectionID:  collection,
		PartitionID:   partition,
		InsertChannel: channel,
	}
}

func CreateTestSegment(collection, partition, segment, node, version int64, channel string) *meta.Segment {
	return &meta.Segment{
		SegmentInfo: CreateTestSegmentInfo(collection, partition, segment, channel),
		Node:        node,
		Version:     version,
	}
}
