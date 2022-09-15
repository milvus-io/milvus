package utils

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func CreateTestLeaderView(id, collection int64, channel string, segments map[int64]int64, growings []int64) *meta.LeaderView {
	return &meta.LeaderView{
		ID:              id,
		CollectionID:    collection,
		Channel:         channel,
		Segments:        segments,
		GrowingSegments: typeutil.NewUniqueSet(growings...),
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
			ReplicaNumber: 3,
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
