package meta

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/suite"
)

type SegmentDistManagerSuite struct {
	suite.Suite
	dist       *SegmentDistManager
	collection int64
	partitions []int64
	nodes      []int64
	segments   map[int64]*Segment
}

func (suite *SegmentDistManagerSuite) SetupSuite() {
	const (
		shardNum = 2
	)
	// Replica 0: 0, 2
	// Replica 1: 1
	suite.collection = 10
	suite.partitions = []int64{1, 2}
	suite.nodes = []int64{0, 1, 2}
	suite.segments = map[int64]*Segment{
		1: SegmentFromInfo(&datapb.SegmentInfo{
			ID:            1,
			CollectionID:  suite.collection,
			PartitionID:   suite.partitions[0],
			InsertChannel: "dmc0",
		}),
		2: SegmentFromInfo(&datapb.SegmentInfo{
			ID:            2,
			CollectionID:  suite.collection,
			PartitionID:   suite.partitions[0],
			InsertChannel: "dmc1",
		}),
		3: SegmentFromInfo(&datapb.SegmentInfo{
			ID:            3,
			CollectionID:  suite.collection,
			PartitionID:   suite.partitions[1],
			InsertChannel: "dmc0",
		}),
		4: SegmentFromInfo(&datapb.SegmentInfo{
			ID:            4,
			CollectionID:  suite.collection,
			PartitionID:   suite.partitions[1],
			InsertChannel: "dmc1",
		}),
	}
}

func (suite *SegmentDistManagerSuite) SetupTest() {
	suite.dist = NewSegmentDistManager()
	// Distribution:
	// node 0 contains channel segment 1, 2
	// node 1 contains channel segment 1, 2, 3, 4
	// node 2 contains channel segment 3, 4
	suite.dist.Update(suite.nodes[0], suite.segments[1].Clone(), suite.segments[2].Clone())
	suite.dist.Update(suite.nodes[1],
		suite.segments[1].Clone(),
		suite.segments[2].Clone(),
		suite.segments[3].Clone(),
		suite.segments[4].Clone())
	suite.dist.Update(suite.nodes[2], suite.segments[3].Clone(), suite.segments[4].Clone())
}

func (suite *SegmentDistManagerSuite) TestGetBy() {
	dist := suite.dist
	// Test GetByNode
	for _, node := range suite.nodes {
		segments := dist.GetByNode(node)
		suite.AssertNode(segments, node)
	}

	// Test GetByShard
	for _, shard := range []string{"dmc0", "dmc1"} {
		segments := dist.GetByShard(shard)
		suite.AssertShard(segments, shard)
	}

	// Test GetByCollection
	segments := dist.GetByCollection(suite.collection)
	suite.Len(segments, 8)
	suite.AssertCollection(segments, suite.collection)
	segments = dist.GetByCollection(-1)
	suite.Len(segments, 0)

	// Test GetByNodeAndCollection
	// 1. Valid node and valid collection
	for _, node := range suite.nodes {
		segments := dist.GetByCollectionAndNode(suite.collection, node)
		suite.AssertNode(segments, node)
		suite.AssertCollection(segments, suite.collection)
	}

	// 2. Valid node and invalid collection
	segments = dist.GetByCollectionAndNode(-1, suite.nodes[1])
	suite.Len(segments, 0)

	// 3. Invalid node and valid collection
	segments = dist.GetByCollectionAndNode(suite.collection, -1)
	suite.Len(segments, 0)
}

func (suite *SegmentDistManagerSuite) AssertIDs(segments []*Segment, ids ...int64) bool {
	for _, segment := range segments {
		hasSegment := false
		for _, id := range ids {
			if segment.ID == id {
				hasSegment = true
				break
			}
		}
		if !suite.True(hasSegment, "segment %v not in the given expected list %+v", segment.GetID(), ids) {
			return false
		}
	}
	return true
}

func (suite *SegmentDistManagerSuite) AssertNode(segments []*Segment, node int64) bool {
	for _, segment := range segments {
		if !suite.Equal(node, segment.Node) {
			return false
		}
	}
	return true
}

func (suite *SegmentDistManagerSuite) AssertCollection(segments []*Segment, collection int64) bool {
	for _, segment := range segments {
		if !suite.Equal(collection, segment.GetCollectionID()) {
			return false
		}
	}
	return true
}

func (suite *SegmentDistManagerSuite) AssertShard(segments []*Segment, shard string) bool {
	for _, segment := range segments {
		if !suite.Equal(shard, segment.GetInsertChannel()) {
			return false
		}
	}
	return true
}

func TestSegmentDistManager(t *testing.T) {
	suite.Run(t, new(SegmentDistManagerSuite))
}
