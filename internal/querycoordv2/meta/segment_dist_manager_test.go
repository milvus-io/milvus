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

package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
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
		segments := dist.GetByFilter(WithNodeID(node))
		suite.AssertNode(segments, node)
	}

	// Test GetByShard
	for _, shard := range []string{"dmc0", "dmc1"} {
		segments := dist.GetByFilter(WithChannel(shard))
		suite.AssertShard(segments, shard)
	}

	// Test GetByCollection
	segments := dist.GetByFilter(WithCollectionID(suite.collection))
	suite.Len(segments, 8)
	suite.AssertCollection(segments, suite.collection)
	segments = dist.GetByFilter(WithCollectionID(-1))
	suite.Len(segments, 0)

	// Test GetByNodeAndCollection
	// 1. Valid node and valid collection
	for _, node := range suite.nodes {
		segments := dist.GetByFilter(WithCollectionID(suite.collection), WithNodeID(node))
		suite.AssertNode(segments, node)
		suite.AssertCollection(segments, suite.collection)
	}

	// 2. Valid node and invalid collection
	segments = dist.GetByFilter(WithCollectionID(-1), WithNodeID(suite.nodes[1]))
	suite.Len(segments, 0)

	// 3. Invalid node and valid collection
	segments = dist.GetByFilter(WithCollectionID(suite.collection), WithNodeID(-1))
	suite.Len(segments, 0)

	// Test GetBy With Wrong Replica
	replica := newReplica(&querypb.Replica{
		ID:           1,
		CollectionID: suite.collection + 1,
		Nodes:        []int64{suite.nodes[0]},
	})
	segments = dist.GetByFilter(WithReplica(replica))
	suite.Len(segments, 0)

	// Test GetBy With Correct Replica
	replica = newReplica(&querypb.Replica{
		ID:           1,
		CollectionID: suite.collection,
		Nodes:        []int64{suite.nodes[0]},
	})
	segments = dist.GetByFilter(WithReplica(replica))
	suite.Len(segments, 2)
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

func TestGetSegmentDistJSON(t *testing.T) {
	// Initialize SegmentDistManager
	manager := NewSegmentDistManager()

	// Add some segments to the SegmentDistManager
	segment1 := SegmentFromInfo(&datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: "channel-1",
		NumOfRows:     1000,
		State:         commonpb.SegmentState_Flushed,
	})
	segment1.Node = 1
	segment1.Version = 1

	segment2 := SegmentFromInfo(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  200,
		PartitionID:   20,
		InsertChannel: "channel-2",
		NumOfRows:     2000,
		State:         commonpb.SegmentState_Flushed,
	})
	segment2.Node = 2
	segment2.Version = 1

	manager.Update(1, segment1)
	manager.Update(2, segment2)

	segments := manager.GetSegmentDist(0)
	assert.Equal(t, 2, len(segments))

	checkResults := func(s *metricsinfo.Segment) {
		if s.SegmentID == 1 {
			assert.Equal(t, int64(100), s.CollectionID)
			assert.Equal(t, int64(10), s.PartitionID)
			assert.Equal(t, "channel-1", s.Channel)
			assert.Equal(t, int64(1000), s.NumOfRows)
			assert.Equal(t, "Flushed", s.State)
			assert.Equal(t, int64(1), s.NodeID)
		} else if s.SegmentID == 2 {
			assert.Equal(t, int64(200), s.CollectionID)
			assert.Equal(t, int64(20), s.PartitionID)
			assert.Equal(t, "channel-2", s.Channel)
			assert.Equal(t, int64(2000), s.NumOfRows)
			assert.Equal(t, "Flushed", s.State)
			assert.Equal(t, int64(2), s.NodeID)
		} else {
			assert.Failf(t, "unexpected segment id", "unexpected segment id %d", s.SegmentID)
		}
	}

	for _, s := range segments {
		checkResults(s)
	}
}
