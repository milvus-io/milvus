package meta

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/suite"
)

type TargetManagerSuite struct {
	suite.Suite

	// Data
	collections []int64
	partitions  map[int64][]int64
	channels    map[int64][]string
	segments    map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	// Derived data
	allChannels []string
	allSegments []int64

	// Test object
	mgr *TargetManager
}

func (suite *TargetManagerSuite) SetupSuite() {
	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101},
		1001: {102, 103},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
		},
		1001: {
			102: {5, 6},
			103: {7, 8},
		},
	}

	suite.allChannels = make([]string, 0)
	suite.allSegments = make([]int64, 0)
	for _, channels := range suite.channels {
		suite.allChannels = append(suite.allChannels, channels...)
	}
	for _, partitions := range suite.segments {
		for _, segments := range partitions {
			suite.allSegments = append(suite.allSegments, segments...)
		}
	}
}

func (suite *TargetManagerSuite) SetupTest() {
	suite.mgr = NewTargetManager()
	for collection, channels := range suite.channels {
		for _, channel := range channels {
			suite.mgr.AddDmChannel(DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channel,
			}))
		}
	}
	for collection, partitions := range suite.segments {
		for partition, segments := range partitions {
			for _, segment := range segments {
				suite.mgr.AddSegment(&datapb.SegmentInfo{
					ID:           segment,
					CollectionID: collection,
					PartitionID:  partition,
				})
			}
		}
	}
}

func (suite *TargetManagerSuite) TestGet() {
	mgr := suite.mgr

	for collection, channels := range suite.channels {
		results := mgr.GetDmChannelsByCollection(collection)
		suite.assertChannels(channels, results)
		for _, channel := range channels {
			suite.True(mgr.ContainDmChannel(channel))
		}
	}

	for collection, partitions := range suite.segments {
		collectionSegments := make([]int64, 0)
		for partition, segments := range partitions {
			results := mgr.GetSegmentsByCollection(collection, partition)
			suite.assertSegments(segments, results)
			for _, segment := range segments {
				suite.True(mgr.ContainSegment(segment))
			}
			collectionSegments = append(collectionSegments, segments...)
		}
		results := mgr.GetSegmentsByCollection(collection)
		suite.assertSegments(collectionSegments, results)
	}
}

func (suite *TargetManagerSuite) TestRemove() {
	mgr := suite.mgr

	for collection, partitions := range suite.segments {
		// Remove first segment of each partition
		for _, segments := range partitions {
			mgr.RemoveSegment(segments[0])
			suite.False(mgr.ContainSegment(segments[0]))
		}

		// Remove first partition of each collection
		firstPartition := suite.partitions[collection][0]
		mgr.RemovePartition(firstPartition)
		segments := mgr.GetSegmentsByCollection(collection, firstPartition)
		suite.Empty(segments)
	}

	// Remove first collection
	firstCollection := suite.collections[0]
	mgr.RemoveCollection(firstCollection)
	channels := mgr.GetDmChannelsByCollection(firstCollection)
	suite.Empty(channels)
	segments := mgr.GetSegmentsByCollection(firstCollection)
	suite.Empty(segments)
}

func (suite *TargetManagerSuite) assertChannels(expected []string, actual []*DmChannel) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewSet(expected...)
	for _, channel := range actual {
		set.Remove(channel.ChannelName)
	}

	return suite.Len(set, 0)
}

func (suite *TargetManagerSuite) assertSegments(expected []int64, actual []*datapb.SegmentInfo) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewUniqueSet(expected...)
	for _, segment := range actual {
		set.Remove(segment.ID)
	}

	return suite.Len(set, 0)
}

func TestTargetManager(t *testing.T) {
	suite.Run(t, new(TargetManagerSuite))
}
