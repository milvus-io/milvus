package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

func TestGetDistributionJSON(t *testing.T) {
	// Initialize DistributionManager
	manager := NewDistributionManager()

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

	manager.SegmentDistManager.Update(1, segment1)
	manager.SegmentDistManager.Update(2, segment2)

	// Add some channels to the ChannelDistManager
	channel1 := DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: 100,
		ChannelName:  "channel-1",
	})
	channel1.Node = 1
	channel1.Version = 1

	channel2 := DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: 200,
		ChannelName:  "channel-2",
	})
	channel2.Node = 2
	channel2.Version = 1

	manager.ChannelDistManager.Update(1, channel1)
	manager.ChannelDistManager.Update(2, channel2)

	// Add some leader views to the LeaderViewManager
	leaderView1 := &LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "channel-1",
		Version:      1,
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1}},
	}

	leaderView2 := &LeaderView{
		ID:           2,
		CollectionID: 200,
		Channel:      "channel-2",
		Version:      1,
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
	}

	manager.LeaderViewManager.Update(1, leaderView1)
	manager.LeaderViewManager.Update(2, leaderView2)

	// Call GetDistributionJSON
	jsonOutput := manager.GetDistributionJSON(0)

	// Verify JSON output
	var dist metricsinfo.QueryCoordDist
	err := json.Unmarshal([]byte(jsonOutput), &dist)
	assert.NoError(t, err)

	assert.Len(t, dist.Segments, 2)
	assert.Len(t, dist.DMChannels, 2)
	assert.Len(t, dist.LeaderViews, 2)

	jsonOutput = manager.GetDistributionJSON(1000)
	var dist2 metricsinfo.QueryCoordDist
	err = json.Unmarshal([]byte(jsonOutput), &dist2)
	assert.NoError(t, err)

	assert.Len(t, dist2.Segments, 0)
	assert.Len(t, dist2.DMChannels, 0)
	assert.Len(t, dist2.LeaderViews, 0)
}
