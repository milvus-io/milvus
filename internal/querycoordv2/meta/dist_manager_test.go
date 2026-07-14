package meta

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func TestDistributionManagerCaptureIsNodeAtomic(t *testing.T) {
	manager := NewDistributionManager(session.NewNodeManager())
	oldSegments := []*Segment{SegmentFromInfo(&datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: "channel-old",
		NumOfRows:     100,
	})}
	oldChannels := []*DmChannel{{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-old"},
		Version:      1,
		View: &LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "channel-old",
			Version:      1,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	}}
	manager.PublishNodeDistribution(1, oldSegments, oldChannels)

	newSegments := []*Segment{SegmentFromInfo(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  100,
		PartitionID:   20,
		InsertChannel: "channel-new",
		NumOfRows:     200,
	})}
	newChannels := []*DmChannel{{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-new"},
		Version:      2,
		View: &LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "channel-new",
			Version:      2,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	}}

	captureLocked := make(chan struct{})
	releaseCapture := make(chan struct{})
	manager.captureHook = func(stage captureStage) {
		if stage == captureStageLocked {
			close(captureLocked)
			<-releaseCapture
		}
	}
	captured := make(chan DistributionSnapshot, 1)
	go func() {
		captured <- manager.Capture()
	}()
	<-captureLocked

	publishAttempted := make(chan struct{})
	manager.publishHook = func(stage publishStage) {
		if stage == publishStageBeforeLock {
			close(publishAttempted)
		}
	}
	published := make(chan struct{})
	go func() {
		defer close(published)
		manager.PublishNodeDistribution(1, newSegments, newChannels)
	}()
	<-publishAttempted
	close(releaseCapture)
	snapshot := <-captured
	<-published

	segmentIDs, channelNames := distributionRecordsForNode(snapshot, 1)
	require.Equal(t, []int64{1}, segmentIDs)
	require.Equal(t, []string{"channel-old"}, channelNames)
}

func distributionRecordsForNode(snapshot DistributionSnapshot, nodeID int64) ([]int64, []string) {
	segmentIDs := make([]int64, 0)
	for _, segment := range snapshot.Segments {
		if segment.NodeID == nodeID {
			segmentIDs = append(segmentIDs, segment.SegmentID)
		}
	}
	channelNames := make([]string, 0)
	for _, channel := range snapshot.Channels {
		if channel.NodeID == nodeID {
			channelNames = append(channelNames, channel.Channel)
		}
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })
	sort.Strings(channelNames)
	return segmentIDs, channelNames
}

func TestGetDistributionJSON(t *testing.T) {
	// Initialize DistributionManager
	manager := NewDistributionManager(session.NewNodeManager())

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
	manager.ChannelDistManager.Update(1, &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "channel-1",
		},
		Node:    1,
		Version: 1,
		View: &LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "channel-1",
			Version:      1,
			Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1}},
		},
	})
	manager.ChannelDistManager.Update(2, &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 200,
			ChannelName:  "channel-2",
		},
		Node:    2,
		Version: 1,
		View: &LeaderView{
			ID:           2,
			CollectionID: 200,
			Channel:      "channel-2",
			Version:      1,
			Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		},
	})

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
