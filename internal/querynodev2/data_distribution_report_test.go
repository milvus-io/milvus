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

package querynodev2

import (
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDataDistributionDeltaTracker(t *testing.T) {
	cache := newDataDistributionDeltaTracker()

	cache.markChannelUpsert("ch-1")
	if _, ok := cache.dirtyChannels["ch-1"]; !ok {
		t.Fatalf("expected channel upsert to dirty channel")
	}
	if _, ok := cache.removedChannels["ch-1"]; ok {
		t.Fatalf("expected channel upsert to clear removed marker")
	}

	cache.markChannelRemove("ch-1")
	if _, ok := cache.dirtyChannels["ch-1"]; ok {
		t.Fatalf("expected channel remove to clear dirty channel marker")
	}
	if _, ok := cache.removedChannels["ch-1"]; !ok {
		t.Fatalf("expected channel remove to be recorded")
	}

	cache.markChannelUpsert("ch-1")
	if _, ok := cache.dirtyChannels["ch-1"]; !ok {
		t.Fatalf("expected channel re-upsert to dirty channel")
	}
	if _, ok := cache.removedChannels["ch-1"]; ok {
		t.Fatalf("expected channel re-upsert to clear removed marker")
	}

	cache.markSegmentUpsert(1)
	if _, ok := cache.dirtySegments[1]; !ok {
		t.Fatalf("expected segment upsert to dirty segment")
	}
	if _, ok := cache.removedSegments[1]; ok {
		t.Fatalf("expected segment upsert to clear removed marker")
	}

	cache.markSegmentRemove(1)
	if _, ok := cache.dirtySegments[1]; ok {
		t.Fatalf("expected segment remove to clear dirty segment marker")
	}
	if _, ok := cache.removedSegments[1]; !ok {
		t.Fatalf("expected segment remove to be recorded")
	}

	cache.markSegmentUpsert(1)
	if _, ok := cache.dirtySegments[1]; !ok {
		t.Fatalf("expected segment re-upsert to dirty segment")
	}
	if _, ok := cache.removedSegments[1]; ok {
		t.Fatalf("expected segment re-upsert to clear removed marker")
	}

	cache.lastReportedTs = 10
	cache.markSegmentRemove(2)
	cache.markChannelRemove("ch-2")

	if cache.canBuildDelta(&querypb.GetDataDistributionRequest{SupportDelta: false, LastUpdateTs: 10}) {
		t.Fatalf("expected unsupported request to require full report")
	}
	if cache.canBuildDelta(&querypb.GetDataDistributionRequest{SupportDelta: true}) {
		t.Fatalf("expected request without last update timestamp to require full report")
	}
	if cache.canBuildDelta(&querypb.GetDataDistributionRequest{SupportDelta: true, LastUpdateTs: 9}) {
		t.Fatalf("expected stale timestamp to require full report")
	}
	if !cache.canBuildDelta(&querypb.GetDataDistributionRequest{SupportDelta: true, LastUpdateTs: 10}) {
		t.Fatalf("expected matching timestamp to support delta report")
	}

	cache.clearDelta()
	if len(cache.dirtySegments) != 0 || len(cache.removedSegments) != 0 ||
		len(cache.dirtyChannels) != 0 || len(cache.removedChannels) != 0 {
		t.Fatalf("expected clearDelta to clear delta markers")
	}

	cache.clearDelta()
	if cache.lastReportedTs != 10 {
		t.Fatalf("expected clearDelta to keep reported timestamp")
	}
	if len(cache.dirtySegments) != 0 || len(cache.removedSegments) != 0 ||
		len(cache.dirtyChannels) != 0 || len(cache.removedChannels) != 0 {
		t.Fatalf("expected clearDelta to clear delta markers")
	}

	var nilTracker *dataDistributionDeltaTracker
	nilTracker.markSegmentUpsert(1)
	nilTracker.markSegmentRemove(1)
	nilTracker.markChannelUpsert("ch-1")
	nilTracker.markChannelRemove("ch-1")

	emptyChannelTracker := newDataDistributionDeltaTracker()
	emptyChannelTracker.markChannelUpsert("")
	emptyChannelTracker.markChannelRemove("")
	if len(emptyChannelTracker.dirtyChannels) != 0 || len(emptyChannelTracker.removedChannels) != 0 {
		t.Fatalf("expected empty channel markers to be ignored")
	}
}

func TestDataDistributionDeltaTrackerClearDeltaKeepsNewDelta(t *testing.T) {
	cache := newDataDistributionDeltaTracker()
	cache.lastReportedTs = 10
	cache.markSegmentUpsert(1)
	cache.markChannelUpsert("ch-1")

	reportSegmentIDs := make([]int64, 0, len(cache.dirtySegments))
	for segmentID := range cache.dirtySegments {
		reportSegmentIDs = append(reportSegmentIDs, segmentID)
	}
	reportChannelNames := make([]string, 0, len(cache.dirtyChannels))
	for channel := range cache.dirtyChannels {
		reportChannelNames = append(reportChannelNames, channel)
	}
	cache.clearDelta()

	if len(reportSegmentIDs) != 1 || reportSegmentIDs[0] != 1 {
		t.Fatalf("expected report to include existing dirty segment, got %v", reportSegmentIDs)
	}
	if len(reportChannelNames) != 1 || reportChannelNames[0] != "ch-1" {
		t.Fatalf("expected report to include existing dirty channel, got %v", reportChannelNames)
	}
	if len(cache.dirtySegments) != 0 || len(cache.dirtyChannels) != 0 {
		t.Fatalf("expected clearDelta to swap out existing delta markers")
	}

	cache.markSegmentUpsert(2)
	cache.markChannelUpsert("ch-2")
	cache.lastReportedTs = 20

	if cache.lastReportedTs != 20 {
		t.Fatalf("expected finish report to update last reported ts")
	}
	if _, ok := cache.dirtySegments[1]; ok {
		t.Fatalf("expected reported segment delta to stay consumed")
	}
	if _, ok := cache.dirtyChannels["ch-1"]; ok {
		t.Fatalf("expected reported channel delta to stay consumed")
	}
	if _, ok := cache.dirtySegments[2]; !ok {
		t.Fatalf("expected new segment delta to be kept")
	}
	if _, ok := cache.dirtyChannels["ch-2"]; !ok {
		t.Fatalf("expected new channel delta to be kept")
	}
}

func TestDataDistributionDeltaTrackerFullReportInvalidatesDeltaBase(t *testing.T) {
	cache := newDataDistributionDeltaTracker()
	cache.lastReportedTs = 10
	cache.markSegmentUpsert(1)

	if !cache.canBuildDelta(&querypb.GetDataDistributionRequest{
		SupportDelta: true,
		LastUpdateTs: 10,
	}) {
		t.Fatalf("expected matching timestamp to support delta report")
	}

	cache.lastReportedTs = 0
	cache.clearDelta()
	if cache.canBuildDelta(&querypb.GetDataDistributionRequest{
		SupportDelta: true,
		LastUpdateTs: 10,
	}) {
		t.Fatalf("expected in-progress full report to invalidate old delta base")
	}
	if len(cache.dirtySegments) != 0 {
		t.Fatalf("expected full report to clear pending delta markers")
	}
}

func TestGetDataDistributionDeltaReportInvalidatesBaseWhileBuilding(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	var blockOnce sync.Once
	segment := segments.NewMockSegment(t)
	segment.EXPECT().ID().Return(int64(1))
	segment.EXPECT().Collection().Return(int64(10))
	segment.EXPECT().Partition().Return(int64(100))
	segment.EXPECT().Shard().Return(metautil.NewChannel("by-dev-rootcoord-dml_0", 10, 1, metautil.NewDynChannelMapper()))
	segment.EXPECT().Version().Return(int64(20))
	segment.EXPECT().Level().Return(datapb.SegmentLevel_L1)
	segment.EXPECT().IsSorted().Return(true)
	segment.EXPECT().LastDeltaTimestamp().Return(uint64(30))
	segment.EXPECT().Indexes().Return(nil)
	segment.EXPECT().GetFieldJSONIndexStats().Return(nil)
	segment.EXPECT().LoadInfo().RunAndReturn(func() *querypb.SegmentLoadInfo {
		blockOnce.Do(func() {
			close(blocked)
			<-release
		})
		return &querypb.SegmentLoadInfo{}
	})

	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().CountBy(segments.WithType(commonpb.SegmentState_Sealed)).Return(1)
	segmentManager.EXPECT().GetSealed(int64(1)).Return(segment)

	node := &QueryNode{
		lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy),
		manager: &segments.Manager{
			Segment: segmentManager,
		},
		delegators:            typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		distDeltaTracker:      newDataDistributionDeltaTracker(),
		subscribingChannels:   typeutil.NewConcurrentSet[string](),
		unsubscribingChannels: typeutil.NewConcurrentSet[string](),
	}
	node.lastModifyTs = 20
	node.distDeltaTracker.lastReportedTs = 10
	node.distDeltaTracker.markSegmentUpsert(1)

	done := make(chan *querypb.GetDataDistributionResponse, 1)
	go func() {
		resp, err := node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{
			SupportDelta: true,
			LastUpdateTs: 10,
		})
		if err != nil {
			t.Errorf("GetDataDistribution returned error: %v", err)
		}
		done <- resp
	}()

	<-blocked
	if node.distDeltaTracker.canBuildDelta(&querypb.GetDataDistributionRequest{
		SupportDelta: true,
		LastUpdateTs: 10,
	}) {
		close(release)
		t.Fatalf("expected in-progress delta report to invalidate old delta base")
	}
	close(release)

	resp := <-done
	if !resp.GetIsDelta() {
		t.Fatalf("expected delta response")
	}
	if node.distDeltaTracker.lastReportedTs != 20 {
		t.Fatalf("expected finished report to publish new base, got %d", node.distDeltaTracker.lastReportedTs)
	}
}

func TestGetDataDistributionSkipsStaleCommitAfterConcurrentReset(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	var blockOnce sync.Once
	segment := segments.NewMockSegment(t)
	segment.EXPECT().ID().Return(int64(1))
	segment.EXPECT().Collection().Return(int64(10))
	segment.EXPECT().Partition().Return(int64(100))
	segment.EXPECT().Shard().Return(metautil.NewChannel("by-dev-rootcoord-dml_0", 10, 1, metautil.NewDynChannelMapper()))
	segment.EXPECT().Version().Return(int64(20))
	segment.EXPECT().Level().Return(datapb.SegmentLevel_L1)
	segment.EXPECT().IsSorted().Return(true)
	segment.EXPECT().LastDeltaTimestamp().Return(uint64(30))
	segment.EXPECT().Indexes().Return(nil)
	segment.EXPECT().GetFieldJSONIndexStats().Return(nil)
	segment.EXPECT().LoadInfo().RunAndReturn(func() *querypb.SegmentLoadInfo {
		blockOnce.Do(func() {
			close(blocked)
			<-release
		})
		return &querypb.SegmentLoadInfo{}
	})

	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().CountBy(segments.WithType(commonpb.SegmentState_Sealed)).Return(1)
	segmentManager.EXPECT().GetSealed(int64(1)).Return(segment)

	node := &QueryNode{
		lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy),
		manager: &segments.Manager{
			Segment: segmentManager,
		},
		delegators:            typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		distDeltaTracker:      newDataDistributionDeltaTracker(),
		subscribingChannels:   typeutil.NewConcurrentSet[string](),
		unsubscribingChannels: typeutil.NewConcurrentSet[string](),
	}
	node.lastModifyTs = 20
	node.distDeltaTracker.lastReportedTs = 10
	node.distDeltaTracker.markSegmentUpsert(1)

	done := make(chan *querypb.GetDataDistributionResponse, 1)
	go func() {
		resp, err := node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{
			SupportDelta: true,
			LastUpdateTs: 10,
		})
		if err != nil {
			t.Errorf("GetDataDistribution returned error: %v", err)
		}
		done <- resp
	}()

	<-blocked
	node.distDeltaTracker.mu.Lock()
	node.distDeltaTracker.lastReportedTs = 30
	node.distDeltaTracker.clearDelta()
	node.distDeltaTracker.mu.Unlock()

	close(release)
	deltaResp := <-done
	if !deltaResp.GetIsDelta() {
		t.Fatalf("expected first response to remain delta")
	}
	if node.distDeltaTracker.lastReportedTs != 30 {
		t.Fatalf("expected stale delta commit to be skipped, got %d", node.distDeltaTracker.lastReportedTs)
	}
	if node.distDeltaTracker.canBuildDelta(&querypb.GetDataDistributionRequest{
		SupportDelta: true,
		LastUpdateTs: 20,
	}) {
		t.Fatalf("expected stale delta base not to be revalidated")
	}
}

func TestDataDistributionReportBuilders(t *testing.T) {
	segmentDist := buildFullSegmentDist([]delegator.SnapshotItem{
		{
			NodeID: 1,
			Segments: []delegator.SegmentEntry{
				{SegmentID: 10, Version: 100},
				{SegmentID: 11, Version: 101},
			},
		},
		{
			NodeID: 2,
			Segments: []delegator.SegmentEntry{
				{SegmentID: 12, Version: 102},
			},
		},
	})

	if len(segmentDist) != 3 {
		t.Fatalf("expected 3 segment dist entries, got %d", len(segmentDist))
	}
	if segmentDist[10].GetNodeID() != 1 || segmentDist[10].GetVersion() != 100 {
		t.Fatalf("unexpected segment dist for segment 10: %+v", segmentDist[10])
	}
	if segmentDist[12].GetNodeID() != 2 || segmentDist[12].GetVersion() != 102 {
		t.Fatalf("unexpected segment dist for segment 12: %+v", segmentDist[12])
	}

	channel := metautil.NewChannel("by-dev-rootcoord-dml_0", 10, 1, metautil.NewDynChannelMapper())
	jsonStats := map[int64]*querypb.JsonStatsInfo{
		101: {FieldID: 101, IndexID: 1001},
	}
	mockSegment := segments.NewMockSegment(t)
	mockSegment.EXPECT().ID().Return(int64(1))
	mockSegment.EXPECT().Collection().Return(int64(10))
	mockSegment.EXPECT().Partition().Return(int64(100))
	mockSegment.EXPECT().Shard().Return(channel)
	mockSegment.EXPECT().Version().Return(int64(20))
	mockSegment.EXPECT().Level().Return(datapb.SegmentLevel_L1)
	mockSegment.EXPECT().IsSorted().Return(true)
	mockSegment.EXPECT().LastDeltaTimestamp().Return(uint64(30))
	mockSegment.EXPECT().Indexes().Return([]*segments.IndexedFieldInfo{
		{IndexInfo: &querypb.FieldIndexInfo{IndexID: 1001, FieldID: 101}},
	})
	mockSegment.EXPECT().GetFieldJSONIndexStats().Return(jsonStats)
	mockSegment.EXPECT().LoadInfo().Return(&querypb.SegmentLoadInfo{
		ManifestPath: "manifest-path",
		DataVersion:  3,
	})

	info := buildSegmentVersionInfo(mockSegment)

	if info.GetID() != 1 || info.GetCollection() != 10 || info.GetPartition() != 100 {
		t.Fatalf("unexpected segment identity: %+v", info)
	}
	if info.GetChannel() != channel.VirtualName() || info.GetVersion() != 20 ||
		info.GetLevel() != datapb.SegmentLevel_L1 || !info.GetIsSorted() ||
		info.GetLastDeltaTimestamp() != 30 {
		t.Fatalf("unexpected segment version fields: %+v", info)
	}
	if info.GetIndexInfo()[1001].GetFieldID() != 101 {
		t.Fatalf("expected index info to be keyed by index id")
	}
	if info.GetJsonStatsInfo()[101].GetIndexID() != 1001 {
		t.Fatalf("expected json stats info to be preserved")
	}
	if info.GetManifestPath() != "manifest-path" || info.GetDataVersion() != 3 {
		t.Fatalf("unexpected load info fields: %+v", info)
	}
}

func TestMarkDataDistributionDelta(t *testing.T) {
	node := &QueryNode{distDeltaTracker: newDataDistributionDeltaTracker()}

	node.markDataDistributionSegmentLoadInfos([]*querypb.SegmentLoadInfo{
		{SegmentID: 1, Level: datapb.SegmentLevel_Legacy},
		{SegmentID: 2, Level: datapb.SegmentLevel_L0},
		{SegmentID: 3, Level: datapb.SegmentLevel_L1},
	})

	if _, ok := node.distDeltaTracker.dirtySegments[1]; !ok {
		t.Fatalf("expected legacy segment to be marked dirty")
	}
	if _, ok := node.distDeltaTracker.dirtySegments[2]; ok {
		t.Fatalf("expected L0 segment to be skipped")
	}
	if _, ok := node.distDeltaTracker.dirtySegments[3]; !ok {
		t.Fatalf("expected L1 segment to be marked dirty")
	}

	node.markDataDistributionLeaderSegmentLoad(&querypb.LoadSegmentsRequest{
		LoadScope: querypb.LoadScope_Full,
		Infos: []*querypb.SegmentLoadInfo{
			{SegmentID: 1, InsertChannel: "ch-1", Level: datapb.SegmentLevel_Legacy},
			{SegmentID: 2, InsertChannel: "ch-2", Level: datapb.SegmentLevel_L0},
			{SegmentID: 3, Level: datapb.SegmentLevel_L1},
		},
	})
	node.markDataDistributionLeaderSegmentLoad(&querypb.LoadSegmentsRequest{
		LoadScope: querypb.LoadScope_Delta,
		Infos: []*querypb.SegmentLoadInfo{
			{SegmentID: 4, InsertChannel: "ch-3", Level: datapb.SegmentLevel_L1},
		},
	})
	node.markDataDistributionLeaderSegmentLoad(&querypb.LoadSegmentsRequest{
		LoadScope: querypb.LoadScope_Reopen,
		Infos: []*querypb.SegmentLoadInfo{
			{SegmentID: 5, InsertChannel: "ch-4", Level: datapb.SegmentLevel_L1},
		},
	})

	if _, ok := node.distDeltaTracker.dirtyChannels["ch-1"]; !ok {
		t.Fatalf("expected full load to mark leader view")
	}
	if _, ok := node.distDeltaTracker.dirtyChannels["ch-2"]; ok {
		t.Fatalf("expected L0 segment to be skipped")
	}
	if _, ok := node.distDeltaTracker.dirtyChannels[""]; ok {
		t.Fatalf("expected empty channel to be skipped")
	}
	if _, ok := node.distDeltaTracker.dirtyChannels["ch-3"]; !ok {
		t.Fatalf("expected delta load to mark leader view")
	}
	if _, ok := node.distDeltaTracker.dirtyChannels["ch-4"]; ok {
		t.Fatalf("expected reopen load to be skipped")
	}

	before := node.getDistributionModifyTS()
	node.markLeaderViewUpdated("ch-5")
	if _, ok := node.distDeltaTracker.dirtyChannels["ch-5"]; !ok {
		t.Fatalf("expected leader view update to mark channel view")
	}
	if got := node.getDistributionModifyTS(); got <= before {
		t.Fatalf("expected leader view update to update distribution modify ts, got %d before %d", got, before)
	}

	after := node.getDistributionModifyTS()
	node.markLeaderViewUpdated("")
	if got := node.getDistributionModifyTS(); got != after {
		t.Fatalf("expected empty leader view channel to be skipped, got %d before %d", got, after)
	}
}
