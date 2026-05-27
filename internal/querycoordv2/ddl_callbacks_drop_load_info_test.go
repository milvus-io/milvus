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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func enableGrowingSourceFlushForTest(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	})
}

type releaseDrainBroadcastAPI struct {
	t         *testing.T
	called    bool
	fenceTs   map[string]uint64
	channels  []string
	segments  map[string][]int64
	omitExtra bool
}

func (api *releaseDrainBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	api.called = true
	manualFlush := message.MustAsBroadcastManualFlushMessageV2(msg)
	require.Equal(api.t, int64(100), manualFlush.Header().GetCollectionId())
	require.ElementsMatch(api.t, api.channels, manualFlush.BroadcastHeader().VChannels)

	results := make(map[string]*types.AppendResult, len(api.fenceTs))
	for channel, ts := range api.fenceTs {
		results[channel] = &types.AppendResult{TimeTick: ts}
		if !api.omitExtra {
			extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: api.segments[channel]})
			require.NoError(api.t, err)
			results[channel].Extra = extra
		}
	}
	return &types.BroadcastAppendResult{AppendResults: results}, nil
}

func (api *releaseDrainBroadcastAPI) Close() {}

type releaseDrainProgressGetter struct {
	progress map[string][]writebuffer.GrowingFlushSegmentProgress
	calls    []releaseDrainProgressCall
}

type releaseDrainProgressCall struct {
	vchannel   string
	segmentIDs []int64
	fenceTs    uint64
}

func (g *releaseDrainProgressGetter) GetGrowingFlushProgress(ctx context.Context, vchannel string, segmentIDs []int64, fenceTs uint64) ([]writebuffer.GrowingFlushSegmentProgress, error) {
	g.calls = append(g.calls, releaseDrainProgressCall{
		vchannel:   vchannel,
		segmentIDs: append([]int64(nil), segmentIDs...),
		fenceTs:    fenceTs,
	})
	return g.progress[vchannel], nil
}

func TestDrainGrowingSourceRelease(t *testing.T) {
	enableGrowingSourceFlushForTest(t)
	ctx := context.Background()
	targetMgr := meta.NewMockTargetManager(t)
	broker := meta.NewMockBroker(t)
	progressGetter := &releaseDrainProgressGetter{
		progress: map[string][]writebuffer.GrowingFlushSegmentProgress{
			"ch1": {
				{SegmentID: 11, TargetOffset: 7, HasGrowingProgress: true, SourceMode: metacache.FlushSourceGrowing},
			},
			"ch2": {
				{SegmentID: 21, TargetOffset: 0, HasGrowingProgress: false, SourceMode: metacache.FlushSourceWriteBuffer},
			},
		},
	}
	drainer := newGrowingSourceReleaseDrainer(broker, targetMgr, nil, progressGetter)

	targetMgr.EXPECT().GetDmChannelsByCollection(ctx, int64(100), meta.CurrentTarget).Return(map[string]*meta.DmChannel{
		"ch2": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch2"}},
	})
	targetMgr.EXPECT().GetDmChannelsByCollection(ctx, int64(100), meta.NextTarget).Return(map[string]*meta.DmChannel{
		"ch1": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch1"}},
	})
	api := &releaseDrainBroadcastAPI{
		t:        t,
		channels: []string{"ch1", "ch2"},
		fenceTs:  map[string]uint64{"ch1": 101, "ch2": 102},
		segments: map[string][]int64{"ch1": {11}, "ch2": {21}},
	}
	fenceTs, err := drainer.DrainChannels(ctx, api, &milvuspb.DescribeCollectionResponse{
		CollectionID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 10, DataType: schemapb.DataType_Text}},
		},
	}, drainer.ReleaseDrainChannels(ctx, 100))
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"ch1": 101, "ch2": 102}, fenceTs)
	require.True(t, api.called)
}

func TestDrainGrowingSourceReleaseSegmentsUsesFencedSegmentsForHandoff(t *testing.T) {
	enableGrowingSourceFlushForTest(t)
	ctx := context.Background()
	progressGetter := &releaseDrainProgressGetter{
		progress: map[string][]writebuffer.GrowingFlushSegmentProgress{
			"ch1": {
				{SegmentID: 11, TargetOffset: 7, HasGrowingProgress: true, SourceMode: metacache.FlushSourceGrowing},
			},
		},
	}
	drainer := newGrowingSourceReleaseDrainer(nil, nil, nil, progressGetter)
	api := &releaseDrainBroadcastAPI{
		t:        t,
		channels: []string{"ch1"},
		fenceTs:  map[string]uint64{"ch1": 101},
		segments: map[string][]int64{"ch1": {11, 12}},
	}

	fenceTs, err := drainer.drainChannels(ctx, api, &milvuspb.DescribeCollectionResponse{
		CollectionID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 10, DataType: schemapb.DataType_Text}},
		},
	}, []string{"ch1"}, map[string][]int64{"ch1": {11}})
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"ch1": 101}, fenceTs)
	require.Equal(t, []releaseDrainProgressCall{{
		vchannel:   "ch1",
		segmentIDs: []int64{11, 12},
		fenceTs:    101,
	}}, progressGetter.calls)
}

func TestDrainGrowingSourceReleaseSegmentsSkipsEmptySegments(t *testing.T) {
	enableGrowingSourceFlushForTest(t)
	ctx := context.Background()
	progressGetter := &releaseDrainProgressGetter{}
	drainer := newGrowingSourceReleaseDrainer(nil, nil, nil, progressGetter)
	api := &releaseDrainBroadcastAPI{
		t:        t,
		channels: []string{"ch1"},
	}

	fenceTs, err := drainer.drainChannels(ctx, api, &milvuspb.DescribeCollectionResponse{
		CollectionID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 10, DataType: schemapb.DataType_Text}},
		},
	}, []string{"ch1"}, map[string][]int64{"ch1": nil})
	require.NoError(t, err)
	require.Nil(t, fenceTs)
	require.False(t, api.called)
	require.Empty(t, progressGetter.calls)
}

func TestDrainGrowingSourceReleaseWithBroadcastResultWithoutExtra(t *testing.T) {
	enableGrowingSourceFlushForTest(t)
	ctx := context.Background()
	targetMgr := meta.NewMockTargetManager(t)
	broker := meta.NewMockBroker(t)
	progressGetter := &releaseDrainProgressGetter{
		progress: map[string][]writebuffer.GrowingFlushSegmentProgress{
			"ch1": {
				{SegmentID: 11, TargetOffset: 7, HasGrowingProgress: true, SourceMode: metacache.FlushSourceGrowing},
			},
		},
	}
	drainer := newGrowingSourceReleaseDrainer(broker, targetMgr, nil, progressGetter)

	targetMgr.EXPECT().GetDmChannelsByCollection(ctx, int64(100), meta.CurrentTarget).Return(map[string]*meta.DmChannel{
		"ch1": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch1"}},
	})
	targetMgr.EXPECT().GetDmChannelsByCollection(ctx, int64(100), meta.NextTarget).Return(map[string]*meta.DmChannel{})
	api := &releaseDrainBroadcastAPI{
		t:         t,
		channels:  []string{"ch1"},
		fenceTs:   map[string]uint64{"ch1": 101},
		omitExtra: true,
	}
	fenceTs, err := drainer.DrainChannels(ctx, api, &milvuspb.DescribeCollectionResponse{
		CollectionID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 10, DataType: schemapb.DataType_Text}},
		},
	}, drainer.ReleaseDrainChannels(ctx, 100))
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"ch1": 101}, fenceTs)
	require.True(t, api.called)
}

func TestDrainGrowingSourceReleaseDisabled(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)
	drainer := newGrowingSourceReleaseDrainer(nil, nil, nil, nil)
	api := &releaseDrainBroadcastAPI{t: t}
	fenceTs, err := drainer.DrainChannels(context.Background(), api, &milvuspb.DescribeCollectionResponse{
		CollectionID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 10, DataType: schemapb.DataType_VarChar}},
		},
	}, []string{"ch1"})
	require.NoError(t, err)
	require.Nil(t, fenceTs)
	require.False(t, api.called)
}
