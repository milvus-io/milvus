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
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

func TestGenerateFullWatchDmChannelsRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dataCoord := newDataCoordMock(ctx)
	handler, err := newGlobalMetaBroker(ctx, nil, dataCoord, nil, nil)
	assert.Nil(t, err)

	deltaChannel := &datapb.VchannelInfo{
		CollectionID:        defaultCollectionID,
		ChannelName:         "delta-channel1",
		UnflushedSegmentIds: []int64{1},
	}

	watchDmChannelsRequest := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
		},
		Infos:  []*datapb.VchannelInfo{deltaChannel},
		NodeID: 1,
	}

	fullWatchDmChannelsRequest, err := generateFullWatchDmChannelsRequest(ctx, handler, watchDmChannelsRequest)
	assert.Nil(t, err)
	assert.NotEmpty(t, fullWatchDmChannelsRequest.GetSegmentInfos())

	dataCoord.returnError = true
	fullWatchDmChannelsRequest2, err := generateFullWatchDmChannelsRequest(ctx, handler, watchDmChannelsRequest)
	assert.Error(t, err)
	assert.Empty(t, fullWatchDmChannelsRequest2.GetSegmentInfos())

	cancel()
}

func TestThinWatchDmChannelsRequest(t *testing.T) {
	var segmentID int64 = 1

	deltaChannel := &datapb.VchannelInfo{
		CollectionID:        defaultCollectionID,
		ChannelName:         "delta-channel1",
		UnflushedSegmentIds: []int64{segmentID},
	}

	segment := &datapb.SegmentInfo{
		ID: segmentID,
	}

	segmentInfos := make(map[int64]*datapb.SegmentInfo)
	segmentInfos[segmentID] = segment

	watchDmChannelsRequest := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
		},
		Infos:        []*datapb.VchannelInfo{deltaChannel},
		NodeID:       1,
		SegmentInfos: segmentInfos,
	}

	thinReq := thinWatchDmChannelsRequest(watchDmChannelsRequest)
	assert.Empty(t, thinReq.GetSegmentInfos())
}

func TestUpgradeCompatibility(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dataCoord := newDataCoordMock(ctx)
	handler, err := newGlobalMetaBroker(ctx, nil, dataCoord, nil, nil)
	assert.Nil(t, err)

	deltaChannel := &datapb.VchannelInfo{
		CollectionID:        defaultCollectionID,
		ChannelName:         "delta-channel1",
		UnflushedSegments:   []*datapb.SegmentInfo{{ID: 1}},
		FlushedSegments:     []*datapb.SegmentInfo{{ID: 2}},
		DroppedSegments:     []*datapb.SegmentInfo{{ID: 3}},
		UnflushedSegmentIds: []int64{1},
	}

	watchDmChannelsRequest := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
		},
		Infos:  []*datapb.VchannelInfo{deltaChannel},
		NodeID: 1,
	}

	fullWatchDmChannelsRequest, err := generateFullWatchDmChannelsRequest(ctx, handler, watchDmChannelsRequest)
	assert.Nil(t, err)
	assert.NotEmpty(t, fullWatchDmChannelsRequest.GetSegmentInfos())
	vChannel := fullWatchDmChannelsRequest.GetInfos()[0]
	assert.Equal(t, []*datapb.SegmentInfo{}, vChannel.GetUnflushedSegments())
	assert.Equal(t, []*datapb.SegmentInfo{}, vChannel.GetFlushedSegments())
	assert.Equal(t, []*datapb.SegmentInfo{}, vChannel.GetDroppedSegments())
	assert.NotEmpty(t, vChannel.GetUnflushedSegmentIds())
	assert.NotEmpty(t, vChannel.GetFlushedSegmentIds())
	assert.NotEmpty(t, vChannel.GetDroppedSegmentIds())

	assert.Equal(t, 1, len(vChannel.GetUnflushedSegmentIds()))
	cancel()
}

func TestGetMissSegment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dataCoord := newDataCoordMock(ctx)
	broker, err := newGlobalMetaBroker(ctx, nil, dataCoord, nil, nil)
	assert.Nil(t, err)

	vChannels, _, err := broker.getRecoveryInfo(ctx, defaultCollectionID, 0)
	assert.Nil(t, err)

	watchDmChannelsRequest := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []int64{1},
		Infos:        vChannels,
		NodeID:       1,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: defaultCollectionID,
			PartitionIDs: []int64{1},
		},
	}

	// inject certain number of error
	dataCoord.returnErrorCount.Store(3)

	_, err = generateFullWatchDmChannelsRequest(ctx, broker, watchDmChannelsRequest)
	assert.NoError(t, err)
	cancel()
}
