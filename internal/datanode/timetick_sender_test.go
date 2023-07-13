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

package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
)

func TestTimetickManagerNormal(t *testing.T) {
	ctx := context.Background()
	manager := newTimeTickSender(&DataCoordFactory{}, 0)

	channelName1 := "channel1"
	ts := uint64(time.Now().UnixMilli())
	var segmentID1 int64 = 28257
	var segmentID2 int64 = 28258
	segmentStats := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   100,
		},
	}
	// update first time
	manager.update(channelName1, ts, segmentStats)

	channel1SegmentStates, channelSegmentStatesExist := manager.channelStatesCaches[channelName1]
	assert.Equal(t, true, channelSegmentStatesExist)
	segmentState1, segmentState1Exist := channel1SegmentStates.data[ts]
	assert.Equal(t, segmentStats[0], segmentState1[0])
	assert.Equal(t, true, segmentState1Exist)

	// update second time
	segmentStats2 := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   10000,
		},
		{
			SegmentID: segmentID2,
			NumRows:   33333,
		},
	}
	ts2 := ts + 100
	manager.update(channelName1, ts2, segmentStats2)

	channelSegmentStates, channelSegmentStatesExist := manager.channelStatesCaches[channelName1]
	assert.Equal(t, true, channelSegmentStatesExist)

	segmentStates, segmentStatesExist := channelSegmentStates.data[ts2]
	assert.Equal(t, true, segmentStatesExist)
	assert.Equal(t, 2, len(segmentStates))

	var segmentID3 int64 = 28259
	var segmentID4 int64 = 28260
	channelName2 := "channel2"
	ts3 := ts2 + 100
	segmentStats3 := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID3,
			NumRows:   1000000,
		},
		{
			SegmentID: segmentID4,
			NumRows:   3333300,
		},
	}
	manager.update(channelName2, ts3, segmentStats3)

	err := manager.sendReport(ctx, 100)
	assert.NoError(t, err)

	_, channelExistAfterSubmit := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelExistAfterSubmit)

	_, channelSegmentStatesExistAfterSubmit := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelSegmentStatesExistAfterSubmit)

	var segmentID5 int64 = 28261
	var segmentID6 int64 = 28262
	channelName3 := "channel3"
	ts4 := ts3 + 100
	segmentStats4 := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID5,
			NumRows:   1000000,
		},
		{
			SegmentID: segmentID6,
			NumRows:   3333300,
		},
	}
	manager.update(channelName3, ts4, segmentStats4)

	err = manager.sendReport(ctx, 100)
	assert.NoError(t, err)

	_, channelExistAfterSubmit2 := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelExistAfterSubmit2)

	_, channelSegmentStatesExistAfterSubmit2 := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelSegmentStatesExistAfterSubmit2)
}

func TestTimetickManagerSendErr(t *testing.T) {
	ctx := context.Background()
	manager := newTimeTickSender(&DataCoordFactory{ReportDataNodeTtMsgsError: true}, 0)

	channelName1 := "channel1"
	ts := uint64(time.Now().Unix())
	var segmentID1 int64 = 28257
	segmentStats := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   100,
		},
	}
	// update first time
	manager.update(channelName1, ts, segmentStats)
	err := manager.sendReport(ctx, 100)
	assert.Error(t, err)
}

func TestTimetickManagerSendNotSuccess(t *testing.T) {
	ctx := context.Background()
	manager := newTimeTickSender(&DataCoordFactory{ReportDataNodeTtMsgsNotSuccess: true}, 0)

	channelName1 := "channel1"
	ts := uint64(time.Now().Unix())
	var segmentID1 int64 = 28257
	segmentStats := []*commonpb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   100,
		},
	}
	// update first time
	manager.update(channelName1, ts, segmentStats)
	err := manager.sendReport(ctx, 100)
	assert.Error(t, err)
}

func TestTimetickManagerSendReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockDataCoord := types.NewMockDataCoord(t)
	tsInMill := time.Now().UnixMilli()

	validTs := atomic.NewBool(false)
	mockDataCoord.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) {
		if req.GetBase().Timestamp > uint64(tsInMill) {
			validTs.Store(true)
		}
	}).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil)
	manager := newTimeTickSender(mockDataCoord, 0)
	go manager.start(ctx)

	assert.Eventually(t, func() bool {
		return validTs.Load()
	}, 2*time.Second, 500*time.Millisecond)
}
