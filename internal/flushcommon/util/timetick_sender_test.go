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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

func TestTimetickManagerNormal(t *testing.T) {
	ctx := context.Background()

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()

	manager := NewTimeTickSender(broker, 0)

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
	manager.Update(channelName1, ts, segmentStats)

	chanStats, exist := manager.statsCache[channelName1]
	assert.Equal(t, true, exist)
	assert.Equal(t, 1, len(chanStats.segStats))
	seg1, exist := manager.statsCache[channelName1].segStats[segmentID1]
	assert.Equal(t, true, exist)
	assert.Equal(t, segmentID1, seg1.GetSegmentID())
	assert.Equal(t, int64(100), seg1.GetNumRows())
	assert.Equal(t, ts, seg1.ts)

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
	manager.Update(channelName1, ts2, segmentStats2)

	chanStats, exist = manager.statsCache[channelName1]
	assert.Equal(t, true, exist)
	assert.Equal(t, 2, len(chanStats.segStats))
	seg1, exist = manager.statsCache[channelName1].segStats[segmentID1]
	assert.Equal(t, true, exist)
	assert.Equal(t, segmentID1, seg1.GetSegmentID())
	assert.Equal(t, int64(10000), seg1.GetNumRows())
	assert.Equal(t, ts2, seg1.ts)
	seg2, exist := manager.statsCache[channelName1].segStats[segmentID2]
	assert.Equal(t, true, exist)
	assert.Equal(t, segmentID2, seg2.GetSegmentID())
	assert.Equal(t, int64(33333), seg2.GetNumRows())
	assert.Equal(t, ts2, seg2.ts)

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
	manager.Update(channelName2, ts3, segmentStats3)

	err := manager.sendReport(ctx)
	assert.NoError(t, err)

	_, exist = manager.statsCache[channelName1]
	assert.Equal(t, false, exist)
	_, exist = manager.statsCache[channelName2]
	assert.Equal(t, false, exist)

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
	manager.Update(channelName3, ts4, segmentStats4)

	err = manager.sendReport(ctx)
	assert.NoError(t, err)

	_, exist = manager.statsCache[channelName3]
	assert.Equal(t, false, exist)
}

func TestTimetickManagerSendErr(t *testing.T) {
	ctx := context.Background()

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(errors.New("mock")).Maybe()

	manager := NewTimeTickSender(broker, 0, retry.Attempts(1))

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
	manager.Update(channelName1, ts, segmentStats)
	err := manager.sendReport(ctx)
	assert.Error(t, err)
}

func TestTimetickManagerSendReport(t *testing.T) {
	mockDataCoord := mocks.NewMockDataCoordClient(t)

	called := atomic.NewBool(false)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ []*msgpb.DataNodeTtMsg) {
			called.Store(true)
		}).
		Return(nil)
	mockDataCoord.EXPECT().ReportDataNodeTtMsgs(mock.Anything, mock.Anything).Return(merr.Status(nil), nil).Maybe()
	manager := NewTimeTickSender(broker, 0)
	manager.Start()

	assert.Eventually(t, func() bool {
		return called.Load()
	}, 2*time.Second, 500*time.Millisecond)

	manager.Stop()
}
