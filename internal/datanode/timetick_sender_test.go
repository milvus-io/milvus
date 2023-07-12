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

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestTimetickManagerNormal(t *testing.T) {
	ctx := context.Background()
	manager := newTimeTickSender(&DataCoordFactory{}, 0)

	channelName1 := "channel1"
	ts := uint64(time.Now().UnixMilli())
	var segmentID1 int64 = 28257
	var segmentID2 int64 = 28258
	segmentStats := []*datapb.SegmentStats{
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
	segmentStats2 := []*datapb.SegmentStats{
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
	segmentStats3 := []*datapb.SegmentStats{
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

	err := manager.sendReport(ctx)
	assert.NoError(t, err)

	_, channelExistAfterSubmit := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelExistAfterSubmit)

	_, channelSegmentStatesExistAfterSubmit := manager.channelStatesCaches[channelName1]
	assert.Equal(t, false, channelSegmentStatesExistAfterSubmit)

	var segmentID5 int64 = 28261
	var segmentID6 int64 = 28262
	channelName3 := "channel3"
	ts4 := ts3 + 100
	segmentStats4 := []*datapb.SegmentStats{
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
	ts5 := ts4 + 100
	segmentStats5 := []*datapb.SegmentStats{
		{
			SegmentID: segmentID5,
			NumRows:   2000000,
		},
		{
			SegmentID: segmentID6,
			NumRows:   3333300,
		},
	}
	manager.update(channelName3, ts5, segmentStats5)

	err = manager.sendReport(ctx)
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
	segmentStats := []*datapb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   100,
		},
	}
	// update first time
	manager.update(channelName1, ts, segmentStats)
	err := manager.sendReport(ctx)
	assert.Error(t, err)
}

func TestTimetickManagerSendNotSuccess(t *testing.T) {
	ctx := context.Background()
	manager := newTimeTickSender(&DataCoordFactory{ReportDataNodeTtMsgsNotSuccess: true}, 0)

	channelName1 := "channel1"
	ts := uint64(time.Now().Unix())
	var segmentID1 int64 = 28257
	segmentStats := []*datapb.SegmentStats{
		{
			SegmentID: segmentID1,
			NumRows:   100,
		},
	}
	// update first time
	manager.update(channelName1, ts, segmentStats)
	err := manager.sendReport(ctx)
	assert.Error(t, err)
}
