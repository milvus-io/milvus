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

package metacache

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type SegmentFilterSuite struct {
	suite.Suite
}

func (s *SegmentFilterSuite) TestFilters() {
	info := &SegmentInfo{}

	partitionID := int64(1001)
	filter := WithPartitionID(partitionID)
	info.partitionID = partitionID + 1
	s.False(filter.Filter(info))
	info.partitionID = partitionID
	s.True(filter.Filter(info))

	segmentID := int64(10001)
	filter = WithSegmentIDs(segmentID)
	info.segmentID = segmentID + 1
	s.False(filter.Filter(info))
	info.segmentID = segmentID
	s.True(filter.Filter(info))

	state := commonpb.SegmentState_Growing
	filter = WithSegmentState(state)
	info.state = commonpb.SegmentState_Flushed
	s.False(filter.Filter(info))
	info.state = state
	s.True(filter.Filter(info))

	filter = WithStartPosNotRecorded()
	info.startPosRecorded = true
	s.False(filter.Filter(info))
	info.startPosRecorded = false
	s.False(filter.Filter(info))
	info.startPosition = &msgpb.MsgPosition{}
	s.True(filter.Filter(info))
	info.startPosRecorded = true
	s.False(filter.Filter(info))
}

func TestFilters(t *testing.T) {
	suite.Run(t, new(SegmentFilterSuite))
}

type SegmentActionSuite struct {
	suite.Suite
}

func (s *SegmentActionSuite) TestActions() {
	info := &SegmentInfo{}

	state := commonpb.SegmentState_Flushed
	action := UpdateState(state)
	action(info)
	s.Equal(state, info.State())

	cp := &msgpb.MsgPosition{
		MsgID:       []byte{1, 2, 3, 4},
		ChannelName: "channel_1",
		Timestamp:   20000,
	}
	action = UpdateCheckpoint(cp)
	action(info)
	s.Equal(cp, info.Checkpoint())

	numOfRows := int64(2048)
	action = UpdateNumOfRows(numOfRows)
	action(info)
	s.Equal(numOfRows, info.NumOfRows())

	info = &SegmentInfo{}
	actions := SegmentActions(UpdateState(state), UpdateCheckpoint(cp), UpdateNumOfRows(numOfRows),
		UpdateBinlogs([]*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		}),
		UpdateStatslogs([]*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		}),
		UpdateDeltalogs([]*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		}),
		UpdateBm25logs([]*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		}),
	)
	actions(info)
	s.Equal(state, info.State())
	s.Equal(cp, info.Checkpoint())
	s.Equal(numOfRows, info.NumOfRows())
	s.Equal(1, len(info.Binlogs()))
	s.Equal(1, len(info.Statslogs()))
	s.Equal(1, len(info.Deltalogs()))
	s.Equal(1, len(info.Bm25logs()))
}

func (s *SegmentActionSuite) TestMergeActions() {
	info := &SegmentInfo{}

	var actions []SegmentAction
	state := commonpb.SegmentState_Flushed
	actions = append(actions, UpdateState(state))

	cp := &msgpb.MsgPosition{
		MsgID:       []byte{1, 2, 3, 4},
		ChannelName: "channel_1",
		Timestamp:   20000,
	}
	actions = append(actions, UpdateCheckpoint(cp))

	numOfRows := int64(2048)
	actions = append(actions, UpdateNumOfRows(numOfRows))

	action := MergeSegmentAction(actions...)
	action(info)

	s.Equal(state, info.State())
	s.Equal(numOfRows, info.NumOfRows())
	s.Equal(cp, info.Checkpoint())
}

func TestActions(t *testing.T) {
	suite.Run(t, new(SegmentActionSuite))
}
