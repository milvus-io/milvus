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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type TestSegmentOperatorSuite struct {
	suite.Suite
}

func (s *TestSegmentOperatorSuite) TestSetMaxRowCount() {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			MaxRowNum: 300,
		},
	}

	ops := SetMaxRowCount(20000)
	updated := ops(segment)
	s.Require().True(updated)
	s.EqualValues(20000, segment.GetMaxRowNum())

	updated = ops(segment)
	s.False(updated)
}

func TestSegmentOperators(t *testing.T) {
	suite.Run(t, new(TestSegmentOperatorSuite))
}

func TestUpdateStartPosition(t *testing.T) {
	for _, tc := range []struct {
		name     string
		level    datapb.SegmentLevel
		position *msgpb.MsgPosition
		accepted bool
	}{
		{"L0 timestamp only", datapb.SegmentLevel_L0, &msgpb.MsgPosition{Timestamp: 100}, true},
		{"L0 WAL position", datapb.SegmentLevel_L0, &msgpb.MsgPosition{MsgID: []byte{1}, Timestamp: 100}, true},
		{"L0 missing position", datapb.SegmentLevel_L0, nil, false},
		{"L0 empty position", datapb.SegmentLevel_L0, &msgpb.MsgPosition{}, false},
		{"L1 timestamp only", datapb.SegmentLevel_L1, &msgpb.MsgPosition{Timestamp: 100}, false},
		{"L1 WAL position", datapb.SegmentLevel_L1, &msgpb.MsgPosition{MsgID: []byte{1}, Timestamp: 100}, true},
		{"L1 message ID only", datapb.SegmentLevel_L1, &msgpb.MsgPosition{MsgID: []byte{1}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &msgpb.MsgPosition{ChannelName: "ch", MsgID: []byte{2}, Timestamp: 50}
			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, Level: tc.level, StartPosition: original,
			}}
			pack := &updateSegmentPack{segments: map[int64]*SegmentInfo{1: segment}}
			if tc.position != nil {
				tc.position.ChannelName = "ch"
			}
			require.True(t, UpdateStartPosition([]*datapb.SegmentStartPosition{{
				SegmentID: 1, StartPosition: tc.position,
			}})(pack))
			want := original
			if tc.accepted {
				want = tc.position
			}
			require.True(t, proto.Equal(want, segment.GetStartPosition()))
		})
	}

	t.Run("missing segment", func(t *testing.T) {
		pack := &updateSegmentPack{
			meta:     &meta{ctx: context.Background(), segments: NewSegmentsInfo()},
			segments: make(map[int64]*SegmentInfo),
		}
		require.True(t, UpdateStartPosition([]*datapb.SegmentStartPosition{{
			SegmentID: 1, StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		}})(pack))
		require.Empty(t, pack.segments)
	})
}

func TestUpdateImportSegmentPosition(t *testing.T) {
	t.Run("segment not found", func(t *testing.T) {
		// Create a meta with empty segments to properly test the "not found" case
		segments := NewSegmentsInfo()
		m := &meta{segments: segments}
		modPack := &updateSegmentPack{
			meta:     m,
			segments: make(map[int64]*SegmentInfo),
		}
		op := UpdateImportSegmentPosition(100, 1000, 2000)
		result := op(modPack)
		assert.False(t, result)
	})

	t.Run("update position successfully", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            100,
				InsertChannel: "test_channel",
			},
		}
		modPack := &updateSegmentPack{
			segments: map[int64]*SegmentInfo{
				100: segment,
			},
		}
		op := UpdateImportSegmentPosition(100, 1000, 2000)
		result := op(modPack)
		assert.True(t, result)

		// Verify StartPosition
		assert.NotNil(t, segment.GetStartPosition())
		assert.Equal(t, "test_channel", segment.GetStartPosition().GetChannelName())
		assert.Nil(t, segment.GetStartPosition().GetMsgID())
		assert.Equal(t, uint64(1000), segment.GetStartPosition().GetTimestamp())

		// Verify DmlPosition
		assert.NotNil(t, segment.GetDmlPosition())
		assert.Equal(t, "test_channel", segment.GetDmlPosition().GetChannelName())
		assert.Nil(t, segment.GetDmlPosition().GetMsgID())
		assert.Equal(t, uint64(2000), segment.GetDmlPosition().GetTimestamp())
	})

	t.Run("update position with zero timestamps", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            101,
				InsertChannel: "channel_2",
			},
		}
		modPack := &updateSegmentPack{
			segments: map[int64]*SegmentInfo{
				101: segment,
			},
		}
		op := UpdateImportSegmentPosition(101, 0, 0)
		result := op(modPack)
		assert.True(t, result)

		assert.NotNil(t, segment.GetStartPosition())
		assert.Equal(t, uint64(0), segment.GetStartPosition().GetTimestamp())
		assert.NotNil(t, segment.GetDmlPosition())
		assert.Equal(t, uint64(0), segment.GetDmlPosition().GetTimestamp())
	})
}
