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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
