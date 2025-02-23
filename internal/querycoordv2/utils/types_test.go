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

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func Test_packLoadSegmentRequest(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-test-packLoadSegmentRequest-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	t0 := tsoutil.ComposeTSByTime(time.Now().Add(-20*time.Minute), 0)
	t1 := tsoutil.ComposeTSByTime(time.Now().Add(-8*time.Minute), 0)
	t2 := tsoutil.ComposeTSByTime(time.Now().Add(-5*time.Minute), 0)
	t3 := tsoutil.ComposeTSByTime(time.Now().Add(-1*time.Minute), 0)

	segmentInfo := &datapb.SegmentInfo{
		ID:            0,
		InsertChannel: mockVChannel,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: mockPChannel,
			Timestamp:   t1,
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: mockPChannel,
			Timestamp:   t2,
		},
	}

	channel := &datapb.VchannelInfo{
		ChannelName: mockVChannel,
		SeekPosition: &msgpb.MsgPosition{
			ChannelName: mockPChannel,
			Timestamp:   t2,
		},
	}

	t.Run("test set deltaPosition from channel seek position", func(t *testing.T) {
		req := PackSegmentLoadInfo(segmentInfo, channel.GetSeekPosition(), nil)
		assert.NotNil(t, req.GetDeltaPosition())
		assert.Equal(t, mockPChannel, req.GetDeltaPosition().ChannelName)
		assert.Equal(t, t2, req.GetDeltaPosition().Timestamp)
	})

	t.Run("test channel cp after segment dml position", func(t *testing.T) {
		channel := proto.Clone(channel).(*datapb.VchannelInfo)
		channel.SeekPosition.Timestamp = t3
		req := PackSegmentLoadInfo(segmentInfo, channel.GetSeekPosition(), nil)
		assert.NotNil(t, req.GetDeltaPosition())
		assert.Equal(t, mockPChannel, req.GetDeltaPosition().ChannelName)
		assert.Equal(t, t3, req.GetDeltaPosition().Timestamp)
	})

	t.Run("test tsLag > 10minutes", func(t *testing.T) {
		channel := proto.Clone(channel).(*datapb.VchannelInfo)
		channel.SeekPosition.Timestamp = t0
		req := PackSegmentLoadInfo(segmentInfo, channel.GetSeekPosition(), nil)
		assert.NotNil(t, req.GetDeltaPosition())
		assert.Equal(t, mockPChannel, req.GetDeltaPosition().ChannelName)
		assert.Equal(t, channel.SeekPosition.Timestamp, req.GetDeltaPosition().GetTimestamp())
	})
}
