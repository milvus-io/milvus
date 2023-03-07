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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
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

	t.Run("test set deltaPosition from channel checkpoint", func(t *testing.T) {
		segmentAction := NewSegmentAction(0, 0, "", 0)
		segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, 0, 0, 0, segmentAction)
		assert.NoError(t, err)

		resp := &datapb.GetSegmentInfoResponse{
			Infos: []*datapb.SegmentInfo{
				proto.Clone(segmentInfo).(*datapb.SegmentInfo),
			},
			ChannelCheckpoint: map[string]*msgpb.MsgPosition{
				mockVChannel: {
					ChannelName: mockPChannel,
					Timestamp:   t3,
				},
			},
		}
		req := packLoadSegmentRequest(segmentTask, segmentAction, nil, nil, nil, resp)
		assert.Equal(t, 1, len(req.GetDeltaPositions()))
		assert.Equal(t, mockPChannel, req.DeltaPositions[0].ChannelName)
		assert.Equal(t, t3, req.DeltaPositions[0].Timestamp)
	})

	t.Run("test set deltaPosition from segment dmlPosition", func(t *testing.T) {
		segmentAction := NewSegmentAction(0, 0, "", 0)
		segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, 0, 0, 0, segmentAction)
		assert.NoError(t, err)

		resp := &datapb.GetSegmentInfoResponse{
			Infos: []*datapb.SegmentInfo{
				proto.Clone(segmentInfo).(*datapb.SegmentInfo),
			},
		}
		req := packLoadSegmentRequest(segmentTask, segmentAction, nil, nil, nil, resp)
		assert.Equal(t, 1, len(req.GetDeltaPositions()))
		assert.Equal(t, mockPChannel, req.DeltaPositions[0].ChannelName)
		assert.Equal(t, t2, req.DeltaPositions[0].Timestamp)
	})

	t.Run("test set deltaPosition from segment startPosition", func(t *testing.T) {
		segmentAction := NewSegmentAction(0, 0, "", 0)
		segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, 0, 0, 0, segmentAction)
		assert.NoError(t, err)

		segInfo := proto.Clone(segmentInfo).(*datapb.SegmentInfo)
		segInfo.DmlPosition = nil
		resp := &datapb.GetSegmentInfoResponse{
			Infos: []*datapb.SegmentInfo{segInfo},
		}
		req := packLoadSegmentRequest(segmentTask, segmentAction, nil, nil, nil, resp)
		assert.Equal(t, 1, len(req.GetDeltaPositions()))
		assert.Equal(t, mockPChannel, req.DeltaPositions[0].ChannelName)
		assert.Equal(t, t1, req.DeltaPositions[0].Timestamp)
	})

	t.Run("test tsLag > 10minutes", func(t *testing.T) {
		segmentAction := NewSegmentAction(0, 0, "", 0)
		segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, 0, 0, 0, segmentAction)
		assert.NoError(t, err)

		segInfo := proto.Clone(segmentInfo).(*datapb.SegmentInfo)
		segInfo.DmlPosition.Timestamp = t0
		resp := &datapb.GetSegmentInfoResponse{
			Infos: []*datapb.SegmentInfo{segInfo},
		}
		req := packLoadSegmentRequest(segmentTask, segmentAction, nil, nil, nil, resp)
		assert.Equal(t, 1, len(req.GetDeltaPositions()))
		assert.Equal(t, mockPChannel, req.DeltaPositions[0].ChannelName)
		assert.Equal(t, t0, req.DeltaPositions[0].Timestamp)
	})
}
