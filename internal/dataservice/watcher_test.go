// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package dataservice

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/stretchr/testify/assert"
)

func TestWatcher(t *testing.T) {
	const collID = UniqueID(0)
	const partID = UniqueID(100)

	Params.Init()

	cluster := newDataNodeCluster()
	defer cluster.ShutDownClients()
	schema := newTestSchema()
	allocator := newMockAllocator()
	meta, err := newMemoryMeta(allocator)
	assert.Nil(t, err)
	segAllocator := newSegmentAllocator(meta, allocator)
	assert.Nil(t, err)

	collInfo := &datapb.CollectionInfo{
		Schema: schema,
		ID:     collID,
	}

	t.Run("Test ProxyTimeTickWatcher", func(t *testing.T) {
		proxyWatcher := newProxyTimeTickWatcher(segAllocator)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go proxyWatcher.StartBackgroundLoop(ctx)

		msg := &msgstream.TimeTickMsg{
			TimeTickMsg: internalpb.TimeTickMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_TimeTick,
					Timestamp: 100,
				},
			},
		}
		proxyWatcher.Watch(msg)
		time.Sleep(time.Second)
	})

	t.Run("Test DataNodeTimeTickWatcher", func(t *testing.T) {
		datanodeWatcher := newDataNodeTimeTickWatcher(meta, segAllocator, cluster)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go datanodeWatcher.StartBackgroundLoop(ctx)

		err = meta.AddCollection(collInfo)
		assert.Nil(t, err)

		cases := []struct {
			sealed   bool
			expired  bool
			expected bool
		}{
			{false, true, false},
			{false, true, false},
			{false, false, false},
			{true, true, true},
			{true, false, false},
			{true, true, true},
		}

		segIDs := make([]UniqueID, 0)
		for i := range cases {
			segID, _, _, err := segAllocator.AllocSegment(ctx, collID, partID, "channel"+strconv.Itoa(i), 100)
			assert.Nil(t, err)
			segIDs = append(segIDs, segID)
		}

		time.Sleep(time.Duration(Params.SegIDAssignExpiration+1000) * time.Millisecond)
		for i, c := range cases {
			if !c.expired {
				_, _, _, err := segAllocator.AllocSegment(ctx, collID, partID, "channel"+strconv.Itoa(i), 100)
				assert.Nil(t, err)
			}
			if c.sealed {
				err := segAllocator.SealSegment(ctx, segIDs[i])
				assert.Nil(t, err)
			}
		}
		ts, err := allocator.allocTimestamp()
		assert.Nil(t, err)

		ttMsg := &msgstream.TimeTickMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			TimeTickMsg: internalpb.TimeTickMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_TimeTick,
					Timestamp: ts,
				},
			},
		}
		datanodeWatcher.Watch(ttMsg)

		time.Sleep(time.Second)

		// check flushed segments been removed from segAllocator
		for i, c := range cases {
			ok := segAllocator.HasSegment(ctx, segIDs[i])
			assert.EqualValues(t, !c.expected, ok)
		}
	})
}
