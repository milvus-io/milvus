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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestDataNode(t *testing.T) {
	Params.Init()
	refreshChannelNames()

	node := newDataNodeMock()
	node.Start()

	t.Run("Test WatchDmChannels", func(t *testing.T) {
		channelNames := Params.InsertChannelNames

		req := &datapb.WatchDmChannelsRequest{
			ChannelNames: channelNames,
		}
		_, err1 := node.WatchDmChannels(node.ctx, req)
		assert.Error(t, err1)

		node.UpdateStateCode(internalpb.StateCode_Initializing)
		_, err2 := node.WatchDmChannels(node.ctx, req)
		assert.Error(t, err2)

		Params.InsertChannelNames = []string{}
		status, err3 := node.WatchDmChannels(node.ctx, req)
		assert.NoError(t, err3)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		Params.InsertChannelNames = channelNames
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		stat, err := node.GetComponentStates(node.ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.Status.ErrorCode)
	})

	t.Run("Test FlushSegments", func(t *testing.T) {
		req := &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{0, 1},
		}
		status, err := node.FlushSegments(node.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err := node.GetTimeTickChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err := node.GetStatisticsChannel(node.ctx)
		assert.NoError(t, err)
	})

	<-node.ctx.Done()
	node.Stop()
}
