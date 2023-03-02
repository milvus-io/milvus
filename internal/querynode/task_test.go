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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestTask_watchDmChannelsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema := genTestCollectionSchema()

	genWatchDMChannelsRequest := func() *querypb.WatchDmChannelsRequest {
		req := &querypb.WatchDmChannelsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_WatchDmChannels, 0),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       schema,
			Infos: []*datapb.VchannelInfo{
				{
					ChannelName: defaultDMLChannel,
				},
			},
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := watchDmChannelsTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genWatchDMChannelsRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := watchDmChannelsTask{
			req: genWatchDMChannelsRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute loadCollection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
			},
		}
		task.req.PartitionIDs = []UniqueID{0}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute repeated watchDmChannelTask", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}

		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
			},
		}
		task.req.PartitionIDs = []UniqueID{0}

		err = task.Execute(ctx)
		assert.NoError(t, err)
		// query coord may submit same watchDmChannelTask
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute loadPartition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		task.req.LoadMeta = &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadPartition,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
			},
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute loadPartition without init collection and partition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
			},
		}
		task.req.CollectionID++
		task.req.PartitionIDs[0]++
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute seek error", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		task.req.LoadMeta = &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadPartition,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID:        defaultCollectionID,
				ChannelName:         defaultDMLChannel,
				UnflushedSegmentIds: []int64{100},
				FlushedSegmentIds:   []int64{101},
				DroppedSegmentIds:   []int64{102},
				SeekPosition: &msgpb.MsgPosition{
					ChannelName: defaultDMLChannel,
					MsgID:       []byte{235, 50, 164, 248, 255, 255, 255, 255},
					Timestamp:   Timestamp(999),
				},
			},
		}
		task.req.SegmentInfos = map[int64]*datapb.SegmentInfo{
			100: {
				ID: 100,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: defaultDMLChannel,
					Timestamp:   Timestamp(1000),
				},
			},
			101: {
				ID: 101,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: defaultDMLChannel,
					Timestamp:   Timestamp(1001),
				},
			},
			102: {
				ID: 102,
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: defaultDMLChannel,
					Timestamp:   Timestamp(1002),
				},
			},
		}
		err = task.Execute(ctx)
		// ["Failed to seek"] [error="topic name = xxx not exist"]
		assert.Error(t, err)
	})

	t.Run("test add excluded segment for flushed segment", func(t *testing.T) {

		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		tmpChannel := defaultDMLChannel + "_1"
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
				SeekPosition: &msgstream.MsgPosition{
					ChannelName: tmpChannel,
					Timestamp:   0,
					MsgID:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
				},
				FlushedSegmentIds: []int64{},
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test add excluded segment for dropped segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}
		tmpChannel := defaultDMLChannel + "_1"
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
				SeekPosition: &msgstream.MsgPosition{
					ChannelName: tmpChannel,
					Timestamp:   0,
					MsgID:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
				},
				DroppedSegmentIds: []int64{},
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test load growing segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}

		assert.NoError(t, err)

		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID:        defaultCollectionID,
				ChannelName:         defaultDMLChannel,
				UnflushedSegmentIds: []int64{},
			},
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}

func TestTask_releaseCollectionTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genReleaseCollectionRequest := func() *querypb.ReleaseCollectionRequest {
		req := &querypb.ReleaseCollectionRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseCollection, 0),
			CollectionID: defaultCollectionID,
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := releaseCollectionTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genReleaseCollectionRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := releaseCollectionTask{
			req: genReleaseCollectionRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		task := releaseCollectionTask{
			req:  genReleaseCollectionRequest(),
			node: node,
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		task := releaseCollectionTask{
			req:  genReleaseCollectionRequest(),
			node: node,
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute remove deltaVChannel tSafe", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.addVDeltaChannels([]Channel{defaultDeltaChannel})

		task := releaseCollectionTask{
			req:  genReleaseCollectionRequest(),
			node: node,
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}

func TestTask_releasePartitionTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genReleasePartitionsRequest := func() *querypb.ReleasePartitionsRequest {
		req := &querypb.ReleasePartitionsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleasePartitions, 0),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := releasePartitionsTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genReleasePartitionsRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := releasePartitionsTask{
			req: genReleasePartitionsRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test isAllPartitionsReleased", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}

		coll, err := node.metaReplica.getCollectionByID(defaultCollectionID)
		require.NoError(t, err)

		assert.False(t, task.isAllPartitionsReleased(nil))
		assert.True(t, task.isAllPartitionsReleased(coll))
		node.metaReplica.addPartition(defaultCollectionID, -1)
		assert.False(t, task.isAllPartitionsReleased(coll))
		node.metaReplica.removePartition(defaultPartitionID)
		node.metaReplica.removePartition(-1)

		assert.True(t, task.isAllPartitionsReleased(coll))
	})

	t.Run("test execute", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		_, err = task.node.dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, map[Channel]*msgstream.MsgPosition{defaultDMLChannel: nil})
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute no partition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		err = node.metaReplica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute non-exist partition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		req := genReleasePartitionsRequest()
		req.PartitionIDs = []int64{-1}
		task := releasePartitionsTask{
			req:  req,
			node: node,
		}

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute remove deltaVChannel", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		err = node.metaReplica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		col.addVDeltaChannels([]Channel{defaultDeltaChannel})
		col.setLoadType(loadTypePartition)

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		_, err = task.node.dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, map[Channel]*msgstream.MsgPosition{defaultDMLChannel: nil})
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}
