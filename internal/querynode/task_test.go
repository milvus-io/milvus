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

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestTask_AddQueryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genAddQueryChanelRequest := func() *querypb.AddQueryChannelRequest {
		return &querypb.AddQueryChannelRequest{
			Base:               genCommonMsgBase(commonpb.MsgType_LoadCollection),
			NodeID:             0,
			CollectionID:       defaultCollectionID,
			QueryChannel:       genQueryChannel(),
			QueryResultChannel: genQueryResultChannel(),
		}
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := addQueryChannelTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genAddQueryChanelRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := addQueryChannelTask{
			req: genAddQueryChanelRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := addQueryChannelTask{
			req:  genAddQueryChanelRequest(),
			node: node,
		}

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
	t.Run("test execute has queryCollection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := addQueryChannelTask{
			req:  genAddQueryChanelRequest(),
			node: node,
		}

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
	t.Run("test execute nil query service", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		node.queryShardService = nil

		task := addQueryChannelTask{
			req:  genAddQueryChanelRequest(),
			node: node,
		}

		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test execute init global sealed segments", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := addQueryChannelTask{
			req:  genAddQueryChanelRequest(),
			node: node,
		}

		task.req.GlobalSealedSegments = []*querypb.SegmentInfo{{
			SegmentID:    defaultSegmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
		}}

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute not init global sealed segments", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := addQueryChannelTask{
			req:  genAddQueryChanelRequest(),
			node: node,
		}

		task.req.GlobalSealedSegments = []*querypb.SegmentInfo{{
			SegmentID:    defaultSegmentID,
			CollectionID: 1000,
			PartitionID:  defaultPartitionID,
		}}

		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
	/*
			t.Run("test execute seek error", func(t *testing.T) {
				node, err := genSimpleQueryNode(ctx)
				assert.NoError(t, err)

		t.Run("test execute seek error", func(t *testing.T) {
			node, err := genSimpleQueryNode(ctx)
			assert.NoError(t, err)

			position := &internalpb.MsgPosition{
				ChannelName: genQueryChannel(),
				MsgID:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
				MsgGroup:    defaultSubName,
				Timestamp:   0,
			}
				task := addQueryChannelTask{
					req:  genAddQueryChanelRequest(),
					node: node,
				}

				task.req.SeekPosition = position

				err = task.Execute(ctx)
				assert.Error(t, err)
			})*/
}

func TestTask_watchDmChannelsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	genWatchDMChannelsRequest := func() *querypb.WatchDmChannelsRequest {
		req := &querypb.WatchDmChannelsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_WatchDmChannels),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       schema,
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
		assert.NoError(t, err)

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

	t.Run("test execute loadPartition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

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
		assert.NoError(t, err)

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

	//t.Run("test execute seek error", func(t *testing.T) {
	//
	//	node, err := genSimpleQueryNode(ctx)
	//	assert.NoError(t, err)
	//
	//	task := watchDmChannelsTask{
	//		req:  genWatchDMChannelsRequest(),
	//		node: node,
	//	}
	//	task.req.Infos = []*datapb.VchannelInfo{
	//		{
	//			CollectionID: defaultCollectionID,
	//			ChannelName:  defaultDMLChannel,
	//			SeekPosition: &msgstream.MsgPosition{
	//				ChannelName: defaultDMLChannel,
	//				MsgID:       []byte{1, 2, 3},
	//				MsgGroup:    defaultSubName,
	//				Timestamp:   0,
	//			},
	//		},
	//	}
	//	err = task.Execute(ctx)
	//	assert.Error(t, err)
	//})

	t.Run("test add excluded segment for flushed segment", func(t *testing.T) {

		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

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
				FlushedSegments: []*datapb.SegmentInfo{
					{
						DmlPosition: &internalpb.MsgPosition{
							ChannelName: tmpChannel,
							Timestamp:   typeutil.MaxTimestamp,
						},
					},
				},
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test add excluded segment for dropped segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

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
				DroppedSegments: []*datapb.SegmentInfo{
					{
						DmlPosition: &internalpb.MsgPosition{
							ChannelName: tmpChannel,
							Timestamp:   typeutil.MaxTimestamp,
						},
					},
				},
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test load growing segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := watchDmChannelsTask{
			req:  genWatchDMChannelsRequest(),
			node: node,
		}

		fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDMLChannel,
				UnflushedSegments: []*datapb.SegmentInfo{
					{
						CollectionID: defaultCollectionID,
						PartitionID:  defaultPartitionID + 1, // load a new partition
						DmlPosition: &internalpb.MsgPosition{
							ChannelName: defaultDMLChannel,
							Timestamp:   typeutil.MaxTimestamp,
						},
						Binlogs: fieldBinlog,
					},
				},
			},
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}

func TestTask_watchDeltaChannelsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genWatchDeltaChannelsRequest := func() *querypb.WatchDeltaChannelsRequest {
		req := &querypb.WatchDeltaChannelsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_WatchDeltaChannels),
			CollectionID: defaultCollectionID,
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := watchDeltaChannelsTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genWatchDeltaChannelsRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := watchDeltaChannelsTask{
			req: genWatchDeltaChannelsRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := watchDeltaChannelsTask{
			req:  genWatchDeltaChannelsRequest(),
			node: node,
		}
		task.ctx = ctx
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDeltaChannel,
				SeekPosition: &internalpb.MsgPosition{
					ChannelName: defaultDMLChannel,
					MsgID:       pulsar.EarliestMessageID().Serialize(),
					MsgGroup:    defaultSubName,
					Timestamp:   0,
				},
			},
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute without init collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := watchDeltaChannelsTask{
			req:  genWatchDeltaChannelsRequest(),
			node: node,
		}
		task.ctx = ctx
		task.req.Infos = []*datapb.VchannelInfo{
			{
				CollectionID: defaultCollectionID,
				ChannelName:  defaultDeltaChannel,
				SeekPosition: &internalpb.MsgPosition{
					ChannelName: defaultDeltaChannel,
					MsgID:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
					MsgGroup:    defaultSubName,
					Timestamp:   0,
				},
			},
		}
		task.req.CollectionID++
		err = task.Execute(ctx)
		assert.Error(t, err)
	})
}

func TestTask_loadSegmentsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	genLoadEmptySegmentsRequest := func() *querypb.LoadSegmentsRequest {
		req := &querypb.LoadSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_LoadSegments),
			CollectionID: defaultCollectionID,
			Schema:       schema,
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := loadSegmentsTask{
			baseTask: baseTask{
				ts: timestamp,
			},
			req: genLoadEmptySegmentsRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := loadSegmentsTask{
			req: genLoadEmptySegmentsRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute grpc", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		task := loadSegmentsTask{
			req:  req,
			node: node,
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test repeated load", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		task := loadSegmentsTask{
			req:  req,
			node: node,
		}
		// execute loadSegmentsTask twice
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
		// expected only one segment in replica
		num := node.historical.getSegmentNum()
		assert.Equal(t, 1, num)
	})

	t.Run("test execute grpc error", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := loadSegmentsTask{
			req:  genLoadEmptySegmentsRequest(),
			node: node,
		}
		task.req.Infos = []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID + 1,
				PartitionID:  defaultPartitionID + 1,
				CollectionID: defaultCollectionID + 1,
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test execute node down", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := loadSegmentsTask{
			req:  genLoadEmptySegmentsRequest(),
			node: node,
		}
		task.req.Infos = []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID + 1,
				PartitionID:  defaultPartitionID + 1,
				CollectionID: defaultCollectionID + 1,
			},
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test OOM", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		totalRAM := Params.QueryNodeCfg.CacheSize * 1024 * 1024 * 1024

		col, err := node.historical.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		assert.NoError(t, err)

		task := loadSegmentsTask{
			req:  genLoadEmptySegmentsRequest(),
			node: node,
		}
		task.req.Infos = []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				NumOfRows:    totalRAM / int64(sizePerRecord),
				SegmentSize:  totalRAM,
			},
		}
		// Reach the segment size that would cause OOM
		for node.loader.checkSegmentSize(defaultCollectionID, task.req.Infos, 1) == nil {
			task.req.Infos[0].SegmentSize *= 2
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "OOM")
	})
}

func TestTask_releaseCollectionTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genReleaseCollectionRequest := func() *querypb.ReleaseCollectionRequest {
		req := &querypb.ReleaseCollectionRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_LoadSegments),
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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		err = node.streaming.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		err = node.historical.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		task := releaseCollectionTask{
			req:  genReleaseCollectionRequest(),
			node: node,
		}
		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test execute remove deltaVChannel tSafe", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		col, err := node.historical.getCollectionByID(defaultCollectionID)
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
			Base:         genCommonMsgBase(commonpb.MsgType_LoadSegments),
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

	t.Run("test execute", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		/*
			err = node.queryService.addQueryCollection(defaultCollectionID)
			assert.NoError(t, err)*/

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		_, err = task.node.dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test execute no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		task := releasePartitionsTask{
			req:  genReleasePartitionsRequest(),
			node: node,
		}
		err = node.historical.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		err = node.streaming.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test execute remove deltaVChannel", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		col, err := node.historical.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		err = node.historical.removePartition(defaultPartitionID)
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
		_, err = task.node.dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}
