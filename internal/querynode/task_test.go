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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestTask_watchDmChannelsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema := genTestCollectionSchema()

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
				FlushedSegmentIds: []int64{},
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
				DroppedSegmentIds: []int64{},
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
	schema := genTestCollectionSchema()

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

		fieldBinlog, statsLog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
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
					Statslogs:    statsLog,
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

		fieldBinlog, statsLog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
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
					Statslogs:    statsLog,
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
		num := node.metaReplica.getSegmentNum(segmentTypeSealed)
		assert.Equal(t, 1, num)
	})

	t.Run("test FromDmlCPLoadDelete", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		vDmChannel := "by-dev-rootcoord-dml_1_2021v1"
		pDmChannel := funcutil.ToPhysicalChannel(vDmChannel)
		stream, err := node.factory.NewMsgStream(node.queryNodeLoopCtx)
		assert.Nil(t, err)
		stream.AsProducer([]string{pDmChannel})
		timeTickMsg := &msgstream.TimeTickMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{1},
			},
			TimeTickMsg: internalpb.TimeTickMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_TimeTick,
					Timestamp: 100,
				},
			},
		}

		deleteMsg := &msgstream.DeleteMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{1, 1, 1},
			},
			DeleteRequest: internalpb.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Delete,
					Timestamp: 110,
				},
				CollectionID: defaultCollectionID,
				PartitionID:  defaultPartitionID,
				PrimaryKeys: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				Timestamps: []Timestamp{110, 110, 110},
				NumRows:    3,
			},
		}

		pos1, err := stream.ProduceMark(&msgstream.MsgPack{Msgs: []msgstream.TsMsg{timeTickMsg}})
		assert.NoError(t, err)
		msgIDs, ok := pos1[pDmChannel]
		assert.True(t, ok)
		assert.Equal(t, 1, len(msgIDs))
		err = stream.Produce(&msgstream.MsgPack{Msgs: []msgstream.TsMsg{deleteMsg}})
		assert.NoError(t, err)

		// to stop reader from cp
		go func() {
			for {
				select {
				case <-ctx.Done():
					break
				default:
					timeTickMsg.Base.Timestamp += 100
					stream.Produce(&msgstream.MsgPack{Msgs: []msgstream.TsMsg{timeTickMsg}})
					time.Sleep(200 * time.Millisecond)
				}
			}
		}()

		segmentID := defaultSegmentID + 1
		fieldBinlog, statsLog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, segmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    segmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
					NumOfRows:    defaultMsgLength,
					Statslogs:    statsLog,
				},
			},
			DeltaPositions: []*internalpb.MsgPosition{
				{
					ChannelName: vDmChannel,
					MsgID:       msgIDs[0].Serialize(),
					Timestamp:   100,
				},
			},
		}

		task := loadSegmentsTask{
			req:  req,
			node: node,
		}
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
		segment, err := node.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
		assert.NoError(t, err)

		// has reload 3 delete log from dm channel, so next delete offset should be 3
		offset := segment.segmentPreDelete(1)
		assert.Equal(t, int64(3), offset)
	})

	t.Run("test OOM", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		totalRAM := Params.QueryNodeCfg.CacheSize * 1024 * 1024 * 1024

		col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
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

		err = node.metaReplica.removeCollection(defaultCollectionID)
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
		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("test execute remove deltaVChannel", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

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
		_, err = task.node.dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})
}
