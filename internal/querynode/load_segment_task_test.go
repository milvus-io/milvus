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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/merr"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestTask_loadSegmentsTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema := genTestCollectionSchema()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)
	testVChannel := "by-dev-rootcoord-dml_1_2021v1"
	fieldBinlog, statsLog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
	assert.NoError(t, err)

	genLoadEmptySegmentsRequest := func() *querypb.LoadSegmentsRequest {
		req := &querypb.LoadSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_LoadSegments, 0),
			CollectionID: defaultCollectionID,
			Schema:       schema,
		}
		return req
	}

	t.Run("test timestamp", func(t *testing.T) {
		timestamp := Timestamp(1000)
		task := loadSegmentsTask{
			baseTask: baseTask{
				ts:  timestamp,
				ctx: ctx,
			},
			req: genLoadEmptySegmentsRequest(),
		}
		resT := task.Timestamp()
		assert.Equal(t, timestamp, resT)
	})

	t.Run("test OnEnqueue", func(t *testing.T) {
		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req: genLoadEmptySegmentsRequest(),
		}
		err := task.OnEnqueue()
		assert.NoError(t, err)
		task.req.Base = nil
		err = task.OnEnqueue()
		assert.NoError(t, err)
	})

	t.Run("test execute grpc", func(t *testing.T) {
		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
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
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  req,
			node: node,
		}
		err = task.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("test repeated load", func(t *testing.T) {
		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
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
			baseTask: baseTask{
				ctx: ctx,
			},
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

	t.Run("test OOM", func(t *testing.T) {
		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		totalRAM := int64(hardware.GetMemoryCount())

		col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		assert.NoError(t, err)

		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  genLoadEmptySegmentsRequest(),
			node: node,
		}
		binlogs := []*datapb.Binlog{
			{
				LogSize: totalRAM,
			},
		}
		task.req.Infos = []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				NumOfRows:    totalRAM / int64(sizePerRecord),
				SegmentSize:  totalRAM,
				BinlogPaths:  []*datapb.FieldBinlog{{Binlogs: binlogs}},
			},
		}
		// Reach the segment size that would cause OOM
		for node.loader.checkSegmentSize(defaultCollectionID, task.req.Infos, 1) == nil {
			task.req.Infos[0].SegmentSize *= 2
		}
		err = task.Execute(ctx)
		assert.ErrorIs(t, err, merr.ErrServiceMemoryLimitExceeded)
	})

	factory := node.loader.factory
	t.Run("test FromDmlCPLoadDelete failed", func(t *testing.T) {
		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		msgStream := &LoadDeleteMsgStream{}
		msgStream.On("AsConsumer", mock.AnythingOfTypeArgument("string"), mock.AnythingOfTypeArgument("string"))
		getLastMsgIDErr := fmt.Errorf("mock err")
		msgStream.On("GetLatestMsgID", mock.AnythingOfType("string")).Return(nil, getLastMsgIDErr)
		factory := &mockMsgStreamFactory{mockMqStream: msgStream}
		node.loader.factory = factory

		segmentLoadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    UniqueID(1000),
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
		}
		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
			Schema: schema,
			Infos:  []*querypb.SegmentLoadInfo{segmentLoadInfo},
			DeltaPositions: []*msgpb.MsgPosition{
				{
					ChannelName: testVChannel,
					MsgID:       rmq.SerializeRmqID(0),
				},
			},
		}

		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  req,
			node: node,
		}
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.Error(t, err)
		// expected 0 segment and 0 delta flow graph because FromDmlCPLoadDelete failed
		num := node.metaReplica.getSegmentNum(segmentTypeSealed)
		assert.Equal(t, 0, num)
		fgNum := node.dataSyncService.getFlowGraphNum()
		assert.Equal(t, 0, fgNum)
	})

	node.loader.factory = factory
	pDmChannel := funcutil.ToPhysicalChannel(testVChannel)
	stream, err := node.factory.NewMsgStream(node.queryNodeLoopCtx)
	assert.Nil(t, err)
	stream.AsProducer([]string{pDmChannel})
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{1},
		},
		TimeTickMsg: msgpb.TimeTickMsg{
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
		DeleteRequest: msgpb.DeleteRequest{
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

	pos1, err := stream.Broadcast(&msgstream.MsgPack{Msgs: []msgstream.TsMsg{timeTickMsg}})
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

	t.Run("test FromDmlCPLoadDelete", func(t *testing.T) {
		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     defaultSegmentID,
					PartitionID:   defaultPartitionID,
					CollectionID:  defaultCollectionID,
					BinlogPaths:   fieldBinlog,
					NumOfRows:     defaultMsgLength,
					Statslogs:     statsLog,
					InsertChannel: testVChannel,
				},
			},
			DeltaPositions: []*msgpb.MsgPosition{
				{
					ChannelName: testVChannel,
					MsgID:       msgIDs[0].Serialize(),
					Timestamp:   100,
				},
			},
		}

		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  req,
			node: node,
		}
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
		segment, err := node.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
		assert.NoError(t, err)

		// has reload 3 delete log from dm channel, so next delete offset should be 3
		offset := segment.segmentPreDelete(1)
		assert.Equal(t, int64(3), offset)
	})

	t.Run("test load with partial success", func(t *testing.T) {
		deltaChannel, err := funcutil.ConvertChannelName(testVChannel, Params.CommonCfg.RootCoordDml.GetValue(), Params.CommonCfg.RootCoordDelta.GetValue())
		assert.NoError(t, err)

		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)
		node.dataSyncService.removeFlowGraphsByDMLChannels([]Channel{testVChannel})
		node.dataSyncService.removeFlowGraphsByDeltaChannels([]Channel{deltaChannel})

		fakeFieldBinlog, fakeStatsBinlog, err := getFakeBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		segmentID1 := defaultSegmentID
		segmentID2 := defaultSegmentID + 1
		req := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     segmentID1,
					PartitionID:   defaultPartitionID,
					CollectionID:  defaultCollectionID,
					BinlogPaths:   fieldBinlog,
					NumOfRows:     defaultMsgLength,
					Statslogs:     statsLog,
					InsertChannel: testVChannel,
				},
				{
					SegmentID:     segmentID2,
					PartitionID:   defaultPartitionID,
					CollectionID:  defaultCollectionID,
					BinlogPaths:   fakeFieldBinlog,
					NumOfRows:     defaultMsgLength,
					Statslogs:     fakeStatsBinlog,
					InsertChannel: testVChannel,
				},
			},
			DeltaPositions: []*msgpb.MsgPosition{
				{
					ChannelName: testVChannel,
					MsgID:       msgIDs[0].Serialize(),
					Timestamp:   100,
				},
			},
		}

		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  req,
			node: node,
		}
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.Error(t, err)
		exist, err := node.metaReplica.hasSegment(segmentID1, segmentTypeSealed)
		assert.NoError(t, err)
		assert.True(t, exist)
		exist, err = node.metaReplica.hasSegment(segmentID2, segmentTypeSealed)
		assert.NoError(t, err)
		assert.False(t, exist)
	})
}

func TestTask_loadSegmentsTaskLoadDelta(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema := genTestCollectionSchema()

	t.Run("test repeated load delta channel", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()
		vDmChannel := "by-dev-rootcoord-dml-test_2_2021v2"

		segmentLoadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    UniqueID(1000),
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
		}
		loadReq := &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
			Schema: schema,
			Infos:  []*querypb.SegmentLoadInfo{segmentLoadInfo},
			DeltaPositions: []*msgpb.MsgPosition{
				{
					ChannelName: vDmChannel,
					MsgID:       rmq.SerializeRmqID(0),
					Timestamp:   100,
				},
			},
		}

		task := loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  loadReq,
			node: node,
		}
		// execute loadSegmentsTask twice
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)
		// expected only one segment in replica
		num := node.metaReplica.getSegmentNum(segmentTypeSealed)
		assert.Equal(t, 2, num)

		// load second segments with same channel
		loadReq = &querypb.LoadSegmentsRequest{
			Base:   genCommonMsgBase(commonpb.MsgType_LoadSegments, node.GetSession().ServerID),
			Schema: schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    UniqueID(1001),
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
				},
			},
			DeltaPositions: []*msgpb.MsgPosition{
				{
					ChannelName: vDmChannel,
					MsgID:       rmq.SerializeRmqID(0),
					Timestamp:   100,
				},
			},
		}

		task = loadSegmentsTask{
			baseTask: baseTask{
				ctx: ctx,
			},
			req:  loadReq,
			node: node,
		}
		// execute loadSegmentsTask twice
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		err = task.Execute(ctx)
		assert.NoError(t, err)

		num = node.metaReplica.getSegmentNum(segmentTypeSealed)
		assert.Equal(t, 3, num)

		ok := node.queryShardService.hasQueryShard(vDmChannel)
		assert.True(t, ok)

		assert.Equal(t, len(node.dataSyncService.dmlChannel2FlowGraph), 0)
		assert.Equal(t, len(node.dataSyncService.deltaChannel2FlowGraph), 1)
	})
}
