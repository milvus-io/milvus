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

package masterservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

type proxyMock struct {
	types.ProxyService
	randVal   int
	collArray []string
	mutex     sync.Mutex
}

func (p *proxyMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: fmt.Sprintf("proxy-time-tick-%d", p.randVal),
	}, nil
}
func (p *proxyMock) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.collArray = append(p.collArray, request.CollectionName)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
func (p *proxyMock) GetCollArray() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ret := make([]string, 0, len(p.collArray))
	ret = append(ret, p.collArray...)
	return ret
}

type dataMock struct {
	types.DataService
	randVal int
}

func (d *dataMock) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	rst := &datapb.GetInsertBinlogPathsResponse{
		FieldIDs: []int64{},
		Paths:    []*internalpb.StringList{},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	for i := 0; i < 200; i++ {
		rst.FieldIDs = append(rst.FieldIDs, int64(i))
		path := &internalpb.StringList{
			Values: []string{fmt.Sprintf("file0-%d", i), fmt.Sprintf("file1-%d", i), fmt.Sprintf("file2-%d", i)},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}
		rst.Paths = append(rst.Paths, path)
	}
	return rst, nil
}

func (d *dataMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Infos: []*datapb.SegmentInfo{
			{
				FlushedTime: 100,
				NumRows:     Params.MinSegmentSizeToEnableIndex,
				State:       commonpb.SegmentState_Flushed,
			},
		},
	}, nil
}

func (d *dataMock) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: fmt.Sprintf("segment-info-channel-%d", d.randVal),
	}, nil
}

type queryMock struct {
	types.QueryService
	collID []typeutil.UniqueID
	mutex  sync.Mutex
}

func (q *queryMock) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.collID = append(q.collID, req.CollectionID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

type indexMock struct {
	types.IndexService
	fileArray  []string
	idxBuildID []int64
	idxID      []int64
	idxDropID  []int64
	mutex      sync.Mutex
}

func (idx *indexMock) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	idx.fileArray = append(idx.fileArray, req.DataPaths...)
	idx.idxBuildID = append(idx.idxBuildID, rand.Int63())
	idx.idxID = append(idx.idxID, req.IndexID)
	return &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		IndexBuildID: idx.idxBuildID[len(idx.idxBuildID)-1],
	}, nil
}

func (idx *indexMock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	idx.idxDropID = append(idx.idxDropID, req.IndexID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (idx *indexMock) getFileArray() []string {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	ret := make([]string, 0, len(idx.fileArray))
	ret = append(ret, idx.fileArray...)
	return ret
}

func consumeMsgChan(timeout time.Duration, targetChan <-chan *msgstream.MsgPack) {
	for {
		select {
		case <-time.After(timeout):
			return
		case <-targetChan:

		}
	}
}

func TestMasterService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := msgstream.NewPmsFactory()
	Params.Init()
	core, err := NewCore(ctx, msFactory)
	assert.Nil(t, err)
	randVal := rand.Int()

	Params.TimeTickChannel = fmt.Sprintf("master-time-tick-%d", randVal)
	Params.DdChannel = fmt.Sprintf("master-dd-%d", randVal)
	Params.StatisticsChannel = fmt.Sprintf("master-statistics-%d", randVal)
	Params.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.MetaRootPath)
	Params.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.KvRootPath)
	Params.MsgChannelSubName = fmt.Sprintf("subname-%d", randVal)

	pm := &proxyMock{
		randVal:   randVal,
		collArray: make([]string, 0, 16),
		mutex:     sync.Mutex{},
	}
	err = core.SetProxyService(ctx, pm)
	assert.Nil(t, err)

	dm := &dataMock{randVal: randVal}
	err = core.SetDataService(ctx, dm)
	assert.Nil(t, err)

	im := &indexMock{
		fileArray:  []string{},
		idxBuildID: []int64{},
		idxID:      []int64{},
		idxDropID:  []int64{},
		mutex:      sync.Mutex{},
	}
	err = core.SetIndexService(im)
	assert.Nil(t, err)

	qm := &queryMock{
		collID: nil,
		mutex:  sync.Mutex{},
	}
	err = core.SetQueryService(qm)
	assert.Nil(t, err)

	err = core.Init()
	assert.Nil(t, err)

	err = core.Start()
	assert.Nil(t, err)

	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	proxyTimeTickStream, _ := msFactory.NewMsgStream(ctx)
	proxyTimeTickStream.AsProducer([]string{Params.ProxyTimeTickChannel})

	dataServiceSegmentStream, _ := msFactory.NewMsgStream(ctx)
	dataServiceSegmentStream.AsProducer([]string{Params.DataServiceSegmentChannel})

	timeTickStream, _ := msFactory.NewMsgStream(ctx)
	timeTickStream.AsConsumer([]string{Params.TimeTickChannel}, Params.MsgChannelSubName)
	timeTickStream.Start()

	ddStream, _ := msFactory.NewMsgStream(ctx)
	ddStream.AsConsumer([]string{Params.DdChannel}, Params.MsgChannelSubName)
	ddStream.Start()

	time.Sleep(time.Second)

	t.Run("time tick", func(t *testing.T) {
		var timeTick typeutil.Timestamp = 100
		msgPack := msgstream.MsgPack{}
		baseMsg := msgstream.BaseMsg{
			BeginTimestamp: timeTick,
			EndTimestamp:   timeTick,
			HashValues:     []uint32{0},
		}
		timeTickResult := internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: timeTick,
				SourceID:  0,
			},
		}
		timeTickMsg := &msgstream.TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
		err := proxyTimeTickStream.Broadcast(&msgPack)
		assert.Nil(t, err)

		ttmsg, ok := <-timeTickStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, len(ttmsg.Msgs), 1)
		ttm, ok := (ttmsg.Msgs[0]).(*msgstream.TimeTickMsg)
		assert.True(t, ok)
		assert.Equal(t, ttm.Base.Timestamp, timeTick)

		ddmsg, ok := <-ddStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, len(ddmsg.Msgs), 1)
		ddm, ok := (ddmsg.Msgs[0]).(*msgstream.TimeTickMsg)
		assert.True(t, ok)
		assert.Equal(t, ddm.Base.Timestamp, timeTick)
	})

	t.Run("create collection", func(t *testing.T) {
		schema := schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "testColl",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "vector",
					IsPrimaryKey: false,
					Description:  "vector",
					DataType:     schemapb.DataType_FloatVector,
					TypeParams:   nil,
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "ik1",
							Value: "iv1",
						},
					},
				},
			},
		}
		sbf, err := proto.Marshal(&schema)
		assert.Nil(t, err)

		req := &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     100,
				Timestamp: 100,
				SourceID:  100,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			Schema:         sbf,
		}
		status, err := core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)

		msg, ok := <-ddStream.Chan()
		assert.True(t, ok)
		assert.True(t, len(msg.Msgs) == 2 || len(msg.Msgs) == 1)

		createMsg, ok := (msg.Msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		createMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, createMsg.CollectionID, createMeta.ID)
		assert.Equal(t, len(createMeta.PartitionIDs), 1)

		if len(msg.Msgs) == 2 {
			createPart, ok := (msg.Msgs[1]).(*msgstream.CreatePartitionMsg)
			assert.True(t, ok)
			assert.Equal(t, createPart.CollectionName, "testColl")
			assert.Equal(t, createPart.PartitionID, createMeta.PartitionIDs[0])
		} else {
			msg, ok = <-ddStream.Chan()
			assert.True(t, ok)
			createPart, ok := (msg.Msgs[0]).(*msgstream.CreatePartitionMsg)
			assert.True(t, ok)
			assert.Equal(t, createPart.CollectionName, "testColl")
			assert.Equal(t, createPart.PartitionID, createMeta.PartitionIDs[0])
		}

		req.Base.MsgID = 101
		req.Base.Timestamp = 101
		req.Base.SourceID = 101
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

		req.Base.MsgID = 102
		req.Base.Timestamp = 102
		req.Base.SourceID = 102
		req.CollectionName = "testColl-again"
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

		schema.Name = req.CollectionName
		sbf, err = proto.Marshal(&schema)
		assert.Nil(t, err)
		req.Schema = sbf
		req.Base.MsgID = 103
		req.Base.Timestamp = 103
		req.Base.SourceID = 103
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)

		msg, ok = <-ddStream.Chan()
		assert.True(t, ok)
		createMsg, ok = (msg.Msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		createMeta, err = core.MetaTable.GetCollectionByName("testColl-again")
		assert.Nil(t, err)
		assert.Equal(t, createMsg.CollectionID, createMeta.ID)
	})

	t.Run("has collection", func(t *testing.T) {
		req := &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     110,
				Timestamp: 110,
				SourceID:  110,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.Value, true)

		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     111,
				Timestamp: 111,
				SourceID:  111,
			},
			DbName:         "testDb",
			CollectionName: "testColl2",
		}
		rsp, err = core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.Value, false)

		// test time stamp go back
		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     111,
				Timestamp: 111,
				SourceID:  111,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err = core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.Value, true)
	})

	t.Run("describe collection", func(t *testing.T) {
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     120,
				Timestamp: 120,
				SourceID:  120,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := core.DescribeCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.Schema.Name, "testColl")
		assert.Equal(t, rsp.CollectionID, collMeta.ID)

	})

	t.Run("show collection", func(t *testing.T) {
		req := &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     130,
				Timestamp: 130,
				SourceID:  130,
			},
			DbName: "testDb",
		}
		rsp, err := core.ShowCollections(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.ElementsMatch(t, rsp.CollectionNames, []string{"testColl", "testColl-again"})
		assert.Equal(t, len(rsp.CollectionNames), 2)
	})

	t.Run("create partition", func(t *testing.T) {
		req := &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     140,
				Timestamp: 140,
				SourceID:  140,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		consumeMsgChan(time.Second, ddStream.Chan())
		status, err := core.CreatePartition(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.PartitionIDs), 2)
		partMeta, err := core.MetaTable.GetPartitionByID(collMeta.PartitionIDs[1])
		assert.Nil(t, err)
		assert.Equal(t, partMeta.PartitionName, "testPartition")

		msg, ok := <-ddStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, len(msg.Msgs), 1)
		partMsg, ok := (msg.Msgs[0]).(*msgstream.CreatePartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, partMsg.CollectionID, collMeta.ID)
		assert.Equal(t, partMsg.PartitionID, partMeta.PartitionID)

		assert.Equal(t, 1, len(pm.GetCollArray()))
		assert.Equal(t, "testColl", pm.GetCollArray()[0])
	})

	t.Run("has partition", func(t *testing.T) {
		req := &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     150,
				Timestamp: 150,
				SourceID:  150,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		rsp, err := core.HasPartition(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.Value, true)
	})

	t.Run("show partition", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		req := &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     160,
				Timestamp: 160,
				SourceID:  160,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			CollectionID:   coll.ID,
		}
		rsp, err := core.ShowPartitions(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, len(rsp.PartitionNames), 2)
		assert.Equal(t, len(rsp.PartitionIDs), 2)
	})

	t.Run("show segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		partID := coll.PartitionIDs[1]
		part, err := core.MetaTable.GetPartitionByID(partID)
		assert.Nil(t, err)
		assert.Zero(t, len(part.SegmentIDs))

		seg := &datapb.SegmentInfo{
			ID:           1000,
			CollectionID: coll.ID,
			PartitionID:  part.PartitionID,
		}

		msgPack := msgstream.MsgPack{}
		baseMsg := msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		}
		segMsg := &msgstream.SegmentInfoMsg{
			BaseMsg: baseMsg,
			SegmentMsg: datapb.SegmentMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SegmentInfo,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  0,
				},
				Segment: seg,
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, segMsg)
		err = dataServiceSegmentStream.Broadcast(&msgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)

		part, err = core.MetaTable.GetPartitionByID(partID)
		assert.Nil(t, err)
		assert.Equal(t, len(part.SegmentIDs), 1)

		req := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     170,
				Timestamp: 170,
				SourceID:  170,
			},
			CollectionID: coll.ID,
			PartitionID:  partID,
		}
		rsp, err := core.ShowSegments(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, rsp.SegmentIDs[0], int64(1000))
		assert.Equal(t, len(rsp.SegmentIDs), 1)
	})

	t.Run("create index", func(t *testing.T) {
		req := &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     180,
				Timestamp: 180,
				SourceID:  180,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   "ik2",
					Value: "iv2",
				},
			},
		}
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.FieldIndexes), 0)

		rsp, err := core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.ErrorCode, commonpb.ErrorCode_Success)
		time.Sleep(time.Second)
		files := im.getFileArray()
		assert.Equal(t, 3, len(files))
		assert.ElementsMatch(t, files, []string{"file0-100", "file1-100", "file2-100"})
		collMeta, err = core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.FieldIndexes), 1)
		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, idxMeta.IndexName, Params.DefaultIndexName)

		req.FieldName = "no field"
		rsp, err = core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.NotEqual(t, rsp.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("describe segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)

		req := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     190,
				Timestamp: 190,
				SourceID:  190,
			},
			CollectionID: coll.ID,
			SegmentID:    1000,
		}
		rsp, err := core.DescribeSegment(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		t.Logf("index id = %d", rsp.IndexID)
	})

	t.Run("describe index", func(t *testing.T) {
		req := &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     200,
				Timestamp: 200,
				SourceID:  200,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			IndexName:      "",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, len(rsp.IndexDescriptions), 1)
		assert.Equal(t, rsp.IndexDescriptions[0].IndexName, Params.DefaultIndexName)
	})

	t.Run("describe index not exist", func(t *testing.T) {
		req := &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     200,
				Timestamp: 200,
				SourceID:  200,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			IndexName:      "not-exist-index",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_IndexNotExist)
		assert.Equal(t, len(rsp.IndexDescriptions), 0)
	})

	t.Run("flush segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		partID := coll.PartitionIDs[1]
		part, err := core.MetaTable.GetPartitionByID(partID)
		assert.Nil(t, err)
		assert.Equal(t, len(part.SegmentIDs), 1)

		seg := &datapb.SegmentInfo{
			ID:           1001,
			CollectionID: coll.ID,
			PartitionID:  part.PartitionID,
		}

		msgPack := msgstream.MsgPack{}
		baseMsg := msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{0},
		}
		segMsg := &msgstream.SegmentInfoMsg{
			BaseMsg: baseMsg,
			SegmentMsg: datapb.SegmentMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SegmentInfo,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  0,
				},
				Segment: seg,
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, segMsg)
		err = dataServiceSegmentStream.Broadcast(&msgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)

		part, err = core.MetaTable.GetPartitionByID(partID)
		assert.Nil(t, err)
		assert.Equal(t, len(part.SegmentIDs), 2)

		flushMsg := &msgstream.FlushCompletedMsg{
			BaseMsg: baseMsg,
			SegmentFlushCompletedMsg: internalpb.SegmentFlushCompletedMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SegmentFlushDone,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  0,
				},
				SegmentID: 1001,
			},
		}
		msgPack.Msgs = []msgstream.TsMsg{flushMsg}
		err = dataServiceSegmentStream.Broadcast(&msgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)

		req := &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     210,
				Timestamp: 210,
				SourceID:  210,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			IndexName:      "",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
		assert.Equal(t, len(rsp.IndexDescriptions), 1)
		assert.Equal(t, rsp.IndexDescriptions[0].IndexName, Params.DefaultIndexName)
	})

	t.Run("over ride index", func(t *testing.T) {
		req := &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     211,
				Timestamp: 211,
				SourceID:  211,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   "ik3",
					Value: "iv3",
				},
			},
		}

		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.FieldIndexes), 1)
		oldIdx := collMeta.FieldIndexes[0].IndexID

		rsp, err := core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.ErrorCode, commonpb.ErrorCode_Success)
		time.Sleep(time.Second)

		collMeta, err = core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.FieldIndexes), 2)

		assert.Equal(t, oldIdx, collMeta.FieldIndexes[0].IndexID)

		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[1].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, idxMeta.IndexName, Params.DefaultIndexName)

		idxMeta, err = core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, idxMeta.IndexName, Params.DefaultIndexName+"_bak")

	})

	t.Run("drop index", func(t *testing.T) {
		req := &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     215,
				Timestamp: 215,
				SourceID:  215,
			},
			DbName:         "",
			CollectionName: "testColl",
			FieldName:      "vector",
			IndexName:      Params.DefaultIndexName,
		}
		idx, err := core.MetaTable.GetIndexByName("testColl", Params.DefaultIndexName)
		assert.Nil(t, err)
		assert.Equal(t, len(idx), 1)

		rsp, err := core.DropIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.ErrorCode, commonpb.ErrorCode_Success)

		im.mutex.Lock()
		assert.Equal(t, len(im.idxDropID), 1)
		assert.Equal(t, im.idxDropID[0], idx[0].IndexID)
		im.mutex.Unlock()

		idx, err = core.MetaTable.GetIndexByName("testColl", Params.DefaultIndexName)
		assert.Nil(t, err)
		assert.Equal(t, len(idx), 0)
	})

	t.Run("drop partition", func(t *testing.T) {
		req := &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     220,
				Timestamp: 220,
				SourceID:  220,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		dropPartID := collMeta.PartitionIDs[1]
		status, err := core.DropPartition(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
		collMeta, err = core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.PartitionIDs), 1)
		partMeta, err := core.MetaTable.GetPartitionByID(collMeta.PartitionIDs[0])
		assert.Nil(t, err)
		assert.Equal(t, partMeta.PartitionName, Params.DefaultPartitionName)

		msg, ok := <-ddStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, len(msg.Msgs), 1)
		dmsg, ok := (msg.Msgs[0]).(*msgstream.DropPartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, dmsg.CollectionID, collMeta.ID)
		assert.Equal(t, dmsg.PartitionID, dropPartID)

		assert.Equal(t, 2, len(pm.GetCollArray()))
		assert.Equal(t, "testColl", pm.GetCollArray()[1])
	})

	t.Run("drop collection", func(t *testing.T) {
		req := &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     230,
				Timestamp: 230,
				SourceID:  230,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		status, err := core.DropCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)

		msg, ok := <-ddStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, len(msg.Msgs), 1)
		dmsg, ok := (msg.Msgs[0]).(*msgstream.DropCollectionMsg)
		assert.True(t, ok)
		assert.Equal(t, dmsg.CollectionID, collMeta.ID)
		collArray := pm.GetCollArray()
		assert.Equal(t, len(collArray), 3)
		assert.Equal(t, collArray[2], "testColl")

		time.Sleep(time.Millisecond * 100)
		qm.mutex.Lock()
		assert.Equal(t, len(qm.collID), 1)
		assert.Equal(t, qm.collID[0], collMeta.ID)
		qm.mutex.Unlock()

		req = &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     231,
				Timestamp: 231,
				SourceID:  231,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		status, err = core.DropCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
		time.Sleep(time.Second)
		assert.Zero(t, len(ddStream.Chan()))
		collArray = pm.GetCollArray()
		assert.Equal(t, len(collArray), 3)
		assert.Equal(t, collArray[2], "testColl")
	})

	t.Run("context_cancel", func(t *testing.T) {
		ctx2, cancel2 := context.WithTimeout(ctx, time.Millisecond*100)
		defer cancel2()
		time.Sleep(time.Millisecond * 150)
		st, err := core.CreateCollection(ctx2, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     1000,
				Timestamp: 1000,
				SourceID:  1000,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropCollection(ctx2, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     1001,
				Timestamp: 1001,
				SourceID:  1001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp1, err := core.HasCollection(ctx2, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     1002,
				Timestamp: 1002,
				SourceID:  1002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp1.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp2, err := core.DescribeCollection(ctx2, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     1003,
				Timestamp: 1003,
				SourceID:  1003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp2.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp3, err := core.ShowCollections(ctx2, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     1004,
				Timestamp: 1004,
				SourceID:  1004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp3.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreatePartition(ctx2, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     1005,
				Timestamp: 1005,
				SourceID:  1005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropPartition(ctx2, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     1006,
				Timestamp: 1006,
				SourceID:  1006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp4, err := core.HasPartition(ctx2, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     1007,
				Timestamp: 1007,
				SourceID:  1007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp4.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp5, err := core.ShowPartitions(ctx2, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     1008,
				Timestamp: 1008,
				SourceID:  1008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp5.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreateIndex(ctx2, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     1009,
				Timestamp: 1009,
				SourceID:  1009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp6, err := core.DescribeIndex(ctx2, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     1010,
				Timestamp: 1010,
				SourceID:  1010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp6.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropIndex(ctx2, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     1011,
				Timestamp: 1011,
				SourceID:  1011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp7, err := core.DescribeSegment(ctx2, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     1012,
				Timestamp: 1012,
				SourceID:  1012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp7.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp8, err := core.ShowSegments(ctx2, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     1013,
				Timestamp: 1013,
				SourceID:  1013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp8.Status.ErrorCode, commonpb.ErrorCode_Success)

	})

	t.Run("undefine req type", func(t *testing.T) {
		st, err := core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2000,
				Timestamp: 2000,
				SourceID:  2000,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2001,
				Timestamp: 2001,
				SourceID:  2001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2002,
				Timestamp: 2002,
				SourceID:  2002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp1.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2003,
				Timestamp: 2003,
				SourceID:  2003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp2.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2004,
				Timestamp: 2004,
				SourceID:  2004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp3.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2005,
				Timestamp: 2005,
				SourceID:  2005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2006,
				Timestamp: 2006,
				SourceID:  2006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2007,
				Timestamp: 2007,
				SourceID:  2007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp4.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2008,
				Timestamp: 2008,
				SourceID:  2008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp5.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2009,
				Timestamp: 2009,
				SourceID:  2009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2010,
				Timestamp: 2010,
				SourceID:  2010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp6.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2011,
				Timestamp: 2011,
				SourceID:  2011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2012,
				Timestamp: 2012,
				SourceID:  2012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp7.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2013,
				Timestamp: 2013,
				SourceID:  2013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp8.Status.ErrorCode, commonpb.ErrorCode_Success)

	})

	t.Run("alloc time tick", func(t *testing.T) {
		req := &masterpb.AllocTimestampRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3000,
				Timestamp: 3000,
				SourceID:  3000,
			},
			Count: 1,
		}
		rsp, err := core.AllocTimestamp(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, uint32(1), rsp.Count)
		assert.NotZero(t, rsp.Timestamp)
	})

	t.Run("alloc id", func(t *testing.T) {
		req := &masterpb.AllocIDRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3001,
				Timestamp: 3001,
				SourceID:  3001,
			},
			Count: 1,
		}
		rsp, err := core.AllocID(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, uint32(1), rsp.Count)
		assert.NotZero(t, rsp.ID)
	})

	t.Run("get_channels", func(t *testing.T) {
		_, err := core.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		_, err = core.GetDdChannel(ctx)
		assert.Nil(t, err)
		_, err = core.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
	})

	err = core.Stop()
	assert.Nil(t, err)
	st, err := core.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, st.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.NotEqual(t, st.State.StateCode, internalpb.StateCode_Healthy)

	t.Run("state_not_healthy", func(t *testing.T) {
		st, err := core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     4000,
				Timestamp: 4000,
				SourceID:  4000,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     4001,
				Timestamp: 4001,
				SourceID:  4001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     4002,
				Timestamp: 4002,
				SourceID:  4002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp1.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     4003,
				Timestamp: 4003,
				SourceID:  4003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp2.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     4004,
				Timestamp: 4004,
				SourceID:  4004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp3.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     4005,
				Timestamp: 4005,
				SourceID:  4005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     4006,
				Timestamp: 4006,
				SourceID:  4006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     4007,
				Timestamp: 4007,
				SourceID:  4007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp4.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     4008,
				Timestamp: 4008,
				SourceID:  4008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp5.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     4009,
				Timestamp: 4009,
				SourceID:  4009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     4010,
				Timestamp: 4010,
				SourceID:  4010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp6.Status.ErrorCode, commonpb.ErrorCode_Success)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     4011,
				Timestamp: 4011,
				SourceID:  4011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_Success)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     4012,
				Timestamp: 4012,
				SourceID:  4012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp7.Status.ErrorCode, commonpb.ErrorCode_Success)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     4013,
				Timestamp: 4013,
				SourceID:  4013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, rsp8.Status.ErrorCode, commonpb.ErrorCode_Success)

	})

	t.Run("alloc_error", func(t *testing.T) {
		core.idAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			return 0, 0, fmt.Errorf("id allocator error test")
		}
		core.tsoAllocator = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("tso allcoator error test")
		}
		r1 := &masterpb.AllocTimestampRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     5000,
				Timestamp: 5000,
				SourceID:  5000,
			},
			Count: 1,
		}
		p1, err := core.AllocTimestamp(ctx, r1)
		assert.Nil(t, err)
		assert.NotEqual(t, p1.Status.ErrorCode, commonpb.ErrorCode_Success)

		r2 := &masterpb.AllocIDRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3001,
				Timestamp: 3001,
				SourceID:  3001,
			},
			Count: 1,
		}
		p2, err := core.AllocID(ctx, r2)
		assert.Nil(t, err)
		assert.NotEqual(t, p2.Status.ErrorCode, commonpb.ErrorCode_Success)
	})
}

func TestCheckInit(t *testing.T) {
	c, err := NewCore(context.Background(), nil)
	assert.Nil(t, err)

	err = c.Start()
	assert.NotNil(t, err)

	err = c.checkInit()
	assert.NotNil(t, err)

	c.MetaTable = &metaTable{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.idAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		return 0, 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.idAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.tsoAllocator = func(count uint32) (typeutil.Timestamp, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.tsoAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.etcdCli = &clientv3.Client{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.metaKV = &etcdkv.EtcdKV{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.kvBase = &etcdkv.EtcdKV{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.ProxyTimeTickChan = make(chan typeutil.Timestamp)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.ddReqQueue = make(chan reqTask)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DdCreateCollectionReq = func(ctx context.Context, req *internalpb.CreateCollectionRequest) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DdDropCollectionReq = func(ctx context.Context, req *internalpb.DropCollectionRequest) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DdCreatePartitionReq = func(ctx context.Context, req *internalpb.CreatePartitionRequest) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DdDropPartitionReq = func(ctx context.Context, req *internalpb.DropPartitionRequest) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DataServiceSegmentChan = make(chan *datapb.SegmentInfo)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.GetBinlogFilePathsFromDataServiceReq = func(segID, fieldID typeutil.UniqueID) ([]string, error) {
		return []string{}, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.GetNumRowsReq = func(segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.BuildIndexReq = func(ctx context.Context, binlog []string, typeParams, indexParams []*commonpb.KeyValuePair, indexID typeutil.UniqueID, indexName string) (typeutil.UniqueID, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DropIndexReq = func(ctx context.Context, indexID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.InvalidateCollectionMetaCache = func(ctx context.Context, ts typeutil.Timestamp, dbName, collectionName string) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.indexTaskQueue = make(chan *CreateIndexTask)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DataNodeSegmentFlushCompletedChan = make(chan int64)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.ReleaseCollection = func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.Nil(t, err)
}
