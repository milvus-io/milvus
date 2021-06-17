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
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

type proxyNodeMock struct {
	types.ProxyNode
	collArray []string
	mutex     sync.Mutex
}

func (p *proxyNodeMock) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.collArray = append(p.collArray, request.CollectionName)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
func (p *proxyNodeMock) GetCollArray() []string {
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
				NumOfRows: Params.MinSegmentSizeToEnableIndex,
				State:     commonpb.SegmentState_Flushed,
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

func clearMsgChan(timeout time.Duration, targetChan <-chan *msgstream.MsgPack) {
	ch := time.After(timeout)
	for {
		select {
		case <-ch:
			return
		case <-targetChan:

		}
	}
}

func GenSegInfoMsgPack(seg *datapb.SegmentInfo) *msgstream.MsgPack {
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
	return &msgPack
}

func GenFlushedSegMsgPack(segID typeutil.UniqueID) *msgstream.MsgPack {
	msgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	segMsg := &msgstream.FlushCompletedMsg{
		BaseMsg: baseMsg,
		SegmentFlushCompletedMsg: internalpb.SegmentFlushCompletedMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SegmentFlushDone,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			SegmentID: segID,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, segMsg)
	return &msgPack
}

func getNotTtMsg(ctx context.Context, n int, ch <-chan *msgstream.MsgPack) []msgstream.TsMsg {
	ret := make([]msgstream.TsMsg, 0, n)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if ok {
				for _, v := range msg.Msgs {
					if _, ok := v.(*msgstream.TimeTickMsg); !ok {
						ret = append(ret, v)
					}
				}
				if len(ret) >= n {
					return ret
				}
			}
		}
	}
}

func TestMasterService(t *testing.T) {
	const (
		dbName   = "testDb"
		collName = "testColl"
		partName = "testPartition"
		segID    = 1001
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreFactory := msgstream.NewPmsFactory()
	Params.Init()
	core, err := NewCore(ctx, coreFactory)
	assert.Nil(t, err)
	randVal := rand.Int()

	Params.TimeTickChannel = fmt.Sprintf("master-time-tick-%d", randVal)
	Params.StatisticsChannel = fmt.Sprintf("master-statistics-%d", randVal)
	Params.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.MetaRootPath)
	Params.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.KvRootPath)
	Params.MsgChannelSubName = fmt.Sprintf("subname-%d", randVal)
	Params.DataServiceSegmentChannel = fmt.Sprintf("data-service-segment-%d", randVal)

	err = core.Register()
	assert.Nil(t, err)

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints, DialTimeout: 5 * time.Second})
	assert.Nil(t, err)
	sessKey := path.Join(Params.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)
	defer func() {
		_, _ = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	}()

	pnb, err := json.Marshal(
		&sessionutil.Session{
			ServerID: 100,
		},
	)
	assert.Nil(t, err)
	_, err = etcdCli.Put(ctx, path.Join(sessKey, typeutil.ProxyNodeRole+"-100"), string(pnb))
	assert.Nil(t, err)

	pnm := &proxyNodeMock{
		collArray: make([]string, 0, 16),
		mutex:     sync.Mutex{},
	}
	core.NewProxyClient = func(*sessionutil.Session) (types.ProxyNode, error) {
		return pnm, nil
	}

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

	tmpFactory := msgstream.NewPmsFactory()

	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024}
	err = tmpFactory.SetParams(m)
	assert.Nil(t, err)

	dataServiceSegmentStream, _ := tmpFactory.NewMsgStream(ctx)
	dataServiceSegmentStream.AsProducer([]string{Params.DataServiceSegmentChannel})

	timeTickStream, _ := tmpFactory.NewMsgStream(ctx)
	timeTickStream.AsConsumer([]string{Params.TimeTickChannel}, Params.MsgChannelSubName)
	timeTickStream.Start()

	dmlStream, _ := tmpFactory.NewMsgStream(ctx)

	// test dataServiceSegmentStream seek
	dataNodeSubName := Params.MsgChannelSubName + "dn"
	flushedSegStream, _ := tmpFactory.NewMsgStream(ctx)
	flushedSegStream.AsConsumer([]string{Params.DataServiceSegmentChannel}, dataNodeSubName)
	flushedSegStream.Start()
	msgPackTmp := GenFlushedSegMsgPack(9999)
	err = dataServiceSegmentStream.Produce(msgPackTmp)
	assert.Nil(t, err)

	flushedSegMsgPack := flushedSegStream.Consume()
	flushedSegStream.Close()

	flushedSegPosStr, _ := EncodeMsgPositions(flushedSegMsgPack.EndPositions)

	_, err = etcdCli.Put(ctx, path.Join(Params.MetaRootPath, FlushedSegMsgEndPosPrefix), flushedSegPosStr)
	assert.Nil(t, err)

	err = core.Init()
	assert.Nil(t, err)

	var localTSO uint64 = 0
	localTSOLock := sync.RWMutex{}
	core.TSOAllocator = func(c uint32) (uint64, error) {
		localTSOLock.Lock()
		defer localTSOLock.Unlock()
		localTSO += uint64(c)
		return localTSO, nil
	}

	err = core.Start()
	assert.Nil(t, err)

	time.Sleep(time.Second)

	t.Run("time tick", func(t *testing.T) {
		ttmsg, ok := <-timeTickStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, 1, len(ttmsg.Msgs))
		ttm, ok := (ttmsg.Msgs[0]).(*msgstream.TimeTickMsg)
		assert.True(t, ok)
		assert.Greater(t, ttm.Base.Timestamp, uint64(0))
		t.Log(ttm.Base.Timestamp)

		ttmsg2, ok := <-timeTickStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, 1, len(ttmsg2.Msgs))
		ttm2, ok := (ttmsg2.Msgs[0]).(*msgstream.TimeTickMsg)
		assert.True(t, ok)
		assert.Greater(t, ttm2.Base.Timestamp, uint64(0))
		assert.Equal(t, ttm2.Base.Timestamp, ttm.Base.Timestamp+1)
	})

	t.Run("create collection", func(t *testing.T) {
		schema := schemapb.CollectionSchema{
			Name:   collName,
			AutoID: true,
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
			DbName:         dbName,
			CollectionName: collName,
			Schema:         sbf,
		}
		status, err := core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		assert.Equal(t, 2, len(core.MetaTable.vChan2Chan))
		assert.Equal(t, 2, len(core.dmlChannels.dml))

		pChan := core.MetaTable.ListCollectionPhysicalChannels()
		dmlStream.AsConsumer([]string{pChan[0]}, Params.MsgChannelSubName)
		dmlStream.Start()

		// get CreateCollectionMsg
		msgPack, ok := <-dmlStream.Chan()
		assert.True(t, ok)
		createMsg, ok := (msgPack.Msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		createMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, createMeta.ID, createMsg.CollectionID)
		assert.Equal(t, 1, len(createMeta.PartitionIDs))
		assert.Equal(t, 2, len(createMeta.VirtualChannelNames))
		assert.Equal(t, 2, len(createMeta.PhysicalChannelNames))

		vChanName := createMeta.VirtualChannelNames[0]
		chanName, err := core.MetaTable.GetChanNameByVirtualChan(vChanName)
		assert.Nil(t, err)
		assert.Equal(t, createMeta.PhysicalChannelNames[0], chanName)

		// get CreatePartitionMsg
		msgPack, ok = <-dmlStream.Chan()
		assert.True(t, ok)
		createPart, ok := (msgPack.Msgs[0]).(*msgstream.CreatePartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, collName, createPart.CollectionName)
		assert.Equal(t, createMeta.PartitionIDs[0], createPart.PartitionID)

		// get TimeTickMsg
		//msgPack, ok = <-dmlStream.Chan()
		//assert.True(t, ok)
		//assert.Equal(t, 1, len(msgPack.Msgs))
		//ddm, ok := (msgPack.Msgs[0]).(*msgstream.TimeTickMsg)
		//assert.True(t, ok)
		//assert.Greater(t, ddm.Base.Timestamp, uint64(0))
		core.chanTimeTick.lock.Lock()
		assert.Equal(t, len(core.chanTimeTick.proxyTimeTick), 2)
		pt, ok := core.chanTimeTick.proxyTimeTick[core.session.ServerID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(pt.ChannelNames))
		assert.Equal(t, 2, len(pt.Timestamps))
		assert.Equal(t, pt.ChannelNames, createMeta.PhysicalChannelNames)
		assert.Equal(t, pt.Timestamps[0], pt.Timestamps[1])
		assert.LessOrEqual(t, createPart.BeginTimestamp, pt.Timestamps[0])
		core.chanTimeTick.lock.Unlock()

		// check DD operation info
		flag, err := core.MetaTable.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.client.Load(DDOperationPrefix, 0)
		assert.Nil(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.Nil(t, err)
		assert.Equal(t, CreateCollectionDDType, ddOp.Type)

		var ddCollReq = internalpb.CreateCollectionRequest{}
		err = proto.UnmarshalText(ddOp.Body, &ddCollReq)
		assert.Nil(t, err)
		assert.Equal(t, createMeta.ID, ddCollReq.CollectionID)

		var ddPartReq = internalpb.CreatePartitionRequest{}
		err = proto.UnmarshalText(ddOp.Body1, &ddPartReq)
		assert.Nil(t, err)
		assert.Equal(t, createMeta.ID, ddPartReq.CollectionID)
		assert.Equal(t, createMeta.PartitionIDs[0], ddPartReq.PartitionID)

		// check invalid operation
		req.Base.MsgID = 101
		req.Base.Timestamp = 101
		req.Base.SourceID = 101
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		req.Base.MsgID = 102
		req.Base.Timestamp = 102
		req.Base.SourceID = 102
		req.CollectionName = "testColl-again"
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		schema.Name = req.CollectionName
		sbf, err = proto.Marshal(&schema)
		assert.Nil(t, err)
		req.Schema = sbf
		req.Base.MsgID = 103
		req.Base.Timestamp = 103
		req.Base.SourceID = 103
		status, err = core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("has collection", func(t *testing.T) {
		req := &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     110,
				Timestamp: 110,
				SourceID:  110,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		rsp, err := core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, true, rsp.Value)

		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     111,
				Timestamp: 111,
				SourceID:  111,
			},
			DbName:         dbName,
			CollectionName: "testColl2",
		}
		rsp, err = core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, false, rsp.Value)

		// test time stamp go back
		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     111,
				Timestamp: 111,
				SourceID:  111,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		rsp, err = core.HasCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, true, rsp.Value)
	})

	t.Run("describe collection", func(t *testing.T) {
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     120,
				Timestamp: 120,
				SourceID:  120,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		rsp, err := core.DescribeCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, collName, rsp.Schema.Name)
		assert.Equal(t, collMeta.ID, rsp.CollectionID)
		assert.Equal(t, 2, len(rsp.VirtualChannelNames))
		assert.Equal(t, 2, len(rsp.PhysicalChannelNames))
	})

	t.Run("show collection", func(t *testing.T) {
		req := &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     130,
				Timestamp: 130,
				SourceID:  130,
			},
			DbName: dbName,
		}
		rsp, err := core.ShowCollections(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.ElementsMatch(t, rsp.CollectionNames, []string{collName, "testColl-again"})
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
			DbName:         dbName,
			CollectionName: collName,
			PartitionName:  partName,
		}
		clearMsgChan(10*time.Millisecond, dmlStream.Chan())
		status, err := core.CreatePartition(ctx, req)
		assert.Nil(t, err)
		t.Log(status.Reason)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(collMeta.PartitionIDs))
		partMeta, err := core.MetaTable.GetPartitionByID(1, collMeta.PartitionIDs[1], 0)
		assert.Nil(t, err)
		assert.Equal(t, partName, partMeta.PartitionName)

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		partMsg, ok := (msgs[0]).(*msgstream.CreatePartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, partMsg.CollectionID)
		assert.Equal(t, partMeta.PartitionID, partMsg.PartitionID)

		assert.Equal(t, 1, len(pnm.GetCollArray()))
		assert.Equal(t, collName, pnm.GetCollArray()[0])

		// check DD operation info
		flag, err := core.MetaTable.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.client.Load(DDOperationPrefix, 0)
		assert.Nil(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.Nil(t, err)
		assert.Equal(t, CreatePartitionDDType, ddOp.Type)

		var ddReq = internalpb.CreatePartitionRequest{}
		err = proto.UnmarshalText(ddOp.Body, &ddReq)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)
		assert.Equal(t, partMeta.PartitionID, ddReq.PartitionID)
	})

	t.Run("has partition", func(t *testing.T) {
		req := &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     150,
				Timestamp: 150,
				SourceID:  150,
			},
			DbName:         dbName,
			CollectionName: collName,
			PartitionName:  partName,
		}
		rsp, err := core.HasPartition(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, true, rsp.Value)
	})

	t.Run("show partition", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		req := &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     160,
				Timestamp: 160,
				SourceID:  160,
			},
			DbName:         dbName,
			CollectionName: collName,
			CollectionID:   coll.ID,
		}
		rsp, err := core.ShowPartitions(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 2, len(rsp.PartitionNames))
		assert.Equal(t, 2, len(rsp.PartitionIDs))
	})

	t.Run("show segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		partID := coll.PartitionIDs[1]
		part, err := core.MetaTable.GetPartitionByID(1, partID, 0)
		assert.Nil(t, err)
		assert.Zero(t, len(part.SegmentIDs))

		seg := &datapb.SegmentInfo{
			ID:           1000,
			CollectionID: coll.ID,
			PartitionID:  part.PartitionID,
		}
		segInfoMsgPack := GenSegInfoMsgPack(seg)
		err = dataServiceSegmentStream.Broadcast(segInfoMsgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)

		part, err = core.MetaTable.GetPartitionByID(1, partID, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(part.SegmentIDs))

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
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, int64(1000), rsp.SegmentIDs[0])
		assert.Equal(t, 1, len(rsp.SegmentIDs))
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
			CollectionName: collName,
			FieldName:      "vector",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   "ik2",
					Value: "iv2",
				},
			},
		}
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(collMeta.FieldIndexes))

		rsp, err := core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
		time.Sleep(time.Second)
		files := im.getFileArray()
		assert.Equal(t, 3, len(files))
		assert.ElementsMatch(t, files, []string{"file0-100", "file1-100", "file2-100"})
		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(collMeta.FieldIndexes))
		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, Params.DefaultIndexName, idxMeta.IndexName)

		req.FieldName = "no field"
		rsp, err = core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	t.Run("describe segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
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
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
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
			CollectionName: collName,
			FieldName:      "vector",
			IndexName:      "",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 1, len(rsp.IndexDescriptions))
		assert.Equal(t, Params.DefaultIndexName, rsp.IndexDescriptions[0].IndexName)
		assert.Equal(t, "vector", rsp.IndexDescriptions[0].FieldName)
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
			CollectionName: collName,
			FieldName:      "vector",
			IndexName:      "not-exist-index",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, rsp.Status.ErrorCode)
		assert.Equal(t, 0, len(rsp.IndexDescriptions))
	})

	t.Run("flush segment", func(t *testing.T) {
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		partID := coll.PartitionIDs[1]
		part, err := core.MetaTable.GetPartitionByID(1, partID, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(part.SegmentIDs))

		seg := &datapb.SegmentInfo{
			ID:           segID,
			CollectionID: coll.ID,
			PartitionID:  part.PartitionID,
		}
		segInfoMsgPack := GenSegInfoMsgPack(seg)
		err = dataServiceSegmentStream.Broadcast(segInfoMsgPack)
		assert.Nil(t, err)
		time.Sleep(time.Second)

		part, err = core.MetaTable.GetPartitionByID(1, partID, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(part.SegmentIDs))

		flushedSegMsgPack := GenFlushedSegMsgPack(segID)
		err = dataServiceSegmentStream.Broadcast(flushedSegMsgPack)
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
			CollectionName: collName,
			FieldName:      "vector",
			IndexName:      "",
		}
		rsp, err := core.DescribeIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 1, len(rsp.IndexDescriptions))
		assert.Equal(t, Params.DefaultIndexName, rsp.IndexDescriptions[0].IndexName)
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
			CollectionName: collName,
			FieldName:      "vector",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   "ik3",
					Value: "iv3",
				},
			},
		}

		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(collMeta.FieldIndexes))
		oldIdx := collMeta.FieldIndexes[0].IndexID

		rsp, err := core.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
		time.Sleep(time.Second)

		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(collMeta.FieldIndexes))
		assert.Equal(t, oldIdx, collMeta.FieldIndexes[0].IndexID)

		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[1].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, Params.DefaultIndexName, idxMeta.IndexName)

		idxMeta, err = core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.Nil(t, err)
		assert.Equal(t, Params.DefaultIndexName+"_bak", idxMeta.IndexName)

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
			CollectionName: collName,
			FieldName:      "vector",
			IndexName:      Params.DefaultIndexName,
		}
		_, idx, err := core.MetaTable.GetIndexByName(collName, Params.DefaultIndexName)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(idx))

		rsp, err := core.DropIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)

		im.mutex.Lock()
		assert.Equal(t, 1, len(im.idxDropID))
		assert.Equal(t, idx[0].IndexID, im.idxDropID[0])
		im.mutex.Unlock()

		_, idx, err = core.MetaTable.GetIndexByName(collName, Params.DefaultIndexName)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(idx))
	})

	t.Run("drop partition", func(t *testing.T) {
		req := &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     220,
				Timestamp: 220,
				SourceID:  220,
			},
			DbName:         dbName,
			CollectionName: collName,
			PartitionName:  partName,
		}
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		dropPartID := collMeta.PartitionIDs[1]
		status, err := core.DropPartition(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(collMeta.PartitionIDs))
		partMeta, err := core.MetaTable.GetPartitionByID(1, collMeta.PartitionIDs[0], 0)
		assert.Nil(t, err)
		assert.Equal(t, Params.DefaultPartitionName, partMeta.PartitionName)

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		dmsg, ok := (msgs[0]).(*msgstream.DropPartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, dmsg.CollectionID)
		assert.Equal(t, dropPartID, dmsg.PartitionID)

		assert.Equal(t, 2, len(pnm.GetCollArray()))
		assert.Equal(t, collName, pnm.GetCollArray()[1])

		// check DD operation info
		flag, err := core.MetaTable.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.client.Load(DDOperationPrefix, 0)
		assert.Nil(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.Nil(t, err)
		assert.Equal(t, DropPartitionDDType, ddOp.Type)

		var ddReq = internalpb.DropPartitionRequest{}
		err = proto.UnmarshalText(ddOp.Body, &ddReq)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)
		assert.Equal(t, dropPartID, ddReq.PartitionID)
	})

	t.Run("drop collection", func(t *testing.T) {
		req := &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     230,
				Timestamp: 230,
				SourceID:  230,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		status, err := core.DropCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		vChanName := collMeta.VirtualChannelNames[0]
		_, err = core.MetaTable.GetChanNameByVirtualChan(vChanName)
		assert.NotNil(t, err)

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		dmsg, ok := (msgs[0]).(*msgstream.DropCollectionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, dmsg.CollectionID)
		collArray := pnm.GetCollArray()
		assert.Equal(t, 3, len(collArray))
		assert.Equal(t, collName, collArray[2])

		time.Sleep(time.Millisecond * 100)
		qm.mutex.Lock()
		assert.Equal(t, 1, len(qm.collID))
		assert.Equal(t, collMeta.ID, qm.collID[0])
		qm.mutex.Unlock()

		req = &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     231,
				Timestamp: 231,
				SourceID:  231,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		status, err = core.DropCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		time.Sleep(time.Second)
		collArray = pnm.GetCollArray()
		assert.Equal(t, 3, len(collArray))
		assert.Equal(t, collName, collArray[2])

		// check DD operation info
		flag, err := core.MetaTable.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.client.Load(DDOperationPrefix, 0)
		assert.Nil(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.Nil(t, err)
		assert.Equal(t, DropCollectionDDType, ddOp.Type)

		var ddReq = internalpb.DropCollectionRequest{}
		err = proto.UnmarshalText(ddOp.Body, &ddReq)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)
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
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx2, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     1001,
				Timestamp: 1001,
				SourceID:  1001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx2, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     1002,
				Timestamp: 1002,
				SourceID:  1002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx2, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     1003,
				Timestamp: 1003,
				SourceID:  1003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx2, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     1004,
				Timestamp: 1004,
				SourceID:  1004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx2, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     1005,
				Timestamp: 1005,
				SourceID:  1005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx2, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     1006,
				Timestamp: 1006,
				SourceID:  1006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx2, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     1007,
				Timestamp: 1007,
				SourceID:  1007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx2, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     1008,
				Timestamp: 1008,
				SourceID:  1008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx2, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     1009,
				Timestamp: 1009,
				SourceID:  1009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx2, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     1010,
				Timestamp: 1010,
				SourceID:  1010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx2, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     1011,
				Timestamp: 1011,
				SourceID:  1011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx2, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     1012,
				Timestamp: 1012,
				SourceID:  1012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx2, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     1013,
				Timestamp: 1013,
				SourceID:  1013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)

	})

	t.Run("undefined req type", func(t *testing.T) {
		st, err := core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2000,
				Timestamp: 2000,
				SourceID:  2000,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2001,
				Timestamp: 2001,
				SourceID:  2001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2002,
				Timestamp: 2002,
				SourceID:  2002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2003,
				Timestamp: 2003,
				SourceID:  2003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2004,
				Timestamp: 2004,
				SourceID:  2004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2005,
				Timestamp: 2005,
				SourceID:  2005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2006,
				Timestamp: 2006,
				SourceID:  2006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2007,
				Timestamp: 2007,
				SourceID:  2007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2008,
				Timestamp: 2008,
				SourceID:  2008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2009,
				Timestamp: 2009,
				SourceID:  2009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2010,
				Timestamp: 2010,
				SourceID:  2010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2011,
				Timestamp: 2011,
				SourceID:  2011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2012,
				Timestamp: 2012,
				SourceID:  2012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2013,
				Timestamp: 2013,
				SourceID:  2013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)

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
		_, err = core.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
	})

	t.Run("channel timetick", func(t *testing.T) {
		const (
			proxyNodeIDInvalid = 102
			proxyNodeName0     = "proxynode_0"
			proxyNodeName1     = "proxynode_1"
			chanName0          = "c0"
			chanName1          = "c1"
			chanName2          = "c2"
			ts0                = uint64(100)
			ts1                = uint64(120)
			ts2                = uint64(150)
		)
		p1 := sessionutil.Session{
			ServerID: 100,
		}

		p2 := sessionutil.Session{
			ServerID: 101,
		}
		ctx2, cancel2 := context.WithTimeout(ctx, RequestTimeout)
		defer cancel2()
		s1, err := json.Marshal(&p1)
		assert.Nil(t, err)
		s2, err := json.Marshal(&p2)
		assert.Nil(t, err)

		_, err = core.etcdCli.Put(ctx2, path.Join(sessKey, typeutil.ProxyNodeRole)+"-1", string(s1))
		assert.Nil(t, err)
		_, err = core.etcdCli.Put(ctx2, path.Join(sessKey, typeutil.ProxyNodeRole)+"-2", string(s2))
		assert.Nil(t, err)
		time.Sleep(time.Second)

		core.dmlChannels.AddProducerChannels("c0", "c1", "c2")

		msg0 := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: 100,
			},
			ChannelNames: []string{chanName0, chanName1},
			Timestamps:   []uint64{ts0, ts2},
		}
		s, _ := core.UpdateChannelTimeTick(ctx, msg0)
		assert.Equal(t, commonpb.ErrorCode_Success, s.ErrorCode)
		time.Sleep(100 * time.Millisecond)
		//t.Log(core.chanTimeTick.proxyTimeTick)

		msg1 := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: 101,
			},
			ChannelNames: []string{chanName1, chanName2},
			Timestamps:   []uint64{ts1, ts2},
		}
		s, _ = core.UpdateChannelTimeTick(ctx, msg1)
		assert.Equal(t, commonpb.ErrorCode_Success, s.ErrorCode)
		time.Sleep(100 * time.Millisecond)

		msgInvalid := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: proxyNodeIDInvalid,
			},
			ChannelNames: []string{"test"},
			Timestamps:   []uint64{0},
		}
		s, _ = core.UpdateChannelTimeTick(ctx, msgInvalid)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, s.ErrorCode)
		time.Sleep(1 * time.Second)

		// 2 proxy nodes, 1 master
		assert.Equal(t, 3, core.chanTimeTick.GetProxyNodeNum())

		// 3 proxy node channels, 2 master channels
		assert.Equal(t, 5, core.chanTimeTick.GetChanNum())
	})

	err = core.Stop()
	assert.Nil(t, err)
	st, err := core.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, st.Status.ErrorCode)
	assert.NotEqual(t, internalpb.StateCode_Healthy, st.State.StateCode)

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
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     4001,
				Timestamp: 4001,
				SourceID:  4001,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     4002,
				Timestamp: 4002,
				SourceID:  4002,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     4003,
				Timestamp: 4003,
				SourceID:  4003,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     4004,
				Timestamp: 4004,
				SourceID:  4004,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     4005,
				Timestamp: 4005,
				SourceID:  4005,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     4006,
				Timestamp: 4006,
				SourceID:  4006,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     4007,
				Timestamp: 4007,
				SourceID:  4007,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     4008,
				Timestamp: 4008,
				SourceID:  4008,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     4009,
				Timestamp: 4009,
				SourceID:  4009,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     4010,
				Timestamp: 4010,
				SourceID:  4010,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     4011,
				Timestamp: 4011,
				SourceID:  4011,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     4012,
				Timestamp: 4012,
				SourceID:  4012,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     4013,
				Timestamp: 4013,
				SourceID:  4013,
			},
		})
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)

	})

	t.Run("alloc_error", func(t *testing.T) {
		core.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			return 0, 0, fmt.Errorf("id allocator error test")
		}
		core.TSOAllocator = func(count uint32) (typeutil.Timestamp, error) {
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
		assert.NotEqual(t, commonpb.ErrorCode_Success, p1.Status.ErrorCode)

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
		assert.NotEqual(t, commonpb.ErrorCode_Success, p2.Status.ErrorCode)
	})
}

func TestMasterService2(t *testing.T) {
	const (
		dbName   = "testDb"
		collName = "testColl"
		partName = "testPartition"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := msgstream.NewPmsFactory()
	Params.Init()
	core, err := NewCore(ctx, msFactory)
	assert.Nil(t, err)
	randVal := rand.Int()

	Params.TimeTickChannel = fmt.Sprintf("master-time-tick-%d", randVal)
	Params.StatisticsChannel = fmt.Sprintf("master-statistics-%d", randVal)
	Params.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.MetaRootPath)
	Params.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.KvRootPath)
	Params.MsgChannelSubName = fmt.Sprintf("subname-%d", randVal)

	err = core.Register()
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

	core.NewProxyClient = func(*sessionutil.Session) (types.ProxyNode, error) {
		return nil, nil
	}

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

	dataServiceSegmentStream, _ := msFactory.NewMsgStream(ctx)
	dataServiceSegmentStream.AsProducer([]string{Params.DataServiceSegmentChannel})

	timeTickStream, _ := msFactory.NewMsgStream(ctx)
	timeTickStream.AsConsumer([]string{Params.TimeTickChannel}, Params.MsgChannelSubName)
	timeTickStream.Start()

	time.Sleep(time.Second)

	t.Run("time tick", func(t *testing.T) {
		ttmsg, ok := <-timeTickStream.Chan()
		assert.True(t, ok)
		assert.Equal(t, 1, len(ttmsg.Msgs))
		ttm, ok := (ttmsg.Msgs[0]).(*msgstream.TimeTickMsg)
		assert.True(t, ok)
		assert.Greater(t, ttm.Base.Timestamp, typeutil.Timestamp(0))
	})

	t.Run("create collection", func(t *testing.T) {
		schema := schemapb.CollectionSchema{
			Name: collName,
		}

		sbf, err := proto.Marshal(&schema)
		assert.Nil(t, err)

		req := &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collName,
			Schema:         sbf,
		}
		status, err := core.CreateCollection(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		pChan := core.MetaTable.ListCollectionPhysicalChannels()
		dmlStream, _ := msFactory.NewMsgStream(ctx)
		dmlStream.AsConsumer(pChan, Params.MsgChannelSubName)
		dmlStream.Start()

		msgs := getNotTtMsg(ctx, 2, dmlStream.Chan())
		assert.Equal(t, 2, len(msgs))

		m1, ok := (msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		m2, ok := (msgs[1]).(*msgstream.CreatePartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, m1.Base.Timestamp, m2.Base.Timestamp)
		t.Log("time tick", m1.Base.Timestamp)
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

	c.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		return 0, 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.IDAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.TSOAllocator = func(count uint32) (typeutil.Timestamp, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.TSOAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.etcdCli = &clientv3.Client{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.kvBase = &etcdkv.EtcdKV{}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.ddReqQueue = make(chan reqTask)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.SendDdCreateCollectionReq = func(context.Context, *internalpb.CreateCollectionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.SendDdDropCollectionReq = func(context.Context, *internalpb.DropCollectionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.SendDdCreatePartitionReq = func(context.Context, *internalpb.CreatePartitionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.SendDdDropPartitionReq = func(context.Context, *internalpb.DropPartitionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.CallGetBinlogFilePathsService = func(segID, fieldID typeutil.UniqueID) ([]string, error) {
		return []string{}, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.CallGetNumRowsService = func(segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.CallBuildIndexService = func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo) (typeutil.UniqueID, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.CallDropIndexService = func(ctx context.Context, indexID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.NewProxyClient = func(*sessionutil.Session) (types.ProxyNode, error) {
		return nil, nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.CallReleaseCollectionService = func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DataServiceSegmentChan = make(chan *msgstream.MsgPack)
	err = c.checkInit()
	assert.NotNil(t, err)

	c.DataNodeFlushedSegmentChan = make(chan *msgstream.MsgPack)
	err = c.checkInit()
	assert.Nil(t, err)
}
