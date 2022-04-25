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

package rootcoord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	TestDMLChannelNum        = 32
	returnError              = "ReturnError"
	returnUnsuccessfulStatus = "ReturnUnsuccessfulStatus"
)

var disabledIndexBuildID []int64

type ctxKey struct{}

type proxyMock struct {
	types.Proxy
	collArray []string
	mutex     sync.Mutex
}

func (p *proxyMock) Stop() error {
	return nil
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

func (p *proxyMock) ReleaseDQLMessageStream(ctx context.Context, request *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

type dataMock struct {
	types.DataCoord
	randVal int
	mu      sync.Mutex
	segs    []typeutil.UniqueID
}

func (d *dataMock) Init() error {
	return nil
}

func (d *dataMock) Start() error {
	return nil
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
				NumOfRows: Params.RootCoordCfg.MinSegmentSizeToEnableIndex,
				State:     commonpb.SegmentState_Flushed,
			},
		},
	}, nil
}

func (d *dataMock) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rsp := &datapb.GetFlushedSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	rsp.Segments = append(rsp.Segments, d.segs...)
	return rsp, nil
}

func (d *dataMock) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return &datapb.WatchChannelsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}}, nil
}

func (d *dataMock) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	if req.GetSegmentId() == 999 /* intended failure seg ID */ {
		return &datapb.SetSegmentStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}
	return &datapb.SetSegmentStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (d *dataMock) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return &datapb.ImportTaskResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (d *dataMock) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return &datapb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

type queryMock struct {
	types.QueryCoord
	collID []typeutil.UniqueID
	mutex  sync.Mutex
}

func (q *queryMock) Init() error {
	return nil
}

func (q *queryMock) Start() error {
	return nil
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

func (q *queryMock) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

type indexMock struct {
	types.IndexCoord
	fileArray  []string
	idxBuildID []int64
	idxID      []int64
	idxDropID  []int64
	mutex      sync.Mutex
}

func (idx *indexMock) Init() error {
	return nil
}

func (idx *indexMock) Start() error {
	return nil
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

func (idx *indexMock) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	v := ctx.Value(ctxKey{}).(string)
	if v == returnError {
		log.Debug("(testing) simulating injected error")
		return nil, fmt.Errorf("injected error")
	} else if v == returnUnsuccessfulStatus {
		log.Debug("(testing) simulating unsuccessful status")
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: 100,
				Reason:    "not so good",
			},
		}, nil
	}
	resp := &indexpb.GetIndexStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "all good",
		},
	}
	log.Debug(fmt.Sprint("(testing) getting index state for index build IDs:", req.IndexBuildIDs))
	log.Debug(fmt.Sprint("(testing) banned index build IDs:", disabledIndexBuildID))
	for _, id := range req.IndexBuildIDs {
		ban := false
		for _, disabled := range disabledIndexBuildID {
			if disabled == id {
				ban = true
				resp.States = append(resp.States, &indexpb.IndexInfo{
					State: commonpb.IndexState_InProgress,
				})
			}
		}
		if !ban {
			resp.States = append(resp.States, &indexpb.IndexInfo{
				State: commonpb.IndexState_Finished,
			})
		}
	}
	return resp, nil
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

func createCollectionInMeta(dbName, collName string, core *Core, shardsNum int32, modifyFunc func(*etcdpb.CollectionInfo)) error {
	schema := schemapb.CollectionSchema{
		Name: collName,
	}

	sbf, err := proto.Marshal(&schema)
	if err != nil {
		return err
	}

	t := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collName,
		Schema:         sbf,
		ShardsNum:      shardsNum,
	}

	err = proto.Unmarshal(t.Schema, &schema)
	if err != nil {
		return fmt.Errorf("unmarshal schema error= %w", err)
	}

	for idx, field := range schema.Fields {
		field.FieldID = int64(idx + StartOfUserFieldID)
	}
	rowIDField := &schemapb.FieldSchema{
		FieldID:      int64(RowIDField),
		Name:         RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	}
	timeStampField := &schemapb.FieldSchema{
		FieldID:      int64(TimeStampField),
		Name:         TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	}
	schema.Fields = append(schema.Fields, rowIDField, timeStampField)

	collID, _, err := core.IDAllocator(1)
	if err != nil {
		return fmt.Errorf("alloc collection id error = %w", err)
	}
	partID, _, err := core.IDAllocator(1)
	if err != nil {
		return fmt.Errorf("alloc partition id error = %w", err)
	}

	vchanNames := make([]string, t.ShardsNum)
	chanNames := make([]string, t.ShardsNum)
	for i := int32(0); i < t.ShardsNum; i++ {
		vchanNames[i] = fmt.Sprintf("%s_%dv%d", core.chanTimeTick.getDmlChannelName(), collID, i)
		chanNames[i] = funcutil.ToPhysicalChannel(vchanNames[i])
	}

	collInfo := etcdpb.CollectionInfo{
		ID:                         collID,
		Schema:                     &schema,
		PartitionIDs:               []typeutil.UniqueID{partID},
		PartitionNames:             []string{Params.CommonCfg.DefaultPartitionName},
		FieldIndexes:               make([]*etcdpb.FieldIndexInfo, 0, 16),
		VirtualChannelNames:        vchanNames,
		PhysicalChannelNames:       chanNames,
		ShardsNum:                  0, // intend to set zero
		PartitionCreatedTimestamps: []uint64{0},
	}

	if modifyFunc != nil {
		modifyFunc(&collInfo)
	}

	idxInfo := make([]*etcdpb.IndexInfo, 0, 16)

	// schema is modified (add RowIDField and TimestampField),
	// so need Marshal again
	schemaBytes, err := proto.Marshal(&schema)
	if err != nil {
		return fmt.Errorf("marshal schema error = %w", err)
	}

	ddCollReq := internalpb.CreateCollectionRequest{
		Base:                 t.Base,
		DbName:               t.DbName,
		CollectionName:       t.CollectionName,
		PartitionName:        Params.CommonCfg.DefaultPartitionName,
		DbID:                 0, //TODO,not used
		CollectionID:         collID,
		PartitionID:          partID,
		Schema:               schemaBytes,
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
	}

	reason := fmt.Sprintf("create collection %d", collID)
	ts, err := core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("tso alloc fail, error = %w", err)
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddCollReq.Base.Timestamp = ts
	ddOpStr, err := EncodeDdOperation(&ddCollReq, CreateCollectionDDType)
	if err != nil {
		return fmt.Errorf("encodeDdOperation fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	createCollectionFn := func() error {
		// lock for ddl operation
		core.ddlLock.Lock()
		defer core.ddlLock.Unlock()

		core.chanTimeTick.addDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer core.chanTimeTick.removeDdlTimeTick(ts, reason)

		err = core.MetaTable.AddCollection(&collInfo, ts, idxInfo, ddOpStr)
		if err != nil {
			return fmt.Errorf("meta table add collection failed,error = %w", err)
		}
		return nil
	}

	err = createCollectionFn()
	if err != nil {
		return err
	}
	return nil
}

// a mock kv that always fail when LoadWithPrefix
type loadPrefixFailKV struct {
	kv.TxnKV
}

// LoadWithPrefix override behavior
func (kv *loadPrefixFailKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return []string{}, []string{}, retry.Unrecoverable(errors.New("mocked fail"))
}

func TestRootCoordInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreFactory := dependency.NewDefaultFactory(true)
	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	core, err := NewCore(ctx, coreFactory)
	require.Nil(t, err)
	assert.NoError(t, err)
	core.SetEtcdClient(etcdCli)
	randVal := rand.Int()

	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	err = core.Init()
	assert.NoError(t, err)
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	// inject kvBaseCreate fail
	core, err = NewCore(ctx, coreFactory)
	core.SetEtcdClient(etcdCli)
	require.Nil(t, err)
	assert.NoError(t, err)
	randVal = rand.Int()

	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	core.kvBaseCreate = func(string) (kv.TxnKV, error) {
		return nil, retry.Unrecoverable(errors.New("injected"))
	}
	core.metaKVCreate = func(root string) (kv.MetaKv, error) {
		return nil, retry.Unrecoverable(errors.New("injected"))
	}
	err = core.Init()
	assert.Error(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	// inject metaKV create fail
	core, err = NewCore(ctx, coreFactory)
	core.SetEtcdClient(etcdCli)
	require.Nil(t, err)
	assert.NoError(t, err)
	randVal = rand.Int()

	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	core.kvBaseCreate = func(root string) (kv.TxnKV, error) {
		if root == Params.EtcdCfg.MetaRootPath {
			return nil, retry.Unrecoverable(errors.New("injected"))
		}
		return memkv.NewMemoryKV(), nil
	}
	core.metaKVCreate = func(root string) (kv.MetaKv, error) {
		return nil, nil
	}
	err = core.Init()
	assert.Error(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	// inject newSuffixSnapshot failure
	core, err = NewCore(ctx, coreFactory)
	core.SetEtcdClient(etcdCli)
	require.Nil(t, err)
	assert.NoError(t, err)
	randVal = rand.Int()

	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	core.kvBaseCreate = func(string) (kv.TxnKV, error) {
		return nil, nil
	}
	core.metaKVCreate = func(root string) (kv.MetaKv, error) {
		return nil, nil
	}
	err = core.Init()
	assert.Error(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	// inject newMetaTable failure
	core, err = NewCore(ctx, coreFactory)
	core.SetEtcdClient(etcdCli)
	require.Nil(t, err)
	assert.NoError(t, err)
	randVal = rand.Int()

	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	core.kvBaseCreate = func(string) (kv.TxnKV, error) {
		kv := memkv.NewMemoryKV()
		return &loadPrefixFailKV{TxnKV: kv}, nil
	}
	core.metaKVCreate = func(root string) (kv.MetaKv, error) {
		return nil, nil
	}
	err = core.Init()
	assert.Error(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
}

func TestRootCoordInitData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreFactory := dependency.NewDefaultFactory(true)
	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	core, err := NewCore(ctx, coreFactory)
	assert.NoError(t, err)
	core.SetEtcdClient(etcdCli)

	randVal := rand.Int()
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)

	// 1. normal init
	err = core.Init()
	assert.NoError(t, err)

	// 2. mock init data error
	// firstly delete data
	err = core.MetaTable.DeleteCredential(util.UserRoot)
	assert.NoError(t, err)

	snapshotKV, err := newMetaSnapshot(etcdCli, Params.EtcdCfg.MetaRootPath, TimestampPrefix, 7)
	assert.NotNil(t, snapshotKV)
	assert.NoError(t, err)
	txnKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	mt, err := NewMetaTable(txnKV, snapshotKV)
	assert.NoError(t, err)
	mockTxnKV := &mockTestTxnKV{
		TxnKV:  mt.txn,
		save:   func(key, value string) error { return txnKV.Save(key, value) },
		remove: func(key string) error { return txnKV.Remove(key) },
	}
	mt.txn = mockTxnKV
	// mock save data error
	mockTxnKV.save = func(key, value string) error {
		return fmt.Errorf("save error")
	}
	core.MetaTable = mt
	err = core.initData()
	assert.Error(t, err)
}

func TestRootCoord_Base(t *testing.T) {
	const (
		dbName    = "testDb"
		collName  = "testColl"
		collName2 = "testColl2"
		aliasName = "alias1"
		partName  = "testPartition"
		segID     = 1001
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreFactory := dependency.NewDefaultFactory(true)
	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum
	Params.RootCoordCfg.ImportIndexCheckInterval = 0.1
	Params.RootCoordCfg.ImportIndexWaitLimit = 0.2
	core, err := NewCore(ctx, coreFactory)
	assert.NoError(t, err)
	randVal := rand.Int()
	Params.CommonCfg.RootCoordTimeTick = fmt.Sprintf("rootcoord-time-tick-%d", randVal)
	Params.CommonCfg.RootCoordStatistics = fmt.Sprintf("rootcoord-statistics-%d", randVal)
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)
	Params.CommonCfg.RootCoordSubName = fmt.Sprintf("subname-%d", randVal)
	Params.CommonCfg.RootCoordDml = fmt.Sprintf("rootcoord-dml-test-%d", randVal)
	Params.CommonCfg.RootCoordDelta = fmt.Sprintf("rootcoord-delta-test-%d", randVal)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	sessKey := path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	assert.NoError(t, err)
	defer func() {
		_, _ = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	}()

	pnb, err := json.Marshal(
		&sessionutil.Session{
			ServerID: 100,
		},
	)
	assert.NoError(t, err)
	_, err = etcdCli.Put(ctx, path.Join(sessKey, typeutil.ProxyRole+"-100"), string(pnb))
	assert.NoError(t, err)

	pnm := &proxyMock{
		collArray: make([]string, 0, 16),
		mutex:     sync.Mutex{},
	}
	core.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return pnm, nil
	}

	dm := &dataMock{randVal: randVal}
	err = core.SetDataCoord(ctx, dm)
	assert.NoError(t, err)

	im := &indexMock{
		fileArray:  []string{},
		idxBuildID: []int64{},
		idxID:      []int64{},
		idxDropID:  []int64{},
		mutex:      sync.Mutex{},
	}
	err = core.SetIndexCoord(im)
	assert.NoError(t, err)

	qm := &queryMock{
		collID: nil,
		mutex:  sync.Mutex{},
	}
	err = core.SetQueryCoord(qm)
	assert.NoError(t, err)

	tmpFactory := dependency.NewDefaultFactory(true)

	dmlStream, _ := tmpFactory.NewMsgStream(ctx)
	defer dmlStream.Close()

	core.SetEtcdClient(etcdCli)

	err = core.Init()
	assert.NoError(t, err)

	var localTSO uint64
	localTSOLock := sync.RWMutex{}
	core.TSOAllocator = func(c uint32) (uint64, error) {
		localTSOLock.Lock()
		defer localTSOLock.Unlock()
		localTSO += uint64(c)
		return localTSO, nil
	}

	err = core.Start()
	assert.NoError(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	shardsNum := int32(8)

	var wg sync.WaitGroup

	wg.Add(1)
	t.Run("create collection", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
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
			ShardsNum:      shardsNum,
		}
		status, err := core.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		assert.Equal(t, shardsNum, int32(core.chanTimeTick.getDmlChannelNum()))

		createMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		dmlStream.AsConsumer([]string{createMeta.PhysicalChannelNames[0]}, Params.CommonCfg.RootCoordSubName)
		dmlStream.Start()

		pChanMap := core.MetaTable.ListCollectionPhysicalChannels()
		assert.Greater(t, len(pChanMap[createMeta.ID]), 0)
		vChanMap := core.MetaTable.ListCollectionVirtualChannels()
		assert.Greater(t, len(vChanMap[createMeta.ID]), 0)

		// get CreateCollectionMsg
		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		createMsg, ok := (msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		assert.Equal(t, createMeta.ID, createMsg.CollectionID)
		assert.Equal(t, 1, len(createMeta.PartitionIDs))
		assert.Equal(t, createMeta.PartitionIDs[0], createMsg.PartitionID)
		assert.Equal(t, 1, len(createMeta.PartitionNames))
		assert.Equal(t, createMeta.PartitionNames[0], createMsg.PartitionName)
		assert.Equal(t, shardsNum, int32(len(createMeta.VirtualChannelNames)))
		assert.Equal(t, shardsNum, int32(len(createMeta.PhysicalChannelNames)))
		assert.Equal(t, shardsNum, createMeta.ShardsNum)

		vChanName := createMeta.VirtualChannelNames[0]
		assert.Equal(t, createMeta.PhysicalChannelNames[0], funcutil.ToPhysicalChannel(vChanName))

		// get TimeTickMsg
		//msgPack, ok = <-dmlStream.Chan()
		//assert.True(t, ok)
		//assert.Equal(t, 1, len(msgPack.Msgs))
		//ddm, ok := (msgPack.Msgs[0]).(*msgstream.TimeTickMsg)
		//assert.True(t, ok)
		//assert.Greater(t, ddm.Base.Timestamp, uint64(0))
		core.chanTimeTick.lock.Lock()
		assert.Equal(t, len(core.chanTimeTick.sess2ChanTsMap), 2)
		pt, ok := core.chanTimeTick.sess2ChanTsMap[core.session.ServerID]
		assert.True(t, ok)
		assert.Equal(t, shardsNum, int32(len(pt.chanTsMap)))
		for chanName, ts := range pt.chanTsMap {
			assert.Contains(t, createMeta.PhysicalChannelNames, chanName)
			assert.Equal(t, pt.defaultTs, ts)
		}
		core.chanTimeTick.lock.Unlock()

		// check DD operation info
		flag, err := core.MetaTable.txn.Load(DDMsgSendPrefix)
		assert.NoError(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.txn.Load(DDOperationPrefix)
		assert.NoError(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.NoError(t, err)
		assert.Equal(t, CreateCollectionDDType, ddOp.Type)

		var ddCollReq = internalpb.CreateCollectionRequest{}
		err = proto.Unmarshal(ddOp.Body, &ddCollReq)
		assert.NoError(t, err)
		assert.Equal(t, createMeta.ID, ddCollReq.CollectionID)
		assert.Equal(t, createMeta.PartitionIDs[0], ddCollReq.PartitionID)

		// check invalid operation
		req.Base.MsgID = 101
		req.Base.Timestamp = 101
		req.Base.SourceID = 101
		status, err = core.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		req.Base.MsgID = 102
		req.Base.Timestamp = 102
		req.Base.SourceID = 102
		req.CollectionName = "testColl-again"
		status, err = core.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		schema.Name = req.CollectionName
		sbf, err = proto.Marshal(&schema)
		assert.NoError(t, err)
		req.Schema = sbf
		req.Base.MsgID = 103
		req.Base.Timestamp = 103
		req.Base.SourceID = 103
		status, err = core.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		err = core.reSendDdMsg(core.ctx, true)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("has collection", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, true, rsp.Value)
	})

	wg.Add(1)
	t.Run("describe collection", func(t *testing.T) {
		defer wg.Done()
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, collName, rsp.Schema.Name)
		assert.Equal(t, collMeta.ID, rsp.CollectionID)
		assert.Equal(t, shardsNum, int32(len(rsp.VirtualChannelNames)))
		assert.Equal(t, shardsNum, int32(len(rsp.PhysicalChannelNames)))
		assert.Equal(t, shardsNum, rsp.ShardsNum)
	})

	wg.Add(1)
	t.Run("show collection", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.ElementsMatch(t, rsp.CollectionNames, []string{collName, "testColl-again"})
		assert.Equal(t, len(rsp.CollectionNames), 2)
	})

	wg.Add(1)
	t.Run("create partition", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		t.Log(status.Reason)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(collMeta.PartitionIDs))
		partNameIdx1, err := core.MetaTable.GetPartitionNameByID(collMeta.ID, collMeta.PartitionIDs[1], 0)
		assert.NoError(t, err)
		assert.Equal(t, partName, partNameIdx1)

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		partMsg, ok := (msgs[0]).(*msgstream.CreatePartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, partMsg.CollectionID)
		assert.Equal(t, collMeta.PartitionIDs[1], partMsg.PartitionID)

		assert.Equal(t, 1, len(pnm.GetCollArray()))
		assert.Equal(t, collName, pnm.GetCollArray()[0])

		// check DD operation info
		flag, err := core.MetaTable.txn.Load(DDMsgSendPrefix)
		assert.NoError(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.txn.Load(DDOperationPrefix)
		assert.NoError(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.NoError(t, err)
		assert.Equal(t, CreatePartitionDDType, ddOp.Type)

		var ddReq = internalpb.CreatePartitionRequest{}
		err = proto.Unmarshal(ddOp.Body, &ddReq)
		assert.NoError(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)
		assert.Equal(t, collMeta.PartitionIDs[1], ddReq.PartitionID)

		err = core.reSendDdMsg(core.ctx, true)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("has partition", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, true, rsp.Value)
	})

	wg.Add(1)
	t.Run("show partition", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 2, len(rsp.PartitionNames))
		assert.Equal(t, 2, len(rsp.PartitionIDs))
	})

	wg.Add(1)
	t.Run("show segment", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		partID := coll.PartitionIDs[1]
		dm.mu.Lock()
		dm.segs = []typeutil.UniqueID{1000, 1001, 1002, 1003, 1004, 1005}
		dm.mu.Unlock()

		req := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     170,
				Timestamp: 170,
				SourceID:  170,
			},
			CollectionID: coll.GetID(),
			PartitionID:  partID,
		}
		rsp, err := core.ShowSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, int64(1000), rsp.SegmentIDs[0])
		assert.Equal(t, int64(1001), rsp.SegmentIDs[1])
		assert.Equal(t, int64(1002), rsp.SegmentIDs[2])
		assert.Equal(t, int64(1003), rsp.SegmentIDs[3])
		assert.Equal(t, int64(1004), rsp.SegmentIDs[4])
		assert.Equal(t, int64(1005), rsp.SegmentIDs[5])
		assert.Equal(t, 6, len(rsp.SegmentIDs))
	})

	wg.Add(1)
	t.Run("create index", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, 0, len(collMeta.FieldIndexes))

		rsp, err := core.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
		time.Sleep(100 * time.Millisecond)
		files := im.getFileArray()
		assert.Equal(t, 6*3, len(files))
		assert.ElementsMatch(t, files,
			[]string{"file0-100", "file1-100", "file2-100",
				"file0-100", "file1-100", "file2-100",
				"file0-100", "file1-100", "file2-100",
				"file0-100", "file1-100", "file2-100",
				"file0-100", "file1-100", "file2-100",
				"file0-100", "file1-100", "file2-100"})
		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(collMeta.FieldIndexes))
		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.NoError(t, err)
		assert.Equal(t, Params.CommonCfg.DefaultIndexName, idxMeta.IndexName)

		req.FieldName = "no field"
		rsp, err = core.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	wg.Add(1)
	t.Run("describe segment", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)

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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		t.Logf("index id = %d", rsp.IndexID)
	})

	wg.Add(1)
	t.Run("describe index", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 1, len(rsp.IndexDescriptions))
		assert.Equal(t, Params.CommonCfg.DefaultIndexName, rsp.IndexDescriptions[0].IndexName)
		assert.Equal(t, "vector", rsp.IndexDescriptions[0].FieldName)
	})

	wg.Add(1)
	t.Run("describe index not exist", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, rsp.Status.ErrorCode)
		assert.Equal(t, 0, len(rsp.IndexDescriptions))
	})

	wg.Add(1)
	t.Run("count complete index", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		// Normal case.
		count, err := core.CountCompleteIndex(context.WithValue(ctx, ctxKey{}, ""),
			collName, coll.ID, []UniqueID{1000, 1001, 1002})
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		// Case with an empty result.
		count, err = core.CountCompleteIndex(ctx, collName, coll.ID, []UniqueID{})
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		// Case where GetIndexStates failed with error.
		_, err = core.CountCompleteIndex(context.WithValue(ctx, ctxKey{}, returnError),
			collName, coll.ID, []UniqueID{1000, 1001, 1002})
		assert.Error(t, err)
		// Case where GetIndexStates failed with bad status.
		_, err = core.CountCompleteIndex(context.WithValue(ctx, ctxKey{}, returnUnsuccessfulStatus),
			collName, coll.ID, []UniqueID{1000, 1001, 1002})
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("flush segment", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		partID := coll.PartitionIDs[1]

		flushMsg := datapb.SegmentFlushCompletedMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentFlushDone,
			},
			Segment: &datapb.SegmentInfo{
				ID:           segID,
				CollectionID: coll.ID,
				PartitionID:  partID,
			},
		}
		st, err := core.SegmentFlushCompleted(ctx, &flushMsg)
		assert.NoError(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_Success)

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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 1, len(rsp.IndexDescriptions))
		assert.Equal(t, Params.CommonCfg.DefaultIndexName, rsp.IndexDescriptions[0].IndexName)
	})

	wg.Add(1)
	t.Run("import", func(t *testing.T) {
		defer wg.Done()
		tID := typeutil.UniqueID(0)
		core.importManager.idAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			tID++
			return tID, 0, nil
		}
		req := &milvuspb.ImportRequest{
			CollectionName: collName,
			PartitionName:  partName,
			RowBased:       true,
			Files:          []string{"f1", "f2", "f3"},
		}
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		core.MetaTable.collName2ID[collName] = coll.GetID()
		rsp, err := core.Import(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("import w/ collection ID not found", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ImportRequest{
			CollectionName: "bad name",
			PartitionName:  partName,
			RowBased:       true,
			Files:          []string{"f1", "f2", "f3"},
		}
		_, err := core.Import(ctx, req)
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("get import state", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.GetImportStateRequest{
			Task: 1,
		}
		rsp, err := core.GetImportState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("list import stasks", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ListImportTasksRequest{}
		rsp, err := core.ListImportTasks(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("report import task timeout", func(t *testing.T) {
		defer wg.Done()
		coll, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		req := &rootcoordpb.ImportResult{
			TaskId:   1,
			RowCount: 100,
			Segments: []int64{1003, 1004, 1005},
			State:    commonpb.ImportState_ImportCompleted,
		}

		for _, segmentID := range []int64{1003, 1004, 1005} {
			describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeSegment,
				},
				CollectionID: coll.ID,
				SegmentID:    segmentID,
			}
			segDesc, err := core.DescribeSegment(ctx, describeSegmentRequest)
			assert.NoError(t, err)
			disabledIndexBuildID = append(disabledIndexBuildID, segDesc.BuildID)
		}

		rsp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
		time.Sleep(500 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("report import update import task fail", func(t *testing.T) {
		defer wg.Done()
		// Case where report import request is nil.
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UpdateImportTaskFailure, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("report import collection name not found", func(t *testing.T) {
		defer wg.Done()
		var tID = typeutil.UniqueID(100)
		core.importManager.idAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			tID++
			return tID, 0, nil
		}
		core.MetaTable.collName2ID["new"+collName] = 123
		core.MetaTable.collID2Meta[123] = etcdpb.CollectionInfo{
			ID:             123,
			PartitionIDs:   []int64{456},
			PartitionNames: []string{"testPartition"}}
		req := &milvuspb.ImportRequest{
			CollectionName: "new" + collName,
			PartitionName:  partName,
			RowBased:       true,
			Files:          []string{"f1", "f2", "f3"},
		}
		rsp, err := core.Import(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		delete(core.MetaTable.collName2ID, "new"+collName)
		delete(core.MetaTable.collID2Meta, 123)

		reqIR := &rootcoordpb.ImportResult{
			TaskId:   101,
			RowCount: 100,
			Segments: []int64{1003, 1004, 1005},
			State:    commonpb.ImportState_ImportCompleted,
		}
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), reqIR)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_CollectionNameNotFound, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("report import with transitional state", func(t *testing.T) {
		defer wg.Done()
		req := &rootcoordpb.ImportResult{
			TaskId:   1,
			RowCount: 100,
			Segments: []int64{1000, 1001, 1002},
			State:    commonpb.ImportState_ImportDownloaded,
		}
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		time.Sleep(500 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("report import bring segments online", func(t *testing.T) {
		defer wg.Done()
		req := &rootcoordpb.ImportResult{
			TaskId:   1,
			RowCount: 100,
			Segments: []int64{1000, 1001, 1002},
			State:    commonpb.ImportState_ImportCompleted,
		}
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		time.Sleep(500 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("report import bring segments online with set segment state fail", func(t *testing.T) {
		defer wg.Done()
		req := &rootcoordpb.ImportResult{
			TaskId:   1,
			RowCount: 100,
			Segments: []int64{999}, /* pre-injected failure for segment ID = 999 */
			State:    commonpb.ImportState_ImportCompleted,
		}
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("report import segments update already failed task", func(t *testing.T) {
		defer wg.Done()
		// Mark task 0 as failed.
		core.importManager.updateTaskState(
			&rootcoordpb.ImportResult{
				TaskId:   1,
				RowCount: 100,
				State:    commonpb.ImportState_ImportFailed,
				Segments: []int64{1000, 1001, 1002},
			})
		// Now try to update this task with a complete status.
		resp, err := core.ReportImport(context.WithValue(ctx, ctxKey{}, ""),
			&rootcoordpb.ImportResult{
				TaskId:   1,
				RowCount: 100,
				State:    commonpb.ImportState_ImportCompleted,
				Segments: []int64{1000, 1001, 1002},
			})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UpdateImportTaskFailure, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("over ride index", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		assert.Equal(t, 1, len(collMeta.FieldIndexes))
		oldIdx := collMeta.FieldIndexes[0].IndexID

		rsp, err := core.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
		time.Sleep(100 * time.Millisecond)

		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(collMeta.FieldIndexes))
		assert.Equal(t, oldIdx, collMeta.FieldIndexes[0].IndexID)

		idxMeta, err := core.MetaTable.GetIndexByID(collMeta.FieldIndexes[1].IndexID)
		assert.NoError(t, err)
		assert.Equal(t, Params.CommonCfg.DefaultIndexName, idxMeta.IndexName)

		idxMeta, err = core.MetaTable.GetIndexByID(collMeta.FieldIndexes[0].IndexID)
		assert.NoError(t, err)
		assert.Equal(t, Params.CommonCfg.DefaultIndexName+"_bak", idxMeta.IndexName)

	})

	wg.Add(1)
	t.Run("drop index", func(t *testing.T) {
		defer wg.Done()
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
			IndexName:      Params.CommonCfg.DefaultIndexName,
		}
		_, idx, err := core.MetaTable.GetIndexByName(collName, Params.CommonCfg.DefaultIndexName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(idx))

		rsp, err := core.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)

		im.mutex.Lock()
		assert.Equal(t, 1, len(im.idxDropID))
		assert.Equal(t, idx[0].IndexID, im.idxDropID[0])
		im.mutex.Unlock()

		_, idx, err = core.MetaTable.GetIndexByName(collName, Params.CommonCfg.DefaultIndexName)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(idx))
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		dropPartID := collMeta.PartitionIDs[1]
		status, err := core.DropPartition(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		collMeta, err = core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(collMeta.PartitionIDs))
		partName, err := core.MetaTable.GetPartitionNameByID(collMeta.ID, collMeta.PartitionIDs[0], 0)
		assert.NoError(t, err)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, partName)

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		dmsg, ok := (msgs[0]).(*msgstream.DropPartitionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, dmsg.CollectionID)
		assert.Equal(t, dropPartID, dmsg.PartitionID)

		assert.Equal(t, 2, len(pnm.GetCollArray()))
		assert.Equal(t, collName, pnm.GetCollArray()[1])

		// check DD operation info
		flag, err := core.MetaTable.txn.Load(DDMsgSendPrefix)
		assert.NoError(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.txn.Load(DDOperationPrefix)
		assert.NoError(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.NoError(t, err)
		assert.Equal(t, DropPartitionDDType, ddOp.Type)

		var ddReq = internalpb.DropPartitionRequest{}
		err = proto.Unmarshal(ddOp.Body, &ddReq)
		assert.NoError(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)
		assert.Equal(t, dropPartID, ddReq.PartitionID)

		err = core.reSendDdMsg(core.ctx, true)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("remove DQL msgstream", func(t *testing.T) {
		defer wg.Done()
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)

		req := &proxypb.ReleaseDQLMessageStreamRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_RemoveQueryChannels,
				SourceID: core.session.ServerID,
			},
			CollectionID: collMeta.ID,
		}
		status, err := core.ReleaseDQLMessageStream(core.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	wg.Add(1)
	t.Run("drop collection", func(t *testing.T) {
		defer wg.Done()
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
		assert.NoError(t, err)
		status, err := core.DropCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		vChanName := collMeta.VirtualChannelNames[0]
		assert.Equal(t, collMeta.PhysicalChannelNames[0], funcutil.ToPhysicalChannel(vChanName))

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))
		dmsg, ok := (msgs[0]).(*msgstream.DropCollectionMsg)
		assert.True(t, ok)
		assert.Equal(t, collMeta.ID, dmsg.CollectionID)
		collArray := pnm.GetCollArray()
		assert.Equal(t, 3, len(collArray))
		assert.Equal(t, collName, collArray[2])

		time.Sleep(100 * time.Millisecond)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		time.Sleep(100 * time.Millisecond)
		collArray = pnm.GetCollArray()
		assert.Equal(t, 3, len(collArray))
		assert.Equal(t, collName, collArray[2])

		// check DD operation info
		flag, err := core.MetaTable.txn.Load(DDMsgSendPrefix)
		assert.NoError(t, err)
		assert.Equal(t, "true", flag)
		ddOpStr, err := core.MetaTable.txn.Load(DDOperationPrefix)
		assert.NoError(t, err)
		var ddOp DdOperation
		err = DecodeDdOperation(ddOpStr, &ddOp)
		assert.NoError(t, err)
		assert.Equal(t, DropCollectionDDType, ddOp.Type)

		var ddReq = internalpb.DropCollectionRequest{}
		err = proto.Unmarshal(ddOp.Body, &ddReq)
		assert.NoError(t, err)
		assert.Equal(t, collMeta.ID, ddReq.CollectionID)

		err = core.reSendDdMsg(core.ctx, true)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("context_cancel", func(t *testing.T) {
		defer wg.Done()
		ctx2, cancel2 := context.WithTimeout(ctx, time.Millisecond*100)
		defer cancel2()
		time.Sleep(100 * time.Millisecond)
		st, err := core.CreateCollection(ctx2, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     1000,
				Timestamp: 1000,
				SourceID:  1000,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx2, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     1001,
				Timestamp: 1001,
				SourceID:  1001,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx2, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     1002,
				Timestamp: 1002,
				SourceID:  1002,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx2, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     1003,
				Timestamp: 1003,
				SourceID:  1003,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx2, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     1004,
				Timestamp: 1004,
				SourceID:  1004,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx2, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     1005,
				Timestamp: 1005,
				SourceID:  1005,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx2, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     1006,
				Timestamp: 1006,
				SourceID:  1006,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx2, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     1007,
				Timestamp: 1007,
				SourceID:  1007,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx2, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     1008,
				Timestamp: 1008,
				SourceID:  1008,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx2, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     1009,
				Timestamp: 1009,
				SourceID:  1009,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx2, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     1010,
				Timestamp: 1010,
				SourceID:  1010,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx2, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     1011,
				Timestamp: 1011,
				SourceID:  1011,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx2, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     1012,
				Timestamp: 1012,
				SourceID:  1012,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx2, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     1013,
				Timestamp: 1013,
				SourceID:  1013,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)
		time.Sleep(1 * time.Second)
	})

	wg.Add(1)
	t.Run("undefined req type", func(t *testing.T) {
		defer wg.Done()
		st, err := core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2000,
				Timestamp: 2000,
				SourceID:  2000,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2001,
				Timestamp: 2001,
				SourceID:  2001,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2002,
				Timestamp: 2002,
				SourceID:  2002,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2003,
				Timestamp: 2003,
				SourceID:  2003,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2004,
				Timestamp: 2004,
				SourceID:  2004,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2005,
				Timestamp: 2005,
				SourceID:  2005,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2006,
				Timestamp: 2006,
				SourceID:  2006,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2007,
				Timestamp: 2007,
				SourceID:  2007,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2008,
				Timestamp: 2008,
				SourceID:  2008,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2009,
				Timestamp: 2009,
				SourceID:  2009,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2010,
				Timestamp: 2010,
				SourceID:  2010,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2011,
				Timestamp: 2011,
				SourceID:  2011,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2012,
				Timestamp: 2012,
				SourceID:  2012,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     2013,
				Timestamp: 2013,
				SourceID:  2013,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)

	})

	wg.Add(1)
	t.Run("alloc time tick", func(t *testing.T) {
		defer wg.Done()
		req := &rootcoordpb.AllocTimestampRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3000,
				Timestamp: 3000,
				SourceID:  3000,
			},
			Count: 1,
		}
		rsp, err := core.AllocTimestamp(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), rsp.Count)
		assert.NotZero(t, rsp.Timestamp)
	})

	wg.Add(1)
	t.Run("alloc id", func(t *testing.T) {
		defer wg.Done()
		req := &rootcoordpb.AllocIDRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3001,
				Timestamp: 3001,
				SourceID:  3001,
			},
			Count: 1,
		}
		rsp, err := core.AllocID(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), rsp.Count)
		assert.NotZero(t, rsp.ID)
	})

	wg.Add(1)
	t.Run("get_channels", func(t *testing.T) {
		defer wg.Done()
		_, err := core.GetTimeTickChannel(ctx)
		assert.NoError(t, err)
		_, err = core.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("channel timetick", func(t *testing.T) {
		defer wg.Done()
		const (
			proxyIDInvalid = 102
			ts0            = uint64(20)
			ts1            = uint64(40)
			ts2            = uint64(60)
		)
		numChan := core.chanTimeTick.getDmlChannelNum()
		p1 := sessionutil.Session{
			ServerID: 100,
		}
		p2 := sessionutil.Session{
			ServerID: 101,
		}
		ctx2, cancel2 := context.WithTimeout(ctx, RequestTimeout)
		defer cancel2()
		s1, err := json.Marshal(&p1)
		assert.NoError(t, err)
		s2, err := json.Marshal(&p2)
		assert.NoError(t, err)

		proxy1 := path.Join(sessKey, typeutil.ProxyRole) + "-1"
		proxy2 := path.Join(sessKey, typeutil.ProxyRole) + "-2"
		_, err = core.etcdCli.Put(ctx2, proxy1, string(s1))
		assert.NoError(t, err)
		_, err = core.etcdCli.Put(ctx2, proxy2, string(s2))
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		cn0 := core.chanTimeTick.getDmlChannelName()
		cn1 := core.chanTimeTick.getDmlChannelName()
		cn2 := core.chanTimeTick.getDmlChannelName()
		core.chanTimeTick.addDmlChannels(cn0, cn1, cn2)

		dn0 := core.chanTimeTick.getDeltaChannelName()
		dn1 := core.chanTimeTick.getDeltaChannelName()
		dn2 := core.chanTimeTick.getDeltaChannelName()
		core.chanTimeTick.addDeltaChannels(dn0, dn1, dn2)

		// wait for local channel reported
		for {
			core.chanTimeTick.lock.Lock()
			_, ok := core.chanTimeTick.sess2ChanTsMap[core.session.ServerID].chanTsMap[cn0]

			if !ok {
				core.chanTimeTick.lock.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			_, ok = core.chanTimeTick.sess2ChanTsMap[core.session.ServerID].chanTsMap[cn1]

			if !ok {
				core.chanTimeTick.lock.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			_, ok = core.chanTimeTick.sess2ChanTsMap[core.session.ServerID].chanTsMap[cn2]

			if !ok {
				core.chanTimeTick.lock.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			core.chanTimeTick.lock.Unlock()
			break
		}
		msg0 := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: 100,
			},
			ChannelNames: []string{cn0, cn1},
			Timestamps:   []uint64{ts0, ts2},
		}
		s, _ := core.UpdateChannelTimeTick(ctx, msg0)
		assert.Equal(t, commonpb.ErrorCode_Success, s.ErrorCode)
		time.Sleep(100 * time.Millisecond)
		//t.Log(core.chanTimeTick.sess2ChanTsMap)

		msg1 := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: 101,
			},
			ChannelNames: []string{cn1, cn2},
			Timestamps:   []uint64{ts1, ts2},
		}
		s, _ = core.UpdateChannelTimeTick(ctx, msg1)
		assert.Equal(t, commonpb.ErrorCode_Success, s.ErrorCode)
		time.Sleep(100 * time.Millisecond)

		msgInvalid := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: proxyIDInvalid,
			},
			ChannelNames: []string{"test"},
			Timestamps:   []uint64{0},
		}
		s, _ = core.UpdateChannelTimeTick(ctx, msgInvalid)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, s.ErrorCode)
		time.Sleep(100 * time.Millisecond)

		// 2 proxy, 1 rootcoord
		assert.Equal(t, 3, core.chanTimeTick.getSessionNum())

		// add 3 proxy channels
		assert.Equal(t, 3, core.chanTimeTick.getDmlChannelNum()-numChan)

		_, err = core.etcdCli.Delete(ctx2, proxy1)
		assert.NoError(t, err)
		_, err = core.etcdCli.Delete(ctx2, proxy2)
		assert.NoError(t, err)
	})

	schema := schemapb.CollectionSchema{
		Name: collName,
	}
	sbf, err := proto.Marshal(&schema)
	assert.NoError(t, err)
	req := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     3011,
			Timestamp: 3011,
			SourceID:  3011,
		},
		DbName:         dbName,
		CollectionName: collName,
		Schema:         sbf,
	}
	status, err := core.CreateCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	wg.Add(1)
	t.Run("create alias", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.CreateAliasRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateAlias,
				MsgID:     3012,
				Timestamp: 3012,
				SourceID:  3012,
			},
			CollectionName: collName,
			Alias:          aliasName,
		}
		rsp, err := core.CreateAlias(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	wg.Add(1)
	t.Run("describe collection2", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     3013,
				Timestamp: 3013,
				SourceID:  3013,
			},
			DbName:         dbName,
			CollectionName: collName,
		}
		rsp, err := core.DescribeCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, rsp.Aliases, []string{aliasName})
	})

	// temporarily create collName2
	schema = schemapb.CollectionSchema{
		Name: collName2,
	}
	sbf, err = proto.Marshal(&schema)
	assert.NoError(t, err)
	req2 := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     3014,
			Timestamp: 3014,
			SourceID:  3014,
		},
		DbName:         dbName,
		CollectionName: collName2,
		Schema:         sbf,
	}
	status, err = core.CreateCollection(ctx, req2)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	wg.Add(1)
	t.Run("alter alias", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.AlterAliasRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_AlterAlias,
				MsgID:     3015,
				Timestamp: 3015,
				SourceID:  3015,
			},
			CollectionName: collName2,
			Alias:          aliasName,
		}
		rsp, err := core.AlterAlias(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	wg.Add(1)
	t.Run("drop collection with alias", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropAlias,
				MsgID:     3016,
				Timestamp: 3016,
				SourceID:  3016,
			},
			CollectionName: aliasName,
		}
		rsp, err := core.DropCollection(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	wg.Add(1)
	t.Run("drop alias", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.DropAliasRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropAlias,
				MsgID:     3017,
				Timestamp: 3017,
				SourceID:  3017,
			},
			Alias: aliasName,
		}
		rsp, err := core.DropAlias(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.ErrorCode)
	})

	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     3018,
			Timestamp: 3018,
			SourceID:  3018,
		},
		DbName:         dbName,
		CollectionName: collName,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     3019,
			Timestamp: 3019,
			SourceID:  3019,
		},
		DbName:         dbName,
		CollectionName: collName2,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	wg.Add(1)
	t.Run("get metrics", func(t *testing.T) {
		defer wg.Done()
		// not healthy
		stateSave := core.stateCode.Load().(internalpb.StateCode)
		core.UpdateStateCode(internalpb.StateCode_Abnormal)
		resp, err := core.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		core.UpdateStateCode(stateSave)

		// failed to parse metric type
		invalidRequest := "invalid request"
		resp, err = core.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Request: invalidRequest,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		resp, err = core.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// normal case
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err = metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		resp, err = core.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get system info", func(t *testing.T) {
		defer wg.Done()
		// normal case
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		resp, err := core.getSystemInfoMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = core.Stop()
	assert.NoError(t, err)
	st, err := core.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, st.Status.ErrorCode)
	assert.NotEqual(t, internalpb.StateCode_Healthy, st.State.StateCode)

	wg.Add(1)
	t.Run("state_not_healthy", func(t *testing.T) {
		defer wg.Done()
		st, err := core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     4000,
				Timestamp: 4000,
				SourceID:  4000,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     4001,
				Timestamp: 4001,
				SourceID:  4001,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp1, err := core.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     4002,
				Timestamp: 4002,
				SourceID:  4002,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp1.Status.ErrorCode)

		rsp2, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     4003,
				Timestamp: 4003,
				SourceID:  4003,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp2.Status.ErrorCode)

		rsp3, err := core.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     4004,
				Timestamp: 4004,
				SourceID:  4004,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp3.Status.ErrorCode)

		st, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     4005,
				Timestamp: 4005,
				SourceID:  4005,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		st, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     4006,
				Timestamp: 4006,
				SourceID:  4006,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp4, err := core.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     4007,
				Timestamp: 4007,
				SourceID:  4007,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp4.Status.ErrorCode)

		rsp5, err := core.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     4008,
				Timestamp: 4008,
				SourceID:  4008,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp5.Status.ErrorCode)

		st, err = core.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateIndex,
				MsgID:     4009,
				Timestamp: 4009,
				SourceID:  4009,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp6, err := core.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeIndex,
				MsgID:     4010,
				Timestamp: 4010,
				SourceID:  4010,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp6.Status.ErrorCode)

		st, err = core.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropIndex,
				MsgID:     4011,
				Timestamp: 4011,
				SourceID:  4011,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, st.ErrorCode)

		rsp7, err := core.DescribeSegment(ctx, &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     4012,
				Timestamp: 4012,
				SourceID:  4012,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp7.Status.ErrorCode)

		rsp8, err := core.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     4013,
				Timestamp: 4013,
				SourceID:  4013,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp8.Status.ErrorCode)

		rsp9, err := core.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "c1",
			PartitionName:  "p1",
			RowBased:       true,
			Files:          []string{"f1", "f2", "f3"},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp9.Status.ErrorCode)

		rsp10, err := core.GetImportState(ctx, &milvuspb.GetImportStateRequest{
			Task: 0,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp10.Status.ErrorCode)

		rsp11, err := core.ReportImport(ctx, &rootcoordpb.ImportResult{
			RowCount: 0,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp11.ErrorCode)

		rsp12, err := core.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, rsp12.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("alloc_error", func(t *testing.T) {
		defer wg.Done()
		core.Stop()
		core.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
			return 0, 0, fmt.Errorf("id allocator error test")
		}
		core.TSOAllocator = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("tso allcoator error test")
		}
		core.Init()
		core.Start()
		r1 := &rootcoordpb.AllocTimestampRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     5000,
				Timestamp: 5000,
				SourceID:  5000,
			},
			Count: 1,
		}
		p1, err := core.AllocTimestamp(ctx, r1)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, p1.Status.ErrorCode)

		r2 := &rootcoordpb.AllocIDRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     3001,
				Timestamp: 3001,
				SourceID:  3001,
			},
			Count: 1,
		}
		p2, err := core.AllocID(ctx, r2)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, p2.Status.ErrorCode)
	})

	wg.Wait()
	err = core.Stop()
	assert.NoError(t, err)
}

func TestRootCoord2(t *testing.T) {
	const (
		dbName   = "testDb"
		collName = "testColl"
		partName = "testPartition"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := dependency.NewDefaultFactory(true)

	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum
	core, err := NewCore(ctx, msFactory)
	assert.NoError(t, err)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	randVal := rand.Int()

	Params.CommonCfg.RootCoordTimeTick = fmt.Sprintf("rootcoord-time-tick-%d", randVal)
	Params.CommonCfg.RootCoordStatistics = fmt.Sprintf("rootcoord-statistics-%d", randVal)
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)
	Params.CommonCfg.RootCoordSubName = fmt.Sprintf("subname-%d", randVal)

	dm := &dataMock{randVal: randVal}
	err = core.SetDataCoord(ctx, dm)
	assert.NoError(t, err)

	im := &indexMock{
		fileArray:  []string{},
		idxBuildID: []int64{},
		idxID:      []int64{},
		idxDropID:  []int64{},
		mutex:      sync.Mutex{},
	}
	err = core.SetIndexCoord(im)
	assert.NoError(t, err)

	qm := &queryMock{
		collID: nil,
		mutex:  sync.Mutex{},
	}
	err = core.SetQueryCoord(qm)
	assert.NoError(t, err)

	core.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return nil, nil
	}

	core.SetEtcdClient(etcdCli)
	err = core.Init()
	assert.NoError(t, err)

	err = core.Start()
	assert.NoError(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("create collection", func(t *testing.T) {
		defer wg.Done()
		schema := schemapb.CollectionSchema{
			Name: collName,
		}

		sbf, err := proto.Marshal(&schema)
		assert.NoError(t, err)

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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		collInfo, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
		dmlStream, _ := msFactory.NewMsgStream(ctx)
		dmlStream.AsConsumer([]string{collInfo.PhysicalChannelNames[0]}, Params.CommonCfg.RootCoordSubName)
		dmlStream.Start()

		msgs := getNotTtMsg(ctx, 1, dmlStream.Chan())
		assert.Equal(t, 1, len(msgs))

		m1, ok := (msgs[0]).(*msgstream.CreateCollectionMsg)
		assert.True(t, ok)
		t.Log("time tick", m1.Base.Timestamp)
	})

	wg.Add(1)
	t.Run("describe collection", func(t *testing.T) {
		defer wg.Done()
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, collName, rsp.Schema.Name)
		assert.Equal(t, collMeta.ID, rsp.CollectionID)
		assert.Equal(t, common.DefaultShardsNum, int32(len(rsp.VirtualChannelNames)))
		assert.Equal(t, common.DefaultShardsNum, int32(len(rsp.PhysicalChannelNames)))
		assert.Equal(t, common.DefaultShardsNum, rsp.ShardsNum)
	})
	wg.Wait()
	err = core.Stop()
	assert.NoError(t, err)
}

func TestCheckInit(t *testing.T) {
	c, err := NewCore(context.Background(), nil)
	assert.NoError(t, err)

	err = c.Start()
	assert.Error(t, err)

	err = c.checkInit()
	assert.Error(t, err)

	c.MetaTable = &MetaTable{}
	err = c.checkInit()
	assert.Error(t, err)

	c.IDAllocator = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		return 0, 0, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.IDAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.TSOAllocator = func(count uint32) (typeutil.Timestamp, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.TSOAllocatorUpdate = func() error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.etcdCli = &clientv3.Client{}
	err = c.checkInit()
	assert.Error(t, err)

	c.kvBase = &etcdkv.EtcdKV{}
	err = c.checkInit()
	assert.Error(t, err)

	c.impTaskKv = &etcdkv.EtcdKV{}
	err = c.checkInit()
	assert.Error(t, err)

	c.SendDdCreateCollectionReq = func(context.Context, *internalpb.CreateCollectionRequest, []string) (map[string][]byte, error) {
		return map[string][]byte{}, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.SendDdDropCollectionReq = func(context.Context, *internalpb.DropCollectionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.SendDdCreatePartitionReq = func(context.Context, *internalpb.CreatePartitionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.SendDdDropPartitionReq = func(context.Context, *internalpb.DropPartitionRequest, []string) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallGetBinlogFilePathsService = func(ctx context.Context, segID, fieldID typeutil.UniqueID) ([]string, error) {
		return []string{}, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallGetNumRowsService = func(ctx context.Context, segID typeutil.UniqueID, isFromFlushedChan bool) (int64, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallGetFlushedSegmentsService = func(ctx context.Context, collID, partID typeutil.UniqueID) ([]typeutil.UniqueID, error) {
		return nil, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallBuildIndexService = func(ctx context.Context, binlog []string, field *schemapb.FieldSchema, idxInfo *etcdpb.IndexInfo, numRows int64) (typeutil.UniqueID, error) {
		return 0, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallDropIndexService = func(ctx context.Context, indexID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return nil, nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallReleaseCollectionService = func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallReleasePartitionService = func(ctx context.Context, ts typeutil.Timestamp, dbID, collectionID typeutil.UniqueID, partitionIDs []typeutil.UniqueID) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallWatchChannels = func(ctx context.Context, collectionID int64, channelNames []string) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallUpdateSegmentStateService = func(ctx context.Context, segID typeutil.UniqueID, ss commonpb.SegmentState) error {
		return nil
	}
	err = c.checkInit()
	assert.Error(t, err)

	c.CallImportService = func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
	}
	err = c.checkInit()
	assert.NoError(t, err)

	err = c.Stop()
	assert.NoError(t, err)
}

func TestCheckFlushedSegments(t *testing.T) {
	const (
		dbName   = "testDb"
		collName = "testColl"
		partName = "testPartition"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := dependency.NewDefaultFactory(true)
	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum
	core, err := NewCore(ctx, msFactory)
	assert.NoError(t, err)
	randVal := rand.Int()

	Params.CommonCfg.RootCoordTimeTick = fmt.Sprintf("rootcoord-time-tick-%d", randVal)
	Params.CommonCfg.RootCoordStatistics = fmt.Sprintf("rootcoord-statistics-%d", randVal)
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)
	Params.CommonCfg.RootCoordSubName = fmt.Sprintf("subname-%d", randVal)

	dm := &dataMock{randVal: randVal}
	err = core.SetDataCoord(ctx, dm)
	assert.NoError(t, err)

	im := &indexMock{
		fileArray:  []string{},
		idxBuildID: []int64{},
		idxID:      []int64{},
		idxDropID:  []int64{},
		mutex:      sync.Mutex{},
	}
	err = core.SetIndexCoord(im)
	assert.NoError(t, err)

	qm := &queryMock{
		collID: nil,
		mutex:  sync.Mutex{},
	}
	err = core.SetQueryCoord(qm)
	assert.NoError(t, err)

	core.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return nil, nil
	}

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()
	core.SetEtcdClient(etcdCli)
	err = core.Init()
	assert.NoError(t, err)

	err = core.Start()
	assert.NoError(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("check flushed segments", func(t *testing.T) {
		defer wg.Done()
		ctx := context.Background()
		var collID int64 = 1
		var partID int64 = 2
		var segID int64 = 1001
		var fieldID int64 = 101
		var indexID int64 = 6001
		core.MetaTable.segID2IndexMeta[segID] = make(map[int64]etcdpb.SegmentIndexInfo)
		core.MetaTable.partID2SegID[partID] = make(map[int64]bool)
		core.MetaTable.collID2Meta[collID] = etcdpb.CollectionInfo{ID: collID}
		// do nothing, since collection has 0 index
		core.checkFlushedSegments(ctx)

		// get field schema by id fail
		core.MetaTable.collID2Meta[collID] = etcdpb.CollectionInfo{
			ID:           collID,
			PartitionIDs: []int64{partID},
			FieldIndexes: []*etcdpb.FieldIndexInfo{
				{
					FiledID: fieldID,
					IndexID: indexID,
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{},
			},
		}
		core.checkFlushedSegments(ctx)

		// fail to get segment id ,dont panic
		core.CallGetFlushedSegmentsService = func(_ context.Context, collID, partID int64) ([]int64, error) {
			return []int64{}, errors.New("service not available")
		}
		core.checkFlushedSegments(core.ctx)
		// non-exist segID
		core.CallGetFlushedSegmentsService = func(_ context.Context, collID, partID int64) ([]int64, error) {
			return []int64{2001}, nil
		}
		core.checkFlushedSegments(core.ctx)

		// missing index info
		core.MetaTable.collID2Meta[collID] = etcdpb.CollectionInfo{
			ID:           collID,
			PartitionIDs: []int64{partID},
			FieldIndexes: []*etcdpb.FieldIndexInfo{
				{
					FiledID: fieldID,
					IndexID: indexID,
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: fieldID,
					},
				},
			},
		}
		core.checkFlushedSegments(ctx)
		// existing segID, buildIndex failed
		core.CallGetFlushedSegmentsService = func(_ context.Context, cid, pid int64) ([]int64, error) {
			assert.Equal(t, collID, cid)
			assert.Equal(t, partID, pid)
			return []int64{segID}, nil
		}
		core.MetaTable.indexID2Meta[indexID] = etcdpb.IndexInfo{
			IndexID: indexID,
		}
		core.CallBuildIndexService = func(_ context.Context, binlog []string, field *schemapb.FieldSchema, idx *etcdpb.IndexInfo, numRows int64) (int64, error) {
			assert.Equal(t, fieldID, field.FieldID)
			assert.Equal(t, indexID, idx.IndexID)
			return -1, errors.New("build index build")
		}

		core.checkFlushedSegments(ctx)

		var indexBuildID int64 = 10001
		core.CallBuildIndexService = func(_ context.Context, binlog []string, field *schemapb.FieldSchema, idx *etcdpb.IndexInfo, numRows int64) (int64, error) {
			return indexBuildID, nil
		}
		core.checkFlushedSegments(core.ctx)

	})
	wg.Wait()
	err = core.Stop()
	assert.NoError(t, err)
}

func TestRootCoord_CheckZeroShardsNum(t *testing.T) {
	const (
		dbName   = "testDb"
		collName = "testColl"
	)

	shardsNum := int32(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := dependency.NewDefaultFactory(true)
	Params.Init()
	Params.RootCoordCfg.DmlChannelNum = TestDMLChannelNum

	core, err := NewCore(ctx, msFactory)
	assert.NoError(t, err)
	randVal := rand.Int()
	Params.CommonCfg.RootCoordTimeTick = fmt.Sprintf("rootcoord-time-tick-%d", randVal)
	Params.CommonCfg.RootCoordStatistics = fmt.Sprintf("rootcoord-statistics-%d", randVal)
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)
	Params.CommonCfg.RootCoordSubName = fmt.Sprintf("subname-%d", randVal)

	dm := &dataMock{randVal: randVal}
	err = core.SetDataCoord(ctx, dm)
	assert.NoError(t, err)

	im := &indexMock{
		fileArray:  []string{},
		idxBuildID: []int64{},
		idxID:      []int64{},
		idxDropID:  []int64{},
		mutex:      sync.Mutex{},
	}
	err = core.SetIndexCoord(im)
	assert.NoError(t, err)

	qm := &queryMock{
		collID: nil,
		mutex:  sync.Mutex{},
	}
	err = core.SetQueryCoord(qm)
	assert.NoError(t, err)

	core.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return nil, nil
	}

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	core.SetEtcdClient(etcdCli)
	err = core.Init()
	assert.NoError(t, err)

	err = core.Start()
	assert.NoError(t, err)

	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	modifyFunc := func(collInfo *etcdpb.CollectionInfo) {
		collInfo.ShardsNum = 0
	}

	createCollectionInMeta(dbName, collName, core, shardsNum, modifyFunc)

	t.Run("describe collection", func(t *testing.T) {
		collMeta, err := core.MetaTable.GetCollectionByName(collName, 0)
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, collName, rsp.Schema.Name)
		assert.Equal(t, collMeta.ID, rsp.CollectionID)
		assert.Equal(t, shardsNum, int32(len(rsp.VirtualChannelNames)))
		assert.Equal(t, shardsNum, int32(len(rsp.PhysicalChannelNames)))
		assert.Equal(t, shardsNum, rsp.ShardsNum)
	})
	err = core.Stop()
	assert.NoError(t, err)
}

func TestCore_GetComponentStates(t *testing.T) {
	n := &Core{}
	n.stateCode.Store(internalpb.StateCode_Healthy)
	resp, err := n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	n.session = &sessionutil.Session{}
	n.session.UpdateRegistered(true)
	resp, err = n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestCore_DescribeSegments(t *testing.T) {
	collID := typeutil.UniqueID(1)
	partID := typeutil.UniqueID(2)
	segID := typeutil.UniqueID(100)
	fieldID := typeutil.UniqueID(3)
	buildID := typeutil.UniqueID(4)
	indexID := typeutil.UniqueID(1000)
	indexName := "test_describe_segments_index"

	c := &Core{
		ctx: context.Background(),
	}

	// not healthy.
	c.stateCode.Store(internalpb.StateCode_Abnormal)
	got1, err := c.DescribeSegments(context.Background(), &rootcoordpb.DescribeSegmentsRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, got1.GetStatus().GetErrorCode())

	// failed to be executed.
	c.CallGetFlushedSegmentsService = func(ctx context.Context, collID, partID typeutil.UniqueID) ([]typeutil.UniqueID, error) {
		return []typeutil.UniqueID{segID}, nil
	}
	c.stateCode.Store(internalpb.StateCode_Healthy)
	shortDuration := time.Nanosecond
	shortCtx, cancel := context.WithTimeout(c.ctx, shortDuration)
	defer cancel()
	time.Sleep(shortDuration)
	got2, err := c.DescribeSegments(shortCtx, &rootcoordpb.DescribeSegmentsRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, got2.GetStatus().GetErrorCode())

	// success.
	c.MetaTable = &MetaTable{
		segID2IndexMeta: map[typeutil.UniqueID]map[typeutil.UniqueID]etcdpb.SegmentIndexInfo{
			segID: {
				indexID: {
					CollectionID: collID,
					PartitionID:  partID,
					SegmentID:    segID,
					FieldID:      fieldID,
					IndexID:      indexID,
					BuildID:      buildID,
					EnableIndex:  true,
				},
			},
		},
		indexID2Meta: map[typeutil.UniqueID]etcdpb.IndexInfo{
			indexID: {
				IndexName:   indexName,
				IndexID:     indexID,
				IndexParams: nil,
			},
		},
	}
	infos, err := c.DescribeSegments(context.Background(), &rootcoordpb.DescribeSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeSegments,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  0,
		},
		CollectionID: collID,
		SegmentIDs:   []typeutil.UniqueID{segID},
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, infos.GetStatus().GetErrorCode())
	assert.Equal(t, 1, len(infos.GetSegmentInfos()))
	segmentInfo, ok := infos.GetSegmentInfos()[segID]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segmentInfo.GetIndexInfos()))
	assert.Equal(t, collID, segmentInfo.GetIndexInfos()[0].GetCollectionID())
	assert.Equal(t, partID, segmentInfo.GetIndexInfos()[0].GetPartitionID())
	assert.Equal(t, segID, segmentInfo.GetIndexInfos()[0].GetSegmentID())
	assert.Equal(t, fieldID, segmentInfo.GetIndexInfos()[0].GetFieldID())
	assert.Equal(t, indexID, segmentInfo.GetIndexInfos()[0].GetIndexID())
	assert.Equal(t, buildID, segmentInfo.GetIndexInfos()[0].GetBuildID())
	assert.Equal(t, true, segmentInfo.GetIndexInfos()[0].GetEnableIndex())

	indexInfo, ok := segmentInfo.GetExtraIndexInfos()[indexID]
	assert.True(t, ok)
	assert.Equal(t, indexName, indexInfo.IndexName)
	assert.Equal(t, indexID, indexInfo.IndexID)
}
