package master

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func TestSegmentManager_AssignSegment(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()

	Init()
	Params.TopicNum = 5
	Params.QueryNodeNum = 3
	Params.SegmentSize = 536870912 / 1024 / 1024
	Params.SegmentSizeFactor = 0.75
	Params.DefaultRecordSize = 1024
	Params.MinSegIDAssignCnt = 1048576 / 1024
	Params.SegIDAssignExpiration = 2000
	etcdAddress := Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	rootPath := "/test/root"
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	assert.Nil(t, err)

	kvBase := etcdkv.NewEtcdKV(cli, rootPath)
	defer kvBase.Close()
	mt, err := NewMetaTable(kvBase)
	assert.Nil(t, err)

	collName := "segmgr_test_coll"
	var collID int64 = 1001
	partitionTag := "test_part"
	schema := &schemapb.CollectionSchema{
		Name: collName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "f1", IsPrimaryKey: false, DataType: schemapb.DataType_INT32},
			{FieldID: 2, Name: "f2", IsPrimaryKey: false, DataType: schemapb.DataType_VECTOR_FLOAT, TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "128"},
			}},
		},
	}
	err = mt.AddCollection(&pb.CollectionMeta{
		ID:            collID,
		Schema:        schema,
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	})
	assert.Nil(t, err)
	err = mt.AddPartition(collID, partitionTag)
	assert.Nil(t, err)

	var cnt int64
	globalIDAllocator := func() (UniqueID, error) {
		val := atomic.AddInt64(&cnt, 1)
		return val, nil
	}
	globalTsoAllocator := func() (Timestamp, error) {
		val := atomic.AddInt64(&cnt, 1)
		phy := time.Now().UnixNano() / int64(time.Millisecond)
		ts := tsoutil.ComposeTS(phy, val)
		return ts, nil
	}
	syncWriteChan := make(chan *msgstream.TimeTickMsg)
	syncProxyChan := make(chan *msgstream.TimeTickMsg)

	segAssigner := NewSegmentAssigner(ctx, mt, globalTsoAllocator, syncProxyChan)
	mockScheduler := &MockFlushScheduler{}
	segManager, err := NewSegmentManager(ctx, mt, globalIDAllocator, globalTsoAllocator, syncWriteChan, mockScheduler, segAssigner)
	assert.Nil(t, err)

	segManager.Start()
	defer segManager.Close()
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	assert.Nil(t, err)
	maxCount := uint32(Params.SegmentSize * 1024 * 1024 / float64(sizePerRecord))
	cases := []struct {
		Count         uint32
		ChannelID     int32
		Err           bool
		SameIDWith    int
		NotSameIDWith int
		ResultCount   int32
	}{
		{1000, 1, false, -1, -1, 1000},
		{2000, 0, false, 0, -1, 2000},
		{maxCount - 2999, 1, false, -1, 0, int32(maxCount - 2999)},
		{maxCount - 3000, 1, false, 0, -1, int32(maxCount - 3000)},
		{2000000000, 1, true, -1, -1, -1},
		{1000, 3, false, -1, 0, 1000},
		{maxCount, 2, false, -1, -1, int32(maxCount)},
	}

	var results = make([]*internalpb.SegIDAssignment, 0)
	for _, c := range cases {
		result, _ := segManager.AssignSegment([]*internalpb.SegIDRequest{{Count: c.Count, ChannelID: c.ChannelID, CollName: collName, PartitionTag: partitionTag}})
		results = append(results, result...)
		if c.Err {
			assert.EqualValues(t, commonpb.ErrorCode_UNEXPECTED_ERROR, result[0].Status.ErrorCode)
			continue
		}
		assert.EqualValues(t, commonpb.ErrorCode_SUCCESS, result[0].Status.ErrorCode)
		if c.SameIDWith != -1 {
			assert.EqualValues(t, result[0].SegID, results[c.SameIDWith].SegID)
		}
		if c.NotSameIDWith != -1 {
			assert.NotEqualValues(t, result[0].SegID, results[c.NotSameIDWith].SegID)
		}
		if c.ResultCount != -1 {
			assert.EqualValues(t, result[0].Count, c.ResultCount)
		}
	}

	time.Sleep(time.Duration(Params.SegIDAssignExpiration))
	timestamp, err := globalTsoAllocator()
	assert.Nil(t, err)
	err = mt.UpdateSegment(&pb.SegmentMeta{
		SegmentID:    results[0].SegID,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: 0,
		ChannelEnd:   1,
		CloseTime:    timestamp,
		NumRows:      400000,
		MemSize:      500000,
	})
	assert.Nil(t, err)
	tsMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: timestamp, EndTimestamp: timestamp, HashValues: []uint32{},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType:   internalpb.MsgType_kTimeTick,
			PeerID:    1,
			Timestamp: timestamp,
		},
	}
	syncWriteChan <- tsMsg
	time.Sleep(300 * time.Millisecond)
	segMeta, err := mt.GetSegmentByID(results[0].SegID)
	assert.Nil(t, err)
	assert.NotEqualValues(t, 0, segMeta.CloseTime)
}
func TestSegmentManager_RPC(t *testing.T) {
	Init()
	refreshMasterAddress()
	etcdAddress := Params.EtcdAddress
	rootPath := "/test/root"
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	assert.Nil(t, err)
	Params = ParamTable{
		Address: Params.Address,
		Port:    Params.Port,

		EtcdAddress:   Params.EtcdAddress,
		MetaRootPath:  "/test/root/meta",
		KvRootPath:    "/test/root/kv",
		PulsarAddress: Params.PulsarAddress,

		ProxyIDList:     []typeutil.UniqueID{1, 2},
		WriteNodeIDList: []typeutil.UniqueID{3, 4},

		TopicNum:                    5,
		QueryNodeNum:                3,
		SoftTimeTickBarrierInterval: 300,

		// segment
		SegmentSize:           536870912 / 1024 / 1024,
		SegmentSizeFactor:     0.75,
		DefaultRecordSize:     1024,
		MinSegIDAssignCnt:     1048576 / 1024,
		MaxSegIDAssignCnt:     Params.MaxSegIDAssignCnt,
		SegIDAssignExpiration: 2000,

		// msgChannel
		ProxyTimeTickChannelNames:     []string{"proxy1", "proxy2"},
		WriteNodeTimeTickChannelNames: []string{"write3", "write4"},
		InsertChannelNames:            []string{"dm0", "dm1"},
		K2SChannelNames:               []string{"k2s0", "k2s1"},
		QueryNodeStatsChannelName:     "statistic",
		MsgChannelSubName:             Params.MsgChannelSubName,

		MaxPartitionNum:     int64(4096),
		DefaultPartitionTag: "_default",
	}

	collName := "test_coll"
	partitionTag := "test_part"
	master, err := CreateServer(ctx)
	assert.Nil(t, err)
	defer master.Close()
	err = master.Run(int64(Params.Port))
	assert.Nil(t, err)
	dialContext, err := grpc.DialContext(ctx, Params.Address, grpc.WithInsecure(), grpc.WithBlock())
	assert.Nil(t, err)
	defer dialContext.Close()
	client := masterpb.NewMasterClient(dialContext)
	schema := &schemapb.CollectionSchema{
		Name:        collName,
		Description: "test coll",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "f1", IsPrimaryKey: false, DataType: schemapb.DataType_INT32},
			{FieldID: 1, Name: "f1", IsPrimaryKey: false, DataType: schemapb.DataType_VECTOR_FLOAT, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	schemaBytes, err := proto.Marshal(schema)
	assert.Nil(t, err)
	_, err = client.CreateCollection(ctx, &internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 100,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	})
	assert.Nil(t, err)
	_, err = client.CreatePartition(ctx, &internalpb.CreatePartitionRequest{
		MsgType:   internalpb.MsgType_kCreatePartition,
		ReqID:     2,
		Timestamp: 101,
		ProxyID:   1,
		PartitionName: &servicepb.PartitionName{
			CollectionName: collName,
			Tag:            partitionTag,
		},
	})
	assert.Nil(t, err)

	resp, err := client.AssignSegmentID(ctx, &internalpb.AssignSegIDRequest{
		PeerID: 1,
		Role:   internalpb.PeerRole_Proxy,
		PerChannelReq: []*internalpb.SegIDRequest{
			{Count: 10000, ChannelID: 0, CollName: collName, PartitionTag: partitionTag},
		},
	})
	assert.Nil(t, err)
	assignments := resp.GetPerChannelAssignment()
	assert.EqualValues(t, 1, len(assignments))
	assert.EqualValues(t, commonpb.ErrorCode_SUCCESS, assignments[0].Status.ErrorCode)
	assert.EqualValues(t, collName, assignments[0].CollName)
	assert.EqualValues(t, partitionTag, assignments[0].PartitionTag)
	assert.EqualValues(t, int32(0), assignments[0].ChannelID)
	assert.EqualValues(t, uint32(10000), assignments[0].Count)

	// test stats
	segID := assignments[0].SegID
	pulsarAddress := Params.PulsarAddress
	ms := msgstream.NewPulsarMsgStream(ctx, 1024)
	ms.SetPulsarClient(pulsarAddress)
	ms.CreatePulsarProducers([]string{"statistic"})
	ms.Start()
	defer ms.Close()

	err = ms.Produce(&msgstream.MsgPack{
		BeginTs: 102,
		EndTs:   104,
		Msgs: []msgstream.TsMsg{
			&msgstream.QueryNodeStatsMsg{
				QueryNodeStats: internalpb.QueryNodeStats{
					MsgType: internalpb.MsgType_kQueryNodeStats,
					PeerID:  1,
					SegStats: []*internalpb.SegmentStats{
						{SegmentID: segID, MemorySize: 600000000, NumRows: 1000000, RecentlyModified: true},
					},
				},
				BaseMsg: msgstream.BaseMsg{
					HashValues: []uint32{0},
				},
			},
		},
	})
	assert.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	segMeta, err := master.metaTable.GetSegmentByID(segID)
	assert.Nil(t, err)
	assert.EqualValues(t, 1000000, segMeta.GetNumRows())
	assert.EqualValues(t, int64(600000000), segMeta.GetMemSize())
}
