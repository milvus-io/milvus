package master

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"go.etcd.io/etcd/clientv3"
)

var mt *metaTable
var segMgr *SegmentManager
var collName = "coll_segmgr_test"
var collID = int64(1001)
var partitionTag = "test"
var kvBase *kv.EtcdKV
var master *Master
var masterCancelFunc context.CancelFunc

func setup() {
	Params.Init()
	etcdAddress := Params.EtcdAddress()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		panic(err)
	}
	rootPath := "/test/root"
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}
	kvBase = kv.NewEtcdKV(cli, rootPath)
	tmpMt, err := NewMetaTable(kvBase)
	if err != nil {
		panic(err)
	}
	mt = tmpMt
	if mt.HasCollection(collID) {
		err := mt.DeleteCollection(collID)
		if err != nil {
			panic(err)
		}
	}
	err = mt.AddCollection(&pb.CollectionMeta{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name: collName,
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	})
	if err != nil {
		panic(err)
	}
	err = mt.AddPartition(collID, partitionTag)
	if err != nil {
		panic(err)
	}
	opt := &Option{
		SegmentThreshold:      536870912,
		SegmentExpireDuration: 2000,
		MinimumAssignSize:     1048576,
		DefaultRecordSize:     1024,
		NumOfQueryNode:        3,
		NumOfChannel:          5,
	}

	var cnt int64

	segMgr = NewSegmentManager(mt, opt,
		func() (UniqueID, error) {
			val := atomic.AddInt64(&cnt, 1)
			return val, nil
		},
		func() (Timestamp, error) {
			val := atomic.AddInt64(&cnt, 1)
			phy := time.Now().UnixNano() / int64(time.Millisecond)
			ts := tsoutil.ComposeTS(phy, val)
			return ts, nil
		},
	)
}

func teardown() {
	err := mt.DeleteCollection(collID)
	if err != nil {
		log.Fatalf(err.Error())
	}
	kvBase.Close()
}

func TestSegmentManager_AssignSegmentID(t *testing.T) {
	setup()
	defer teardown()
	reqs := []*internalpb.SegIDRequest{
		{CollName: collName, PartitionTag: partitionTag, Count: 25000, ChannelID: 0},
		{CollName: collName, PartitionTag: partitionTag, Count: 10000, ChannelID: 1},
		{CollName: collName, PartitionTag: partitionTag, Count: 30000, ChannelID: 2},
		{CollName: collName, PartitionTag: partitionTag, Count: 25000, ChannelID: 3},
		{CollName: collName, PartitionTag: partitionTag, Count: 10000, ChannelID: 4},
	}

	segAssigns, err := segMgr.AssignSegmentID(reqs)
	assert.Nil(t, err)

	assert.Equal(t, uint32(25000), segAssigns[0].Count)
	assert.Equal(t, uint32(10000), segAssigns[1].Count)
	assert.Equal(t, uint32(30000), segAssigns[2].Count)
	assert.Equal(t, uint32(25000), segAssigns[3].Count)
	assert.Equal(t, uint32(10000), segAssigns[4].Count)

	assert.Equal(t, segAssigns[0].SegID, segAssigns[1].SegID)
	assert.Equal(t, segAssigns[2].SegID, segAssigns[3].SegID)

	newReqs := []*internalpb.SegIDRequest{
		{CollName: collName, PartitionTag: partitionTag, Count: 500000, ChannelID: 0},
	}
	// test open a new segment
	newAssign, err := segMgr.AssignSegmentID(newReqs)
	assert.Nil(t, err)
	assert.NotNil(t, newAssign)
	assert.Equal(t, uint32(500000), newAssign[0].Count)
	assert.NotEqual(t, segAssigns[0].SegID, newAssign[0].SegID)

	// test assignment expiration
	time.Sleep(3 * time.Second)

	assignAfterExpiration, err := segMgr.AssignSegmentID(newReqs)
	assert.Nil(t, err)
	assert.NotNil(t, assignAfterExpiration)
	assert.Equal(t, uint32(500000), assignAfterExpiration[0].Count)
	assert.Equal(t, segAssigns[0].SegID, assignAfterExpiration[0].SegID)

	// test invalid params
	newReqs[0].CollName = "wrong_collname"
	_, err = segMgr.AssignSegmentID(newReqs)
	assert.Error(t, errors.Errorf("can not find collection with id=%d", collID), err)

	newReqs[0].Count = 1000000
	_, err = segMgr.AssignSegmentID(newReqs)
	assert.Error(t, errors.Errorf("request with count %d need about %d mem size which is larger than segment threshold",
		1000000, 1024*1000000), err)
}

func TestSegmentManager_SegmentStats(t *testing.T) {
	setup()
	defer teardown()
	ts, err := segMgr.globalTSOAllocator()
	assert.Nil(t, err)
	err = mt.AddSegment(&pb.SegmentMeta{
		SegmentID:    100,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: 0,
		ChannelEnd:   1,
		OpenTime:     ts,
	})
	assert.Nil(t, err)
	stats := internalpb.QueryNodeSegStats{
		MsgType: internalpb.MsgType_kQueryNodeSegStats,
		PeerID:  1,
		SegStats: []*internalpb.SegmentStats{
			{SegmentID: 100, MemorySize: 2500000, NumRows: 25000, RecentlyModified: true},
		},
	}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{1},
	}
	msg := msgstream.QueryNodeSegStatsMsg{
		QueryNodeSegStats: stats,
		BaseMsg:           baseMsg,
	}

	var tsMsg msgstream.TsMsg = &msg
	msgPack := msgstream.MsgPack{
		Msgs: make([]msgstream.TsMsg, 0),
	}
	msgPack.Msgs = append(msgPack.Msgs, tsMsg)
	err = segMgr.HandleQueryNodeMsgPack(&msgPack)
	assert.Nil(t, err)

	segMeta, _ := mt.GetSegmentByID(100)
	assert.Equal(t, int64(100), segMeta.SegmentID)
	assert.Equal(t, int64(2500000), segMeta.MemSize)
	assert.Equal(t, int64(25000), segMeta.NumRows)

	// close segment
	stats.SegStats[0].NumRows = 600000
	stats.SegStats[0].MemorySize = 600000000
	err = segMgr.HandleQueryNodeMsgPack(&msgPack)
	assert.Nil(t, err)
	segMeta, _ = mt.GetSegmentByID(100)
	assert.Equal(t, int64(100), segMeta.SegmentID)
	assert.NotEqual(t, uint64(0), segMeta.CloseTime)
}

func startupMaster() {
	Params.Init()
	etcdAddress := Params.EtcdAddress()
	rootPath := "/test/root"
	ctx, cancel := context.WithCancel(context.TODO())
	masterCancelFunc = cancel
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		panic(err)
	}
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}
	pulsarAddress := Params.PulsarAddress()

	opt := &Option{
		KVRootPath:            "/test/root/kv",
		MetaRootPath:          "/test/root/meta",
		EtcdAddr:              []string{etcdAddress},
		PulsarAddr:            pulsarAddress,
		ProxyIDs:              []typeutil.UniqueID{1, 2},
		PulsarProxyChannels:   []string{"proxy1", "proxy2"},
		PulsarProxySubName:    "proxyTopics",
		SoftTTBInterval:       300,
		WriteIDs:              []typeutil.UniqueID{3, 4},
		PulsarWriteChannels:   []string{"write3", "write4"},
		PulsarWriteSubName:    "writeTopics",
		PulsarDMChannels:      []string{"dm0", "dm1"},
		PulsarK2SChannels:     []string{"k2s0", "k2s1"},
		DefaultRecordSize:     1024,
		MinimumAssignSize:     1048576,
		SegmentThreshold:      536870912,
		SegmentExpireDuration: 2000,
		NumOfChannel:          5,
		NumOfQueryNode:        3,
		StatsChannels:         "statistic",
	}

	master, err = CreateServer(ctx, opt)
	if err != nil {
		panic(err)
	}
	err = master.Run(10013)

	if err != nil {
		panic(err)
	}
}

func shutdownMaster() {
	masterCancelFunc()
	master.Close()
}

func TestSegmentManager_RPC(t *testing.T) {
	startupMaster()
	defer shutdownMaster()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	dialContext, err := grpc.DialContext(ctx, "127.0.0.1:10013", grpc.WithInsecure(), grpc.WithBlock())
	assert.Nil(t, err)
	defer dialContext.Close()
	client := masterpb.NewMasterClient(dialContext)
	schema := &schemapb.CollectionSchema{
		Name:        collName,
		Description: "test coll",
		AutoID:      false,
		Fields:      []*schemapb.FieldSchema{},
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
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, resp.Status.ErrorCode)
	assignments := resp.GetPerChannelAssignment()
	assert.Equal(t, 1, len(assignments))
	assert.Equal(t, collName, assignments[0].CollName)
	assert.Equal(t, partitionTag, assignments[0].PartitionTag)
	assert.Equal(t, int32(0), assignments[0].ChannelID)
	assert.Equal(t, uint32(10000), assignments[0].Count)

	// test stats
	segID := assignments[0].SegID
	pulsarAddress := Params.PulsarAddress()
	ms := msgstream.NewPulsarMsgStream(ctx, 1024)
	ms.SetPulsarClient(pulsarAddress)
	ms.CreatePulsarProducers([]string{"statistic"})
	ms.Start()
	defer ms.Close()

	err = ms.Produce(&msgstream.MsgPack{
		BeginTs: 102,
		EndTs:   104,
		Msgs: []msgstream.TsMsg{
			&msgstream.QueryNodeSegStatsMsg{
				QueryNodeSegStats: internalpb.QueryNodeSegStats{
					MsgType: internalpb.MsgType_kQueryNodeSegStats,
					PeerID:  1,
					SegStats: []*internalpb.SegmentStats{
						{SegmentID: segID, MemorySize: 600000000, NumRows: 1000000, RecentlyModified: true},
					},
				},
				BaseMsg: msgstream.BaseMsg{
					HashValues: []int32{0},
				},
			},
		},
	})
	assert.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	segMeta, err := master.metaTable.GetSegmentByID(segID)
	assert.Nil(t, err)
	assert.NotEqual(t, uint64(0), segMeta.GetCloseTime())
	assert.Equal(t, int64(600000000), segMeta.GetMemSize())
}
