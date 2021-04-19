package master

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	masterParam "github.com/zilliztech/milvus-distributed/internal/master/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/master/tso"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
)

var mt *metaTable
var segMgr *SegmentManager
var collName = "coll_segmgr_test"
var collID = int64(1001)
var partitionTag = "test"
var kvBase *kv.EtcdKV

func setup() {
	masterParam.Params.Init()
	etcdAddress, err := masterParam.Params.EtcdAddress()
	if err != nil {
		panic(err)
	}
	rootPath, err := masterParam.Params.EtcdRootPath()
	if err != nil {
		panic(err)
	}
	id.Init([]string{etcdAddress}, rootPath)
	tso.Init([]string{etcdAddress}, rootPath)

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		panic(err)
	}
	rootpath := "/etcd/test/root"
	kvBase = kv.NewEtcdKV(cli, rootpath)
	tmpMt, err := NewMetaTable(kvBase)
	if err != nil {
		panic(err)
	}
	mt = tmpMt
	if mt.HasCollection(collID) {
		mt.DeleteCollection(collID)
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
	segMgr = NewSegmentManager(mt, opt)
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
		1000000, masterParam.Params.DefaultRecordSize()*1000000), err)
}

func TestSegmentManager_SegmentStats(t *testing.T) {
	setup()
	defer teardown()
	ts, err := tso.AllocOne()
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
			{SegmentID: 100, MemorySize: 25000 * masterParam.Params.DefaultRecordSize(), NumRows: 25000, RecentlyModified: true},
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

	time.Sleep(1 * time.Second)
	segMeta, _ := mt.GetSegmentByID(100)
	assert.Equal(t, int64(100), segMeta.SegmentID)
	assert.Equal(t, 25000*masterParam.Params.DefaultRecordSize(), segMeta.MemSize)
	assert.Equal(t, int64(25000), segMeta.NumRows)

	// close segment
	stats.SegStats[0].NumRows = 520000
	stats.SegStats[0].MemorySize = 520000 * masterParam.Params.DefaultRecordSize()
	err = segMgr.HandleQueryNodeMsgPack(&msgPack)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	segMeta, _ = mt.GetSegmentByID(100)
	assert.Equal(t, int64(100), segMeta.SegmentID)
	assert.NotEqual(t, 0, segMeta.CloseTime)
}
