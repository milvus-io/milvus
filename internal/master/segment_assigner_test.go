package master

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func TestSegmentManager_AssignSegmentID(t *testing.T) {
	Init()
	Params.TopicNum = 5
	Params.QueryNodeNum = 3
	Params.SegmentSize = 536870912 / 1024 / 1024
	Params.SegmentSizeFactor = 0.75
	Params.DefaultRecordSize = 1024
	Params.MinSegIDAssignCnt = 1048576 / 1024
	Params.SegIDAssignExpiration = 2000
	collName := "coll_segmgr_test"
	collID := int64(1001)
	partitionTag := "test"
	etcdAddress := Params.EtcdAddress

	var cnt int64
	globalTsoAllocator := func() (Timestamp, error) {
		val := atomic.AddInt64(&cnt, 1)
		phy := time.Now().UnixNano() / int64(time.Millisecond)
		ts := tsoutil.ComposeTS(phy, val)
		return ts, nil
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	rootPath := "/test/root"
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	assert.Nil(t, err)
	kvBase := etcdkv.NewEtcdKV(cli, rootPath)
	defer kvBase.Close()
	mt, err := NewMetaTable(kvBase)
	assert.Nil(t, err)
	err = mt.AddCollection(&pb.CollectionMeta{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name: collName,
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	})
	assert.Nil(t, err)
	err = mt.AddPartition(collID, partitionTag)
	assert.Nil(t, err)
	timestamp, err := globalTsoAllocator()
	assert.Nil(t, err)
	err = mt.AddSegment(&pb.SegmentMeta{
		SegmentID:    100,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: 0,
		ChannelEnd:   1,
		OpenTime:     timestamp,
	})
	assert.Nil(t, err)
	proxySyncChan := make(chan *msgstream.TimeTickMsg)

	segAssigner := NewSegmentAssigner(ctx, mt, globalTsoAllocator, proxySyncChan)

	segAssigner.Start()
	defer segAssigner.Close()

	_, err = segAssigner.Assign(100, 100)
	assert.NotNil(t, err)
	err = segAssigner.OpenSegment(100, 100000)
	assert.Nil(t, err)
	result, err := segAssigner.Assign(100, 10000)
	assert.Nil(t, err)
	assert.True(t, result.isSuccess)

	result, err = segAssigner.Assign(100, 95000)
	assert.Nil(t, err)
	assert.False(t, result.isSuccess)

	time.Sleep(2 * time.Second)
	timestamp, err = globalTsoAllocator()
	assert.Nil(t, err)
	tickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: timestamp, EndTimestamp: timestamp, HashValues: []uint32{},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType: internalpb.MsgType_kTimeTick, PeerID: 1, Timestamp: timestamp,
		},
	}

	proxySyncChan <- tickMsg
	time.Sleep(500 * time.Millisecond)
	result, err = segAssigner.Assign(100, 100000)
	assert.Nil(t, err)
	assert.True(t, result.isSuccess)

	err = segAssigner.CloseSegment(100)
	assert.Nil(t, err)
	_, err = segAssigner.Assign(100, 100)
	assert.NotNil(t, err)

	err = mt.AddSegment(&pb.SegmentMeta{
		SegmentID:    200,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: 1,
		ChannelEnd:   1,
		OpenTime:     100,
		NumRows:      10000,
		MemSize:      100,
	})
	assert.Nil(t, err)

	err = segAssigner.OpenSegment(200, 20000)
	assert.Nil(t, err)
	result, err = segAssigner.Assign(200, 10001)
	assert.Nil(t, err)
	assert.False(t, result.isSuccess)
	result, err = segAssigner.Assign(200, 10000)
	assert.Nil(t, err)
	assert.True(t, result.isSuccess)
}
