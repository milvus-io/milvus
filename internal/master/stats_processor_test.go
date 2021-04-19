package master

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"go.etcd.io/etcd/clientv3"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func TestStatsProcess(t *testing.T) {
	Init()
	etcdAddress := Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	rootPath := "/test/root"
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	_, err = cli.Delete(ctx, rootPath, clientv3.WithPrefix())
	assert.Nil(t, err)

	kvBase := etcdkv.NewEtcdKV(cli, rootPath)
	mt, err := NewMetaTable(kvBase)
	assert.Nil(t, err)

	var cnt int64 = 0
	globalTsoAllocator := func() (Timestamp, error) {
		val := atomic.AddInt64(&cnt, 1)
		phy := time.Now().UnixNano() / int64(time.Millisecond)
		ts := tsoutil.ComposeTS(phy, val)
		return ts, nil
	}
	runtimeStats := NewRuntimeStats()
	statsProcessor := NewStatsProcessor(mt, runtimeStats, globalTsoAllocator)

	ts, err := globalTsoAllocator()
	assert.Nil(t, err)

	collID := int64(1001)
	collName := "test_coll"
	partitionTag := "test_part"
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
	err = mt.AddSegment(&pb.SegmentMeta{
		SegmentID:    100,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: 0,
		ChannelEnd:   1,
		OpenTime:     ts,
	})
	assert.Nil(t, err)
	stats := internalpb.QueryNodeStats{
		MsgType: internalpb.MsgType_kQueryNodeStats,
		PeerID:  1,
		SegStats: []*internalpb.SegmentStats{
			{SegmentID: 100, MemorySize: 2500000, NumRows: 25000, RecentlyModified: true},
		},
		FieldStats: []*internalpb.FieldStats{
			{CollectionID: 1, FieldID: 100, IndexStats: []*internalpb.IndexStats{
				{IndexParams: []*commonpb.KeyValuePair{}, NumRelatedSegments: 100},
			}},
			{CollectionID: 2, FieldID: 100, IndexStats: []*internalpb.IndexStats{
				{IndexParams: []*commonpb.KeyValuePair{}, NumRelatedSegments: 200},
			}},
		},
	}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1},
	}
	msg := msgstream.QueryNodeStatsMsg{
		QueryNodeStats: stats,
		BaseMsg:        baseMsg,
	}

	var tsMsg msgstream.TsMsg = &msg
	msgPack := msgstream.MsgPack{
		Msgs: make([]msgstream.TsMsg, 0),
	}
	msgPack.Msgs = append(msgPack.Msgs, tsMsg)
	err = statsProcessor.ProcessQueryNodeStats(&msgPack)
	assert.Nil(t, err)

	segMeta, _ := mt.GetSegmentByID(100)
	assert.Equal(t, int64(100), segMeta.SegmentID)
	assert.Equal(t, int64(2500000), segMeta.MemSize)
	assert.Equal(t, int64(25000), segMeta.NumRows)

	assert.EqualValues(t, 100, runtimeStats.collStats[1].fieldIndexStats[100][0].numOfRelatedSegments)
	assert.EqualValues(t, 200, runtimeStats.collStats[2].fieldIndexStats[100][0].numOfRelatedSegments)

}
