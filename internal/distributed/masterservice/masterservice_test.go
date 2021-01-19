package masterservice

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

func TestGrpcService(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	cms.Params.Address = "127.0.0.1"
	cms.Params.Port = (randVal % 100) + 10000
	cms.Params.NodeID = 0

	cms.Params.PulsarAddress = "pulsar://127.0.0.1:6650"
	cms.Params.EtcdAddress = "127.0.0.1:2379"
	cms.Params.MetaRootPath = fmt.Sprintf("/%d/test/meta", randVal)
	cms.Params.KvRootPath = fmt.Sprintf("/%d/test/kv", randVal)
	cms.Params.ProxyTimeTickChannel = fmt.Sprintf("proxyTimeTick%d", randVal)
	cms.Params.MsgChannelSubName = fmt.Sprintf("msgChannel%d", randVal)
	cms.Params.TimeTickChannel = fmt.Sprintf("timeTick%d", randVal)
	cms.Params.DdChannel = fmt.Sprintf("ddChannel%d", randVal)
	cms.Params.StatisticsChannel = fmt.Sprintf("stateChannel%d", randVal)

	cms.Params.MaxPartitionNum = 64
	cms.Params.DefaultPartitionTag = "_default"

	t.Logf("master service port = %d", cms.Params.Port)

	svr, err := NewGrpcServer()
	assert.Nil(t, err)

	core := svr.core.(*cms.Core)

	core.ProxyTimeTickChan = make(chan typeutil.Timestamp, 8)

	timeTickArray := make([]typeutil.Timestamp, 0, 16)
	core.SendTimeTick = func(ts typeutil.Timestamp) error {
		t.Logf("send time tick %d", ts)
		timeTickArray = append(timeTickArray, ts)
		return nil
	}
	createCollectionArray := make([]*cms.CreateCollectionReqTask, 0, 16)
	core.DdCreateCollectionReq = func(req *cms.CreateCollectionReqTask) error {
		t.Logf("Create Colllection %s", req.Req.CollectionName)
		createCollectionArray = append(createCollectionArray, req)
		return nil
	}

	dropCollectionArray := make([]*cms.DropCollectionReqTask, 0, 16)
	core.DdDropCollectionReq = func(req *cms.DropCollectionReqTask) error {
		t.Logf("Drop Collection %s", req.Req.CollectionName)
		dropCollectionArray = append(dropCollectionArray, req)
		return nil
	}

	createPartitionArray := make([]*cms.CreatePartitionReqTask, 0, 16)
	core.DdCreatePartitionReq = func(req *cms.CreatePartitionReqTask) error {
		t.Logf("Create Partition %s", req.Req.PartitionName)
		createPartitionArray = append(createPartitionArray, req)
		return nil
	}

	dropPartitionArray := make([]*cms.DropPartitionReqTask, 0, 16)
	core.DdDropPartitionReq = func(req *cms.DropPartitionReqTask) error {
		t.Logf("Drop Partition %s", req.Req.PartitionName)
		dropPartitionArray = append(dropPartitionArray, req)
		return nil
	}

	core.GetSegmentMeta = func(id typeutil.UniqueID) (*etcdpb.SegmentMeta, error) {
		return &etcdpb.SegmentMeta{
			SegmentID:    20,
			CollectionID: 10,
			PartitionTag: "_default",
			ChannelStart: 50,
			ChannelEnd:   100,
			OpenTime:     1000,
			CloseTime:    2000,
			NumRows:      16,
			MemSize:      1024,
			BinlogFilePaths: []*etcdpb.FieldBinlogFiles{
				{
					FieldID:     101,
					BinlogFiles: []string{"/test/binlog/file"},
				},
			},
		}, nil
	}

	err = svr.Init(&cms.InitParams{ProxyTimeTickChannel: fmt.Sprintf("proxyTimeTick%d", randVal)})
	assert.Nil(t, err)
	err = svr.Start()
	assert.Nil(t, err)

	cli, err := NewGrpcClient(fmt.Sprintf("127.0.0.1:%d", cms.Params.Port), 3*time.Second)
	assert.Nil(t, err)

	err = cli.Init(&cms.InitParams{ProxyTimeTickChannel: fmt.Sprintf("proxyTimeTick%d", randVal)})
	assert.Nil(t, err)

	err = cli.Start()
	assert.Nil(t, err)

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
					DataType:     schemapb.DataType_VECTOR_FLOAT,
					TypeParams:   nil,
					IndexParams:  nil,
				},
			},
		}

		sbf, err := proto.Marshal(&schema)
		assert.Nil(t, err)

		req := &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreateCollection,
				MsgID:     100,
				Timestamp: 100,
				SourceID:  100,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			Schema:         sbf,
		}

		status, err := cli.CreateCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, len(createCollectionArray), 1)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, createCollectionArray[0].Req.Base.MsgType, commonpb.MsgType_kCreateCollection)
		assert.Equal(t, createCollectionArray[0].Req.CollectionName, "testColl")
	})

	t.Run("has collection", func(t *testing.T) {
		req := &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasCollection,
				MsgID:     101,
				Timestamp: 101,
				SourceID:  101,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := cli.HasCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, rsp.Value, true)

		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasCollection,
				MsgID:     102,
				Timestamp: 102,
				SourceID:  102,
			},
			DbName:         "testDb",
			CollectionName: "testColl2",
		}
		rsp, err = cli.HasCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, rsp.Value, false)

		req = &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasCollection,
				MsgID:     102,
				Timestamp: 102,
				SourceID:  102,
			},
			DbName:         "testDb",
			CollectionName: "testColl2",
		}
		rsp, err = cli.HasCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_UNEXPECTED_ERROR)

	})

	t.Run("describe collection", func(t *testing.T) {
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDescribeCollection,
				MsgID:     103,
				Timestamp: 103,
				SourceID:  103,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := cli.DescribeCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, rsp.Schema.Name, "testColl")
	})

	t.Run("get collection statistics", func(t *testing.T) {
		req := &milvuspb.CollectionStatsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO,miss msg type
				MsgID:     104,
				Timestamp: 104,
				SourceID:  104,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := cli.GetCollectionStatistics(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, len(rsp.Stats), 2)
		assert.Equal(t, rsp.Stats[0].Key, "row_count")
		assert.Equal(t, rsp.Stats[0].Value, "0")
		assert.Equal(t, rsp.Stats[1].Key, "data_size")
		assert.Equal(t, rsp.Stats[1].Value, "0")

		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		seg := &etcdpb.SegmentMeta{
			SegmentID:    101,
			CollectionID: collMeta.ID,
			PartitionTag: cms.Params.DefaultPartitionTag,
		}
		err = core.MetaTable.AddSegment(seg)
		assert.Nil(t, err)

		req = &milvuspb.CollectionStatsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO,miss msg type
				MsgID:     105,
				Timestamp: 105,
				SourceID:  105,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err = cli.GetCollectionStatistics(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, len(rsp.Stats), 2)
		assert.Equal(t, rsp.Stats[0].Key, "row_count")
		assert.Equal(t, rsp.Stats[0].Value, "16")
		assert.Equal(t, rsp.Stats[1].Key, "data_size")
		assert.Equal(t, rsp.Stats[1].Value, "1024")

	})

	t.Run("show collection", func(t *testing.T) {
		req := &milvuspb.ShowCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowCollections,
				MsgID:     106,
				Timestamp: 106,
				SourceID:  106,
			},
			DbName: "testDb",
		}
		rsp, err := cli.ShowCollections(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, rsp.CollectionNames[0], "testColl")
		assert.Equal(t, len(rsp.CollectionNames), 1)
	})

	t.Run("create partition", func(t *testing.T) {
		req := &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     107,
				Timestamp: 107,
				SourceID:  107,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		status, err := cli.CreatePartition(req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.PartitionIDs), 2)
		assert.Equal(t, collMeta.PartitionTags[1], "testPartition")

	})

	t.Run("has partition", func(t *testing.T) {
		req := &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasPartition,
				MsgID:     108,
				Timestamp: 108,
				SourceID:  108,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		rsp, err := cli.HasPartition(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, rsp.Value, true)
	})

	t.Run("get partition statistics", func(t *testing.T) {
		req := &milvuspb.PartitionStatsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     109,
				Timestamp: 109,
				SourceID:  109,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  cms.Params.DefaultPartitionTag,
		}
		rsp, err := cli.GetPartitionStatistics(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, len(rsp.Stats), 2)
		assert.Equal(t, rsp.Stats[0].Key, "row_count")
		assert.Equal(t, rsp.Stats[0].Value, "16")
		assert.Equal(t, rsp.Stats[1].Key, "data_size")
		assert.Equal(t, rsp.Stats[1].Value, "1024")
	})

	t.Run("show partition", func(t *testing.T) {
		req := &milvuspb.ShowPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowPartitions,
				MsgID:     110,
				Timestamp: 110,
				SourceID:  110,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		rsp, err := cli.ShowPartitions(req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, len(rsp.PartitionNames), 2)
	})

	t.Run("drop partition", func(t *testing.T) {
		req := &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropPartition,
				MsgID:     199,
				Timestamp: 199,
				SourceID:  199,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
			PartitionName:  "testPartition",
		}
		status, err := cli.DropPartition(req)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		collMeta, err := core.MetaTable.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, len(collMeta.PartitionIDs), 1)
		assert.Equal(t, collMeta.PartitionTags[0], cms.Params.DefaultPartitionTag)
	})

	t.Run("drop collection", func(t *testing.T) {
		req := &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     200,
				Timestamp: 200,
				SourceID:  200,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}

		status, err := cli.DropCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, len(dropCollectionArray), 1)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_SUCCESS)
		assert.Equal(t, dropCollectionArray[0].Req.Base.MsgType, commonpb.MsgType_kDropCollection)
		assert.Equal(t, dropCollectionArray[0].Req.CollectionName, "testColl")

		req = &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     200,
				Timestamp: 200,
				SourceID:  200,
			},
			DbName:         "testDb",
			CollectionName: "testColl",
		}
		status, err = cli.DropCollection(req)
		assert.Nil(t, err)
		assert.Equal(t, len(dropCollectionArray), 1)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UNEXPECTED_ERROR)
	})

	err = cli.Stop()
	assert.Nil(t, err)

	err = svr.Stop()
	assert.Nil(t, err)
}
