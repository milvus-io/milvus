package master

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func TestMaster_Partition(t *testing.T) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}
	etcdAddr := "127.0.0.1:" + etcdPort

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	_, err = etcdCli.Delete(ctx, "/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	port := 10000 + rand.Intn(1000)
	svr, err := CreateServer(ctx, "/test/root/kv", "/test/root/meta", []string{etcdAddr})
	assert.Nil(t, err)
	err = svr.Run(int64(port))
	assert.Nil(t, err)

	conn, err := grpc.DialContext(ctx, "127.0.0.1:"+strconv.Itoa(port), grpc.WithInsecure(), grpc.WithBlock())
	assert.Nil(t, err)
	defer conn.Close()

	cli := masterpb.NewMasterClient(conn)
	sch := schemapb.CollectionSchema{
		Name:        "col1",
		Description: "test collection",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:        "col1_f1",
				Description: "test collection filed 1",
				DataType:    schemapb.DataType_VECTOR_FLOAT,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f1_tk1",
						Value: "col1_f1_tv1",
					},
					{
						Key:   "col1_f1_tk2",
						Value: "col1_f1_tv2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f1_ik1",
						Value: "col1_f1_iv1",
					},
					{
						Key:   "col1_f1_ik2",
						Value: "col1_f1_iv2",
					},
				},
			},
			{
				Name:        "col1_f2",
				Description: "test collection filed 2",
				DataType:    schemapb.DataType_VECTOR_BINARY,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f2_tk1",
						Value: "col1_f2_tv1",
					},
					{
						Key:   "col1_f2_tk2",
						Value: "col1_f2_tv2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "col1_f2_ik1",
						Value: "col1_f2_iv1",
					},
					{
						Key:   "col1_f2_ik2",
						Value: "col1_f2_iv2",
					},
				},
			},
		},
	}
	schemaBytes, err := proto.Marshal(&sch)
	assert.Nil(t, err)

	createCollectionReq := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreatePartition,
		ReqID:     1,
		Timestamp: 1,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}
	st, _ := cli.CreateCollection(ctx, &createCollectionReq)
	assert.NotNil(t, st)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

	createPartitionReq := internalpb.CreatePartitionRequest{
		MsgType:       internalpb.MsgType_kCreatePartition,
		ReqID:         1,
		Timestamp:     2,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}
	st, _ = cli.CreatePartition(ctx, &createPartitionReq)
	assert.NotNil(t, st)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

	createPartitionReq = internalpb.CreatePartitionRequest{
		MsgType:       internalpb.MsgType_kCreatePartition,
		ReqID:         1,
		Timestamp:     1,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}
	st, _ = cli.CreatePartition(ctx, &createPartitionReq)
	assert.NotNil(t, st)
	assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, st.ErrorCode)

	createPartitionReq = internalpb.CreatePartitionRequest{
		MsgType:       internalpb.MsgType_kCreatePartition,
		ReqID:         1,
		Timestamp:     3,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition2"},
	}
	st, _ = cli.CreatePartition(ctx, &createPartitionReq)
	assert.NotNil(t, st)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

	collMeta, err := svr.mt.GetCollectionByName(sch.Name)
	assert.Nil(t, err)
	t.Logf("collection id = %d", collMeta.ID)
	assert.Equal(t, collMeta.CreateTime, uint64(1))
	assert.Equal(t, collMeta.Schema.Name, "col1")
	assert.Equal(t, collMeta.Schema.AutoID, false)
	assert.Equal(t, len(collMeta.Schema.Fields), 2)
	assert.Equal(t, collMeta.Schema.Fields[0].Name, "col1_f1")
	assert.Equal(t, collMeta.Schema.Fields[1].Name, "col1_f2")
	assert.Equal(t, collMeta.Schema.Fields[0].DataType, schemapb.DataType_VECTOR_FLOAT)
	assert.Equal(t, collMeta.Schema.Fields[1].DataType, schemapb.DataType_VECTOR_BINARY)
	assert.Equal(t, len(collMeta.Schema.Fields[0].TypeParams), 2)
	assert.Equal(t, len(collMeta.Schema.Fields[0].IndexParams), 2)
	assert.Equal(t, len(collMeta.Schema.Fields[1].TypeParams), 2)
	assert.Equal(t, len(collMeta.Schema.Fields[1].IndexParams), 2)
	assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Key, "col1_f1_tk1")
	assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Key, "col1_f1_tk2")
	assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Value, "col1_f1_tv1")
	assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Value, "col1_f1_tv2")
	assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Key, "col1_f1_ik1")
	assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Key, "col1_f1_ik2")
	assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Value, "col1_f1_iv1")
	assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Value, "col1_f1_iv2")

	assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Key, "col1_f2_tk1")
	assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Key, "col1_f2_tk2")
	assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Value, "col1_f2_tv1")
	assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Value, "col1_f2_tv2")
	assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Key, "col1_f2_ik1")
	assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Key, "col1_f2_ik2")
	assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Value, "col1_f2_iv1")
	assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Value, "col1_f2_iv2")

	assert.Equal(t, collMeta.PartitionTags[0], "partition1")
	assert.Equal(t, collMeta.PartitionTags[1], "partition2")

	showPartitionReq := internalpb.ShowPartitionRequest{
		MsgType:        internalpb.MsgType_kShowPartitions,
		ReqID:          1,
		Timestamp:      4,
		ProxyID:        1,
		CollectionName: &servicepb.CollectionName{CollectionName: "col1"},
	}

	stringList, err := cli.ShowPartitions(ctx, &showPartitionReq)
	assert.Nil(t, err)
	assert.ElementsMatch(t, []string{"partition1", "partition2"}, stringList.Values)

	showPartitionReq = internalpb.ShowPartitionRequest{
		MsgType:        internalpb.MsgType_kShowPartitions,
		ReqID:          1,
		Timestamp:      3,
		ProxyID:        1,
		CollectionName: &servicepb.CollectionName{CollectionName: "col1"},
	}

	stringList, _ = cli.ShowPartitions(ctx, &showPartitionReq)
	assert.NotNil(t, stringList)
	assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, stringList.Status.ErrorCode)

	hasPartitionReq := internalpb.HasPartitionRequest{
		MsgType:       internalpb.MsgType_kHasPartition,
		ReqID:         1,
		Timestamp:     5,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}

	hasPartition, err := cli.HasPartition(ctx, &hasPartitionReq)
	assert.Nil(t, err)
	assert.True(t, hasPartition.Value)

	hasPartitionReq = internalpb.HasPartitionRequest{
		MsgType:       internalpb.MsgType_kHasPartition,
		ReqID:         1,
		Timestamp:     4,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}

	hasPartition, _ = cli.HasPartition(ctx, &hasPartitionReq)
	assert.NotNil(t, hasPartition)
	assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, stringList.Status.ErrorCode)

	hasPartitionReq = internalpb.HasPartitionRequest{
		MsgType:       internalpb.MsgType_kHasPartition,
		ReqID:         1,
		Timestamp:     6,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition3"},
	}

	hasPartition, err = cli.HasPartition(ctx, &hasPartitionReq)
	assert.Nil(t, err)
	assert.False(t, hasPartition.Value)

	deletePartitionReq := internalpb.DropPartitionRequest{
		MsgType:       internalpb.MsgType_kDropPartition,
		ReqID:         1,
		Timestamp:     7,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition2"},
	}

	st, err = cli.DropPartition(ctx, &deletePartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

	deletePartitionReq = internalpb.DropPartitionRequest{
		MsgType:       internalpb.MsgType_kDropPartition,
		ReqID:         1,
		Timestamp:     6,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition2"},
	}

	st, _ = cli.DropPartition(ctx, &deletePartitionReq)
	assert.NotNil(t, st)
	assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, st.ErrorCode)

	hasPartitionReq = internalpb.HasPartitionRequest{
		MsgType:       internalpb.MsgType_kHasPartition,
		ReqID:         1,
		Timestamp:     8,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition2"},
	}

	hasPartition, err = cli.HasPartition(ctx, &hasPartitionReq)
	assert.Nil(t, err)
	assert.False(t, hasPartition.Value)

	describePartitionReq := internalpb.DescribePartitionRequest{
		MsgType:       internalpb.MsgType_kDescribePartition,
		ReqID:         1,
		Timestamp:     9,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}

	describePartition, err := cli.DescribePartition(ctx, &describePartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"}, describePartition.Name)

	describePartitionReq = internalpb.DescribePartitionRequest{
		MsgType:       internalpb.MsgType_kDescribePartition,
		ReqID:         1,
		Timestamp:     8,
		ProxyID:       1,
		PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
	}

	describePartition, _ = cli.DescribePartition(ctx, &describePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, describePartition.Status.ErrorCode)

	svr.Close()
}
