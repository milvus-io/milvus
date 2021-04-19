package master

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	masterParams "github.com/zilliztech/milvus-distributed/internal/master/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func TestMaster_CreateCollection(t *testing.T) {
	Init()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	etcdAddr, err := masterParams.Params.EtcdAddress()
	if err != nil {
		panic(err)
	}
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	_, err = etcdCli.Delete(ctx, "/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	opt := Option{
		KVRootPath:            "/test/root/kv",
		MetaRootPath:          "/test/root/meta",
		EtcdAddr:              []string{etcdAddr},
		PulsarAddr:            "pulsar://localhost:6650",
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

	svr, err := CreateServer(ctx, &opt)
	assert.Nil(t, err)
	err = svr.Run(10001)
	assert.Nil(t, err)

	conn, err := grpc.DialContext(ctx, "127.0.0.1:10001", grpc.WithInsecure(), grpc.WithBlock())
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

	req := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}
	st, err := cli.CreateCollection(ctx, &req)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	collMeta, err := svr.mt.GetCollectionByName(sch.Name)
	assert.Nil(t, err)
	t.Logf("collection id = %d", collMeta.ID)
	assert.Equal(t, collMeta.CreateTime, uint64(11))
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

	req.Timestamp = Timestamp(10)
	st, err = cli.CreateCollection(ctx, &req)
	assert.Nil(t, err)
	assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	svr.Close()
}
