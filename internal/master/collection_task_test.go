package master

import (
	"context"
	"log"
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

func TestMaster_CollectionTask(t *testing.T) {
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

	svr, err := CreateServer(ctx, "/test/root/kv", "/test/root/meta", []string{etcdAddr})
	assert.Nil(t, err)
	err = svr.Run(10002)
	assert.Nil(t, err)

	conn, err := grpc.DialContext(ctx, "127.0.0.1:10002", grpc.WithInsecure(), grpc.WithBlock())
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
	log.Printf("... [Create] collection col1\n")
	st, err := cli.CreateCollection(ctx, &req)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	// HasCollection
	reqHasCollection := internalpb.HasCollectionRequest{
		MsgType:   internalpb.MsgType_kHasCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		CollectionName: &servicepb.CollectionName{
			CollectionName: "col1",
		},
	}

	// "col1" is true
	log.Printf("... [Has] collection col1\n")
	boolResp, err := cli.HasCollection(ctx, &reqHasCollection)
	assert.Nil(t, err)
	assert.Equal(t, true, boolResp.Value)
	assert.Equal(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

	// "colNotExist" is false
	reqHasCollection.CollectionName.CollectionName = "colNotExist"
	boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
	assert.Nil(t, err)
	assert.Equal(t, boolResp.Value, false)
	assert.Equal(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

	// error
	reqHasCollection.Timestamp = Timestamp(10)
	reqHasCollection.CollectionName.CollectionName = "col1"
	boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
	assert.Nil(t, err)
	assert.NotEqual(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

	// ShowCollection
	reqShowCollection := internalpb.ShowCollectionRequest{
		MsgType:   internalpb.MsgType_kShowCollections,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
	}

	listResp, err := cli.ShowCollections(ctx, &reqShowCollection)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
	assert.Equal(t, 1, len(listResp.Values))
	assert.Equal(t, "col1", listResp.Values[0])

	reqShowCollection.Timestamp = Timestamp(10)
	listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)

	// CreateCollection Test
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

	// DescribeCollection Test
	reqDescribe := &internalpb.DescribeCollectionRequest{
		MsgType:   internalpb.MsgType_kDescribeCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		CollectionName: &servicepb.CollectionName{
			CollectionName: "col1",
		},
	}
	des, err := cli.DescribeCollection(ctx, reqDescribe)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)

	assert.Equal(t, "col1", des.Schema.Name)
	assert.Equal(t, false, des.Schema.AutoID)
	assert.Equal(t, 2, len(des.Schema.Fields))
	assert.Equal(t, "col1_f1", des.Schema.Fields[0].Name)
	assert.Equal(t, "col1_f2", des.Schema.Fields[1].Name)
	assert.Equal(t, schemapb.DataType_VECTOR_FLOAT, des.Schema.Fields[0].DataType)
	assert.Equal(t, schemapb.DataType_VECTOR_BINARY, des.Schema.Fields[1].DataType)
	assert.Equal(t, 2, len(des.Schema.Fields[0].TypeParams))
	assert.Equal(t, 2, len(des.Schema.Fields[0].IndexParams))
	assert.Equal(t, 2, len(des.Schema.Fields[1].TypeParams))
	assert.Equal(t, 2, len(des.Schema.Fields[1].IndexParams))
	assert.Equal(t, "col1_f1_tk1", des.Schema.Fields[0].TypeParams[0].Key)
	assert.Equal(t, "col1_f1_tv1", des.Schema.Fields[0].TypeParams[0].Value)
	assert.Equal(t, "col1_f1_ik1", des.Schema.Fields[0].IndexParams[0].Key)
	assert.Equal(t, "col1_f1_iv1", des.Schema.Fields[0].IndexParams[0].Value)
	assert.Equal(t, "col1_f1_tk2", des.Schema.Fields[0].TypeParams[1].Key)
	assert.Equal(t, "col1_f1_tv2", des.Schema.Fields[0].TypeParams[1].Value)
	assert.Equal(t, "col1_f1_ik2", des.Schema.Fields[0].IndexParams[1].Key)
	assert.Equal(t, "col1_f1_iv2", des.Schema.Fields[0].IndexParams[1].Value)

	assert.Equal(t, "col1_f2_tk1", des.Schema.Fields[1].TypeParams[0].Key)
	assert.Equal(t, "col1_f2_tv1", des.Schema.Fields[1].TypeParams[0].Value)
	assert.Equal(t, "col1_f2_ik1", des.Schema.Fields[1].IndexParams[0].Key)
	assert.Equal(t, "col1_f2_iv1", des.Schema.Fields[1].IndexParams[0].Value)
	assert.Equal(t, "col1_f2_tk2", des.Schema.Fields[1].TypeParams[1].Key)
	assert.Equal(t, "col1_f2_tv2", des.Schema.Fields[1].TypeParams[1].Value)
	assert.Equal(t, "col1_f2_ik2", des.Schema.Fields[1].IndexParams[1].Key)
	assert.Equal(t, "col1_f2_iv2", des.Schema.Fields[1].IndexParams[1].Value)

	reqDescribe.CollectionName.CollectionName = "colNotExist"
	des, err = cli.DescribeCollection(ctx, reqDescribe)
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)
	log.Printf(des.Status.Reason)

	reqDescribe.CollectionName.CollectionName = "col1"
	reqDescribe.Timestamp = Timestamp(10)
	des, err = cli.DescribeCollection(ctx, reqDescribe)
	assert.Nil(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)
	log.Printf(des.Status.Reason)

	// ------------------------------DropCollectionTask---------------------------
	log.Printf("... [Drop] collection col1\n")
	ser := servicepb.CollectionName{CollectionName: "col1"}

	reqDrop := internalpb.DropCollectionRequest{
		MsgType:        internalpb.MsgType_kDropCollection,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: &ser,
	}

	// DropCollection
	st, err = cli.DropCollection(ctx, &reqDrop)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	collMeta, err = svr.mt.GetCollectionByName(sch.Name)
	assert.NotNil(t, err)

	// HasCollection "col1" is false
	reqHasCollection.Timestamp = Timestamp(11)
	reqHasCollection.CollectionName.CollectionName = "col1"
	boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
	assert.Nil(t, err)
	assert.Equal(t, false, boolResp.Value)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, boolResp.Status.ErrorCode)

	// ShowCollections
	reqShowCollection.Timestamp = Timestamp(11)
	listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
	assert.Equal(t, 0, len(listResp.Values))

	// Drop again
	st, err = cli.DropCollection(ctx, &reqDrop)
	assert.Nil(t, err)
	assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	// Create "col1"
	req.Timestamp = Timestamp(11)
	st, err = cli.CreateCollection(ctx, &req)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

	boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
	assert.Nil(t, err)
	assert.Equal(t, true, boolResp.Value)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, boolResp.Status.ErrorCode)

	// Create "col2"
	sch.Name = "col2"
	schemaBytes, err = proto.Marshal(&sch)
	assert.Nil(t, err)

	req = internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}
	st, err = cli.CreateCollection(ctx, &req)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

	// Show Collections
	listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
	assert.Equal(t, 2, len(listResp.Values))
	assert.ElementsMatch(t, []string{"col1", "col2"}, listResp.Values)

	svr.Close()
}
