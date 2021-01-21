package datanode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "-test-data-node" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
}

func startMaster(ctx context.Context) {
	master.Init()
	etcdAddr := master.Params.EtcdAddress
	metaRootPath := master.Params.MetaRootPath

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		panic(err)
	}
	_, err = etcdCli.Delete(context.TODO(), metaRootPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	masterPort := 53101
	master.Params.Port = masterPort
	svr, err := master.CreateServer(ctx)
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}
	if err := svr.Run(int64(master.Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())
	Params.MasterAddress = master.Params.Address + ":" + strconv.Itoa(masterPort)
}

func TestMain(m *testing.M) {
	Params.Init()
	refreshChannelNames()
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = true
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	startMaster(ctx)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func newDataNode() *DataNode {

	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = true
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		go func() {
			<-ctx.Done()
			cancel()
		}()
	} else {
		ctx = context.Background()
	}

	svr := NewDataNode(ctx, 0)
	return svr

}

func genTestDataNodeCollectionMeta(collectionName string, collectionID UniqueID, isBinary bool) *etcdpb.CollectionMeta {
	var fieldVec schemapb.FieldSchema
	if isBinary {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(100),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_VECTOR_BINARY,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "metric_type",
					Value: "JACCARD",
				},
			},
		}
	} else {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(100),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_VECTOR_FLOAT,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "16",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "metric_type",
					Value: "L2",
				},
			},
		}
	}

	fieldInt := schemapb.FieldSchema{
		FieldID:      UniqueID(101),
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT32,
	}

	schema := schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	return &collectionMeta
}

func initTestMeta(t *testing.T, node *DataNode, collectionName string, collectionID UniqueID, segmentID UniqueID, optional ...bool) {
	isBinary := false
	if len(optional) > 0 {
		isBinary = optional[0]
	}
	collectionMeta := genTestDataNodeCollectionMeta(collectionName, collectionID, isBinary)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	require.NotEqual(t, "", schemaBlob)

	var err = node.replica.addCollection(collectionMeta.ID, schemaBlob)
	require.NoError(t, err)

	collection, err := node.replica.getCollectionByName(collectionName)
	require.NoError(t, err)
	require.Equal(t, collection.Name(), collectionName)
	require.Equal(t, collection.ID(), collectionID)
	require.Equal(t, node.replica.getCollectionNum(), 1)
}
