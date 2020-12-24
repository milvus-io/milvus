package querynode

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const ctxTimeInMillisecond = 2000
const closeWithDeadline = true

func setup() {
	Params.Init()
	Params.MetaRootPath = "/etcd/test/root/querynode"
}

func genTestCollectionMeta(collectionName string, collectionID UniqueID) *etcdpb.CollectionMeta {
	fieldVec := schemapb.FieldSchema{
		FieldID:      UniqueID(0),
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

	fieldInt := schemapb.FieldSchema{
		FieldID:      UniqueID(1),
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

func initTestMeta(t *testing.T, node *QueryNode, collectionName string, collectionID UniqueID, segmentID UniqueID) {
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	var err = node.replica.addCollection(collectionMeta.ID, schemaBlob)
	assert.NoError(t, err)

	collection, err := node.replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, node.replica.getCollectionNum(), 1)

	err = node.replica.addPartition(collection.ID(), collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	err = node.replica.addSegment(segmentID, collectionMeta.PartitionTags[0], collectionID)
	assert.NoError(t, err)
}

func newQueryNode() *QueryNode {

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

	svr := NewQueryNode(ctx, 0)
	return svr

}

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "-test-query-node" + strconv.FormatInt(rand.Int63n(1000000), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
	Params.SearchChannelNames = makeNewChannelNames(Params.SearchChannelNames, suffix)
	Params.SearchResultChannelNames = makeNewChannelNames(Params.SearchResultChannelNames, suffix)
	Params.StatsChannelName = Params.StatsChannelName + suffix
	Params.LoadIndexChannelNames = makeNewChannelNames(Params.LoadIndexChannelNames, suffix)
}

func TestMain(m *testing.M) {
	setup()
	refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

// NOTE: start pulsar and etcd before test
func TestQueryNode_Start(t *testing.T) {
	localNode := newQueryNode()
	err := localNode.Start()
	assert.Nil(t, err)
	localNode.Close()
}
