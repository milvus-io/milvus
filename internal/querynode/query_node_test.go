package querynode

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/types"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const ctxTimeInMillisecond = 5000
const debug = false

const defaultPartitionID = UniqueID(2021)

type queryServiceMock struct {
	types.QueryService
}

func setup() {
	os.Setenv("QUERY_NODE_ID", "1")
	Params.Init()
	//Params.QueryNodeID = 1
	Params.initQueryTimeTickChannelName()
	Params.initSearchResultChannelNames()
	Params.initStatsChannelName()
	Params.initSearchChannelNames()
	Params.MetaRootPath = "/etcd/test/root/querynode"

}

func genTestCollectionMeta(collectionID UniqueID, isBinary bool) *etcdpb.CollectionInfo {
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
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionInfo{
		ID:           collectionID,
		Schema:       &schema,
		CreateTime:   Timestamp(0),
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	return &collectionMeta
}

func initTestMeta(t *testing.T, node *QueryNode, collectionID UniqueID, segmentID UniqueID, optional ...bool) {
	isBinary := false
	if len(optional) > 0 {
		isBinary = optional[0]
	}
	collectionMeta := genTestCollectionMeta(collectionID, isBinary)

	var err = node.replica.addCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.NoError(t, err)

	collection, err := node.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, node.replica.getCollectionNum(), 1)

	err = node.replica.addPartition(collection.ID(), collectionMeta.PartitionIDs[0])
	assert.NoError(t, err)

	err = node.replica.addSegment(segmentID, collectionMeta.PartitionIDs[0], collectionID, segmentTypeGrowing)
	assert.NoError(t, err)
}

func initDmChannel(insertChannels []string, node *QueryNode) {
	watchReq := &querypb.WatchDmChannelsRequest{
		ChannelIDs: insertChannels,
	}
	_, err := node.WatchDmChannels(watchReq)
	if err != nil {
		panic(err)
	}
}

func initSearchChannel(searchChan string, resultChan string, node *QueryNode) {
	searchReq := &querypb.AddQueryChannelsRequest{
		RequestChannelID: searchChan,
		ResultChannelID:  resultChan,
	}
	_, err := node.AddQueryChannel(searchReq)
	if err != nil {
		panic(err)
	}
}

func newQueryNodeMock() *QueryNode {

	var ctx context.Context

	if debug {
		ctx = context.Background()
	} else {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}

	msFactory := pulsarms.NewFactory()
	svr := NewQueryNode(ctx, Params.QueryNodeID, msFactory)
	err := svr.SetQueryService(&queryServiceMock{})
	if err != nil {
		panic(err)
	}

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
}

func (q *queryServiceMock) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		InitParams: &internalpb2.InitParams{
			NodeID: int64(1),
		},
	}, nil
}

func TestMain(m *testing.M) {
	setup()
	refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

// NOTE: start pulsar and etcd before test
func TestQueryNode_Start(t *testing.T) {
	localNode := newQueryNodeMock()
	localNode.Start()
	<-localNode.queryNodeLoopCtx.Done()
	localNode.Stop()
}
