// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
)

const ctxTimeInMillisecond = 5000
const debug = false

const defaultPartitionID = UniqueID(2021)

type queryCoordMock struct {
	types.QueryCoord
}

func setup() {
	os.Setenv("QUERY_NODE_ID", "1")
	Params.Init()
	//Params.QueryNodeID = 1
	Params.initQueryTimeTickChannelName()
	Params.initStatsChannelName()
	Params.MetaRootPath = "/etcd/test/root/querynode"
}

func genTestCollectionMeta(collectionID UniqueID, isBinary bool) *etcdpb.CollectionInfo {
	var fieldVec schemapb.FieldSchema
	if isBinary {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(100),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_BinaryVector,
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
			DataType:     schemapb.DataType_FloatVector,
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
		DataType:     schemapb.DataType_Int32,
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

	var err = node.historical.replica.addCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.NoError(t, err)

	collection, err := node.historical.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, node.historical.replica.getCollectionNum(), 1)

	err = node.historical.replica.addPartition(collection.ID(), collectionMeta.PartitionIDs[0])
	assert.NoError(t, err)

	err = node.historical.replica.addSegment(segmentID, collectionMeta.PartitionIDs[0], collectionID, "", segmentTypeGrowing, true)
	assert.NoError(t, err)
}

func initSearchChannel(ctx context.Context, searchChan string, resultChan string, node *QueryNode) {
	searchReq := &querypb.AddQueryChannelRequest{
		RequestChannelID: searchChan,
		ResultChannelID:  resultChan,
	}
	_, err := node.AddQueryChannel(ctx, searchReq)
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

	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
	if err != nil {
		panic(err)
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)

	msFactory, err := newMessageStreamFactory()
	if err != nil {
		panic(err)
	}
	svr := NewQueryNode(ctx, msFactory)
	svr.historical = newHistorical(svr.queryNodeLoopCtx, nil, nil, svr.msFactory, etcdKV)
	svr.streaming = newStreaming(ctx, msFactory, etcdKV)
	svr.etcdKV = etcdKV

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
	Params.StatsChannelName = Params.StatsChannelName + suffix
}

func newMessageStreamFactory() (msgstream.Factory, error) {
	const receiveBufSize = 1024

	pulsarURL := Params.PulsarAddress
	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	return msFactory, err
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
