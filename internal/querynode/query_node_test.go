// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

import (
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/dependency"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

var embedetcdServer *embed.Etcd

// mock of query coordinator client
type queryCoordMock struct {
	types.QueryCoord
}

func setup() {
	os.Setenv("QUERY_NODE_ID", "1")
	Params.Init()
	Params.EtcdCfg.MetaRootPath = "/etcd/test/root/querynode"
}

func initTestMeta(t *testing.T, node *QueryNode, collectionID UniqueID, segmentID UniqueID, optional ...bool) {
	schema := genTestCollectionSchema()

	node.metaReplica.addCollection(defaultCollectionID, schema)

	collection, err := node.metaReplica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, node.metaReplica.getCollectionNum(), 1)

	err = node.metaReplica.addPartition(collection.ID(), defaultPartitionID)
	assert.NoError(t, err)

	err = node.metaReplica.addSegment(segmentID, defaultPartitionID, collectionID, "", defaultSegmentVersion, segmentTypeSealed)
	assert.NoError(t, err)
}

func newQueryNodeMock() *QueryNode {

	var ctx context.Context

	if debugUT {
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
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		panic(err)
	}
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	factory := newMessageStreamFactory()
	svr := NewQueryNode(ctx, factory)
	tsReplica := newTSafeReplica()

	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0))
	if err != nil {
		panic(err)
	}

	replica := newCollectionReplica(pool)
	svr.metaReplica = replica
	svr.dataSyncService = newDataSyncService(ctx, svr.metaReplica, tsReplica, factory)
	svr.vectorStorage, err = factory.NewVectorStorageChunkManager(ctx)
	if err != nil {
		panic(err)
	}
	svr.cacheStorage, err = factory.NewCacheStorageChunkManager(ctx)
	if err != nil {
		panic(err)
	}
	svr.loader = newSegmentLoader(svr.metaReplica, etcdKV, svr.vectorStorage, factory, pool)
	svr.etcdKV = etcdKV

	return svr
}

func newMessageStreamFactory() dependency.Factory {
	return dependency.NewDefaultFactory(true)
}

func startEmbedEtcdServer() (*embed.Etcd, error) {
	dir, err := ioutil.TempDir(os.TempDir(), "milvus_ut")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	config := embed.NewConfig()

	config.Dir = os.TempDir()
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:8989")
	if err != nil {
		return nil, err
	}
	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:8990")
	if err != nil {
		return nil, err
	}
	config.LPUrls = []url.URL{*u}

	return embed.StartEtcd(config)
}

func TestMain(m *testing.M) {
	setup()
	var err error
	rateCol, err = newRateCollector()
	if err != nil {
		panic("init test failed, err = " + err.Error())
	}
	// init embed etcd
	embedetcdServer, err = startEmbedEtcdServer()
	if err != nil {
		os.Exit(1)
	}
	defer embedetcdServer.Close()
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

func TestQueryNode_register(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	etcdcli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdcli.Close()
	node.SetEtcdClient(etcdcli)
	err = node.initSession()
	assert.NoError(t, err)

	node.session.TriggerKill = false
	err = node.Register()
	assert.NoError(t, err)
}

func TestQueryNode_init(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)
	etcdcli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdcli.Close()
	node.SetEtcdClient(etcdcli)
	err = node.Init()
	assert.Nil(t, err)
}

func genSimpleQueryNodeToTestWatchChangeInfo(ctx context.Context) (*QueryNode, error) {
	node, err := genSimpleQueryNode(ctx)
	if err != nil {
		return nil, err
	}

	/*
		err = node.queryService.addQueryCollection(defaultCollectionID)
		if err != nil {
			return nil, err
		}

		qc, err := node.queryService.getQueryCollection(defaultCollectionID)
		if err != nil {
			return nil, err
		}*/
	//qc.globalSegmentManager.addGlobalSegmentInfo(genSimpleSegmentInfo())
	return node, nil
}

func TestQueryNode_adjustByChangeInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test cleanup segments", func(t *testing.T) {
		defer wg.Done()
		_, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

	})

	wg.Add(1)
	t.Run("test cleanup segments no segment", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)
		segmentChangeInfos := genSimpleChangeInfo()
		segmentChangeInfos.Infos[0].OnlineSegments = nil
		segmentChangeInfos.Infos[0].OfflineNodeID = Params.QueryNodeCfg.GetNodeID()

		/*
			qc, err := node.queryService.getQueryCollection(defaultCollectionID)
			assert.NoError(t, err)
			qc.globalSegmentManager.removeGlobalSealedSegmentInfo(defaultSegmentID)
		*/

	})
	wg.Wait()
}

func TestQueryNode_watchChangeInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test watchChangeInfo", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		go node.watchChangeInfo()

		info := genSimpleSegmentInfo()
		value, err := proto.Marshal(info)
		assert.NoError(t, err)
		err = saveChangeInfo("0", string(value))
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("test watchChangeInfo key error", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		go node.watchChangeInfo()

		err = saveChangeInfo("*$&#%^^", "%EUY%&#^$%&@")
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("test watchChangeInfo unmarshal error", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		go node.watchChangeInfo()

		err = saveChangeInfo("0", "$%^$*&%^#$&*")
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("test watchChangeInfo adjustByChangeInfo error", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		segmentChangeInfos := genSimpleChangeInfo()
		segmentChangeInfos.Infos[0].OnlineSegments = nil
		segmentChangeInfos.Infos[0].OfflineNodeID = Params.QueryNodeCfg.GetNodeID()

		/*
			qc, err := node.queryService.getQueryCollection(defaultCollectionID)
			assert.NoError(t, err)
		qc.globalSegmentManager.removeGlobalSealedSegmentInfo(defaultSegmentID)*/

		go node.watchChangeInfo()

		value, err := proto.Marshal(segmentChangeInfos)
		assert.NoError(t, err)
		err = saveChangeInfo("0", string(value))
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
	})
	wg.Wait()
}

func TestQueryNode_splitChangeChannel(t *testing.T) {

	type testCase struct {
		name           string
		info           *querypb.SegmentChangeInfo
		expectedResult map[string]*querypb.SegmentChangeInfo
	}

	cases := []testCase{
		{
			name:           "empty info",
			info:           &querypb.SegmentChangeInfo{},
			expectedResult: map[string]*querypb.SegmentChangeInfo{},
		},
		{
			name: "normal segment change info",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
				OfflineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
			},
			expectedResult: map[string]*querypb.SegmentChangeInfo{
				defaultDMLChannel: {
					OnlineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
					OfflineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
				},
			},
		},
		{
			name: "empty offline change info",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
			},
			expectedResult: map[string]*querypb.SegmentChangeInfo{
				defaultDMLChannel: {
					OnlineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
				},
			},
		},
		{
			name: "empty online change info",
			info: &querypb.SegmentChangeInfo{
				OfflineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
			},
			expectedResult: map[string]*querypb.SegmentChangeInfo{
				defaultDMLChannel: {
					OfflineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
				},
			},
		},
		{
			name: "different channel in online",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
					{DmChannel: "other_channel"},
				},
			},
			expectedResult: map[string]*querypb.SegmentChangeInfo{
				defaultDMLChannel: {
					OnlineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
				},
				"other_channel": {
					OnlineSegments: []*querypb.SegmentInfo{
						{DmChannel: "other_channel"},
					},
				},
			},
		},
		{
			name: "different channel in offline",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
				OfflineSegments: []*querypb.SegmentInfo{
					{DmChannel: "other_channel"},
				},
			},
			expectedResult: map[string]*querypb.SegmentChangeInfo{
				defaultDMLChannel: {
					OnlineSegments: []*querypb.SegmentInfo{
						{DmChannel: defaultDMLChannel},
					},
				},
				"other_channel": {
					OfflineSegments: []*querypb.SegmentInfo{
						{DmChannel: "other_channel"},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := splitSegmentsChange(tc.info)
			assert.Equal(t, len(tc.expectedResult), len(result))
			for k, v := range tc.expectedResult {
				r := assert.True(t, proto.Equal(v, result[k]))

				if !r {
					t.Log(v)
					t.Log(result[k])
				}
			}
		})
	}
}

func TestQueryNode_handleSealedSegmentsChangeInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	qn, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)

	t.Run("empty info", func(t *testing.T) {
		assert.NotPanics(t, func() {
			qn.handleSealedSegmentsChangeInfo(&querypb.SealedSegmentsChangeInfo{})
		})
		assert.NotPanics(t, func() {
			qn.handleSealedSegmentsChangeInfo(nil)
		})
	})

	t.Run("normal segment change info", func(t *testing.T) {
		assert.NotPanics(t, func() {
			qn.handleSealedSegmentsChangeInfo(&querypb.SealedSegmentsChangeInfo{
				Infos: []*querypb.SegmentChangeInfo{
					{
						OnlineSegments: []*querypb.SegmentInfo{
							{DmChannel: defaultDMLChannel},
						},
						OfflineSegments: []*querypb.SegmentInfo{
							{DmChannel: defaultDMLChannel},
						},
					},
				},
			})
		})
	})

	t.Run("multple vchannel change info", func(t *testing.T) {
		assert.NotPanics(t, func() {
			qn.handleSealedSegmentsChangeInfo(&querypb.SealedSegmentsChangeInfo{
				Infos: []*querypb.SegmentChangeInfo{
					{
						OnlineSegments: []*querypb.SegmentInfo{
							{DmChannel: defaultDMLChannel},
						},
						OfflineSegments: []*querypb.SegmentInfo{
							{DmChannel: "other_channel"},
						},
					},
				},
			})
		})

	})
}
