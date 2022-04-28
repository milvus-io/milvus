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
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/milvus-io/milvus/internal/util/dependency"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
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

//func genTestCollectionSchema(collectionID UniqueID, isBinary bool, dim int) *schemapb.CollectionSchema {
//	var fieldVec schemapb.FieldSchema
//	if isBinary {
//		fieldVec = schemapb.FieldSchema{
//			FieldID:      UniqueID(100),
//			Name:         "vec",
//			IsPrimaryKey: false,
//			DataType:     schemapb.DataType_BinaryVector,
//			TypeParams: []*commonpb.KeyValuePair{
//				{
//					Key:   "dim",
//					Value: strconv.Itoa(dim * 8),
//				},
//			},
//			IndexParams: []*commonpb.KeyValuePair{
//				{
//					Key:   "metric_type",
//					Value: "JACCARD",
//				},
//			},
//		}
//	} else {
//		fieldVec = schemapb.FieldSchema{
//			FieldID:      UniqueID(100),
//			Name:         "vec",
//			IsPrimaryKey: false,
//			DataType:     schemapb.DataType_FloatVector,
//			TypeParams: []*commonpb.KeyValuePair{
//				{
//					Key:   "dim",
//					Value: strconv.Itoa(dim),
//				},
//			},
//			IndexParams: []*commonpb.KeyValuePair{
//				{
//					Key:   "metric_type",
//					Value: "L2",
//				},
//			},
//		}
//	}
//
//	fieldInt := schemapb.FieldSchema{
//		FieldID:      UniqueID(101),
//		Name:         "age",
//		IsPrimaryKey: false,
//		DataType:     schemapb.DataType_Int32,
//	}
//
//	schema := &schemapb.CollectionSchema{
//		AutoID: true,
//		Fields: []*schemapb.FieldSchema{
//			&fieldVec, &fieldInt,
//		},
//	}
//
//	return schema
//}
//
//func genTestCollectionMeta(collectionID UniqueID, isBinary bool) *etcdpb.CollectionInfo {
//	schema := genTestCollectionSchema(collectionID, isBinary, 16)
//
//	collectionMeta := etcdpb.CollectionInfo{
//		ID:           collectionID,
//		Schema:       schema,
//		CreateTime:   Timestamp(0),
//		PartitionIDs: []UniqueID{defaultPartitionID},
//	}
//
//	return &collectionMeta
//}
//
//func genTestCollectionMetaWithPK(collectionID UniqueID, isBinary bool) *etcdpb.CollectionInfo {
//	schema := genTestCollectionSchema(collectionID, isBinary, 16)
//	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
//		FieldID:      UniqueID(0),
//		Name:         "id",
//		IsPrimaryKey: true,
//		DataType:     schemapb.DataType_Int64,
//	})
//
//	collectionMeta := etcdpb.CollectionInfo{
//		ID:           collectionID,
//		Schema:       schema,
//		CreateTime:   Timestamp(0),
//		PartitionIDs: []UniqueID{defaultPartitionID},
//	}
//
//	return &collectionMeta
//}

func initTestMeta(t *testing.T, node *QueryNode, collectionID UniqueID, segmentID UniqueID, optional ...bool) {
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	node.historical.replica.addCollection(defaultCollectionID, schema)

	collection, err := node.historical.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, node.historical.replica.getCollectionNum(), 1)

	err = node.historical.replica.addPartition(collection.ID(), defaultPartitionID)
	assert.NoError(t, err)

	err = node.historical.replica.addSegment(segmentID, defaultPartitionID, collectionID, "", segmentTypeSealed, true)
	assert.NoError(t, err)
}

func initSearchChannel(ctx context.Context, searchChan string, resultChan string, node *QueryNode) {
	searchReq := &querypb.AddQueryChannelRequest{
		QueryChannel:       searchChan,
		QueryResultChannel: resultChan,
	}
	_, err := node.AddQueryChannel(ctx, searchReq)
	if err != nil {
		panic(err)
	}
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
	streamingReplica := newCollectionReplica(etcdKV)
	historicalReplica := newCollectionReplica(etcdKV)
	svr.historical = newHistorical(svr.queryNodeLoopCtx, historicalReplica, tsReplica)
	svr.streaming = newStreaming(ctx, streamingReplica, factory, etcdKV, tsReplica)
	svr.dataSyncService = newDataSyncService(ctx, svr.streaming.replica, svr.historical.replica, tsReplica, factory)
	svr.statsService = newStatsService(ctx, svr.historical.replica, factory)
	svr.vectorStorage, err = factory.NewVectorStorageChunkManager(ctx)
	if err != nil {
		panic(err)
	}
	svr.cacheStorage, err = factory.NewCacheStorageChunkManager(ctx)
	if err != nil {
		panic(err)
	}
	svr.loader = newSegmentLoader(svr.historical.replica, svr.streaming.replica, etcdKV, svr.vectorStorage, factory)
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
	u, err := url.Parse("http://localhost:2389")
	if err != nil {
		return nil, err
	}
	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:2390")
	if err != nil {
		return nil, err
	}
	config.LPUrls = []url.URL{*u}

	return embed.StartEtcd(config)
}

func TestMain(m *testing.M) {
	setup()
	Params.CommonCfg.QueryNodeStats = Params.CommonCfg.QueryNodeStats + strconv.Itoa(rand.Int())
	// init embed etcd
	var err error
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
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		err = node.removeSegments(genSimpleChangeInfo())
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("test cleanup segments no segment", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNodeToTestWatchChangeInfo(ctx)
		assert.NoError(t, err)

		err = node.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		segmentChangeInfos := genSimpleChangeInfo()
		segmentChangeInfos.Infos[0].OnlineSegments = nil
		segmentChangeInfos.Infos[0].OfflineNodeID = Params.QueryNodeCfg.GetNodeID()

		/*
			qc, err := node.queryService.getQueryCollection(defaultCollectionID)
			assert.NoError(t, err)
			qc.globalSegmentManager.removeGlobalSealedSegmentInfo(defaultSegmentID)
		*/
		err = node.removeSegments(segmentChangeInfos)
		assert.Error(t, err)
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

		err = node.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

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

func TestQueryNode_watchService(t *testing.T) {
	t.Run("watch channel closed", func(t *testing.T) {
		ech := make(chan *sessionutil.SessionEvent)
		qn := &QueryNode{
			session: &sessionutil.Session{
				TriggerKill: true,
				ServerID:    0,
			},
			wg:                  sync.WaitGroup{},
			eventCh:             ech,
			queryNodeLoopCancel: func() {},
		}
		flag := false
		closed := false

		sigDone := make(chan struct{}, 1)
		sigQuit := make(chan struct{}, 1)
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGINT)

		defer signal.Reset(syscall.SIGINT)

		qn.wg.Add(1)

		go func() {
			qn.watchService(context.Background())
			flag = true
			sigDone <- struct{}{}
		}()
		go func() {
			<-sc
			closed = true
			sigQuit <- struct{}{}
		}()

		close(ech)
		<-sigDone
		<-sigQuit
		assert.True(t, flag)
		assert.True(t, closed)
	})

	t.Run("context done", func(t *testing.T) {
		ech := make(chan *sessionutil.SessionEvent)
		qn := &QueryNode{
			session: &sessionutil.Session{
				TriggerKill: true,
				ServerID:    0,
			},
			wg:      sync.WaitGroup{},
			eventCh: ech,
		}
		flag := false

		sigDone := make(chan struct{}, 1)
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGINT)

		defer signal.Reset(syscall.SIGINT)

		qn.wg.Add(1)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			qn.watchService(ctx)
			flag = true
			sigDone <- struct{}{}
		}()

		assert.False(t, flag)
		cancel()
		<-sigDone
		assert.True(t, flag)
	})
}

func TestQueryNode_validateChangeChannel(t *testing.T) {

	type testCase struct {
		name                string
		info                *querypb.SegmentChangeInfo
		expectedError       bool
		expectedChannelName string
	}

	cases := []testCase{
		{
			name:          "empty info",
			info:          &querypb.SegmentChangeInfo{},
			expectedError: true,
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
			expectedError:       false,
			expectedChannelName: defaultDMLChannel,
		},
		{
			name: "empty offline change info",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
			},
			expectedError:       false,
			expectedChannelName: defaultDMLChannel,
		},
		{
			name: "empty online change info",
			info: &querypb.SegmentChangeInfo{
				OfflineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
				},
			},
			expectedError:       false,
			expectedChannelName: defaultDMLChannel,
		},
		{
			name: "different channel in online",
			info: &querypb.SegmentChangeInfo{
				OnlineSegments: []*querypb.SegmentInfo{
					{DmChannel: defaultDMLChannel},
					{DmChannel: "other_channel"},
				},
			},
			expectedError: true,
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
			expectedError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			channelName, err := validateChangeChannel(tc.info)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedChannelName, channelName)
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

	t.Run("bad change info", func(t *testing.T) {
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
