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

package querycoord

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

var queryCoordTestDir = "/tmp/milvus_test/query_coord"

func setup() {
	Params.Init()
}

func refreshParams() {
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-query-Coord" + strconv.FormatInt(rand.Int63(), 10)
	Params.CommonCfg.QueryNodeStats = Params.CommonCfg.QueryNodeStats + suffix
	Params.CommonCfg.QueryCoordTimeTick = Params.CommonCfg.QueryCoordTimeTick + suffix
	Params.EtcdCfg.MetaRootPath = Params.EtcdCfg.MetaRootPath + suffix
	GlobalSegmentInfos = make(map[UniqueID]*querypb.SegmentInfo)
}

func TestMain(m *testing.M) {
	setup()
	//refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func NewQueryCoordTest(ctx context.Context, factory dependency.Factory) (*QueryCoord, error) {
	queryCoord, err := NewQueryCoord(ctx, factory)
	if err != nil {
		return nil, err
	}
	queryCoord.newNodeFn = newQueryNodeTest
	return queryCoord, nil
}

func startQueryCoord(ctx context.Context) (*QueryCoord, error) {
	factory := dependency.NewDefaultFactory(true)

	coord, err := NewQueryCoordTest(ctx, factory)
	if err != nil {
		return nil, err
	}

	rootCoord := newRootCoordMock(ctx)
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)

	dataCoord := newDataCoordMock(ctx)
	indexCoord, err := newIndexCoordMock(queryCoordTestDir)
	if err != nil {
		return nil, err
	}

	coord.SetRootCoord(rootCoord)
	coord.SetDataCoord(dataCoord)
	coord.SetIndexCoord(indexCoord)
	etcd, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	coord.SetEtcdClient(etcd)
	err = coord.Init()
	if err != nil {
		return nil, err
	}
	err = coord.Start()
	if err != nil {
		return nil, err
	}
	err = coord.Register()
	if err != nil {
		return nil, err
	}
	return coord, nil
}

func createDefaultPartition(ctx context.Context, queryCoord *QueryCoord) error {
	_, err := queryCoord.rootCoordClient.CreatePartition(ctx, nil)
	return err
}

func startUnHealthyQueryCoord(ctx context.Context) (*QueryCoord, error) {
	factory := dependency.NewDefaultFactory(true)

	coord, err := NewQueryCoordTest(ctx, factory)
	if err != nil {
		return nil, err
	}

	rootCoord := newRootCoordMock(ctx)
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)
	dataCoord := newDataCoordMock(ctx)

	coord.SetRootCoord(rootCoord)
	coord.SetDataCoord(dataCoord)
	etcd, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	coord.SetEtcdClient(etcd)
	err = coord.Init()
	if err != nil {
		return nil, err
	}
	err = coord.Register()
	if err != nil {
		return nil, err
	}

	return coord, nil
}

func TestWatchNodeLoop(t *testing.T) {
	baseCtx := context.Background()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	t.Run("Test OfflineNodes", func(t *testing.T) {
		refreshParams()

		kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

		kvs := make(map[string]string)
		session := &sessionutil.Session{
			ServerID: 100,
			Address:  "localhost",
		}
		sessionBlob, err := json.Marshal(session)
		assert.Nil(t, err)
		sessionKey := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, 100)
		kvs[sessionKey] = string(sessionBlob)

		collectionInfo := &querypb.CollectionInfo{
			CollectionID: defaultCollectionID,
		}
		collectionBlobs, err := proto.Marshal(collectionInfo)
		assert.Nil(t, err)
		nodeKey := fmt.Sprintf("%s/%d", collectionMetaPrefix, defaultCollectionID)
		kvs[nodeKey] = string(collectionBlobs)

		err = kv.MultiSave(kvs)
		assert.Nil(t, err)

		queryCoord, err := startQueryCoord(baseCtx)
		assert.Nil(t, err)

		for {
			offlineNodeIDs := queryCoord.cluster.OfflineNodeIDs()
			if len(offlineNodeIDs) != 0 {
				log.Warn("find offline Nodes", zap.Int64s("offlineNodeIDs", offlineNodeIDs))
				break
			}
			// if session id not exist, means querycoord already handled it and remove
			_, err = kv.Load(sessionKey)
			if err != nil {
				log.Warn("already handled by querycoord", zap.Error(err))
				break
			}
			time.Sleep(time.Duration(1) * time.Second)
		}

		queryCoord.Stop()
		err = removeAllSession()
		assert.Nil(t, err)
	})

	t.Run("Test RegisterNewNode", func(t *testing.T) {
		refreshParams()
		queryCoord, err := startQueryCoord(baseCtx)
		assert.Nil(t, err)

		queryNode1, err := startQueryNodeServer(baseCtx)
		assert.Nil(t, err)

		nodeID := queryNode1.queryNodeID
		waitQueryNodeOnline(queryCoord.cluster, nodeID)

		queryCoord.Stop()
		queryNode1.stop()
		err = removeAllSession()
		assert.Nil(t, err)
	})

	t.Run("Test RemoveNode", func(t *testing.T) {
		refreshParams()
		queryNode1, err := startQueryNodeServer(baseCtx)
		assert.Nil(t, err)

		queryCoord, err := startQueryCoord(baseCtx)
		assert.Nil(t, err)

		nodeID := queryNode1.queryNodeID
		waitQueryNodeOnline(queryCoord.cluster, nodeID)
		onlineNodeIDs := queryCoord.cluster.OnlineNodeIDs()
		assert.Equal(t, 1, len(onlineNodeIDs))

		queryNode1.stop()
		err = removeNodeSession(nodeID)
		assert.Nil(t, err)

		waitAllQueryNodeOffline(queryCoord.cluster, onlineNodeIDs...)

		queryCoord.Stop()
		err = removeAllSession()
		assert.Nil(t, err)
	})
}

func TestHandleNodeEventClosed(t *testing.T) {
	ech := make(chan *sessionutil.SessionEvent)
	qc := &QueryCoord{
		eventChan: ech,
		session: &sessionutil.Session{
			TriggerKill: true,
			ServerID:    0,
		},
	}
	flag := false
	closed := false

	sigDone := make(chan struct{}, 1)
	sigQuit := make(chan struct{}, 1)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)

	defer signal.Reset(syscall.SIGINT)

	go func() {
		qc.handleNodeEvent(context.Background())
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
}

func TestHandoffSegmentLoop(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	rootCoord := queryCoord.rootCoordClient.(*rootCoordMock)
	rootCoord.enableIndex = true

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)

	t.Run("Test watchHandoffLoop", func(t *testing.T) {
		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentState: commonpb.SegmentState_Sealed,
		}

		key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
		value, err := proto.Marshal(segmentInfo)
		assert.Nil(t, err)
		err = queryCoord.kvClient.Save(key, string(value))
		assert.Nil(t, err)
		for {
			_, err := queryCoord.kvClient.Load(key)
			if err != nil {
				break
			}
		}
	})

	loadPartitionTask := genLoadPartitionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadPartitionTask, taskExpired)

	t.Run("Test partitionNotLoaded", func(t *testing.T) {
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)
		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID + 1,
			SegmentState: commonpb.SegmentState_Sealed,
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskExpired)
	})

	// genReleaseCollectionTask(baseCtx, queryCoord)
	queryCoord.meta.releaseCollection(defaultCollectionID)
	loadCollectionTask := genLoadCollectionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	t.Run("Test handoffGrowingSegment", func(t *testing.T) {
		infos := queryCoord.meta.showSegmentInfos(defaultCollectionID, nil)
		assert.NotEqual(t, 0, len(infos))
		segmentID := defaultSegmentID + 4
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)

		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    segmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID + 2,
			SegmentState: commonpb.SegmentState_Sealed,
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskExpired)
	})

	t.Run("Test binlogNotExist", func(t *testing.T) {
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)
		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID + 100,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentState: commonpb.SegmentState_Sealed,
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskFailed)
	})

	t.Run("Test sealedSegmentExist", func(t *testing.T) {
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)
		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentState: commonpb.SegmentState_Sealed,
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskFailed)
	})

	t.Run("Test handoffCompactionSegment", func(t *testing.T) {
		infos := queryCoord.meta.showSegmentInfos(defaultCollectionID, nil)
		assert.NotEqual(t, 0, len(infos))
		segmentID := defaultSegmentID + 5
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)

		segmentInfo := &querypb.SegmentInfo{
			SegmentID:      segmentID,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID + 2,
			SegmentState:   commonpb.SegmentState_Sealed,
			CompactionFrom: []UniqueID{defaultSegmentID, defaultSegmentID + 1},
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos:     []*querypb.SegmentInfo{segmentInfo},
			ReleasedSegments: []int64{defaultSegmentID, defaultSegmentID + 1},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskExpired)

		_, err = queryCoord.meta.getSegmentInfoByID(segmentID)
		assert.Nil(t, err)
		_, err = queryCoord.meta.getSegmentInfoByID(defaultSegmentID)
		assert.NotNil(t, err)
		_, err = queryCoord.meta.getSegmentInfoByID(defaultSegmentID + 1)
		assert.NotNil(t, err)
	})

	t.Run("Test handoffCompactionSegmentNotExist", func(t *testing.T) {
		infos := queryCoord.meta.showSegmentInfos(defaultCollectionID, nil)
		assert.NotEqual(t, 0, len(infos))
		segmentID := defaultSegmentID + 6
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)

		segmentInfo := &querypb.SegmentInfo{
			SegmentID:      segmentID,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID + 2,
			SegmentState:   commonpb.SegmentState_Sealed,
			CompactionFrom: []UniqueID{defaultSegmentID + 2, defaultSegmentID + 100},
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskFailed)

		_, err = queryCoord.meta.getSegmentInfoByID(segmentID)
		assert.NotNil(t, err)
		_, err = queryCoord.meta.getSegmentInfoByID(defaultSegmentID + 2)
		assert.Nil(t, err)
		_, err = queryCoord.meta.getSegmentInfoByID(defaultSegmentID + 100)
		assert.NotNil(t, err)
	})

	releasePartitionTask := genReleasePartitionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(releasePartitionTask)
	assert.Nil(t, err)
	waitTaskFinalState(releasePartitionTask, taskExpired)

	t.Run("Test handoffReleasedPartition", func(t *testing.T) {
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_Handoff)
		segmentInfo := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID,
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentState: commonpb.SegmentState_Sealed,
		}
		handoffReq := &querypb.HandoffSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_HandoffSegments,
			},
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			broker:                 queryCoord.broker,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskExpired)
	})

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceSegmentLoop(t *testing.T) {
	refreshParams()
	defer removeAllSession()

	Params.QueryCoordCfg.BalanceIntervalSeconds = 10
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	queryCoord.cluster.(*queryNodeCluster).segmentAllocator = shuffleSegmentsToQueryNode
	defer queryCoord.Stop()

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)
	defer queryNode1.stop()

	loadCollectionTask := genLoadCollectionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	memory := uint64(1 << 30)
	queryNode1.getMetrics = func() (*milvuspb.GetMetricsResponse, error) {
		nodeInfo := metricsinfo.QueryNodeInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				HardwareInfos: metricsinfo.HardwareMetrics{
					Memory:      1 << 30,
					MemoryUsage: memory - memory/10, // >= memory*0.9
				},
			},
		}

		nodeInfoResp, err := metricsinfo.MarshalComponentInfos(nodeInfo)
		return &milvuspb.GetMetricsResponse{
			Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Response: nodeInfoResp,
		}, err
	}
	nodeInfo, err := queryCoord.cluster.GetNodeInfoByID(queryNode1.queryNodeID)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, nodeInfo.(*queryNode).memUsageRate, Params.QueryCoordCfg.OverloadedMemoryThresholdPercentage)

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode2.queryNodeID)
	defer queryNode2.stop()

	// if sealed has been balance to query node2, than balance work
	for {
		segmentInfos, err := queryCoord.cluster.GetSegmentInfoByNode(baseCtx, queryNode2.queryNodeID, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadBalanceSegments,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Nil(t, err)
		if len(segmentInfos) > 0 {
			queryNode1.getMetrics = returnSuccessGetMetricsResult
			break
		}

		time.Sleep(time.Second)
	}
}

func TestQueryCoord_watchHandoffSegmentLoop(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	broker, _, _, err := getMockGlobalMetaBroker(ctx)
	require.NoError(t, err)
	scheduler, err := newTaskScheduler(ctx, nil, nil, etcdKV, broker, func() (UniqueID, error) {
		return 1, nil
	})
	require.NoError(t, err)

	qc := &QueryCoord{
		loopCtx:  ctx,
		loopWg:   sync.WaitGroup{},
		kvClient: etcdKV,
		handoffHandler: &HandoffHandler{
			ctx:       ctx,
			cancel:    cancel,
			client:    etcdKV,
			scheduler: scheduler,
		},
		scheduler: scheduler,
	}

	t.Run("chan closed", func(t *testing.T) {
		qc.loopWg.Add(1)
		go func() {
			assert.Panics(t, func() {
				qc.handoffNotificationLoop()
			})
		}()

		etcdCli.Close()
		qc.loopWg.Wait()
	})

	t.Run("etcd compaction", func(t *testing.T) {
		etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
		assert.Nil(t, err)
		etcdKV = etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
		qc.kvClient = etcdKV
		qc.handoffHandler.client = etcdKV
		qc.handoffHandler.revision = 0
		qc.meta = &MetaReplica{}
		qc.handoffHandler.meta = qc.meta
		qc.handoffHandler.tasks = make(map[int64]*HandOffTask)

		for i := 1; i < 10; i++ {
			segInfo := &querypb.SegmentInfo{
				SegmentID: UniqueID(i),
			}
			v, err := proto.Marshal(segInfo)
			assert.Nil(t, err)
			key := path.Join(util.HandoffSegmentPrefix, strconv.Itoa(i))
			err = etcdKV.Save(key, string(v))
			assert.Nil(t, err)
		}
		// The reason there the error is no handle is that if you run compact twice, an error will be reported;
		// error msg is "etcdserver: mvcc: required revision has been compacted"
		etcdCli.Compact(ctx, 10)
		qc.loopWg.Add(1)
		go qc.handoffNotificationLoop()

		time.Sleep(2 * time.Second)
		for i := 1; i < 10; i++ {
			k := path.Join(util.HandoffSegmentPrefix, strconv.Itoa(i))
			err = etcdKV.Remove(k)
			assert.Nil(t, err)
		}
		cancel()
		qc.loopWg.Wait()
	})

	t.Run("etcd compaction and reload failed", func(t *testing.T) {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		etcdCli, err = etcd.GetEtcdClient(&Params.EtcdCfg)
		assert.Nil(t, err)
		etcdKV = etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
		qc.loopCtx = ctx
		qc.loopCancel = cancel
		qc.kvClient = etcdKV
		qc.handoffHandler.client = etcdKV
		qc.handoffHandler.revision = 0
		qc.handoffHandler.tasks = make(map[int64]*HandOffTask)

		for i := 1; i < 10; i++ {
			key := path.Join(util.HandoffSegmentPrefix, strconv.Itoa(i))
			v := "segment-" + strconv.Itoa(i)
			err = etcdKV.Save(key, v)
			assert.Nil(t, err)
		}
		// The reason there the error is no handle is that if you run compact twice, an error will be reported;
		// error msg is "etcdserver: mvcc: required revision has been compacted"
		etcdCli.Compact(ctx, 10)
		qc.loopWg.Add(1)
		go func() {
			assert.Panics(t, func() {
				qc.handoffNotificationLoop()
			})
		}()
		qc.loopWg.Wait()

		for i := 1; i < 10; i++ {
			k := path.Join(util.HandoffSegmentPrefix, strconv.Itoa(i))
			err = etcdKV.Remove(k)
			assert.Nil(t, err)
		}
	})
}
