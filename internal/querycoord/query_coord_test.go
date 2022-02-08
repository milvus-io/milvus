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
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"go.uber.org/zap"
)

func setup() {
	Params.Init()
}

func refreshParams() {
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-query-Coord" + strconv.FormatInt(rand.Int63(), 10)
	Params.MsgChannelCfg.QueryNodeStats = Params.MsgChannelCfg.QueryNodeStats + suffix
	Params.MsgChannelCfg.QueryCoordTimeTick = Params.MsgChannelCfg.QueryCoordTimeTick + suffix
	Params.EtcdCfg.MetaRootPath = Params.EtcdCfg.MetaRootPath + suffix
	Params.MsgChannelCfg.RootCoordDml = "Dml"
	Params.MsgChannelCfg.RootCoordDelta = "delta"
	GlobalSegmentInfos = make(map[UniqueID]*querypb.SegmentInfo)
}

func TestMain(m *testing.M) {
	setup()
	//refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func NewQueryCoordTest(ctx context.Context, factory msgstream.Factory) (*QueryCoord, error) {
	queryCoord, err := NewQueryCoord(ctx, factory)
	if err != nil {
		return nil, err
	}
	queryCoord.newNodeFn = newQueryNodeTest
	return queryCoord, nil
}

func startQueryCoord(ctx context.Context) (*QueryCoord, error) {
	factory := msgstream.NewPmsFactory()

	coord, err := NewQueryCoordTest(ctx, factory)
	if err != nil {
		return nil, err
	}

	rootCoord := newRootCoordMock()
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)

	dataCoord, err := newDataCoordMock(ctx)
	if err != nil {
		return nil, err
	}

	indexCoord := newIndexCoordMock()

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
	coord.cluster.(*queryNodeCluster).segSizeEstimator = segSizeEstimateForTest
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
	factory := msgstream.NewPmsFactory()

	coord, err := NewQueryCoordTest(ctx, factory)
	if err != nil {
		return nil, err
	}

	rootCoord := newRootCoordMock()
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)

	dataCoord, err := newDataCoordMock(ctx)
	if err != nil {
		return nil, err
	}

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
		nodeKey := fmt.Sprintf("%s/%d", collectionMetaPrefix, 100)
		kvs[nodeKey] = string(collectionBlobs)

		err = kv.MultiSave(kvs)
		assert.Nil(t, err)

		queryCoord, err := startQueryCoord(baseCtx)
		assert.Nil(t, err)

		for {
			offlineNodeIDs := queryCoord.cluster.offlineNodeIDs()
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
		onlineNodeIDs := queryCoord.cluster.onlineNodeIDs()
		assert.Equal(t, 1, len(onlineNodeIDs))

		queryNode1.stop()
		err = removeNodeSession(nodeID)
		assert.Nil(t, err)

		waitAllQueryNodeOffline(queryCoord.cluster, onlineNodeIDs)

		queryCoord.Stop()
		err = removeAllSession()
		assert.Nil(t, err)
	})
}

func TestHandoffSegmentLoop(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	indexCoord := newIndexCoordMock()
	indexCoord.returnIndexFile = true
	queryCoord.indexCoordClient = indexCoord

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

		key := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
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
			dataCoord:              queryCoord.dataCoordClient,
			cluster:                queryCoord.cluster,
			meta:                   queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(handoffTask)
		assert.Nil(t, err)

		waitTaskFinalState(handoffTask, taskExpired)
	})

	loadCollectionTask := genLoadCollectionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)
	queryCoord.meta.setLoadType(defaultCollectionID, querypb.LoadType_loadCollection)

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
			dataCoord:              queryCoord.dataCoordClient,
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
			dataCoord:              queryCoord.dataCoordClient,
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
			dataCoord:              queryCoord.dataCoordClient,
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
			SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: handoffReq,
			dataCoord:              queryCoord.dataCoordClient,
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
			dataCoord:              queryCoord.dataCoordClient,
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
			dataCoord:              queryCoord.dataCoordClient,
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
	Params.QueryCoordCfg.BalanceIntervalSeconds = 10
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	queryCoord.cluster.(*queryNodeCluster).segmentAllocator = shuffleSegmentsToQueryNode

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(baseCtx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	partitionID := defaultPartitionID
	for {
		req := &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{partitionID},
			Schema:       genDefaultCollectionSchema(false),
		}
		baseTask := newBaseTask(baseCtx, querypb.TriggerCondition_GrpcRequest)
		loadPartitionTask := &loadPartitionTask{
			baseTask:              baseTask,
			LoadPartitionsRequest: req,
			rootCoord:             queryCoord.rootCoordClient,
			dataCoord:             queryCoord.dataCoordClient,
			indexCoord:            queryCoord.indexCoordClient,
			cluster:               queryCoord.cluster,
			meta:                  queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadPartitionTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadPartitionTask, taskExpired)
		nodeInfo, err := queryCoord.cluster.getNodeInfoByID(queryNode1.queryNodeID)
		assert.Nil(t, err)
		if nodeInfo.(*queryNode).memUsageRate >= Params.QueryCoordCfg.OverloadedMemoryThresholdPercentage {
			break
		}
		partitionID++
	}

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode2.queryNodeID)

	// if sealed has been balance to query node2, than balance work
	for {
		segmentInfos, err := queryCoord.cluster.getSegmentInfoByNode(baseCtx, queryNode2.queryNodeID, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadBalanceSegments,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Nil(t, err)
		if len(segmentInfos) > 0 {
			break
		}
	}

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}
