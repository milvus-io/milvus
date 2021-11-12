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

package querycoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	handoffSegmentPrefix = "querycoord-handoff"
)

// Timestamp is an alias for the Int64 type
type Timestamp = typeutil.Timestamp

type queryChannelInfo struct {
	requestChannel  string
	responseChannel string
}

// QueryCoord is the coordinator of queryNodes
type QueryCoord struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc
	loopWg     sync.WaitGroup
	kvClient   *etcdkv.EtcdKV

	initOnce sync.Once

	queryCoordID uint64
	meta         Meta
	cluster      Cluster
	newNodeFn    newQueryNodeFn
	scheduler    *TaskScheduler
	idAllocator  func() (UniqueID, error)

	metricsCacheManager *metricsinfo.MetricsCacheManager

	dataCoordClient  types.DataCoord
	rootCoordClient  types.RootCoord
	indexCoordClient types.IndexCoord

	session   *sessionutil.Session
	eventChan <-chan *sessionutil.SessionEvent

	stateCode  atomic.Value
	enableGrpc bool

	msFactory msgstream.Factory
}

// Register register query service at etcd
func (qc *QueryCoord) Register() error {
	log.Debug("query coord session info", zap.String("metaPath", Params.MetaRootPath), zap.Strings("etcdEndPoints", Params.EtcdEndpoints), zap.String("address", Params.Address))
	qc.session = sessionutil.NewSession(qc.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	qc.session.Init(typeutil.QueryCoordRole, Params.Address, true)
	Params.NodeID = uint64(qc.session.ServerID)
	Params.SetLogger(typeutil.UniqueID(-1))
	return nil
}

// Init function initializes the queryCoord's meta, cluster, etcdKV and task scheduler
func (qc *QueryCoord) Init() error {
	log.Debug("query coordinator start init")
	//connect etcd
	connectEtcdFn := func() error {
		etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		if err != nil {
			return err
		}
		qc.kvClient = etcdKV
		return nil
	}
	var initError error = nil
	qc.initOnce.Do(func() {
		log.Debug("query coordinator try to connect etcd")
		initError = retry.Do(qc.loopCtx, connectEtcdFn, retry.Attempts(300))
		if initError != nil {
			log.Debug("query coordinator try to connect etcd failed", zap.Error(initError))
			return
		}
		log.Debug("query coordinator try to connect etcd success")

		// init id allocator
		var idAllocatorKV *etcdkv.EtcdKV
		idAllocatorKV, initError = tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "queryCoordTaskID")
		if initError != nil {
			return
		}
		idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
		initError = idAllocator.Initialize()
		if initError != nil {
			log.Debug("query coordinator idAllocator initialize failed", zap.Error(initError))
			return
		}
		qc.idAllocator = func() (UniqueID, error) {
			return idAllocator.AllocOne()
		}

		// init meta
		qc.meta, initError = newMeta(qc.loopCtx, qc.kvClient, qc.msFactory, qc.idAllocator)
		if initError != nil {
			log.Error("query coordinator init meta failed", zap.Error(initError))
			return
		}

		// init cluster
		qc.cluster, initError = newQueryNodeCluster(qc.loopCtx, qc.meta, qc.kvClient, qc.newNodeFn, qc.session)
		if initError != nil {
			log.Error("query coordinator init cluster failed", zap.Error(initError))
			return
		}

		// init task scheduler
		qc.scheduler, initError = NewTaskScheduler(qc.loopCtx, qc.meta, qc.cluster, qc.kvClient, qc.rootCoordClient, qc.dataCoordClient, qc.idAllocator)
		if initError != nil {
			log.Error("query coordinator init task scheduler failed", zap.Error(initError))
			return
		}

		qc.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	})

	return initError
}

// Start function starts the goroutines to watch the meta and node updates
func (qc *QueryCoord) Start() error {
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err := qc.msFactory.SetParams(m)
	if err != nil {
		return err
	}
	qc.scheduler.Start()
	log.Debug("start scheduler ...")

	Params.CreatedTime = time.Now()
	Params.UpdatedTime = time.Now()

	qc.UpdateStateCode(internalpb.StateCode_Healthy)

	qc.loopWg.Add(1)
	go qc.watchNodeLoop()

	qc.loopWg.Add(1)
	go qc.watchHandoffSegmentLoop()

	go qc.session.LivenessCheck(qc.loopCtx, func() {
		log.Error("Query Coord disconnected from etcd, process will exit", zap.Int64("Server Id", qc.session.ServerID))
		if err := qc.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
	})

	return nil
}

// Stop function stops watching the meta and node updates
func (qc *QueryCoord) Stop() error {
	qc.scheduler.Close()
	log.Debug("close scheduler ...")
	qc.loopCancel()
	qc.UpdateStateCode(internalpb.StateCode_Abnormal)

	qc.loopWg.Wait()
	return nil
}

// UpdateStateCode updates the status of the coord, including healthy, unhealthy
func (qc *QueryCoord) UpdateStateCode(code internalpb.StateCode) {
	qc.stateCode.Store(code)
}

// NewQueryCoord creates a QueryCoord object.
func NewQueryCoord(ctx context.Context, factory msgstream.Factory) (*QueryCoord, error) {
	rand.Seed(time.Now().UnixNano())
	queryChannels := make([]*queryChannelInfo, 0)
	channelID := len(queryChannels)
	searchPrefix := Params.SearchChannelPrefix
	searchResultPrefix := Params.SearchResultChannelPrefix
	allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(int64(channelID), 10)

	queryChannels = append(queryChannels, &queryChannelInfo{
		requestChannel:  allocatedQueryChannel,
		responseChannel: allocatedQueryResultChannel,
	})

	ctx1, cancel := context.WithCancel(ctx)
	service := &QueryCoord{
		loopCtx:    ctx1,
		loopCancel: cancel,
		msFactory:  factory,
		newNodeFn:  newQueryNode,
	}

	service.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("query coordinator", zap.Any("queryChannels", queryChannels))
	return service, nil
}

// SetRootCoord sets root coordinator's client
func (qc *QueryCoord) SetRootCoord(rootCoord types.RootCoord) error {
	if rootCoord == nil {
		return errors.New("null root coordinator interface")
	}

	qc.rootCoordClient = rootCoord
	return nil
}

// SetDataCoord sets data coordinator's client
func (qc *QueryCoord) SetDataCoord(dataCoord types.DataCoord) error {
	if dataCoord == nil {
		return errors.New("null data coordinator interface")
	}

	qc.dataCoordClient = dataCoord
	return nil
}

func (qc *QueryCoord) SetIndexCoord(indexCoord types.IndexCoord) error {
	if indexCoord == nil {
		return errors.New("null index coordinator interface")
	}

	qc.indexCoordClient = indexCoord
	return nil
}

func (qc *QueryCoord) watchNodeLoop() {
	ctx, cancel := context.WithCancel(qc.loopCtx)
	defer cancel()
	defer qc.loopWg.Done()
	log.Debug("query coordinator start watch node loop")

	offlineNodes, err := qc.cluster.offlineNodes()
	if err == nil {
		offlineNodeIDs := make([]int64, 0)
		for id := range offlineNodes {
			offlineNodeIDs = append(offlineNodeIDs, id)
		}
		loadBalanceSegment := &querypb.LoadBalanceRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_LoadBalanceSegments,
				SourceID: qc.session.ServerID,
			},
			SourceNodeIDs: offlineNodeIDs,
		}

		baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_nodeDown)
		loadBalanceTask := &loadBalanceTask{
			baseTask:           baseTask,
			LoadBalanceRequest: loadBalanceSegment,
			rootCoord:          qc.rootCoordClient,
			dataCoord:          qc.dataCoordClient,
			cluster:            qc.cluster,
			meta:               qc.meta,
		}
		//TODO::deal enqueue error
		qc.scheduler.Enqueue(loadBalanceTask)
		log.Debug("start a loadBalance task", zap.Any("task", loadBalanceTask))
	}

	qc.eventChan = qc.session.WatchServices(typeutil.QueryNodeRole, qc.cluster.getSessionVersion()+1)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-qc.eventChan:
			if !ok {
				return
			}
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				log.Debug("start add a queryNode to cluster", zap.Any("nodeID", serverID))
				err := qc.cluster.registerNode(ctx, event.Session, serverID, disConnect)
				if err != nil {
					log.Error("query node failed to register", zap.Int64("nodeID", serverID), zap.String("error info", err.Error()))
				}
				qc.metricsCacheManager.InvalidateSystemInfoMetrics()
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Debug("get a del event after queryNode down", zap.Int64("nodeID", serverID))
				nodeExist := qc.cluster.hasNode(serverID)
				if !nodeExist {
					log.Error("queryNode not exist", zap.Int64("nodeID", serverID))
					continue
				}

				qc.cluster.stopNode(serverID)
				loadBalanceSegment := &querypb.LoadBalanceRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_LoadBalanceSegments,
						SourceID: qc.session.ServerID,
					},
					SourceNodeIDs: []int64{serverID},
					BalanceReason: querypb.TriggerCondition_nodeDown,
				}

				baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_nodeDown)
				loadBalanceTask := &loadBalanceTask{
					baseTask:           baseTask,
					LoadBalanceRequest: loadBalanceSegment,
					rootCoord:          qc.rootCoordClient,
					dataCoord:          qc.dataCoordClient,
					cluster:            qc.cluster,
					meta:               qc.meta,
				}
				qc.metricsCacheManager.InvalidateSystemInfoMetrics()
				//TODO:: deal enqueue error
				qc.scheduler.Enqueue(loadBalanceTask)
				log.Debug("start a loadBalance task", zap.Any("task", loadBalanceTask))
			}
		}
	}
}

func (qc *QueryCoord) watchHandoffSegmentLoop() {
	ctx, cancel := context.WithCancel(qc.loopCtx)

	defer cancel()
	defer qc.loopWg.Done()
	log.Debug("query coordinator start watch segment loop")

	// TODO:: recover handoff task when coord down
	watchChan := qc.kvClient.WatchWithPrefix(handoffSegmentPrefix)
	unIndexedSegmentChan := make(chan *querypb.SegmentInfo, 1024)
	indexSegmentChan := make(chan *querypb.SegmentInfo, 1024)

	go qc.checkIndexLoop(ctx, unIndexedSegmentChan, indexSegmentChan)
	go qc.processHandoffAfterIndexDone(ctx, indexSegmentChan)

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-watchChan:
			for _, event := range resp.Events {
				segmentInfo := &querypb.SegmentInfo{}
				err := proto.Unmarshal(event.Kv.Value, segmentInfo)
				if err != nil {
					log.Error("watchHandoffSegmentLoop: unmarshal failed", zap.Any("error", err.Error()))
					continue
				}
				switch event.Type {
				case mvccpb.PUT:
					processDone := true
					// if collection has not been loaded, then skip the segment
					collectionInfo, err := qc.meta.getCollectionInfoByID(segmentInfo.CollectionID)
					if err != nil {
						log.Debug("watchHandoffSegmentLoop: collection has not been loaded into memory", zap.Int64("collectionID", segmentInfo.CollectionID))
					} else {
						// if partition has not been loaded or released, then skip handoff the segment
						if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
							for _, id := range collectionInfo.PartitionIDs {
								if id == segmentInfo.PartitionID {
									unIndexedSegmentChan <- segmentInfo
									processDone = false
									break
								}
							}
						} else {
							partitionReleased := false
							for _, id := range collectionInfo.ReleasedPartitionIDs {
								if id == segmentInfo.PartitionID {
									partitionReleased = true
								}
							}
							if !partitionReleased {
								unIndexedSegmentChan <- segmentInfo
								processDone = false
							}
						}
					}

					if processDone {
						buildQuerySegmentPath := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.SegmentID)
						err = qc.kvClient.Remove(buildQuerySegmentPath)
						if err != nil {
							log.Error("watchHandoffSegmentLoop: remove handoff segment from etcd failed", zap.Error(err))
						}
					}
				default:
					// do nothing
				}
			}
		}
	}
}

func (qc *QueryCoord) checkIndexLoop(ctx context.Context, unIndexedChan chan *querypb.SegmentInfo, indexedChan chan *querypb.SegmentInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case segmentInfo := <-unIndexedChan:
			processDone := true
			// TODO:: check whether the index exists in parallel, in case indexCoord cannot create the index normally, and then block the loop
			for {
				// if the collection has been released, then skip handoff the segment
				collectionInfo, err := qc.meta.getCollectionInfoByID(segmentInfo.CollectionID)
				if err != nil {
					break
				}

				//  if the partition has been released, then skip handoff the segment
				partitionReleased := false
				for _, id := range collectionInfo.ReleasedPartitionIDs {
					if id == segmentInfo.PartitionID {
						partitionReleased = true
						break
					}
				}
				if partitionReleased {
					break
				}

				// check the buildID of the segment's index whether exist on rootCoord
				req := &milvuspb.DescribeSegmentRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_DescribeSegment,
					},
					CollectionID: segmentInfo.CollectionID,
					SegmentID:    segmentInfo.SegmentID,
				}
				response, err := qc.rootCoordClient.DescribeSegment(ctx, req)
				if err != nil || response.Status.ErrorCode != commonpb.ErrorCode_Success {
					continue
				}

				// if the segment.EnableIndex == false, then load the segment immediately
				// only sealed segment can be balanced, so the handoff is needed
				if !response.EnableIndex {
					log.Debug("checkIndexLoop: segment's enableIndex equal to false, ready to handoff", zap.Int64("segmentID", segmentInfo.SegmentID))
					indexedChan <- segmentInfo
					processDone = false
					break
				}

				indexFilePathRequest := &indexpb.GetIndexFilePathsRequest{
					IndexBuildIDs: []UniqueID{response.BuildID},
				}
				// if index created done on indexNode, then handoff start
				pathResponse, err := qc.indexCoordClient.GetIndexFilePaths(ctx, indexFilePathRequest)
				if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
					continue
				}

				log.Debug("checkIndexLoop: create segment's index done, ready to handoff", zap.Int64("segmentID", segmentInfo.SegmentID))
				indexedChan <- segmentInfo
				processDone = false
				break
			}

			if processDone {
				buildQuerySegmentPath := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.SegmentID)
				err := qc.kvClient.Remove(buildQuerySegmentPath)
				if err != nil {
					log.Error("watchHandoffSegmentLoop: remove handoff segment from etcd failed", zap.Error(err))
				}
			}
		}
	}
}

func (qc *QueryCoord) processHandoffAfterIndexDone(ctx context.Context, indexedChan chan *querypb.SegmentInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case segmentInfo := <-indexedChan:
			collectionID := segmentInfo.CollectionID
			partitionID := segmentInfo.PartitionID
			segmentID := segmentInfo.SegmentID
			if Params.AutoHandoff {
				log.Debug("processHandoffAfterIndexDone: handoff segment received",
					zap.Int64("collectionID", collectionID),
					zap.Int64("partitionID", partitionID),
					zap.Int64("segmentID", segmentID),
					zap.Any("segmentInfo", segmentInfo),
				)
				baseTask := newBaseTask(ctx, querypb.TriggerCondition_handoff)
				handoffReq := &querypb.HandoffSegmentsRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_HandoffSegments,
					},
					SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
				}
				handoffTask := &handoffTask{
					baseTask:               baseTask,
					HandoffSegmentsRequest: handoffReq,
					dataCoord:              qc.dataCoordClient,
					cluster:                qc.cluster,
					meta:                   qc.meta,
				}
				err := qc.scheduler.Enqueue(handoffTask)
				if err != nil {
					log.Error("processHandoffAfterIndexDone: handoffTask enqueue failed", zap.Error(err))
					break
				}

				go func() {
					err := handoffTask.waitToFinish()
					if err != nil {
						log.Error("processHandoffAfterIndexDone: handoffTask failed", zap.Error(err))
					}
				}()

				log.Debug("processHandoffAfterIndexDone: handoffTask completed",
					zap.Any("collectionID", collectionID),
					zap.Any("partitionID", partitionID),
					zap.Any("segmentID", segmentID),
					zap.Any("channel", segmentInfo.ChannelID),
				)
			}

			buildQuerySegmentPath := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, collectionID, partitionID, segmentID)
			err := qc.kvClient.Remove(buildQuerySegmentPath)
			if err != nil {
				log.Error("processHandoffAfterIndexDone: remove handoff segment from etcd failed", zap.Error(err))
			}
		}
	}
}
