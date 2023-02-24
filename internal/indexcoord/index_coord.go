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

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/metautil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// make sure IndexCoord implements types.IndexCoord
var _ types.IndexCoord = (*IndexCoord)(nil)

var Params paramtable.ComponentParam

// IndexCoord is a component responsible for scheduling index construction segments and maintaining index status.
// IndexCoord accepts requests from rootcoord to build indexes, delete indexes, and query index information.
// IndexCoord is responsible for assigning IndexBuildID to the request to build the index, and forwarding the
// request to build the index to IndexNode. IndexCoord records the status of the index, and the index file.
type IndexCoord struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	sched    *TaskScheduler
	session  *sessionutil.Session
	serverID UniqueID

	eventChan <-chan *sessionutil.SessionEvent

	factory      dependency.Factory
	etcdCli      *clientv3.Client
	etcdKV       kv.MetaKv
	chunkManager storage.ChunkManager

	metaTable             *metaTable
	nodeManager           *NodeManager
	indexBuilder          *indexBuilder
	garbageCollector      *garbageCollector
	flushedSegmentWatcher *flushedSegmentWatcher
	handoff               *handoff

	metricsCacheManager *metricsinfo.MetricsCacheManager

	indexGCLock sync.RWMutex

	initOnce  sync.Once
	startOnce sync.Once

	reqTimeoutInterval time.Duration

	dataCoordClient types.DataCoord
	rootCoordClient types.RootCoord

	enableActiveStandBy bool
	activateFunc        func() error

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// NewIndexCoord creates a new IndexCoord component.
func NewIndexCoord(ctx context.Context, factory dependency.Factory) (*IndexCoord, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	i := &IndexCoord{
		loopCtx:             ctx1,
		loopCancel:          cancel,
		reqTimeoutInterval:  time.Second * 10,
		factory:             factory,
		enableActiveStandBy: Params.IndexCoordCfg.EnableActiveStandby,
		indexGCLock:         sync.RWMutex{},
	}
	i.UpdateStateCode(commonpb.StateCode_Abnormal)
	return i, nil
}

// Register register IndexCoord role at etcd.
func (i *IndexCoord) Register() error {
	i.session.Register()
	if i.enableActiveStandBy {
		i.session.ProcessActiveStandBy(i.activateFunc)
	}
	metrics.NumNodes.WithLabelValues(strconv.FormatInt(i.session.ServerID, 10), typeutil.IndexCoordRole).Inc()
	log.Info("IndexCoord Register Finished")
	i.session.LivenessCheck(i.loopCtx, func() {
		log.Error("Index Coord disconnected from etcd, process will exit", zap.Int64("Server Id", i.session.ServerID))
		if err := i.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(strconv.FormatInt(i.session.ServerID, 10), typeutil.IndexCoordRole).Dec()
		// manually send signal to starter goroutine
		if i.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (i *IndexCoord) initSession() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.EtcdCfg.MetaRootPath, i.etcdCli)
	if i.session == nil {
		return errors.New("failed to initialize session")
	}
	i.session.Init(typeutil.IndexCoordRole, Params.IndexCoordCfg.Address, true, true)
	i.session.SetEnableActiveStandBy(i.enableActiveStandBy)
	Params.SetLogger(i.session.ServerID)
	i.serverID = i.session.ServerID
	return nil
}

// Init initializes the IndexCoord component.
func (i *IndexCoord) Init() error {
	var initErr error
	Params.InitOnce()
	i.factory.Init(&Params)
	initErr = i.initSession()
	if initErr != nil {
		log.Error(initErr.Error())
		return initErr
	}

	if i.enableActiveStandBy {
		i.activateFunc = func() error {
			log.Info("IndexCoord switch from standby to active, activating")
			if err := i.initIndexCoord(); err != nil {
				log.Error("IndexCoord init failed", zap.Error(err))
				return err
			}
			i.startIndexCoord()
			log.Info("IndexCoord startup success")
			return nil
		}
		i.stateCode.Store(commonpb.StateCode_StandBy)
		log.Info("IndexCoord enter standby mode successfully")
	} else {
		i.initOnce.Do(func() {
			initErr = i.initIndexCoord()
		})
	}

	return initErr
}

func (i *IndexCoord) initIndexCoord() error {
	i.UpdateStateCode(commonpb.StateCode_Initializing)
	log.Info("IndexCoord init", zap.Any("stateCode", i.stateCode.Load().(commonpb.StateCode)))
	var err error
	connectEtcdFn := func() error {
		i.etcdKV = etcdkv.NewEtcdKV(i.etcdCli, Params.EtcdCfg.MetaRootPath)
		i.metaTable, err = NewMetaTable(i.etcdKV)
		return err
	}
	log.Info("IndexCoord try to connect etcd")
	err = retry.Do(i.loopCtx, connectEtcdFn, retry.Attempts(100))
	if err != nil {
		log.Error("IndexCoord try to connect etcd failed", zap.Error(err))
		return err
	}

	log.Info("IndexCoord try to connect etcd success")
	i.nodeManager = NewNodeManager(i.loopCtx)

	sessions, revision, err := i.session.GetSessions(typeutil.IndexNodeRole)
	log.Info("IndexCoord", zap.Int("session number", len(sessions)), zap.Int64("revision", revision))
	if err != nil {
		log.Error("IndexCoord Get IndexNode Sessions error", zap.Error(err))
		return err
	}
	log.Info("IndexCoord get node sessions from etcd", zap.Bool("bind mode", Params.IndexCoordCfg.BindIndexNodeMode),
		zap.String("node address", Params.IndexCoordCfg.IndexNodeAddress))
	aliveNodeID := make([]UniqueID, 0)
	if Params.IndexCoordCfg.BindIndexNodeMode {
		if err = i.nodeManager.AddNode(Params.IndexCoordCfg.IndexNodeID, Params.IndexCoordCfg.IndexNodeAddress); err != nil {
			log.Error("IndexCoord add node fail", zap.Int64("ServerID", Params.IndexCoordCfg.IndexNodeID),
				zap.String("address", Params.IndexCoordCfg.IndexNodeAddress), zap.Error(err))
			return err
		}
		log.Info("IndexCoord add node success", zap.String("IndexNode address", Params.IndexCoordCfg.IndexNodeAddress),
			zap.Int64("nodeID", Params.IndexCoordCfg.IndexNodeID))
		aliveNodeID = append(aliveNodeID, Params.IndexCoordCfg.IndexNodeID)
	} else {
		for _, session := range sessions {
			session := session
			if err := i.nodeManager.AddNode(session.ServerID, session.Address); err != nil {
				log.Error("IndexCoord", zap.Int64("ServerID", session.ServerID),
					zap.Error(err))
				continue
			}
			aliveNodeID = append(aliveNodeID, session.ServerID)
		}
	}
	log.Info("IndexCoord", zap.Int("IndexNode number", len(i.nodeManager.GetAllClients())))
	i.indexBuilder = newIndexBuilder(i.loopCtx, i, i.metaTable, aliveNodeID)

	// TODO silverxia add Rewatch logic
	i.eventChan = i.session.WatchServices(typeutil.IndexNodeRole, revision+1, nil)

	chunkManager, err := i.factory.NewPersistentStorageChunkManager(i.loopCtx)
	if err != nil {
		log.Error("IndexCoord new minio chunkManager failed", zap.Error(err))
		return err
	}
	log.Info("IndexCoord new minio chunkManager success")
	i.chunkManager = chunkManager

	i.garbageCollector = newGarbageCollector(i.loopCtx, i.metaTable, i.chunkManager, i)
	i.handoff = newHandoff(i.loopCtx, i.metaTable, i.etcdKV, i)
	i.flushedSegmentWatcher, err = newFlushSegmentWatcher(i.loopCtx, i.etcdKV, i.metaTable, i.indexBuilder, i.handoff, i)
	if err != nil {
		return err
	}

	i.sched, err = NewTaskScheduler(i.loopCtx, i.rootCoordClient, i.chunkManager, i.metaTable)
	if err != nil {
		log.Error("IndexCoord new task scheduler failed", zap.Error(err))
		return err
	}
	log.Info("IndexCoord new task scheduler success")

	i.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	log.Info("IndexCoord init finished")

	return nil
}

// Start starts the IndexCoord component.
func (i *IndexCoord) Start() error {
	if !i.enableActiveStandBy {
		i.startIndexCoord()
		log.Info("IndexCoord start successfully", zap.Any("state", i.stateCode.Load()))
	}

	Params.IndexCoordCfg.CreatedTime = time.Now()
	Params.IndexCoordCfg.UpdatedTime = time.Now()
	return nil
}

func (i *IndexCoord) startIndexCoord() {
	i.startOnce.Do(func() {
		i.loopWg.Add(1)
		go i.watchNodeLoop()

		i.loopWg.Add(1)
		go i.watchFlushedSegmentLoop()

		i.sched.Start()

		i.indexBuilder.Start()
		i.garbageCollector.Start()
		i.handoff.Start()
		i.flushedSegmentWatcher.Start()

	})
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	i.UpdateStateCode(commonpb.StateCode_Healthy)
}

// Stop stops the IndexCoord component.
func (i *IndexCoord) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	i.UpdateStateCode(commonpb.StateCode_Abnormal)

	if i.loopCancel != nil {
		i.loopCancel()
		log.Info("cancel the loop of IndexCoord")
	}

	if i.sched != nil {
		i.sched.Close()
		log.Info("close the task scheduler of IndexCoord")
	}
	i.loopWg.Wait()

	if i.indexBuilder != nil {
		i.indexBuilder.Stop()
		log.Info("stop the index builder of IndexCoord")
	}
	if i.garbageCollector != nil {
		i.garbageCollector.Stop()
		log.Info("stop the garbage collector of IndexCoord")
	}
	if i.flushedSegmentWatcher != nil {
		i.flushedSegmentWatcher.Stop()
		log.Info("stop the flushed segment watcher")
	}

	for _, cb := range i.closeCallbacks {
		cb()
	}
	i.session.Revoke(time.Second)

	return nil
}

func (i *IndexCoord) SetEtcdClient(etcdClient *clientv3.Client) {
	i.etcdCli = etcdClient
}

// SetDataCoord sets data coordinator's client
func (i *IndexCoord) SetDataCoord(dataCoord types.DataCoord) error {
	if dataCoord == nil {
		return errors.New("null DataCoord interface")
	}

	i.dataCoordClient = dataCoord
	return nil
}

// SetRootCoord sets data coordinator's client
func (i *IndexCoord) SetRootCoord(rootCoord types.RootCoord) error {
	if rootCoord == nil {
		return errors.New("null RootCoord interface")
	}

	i.rootCoordClient = rootCoord
	return nil
}

// UpdateStateCode updates the component state of IndexCoord.
func (i *IndexCoord) UpdateStateCode(code commonpb.StateCode) {
	i.stateCode.Store(code)
}

func (i *IndexCoord) isHealthy() bool {
	code := i.stateCode.Load().(commonpb.StateCode)
	return code == commonpb.StateCode_Healthy
}

// GetComponentStates gets the component states of IndexCoord.
func (i *IndexCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	log.Debug("get IndexCoord component states ...")

	nodeID := common.NotRegisteredID
	if i.session != nil && i.session.Registered() {
		nodeID = i.session.ServerID
	}

	stateInfo := &milvuspb.ComponentInfo{
		NodeID:    nodeID,
		Role:      "IndexCoord",
		StateCode: i.stateCode.Load().(commonpb.StateCode),
	}

	ret := &milvuspb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	log.Debug("IndexCoord GetComponentStates", zap.Any("IndexCoord component state", stateInfo))
	return ret, nil
}

// GetStatisticsChannel gets the statistics channel of IndexCoord.
func (i *IndexCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexCoord statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

// CreateIndex create an index on collection.
// Index building is asynchronous, so when an index building request comes, an IndexID is assigned to the task and
// will get all flushed segments from DataCoord and record tasks with these segments. The background process
// indexBuilder will find this task and assign it to IndexNode for execution.
func (i *IndexCoord) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msgIndexCoordIsUnhealthy(i.serverID),
		}, nil
	}
	log.Info("IndexCoord receive create index request", zap.Int64("CollectionID", req.CollectionID),
		zap.String("IndexName", req.IndexName), zap.Int64("fieldID", req.FieldID),
		zap.Any("TypeParams", req.TypeParams),
		zap.Any("IndexParams", req.IndexParams))

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	ok, err := i.metaTable.CanCreateIndex(req)
	if !ok {
		log.Error("CreateIndex failed", zap.Error(err))
		ret.Reason = err.Error()
		return ret, nil
	}

	t := &CreateIndexTask{
		BaseTask: BaseTask{
			ctx:   ctx,
			done:  make(chan error),
			table: i.metaTable,
		},
		dataCoordClient:  i.dataCoordClient,
		rootCoordClient:  i.rootCoordClient,
		indexCoordClient: i,
		req:              req,
	}

	err = i.sched.IndexAddQueue.Enqueue(t)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Info("IndexCoord create index enqueue successfully", zap.Int64("IndexID", t.indexID))

	err = t.WaitToFinish()
	if err != nil {
		log.Error("IndexCoord scheduler creating index task fail", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.String("indexName", req.IndexName), zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}

	ret.ErrorCode = commonpb.ErrorCode_Success
	return ret, nil
}

// GetIndexState gets the index state of the index name in the request from Proxy.
func (i *IndexCoord) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	log.Info("IndexCoord get index state", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.IndexName))

	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &indexpb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
		}, nil
	}

	indexID2CreateTs := i.metaTable.GetIndexIDByName(req.CollectionID, req.IndexName)
	if len(indexID2CreateTs) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("IndexCoord get index state fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errMsg))
		return &indexpb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
		}, nil
	}
	if req.GetIndexName() == "" && len(indexID2CreateTs) > 1 {
		log.Warn(ErrMsgAmbiguousIndexName)
		return &indexpb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    ErrMsgAmbiguousIndexName,
			},
		}, nil
	}
	ret := &indexpb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State: commonpb.IndexState_Finished,
	}

	for indexID, createTs := range indexID2CreateTs {
		indexStates, _ := i.metaTable.GetIndexStates(indexID, createTs)
		for _, state := range indexStates {
			if state.state != commonpb.IndexState_Finished {
				ret.State = state.state
				ret.FailReason = state.failReason
				log.Info("IndexCoord get index state success", zap.Int64("collectionID", req.CollectionID),
					zap.String("indexName", req.IndexName), zap.String("state", ret.State.String()))
				return ret, nil
			}
		}
	}

	log.Info("IndexCoord get index state success", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.IndexName), zap.String("state", ret.State.String()))
	return ret, nil
}

func (i *IndexCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	log.Info("IndexCoord get index state", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.IndexName))

	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &indexpb.GetSegmentIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
		}, nil
	}

	ret := &indexpb.GetSegmentIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: make([]*indexpb.SegmentIndexState, 0),
	}
	indexID2CreateTs := i.metaTable.GetIndexIDByName(req.CollectionID, req.IndexName)
	if len(indexID2CreateTs) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("IndexCoord get index state fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errMsg))
		return &indexpb.GetSegmentIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
		}, nil
	}
	for _, segID := range req.SegmentIDs {
		state := i.metaTable.GetSegmentIndexState(segID)
		ret.States = append(ret.States, &indexpb.SegmentIndexState{
			SegmentID:  segID,
			State:      state.state,
			FailReason: state.failReason,
		})
	}
	return ret, nil
}

// completeIndexInfo get the building index row count and index task state
func (i *IndexCoord) completeIndexInfo(indexInfo *indexpb.IndexInfo, segIDs []UniqueID) error {
	collectionID := indexInfo.CollectionID
	indexName := indexInfo.IndexName
	log.Info("IndexCoord completeIndexInfo", zap.Int64("collID", collectionID),
		zap.String("indexName", indexName))

	indexID2CreateTs := i.metaTable.GetIndexIDByName(collectionID, indexName)
	if len(indexID2CreateTs) == 0 {
		log.Error("there is no index on collection", zap.Int64("collectionID", collectionID), zap.String("indexName", indexName))
		return nil
	}

	var indexID int64
	var createTs uint64
	// the size of `indexID2CreateTs` map is one
	// and we need to get key and value through the `for` statement
	for k, v := range indexID2CreateTs {
		indexID = k
		createTs = v
		break
	}

	segSet := typeutil.NewSet(segIDs...)

	indexStates, indexStateCnt := i.metaTable.GetIndexStates(indexID, createTs, func(segIdx *model.SegmentIndex) bool {
		return segSet.Contain(segIdx.SegmentID)
	})
	allCnt := len(indexStates)
	switch {
	case indexStateCnt.Failed > 0:
		indexInfo.State = commonpb.IndexState_Failed
		indexInfo.IndexStateFailReason = indexStateCnt.FailReason
	case indexStateCnt.Finished == allCnt:
		indexInfo.State = commonpb.IndexState_Finished
		indexInfo.IndexedRows = i.metaTable.GetIndexBuildProgress(indexID, segIDs)
	default:
		indexInfo.State = commonpb.IndexState_InProgress
		indexInfo.IndexedRows = i.metaTable.GetIndexBuildProgress(indexID, segIDs)
	}

	log.Info("IndexCoord completeIndexInfo success", zap.Int64("collID", collectionID),
		zap.Int64("totalRows", indexInfo.TotalRows), zap.Int64("indexRows", indexInfo.IndexedRows),
		zap.Any("state", indexInfo.State), zap.String("failReason", indexInfo.IndexStateFailReason))
	return nil
}

// GetIndexBuildProgress get the index building progress by num rows.
func (i *IndexCoord) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	log.Info("IndexCoord receive GetIndexBuildProgress request", zap.Int64("collID", req.CollectionID),
		zap.String("indexName", req.IndexName))
	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
		}, nil
	}

	indexID2CreateTs := i.metaTable.GetIndexIDByName(req.CollectionID, req.IndexName)
	if len(indexID2CreateTs) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("IndexCoord get index state fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errMsg))
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
		}, nil
	}
	if req.GetIndexName() == "" && len(indexID2CreateTs) > 1 {
		log.Warn(ErrMsgAmbiguousIndexName)
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    ErrMsgAmbiguousIndexName,
			},
		}, nil
	}

	flushSegments, err := i.dataCoordClient.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID:     req.CollectionID,
		PartitionID:      -1,
		IncludeUnhealthy: false,
	})
	if err != nil {
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, err
	}

	resp, err := i.dataCoordClient.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       flushSegments.Segments,
		IncludeUnHealthy: true,
	})
	if err != nil {
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, err
	}
	totalRows, indexRows := int64(0), int64(0)

	for _, seg := range resp.Infos {
		totalRows += seg.NumOfRows
	}

	for indexID := range indexID2CreateTs {
		indexRows = i.metaTable.GetIndexBuildProgress(indexID, flushSegments.Segments)
		break
	}

	log.Info("IndexCoord get index build progress success", zap.Int64("collID", req.CollectionID),
		zap.Int64("totalRows", totalRows), zap.Int64("indexRows", indexRows),
		zap.Int("seg num", len(flushSegments.Segments)))
	return &indexpb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexedRows: indexRows,
		TotalRows:   totalRows,
	}, nil
}

// DropIndex deletes indexes based on IndexName. One IndexName corresponds to the index of an entire column. A column is
// divided into many segments, and each segment corresponds to an IndexBuildID. IndexCoord uses IndexBuildID to record
// index tasks.
func (i *IndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log.Info("IndexCoord DropIndex", zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs), zap.String("indexName", req.IndexName))
	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msgIndexCoordIsUnhealthy(i.serverID),
		}, nil
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	indexID2CreateTs := i.metaTable.GetIndexIDByName(req.CollectionID, req.IndexName)
	if len(indexID2CreateTs) == 0 {
		log.Warn(fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName))
		return ret, nil
	}
	if !req.GetDropAll() && len(indexID2CreateTs) > 1 {
		log.Warn(ErrMsgAmbiguousIndexName)
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = ErrMsgAmbiguousIndexName
		return ret, nil
	}
	indexIDs := make([]UniqueID, 0)
	for indexID := range indexID2CreateTs {
		indexIDs = append(indexIDs, indexID)
	}
	if len(req.GetPartitionIDs()) == 0 {
		// drop collection index
		err := i.metaTable.MarkIndexAsDeleted(req.CollectionID, indexIDs)
		if err != nil {
			log.Error("IndexCoord drop index fail", zap.Int64("collectionID", req.CollectionID),
				zap.String("indexName", req.IndexName), zap.Error(err))
			ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
			ret.Reason = err.Error()
			return ret, nil
		}
	} else {
		err := i.metaTable.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			for _, partitionID := range req.PartitionIDs {
				if segIndex.CollectionID == req.CollectionID && segIndex.PartitionID == partitionID {
					return true
				}
			}
			return false
		})
		if err != nil {
			log.Error("IndexCoord drop index fail", zap.Int64("collectionID", req.CollectionID),
				zap.Int64s("partitionIDs", req.PartitionIDs), zap.String("indexName", req.IndexName), zap.Error(err))
			ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
			ret.Reason = err.Error()
			return ret, nil
		}
	}

	log.Info("IndexCoord DropIndex success", zap.Int64("collID", req.CollectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs), zap.String("indexName", req.IndexName),
		zap.Int64s("indexIDs", indexIDs))
	return ret, nil
}

// TODO @xiaocai2333: drop index on the segments when drop partition. (need?)

// GetIndexInfos gets the index file paths from IndexCoord.
func (i *IndexCoord) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	log.Debug("IndexCoord GetIndexInfos", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.GetIndexName()), zap.Int64s("segIDs", req.GetSegmentIDs()))
	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &indexpb.GetIndexInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
			SegmentInfo: nil,
		}, nil
	}
	ret := &indexpb.GetIndexInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegmentInfo: map[int64]*indexpb.SegmentInfo{},
	}

	for _, segID := range req.SegmentIDs {
		segIdxes := i.metaTable.GetSegmentIndexes(segID)
		ret.SegmentInfo[segID] = &indexpb.SegmentInfo{
			CollectionID: req.CollectionID,
			SegmentID:    segID,
			EnableIndex:  false,
			IndexInfos:   make([]*indexpb.IndexFilePathInfo, 0),
		}
		if len(segIdxes) != 0 {
			ret.SegmentInfo[segID].EnableIndex = true
			for _, segIdx := range segIdxes {
				indexFilePaths := metautil.BuildSegmentIndexFilePaths(i.chunkManager.RootPath(), segIdx.BuildID, segIdx.IndexVersion,
					segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexFileKeys)

				if segIdx.IndexState == commonpb.IndexState_Finished {
					ret.SegmentInfo[segID].IndexInfos = append(ret.SegmentInfo[segID].IndexInfos,
						&indexpb.IndexFilePathInfo{
							SegmentID:      segID,
							FieldID:        i.metaTable.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID),
							IndexID:        segIdx.IndexID,
							BuildID:        segIdx.BuildID,
							IndexName:      i.metaTable.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID),
							IndexParams:    i.metaTable.GetIndexParams(segIdx.CollectionID, segIdx.IndexID),
							IndexFilePaths: indexFilePaths,
							SerializedSize: segIdx.IndexSize,
							IndexVersion:   segIdx.IndexVersion,
							NumRows:        segIdx.NumRows,
						})
				}
			}
		}
	}

	return ret, nil
}

// DescribeIndex describe the index info of the collection.
func (i *IndexCoord) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	log.Info("IndexCoord DescribeIndex", zap.Int64("collectionID", req.CollectionID), zap.String("indexName", req.GetIndexName()))
	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))
		return &indexpb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
			IndexInfos: nil,
		}, nil
	}

	indexes := i.metaTable.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		return &indexpb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IndexNotExist,
				Reason:    fmt.Sprint("index doesn't exist, collectionID ", req.CollectionID),
			},
		}, nil
	}

	ret := &indexpb.DescribeIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
	}

	// The total rows of all indexes should be based on the current perspective
	flushedSegmentR, err := i.dataCoordClient.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID:     req.GetCollectionID(),
		PartitionID:      -1,
		IncludeUnhealthy: false,
	})
	if err != nil {
		return ret, err
	}
	if flushedSegmentR.Status.GetErrorCode() != commonpb.ErrorCode_Success {
		ret.Status.ErrorCode = flushedSegmentR.Status.GetErrorCode()
		ret.Status.Reason = flushedSegmentR.Status.GetReason()
		return ret, nil
	}

	resp, err := i.dataCoordClient.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       flushedSegmentR.Segments,
		IncludeUnHealthy: true,
	})
	if err != nil {
		return ret, err
	}
	if resp.Status.GetErrorCode() != commonpb.ErrorCode_Success {
		ret.Status.ErrorCode = resp.Status.GetErrorCode()
		ret.Status.Reason = resp.Status.GetReason()
		return ret, nil
	}

	totalRows := int64(0)
	for _, seg := range resp.Infos {
		totalRows += seg.NumOfRows
	}

	indexInfos := make([]*indexpb.IndexInfo, 0)
	for _, index := range indexes {
		indexInfo := &indexpb.IndexInfo{
			CollectionID:    index.CollectionID,
			FieldID:         index.FieldID,
			IndexName:       index.IndexName,
			TypeParams:      index.TypeParams,
			IndexParams:     index.IndexParams,
			IsAutoIndex:     index.IsAutoIndex,
			UserIndexParams: index.UserIndexParams,
			IndexID:         index.IndexID,
			TotalRows:       totalRows,
		}
		if err := i.completeIndexInfo(indexInfo, flushedSegmentR.Segments); err != nil {
			log.Error("IndexCoord describe index fail", zap.Int64("collectionID", req.CollectionID),
				zap.String("indexName", req.IndexName), zap.Error(err))
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
		log.Info("IndexCoord describe index success", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("indexID", indexInfo.IndexID), zap.Int64("total rows", indexInfo.TotalRows),
			zap.Int64("index rows", indexInfo.IndexedRows), zap.String("index state", indexInfo.State.String()))
		indexInfos = append(indexInfos, indexInfo)
	}

	return &indexpb.DescribeIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: indexInfos,
	}, nil
}

// ShowConfigurations returns the configurations of indexCoord matching req.Pattern
func (i *IndexCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("IndexCoord.ShowConfigurations", zap.String("pattern", req.Pattern))
	if !i.isHealthy() {
		log.Warn("IndexCoord.ShowConfigurations failed",
			zap.Int64("nodeId", i.serverID),
			zap.String("req", req.Pattern),
			zap.Error(errIndexCoordIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(Params.QueryNodeCfg.GetNodeID()),
			},
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics gets the metrics info of IndexCoord.
func (i *IndexCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", Params.IndexCoordCfg.GetNodeID()),
		zap.String("req", req.GetRequest()))
	log.RatedInfo(60, "IndexCoord.GetMetrics")

	if !i.isHealthy() {
		log.Warn(msgIndexCoordIsUnhealthy(i.serverID))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.serverID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Error("IndexCoord.GetMetrics failed to parse metric type",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := i.metricsCacheManager.GetSystemInfoMetrics()
		if err != nil {
			// Miss cache
			metrics, err = getSystemInfoMetrics(ctx, req, i)
		}

		log.RatedDebug(60, "IndexCoord.GetMetrics",
			zap.String("metricType", metricType),
			zap.String("metrics", metrics.Response), // TODO(dragondriver): necessary? may be very large
			zap.Error(err),
		)

		i.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.RatedWarn(60, "IndexCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

func (i *IndexCoord) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if !i.isHealthy() {
		reason := errorutil.UnHealthReason("indexcoord", i.session.ServerID, "indexcoord is unhealthy")
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: []string{reason}}, nil
	}

	mu := &sync.Mutex{}
	group, ctx := errgroup.WithContext(ctx)
	errReasons := make([]string, 0, len(i.nodeManager.GetAllClients()))

	for nodeID, indexClient := range i.nodeManager.GetAllClients() {
		nodeID := nodeID
		indexClient := indexClient
		group.Go(func() error {
			sta, err := indexClient.GetComponentStates(ctx)
			isHealthy, reason := errorutil.UnHealthReasonWithComponentStatesOrErr("indexnode", nodeID, sta, err)
			if !isHealthy {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, reason)
			}
			return err
		})
	}

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: errReasons}, nil
	}

	return &milvuspb.CheckHealthResponse{IsHealthy: true, Reasons: errReasons}, nil
}

// watchNodeLoop is used to monitor IndexNode going online and offline.
// fix datarace in unittest
// startWatchService will only be invoked at start procedure
// otherwise, remove the annotation and add atomic protection
//
//go:norace
func (i *IndexCoord) watchNodeLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-i.eventChan:
			if !ok {
				// ErrCompacted is handled inside SessionWatcher
				log.Error("Session Watcher channel closed", zap.Int64("server id", i.session.ServerID))
				go i.Stop()
				if i.session.TriggerKill {
					if p, err := os.FindProcess(os.Getpid()); err == nil {
						p.Signal(syscall.SIGINT)
					}
				}
				return
			}
			if Params.IndexCoordCfg.BindIndexNodeMode {
				continue
			}
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				log.Info("IndexCoord watchNodeLoop SessionAddEvent", zap.Int64("serverID", serverID),
					zap.String("address", event.Session.Address))
				go func() {
					err := i.nodeManager.AddNode(serverID, event.Session.Address)
					if err != nil {
						log.Error("IndexCoord", zap.Any("Add IndexNode err", err))
					}
				}()
				i.metricsCacheManager.InvalidateSystemInfoMetrics()
			case sessionutil.SessionUpdateEvent:
				serverID := event.Session.ServerID
				log.Info("IndexCoord watchNodeLoop SessionUpdateEvent", zap.Int64("serverID", serverID))
				i.nodeManager.StoppingNode(serverID)
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Info("IndexCoord watchNodeLoop SessionDelEvent", zap.Int64("serverID", serverID))
				i.nodeManager.RemoveNode(serverID)
				// remove tasks on nodeID
				i.indexBuilder.nodeDown(serverID)
				i.metricsCacheManager.InvalidateSystemInfoMetrics()
			}
		}
	}
}

func (i *IndexCoord) tryAcquireSegmentReferLock(ctx context.Context, buildID UniqueID, nodeID UniqueID, segIDs []UniqueID) error {
	// IndexCoord use buildID instead of taskID.
	log.Info("try to acquire segment reference lock", zap.Int64("buildID", buildID),
		zap.Int64("ndoeID", nodeID), zap.Int64s("segIDs", segIDs))
	ctx1, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()
	status, err := i.dataCoordClient.AcquireSegmentLock(ctx1, &datapb.AcquireSegmentLockRequest{
		TaskID:     buildID,
		NodeID:     nodeID,
		SegmentIDs: segIDs,
	})
	if err != nil {
		log.Error("IndexCoord try to acquire segment reference lock failed", zap.Int64("buildID", buildID),
			zap.Int64("nodeID", nodeID), zap.Int64s("segIDs", segIDs), zap.Error(err))
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("IndexCoord try to acquire segment reference lock failed", zap.Int64("buildID", buildID),
			zap.Int64("nodeID", nodeID), zap.Int64s("segIDs", segIDs), zap.Error(errors.New(status.Reason)))
		return errors.New(status.Reason)
	}
	log.Info("try to acquire segment reference lock success", zap.Int64("buildID", buildID),
		zap.Int64("ndoeID", nodeID), zap.Int64s("segIDs", segIDs))
	return nil
}

func (i *IndexCoord) tryReleaseSegmentReferLock(ctx context.Context, buildID UniqueID, nodeID UniqueID) error {
	releaseLock := func() error {
		ctx1, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
		defer cancel()
		status, err := i.dataCoordClient.ReleaseSegmentLock(ctx1, &datapb.ReleaseSegmentLockRequest{
			TaskID: buildID,
			NodeID: nodeID,
		})
		if err != nil {
			return err
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(status.Reason)
		}
		return nil
	}
	err := retry.Do(ctx, releaseLock, retry.Attempts(100))
	if err != nil {
		log.Error("IndexCoord try to release segment reference lock failed", zap.Int64("buildID", buildID),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}
	return nil
}

// assignTask sends the index task to the IndexNode, it has a timeout interval, if the IndexNode doesn't respond within
// the interval, it is considered that the task sending failed.
func (i *IndexCoord) assignTask(builderClient types.IndexNode, req *indexpb.CreateJobRequest) error {
	ctx, cancel := context.WithTimeout(i.loopCtx, i.reqTimeoutInterval)
	defer cancel()
	resp, err := builderClient.CreateJob(ctx, req)
	if err != nil {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.Error(err))
		return err
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.String("Reason", resp.Reason))
		return errors.New(resp.Reason)
	}
	return nil
}

func (i *IndexCoord) createIndexForSegment(segIdx *model.SegmentIndex) (bool, UniqueID, error) {
	log.Info("create index for flushed segment", zap.Int64("collID", segIdx.CollectionID),
		zap.Int64("segID", segIdx.SegmentID), zap.Int64("numRows", segIdx.NumRows))
	//if segIdx.NumRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
	//	log.Debug("no need to build index", zap.Int64("collID", segIdx.CollectionID),
	//		zap.Int64("segID", segIdx.SegmentID), zap.Int64("numRows", segIdx.NumRows))
	//	return false, 0, nil
	//}

	hasIndex, indexBuildID := i.metaTable.HasSameIndex(segIdx.SegmentID, segIdx.IndexID)
	if hasIndex {
		log.Info("IndexCoord has same index", zap.Int64("buildID", indexBuildID), zap.Int64("segmentID", segIdx.SegmentID))
		return true, indexBuildID, nil
	}

	t := &IndexAddTask{
		BaseTask: BaseTask{
			ctx:   i.loopCtx,
			done:  make(chan error),
			table: i.metaTable,
		},
		segmentIndex:    segIdx,
		rootcoordClient: i.rootCoordClient,
	}

	metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.TotalLabel).Inc()

	err := i.sched.IndexAddQueue.Enqueue(t)
	if err != nil {
		metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		log.Error("IndexCoord createIndex enqueue failed", zap.Int64("collID", segIdx.CollectionID),
			zap.Int64("segID", segIdx.SegmentID), zap.Error(err))
		return false, 0, err
	}
	log.Info("IndexCoord createIndex Enqueue successfully", zap.Int64("collID", segIdx.CollectionID),
		zap.Int64("segID", segIdx.SegmentID), zap.Int64("IndexBuildID", t.segmentIndex.BuildID))

	err = t.WaitToFinish()
	if err != nil {
		log.Error("IndexCoord scheduler index task failed", zap.Int64("buildID", t.segmentIndex.BuildID))
		metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return false, 0, err
	}
	metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.SuccessLabel).Inc()

	return false, t.segmentIndex.BuildID, nil
}

func (i *IndexCoord) watchFlushedSegmentLoop() {
	log.Info("IndexCoord start watching flushed segments...")
	defer i.loopWg.Done()

	watchChan := i.etcdKV.WatchWithRevision(util.FlushedSegmentPrefix, i.flushedSegmentWatcher.etcdRevision+1)
	for {
		select {
		case <-i.loopCtx.Done():
			log.Warn("IndexCoord context done, exit...")
			return
		case resp, ok := <-watchChan:
			if !ok {
				log.Warn("IndexCoord watch flush segments loop failed because watch channel closed")
				return
			}
			if err := resp.Err(); err != nil {
				log.Warn("IndexCoord watchFlushedSegmentLoo receive etcd compacted error")
				if err == v3rpc.ErrCompacted {
					err = i.flushedSegmentWatcher.reloadFromKV()
					if err != nil {
						log.Error("Constructing flushed segment watcher fails when etcd has a compaction error",
							zap.String("etcd error", err.Error()), zap.Error(err))
						panic("failed to handle etcd request, exit..")
					}
					i.loopWg.Add(1)
					go i.watchFlushedSegmentLoop()
					return
				}
				log.Error("received error event from flushed segment watcher",
					zap.String("prefix", util.FlushedSegmentPrefix), zap.Error(err))
				panic("failed to handle etcd request, exit..")
			}
			events := resp.Events
			for _, event := range events {
				switch event.Type {
				case mvccpb.PUT:
					segmentInfo := &datapb.SegmentInfo{}
					if err := proto.Unmarshal(event.Kv.Value, segmentInfo); err != nil {
						// just for  backward compatibility
						segID, err := strconv.ParseInt(string(event.Kv.Value), 10, 64)
						if err != nil {
							log.Error("watchFlushedSegmentLoop unmarshal fail", zap.String("value", string(event.Kv.Value)), zap.Error(err))
							continue
						}
						segmentInfo.ID = segID
					}

					log.Info("watchFlushedSegmentLoop watch event",
						zap.Int64("segID", segmentInfo.GetID()),
						zap.Any("isFake", segmentInfo.GetIsFake()))
					i.flushedSegmentWatcher.enqueueInternalTask(segmentInfo)
				case mvccpb.DELETE:
					log.Info("the segment info has been deleted", zap.String("key", string(event.Kv.Key)))
				}
			}
		}
	}
}

func (i *IndexCoord) pullSegmentInfo(ctx context.Context, segmentID UniqueID) (*datapb.SegmentInfo, error) {
	ctx1, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()
	resp, err := i.dataCoordClient.GetSegmentInfo(ctx1, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       []int64{segmentID},
		IncludeUnHealthy: false,
	})
	if err != nil {
		log.Warn("IndexCoord get segment info fail", zap.Int64("segID", segmentID), zap.Error(err))
		return nil, err
	}
	if resp.Status.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("IndexCoord get segment info fail", zap.Int64("segID", segmentID),
			zap.String("fail reason", resp.Status.GetReason()))
		if resp.Status.GetReason() == msgSegmentNotFound(segmentID) {
			return nil, errSegmentNotFound(segmentID)
		}
		return nil, errors.New(resp.Status.GetReason())
	}
	for _, info := range resp.Infos {
		if info.ID == segmentID {
			return info, nil
		}
	}
	errMsg := msgSegmentNotFound(segmentID)
	log.Warn(errMsg)
	return nil, errSegmentNotFound(segmentID)
}
