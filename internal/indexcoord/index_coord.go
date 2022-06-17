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
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// make sure IndexCoord implements types.IndexCoord
var _ types.IndexCoord = (*IndexCoord)(nil)

var Params paramtable.ComponentParam

// IndexCoord is a component responsible for scheduling index construction tasks and maintaining index status.
// IndexCoord accepts requests from rootcoord to build indexes, delete indexes, and query index information.
// IndexCoord is responsible for assigning IndexBuildID to the request to build the index, and forwarding the
// request to build the index to IndexNode. IndexCoord records the status of the index, and the index file.
type IndexCoord struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	sched   *TaskScheduler
	session *sessionutil.Session

	eventChan <-chan *sessionutil.SessionEvent

	idAllocator *allocator.GlobalIDAllocator

	factory      dependency.Factory
	etcdCli      *clientv3.Client
	chunkManager storage.ChunkManager

	metaTable   *metaTable
	nodeManager *NodeManager

	metricsCacheManager *metricsinfo.MetricsCacheManager

	nodeLock sync.RWMutex

	initOnce  sync.Once
	startOnce sync.Once

	reqTimeoutInterval time.Duration
	durationInterval   time.Duration
	assignTaskInterval time.Duration
	taskLimit          int

	dataCoordClient types.DataCoord

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
		loopCtx:            ctx1,
		loopCancel:         cancel,
		reqTimeoutInterval: time.Second * 10,
		durationInterval:   time.Second * 10,
		assignTaskInterval: time.Second * 3,
		taskLimit:          20,
		factory:            factory,
	}
	i.UpdateStateCode(internalpb.StateCode_Abnormal)
	return i, nil
}

// Register register IndexCoord role at etcd.
func (i *IndexCoord) Register() error {
	i.session.Register()
	go i.session.LivenessCheck(i.loopCtx, func() {
		log.Error("Index Coord disconnected from etcd, process will exit", zap.Int64("Server Id", i.session.ServerID))
		if err := i.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
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
	Params.SetLogger(i.session.ServerID)
	return nil
}

// Init initializes the IndexCoord component.
func (i *IndexCoord) Init() error {
	var initErr error
	Params.InitOnce()
	i.initOnce.Do(func() {
		i.UpdateStateCode(internalpb.StateCode_Initializing)
		log.Debug("IndexCoord init", zap.Any("stateCode", i.stateCode.Load().(internalpb.StateCode)))

		i.factory.Init(&Params)

		err := i.initSession()
		if err != nil {
			log.Error(err.Error())
			initErr = err
			return
		}

		connectEtcdFn := func() error {
			etcdKV := etcdkv.NewEtcdKV(i.etcdCli, Params.EtcdCfg.MetaRootPath)
			metakv, err := NewMetaTable(etcdKV)
			if err != nil {
				return err
			}
			i.metaTable = metakv
			return err
		}
		log.Debug("IndexCoord try to connect etcd")
		err = retry.Do(i.loopCtx, connectEtcdFn, retry.Attempts(100))
		if err != nil {
			log.Error("IndexCoord try to connect etcd failed", zap.Error(err))
			initErr = err
			return
		}
		log.Debug("IndexCoord try to connect etcd success")
		i.nodeManager = NewNodeManager(i.loopCtx)

		sessions, revision, err := i.session.GetSessions(typeutil.IndexNodeRole)
		log.Debug("IndexCoord", zap.Int("session number", len(sessions)), zap.Int64("revision", revision))
		if err != nil {
			log.Error("IndexCoord Get IndexNode Sessions error", zap.Error(err))
			initErr = err
			return
		}
		for _, session := range sessions {
			session := session
			go func() {
				if err := i.nodeManager.AddNode(session.ServerID, session.Address); err != nil {
					log.Error("IndexCoord", zap.Int64("ServerID", session.ServerID),
						zap.Error(err))
				}
			}()

		}
		log.Debug("IndexCoord", zap.Int("IndexNode number", len(i.nodeManager.nodeClients)))
		// TODO silverxia add Rewatch logic
		i.eventChan = i.session.WatchServices(typeutil.IndexNodeRole, revision+1, nil)
		nodeTasks := i.metaTable.GetNodeTaskStats()
		for nodeID, taskNum := range nodeTasks {
			i.nodeManager.pq.UpdatePriority(nodeID, taskNum)
		}

		//init idAllocator
		kvRootPath := Params.EtcdCfg.KvRootPath
		etcdKV := tsoutil.NewTSOKVBase(i.etcdCli, kvRootPath, "index_gid")

		i.idAllocator = allocator.NewGlobalIDAllocator("idTimestamp", etcdKV)
		if err := i.idAllocator.Initialize(); err != nil {
			log.Error("IndexCoord idAllocator initialize failed", zap.Error(err))
			return
		}

		chunkManager, err := i.factory.NewVectorStorageChunkManager(i.loopCtx)

		if err != nil {
			log.Error("IndexCoord new minio chunkManager failed", zap.Error(err))
			initErr = err
			return
		}
		log.Debug("IndexCoord new minio chunkManager success")
		i.chunkManager = chunkManager

		i.sched, err = NewTaskScheduler(i.loopCtx, i.idAllocator, i.chunkManager, i.metaTable)
		if err != nil {
			log.Error("IndexCoord new task scheduler failed", zap.Error(err))
			initErr = err
			return
		}
		log.Debug("IndexCoord new task scheduler success")

		i.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	})

	log.Debug("IndexCoord init finished", zap.Error(initErr))

	return initErr
}

// Start starts the IndexCoord component.
func (i *IndexCoord) Start() error {
	var startErr error
	i.startOnce.Do(func() {
		i.loopWg.Add(1)
		go i.tsLoop()

		i.loopWg.Add(1)
		go i.recycleUnusedIndexFiles()

		i.loopWg.Add(1)
		go i.assignTaskLoop()

		i.loopWg.Add(1)
		go i.watchNodeLoop()

		i.loopWg.Add(1)
		go i.watchMetaLoop()

		startErr = i.sched.Start()

		i.UpdateStateCode(internalpb.StateCode_Healthy)
	})
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}

	Params.IndexCoordCfg.CreatedTime = time.Now()
	Params.IndexCoordCfg.UpdatedTime = time.Now()

	i.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("IndexCoord start successfully", zap.Any("State", i.stateCode.Load()))

	return startErr
}

// Stop stops the IndexCoord component.
func (i *IndexCoord) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	i.UpdateStateCode(internalpb.StateCode_Abnormal)

	if i.loopCancel != nil {
		i.loopCancel()
		log.Info("cancel the loop of IndexCoord")
	}

	if i.sched != nil {
		i.sched.Close()
		log.Info("close the task scheduler of IndexCoord")
	}

	i.loopWg.Wait()

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

// UpdateStateCode updates the component state of IndexCoord.
func (i *IndexCoord) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode.Store(code)
}

func (i *IndexCoord) isHealthy() bool {
	code := i.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

// GetComponentStates gets the component states of IndexCoord.
func (i *IndexCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("get IndexCoord component states ...")

	nodeID := common.NotRegisteredID
	if i.session != nil && i.session.Registered() {
		nodeID = i.session.ServerID
	}

	stateInfo := &internalpb.ComponentInfo{
		NodeID:    nodeID,
		Role:      "IndexCoord",
		StateCode: i.stateCode.Load().(internalpb.StateCode),
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	log.Debug("IndexCoord GetComponentStates", zap.Any("IndexCoord component state", stateInfo))
	return ret, nil
}

// GetTimeTickChannel gets the time tick channel of IndexCoord.
func (i *IndexCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexCoord time tick channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
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

// BuildIndex receives request from RootCoordinator to build an index.
// Index building is asynchronous, so when an index building request comes, an IndexBuildID is assigned to the task and
// the task is recorded in Meta. The background process assignTaskLoop will find this task and assign it to IndexNode for
// execution.
func (i *IndexCoord) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	if !i.isHealthy() {
		errMsg := "IndexCoord is not healthy"
		err := errors.New(errMsg)
		log.Warn(errMsg)
		return &indexpb.BuildIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
		}, err
	}
	log.Debug("IndexCoord building index ...", zap.Int64("segmentID", req.SegmentID),
		zap.String("IndexName", req.IndexName), zap.Int64("IndexID", req.IndexID),
		zap.Strings("DataPath", req.DataPaths), zap.Any("TypeParams", req.TypeParams),
		zap.Any("IndexParams", req.IndexParams), zap.Int64("numRows", req.NumRows),
		zap.Any("field type", req.FieldSchema.DataType))

	ret := &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		IndexBuildID: 0,
	}

	metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.TotalLabel).Inc()

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "IndexCoord-BuildIndex")
	defer sp.Finish()
	hasIndex, indexBuildID := i.metaTable.HasSameReq(req)
	if hasIndex {
		log.Debug("IndexCoord has same index", zap.Int64("buildID", indexBuildID), zap.Int64("segmentID", req.SegmentID))
		return &indexpb.BuildIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "already have same index",
			},
			IndexBuildID: indexBuildID,
		}, nil
	}

	t := &IndexAddTask{
		BaseTask: BaseTask{
			ctx:   ctx,
			done:  make(chan error),
			table: i.metaTable,
		},
		req:         req,
		idAllocator: i.idAllocator,
	}

	var cancel func()
	t.ctx, cancel = context.WithTimeout(ctx, i.reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("IndexAddQueue enqueue timeout")
		default:
			return i.sched.IndexAddQueue.Enqueue(t)
		}
	}
	err := fn()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Status.Reason = err.Error()
		metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return ret, nil
	}
	log.Debug("IndexCoord BuildIndex Enqueue successfully", zap.Int64("IndexBuildID", t.indexBuildID))

	err = t.WaitToFinish()
	if err != nil {
		log.Error("IndexCoord scheduler index task failed", zap.Int64("IndexBuildID", t.indexBuildID))
		ret.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Status.Reason = err.Error()
		metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return ret, nil
	}
	sp.SetTag("IndexCoord-IndexBuildID", strconv.FormatInt(t.indexBuildID, 10))
	ret.Status.ErrorCode = commonpb.ErrorCode_Success
	ret.IndexBuildID = t.indexBuildID
	metrics.IndexCoordIndexRequestCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	return ret, nil
}

// GetIndexStates gets the index states from IndexCoord.
func (i *IndexCoord) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	log.Debug("IndexCoord get index states", zap.Int64s("IndexBuildIDs", req.IndexBuildIDs))
	if !i.isHealthy() {
		errMsg := "IndexCoord is not healthy"
		log.Warn(errMsg)
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
		}, nil
	}
	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "IndexCoord-BuildIndex")
	defer sp.Finish()
	var (
		cntNone       = 0
		cntUnissued   = 0
		cntInprogress = 0
		cntFinished   = 0
		cntFailed     = 0
	)
	indexStates := i.metaTable.GetIndexStates(req.IndexBuildIDs)
	for _, state := range indexStates {
		switch state.State {
		case commonpb.IndexState_IndexStateNone:
			cntNone++
		case commonpb.IndexState_Unissued:
			cntUnissued++
		case commonpb.IndexState_InProgress:
			cntInprogress++
		case commonpb.IndexState_Finished:
			cntFinished++
		case commonpb.IndexState_Failed:
			cntFailed++
		}
	}
	log.Debug("IndexCoord get index states success",
		zap.Int("total", len(indexStates)), zap.Int("None", cntNone), zap.Int("Unissued", cntUnissued),
		zap.Int("InProgress", cntInprogress), zap.Int("Finished", cntFinished), zap.Int("Failed", cntFailed))

	ret := &indexpb.GetIndexStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: indexStates,
	}
	return ret, nil
}

// DropIndex deletes indexes based on IndexID. One IndexID corresponds to the index of an entire column. A column is
// divided into many segments, and each segment corresponds to an IndexBuildID. IndexCoord uses IndexBuildID to record
// index tasks. Therefore, when DropIndex, delete all tasks corresponding to IndexBuildID corresponding to IndexID.
func (i *IndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log.Debug("IndexCoord DropIndex", zap.Int64("IndexID", req.IndexID))
	if !i.isHealthy() {
		errMsg := "IndexCoord is not healthy"
		log.Warn(errMsg)
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}, nil
	}
	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "IndexCoord-BuildIndex")
	defer sp.Finish()

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	err := i.metaTable.MarkIndexAsDeleted(req.IndexID)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}

	defer func() {
		go func() {
			unissuedIndexBuildIDs := i.sched.IndexAddQueue.tryToRemoveUselessIndexAddTask(req.IndexID)
			for _, indexBuildID := range unissuedIndexBuildIDs {
				i.metaTable.DeleteIndex(indexBuildID)
			}
		}()
	}()

	log.Debug("IndexCoord DropIndex success", zap.Int64("IndexID", req.IndexID))
	return ret, nil
}

func (i *IndexCoord) RemoveIndex(ctx context.Context, req *indexpb.RemoveIndexRequest) (*commonpb.Status, error) {
	log.Info("IndexCoord receive RemoveIndex", zap.Int64s("buildIDs", req.BuildIDs))

	if !i.isHealthy() {
		errMsg := "IndexCoord is not healthy"
		log.Warn(errMsg)
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}, nil
	}

	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "IndexCoord-RemoveIndex")
	defer sp.Finish()

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	err := i.metaTable.MarkIndexAsDeletedByBuildIDs(req.GetBuildIDs())
	if err != nil {
		log.Error("IndexCoord MarkIndexAsDeletedByBuildIDs failed", zap.Int64s("buildIDs", req.GetBuildIDs()),
			zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}

	return ret, nil
}

// GetIndexFilePaths gets the index file paths from IndexCoord.
func (i *IndexCoord) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	log.Debug("IndexCoord GetIndexFilePaths", zap.Int("number of IndexBuildIds", len(req.IndexBuildIDs)))
	if !i.isHealthy() {
		errMsg := "IndexCoord is not healthy"
		log.Warn(errMsg)
		return &indexpb.GetIndexFilePathsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			},
			FilePaths: nil,
		}, nil
	}
	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "IndexCoord-BuildIndex")
	defer sp.Finish()
	var indexPaths []*indexpb.IndexFilePathInfo

	for _, buildID := range req.IndexBuildIDs {
		indexPathInfo, err := i.metaTable.GetIndexFilePathInfo(buildID)
		if err != nil {
			log.Warn("IndexCoord GetIndexFilePaths failed", zap.Int64("indexBuildID", buildID), zap.Error(err))
			return &indexpb.GetIndexFilePathsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
		indexPaths = append(indexPaths, indexPathInfo)
	}

	ret := &indexpb.GetIndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		FilePaths: indexPaths,
	}
	log.Debug("IndexCoord GetIndexFilePaths ", zap.Int("indexBuildIDs num", len(req.IndexBuildIDs)), zap.Int("file path num", len(ret.FilePaths)))

	return ret, nil
}

// GetMetrics gets the metrics info of IndexCoord.
func (i *IndexCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("IndexCoord.GetMetrics",
		zap.Int64("node id", i.session.ServerID),
		zap.String("req", req.Request))

	if !i.isHealthy() {
		log.Warn("IndexCoord.GetMetrics failed",
			zap.Int64("node id", i.session.ServerID),
			zap.String("req", req.Request),
			zap.Error(errIndexCoordIsUnhealthy(i.session.ServerID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexCoordIsUnhealthy(i.session.ServerID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Error("IndexCoord.GetMetrics failed to parse metric type",
			zap.Int64("node id", i.session.ServerID),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	log.Debug("IndexCoord.GetMetrics",
		zap.String("metric type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := i.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}
		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.Error(err))

		metrics, err := getSystemInfoMetrics(ctx, req, i)

		log.Debug("IndexCoord.GetMetrics",
			zap.Int64("node id", i.session.ServerID),
			zap.String("req", req.Request),
			zap.String("metric type", metricType),
			zap.String("metrics", metrics.Response), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		i.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.Debug("IndexCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node id", i.session.ServerID),
		zap.String("req", req.Request),
		zap.String("metric type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

func (i *IndexCoord) tsLoop() {
	tsoTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(i.loopCtx)
	defer cancel()
	defer i.loopWg.Done()
	for {
		select {
		case <-tsoTicker.C:
			if err := i.idAllocator.UpdateID(); err != nil {
				log.Error("IndexCoord tsLoop UpdateID failed", zap.Error(err))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Debug("IndexCoord tsLoop is closed")
			return
		}
	}
}

// recycleUnusedIndexFiles is used to delete useless index files, including lower version index files and index files
// corresponding to the deleted index.
func (i *IndexCoord) recycleUnusedIndexFiles() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()

	timeTicker := time.NewTicker(i.durationInterval)
	log.Debug("IndexCoord start recycleUnusedIndexFiles loop")

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeTicker.C:
			metas := i.metaTable.GetUnusedIndexFiles(i.taskLimit)
			for _, meta := range metas {
				if meta.indexMeta.MarkDeleted {
					unusedIndexFilePathPrefix := Params.IndexCoordCfg.IndexStorageRootPath + "/" + strconv.Itoa(int(meta.indexMeta.IndexBuildID))
					log.Debug("IndexCoord recycleUnusedIndexFiles",
						zap.Int64("Recycle the index files for deleted index with indexBuildID", meta.indexMeta.IndexBuildID))
					if err := i.chunkManager.RemoveWithPrefix(unusedIndexFilePathPrefix); err != nil {
						log.Error("IndexCoord recycleUnusedIndexFiles Remove index files failed",
							zap.Bool("MarkDeleted", true), zap.Error(err))
					}
					i.metaTable.DeleteIndex(meta.indexMeta.IndexBuildID)
					log.Debug("IndexCoord recycleUnusedIndexFiles",
						zap.Int64("Recycle the index files successfully for deleted index with indexBuildID", meta.indexMeta.IndexBuildID))
				} else {
					log.Debug("IndexCoord recycleUnusedIndexFiles",
						zap.Int64("Recycle the low version index files of the index with indexBuildID", meta.indexMeta.IndexBuildID),
						zap.Int64("indexMeta version", meta.indexMeta.Version))
					for j := 1; j < int(meta.indexMeta.Version); j++ {
						unusedIndexFilePathPrefix := Params.IndexCoordCfg.IndexStorageRootPath + "/" + strconv.Itoa(int(meta.indexMeta.IndexBuildID)) + "/" + strconv.Itoa(j)
						if err := i.chunkManager.RemoveWithPrefix(unusedIndexFilePathPrefix); err != nil {
							log.Error("IndexCoord recycleUnusedIndexFiles Remove index files failed",
								zap.Bool("MarkDeleted", false), zap.Error(err))
						}
					}
					if err := i.metaTable.UpdateRecycleState(meta.indexMeta.IndexBuildID); err != nil {
						log.Error("IndexCoord recycleUnusedIndexFiles UpdateRecycleState failed", zap.Error(err))
					}
					log.Debug("IndexCoord recycleUnusedIndexFiles",
						zap.Int64("Recycle the low version index files successfully of the index with indexBuildID", meta.indexMeta.IndexBuildID))
				}
				metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.RecycledIndexTaskLabel).Inc()
			}
		}
	}
}

// watchNodeLoop is used to monitor IndexNode going online and offline.
//go:norace
// fix datarace in unittest
// startWatchService will only be invoked at start procedure
// otherwise, remove the annotation and add atomic protection
func (i *IndexCoord) watchNodeLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()
	log.Debug("IndexCoord watchNodeLoop start")

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
			log.Debug("IndexCoord watchNodeLoop event updated")
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				log.Debug("IndexCoord watchNodeLoop SessionAddEvent", zap.Int64("serverID", serverID),
					zap.String("address", event.Session.Address))
				go func() {
					err := i.nodeManager.AddNode(serverID, event.Session.Address)
					if err != nil {
						log.Error("IndexCoord", zap.Any("Add IndexNode err", err))
					}
					log.Debug("IndexCoord", zap.Int("IndexNode number", len(i.nodeManager.nodeClients)))
				}()
				i.metricsCacheManager.InvalidateSystemInfoMetrics()
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Debug("IndexCoord watchNodeLoop SessionDelEvent", zap.Int64("serverID", serverID))
				i.nodeManager.RemoveNode(serverID)
				i.metricsCacheManager.InvalidateSystemInfoMetrics()
			}
		}
	}
}

// watchMetaLoop is used to monitor whether the Meta in etcd has been changed.
func (i *IndexCoord) watchMetaLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()
	log.Debug("IndexCoord watchMetaLoop start")

	watchChan := i.metaTable.client.WatchWithPrefix(indexFilePrefix)

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchChan:
			if !ok {
				log.Warn("index coord watch meta loop failed because watch channel closed")
				return
			}
			if err := resp.Err(); err != nil {
				log.Error("received error event from etcd watcher", zap.String("path", indexFilePrefix), zap.Error(err))
				panic("failed to handle etcd request, exit..")
			}
			for _, event := range resp.Events {
				eventRevision := event.Kv.Version
				indexMeta := &indexpb.IndexMeta{}
				err := proto.Unmarshal(event.Kv.Value, indexMeta)
				indexBuildID := indexMeta.IndexBuildID
				log.Info("IndexCoord watchMetaLoop", zap.Int64("IndexBuildID", indexBuildID))
				if err != nil {
					log.Warn("IndexCoord unmarshal indexMeta failed", zap.Int64("IndexBuildID", indexBuildID),
						zap.Error(err))
					continue
				}
				switch event.Type {
				case mvccpb.PUT:
					reload := i.metaTable.LoadMetaFromETCD(indexBuildID, eventRevision)
					log.Debug("IndexCoord watchMetaLoop PUT", zap.Int64("IndexBuildID", indexBuildID), zap.Bool("reload", reload))
					if reload {
						log.Debug("This task has finished", zap.Int64("indexBuildID", indexBuildID),
							zap.Int64("Finish by IndexNode", indexMeta.NodeID),
							zap.Int64("The version of the task", indexMeta.Version))
						if err = i.tryReleaseSegmentReferLock(ctx, indexBuildID, []UniqueID{indexMeta.Req.SegmentID}); err != nil {
							panic(err)
						}
						i.nodeManager.pq.IncPriority(indexMeta.NodeID, -1)
						metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.InProgressIndexTaskLabel).Dec()
						if indexMeta.State == commonpb.IndexState_Finished {
							metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.FinishedIndexTaskLabel).Inc()
						}
						if indexMeta.State == commonpb.IndexState_Failed {
							metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.FailedIndexTaskLabel).Inc()
						}
					}
				case mvccpb.DELETE:
					log.Debug("IndexCoord watchMetaLoop DELETE", zap.Int64("The meta has been deleted of indexBuildID", indexBuildID))
				}
			}
		}
	}
}

func (i *IndexCoord) tryAcquireSegmentReferLock(ctx context.Context, buildID UniqueID, segIDs []UniqueID) error {
	// IndexCoord use buildID instead of taskID.
	status, err := i.dataCoordClient.AcquireSegmentLock(ctx, &datapb.AcquireSegmentLockRequest{
		TaskID:     buildID,
		NodeID:     i.session.ServerID,
		SegmentIDs: segIDs,
	})
	if err != nil {
		log.Error("IndexCoord try to acquire segment reference lock failed", zap.Int64s("segIDs", segIDs),
			zap.Error(err))
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("IndexCoord try to acquire segment reference lock failed", zap.Int64s("segIDs", segIDs),
			zap.Error(errors.New(status.Reason)))
		return errors.New(status.Reason)
	}
	return nil
}

func (i *IndexCoord) tryReleaseSegmentReferLock(ctx context.Context, buildID UniqueID, segIDs []UniqueID) error {
	releaseLock := func() error {
		status, err := i.dataCoordClient.ReleaseSegmentLock(ctx, &datapb.ReleaseSegmentLockRequest{
			TaskID: buildID,
			NodeID: i.session.ServerID,
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
		log.Error("IndexCoord try to release segment reference lock failed", zap.Int64s("segIDs", segIDs),
			zap.Error(err))
		return err
	}
	return nil
}

// assignTask sends the index task to the IndexNode, it has a timeout interval, if the IndexNode doesn't respond within
// the interval, it is considered that the task sending failed.
func (i *IndexCoord) assignTask(builderClient types.IndexNode, req *indexpb.CreateIndexRequest) bool {
	ctx, cancel := context.WithTimeout(i.loopCtx, i.reqTimeoutInterval)
	defer cancel()
	resp, err := builderClient.CreateIndex(ctx, req)
	if err != nil {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.Error(err))
		return false
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.String("Reason", resp.Reason))
		return false
	}
	return true
}

// assignTaskLoop is used to assign index construction tasks.
func (i *IndexCoord) assignTaskLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()

	timeTicker := time.NewTicker(i.assignTaskInterval)
	log.Debug("IndexCoord start assignTask loop")

	for {
		select {
		case <-ctx.Done():
			log.Debug("IndexCoord assignTaskLoop ctx Done")
			return
		case <-timeTicker.C:
			serverIDs := i.nodeManager.ListNode()
			if len(serverIDs) == 0 {
				log.Warn("there is no indexnode online")
				continue
			}
			metas := i.metaTable.GetUnassignedTasks(serverIDs)
			sort.Slice(metas, func(i, j int) bool {
				return metas[i].indexMeta.Version <= metas[j].indexMeta.Version
			})
			// only log if we find unassigned tasks
			if len(metas) != 0 {
				log.Debug("IndexCoord find unassigned tasks ", zap.Int("Unassigned tasks number", len(metas)), zap.Int64s("Available IndexNode IDs", serverIDs))
			}
			for index, meta := range metas {
				indexBuildID := meta.indexMeta.IndexBuildID
				segID := meta.indexMeta.Req.SegmentID
				if err := i.tryAcquireSegmentReferLock(ctx, indexBuildID, []UniqueID{segID}); err != nil {
					log.Warn("IndexCoord try to acquire segment reference lock failed, maybe this segment has been compacted",
						zap.Int64("segID", segID), zap.Int64("buildID", indexBuildID), zap.Error(err))
					continue
				}
				if err := i.metaTable.UpdateVersion(indexBuildID); err != nil {
					log.Warn("IndexCoord assignmentTasksLoop metaTable.UpdateVersion failed", zap.Error(err))
					continue
				}
				log.Debug("The version of the task has been updated", zap.Int64("indexBuildID", indexBuildID))

				nodeID, builderClient := i.nodeManager.PeekClient(meta)
				if builderClient == nil && nodeID == -1 {
					log.Warn("there is no indexnode online")
					break
				}
				if builderClient == nil && nodeID == 0 {
					log.Warn("The memory of all indexnodes does not meet the requirements")
					continue
				}
				log.Debug("IndexCoord PeekClient success", zap.Int64("nodeID", nodeID))
				req := &indexpb.CreateIndexRequest{
					IndexBuildID: indexBuildID,
					IndexName:    meta.indexMeta.Req.IndexName,
					IndexID:      meta.indexMeta.Req.IndexID,
					Version:      meta.indexMeta.Version + 1,
					MetaPath:     path.Join(indexFilePrefix, strconv.FormatInt(indexBuildID, 10)),
					DataPaths:    meta.indexMeta.Req.DataPaths,
					TypeParams:   meta.indexMeta.Req.TypeParams,
					IndexParams:  meta.indexMeta.Req.IndexParams,
				}
				if !i.assignTask(builderClient, req) {
					log.Warn("IndexCoord assignTask assign task to IndexNode failed")
					continue
				}
				if err := i.metaTable.BuildIndex(indexBuildID, nodeID); err != nil {
					log.Error("IndexCoord assignmentTasksLoop metaTable.BuildIndex failed", zap.Error(err))
					break
				}
				log.Debug("This task has been assigned successfully", zap.Int64("indexBuildID", indexBuildID), zap.Int64("nodeID", nodeID))
				i.nodeManager.pq.IncPriority(nodeID, 1)
				if index > i.taskLimit {
					break
				}
			}
		}
	}
}
