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

// Package datanode implements data persistence logic.
//
// Data node persists insert logs into persistent storage like minIO/S3.
package datanode

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/gjson"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/datanode/external"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// ConnectEtcdMaxRetryTime is used to limit the max retry time for connection etcd
	ConnectEtcdMaxRetryTime = 100
)

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

// Params from config.yaml
var Params *paramtable.ComponentParam = paramtable.Get()

// DataNode communicates with outside services and unioun all
// services in datanode package.
//
// DataNode implements `types.Component`, `types.DataNode` interfaces.
//
//	`etcdCli`   is a connection of etcd
//	`rootCoord` is a grpc client of root coordinator.
//	`dataCoord` is a grpc client of data service.
//	`stateCode` is current statement of this data node, indicating whether it's healthy.
type DataNode struct {
	ctx      context.Context
	cancel   context.CancelFunc
	Role     string
	lifetime lifetime.Lifetime[commonpb.StateCode]

	syncMgr         syncmgr.SyncManager
	importTaskMgr   importv2.TaskManager
	importScheduler importv2.Scheduler

	// indexnode related
	storageFactory StorageFactory
	taskScheduler  *index.TaskScheduler
	taskManager    *index.TaskManager

	externalCollectionManager *external.ExternalCollectionManager

	compactionExecutor compactor.Executor

	etcdCli *clientv3.Client
	address string

	// call once
	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once
	sessionMu sync.Mutex // to fix data race
	session   *sessionutil.Session

	closer io.Closer

	reportImportRetryTimes uint // unitest set this value to 1 to save time, default is 10
	pool                   *conc.Pool[any]

	metricsRequest *metricsinfo.MetricsRequest
}

// NewDataNode will return a DataNode with abnormal state.
func NewDataNode(ctx context.Context) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:                    ctx2,
		cancel:                 cancel2,
		Role:                   typeutil.DataNodeRole,
		lifetime:               lifetime.NewLifetime(commonpb.StateCode_Abnormal),
		compactionExecutor:     compactor.NewExecutor(),
		reportImportRetryTimes: 10,
		metricsRequest:         metricsinfo.NewMetricsRequest(),
	}
	sc := index.NewTaskScheduler(ctx2)
	node.storageFactory = NewChunkMgrFactory()
	node.taskScheduler = sc
	node.taskManager = index.NewTaskManager(ctx2)
	node.externalCollectionManager = external.NewExternalCollectionManager(ctx2, 8)
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	expr.Register("datanode", node)
	return node
}

func (node *DataNode) SetAddress(address string) {
	node.address = address
}

func (node *DataNode) GetAddress() string {
	return node.address
}

// SetEtcdClient sets etcd client for DataNode
func (node *DataNode) SetEtcdClient(etcdCli *clientv3.Client) {
	node.etcdCli = etcdCli
}

// SetRootCoordClient sets RootCoord's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetMixCoordClient(mixc types.MixCoordClient) error {
	return nil
}

// Register register datanode to etcd
func (node *DataNode) Register() error {
	log := log.Ctx(node.ctx)
	log.Debug("node begin to register to etcd", zap.String("serverName", node.session.ServerName), zap.Int64("ServerID", node.session.ServerID))
	node.session.Register()

	metrics.NumNodes.WithLabelValues(fmt.Sprint(node.GetNodeID()), typeutil.DataNodeRole).Inc()
	log.Info("DataNode Register Finished")
	return nil
}

func (node *DataNode) initSession() error {
	node.session = sessionutil.NewSession(node.ctx)
	if node.session == nil {
		return errors.New("failed to initialize session")
	}
	node.session.Init(typeutil.DataNodeRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.DataNodeRole, node.session.ServerID)
	return nil
}

func (node *DataNode) GetNodeID() int64 {
	if node.session != nil {
		return node.session.ServerID
	}
	return paramtable.GetNodeID()
}

func (node *DataNode) Init() error {
	var initError error
	node.initOnce.Do(func() {
		node.registerMetricsRequest()
		log.Ctx(node.ctx).Info("DataNode server initializing")
		if err := node.initSession(); err != nil {
			log.Error("DataNode server init session failed", zap.Error(err))
			initError = err
			return
		}

		serverID := node.GetNodeID()
		log := log.Ctx(node.ctx).With(zap.String("role", typeutil.DataNodeRole), zap.Int64("nodeID", serverID))
		log.Info("DataNode server init succeeded")

		syncMgr := syncmgr.NewSyncManager(nil)
		node.syncMgr = syncMgr

		fileMode := fileresource.ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue())
		if fileMode == fileresource.SyncMode {
			cm, err := node.storageFactory.NewChunkManager(node.ctx, compaction.CreateStorageConfig())
			if err != nil {
				log.Error("Init chunk manager for file resource manager failed", zap.Error(err))
				initError = err
				return
			}
			fileresource.InitManager(cm, fileMode)
		} else {
			fileresource.InitManager(nil, fileMode)
		}

		node.importTaskMgr = importv2.NewTaskManager()
		node.importScheduler = importv2.NewScheduler(node.importTaskMgr)

		err := index.InitSegcore(serverID)
		if err != nil {
			initError = err
		}

		analyzer.InitOptions()
		log.Info("init datanode done", zap.String("Address", node.address))
	})
	return initError
}

func (node *DataNode) registerMetricsRequest() {
	node.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return node.getSystemInfoMetrics(ctx, req)
		})

	node.metricsRequest.RegisterMetricsRequest(metricsinfo.SyncTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return node.syncMgr.TaskStatsJSON(), nil
		})
	log.Ctx(node.ctx).Info("register metrics actions finished")
}

// Start will update DataNode state to HEALTHY
func (node *DataNode) Start() error {
	log := log.Ctx(node.ctx)
	var startErr error
	node.startOnce.Do(func() {
		go node.compactionExecutor.Start(node.ctx)

		go node.importScheduler.Start()

		err := node.taskScheduler.Start()
		if err != nil {
			startErr = err
			return
		}

		node.UpdateStateCode(commonpb.StateCode_Healthy)
		log.Info("datanode start successfully")
	})
	return startErr
}

// UpdateStateCode updates datanode's state code
func (node *DataNode) UpdateStateCode(code commonpb.StateCode) {
	node.lifetime.SetState(code)
}

// GetStateCode return datanode's state code
func (node *DataNode) GetStateCode() commonpb.StateCode {
	return node.lifetime.GetState()
}

func (node *DataNode) isHealthy() bool {
	return node.GetStateCode() == commonpb.StateCode_Healthy
}

// ReadyToFlush tells whether DataNode is ready for flushing
func (node *DataNode) ReadyToFlush() error {
	if !node.isHealthy() {
		return errors.New("DataNode not in HEALTHY state")
	}
	return nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	node.stopOnce.Do(func() {
		// https://github.com/milvus-io/milvus/issues/12282
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.lifetime.Wait()

		if node.syncMgr != nil {
			err := node.syncMgr.Close()
			if err != nil {
				log.Error("sync manager close failed", zap.Error(err))
			}
		}

		if node.closer != nil {
			node.closer.Close()
		}

		if node.session != nil {
			node.session.Stop()
		}

		if node.importScheduler != nil {
			node.importScheduler.Close()
		}

		if node.externalCollectionManager != nil {
			node.externalCollectionManager.Close()
		}

		// cleanup all running tasks
		node.taskManager.DeleteAllTasks()

		if node.taskScheduler != nil {
			node.taskScheduler.Close()
		}

		index.CloseSegcore()

		metrics.CleanupDataNodeCompactionMetrics(paramtable.GetNodeID())

		// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed the flow graph
		node.cancel()
	})
	return nil
}

// SetSession to fix data race
func (node *DataNode) SetSession(session *sessionutil.Session) {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	node.session = session
}

// GetSession to fix data race
func (node *DataNode) GetSession() *sessionutil.Session {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	return node.session
}
