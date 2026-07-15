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

package querycoordv2

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/expr"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Only for re-export
var Params = params.Params

type Server struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	status              atomic.Int32
	etcdCli             *clientv3.Client
	tikvCli             *txnkv.Client
	address             string
	session             sessionutil.SessionInterface
	sessionWatcher      sessionutil.SessionWatcher
	sessionWatcherMu    sync.Mutex
	kv                  kv.MetaKv
	idAllocator         func() (int64, error)
	metricsCacheManager *metricsinfo.MetricsCacheManager

	// Coordinators
	mixCoord types.MixCoord

	// Meta
	store     metastore.QueryCoordCatalog
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr meta.TargetManagerInterface
	broker    meta.Broker

	// Session
	cluster          session.Cluster
	nodeMgr          *session.NodeManager
	queryNodeCreator session.QueryNodeCreator

	// Schedulers
	jobScheduler  *job.Scheduler
	taskScheduler task.Scheduler

	// HeartBeat
	distController dist.Controller

	// Checkers
	checkerController *checkers.CheckerController

	// Observers
	collectionObserver   *observers.CollectionObserver
	targetObserver       *observers.TargetObserver
	replicaObserver      *observers.ReplicaObserver
	resourceObserver     *observers.ResourceObserver
	leaderCacheObserver  *observers.LeaderCacheObserver
	fileResourceObserver FileResourceObserver

	// Active-standby
	enableActiveStandBy bool
	activateFunc        func() error

	// proxy client manager
	proxyCreator       proxyutil.ProxyCreator
	proxyWatcher       proxyutil.ProxyWatcherInterface
	proxyClientManager proxyutil.ProxyClientManagerInterface

	metricsRequest *metricsinfo.MetricsRequest

	// for balance streaming node request
	// now only used for run analyzer and validate analyzer
	nodeIdx atomic.Uint32

	// load config watcher
	loadConfigWatcher *LoadConfigWatcher
}

type FileResourceObserver interface {
	InitQueryCoord(manager *session.NodeManager, cluster session.Cluster)
	Notify()
}

func NewQueryCoord(ctx context.Context) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:            ctx,
		cancel:         cancel,
		metricsRequest: metricsinfo.NewMetricsRequest(),
	}
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	server.queryNodeCreator = session.DefaultQueryNodeCreator
	expr.Register("querycoord", server)
	return server, nil
}

func (s *Server) Register() error {
	return nil
}

func (s *Server) SetSession(session sessionutil.SessionInterface) error {
	s.session = session
	if s.session == nil {
		return merr.WrapErrServiceNotReadyMsg("session is nil, the etcd client connection may have failed")
	}
	return nil
}

func (s *Server) ServerExist(serverID int64) bool {
	sessions, _, err := s.session.GetSessions(s.ctx, typeutil.QueryNodeRole)
	if err != nil {
		mlog.Warn(s.ctx, "failed to get sessions", mlog.Err(err))
		return false
	}
	sessionMap := lo.MapKeys(sessions, func(s *sessionutil.Session, _ string) int64 {
		return s.ServerID
	})
	_, exists := sessionMap[serverID]
	return exists
}

func (s *Server) registerMetricsRequest() {
	getSystemInfoAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.getSystemInfoMetrics(ctx, req)
	}

	QueryTasksAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.taskScheduler.GetTasksJSON(), nil
	}

	QueryDistAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
		return s.dist.GetDistributionJSON(collectionID), nil
	}

	QueryTargetAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		scope := meta.CurrentTarget
		v := jsonReq.Get(metricsinfo.MetricRequestParamTargetScopeKey)
		if v.Exists() {
			scope = meta.TargetScope(v.Int())
		}

		collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
		return s.targetMgr.GetTargetJSON(ctx, scope, collectionID), nil
	}

	QueryReplicasAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.meta.GetReplicasJSON(ctx, s.meta), nil
	}

	QueryResourceGroupsAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.meta.GetResourceGroupsJSON(ctx), nil
	}

	QuerySegmentsAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.getSegmentsJSON(ctx, req, jsonReq)
	}

	QueryChannelsAction := func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
		return s.getChannelsFromQueryNode(ctx, req)
	}

	// register actions that requests are processed in querycoord
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics, getSystemInfoAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.AllTaskKey, QueryTasksAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.DistKey, QueryDistAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.TargetKey, QueryTargetAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ReplicaKey, QueryReplicasAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ResourceGroupKey, QueryResourceGroupsAction)

	// register actions that requests are processed in querynode
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SegmentKey, QuerySegmentsAction)
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ChannelKey, QueryChannelsAction)
	mlog.Info(s.ctx, "register metrics actions finished")
}

func (s *Server) SetFileResourceObserver(observer FileResourceObserver) {
	s.fileResourceObserver = observer
}

func (s *Server) Init() error {
	mlog.Info(s.ctx, "QueryCoord start init",
		mlog.String("meta-root-path", Params.EtcdCfg.MetaRootPath.GetValue()),
		mlog.String("address", s.address))

	s.registerMetricsRequest()
	if err := s.initSession(); err != nil {
		return err
	}

	return s.initQueryCoord()
}

func (s *Server) initSession() error {
	// Init QueryCoord session
	if s.session == nil {
		s.session = sessionutil.NewSession(s.ctx)
		s.session.Init(typeutil.QueryCoordRole, s.address, true)
		s.enableActiveStandBy = Params.QueryCoordCfg.EnableActiveStandby.GetAsBool()
		s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	return nil
}

func (s *Server) initQueryCoord() error {
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	mlog.Info(s.ctx, "start init querycoord", mlog.Any("State", commonpb.StateCode_Initializing))
	// Init KV and ID allocator
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	var idAllocatorKV kv.TxnKV
	mlog.Info(s.ctx,
		fmt.Sprintf("query coordinator connecting to %s.", metaType))
	switch metaType {
	case util.MetaStoreTypeTiKV:
		s.kv = tikv.NewTiKV(s.tikvCli, Params.TiKVCfg.MetaRootPath.GetValue(),
			tikv.WithRequestTimeout(paramtable.Get().TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
		idAllocatorKV = tsoutil.NewTSOTiKVBase(s.tikvCli, Params.TiKVCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	case util.MetaStoreTypeEtcd:
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
			etcdkv.WithRequestTimeout(paramtable.Get().EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
		idAllocatorKV = tsoutil.NewTSOKVBase(s.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	default:
		return merr.WrapErrServiceInternalMsg("unsupported meta store: %s", metaType)
	}
	mlog.Info(s.ctx,
		fmt.Sprintf("query coordinator successfully connected to %s.", metaType))

	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
	err := idAllocator.Initialize()
	if err != nil {
		mlog.Error(s.ctx, "query coordinator id allocator initialize failed", mlog.Err(err))
		return err
	}
	s.idAllocator = func() (int64, error) {
		return idAllocator.AllocOne()
	}
	mlog.Info(s.ctx, "init ID allocator done")

	// Init metrics cache manager
	s.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	// Init meta
	s.nodeMgr = session.NewNodeManager()
	s.nodeMgr.Start(s.ctx)
	err = s.initMeta()
	if err != nil {
		return err
	}
	// Init session
	mlog.Info(s.ctx, "init session")
	s.cluster = session.NewCluster(s.nodeMgr, s.queryNodeCreator)

	// Init schedulers
	mlog.Info(s.ctx, "init schedulers")
	s.jobScheduler = job.NewScheduler()
	concreteTaskScheduler := task.NewScheduler(
		s.ctx,
		s.meta,
		s.dist,
		s.targetMgr,
		s.broker,
		s.cluster,
		s.nodeMgr,
	)
	s.taskScheduler = concreteTaskScheduler

	// init proxy client manager
	s.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)
	s.proxyWatcher = proxyutil.NewProxyWatcher(
		s.etcdCli,
		s.proxyClientManager.SetProxyClients,
	)
	s.proxyWatcher.AddSessionFunc(s.proxyClientManager.AddProxyClient)
	s.proxyWatcher.DelSessionFunc(s.proxyClientManager.DelProxyClient)
	mlog.Info(s.ctx, "init proxy manager done")

	// Init global assign policy factory
	mlog.Info(s.ctx, "init global assign policy factory")
	assign.InitGlobalAssignPolicyFactory(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)

	// Init global balancer factory
	mlog.Info(s.ctx, "init global balancer factory")
	balance.InitGlobalBalancerFactory(s.taskScheduler, s.nodeMgr, s.dist, s.targetMgr)

	// Init checker controller
	mlog.Info(s.ctx, "init checker controller")
	s.checkerController = checkers.NewCheckerController(
		s.meta,
		s.dist,
		s.targetMgr,
		s.nodeMgr,
		s.taskScheduler,
		s.broker,
	)

	// Init observers
	s.initObserver()

	// Init heartbeat
	mlog.Info(s.ctx, "init dist controller")
	s.distController = dist.NewDistController(
		s.cluster,
		s.nodeMgr,
		s.dist,
		s.targetMgr,
		s.taskScheduler,
		s.leaderCacheObserver.RegisterEvent,
	)

	// Init load status cache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	RegisterDDLCallbacks(s)
	mlog.Info(s.ctx, "init querycoord done", mlog.FieldNodeID(paramtable.GetNodeID()), mlog.String("Address", s.address))
	return err
}

func (s *Server) initMeta() error {
	record := timerecord.NewTimeRecorder("querycoord")

	mlog.Info(s.ctx, "init meta")
	s.store = querycoord.NewCatalog(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store, s.nodeMgr)

	s.broker = meta.NewCoordinatorBroker(
		s.mixCoord,
	)

	mlog.Info(s.ctx, "recover meta...")
	err := s.meta.CollectionManager.Recover(s.ctx, s.broker)
	if err != nil {
		mlog.Warn(s.ctx, "failed to recover collections", mlog.Err(err))
		return err
	}
	collections := s.meta.GetAll(s.ctx)
	mlog.Info(s.ctx, "recovering collections...", mlog.Int64s("collections", collections))

	// We really update the metric after observers think the collection loaded.
	metrics.QueryCoordNumCollections.WithLabelValues().Set(0)

	metrics.QueryCoordNumPartitions.WithLabelValues().Set(float64(len(s.meta.GetAllPartitions(s.ctx))))

	err = s.meta.ReplicaManager.Recover(s.ctx, collections)
	if err != nil {
		mlog.Warn(s.ctx, "failed to recover replicas", mlog.Err(err))
		return err
	}

	err = s.meta.ResourceManager.Recover(s.ctx)
	if err != nil {
		mlog.Warn(s.ctx, "failed to recover resource groups", mlog.Err(err))
		return err
	}

	s.dist = meta.NewDistributionManager(s.nodeMgr)
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)
	err = s.targetMgr.Recover(s.ctx, s.store)
	if err != nil {
		mlog.Warn(s.ctx, "failed to recover collection targets", mlog.Err(err))
	}

	mlog.Info(s.ctx, "QueryCoord server initMeta done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *Server) initObserver() {
	mlog.Info(s.ctx, "init observers")
	s.targetObserver = observers.NewTargetObserver(
		s.meta,
		s.targetMgr,
		s.dist,
		s.broker,
		s.cluster,
		s.nodeMgr,
	)
	s.collectionObserver = observers.NewCollectionObserver(
		s.dist,
		s.meta,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
		s.proxyClientManager,
	)

	s.replicaObserver = observers.NewReplicaObserver(
		s.meta,
		s.dist,
		s.targetMgr,
	)

	s.resourceObserver = observers.NewResourceObserver(s.meta)

	s.leaderCacheObserver = observers.NewLeaderCacheObserver(
		s.proxyClientManager,
	)

	if s.fileResourceObserver != nil {
		s.fileResourceObserver.InitQueryCoord(s.nodeMgr, s.cluster)
	}
}

func (s *Server) afterStart() {}

func (s *Server) Start() error {
	if err := s.startQueryCoord(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "QueryCoord started")

	return nil
}

func (s *Server) startQueryCoord() error {
	mlog.Info(s.ctx, "start watcher...")
	sessions, revision, err := s.session.GetSessions(s.ctx, typeutil.QueryNodeRole)
	if err != nil {
		return err
	}

	mlog.Info(s.ctx, "rewatch nodes", mlog.Any("sessions", sessions))
	err = s.rewatchNodes(sessions)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go s.watchNodes(revision)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err := s.proxyWatcher.WatchProxy(s.ctx); err != nil {
		mlog.Warn(s.ctx, "querycoord failed to watch proxy", mlog.Err(err))
	}

	s.startServerLoop()
	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.MixCoordRole, s.session.GetServerID())
	// check replica changes after restart
	// Note: this should be called after start progress is done
	s.watchLoadConfigChanges()
	return nil
}

func (s *Server) startServerLoop() {
	// leader cache observer shall be started before `SyncAll` call
	s.leaderCacheObserver.Start(s.ctx)
	// Recover dist, to avoid generate too much task when dist not ready after restart
	s.distController.SyncAll(s.ctx)

	// start the components from inside to outside,
	// to make the dependencies ready for every component
	mlog.Info(s.ctx, "start cluster...")
	s.cluster.Start()

	mlog.Info(s.ctx, "start observers...")
	s.collectionObserver.Start()
	s.targetObserver.Start()
	s.replicaObserver.Start()
	s.resourceObserver.Start()

	mlog.Info(s.ctx, "start task scheduler...")
	s.taskScheduler.Start()

	mlog.Info(s.ctx, "start checker controller...")
	s.checkerController.Start()

	mlog.Info(s.ctx, "start job scheduler...")
	s.jobScheduler.Start()
}

func (s *Server) Stop() error {
	// FOLLOW the dependence graph:
	// job scheduler -> checker controller -> task scheduler -> dist controller -> cluster -> session
	// observers -> dist controller

	if s.loadConfigWatcher != nil {
		mlog.Info(s.ctx, "stop load config watcher...")
		s.loadConfigWatcher.Close()
	}

	if s.jobScheduler != nil {
		mlog.Info(s.ctx, "stop job scheduler...")
		s.jobScheduler.Stop()
	}

	if s.checkerController != nil {
		mlog.Info(s.ctx, "stop checker controller...")
		s.checkerController.Stop()
	}

	if s.taskScheduler != nil {
		mlog.Info(s.ctx, "stop task scheduler...")
		s.taskScheduler.Stop()
	}

	mlog.Info(s.ctx, "stop observers...")
	if s.collectionObserver != nil {
		s.collectionObserver.Stop()
	}
	if s.targetObserver != nil {
		s.targetObserver.Stop()
	}

	// save target to meta store, after querycoord restart, make it fast to recover current target
	// should save target after target observer stop, incase of target changed
	if s.targetMgr != nil {
		s.targetMgr.SaveCurrentTarget(s.ctx, s.store)
	}

	if s.replicaObserver != nil {
		s.replicaObserver.Stop()
	}
	if s.resourceObserver != nil {
		s.resourceObserver.Stop()
	}
	if s.leaderCacheObserver != nil {
		s.leaderCacheObserver.Stop()
	}

	if s.distController != nil {
		mlog.Info(s.ctx, "stop dist controller...")
		s.distController.Stop()
	}

	if s.cluster != nil {
		mlog.Info(s.ctx, "stop cluster...")
		s.cluster.Stop()
	}

	s.sessionWatcherMu.Lock()
	if s.sessionWatcher != nil {
		s.sessionWatcher.Stop()
	}
	s.sessionWatcherMu.Unlock()

	s.cancel()
	s.wg.Wait()

	if s.session != nil {
		s.session.Stop()
	}

	mlog.Info(s.ctx, "QueryCoord stop successfully")
	return nil
}

// UpdateStateCode updates the status of the coord, including healthy, unhealthy
func (s *Server) UpdateStateCode(code commonpb.StateCode) {
	s.status.Store(int32(code))
	mlog.Info(s.ctx, "update querycoord state", mlog.String("state", code.String()))
}

func (s *Server) State() commonpb.StateCode {
	return commonpb.StateCode(s.status.Load())
}

func (s *Server) SetAddress(address string) {
	s.address = address
}

// SetEtcdClient sets etcd's client
func (s *Server) SetEtcdClient(etcdClient *clientv3.Client) {
	s.etcdCli = etcdClient
}

func (s *Server) SetTiKVClient(client *txnkv.Client) {
	s.tikvCli = client
}

func (s *Server) SetMixCoord(mixCoord types.MixCoord) {
	s.mixCoord = mixCoord
}

func (s *Server) SetQueryNodeCreator(f func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)) {
	s.queryNodeCreator = f
}

func (s *Server) watchNodes(revision int64) {
	defer s.wg.Done()

	s.sessionWatcherMu.Lock()
	s.sessionWatcher = s.session.WatchServices(typeutil.QueryNodeRole, revision+1, s.rewatchNodes)
	s.sessionWatcherMu.Unlock()
	for {
		select {
		case <-s.ctx.Done():
			mlog.Info(s.ctx, "stop watching nodes, QueryCoord stopped")
			return

		case event, ok := <-s.sessionWatcher.EventChannel():
			if !ok {
				// ErrCompacted is handled inside SessionWatcher
				mlog.Warn(s.ctx, "Session Watcher channel closed", mlog.Int64("serverID", paramtable.GetNodeID()))
				if s.ctx.Err() == nil {
					// ctx is still active, meaning this is not a normal shutdown but a genuine watch failure.
					// Force exit so the process can be restarted by the orchestrator (e.g. K8s).
					mlog.Error(s.ctx, "force exit due to unexpected session watcher failure")
					mlog.Cleanup()
					os.Exit(sessionutil.ExitCodeEtcd)
				}
				return
			}

			nodeID := event.Session.ServerID
			addr := event.Session.Address
			log := mlog.With(
				mlog.FieldNodeID(nodeID),
				mlog.String("nodeAddr", addr),
			)

			switch event.EventType {
			case sessionutil.SessionAddEvent:
				s.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   nodeID,
					Address:  addr,
					Hostname: event.Session.HostName,
					Version:  event.Session.Version,
					Labels:   event.Session.GetServerLabel(),
				}))
				s.handleNodeUp(nodeID)
				if s.fileResourceObserver != nil {
					s.fileResourceObserver.Notify()
				}

			case sessionutil.SessionUpdateEvent:
				log.Info(s.ctx, "stopping the node")
				s.nodeMgr.Stopping(nodeID)
				s.handleNodeStopping(nodeID)

			case sessionutil.SessionDelEvent:
				log.Info(s.ctx, "a node down, remove it")
				s.nodeMgr.Remove(nodeID)
				s.handleNodeDown(nodeID)
			}
		}
	}
}

// rewatchNodes is used to re-watch nodes when querycoord restart or reconnect to etcd
// Note: may apply same node multiple times, so rewatchNodes must be idempotent
func (s *Server) rewatchNodes(sessions map[string]*sessionutil.Session) error {
	sessionMap := lo.MapKeys(sessions, func(s *sessionutil.Session, _ string) int64 {
		return s.ServerID
	})

	// first remove all offline nodes
	for _, node := range s.nodeMgr.GetAll() {
		nodeSession, ok := sessionMap[node.ID()]
		if !ok {
			// node in node manager but session not exist, means it's offline
			s.nodeMgr.Remove(node.ID())
			s.handleNodeDown(node.ID())
		} else {
			if nodeSession.Stopping && !node.IsStoppingState() {
				// node in node manager but session is stopping, means it's stopping
				mlog.Warn(s.ctx, "rewatch found old querynode in stopping state", mlog.FieldNodeID(nodeSession.ServerID))
				s.nodeMgr.Stopping(node.ID())
				s.handleNodeStopping(node.ID())
			}
			delete(sessionMap, node.ID())
		}
	}

	// then add all on new online nodes
	for _, nodeSession := range sessionMap {
		nodeInfo := s.nodeMgr.Get(nodeSession.ServerID)
		if nodeInfo == nil {
			s.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
				NodeID:   nodeSession.GetServerID(),
				Address:  nodeSession.GetAddress(),
				Hostname: nodeSession.HostName,
				Version:  nodeSession.Version,
				Labels:   nodeSession.GetServerLabel(),
			}))

			// call handleNodeUp no matter what state new querynode is in
			// all component need this op so that stopping balance could work correctly
			s.handleNodeUp(nodeSession.GetServerID())

			if nodeSession.Stopping {
				mlog.Warn(s.ctx, "rewatch found new querynode in stopping state", mlog.FieldNodeID(nodeSession.ServerID))
				s.nodeMgr.Stopping(nodeSession.ServerID)
				s.handleNodeStopping(nodeSession.ServerID)
			}
		}
	}

	// Note: Node manager doesn't persist node list, so after query coord restart, we cannot
	// update all node statuses in resource manager based on session and node manager's node list.
	// Therefore, manual status checking of all nodes in resource manager is needed.
	s.meta.CheckNodesInResourceGroup(s.ctx)

	return nil
}

func (s *Server) handleNodeUp(node int64) {
	nodeInfo := s.nodeMgr.Get(node)
	if nodeInfo == nil {
		mlog.Warn(s.ctx, "node already down", mlog.FieldNodeID(node))
		return
	}

	// add executor to task scheduler
	s.taskScheduler.AddExecutor(node)

	// start dist handler
	s.distController.StartDistInstance(s.ctx, node)

	// need assign to new rg and replica
	s.meta.HandleNodeUp(s.ctx, node)

	s.metricsCacheManager.InvalidateSystemInfoMetrics()
	s.checkerController.Check()
}

func (s *Server) handleNodeDown(node int64) {
	s.taskScheduler.RemoveExecutor(node)
	s.distController.Remove(node)

	// Clear dist
	s.dist.RemoveNodeDistribution(node)

	// Clear tasks
	s.taskScheduler.RemoveByNode(node)

	s.meta.HandleNodeDown(context.Background(), node)

	// clean node's metrics
	metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(node))
	s.metricsCacheManager.InvalidateSystemInfoMetrics()
}

func (s *Server) handleNodeStopping(node int64) {
	// mark node as stopping in node manager
	s.nodeMgr.Stopping(node)

	// mark node as stopping in resource manager
	s.meta.HandleNodeStopping(context.Background(), node)

	// trigger checker to check stopping node
	s.checkerController.Check()
}

func (s *Server) updateBalanceConfigLoop(ctx context.Context) {
	success := s.updateBalanceConfig()
	if success {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		interval := Params.QueryCoordCfg.CheckAutoBalanceConfigInterval.GetAsDuration(time.Second)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				mlog.Info(ctx, "update balance config loop exit!")
				return

			case <-ticker.C:
				success := s.updateBalanceConfig()
				if success {
					return
				}
				// apply dynamic update only when changed
				newInterval := Params.QueryCoordCfg.CheckAutoBalanceConfigInterval.GetAsDuration(time.Second)
				if newInterval != interval {
					interval = newInterval
					select {
					case <-ticker.C:
					default:
					}
					ticker.Reset(interval)
				}
			}
		}
	}()
}

func (s *Server) updateBalanceConfig() bool {
	r := semver.MustParseRange("<2.3.0")
	sessions, _, err := s.session.GetSessionsWithVersionRange(typeutil.QueryNodeRole, r)
	if err != nil {
		mlog.Warn(s.ctx, "check query node version occur error on etcd", mlog.Err(err))
		return false
	}

	if len(sessions) == 0 {
		// only balance channel when all query node's version >= 2.3.0
		Params.Reset(Params.QueryCoordCfg.AutoBalance.Key)
		mlog.Info(s.ctx, "all old query node down, enable auto balance!")
		return true
	}

	Params.Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
	mlog.RatedDebug(s.ctx, rate.Limit(10), "old query node exist", mlog.Strings("sessions", lo.Keys(sessions)))
	return false
}

func (s *Server) watchLoadConfigChanges() {
	w := NewLoadConfigWatcher(s)
	s.loadConfigWatcher = w
	w.Trigger()

	replicaNumHandler := config.NewHandler("watchReplicaNumberChanges", func(e *config.Event) { w.Trigger() })
	paramtable.Get().Watch(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, replicaNumHandler)

	rgHandler := config.NewHandler("watchResourceGroupChanges", func(e *config.Event) { w.Trigger() })
	paramtable.Get().Watch(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, rgHandler)

	forceOverrideHandler := config.NewHandler("watchForceOverrideUserReplicaModeChanges", func(e *config.Event) { w.Trigger() })
	paramtable.Get().Watch(paramtable.Get().QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, forceOverrideHandler)
}

// GetInternalReplicasByCollection returns replicas for a collection from internal meta.
// This method provides access to internal replica information including resource groups.
func (s *Server) GetInternalReplicasByCollection(ctx context.Context, collectionID int64) []*meta.Replica {
	return s.meta.GetByCollection(ctx, collectionID)
}

// IsCollectionUserSpecifiedReplicaMode returns whether the collection load config
// was created from a request with an explicit replica number.
func (s *Server) IsCollectionUserSpecifiedReplicaMode(ctx context.Context, collectionID int64) bool {
	if s.meta == nil {
		return false
	}
	collection := s.meta.GetCollection(ctx, collectionID)
	return collection != nil && collection.UserSpecifiedReplicaMode
}

// CheckAllReplicasServiceable returns an error if any replica has a non-serviceable
// shard leader for any channel in the collection's current target. Unlike
// CalculateLoadPercentage (which reads the CollectionObserver's periodically-persisted
// snapshot), this performs a live check against the distribution manager and so
// reflects the real-time serviceability after scale-up / scale-down.
func (s *Server) CheckAllReplicasServiceable(ctx context.Context, collectionID int64) error {
	replicas := s.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return merr.WrapErrServiceInternalMsg("no replica found")
	}
	for _, replica := range replicas {
		if err := s.checkReplicaServiceable(ctx, replica); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) checkReplicaServiceable(ctx context.Context, replica *meta.Replica) error {
	channels := s.targetMgr.GetDmChannelsByCollection(ctx, replica.GetCollectionID(), meta.CurrentTarget)
	if len(channels) == 0 {
		return merr.WrapErrServiceInternalMsg("no channels in current target")
	}
	for channelName := range channels {
		leader := s.dist.ChannelDistManager.GetShardLeader(channelName, replica)
		if leader == nil || leader.View == nil {
			return merr.WrapErrServiceInternalMsg("replica %d (rg=%s): no leader for channel %s",
				replica.GetID(), replica.GetResourceGroup(), channelName)
		}
		if err := utils.CheckDelegatorDataReady(s.nodeMgr, s.targetMgr, leader.View, meta.CurrentTarget); err != nil {
			return merr.Wrapf(err, "replica %d (rg=%s) not serviceable", replica.GetID(), replica.GetResourceGroup())
		}
		if !leader.IsServiceable() {
			err := merr.WrapErrChannelNotAvailable(channelName, "delegator reported not serviceable")
			return merr.Wrapf(err, "replica %d (rg=%s) not serviceable", replica.GetID(), replica.GetResourceGroup())
		}
	}
	return nil
}

// GetLeakedResourcesByCollection returns the number of segments and channels still held by
// querynodes that are NOT part of any current replica of the collection. A non-zero result
// means physical resources have not been fully released yet (e.g., during scale-down a
// decommissioned replica's querynode may still hold segments while release RPCs are in flight).
func (s *Server) GetLeakedResourcesByCollection(ctx context.Context, collectionID int64) (leakedSegments, leakedChannels int) {
	replicas := s.meta.GetByCollection(ctx, collectionID)
	validNodes := typeutil.NewUniqueSet()
	for _, r := range replicas {
		validNodes.Insert(r.GetNodes()...)
	}
	for _, seg := range s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID)) {
		if !validNodes.Contain(seg.Node) {
			leakedSegments++
		}
	}
	for _, ch := range s.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID)) {
		if !validNodes.Contain(ch.Node) {
			leakedChannels++
		}
	}
	return leakedSegments, leakedChannels
}
