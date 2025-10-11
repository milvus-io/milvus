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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	collectionObserver  *observers.CollectionObserver
	targetObserver      *observers.TargetObserver
	replicaObserver     *observers.ReplicaObserver
	resourceObserver    *observers.ResourceObserver
	leaderCacheObserver *observers.LeaderCacheObserver

	getBalancerFunc checkers.GetBalancerFunc
	balancerMap     map[string]balance.Balance
	balancerLock    sync.RWMutex

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
}

func NewQueryCoord(ctx context.Context) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:            ctx,
		cancel:         cancel,
		balancerMap:    make(map[string]balance.Balance),
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
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	return nil
}

func (s *Server) ServerExist(serverID int64) bool {
	sessions, _, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		log.Ctx(s.ctx).Warn("failed to get sessions", zap.Error(err))
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
	log.Ctx(s.ctx).Info("register metrics actions finished")
}

func (s *Server) Init() error {
	log := log.Ctx(s.ctx)
	log.Info("QueryCoord start init",
		zap.String("meta-root-path", Params.EtcdCfg.MetaRootPath.GetValue()),
		zap.String("address", s.address))

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
		s.session.Init(typeutil.QueryCoordRole, s.address, true, true)
		s.enableActiveStandBy = Params.QueryCoordCfg.EnableActiveStandby.GetAsBool()
		s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	return nil
}

func (s *Server) initQueryCoord() error {
	log := log.Ctx(s.ctx)
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	log.Info("start init querycoord", zap.Any("State", commonpb.StateCode_Initializing))
	// Init KV and ID allocator
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	var idAllocatorKV kv.TxnKV
	log.Info(fmt.Sprintf("query coordinator connecting to %s.", metaType))
	if metaType == util.MetaStoreTypeTiKV {
		s.kv = tikv.NewTiKV(s.tikvCli, Params.TiKVCfg.MetaRootPath.GetValue(),
			tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
		idAllocatorKV = tsoutil.NewTSOTiKVBase(s.tikvCli, Params.TiKVCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	} else if metaType == util.MetaStoreTypeEtcd {
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
		idAllocatorKV = tsoutil.NewTSOKVBase(s.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	} else {
		return fmt.Errorf("not supported meta store: %s", metaType)
	}
	log.Info(fmt.Sprintf("query coordinator successfully connected to %s.", metaType))

	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
	err := idAllocator.Initialize()
	if err != nil {
		log.Error("query coordinator id allocator initialize failed", zap.Error(err))
		return err
	}
	s.idAllocator = func() (int64, error) {
		return idAllocator.AllocOne()
	}
	log.Info("init ID allocator done")

	// Init metrics cache manager
	s.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	// Init meta
	s.nodeMgr = session.NewNodeManager()
	err = s.initMeta()
	if err != nil {
		return err
	}
	// Init session
	log.Info("init session")
	s.cluster = session.NewCluster(s.nodeMgr, s.queryNodeCreator)

	// Init schedulers
	log.Info("init schedulers")
	s.jobScheduler = job.NewScheduler()
	s.taskScheduler = task.NewScheduler(
		s.ctx,
		s.meta,
		s.dist,
		s.targetMgr,
		s.broker,
		s.cluster,
		s.nodeMgr,
	)

	// init proxy client manager
	s.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)
	s.proxyWatcher = proxyutil.NewProxyWatcher(
		s.etcdCli,
		s.proxyClientManager.AddProxyClients,
	)
	s.proxyWatcher.AddSessionFunc(s.proxyClientManager.AddProxyClient)
	s.proxyWatcher.DelSessionFunc(s.proxyClientManager.DelProxyClient)
	log.Info("init proxy manager done")

	// Init checker controller
	log.Info("init checker controller")
	s.getBalancerFunc = func() balance.Balance {
		balanceKey := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
		s.balancerLock.Lock()
		defer s.balancerLock.Unlock()

		balancer, ok := s.balancerMap[balanceKey]
		if ok {
			return balancer
		}

		log.Info("switch to new balancer", zap.String("name", balanceKey))
		switch balanceKey {
		case meta.RoundRobinBalancerName:
			balancer = balance.NewRoundRobinBalancer(s.taskScheduler, s.nodeMgr)
		case meta.RowCountBasedBalancerName:
			balancer = balance.NewRowCountBasedBalancer(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)
		case meta.ScoreBasedBalancerName:
			balancer = balance.NewScoreBasedBalancer(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)
		case meta.MultiTargetBalancerName:
			balancer = balance.NewMultiTargetBalancer(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)
		case meta.ChannelLevelScoreBalancerName:
			balancer = balance.NewChannelLevelScoreBalancer(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)
		default:
			log.Info(fmt.Sprintf("default to use %s", meta.ScoreBasedBalancerName))
			balancer = balance.NewScoreBasedBalancer(s.taskScheduler, s.nodeMgr, s.dist, s.meta, s.targetMgr)
		}

		s.balancerMap[balanceKey] = balancer
		return balancer
	}
	s.checkerController = checkers.NewCheckerController(
		s.meta,
		s.dist,
		s.targetMgr,
		s.nodeMgr,
		s.taskScheduler,
		s.broker,
		s.getBalancerFunc,
	)

	// Init observers
	s.initObserver()

	// Init heartbeat
	log.Info("init dist controller")
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

	log.Info("init querycoord done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", s.address))
	return err
}

func (s *Server) initMeta() error {
	log := log.Ctx(s.ctx)
	record := timerecord.NewTimeRecorder("querycoord")

	log.Info("init meta")
	s.store = querycoord.NewCatalog(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store, s.nodeMgr)

	s.broker = meta.NewCoordinatorBroker(
		s.mixCoord,
	)

	log.Info("recover meta...")
	err := s.meta.CollectionManager.Recover(s.ctx, s.broker)
	if err != nil {
		log.Warn("failed to recover collections", zap.Error(err))
		return err
	}
	collections := s.meta.GetAll(s.ctx)
	log.Info("recovering collections...", zap.Int64s("collections", collections))

	// We really update the metric after observers think the collection loaded.
	metrics.QueryCoordNumCollections.WithLabelValues().Set(0)

	metrics.QueryCoordNumPartitions.WithLabelValues().Set(float64(len(s.meta.GetAllPartitions(s.ctx))))

	err = s.meta.ReplicaManager.Recover(s.ctx, collections)
	if err != nil {
		log.Warn("failed to recover replicas", zap.Error(err))
		return err
	}

	err = s.meta.ResourceManager.Recover(s.ctx)
	if err != nil {
		log.Warn("failed to recover resource groups", zap.Error(err))
		return err
	}

	s.dist = meta.NewDistributionManager(s.nodeMgr)
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)
	err = s.targetMgr.Recover(s.ctx, s.store)
	if err != nil {
		log.Warn("failed to recover collection targets", zap.Error(err))
	}

	log.Info("QueryCoord server initMeta done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *Server) initObserver() {
	log.Ctx(s.ctx).Info("init observers")
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
	)

	s.resourceObserver = observers.NewResourceObserver(s.meta)

	s.leaderCacheObserver = observers.NewLeaderCacheObserver(
		s.proxyClientManager,
	)
}

func (s *Server) afterStart() {}

func (s *Server) Start() error {
	if err := s.startQueryCoord(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("QueryCoord started")

	return nil
}

func (s *Server) startQueryCoord() error {
	log.Ctx(s.ctx).Info("start watcher...")
	sessions, revision, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		return err
	}

	log.Info("rewatch nodes", zap.Any("sessions", sessions))
	err = s.rewatchNodes(sessions)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go s.watchNodes(revision)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err := s.proxyWatcher.WatchProxy(s.ctx); err != nil {
		log.Ctx(s.ctx).Warn("querycoord failed to watch proxy", zap.Error(err))
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
	log := log.Ctx(s.ctx)
	// leader cache observer shall be started before `SyncAll` call
	s.leaderCacheObserver.Start(s.ctx)
	// Recover dist, to avoid generate too much task when dist not ready after restart
	s.distController.SyncAll(s.ctx)

	// start the components from inside to outside,
	// to make the dependencies ready for every component
	log.Info("start cluster...")
	s.cluster.Start()

	log.Info("start observers...")
	s.collectionObserver.Start()
	s.targetObserver.Start()
	s.replicaObserver.Start()
	s.resourceObserver.Start()

	log.Info("start task scheduler...")
	s.taskScheduler.Start()

	log.Info("start checker controller...")
	s.checkerController.Start()

	log.Info("start job scheduler...")
	s.jobScheduler.Start()
}

func (s *Server) Stop() error {
	log := log.Ctx(s.ctx)
	// FOLLOW the dependence graph:
	// job scheduler -> checker controller -> task scheduler -> dist controller -> cluster -> session
	// observers -> dist controller

	if s.jobScheduler != nil {
		log.Info("stop job scheduler...")
		s.jobScheduler.Stop()
	}

	if s.checkerController != nil {
		log.Info("stop checker controller...")
		s.checkerController.Stop()
	}

	if s.taskScheduler != nil {
		log.Info("stop task scheduler...")
		s.taskScheduler.Stop()
	}

	log.Info("stop observers...")
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
		log.Info("stop dist controller...")
		s.distController.Stop()
	}

	if s.cluster != nil {
		log.Info("stop cluster...")
		s.cluster.Stop()
	}

	if s.session != nil {
		s.session.Stop()
	}

	s.cancel()
	s.wg.Wait()
	log.Info("QueryCoord stop successfully")
	return nil
}

// UpdateStateCode updates the status of the coord, including healthy, unhealthy
func (s *Server) UpdateStateCode(code commonpb.StateCode) {
	s.status.Store(int32(code))
	log.Ctx(s.ctx).Info("update querycoord state", zap.String("state", code.String()))
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
	log := log.Ctx(s.ctx)
	defer s.wg.Done()

	eventChan := s.session.WatchServices(typeutil.QueryNodeRole, revision+1, s.rewatchNodes)
	for {
		select {
		case <-s.ctx.Done():
			log.Info("stop watching nodes, QueryCoord stopped")
			return

		case event, ok := <-eventChan:
			if !ok {
				// ErrCompacted is handled inside SessionWatcher
				log.Warn("Session Watcher channel closed", zap.Int64("serverID", paramtable.GetNodeID()))
				go s.Stop()
				if s.session.IsTriggerKill() {
					if p, err := os.FindProcess(os.Getpid()); err == nil {
						p.Signal(syscall.SIGINT)
					}
				}
				return
			}

			nodeID := event.Session.ServerID
			addr := event.Session.Address
			log := log.With(
				zap.Int64("nodeID", nodeID),
				zap.String("nodeAddr", addr),
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

			case sessionutil.SessionUpdateEvent:
				log.Info("stopping the node")
				s.nodeMgr.Stopping(nodeID)
				s.handleNodeStopping(nodeID)

			case sessionutil.SessionDelEvent:
				log.Info("a node down, remove it")
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
		} else if nodeSession.Stopping && !node.IsStoppingState() {
			// node in node manager but session is stopping, means it's stopping
			s.nodeMgr.Stopping(node.ID())
			s.handleNodeStopping(node.ID())
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

			if nodeSession.Stopping {
				s.nodeMgr.Stopping(nodeSession.ServerID)
				s.handleNodeStopping(nodeSession.ServerID)
			} else {
				s.handleNodeUp(nodeSession.GetServerID())
			}
		}
	}

	// Note: Node manager doesn't persist node list, so after query coord restart, we cannot
	// update all node statuses in resource manager based on session and node manager's node list.
	// Therefore, manual status checking of all nodes in resource manager is needed.
	s.meta.ResourceManager.CheckNodesInResourceGroup(s.ctx)

	return nil
}

func (s *Server) handleNodeUp(node int64) {
	nodeInfo := s.nodeMgr.Get(node)
	if nodeInfo == nil {
		log.Ctx(s.ctx).Warn("node already down", zap.Int64("nodeID", node))
		return
	}

	// add executor to task scheduler
	s.taskScheduler.AddExecutor(node)

	// start dist handler
	s.distController.StartDistInstance(s.ctx, node)

	// need assign to new rg and replica
	s.meta.ResourceManager.HandleNodeUp(s.ctx, node)

	s.metricsCacheManager.InvalidateSystemInfoMetrics()
	s.checkerController.Check()
}

func (s *Server) handleNodeDown(node int64) {
	s.taskScheduler.RemoveExecutor(node)
	s.distController.Remove(node)

	// Clear dist
	s.dist.ChannelDistManager.Update(node)
	s.dist.SegmentDistManager.Update(node)

	// Clear tasks
	s.taskScheduler.RemoveByNode(node)

	s.meta.ResourceManager.HandleNodeDown(context.Background(), node)

	// clean node's metrics
	metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(node))
	s.metricsCacheManager.InvalidateSystemInfoMetrics()
}

func (s *Server) handleNodeStopping(node int64) {
	// mark node as stopping in node manager
	s.nodeMgr.Stopping(node)

	// mark node as stopping in resource manager
	s.meta.ResourceManager.HandleNodeStopping(context.Background(), node)

	// trigger checker to check stopping node
	s.checkerController.Check()
}

func (s *Server) updateBalanceConfigLoop(ctx context.Context) {
	log := log.Ctx(s.ctx)
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
				log.Info("update balance config loop exit!")
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
	log := log.Ctx(s.ctx).WithRateGroup("qcv2.updateBalanceConfigLoop", 1, 60)
	r := semver.MustParseRange("<2.3.0")
	sessions, _, err := s.session.GetSessionsWithVersionRange(typeutil.QueryNodeRole, r)
	if err != nil {
		log.Warn("check query node version occur error on etcd", zap.Error(err))
		return false
	}

	if len(sessions) == 0 {
		// only balance channel when all query node's version >= 2.3.0
		Params.Reset(Params.QueryCoordCfg.AutoBalance.Key)
		log.Info("all old query node down, enable auto balance!")
		return true
	}

	Params.Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
	log.RatedDebug(10, "old query node exist", zap.Strings("sessions", lo.Keys(sessions)))
	return false
}

func (s *Server) applyLoadConfigChanges(ctx context.Context, newReplicaNum int32, newRGs []string) {
	if newReplicaNum <= 0 && len(newRGs) == 0 {
		log.Info("invalid cluster level load config, skip it", zap.Int32("replica_num", newReplicaNum), zap.Strings("resource_groups", newRGs))
		return
	}

	// try to check load config changes after restart, and try to update replicas
	collectionIDs := s.meta.GetAll(ctx)
	collectionIDs = lo.Filter(collectionIDs, func(collectionID int64, _ int) bool {
		collection := s.meta.GetCollection(ctx, collectionID)
		if collection.UserSpecifiedReplicaMode {
			log.Info("collection is user specified replica mode, skip update load config", zap.Int64("collectionID", collectionID))
			return false
		}
		return true
	})

	if len(collectionIDs) == 0 {
		log.Info("no collection to update load config, skip it")
		return
	}

	log.Info("apply load config changes",
		zap.Int64s("collectionIDs", collectionIDs),
		zap.Int32("replicaNum", newReplicaNum),
		zap.Strings("resourceGroups", newRGs))
	err := s.updateLoadConfig(ctx, collectionIDs, newReplicaNum, newRGs)
	if err != nil {
		log.Warn("failed to update load config", zap.Error(err))
	}
}

func (s *Server) watchLoadConfigChanges() {
	// first apply load config change from params
	replicaNum := paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsUint32()
	rgs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	s.applyLoadConfigChanges(s.ctx, int32(replicaNum), rgs)

	log := log.Ctx(s.ctx)
	replicaNumHandler := config.NewHandler("watchReplicaNumberChanges", func(e *config.Event) {
		log.Info("watch load config changes", zap.String("key", e.Key), zap.String("value", e.Value), zap.String("type", e.EventType))
		replicaNum, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			log.Warn("invalid cluster level load config, skip it", zap.String("key", e.Key), zap.String("value", e.Value))
			return
		}
		rgs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()

		s.applyLoadConfigChanges(s.ctx, int32(replicaNum), rgs)
	})
	paramtable.Get().Watch(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, replicaNumHandler)

	rgHandler := config.NewHandler("watchResourceGroupChanges", func(e *config.Event) {
		log.Info("watch load config changes", zap.String("key", e.Key), zap.String("value", e.Value), zap.String("type", e.EventType))
		if len(e.Value) == 0 {
			log.Warn("invalid cluster level load config, skip it", zap.String("key", e.Key), zap.String("value", e.Value))
			return
		}

		rgs := strings.Split(e.Value, ",")
		rgs = lo.Map(rgs, func(rg string, _ int) string { return strings.TrimSpace(rg) })
		replicaNum := paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt64()
		s.applyLoadConfigChanges(s.ctx, int32(replicaNum), rgs)
	})
	paramtable.Get().Watch(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, rgHandler)
}
