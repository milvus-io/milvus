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
	"syscall"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	dataCoord types.DataCoordClient
	rootCoord types.RootCoordClient

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

	nodeUpEventChan chan int64
	notifyNodeUp    chan struct{}

	// proxy client manager
	proxyCreator       proxyutil.ProxyCreator
	proxyWatcher       proxyutil.ProxyWatcherInterface
	proxyClientManager proxyutil.ProxyClientManagerInterface
}

func NewQueryCoord(ctx context.Context) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:             ctx,
		cancel:          cancel,
		nodeUpEventChan: make(chan int64, 10240),
		notifyNodeUp:    make(chan struct{}),
		balancerMap:     make(map[string]balance.Balance),
	}
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	server.queryNodeCreator = session.DefaultQueryNodeCreator
	expr.Register("querycoord", server)
	return server, nil
}

func (s *Server) Register() error {
	s.session.Register()
	afterRegister := func() {
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.QueryCoordRole).Inc()
		s.session.LivenessCheck(s.ctx, func() {
			log.Error("QueryCoord disconnected from etcd, process will exit", zap.Int64("serverID", s.session.GetServerID()))
			os.Exit(1)
		})
	}
	if s.enableActiveStandBy {
		go func() {
			if err := s.session.ProcessActiveStandBy(s.activateFunc); err != nil {
				log.Error("failed to activate standby server", zap.Error(err))
				panic(err)
			}
			afterRegister()
		}()
	} else {
		afterRegister()
	}
	return nil
}

func (s *Server) initSession() error {
	// Init QueryCoord session
	s.session = sessionutil.NewSession(s.ctx)
	if s.session == nil {
		return fmt.Errorf("failed to create session")
	}
	s.session.Init(typeutil.QueryCoordRole, s.address, true, true)
	s.enableActiveStandBy = Params.QueryCoordCfg.EnableActiveStandby.GetAsBool()
	s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	return nil
}

func (s *Server) Init() error {
	log.Info("QueryCoord start init",
		zap.String("meta-root-path", Params.EtcdCfg.MetaRootPath.GetValue()),
		zap.String("address", s.address))

	if err := s.initSession(); err != nil {
		return err
	}

	if s.enableActiveStandBy {
		s.activateFunc = func() error {
			log.Info("QueryCoord switch from standby to active, activating")
			if err := s.initQueryCoord(); err != nil {
				log.Error("QueryCoord init failed", zap.Error(err))
				return err
			}
			if err := s.startQueryCoord(); err != nil {
				log.Error("QueryCoord init failed", zap.Error(err))
				return err
			}
			log.Info("QueryCoord startup success")
			return nil
		}
		s.UpdateStateCode(commonpb.StateCode_StandBy)
		log.Info("QueryCoord enter standby mode successfully")
		return nil
	}

	return s.initQueryCoord()
}

func (s *Server) initQueryCoord() error {
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

	// Init heartbeat
	log.Info("init dist controller")
	s.distController = dist.NewDistController(
		s.cluster,
		s.nodeMgr,
		s.dist,
		s.targetMgr,
		s.taskScheduler,
	)

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

	// Init load status cache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	log.Info("init querycoord done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", s.address))
	return err
}

func (s *Server) initMeta() error {
	record := timerecord.NewTimeRecorder("querycoord")

	log.Info("init meta")
	s.store = querycoord.NewCatalog(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store, s.nodeMgr)

	s.broker = meta.NewCoordinatorBroker(
		s.dataCoord,
		s.rootCoord,
	)

	log.Info("recover meta...")
	err := s.meta.CollectionManager.Recover(s.broker)
	if err != nil {
		log.Warn("failed to recover collections", zap.Error(err))
		return err
	}
	collections := s.meta.GetAll()
	log.Info("recovering collections...", zap.Int64s("collections", collections))

	// We really update the metric after observers think the collection loaded.
	metrics.QueryCoordNumCollections.WithLabelValues().Set(0)

	metrics.QueryCoordNumPartitions.WithLabelValues().Set(float64(len(s.meta.GetAllPartitions())))

	err = s.meta.ReplicaManager.Recover(collections)
	if err != nil {
		log.Warn("failed to recover replicas", zap.Error(err))
		return err
	}

	err = s.meta.ResourceManager.Recover()
	if err != nil {
		log.Warn("failed to recover resource groups", zap.Error(err))
		return err
	}

	s.dist = &meta.DistributionManager{
		SegmentDistManager: meta.NewSegmentDistManager(),
		ChannelDistManager: meta.NewChannelDistManager(),
		LeaderViewManager:  meta.NewLeaderViewManager(),
	}
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)
	err = s.targetMgr.Recover(s.store)
	if err != nil {
		log.Warn("failed to recover collection targets", zap.Error(err))
	}

	log.Info("QueryCoord server initMeta done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *Server) initObserver() {
	log.Info("init observers")
	s.targetObserver = observers.NewTargetObserver(
		s.meta,
		s.targetMgr,
		s.dist,
		s.broker,
		s.cluster,
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
	s.dist.LeaderViewManager.SetNotifyFunc(s.leaderCacheObserver.RegisterEvent)
}

func (s *Server) afterStart() {}

func (s *Server) Start() error {
	if !s.enableActiveStandBy {
		if err := s.startQueryCoord(); err != nil {
			return err
		}
		log.Info("QueryCoord started")
	}
	return nil
}

func (s *Server) startQueryCoord() error {
	log.Info("start watcher...")
	sessions, revision, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		return err
	}
	for _, node := range sessions {
		s.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node.ServerID,
			Address:  node.Address,
			Hostname: node.HostName,
			Version:  node.Version,
		}))
		s.taskScheduler.AddExecutor(node.ServerID)

		if node.Stopping {
			s.nodeMgr.Stopping(node.ServerID)
		}
	}
	s.checkNodeStateInRG()
	for _, node := range sessions {
		s.handleNodeUp(node.ServerID)
	}

	s.wg.Add(2)
	go s.handleNodeUpLoop()
	go s.watchNodes(revision)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err := s.proxyWatcher.WatchProxy(s.ctx); err != nil {
		log.Warn("querycoord failed to watch proxy", zap.Error(err))
	}

	s.startServerLoop()
	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.QueryCoordRole, s.session.GetServerID())
	return nil
}

func (s *Server) startServerLoop() {
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
		s.targetMgr.SaveCurrentTarget(s.store)
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
}

func (s *Server) State() commonpb.StateCode {
	return commonpb.StateCode(s.status.Load())
}

func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	log.Debug("QueryCoord current state", zap.String("StateCode", s.State().String()))
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.GetServerID()
	}
	serviceComponentInfo := &milvuspb.ComponentInfo{
		// NodeID:    Params.QueryCoordID, // will race with QueryCoord.Register()
		NodeID:    nodeID,
		StateCode: s.State(),
	}

	return &milvuspb.ComponentStates{
		Status: merr.Success(),
		State:  serviceComponentInfo,
		// SubcomponentStates: subComponentInfos,
	}, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.QueryCoordTimeTick.GetValue(),
	}, nil
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

// SetRootCoord sets root coordinator's client
func (s *Server) SetRootCoordClient(rootCoord types.RootCoordClient) error {
	if rootCoord == nil {
		return errors.New("null RootCoord interface")
	}

	s.rootCoord = rootCoord
	return nil
}

// SetDataCoord sets data coordinator's client
func (s *Server) SetDataCoordClient(dataCoord types.DataCoordClient) error {
	if dataCoord == nil {
		return errors.New("null DataCoord interface")
	}

	s.dataCoord = dataCoord
	return nil
}

func (s *Server) SetQueryNodeCreator(f func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)) {
	s.queryNodeCreator = f
}

func (s *Server) watchNodes(revision int64) {
	defer s.wg.Done()

	eventChan := s.session.WatchServices(typeutil.QueryNodeRole, revision+1, nil)
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

			switch event.EventType {
			case sessionutil.SessionAddEvent:
				nodeID := event.Session.ServerID
				addr := event.Session.Address
				log.Info("add node to NodeManager",
					zap.Int64("nodeID", nodeID),
					zap.String("nodeAddr", addr),
				)
				s.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   nodeID,
					Address:  addr,
					Hostname: event.Session.HostName,
					Version:  event.Session.Version,
				}))
				s.nodeUpEventChan <- nodeID
				select {
				case s.notifyNodeUp <- struct{}{}:
				default:
				}

			case sessionutil.SessionUpdateEvent:
				nodeID := event.Session.ServerID
				addr := event.Session.Address
				log.Info("stopping the node",
					zap.Int64("nodeID", nodeID),
					zap.String("nodeAddr", addr),
				)
				s.nodeMgr.Stopping(nodeID)
				s.checkerController.Check()
				s.meta.ResourceManager.HandleNodeStopping(nodeID)

			case sessionutil.SessionDelEvent:
				nodeID := event.Session.ServerID
				log.Info("a node down, remove it", zap.Int64("nodeID", nodeID))
				s.nodeMgr.Remove(nodeID)
				s.handleNodeDown(nodeID)
				s.metricsCacheManager.InvalidateSystemInfoMetrics()
			}
		}
	}
}

func (s *Server) handleNodeUpLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(Params.QueryCoordCfg.CheckHealthInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			log.Info("handle node up loop exit due to context done")
			return
		case <-s.notifyNodeUp:
			s.tryHandleNodeUp()
		case <-ticker.C:
			s.tryHandleNodeUp()
		}
	}
}

func (s *Server) tryHandleNodeUp() {
	log := log.Ctx(s.ctx).WithRateGroup("qcv2.Server", 1, 60)
	ctx, cancel := context.WithTimeout(s.ctx, Params.QueryCoordCfg.CheckHealthRPCTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	reasons, err := s.checkNodeHealth(ctx)
	if err != nil {
		log.RatedWarn(10, "unhealthy node exist, node up will be delayed",
			zap.Int("delayedNodeUpEvents", len(s.nodeUpEventChan)),
			zap.Int("unhealthyNodeNum", len(reasons)),
			zap.Strings("unhealthyReason", reasons))
		return
	}
	for len(s.nodeUpEventChan) > 0 {
		nodeID := <-s.nodeUpEventChan
		if s.nodeMgr.Get(nodeID) != nil {
			// only if all nodes are healthy, node up event will be handled
			s.handleNodeUp(nodeID)
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			s.checkerController.Check()
		} else {
			log.Warn("node already down",
				zap.Int64("nodeID", nodeID))
		}
	}
}

func (s *Server) handleNodeUp(node int64) {
	s.taskScheduler.AddExecutor(node)
	s.distController.StartDistInstance(s.ctx, node)
	// need assign to new rg and replica
	s.meta.ResourceManager.HandleNodeUp(node)
}

func (s *Server) handleNodeDown(node int64) {
	s.taskScheduler.RemoveExecutor(node)
	s.distController.Remove(node)

	// Clear dist
	s.dist.LeaderViewManager.Update(node)
	s.dist.ChannelDistManager.Update(node)
	s.dist.SegmentDistManager.Update(node)

	// Clear tasks
	s.taskScheduler.RemoveByNode(node)

	s.meta.ResourceManager.HandleNodeDown(node)
}

func (s *Server) checkNodeStateInRG() {
	for _, rgName := range s.meta.ListResourceGroups() {
		rg := s.meta.ResourceManager.GetResourceGroup(rgName)
		for _, node := range rg.GetNodes() {
			info := s.nodeMgr.Get(node)
			if info == nil {
				s.meta.ResourceManager.HandleNodeDown(node)
			} else if info.IsStoppingState() {
				s.meta.ResourceManager.HandleNodeStopping(node)
			}
		}
	}
}

func (s *Server) updateBalanceConfigLoop(ctx context.Context) {
	success := s.updateBalanceConfig()
	if success {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(Params.QueryCoordCfg.CheckAutoBalanceConfigInterval.GetAsDuration(time.Second))
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
