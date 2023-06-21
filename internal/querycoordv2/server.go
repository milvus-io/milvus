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

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
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
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// Only for re-export
	Params = params.Params
)

type Server struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	status              atomic.Int32
	etcdCli             *clientv3.Client
	address             string
	session             *sessionutil.Session
	kv                  kv.MetaKv
	idAllocator         func() (int64, error)
	metricsCacheManager *metricsinfo.MetricsCacheManager

	// Coordinators
	dataCoord types.DataCoord
	rootCoord types.RootCoord

	// Meta
	store     meta.Store
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
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
	collectionObserver *observers.CollectionObserver
	leaderObserver     *observers.LeaderObserver
	targetObserver     *observers.TargetObserver
	replicaObserver    *observers.ReplicaObserver
	resourceObserver   *observers.ResourceObserver

	balancer    balance.Balance
	balancerMap map[string]balance.Balance

	// Active-standby
	enableActiveStandBy bool
	activateFunc        func() error

	nodeUpEventChan chan int64
	notifyNodeUp    chan struct{}
}

func NewQueryCoord(ctx context.Context) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:             ctx,
		cancel:          cancel,
		nodeUpEventChan: make(chan int64, 10240),
		notifyNodeUp:    make(chan struct{}),
	}
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	server.queryNodeCreator = session.DefaultQueryNodeCreator
	return server, nil
}

func (s *Server) Register() error {
	s.session.Register()
	if s.enableActiveStandBy {
		if err := s.session.ProcessActiveStandBy(s.activateFunc); err != nil {
			log.Error("failed to activate standby server", zap.Error(err))
			return err
		}
	}

	s.session.LivenessCheck(s.ctx, func() {
		log.Error("QueryCoord disconnected from etcd, process will exit", zap.Int64("serverID", s.session.ServerID))
		if err := s.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if s.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (s *Server) initSession() error {
	// Init QueryCoord session
	s.session = sessionutil.NewSession(s.ctx, Params.EtcdCfg.MetaRootPath.GetValue(), s.etcdCli)
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
	log.Info("QueryCoord", zap.Any("State", commonpb.StateCode_Initializing))
	// Init KV
	etcdKV := etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
	s.kv = etcdKV
	log.Info("query coordinator try to connect etcd success")

	// Init ID allocator
	idAllocatorKV := tsoutil.NewTSOKVBase(s.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
	err := idAllocator.Initialize()
	if err != nil {
		log.Error("query coordinator id allocator initialize failed", zap.Error(err))
		return err
	}
	s.idAllocator = func() (int64, error) {
		return idAllocator.AllocOne()
	}

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

	// Init heartbeat
	log.Info("init dist controller")
	s.distController = dist.NewDistController(
		s.cluster,
		s.nodeMgr,
		s.dist,
		s.targetMgr,
		s.taskScheduler,
	)

	// Init balancer map and balancer
	log.Info("init all available balancer")
	s.balancerMap = make(map[string]balance.Balance)
	s.balancerMap[balance.RoundRobinBalancerName] = balance.NewRoundRobinBalancer(s.taskScheduler, s.nodeMgr)
	s.balancerMap[balance.RowCountBasedBalancerName] = balance.NewRowCountBasedBalancer(s.taskScheduler,
		s.nodeMgr, s.dist, s.meta, s.targetMgr)
	s.balancerMap[balance.ScoreBasedBalancerName] = balance.NewScoreBasedBalancer(s.taskScheduler,
		s.nodeMgr, s.dist, s.meta, s.targetMgr)
	if balancer, ok := s.balancerMap[params.Params.QueryCoordCfg.Balancer.GetValue()]; ok {
		s.balancer = balancer
		log.Info("use config balancer", zap.String("balancer", params.Params.QueryCoordCfg.Balancer.GetValue()))
	} else {
		s.balancer = s.balancerMap[balance.RowCountBasedBalancerName]
		log.Info("use rowCountBased auto balancer")
	}

	// Init checker controller
	log.Info("init checker controller")
	s.checkerController = checkers.NewCheckerController(
		s.meta,
		s.dist,
		s.targetMgr,
		s.balancer,
		s.nodeMgr,
		s.taskScheduler,
	)

	// Init observers
	s.initObserver()

	// Init load status cache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	log.Info("QueryCoord init success")
	return err
}

func (s *Server) initMeta() error {
	record := timerecord.NewTimeRecorder("querycoord")

	log.Info("init meta")
	s.store = meta.NewMetaStore(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store, s.nodeMgr)

	s.broker = meta.NewCoordinatorBroker(
		s.dataCoord,
		s.rootCoord,
	)

	log.Info("recover meta...")
	err := s.meta.CollectionManager.Recover(s.broker)
	if err != nil {
		log.Error("failed to recover collections")
		return err
	}
	collections := s.meta.GetAll()
	log.Info("recovering collections...", zap.Int64s("collections", collections))

	// We really update the metric after observers think the collection loaded.
	metrics.QueryCoordNumCollections.WithLabelValues().Set(0)

	metrics.QueryCoordNumPartitions.WithLabelValues().Set(float64(len(s.meta.GetAllPartitions())))

	err = s.meta.ReplicaManager.Recover(collections)
	if err != nil {
		log.Error("failed to recover replicas")
		return err
	}

	err = s.meta.ResourceManager.Recover()
	if err != nil {
		log.Error("failed to recover resource groups")
		return err
	}

	s.dist = &meta.DistributionManager{
		SegmentDistManager: meta.NewSegmentDistManager(),
		ChannelDistManager: meta.NewChannelDistManager(),
		LeaderViewManager:  meta.NewLeaderViewManager(),
	}
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)
	log.Info("QueryCoord server initMeta done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (s *Server) initObserver() {
	log.Info("init observers")
	s.leaderObserver = observers.NewLeaderObserver(
		s.dist,
		s.meta,
		s.targetMgr,
		s.broker,
		s.cluster,
	)
	s.targetObserver = observers.NewTargetObserver(
		s.meta,
		s.targetMgr,
		s.dist,
		s.broker,
	)
	s.collectionObserver = observers.NewCollectionObserver(
		s.dist,
		s.meta,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
	)

	s.replicaObserver = observers.NewReplicaObserver(
		s.meta,
		s.dist,
	)

	s.resourceObserver = observers.NewResourceObserver(s.meta)
}

func (s *Server) afterStart() {
}

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
		s.nodeMgr.Add(session.NewNodeInfo(node.ServerID, node.Address))
		s.taskScheduler.AddExecutor(node.ServerID)
	}
	s.checkReplicas()
	for _, node := range sessions {
		s.handleNodeUp(node.ServerID)
	}

	s.wg.Add(2)
	go s.handleNodeUpLoop()
	go s.watchNodes(revision)

	log.Info("start recovering dist and target")
	err = s.recover()
	if err != nil {
		return err
	}
	s.startServerLoop()
	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	return nil
}

func (s *Server) startServerLoop() {
	log.Info("start cluster...")
	s.cluster.Start(s.ctx)

	log.Info("start job scheduler...")
	s.jobScheduler.Start(s.ctx)

	log.Info("start task scheduler...")
	s.taskScheduler.Start(s.ctx)

	log.Info("start checker controller...")
	s.checkerController.Start(s.ctx)

	log.Info("start observers...")
	s.collectionObserver.Start(s.ctx)
	s.leaderObserver.Start(s.ctx)
	s.targetObserver.Start(s.ctx)
	s.replicaObserver.Start(s.ctx)
	s.resourceObserver.Start(s.ctx)
}

func (s *Server) Stop() error {
	s.cancel()
	if s.session != nil {
		s.session.Stop()
	}

	if s.cluster != nil {
		log.Info("stop cluster...")
		s.cluster.Stop()
	}

	if s.distController != nil {
		log.Info("stop dist controller...")
		s.distController.Stop()
	}

	if s.checkerController != nil {
		log.Info("stop checker controller...")
		s.checkerController.Stop()
	}

	if s.taskScheduler != nil {
		log.Info("stop task scheduler...")
		s.taskScheduler.Stop()
	}

	if s.jobScheduler != nil {
		log.Info("stop job scheduler...")
		s.jobScheduler.Stop()
	}

	log.Info("stop observers...")
	if s.collectionObserver != nil {
		s.collectionObserver.Stop()
	}
	if s.leaderObserver != nil {
		s.leaderObserver.Stop()
	}
	if s.targetObserver != nil {
		s.targetObserver.Stop()
	}
	if s.replicaObserver != nil {
		s.replicaObserver.Stop()
	}
	if s.resourceObserver != nil {
		s.resourceObserver.Stop()
	}

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

func (s *Server) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID
	}
	serviceComponentInfo := &milvuspb.ComponentInfo{
		// NodeID:    Params.QueryCoordID, // will race with QueryCoord.Register()
		NodeID:    nodeID,
		StateCode: s.State(),
	}

	return &milvuspb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State: serviceComponentInfo,
		//SubcomponentStates: subComponentInfos,
	}, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.QueryCoordTimeTick.GetValue(),
	}, nil
}

func (s *Server) SetAddress(address string) {
	s.address = address
}

// SetEtcdClient sets etcd's client
func (s *Server) SetEtcdClient(etcdClient *clientv3.Client) {
	s.etcdCli = etcdClient
}

// SetRootCoord sets root coordinator's client
func (s *Server) SetRootCoord(rootCoord types.RootCoord) error {
	if rootCoord == nil {
		return errors.New("null RootCoord interface")
	}

	s.rootCoord = rootCoord
	return nil
}

// SetDataCoord sets data coordinator's client
func (s *Server) SetDataCoord(dataCoord types.DataCoord) error {
	if dataCoord == nil {
		return errors.New("null DataCoord interface")
	}

	s.dataCoord = dataCoord
	return nil
}

func (s *Server) SetQueryNodeCreator(f func(ctx context.Context, addr string) (types.QueryNode, error)) {
	s.queryNodeCreator = f
}

func (s *Server) recover() error {
	// Recover target managers
	group, ctx := errgroup.WithContext(s.ctx)
	for _, collection := range s.meta.GetAll() {
		collection := collection
		group.Go(func() error {
			return s.recoverCollectionTargets(ctx, collection)
		})
	}
	err := group.Wait()
	if err != nil {
		return err
	}

	// Recover dist
	s.distController.SyncAll(s.ctx)

	return nil
}

func (s *Server) recoverCollectionTargets(ctx context.Context, collection int64) error {
	err := s.targetMgr.UpdateCollectionNextTarget(collection)
	if err != nil {
		s.meta.CollectionManager.RemoveCollection(collection)
		s.meta.ReplicaManager.RemoveCollection(collection)
		log.Error("failed to recover collection due to update next target failed",
			zap.Int64("collectionID", collection),
			zap.Error(err),
		)
	}
	return nil
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
				log.Error("Session Watcher channel closed", zap.Int64("serverID", paramtable.GetNodeID()))
				go s.Stop()
				if s.session.TriggerKill {
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
				s.nodeMgr.Add(session.NewNodeInfo(nodeID, addr))
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
	log := log.With(zap.Int64("nodeID", node))
	s.taskScheduler.AddExecutor(node)
	s.distController.StartDistInstance(s.ctx, node)

	// need assign to new rg and replica
	rgName, err := s.meta.ResourceManager.HandleNodeUp(node)
	if err != nil {
		log.Warn("HandleNodeUp: failed to assign node to resource group",
			zap.Error(err),
		)
		return
	}

	log.Info("HandleNodeUp: assign node to resource group",
		zap.String("resourceGroup", rgName),
	)

	utils.AddNodesToCollectionsInRG(s.meta, meta.DefaultResourceGroupName, node)
}

func (s *Server) handleNodeDown(node int64) {
	log := log.With(zap.Int64("nodeID", node))
	s.taskScheduler.RemoveExecutor(node)
	s.distController.Remove(node)

	// Clear dist
	s.dist.LeaderViewManager.Update(node)
	s.dist.ChannelDistManager.Update(node)
	s.dist.SegmentDistManager.Update(node)

	// Clear meta
	for _, collection := range s.meta.CollectionManager.GetAll() {
		log := log.With(zap.Int64("collectionID", collection))
		replica := s.meta.ReplicaManager.GetByCollectionAndNode(collection, node)
		if replica == nil {
			continue
		}
		err := s.meta.ReplicaManager.RemoveNode(replica.GetID(), node)
		if err != nil {
			log.Warn("failed to remove node from collection's replicas",
				zap.Int64("replicaID", replica.GetID()),
				zap.Error(err),
			)
		}
		log.Info("remove node from replica",
			zap.Int64("replicaID", replica.GetID()))
	}

	// Clear tasks
	s.taskScheduler.RemoveByNode(node)

	rgName, err := s.meta.ResourceManager.HandleNodeDown(node)
	if err != nil {
		log.Warn("HandleNodeDown: failed to remove node from resource group",
			zap.String("resourceGroup", rgName),
			zap.Error(err),
		)
		return
	}

	log.Info("HandleNodeDown: remove node from resource group",
		zap.String("resourceGroup", rgName),
	)
}

// checkReplicas checks whether replica contains offline node, and remove those nodes
func (s *Server) checkReplicas() {
	for _, collection := range s.meta.CollectionManager.GetAll() {
		log := log.With(zap.Int64("collectionID", collection))
		replicas := s.meta.ReplicaManager.GetByCollection(collection)
		for _, replica := range replicas {
			replica := replica.Clone()
			toRemove := make([]int64, 0)
			for _, node := range replica.GetNodes() {
				if s.nodeMgr.Get(node) == nil {
					toRemove = append(toRemove, node)
				}
			}

			if len(toRemove) > 0 {
				log := log.With(
					zap.Int64("replicaID", replica.GetID()),
					zap.Int64s("offlineNodes", toRemove),
				)
				log.Info("some nodes are offline, remove them from replica", zap.Any("toRemove", toRemove))
				replica.RemoveNode(toRemove...)
				err := s.meta.ReplicaManager.Put(replica)
				if err != nil {
					log.Warn("failed to remove offline nodes from replica")
				}
			}
		}
	}
}
