package querycoordv2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	// Only for re-export
	Params = &params.Params
)

type Server struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	status              atomic.Value
	etcdCli             *clientv3.Client
	session             *sessionutil.Session
	kv                  kv.MetaKv
	idAllocator         func() (int64, error)
	factory             dependency.Factory
	metricsCacheManager *metricsinfo.MetricsCacheManager
	chunkManager        storage.ChunkManager

	// Coordinators
	dataCoord  types.DataCoord
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	// Meta
	store     meta.Store
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    meta.Broker

	// Session
	cluster session.Cluster
	nodeMgr *session.NodeManager

	// Schedulers
	jobScheduler  *job.Scheduler
	taskScheduler task.Scheduler

	// HeartBeat
	distController *dist.Controller

	// Checkers
	checkerController *checkers.CheckerController

	// Observers
	collectionObserver *observers.CollectionObserver
	leaderObserver     *observers.LeaderObserver
	handoffObserver    *observers.HandoffObserver

	balancer balance.Balance
}

func NewQueryCoord(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:     ctx,
		cancel:  cancel,
		factory: factory,
	}
	server.UpdateStateCode(internalpb.StateCode_Abnormal)
	return server, nil
}

func (s *Server) Register() error {
	s.session.Register()
	go s.session.LivenessCheck(s.ctx, func() {
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

func (s *Server) Init() error {
	log.Info("QueryCoord start init",
		zap.String("meta-root-path", Params.EtcdCfg.MetaRootPath),
		zap.String("address", Params.QueryCoordCfg.Address))

	// Init QueryCoord session
	s.session = sessionutil.NewSession(s.ctx, Params.EtcdCfg.MetaRootPath, s.etcdCli)
	if s.session == nil {
		return fmt.Errorf("failed to create session")
	}
	s.session.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, true)
	Params.QueryCoordCfg.SetNodeID(s.session.ServerID)
	Params.SetLogger(s.session.ServerID)
	s.factory.Init(Params)

	// Init KV
	etcdKV := etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath)
	s.kv = etcdKV
	log.Debug("query coordinator try to connect etcd success")

	// Init ID allocator
	idAllocatorKV := tsoutil.NewTSOKVBase(s.etcdCli, Params.EtcdCfg.KvRootPath, "querycoord-id-allocator")
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

	// Init chunk manager
	s.chunkManager, err = s.factory.NewVectorStorageChunkManager(s.ctx)
	if err != nil {
		log.Error("failed to init chunk manager", zap.Error(err))
		return err
	}

	// Init meta
	err = s.initMeta()
	if err != nil {
		return err
	}
	// Init session
	log.Debug("init session")
	s.nodeMgr = session.NewNodeManager()
	s.cluster = session.NewCluster(s.nodeMgr)

	// Init schedulers
	log.Debug("init schedulers")
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
	log.Debug("init dist controller")
	s.distController = dist.NewDistController(
		s.cluster,
		s.nodeMgr,
		s.dist,
		s.targetMgr,
		s.taskScheduler,
	)

	// Init balancer
	log.Debug("init balancer")
	s.balancer = balance.NewRowCountBasedBalancer(
		s.taskScheduler,
		s.nodeMgr,
		s.dist,
		s.meta,
	)

	// Init checker controller
	log.Debug("init checker controller")
	s.checkerController = checkers.NewCheckerController(
		s.meta,
		s.dist,
		s.targetMgr,
		s.balancer,
		s.taskScheduler,
	)

	// Init observers
	s.initObserver()

	log.Info("QueryCoord init success")
	return err
}

func (s *Server) initMeta() error {
	log.Debug("init meta")
	s.store = meta.NewMetaStore(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store)

	log.Debug("recover meta...")
	err := s.meta.CollectionManager.Recover()
	if err != nil {
		log.Error("failed to recover collections")
		return err
	}
	err = s.meta.ReplicaManager.Recover()
	if err != nil {
		log.Error("failed to recover replicas")
		return err
	}

	s.dist = &meta.DistributionManager{
		SegmentDistManager: meta.NewSegmentDistManager(),
		ChannelDistManager: meta.NewChannelDistManager(),
		LeaderViewManager:  meta.NewLeaderViewManager(),
	}
	s.targetMgr = meta.NewTargetManager()
	s.broker = meta.NewCoordinatorBroker(
		s.dataCoord,
		s.rootCoord,
		s.indexCoord,
		s.chunkManager,
	)
	return nil
}

func (s *Server) initObserver() {
	log.Debug("init observers")
	s.collectionObserver = observers.NewCollectionObserver(
		s.dist,
		s.meta,
		s.targetMgr,
	)
	s.leaderObserver = observers.NewLeaderObserver(
		s.dist,
		s.meta,
		s.targetMgr,
		s.cluster,
	)
	s.handoffObserver = observers.NewHandoffObserver(
		s.store,
		s.meta,
		s.dist,
		s.targetMgr,
	)
}

func (s *Server) Start() error {
	log.Info("start watcher....")
	sessions, revision, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		return err
	}
	for _, node := range sessions {
		s.nodeMgr.Add(session.NewNodeInfo(node.ServerID, node.Address))
	}
	s.checkReplicas()
	for _, node := range sessions {
		s.handleNodeUp(node.ServerID)
	}
	s.wg.Add(1)
	go s.watchNodes(revision)

	log.Info("start recovering dist and target")
	err = s.recover()
	if err != nil {
		return err
	}

	log.Info("start cluster...")
	s.cluster.Start(s.ctx)

	log.Info("start job scheduler...")
	s.jobScheduler.Start(s.ctx)

	log.Info("start checker controller...")
	s.checkerController.Start(s.ctx)

	log.Info("start observers...")
	s.collectionObserver.Start(s.ctx)
	s.leaderObserver.Start(s.ctx)
	if err := s.handoffObserver.Start(s.ctx); err != nil {
		log.Error("start handoff observer failed, exit...", zap.Error(err))
		panic(err.Error())
	}

	s.status.Store(internalpb.StateCode_Healthy)
	log.Info("QueryCoord started")

	return nil
}

func (s *Server) Stop() error {
	s.cancel()
	s.session.Revoke(time.Second)

	log.Info("stop cluster...")
	s.cluster.Stop()

	log.Info("stop dist controller...")
	s.distController.Stop()

	log.Info("stop checker controller...")
	s.checkerController.Stop()

	log.Info("stop job scheduler...")
	s.jobScheduler.Stop()

	log.Info("stop observers...")
	s.collectionObserver.Stop()
	s.leaderObserver.Stop()
	s.handoffObserver.Stop()

	s.wg.Wait()
	return nil
}

// UpdateStateCode updates the status of the coord, including healthy, unhealthy
func (s *Server) UpdateStateCode(code internalpb.StateCode) {
	s.status.Store(code)
}

func (s *Server) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID
	}
	serviceComponentInfo := &internalpb.ComponentInfo{
		// NodeID:    Params.QueryCoordID, // will race with QueryCoord.Register()
		NodeID:    nodeID,
		StateCode: s.status.Load().(internalpb.StateCode),
	}

	return &internalpb.ComponentStates{
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
		Value: Params.CommonCfg.QueryCoordTimeTick,
	}, nil
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

// SetIndexCoord sets index coordinator's client
func (s *Server) SetIndexCoord(indexCoord types.IndexCoord) error {
	if indexCoord == nil {
		return errors.New("null IndexCoord interface")
	}

	s.indexCoord = indexCoord
	return nil
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
	var (
		partitions []int64
		err        error
	)
	if s.meta.GetLoadType(collection) == querypb.LoadType_LoadCollection {
		partitions, err = s.broker.GetPartitions(ctx, collection)
		if err != nil {
			msg := "failed to get partitions from RootCoord"
			log.Error(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
	} else {
		partitions = lo.Map(s.meta.GetPartitionsByCollection(collection), func(partition *meta.Partition, _ int) int64 {
			return partition.GetPartitionID()
		})
	}

	s.handoffObserver.Register(collection)
	err = utils.RegisterTargets(
		ctx,
		s.targetMgr,
		s.broker,
		collection,
		partitions,
	)
	if err != nil {
		return err
	}
	s.handoffObserver.StartHandoff(collection)
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
				log.Error("Session Watcher channel closed", zap.Int64("serverID", s.session.ServerID))
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
				s.handleNodeUp(nodeID)
				s.metricsCacheManager.InvalidateSystemInfoMetrics()

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

func (s *Server) handleNodeUp(node int64) {
	log := log.With(zap.Int64("nodeID", node))
	s.distController.StartDistInstance(s.ctx, node)

	for _, collection := range s.meta.CollectionManager.GetAll() {
		log := log.With(zap.Int64("collectionID", collection))
		replica := s.meta.ReplicaManager.GetByCollectionAndNode(collection, node)
		if replica == nil {
			replicas := s.meta.ReplicaManager.GetByCollection(collection)
			sort.Slice(replicas, func(i, j int) bool {
				return replicas[i].Nodes.Len() < replicas[j].Nodes.Len()
			})
			replica := replicas[0]
			// TODO(yah01): this may fail, need a component to check whether a node is assigned
			err := s.meta.ReplicaManager.AddNode(replica.GetID(), node)
			if err != nil {
				log.Warn("failed to assign node to replicas",
					zap.Int64("replicaID", replica.GetID()),
					zap.Error(err),
				)
			}
			log.Info("assign node to replica",
				zap.Int64("replicaID", replica.GetID()))
		}
	}
}

func (s *Server) handleNodeDown(node int64) {
	log := log.With(zap.Int64("nodeID", node))
	s.distController.Remove(node)

	// Refresh the targets, to avoid consuming messages too early from channel
	// FIXME(yah01): the leads to miss data, the segments flushed between the two check points
	// are missed, it will recover for a while.
	channels := s.dist.ChannelDistManager.GetByNode(node)
	for _, channel := range channels {
		partitions, err := utils.GetPartitions(s.meta.CollectionManager,
			s.broker,
			channel.GetCollectionID())
		if err != nil {
			log.Warn("failed to refresh targets of collection",
				zap.Int64("collectionID", channel.GetCollectionID()),
				zap.Error(err))
		}
		err = utils.RegisterTargets(s.ctx,
			s.targetMgr,
			s.broker,
			channel.GetCollectionID(),
			partitions)
		if err != nil {
			log.Warn("failed to refresh targets of collection",
				zap.Int64("collectionID", channel.GetCollectionID()),
				zap.Error(err))
		}
	}

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
}

// checkReplicas checks whether replica contains offline node, and remove those nodes
func (s *Server) checkReplicas() {
	for _, collection := range s.meta.CollectionManager.GetAll() {
		log := log.With(zap.Int64("collectionID", collection))
		replicas := s.meta.ReplicaManager.GetByCollection(collection)
		for _, replica := range replicas {
			replica := replica.Clone()
			toRemove := make([]int64, 0)
			for node := range replica.Nodes {
				if s.nodeMgr.Get(node) == nil {
					toRemove = append(toRemove, node)
				}
			}
			log := log.With(
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("offlineNodes", toRemove),
			)

			log.Debug("some nodes are offline, remove them from replica")
			if len(toRemove) > 0 {
				replica.RemoveNode(toRemove...)
				err := s.meta.ReplicaManager.Put(replica)
				if err != nil {
					log.Warn("failed to remove offline nodes from replica")
				}
			}
		}
	}
}
