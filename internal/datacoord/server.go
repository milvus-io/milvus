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

package datacoord

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	indexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	rootcoordclient "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	streamingcoord "github.com/milvus-io/milvus/internal/streamingcoord/server"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	connMetaMaxRetryTime = 100
	allPartitionID       = 0 // partitionID means no filtering
)

var (
	// TODO: sunby put to config
	enableTtChecker           = true
	ttCheckerName             = "dataTtChecker"
	ttMaxInterval             = 2 * time.Minute
	ttCheckerWarnMsg          = fmt.Sprintf("Datacoord haven't received tt for %f minutes", ttMaxInterval.Minutes())
	segmentTimedFlushDuration = 10.0
)

type (
	// UniqueID shortcut for typeutil.UniqueID
	UniqueID = typeutil.UniqueID
	// Timestamp shortcurt for typeutil.Timestamp
	Timestamp = typeutil.Timestamp
)

type rootCoordCreatorFunc func(ctx context.Context) (types.RootCoordClient, error)

// makes sure Server implements `DataCoord`
var _ types.DataCoord = (*Server)(nil)

var Params = paramtable.Get()

// Server implements `types.DataCoord`
// handles Data Coordinator related jobs
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	quitCh           chan struct{}
	stateCode        atomic.Value

	etcdCli          *clientv3.Client
	tikvCli          *txnkv.Client
	address          string
	watchClient      kv.WatchKV
	kv               kv.MetaKv
	meta             *meta
	segmentManager   Manager
	allocator        allocator.Allocator
	cluster          Cluster
	sessionManager   session.DataNodeManager
	channelManager   ChannelManager
	rootCoordClient  types.RootCoordClient
	garbageCollector *garbageCollector
	gcOpt            GcOption
	handler          Handler
	importMeta       ImportMeta
	importScheduler  ImportScheduler
	importChecker    ImportChecker

	compactionTrigger        trigger
	compactionHandler        compactionPlanContext
	compactionTriggerManager TriggerManager

	syncSegmentsScheduler *SyncSegmentsScheduler
	metricsCacheManager   *metricsinfo.MetricsCacheManager

	flushCh         chan UniqueID
	statsCh         chan UniqueID
	buildIndexCh    chan UniqueID
	notifyIndexChan chan UniqueID
	factory         dependency.Factory

	session   sessionutil.SessionInterface
	icSession *sessionutil.Session
	dnEventCh <-chan *sessionutil.SessionEvent
	inEventCh <-chan *sessionutil.SessionEvent
	// qcEventCh <-chan *sessionutil.SessionEvent
	qnEventCh <-chan *sessionutil.SessionEvent

	enableActiveStandBy bool
	activateFunc        func() error

	dataNodeCreator        session.DataNodeCreatorFunc
	indexNodeCreator       session.IndexNodeCreatorFunc
	rootCoordClientCreator rootCoordCreatorFunc
	// indexCoord             types.IndexCoord

	// segReferManager  *SegmentReferenceManager
	indexNodeManager          *session.IndexNodeManager
	indexEngineVersionManager IndexEngineVersionManager

	taskScheduler *taskScheduler

	// manage ways that data coord access other coord
	broker broker.Broker

	// streamingcoord server is embedding in datacoord now.
	streamingCoord *streamingcoord.Server
}

type CollectionNameInfo struct {
	CollectionName string
	DBName         string
}

// Option utility function signature to set DataCoord server attributes
type Option func(svr *Server)

// WithRootCoordCreator returns an `Option` setting RootCoord creator with provided parameter
func WithRootCoordCreator(creator rootCoordCreatorFunc) Option {
	return func(svr *Server) {
		svr.rootCoordClientCreator = creator
	}
}

// WithCluster returns an `Option` setting Cluster with provided parameter
func WithCluster(cluster Cluster) Option {
	return func(svr *Server) {
		svr.cluster = cluster
	}
}

// WithDataNodeCreator returns an `Option` setting DataNode create function
func WithDataNodeCreator(creator session.DataNodeCreatorFunc) Option {
	return func(svr *Server) {
		svr.dataNodeCreator = creator
	}
}

// WithSegmentManager returns an Option to set SegmentManager
func WithSegmentManager(manager Manager) Option {
	return func(svr *Server) {
		svr.segmentManager = manager
	}
}

// CreateServer creates a `Server` instance
func CreateServer(ctx context.Context, factory dependency.Factory, opts ...Option) *Server {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:                    ctx,
		quitCh:                 make(chan struct{}),
		factory:                factory,
		flushCh:                make(chan UniqueID, 1024),
		statsCh:                make(chan UniqueID, 1024),
		buildIndexCh:           make(chan UniqueID, 1024),
		notifyIndexChan:        make(chan UniqueID),
		dataNodeCreator:        defaultDataNodeCreatorFunc,
		indexNodeCreator:       defaultIndexNodeCreatorFunc,
		rootCoordClientCreator: defaultRootCoordCreatorFunc,
		metricsCacheManager:    metricsinfo.NewMetricsCacheManager(),
		enableActiveStandBy:    Params.DataCoordCfg.EnableActiveStandby.GetAsBool(),
	}

	for _, opt := range opts {
		opt(s)
	}
	expr.Register("datacoord", s)
	return s
}

func defaultDataNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
	return datanodeclient.NewClient(ctx, addr, nodeID)
}

func defaultIndexNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.IndexNodeClient, error) {
	return indexnodeclient.NewClient(ctx, addr, nodeID, Params.DataCoordCfg.WithCredential.GetAsBool())
}

func defaultRootCoordCreatorFunc(ctx context.Context) (types.RootCoordClient, error) {
	return rootcoordclient.NewClient(ctx)
}

// QuitSignal returns signal when server quits
func (s *Server) QuitSignal() <-chan struct{} {
	return s.quitCh
}

// Register registers data service at etcd
func (s *Server) Register() error {
	// first register indexCoord
	s.icSession.Register()
	s.session.Register()
	afterRegister := func() {
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.DataCoordRole).Inc()
		log.Info("DataCoord Register Finished")

		s.session.LivenessCheck(s.ctx, func() {
			logutil.Logger(s.ctx).Error("disconnected from etcd and exited", zap.Int64("serverID", s.session.GetServerID()))
			os.Exit(1)
		})
	}
	if s.enableActiveStandBy {
		go func() {
			err := s.session.ProcessActiveStandBy(s.activateFunc)
			if err != nil {
				log.Error("failed to activate standby datacoord server", zap.Error(err))
				panic(err)
			}

			err = s.icSession.ForceActiveStandby(nil)
			if err != nil {
				log.Error("failed to force activate standby indexcoord server", zap.Error(err))
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
	s.icSession = sessionutil.NewSession(s.ctx)
	if s.icSession == nil {
		return errors.New("failed to initialize IndexCoord session")
	}
	s.icSession.Init(typeutil.IndexCoordRole, s.address, true, true)
	s.icSession.SetEnableActiveStandBy(s.enableActiveStandBy)

	s.session = sessionutil.NewSession(s.ctx)
	if s.session == nil {
		return errors.New("failed to initialize session")
	}
	s.session.Init(typeutil.DataCoordRole, s.address, true, true)
	s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	return nil
}

// Init change server state to Initializing
func (s *Server) Init() error {
	var err error
	s.factory.Init(Params)
	if err = s.initSession(); err != nil {
		return err
	}
	if s.enableActiveStandBy {
		s.activateFunc = func() error {
			log.Info("DataCoord switch from standby to active, activating")
			if err := s.initDataCoord(); err != nil {
				log.Error("DataCoord init failed", zap.Error(err))
				return err
			}
			s.startDataCoord()
			log.Info("DataCoord startup success")

			if s.streamingCoord != nil {
				s.streamingCoord.Start()
				log.Info("StreamingCoord stratup successfully at standby mode")
			}
			return nil
		}
		s.stateCode.Store(commonpb.StateCode_StandBy)
		log.Info("DataCoord enter standby mode successfully")
		return nil
	}

	return s.initDataCoord()
}

func (s *Server) RegisterStreamingCoordGRPCService(server *grpc.Server) {
	s.streamingCoord.RegisterGRPCService(server)
}

func (s *Server) initDataCoord() error {
	s.stateCode.Store(commonpb.StateCode_Initializing)
	var err error
	if err = s.initRootCoordClient(); err != nil {
		return err
	}
	log.Info("init rootcoord client done")

	s.broker = broker.NewCoordinatorBroker(s.rootCoordClient)
	s.allocator = allocator.NewRootCoordAllocator(s.rootCoordClient)

	storageCli, err := s.newChunkManagerFactory()
	if err != nil {
		return err
	}
	log.Info("init chunk manager factory done")

	if err = s.initMeta(storageCli); err != nil {
		return err
	}

	// Initialize streaming coordinator.
	if streamingutil.IsStreamingServiceEnabled() {
		s.streamingCoord = streamingcoord.NewServerBuilder().
			WithETCD(s.etcdCli).
			WithMetaKV(s.kv).
			WithSession(s.session).Build()
		if err = s.streamingCoord.Init(context.TODO()); err != nil {
			return err
		}
		log.Info("init streaming coordinator done")
	}

	s.handler = newServerHandler(s)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err = s.initCluster(); err != nil {
		return err
	}
	log.Info("init datanode cluster done")

	s.initIndexNodeManager()

	if err = s.initServiceDiscovery(); err != nil {
		return err
	}
	log.Info("init service discovery done")

	s.initTaskScheduler(storageCli)
	log.Info("init task scheduler done")

	s.initCompaction()
	log.Info("init compaction done")

	if err = s.initSegmentManager(); err != nil {
		return err
	}
	log.Info("init segment manager done")

	s.initGarbageCollection(storageCli)

	s.importMeta, err = NewImportMeta(s.meta.catalog)
	if err != nil {
		return err
	}
	s.importScheduler = NewImportScheduler(s.meta, s.cluster, s.allocator, s.importMeta, s.statsCh)
	s.importChecker = NewImportChecker(s.meta, s.broker, s.cluster, s.allocator, s.segmentManager, s.importMeta)

	s.syncSegmentsScheduler = newSyncSegmentsScheduler(s.meta, s.channelManager, s.sessionManager)

	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)

	log.Info("init datacoord done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", s.address))
	return nil
}

// Start initialize `Server` members and start loops, follow steps are taken:
//  1. initialize message factory parameters
//  2. initialize root coord client, meta, datanode cluster, segment info channel,
//     allocator, segment manager
//  3. start service discovery and server loops, which includes message stream handler (segment statistics,datanode tt)
//     datanodes etcd watch, etcd alive check and flush completed status check
//  4. set server state to Healthy
func (s *Server) Start() error {
	if !s.enableActiveStandBy {
		s.startDataCoord()
		log.Info("DataCoord startup successfully")
		if s.streamingCoord != nil {
			s.streamingCoord.Start()
			log.Info("StreamingCoord stratup successfully")
		}
	}

	return nil
}

func (s *Server) startDataCoord() {
	s.startTaskScheduler()
	s.startServerLoop()

	// http.Register(&http.Handler{
	// 	Path: "/datacoord/garbage_collection/pause",
	// 	HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
	// 		pauseSeconds := req.URL.Query().Get("pause_seconds")
	// 		seconds, err := strconv.ParseInt(pauseSeconds, 10, 64)
	// 		if err != nil {
	// 			w.WriteHeader(400)
	// 			w.Write([]byte(fmt.Sprintf(`{"msg": "invalid pause seconds(%v)"}`, pauseSeconds)))
	// 			return
	// 		}

	// 		err = s.garbageCollector.Pause(req.Context(), time.Duration(seconds)*time.Second)
	// 		if err != nil {
	// 			w.WriteHeader(500)
	// 			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, err.Error())))
	// 			return
	// 		}
	// 		w.WriteHeader(200)
	// 		w.Write([]byte(`{"msg": "OK"}`))
	// 		return
	// 	},
	// })
	// http.Register(&http.Handler{
	// 	Path: "/datacoord/garbage_collection/resume",
	// 	HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
	// 		err := s.garbageCollector.Resume(req.Context())
	// 		if err != nil {
	// 			w.WriteHeader(500)
	// 			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, err.Error())))
	// 			return
	// 		}
	// 		w.WriteHeader(200)
	// 		w.Write([]byte(`{"msg": "OK"}`))
	// 		return
	// 	},
	// })

	s.afterStart()
	s.stateCode.Store(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.DataCoordRole, s.session.GetServerID())
}

func (s *Server) GetServerID() int64 {
	if s.session != nil {
		return s.session.GetServerID()
	}
	return paramtable.GetNodeID()
}

func (s *Server) afterStart() {}

func (s *Server) initCluster() error {
	if s.cluster != nil {
		return nil
	}

	s.sessionManager = session.NewDataNodeManagerImpl(session.WithDataNodeCreator(s.dataNodeCreator))

	var err error
	s.channelManager, err = NewChannelManager(s.watchClient, s.handler, s.sessionManager, s.allocator, withCheckerV2())
	if err != nil {
		return err
	}
	s.cluster = NewClusterImpl(s.sessionManager, s.channelManager)
	return nil
}

func (s *Server) SetAddress(address string) {
	s.address = address
}

// SetEtcdClient sets etcd client for datacoord.
func (s *Server) SetEtcdClient(client *clientv3.Client) {
	s.etcdCli = client
}

func (s *Server) SetTiKVClient(client *txnkv.Client) {
	s.tikvCli = client
}

func (s *Server) SetRootCoordClient(rootCoord types.RootCoordClient) {
	s.rootCoordClient = rootCoord
}

func (s *Server) SetDataNodeCreator(f func(context.Context, string, int64) (types.DataNodeClient, error)) {
	s.dataNodeCreator = f
}

func (s *Server) SetIndexNodeCreator(f func(context.Context, string, int64) (types.IndexNodeClient, error)) {
	s.indexNodeCreator = f
}

func (s *Server) newChunkManagerFactory() (storage.ChunkManager, error) {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(s.ctx)
	if err != nil {
		log.Error("chunk manager init failed", zap.Error(err))
		return nil, err
	}
	return cli, err
}

func (s *Server) initGarbageCollection(cli storage.ChunkManager) {
	s.garbageCollector = newGarbageCollector(s.meta, s.handler, GcOption{
		cli:              cli,
		broker:           s.broker,
		enabled:          Params.DataCoordCfg.EnableGarbageCollection.GetAsBool(),
		checkInterval:    Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second),
		scanInterval:     Params.DataCoordCfg.GCScanIntervalInHour.GetAsDuration(time.Hour),
		missingTolerance: Params.DataCoordCfg.GCMissingTolerance.GetAsDuration(time.Second),
		dropTolerance:    Params.DataCoordCfg.GCDropTolerance.GetAsDuration(time.Second),
	})
}

func (s *Server) initServiceDiscovery() error {
	r := semver.MustParseRange(">=2.2.3")
	sessions, rev, err := s.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		log.Warn("DataCoord failed to init service discovery", zap.Error(err))
		return err
	}
	log.Info("DataCoord success to get DataNode sessions", zap.Any("sessions", sessions))

	datanodes := make([]*session.NodeInfo, 0, len(sessions))
	legacyVersion, err := semver.Parse(paramtable.Get().DataCoordCfg.LegacyVersionWithoutRPCWatch.GetValue())
	if err != nil {
		log.Warn("DataCoord failed to init service discovery", zap.Error(err))
	}

	for _, s := range sessions {
		info := &session.NodeInfo{
			NodeID:  s.ServerID,
			Address: s.Address,
		}

		if s.Version.LTE(legacyVersion) {
			info.IsLegacy = true
		}

		datanodes = append(datanodes, info)
	}

	log.Info("DataCoord Cluster Manager start up")
	if err := s.cluster.Startup(s.ctx, datanodes); err != nil {
		log.Warn("DataCoord Cluster Manager failed to start up", zap.Error(err))
		return err
	}
	log.Info("DataCoord Cluster Manager start up successfully")

	// TODO implement rewatch logic
	s.dnEventCh = s.session.WatchServicesWithVersionRange(typeutil.DataNodeRole, r, rev+1, nil)

	inSessions, inRevision, err := s.session.GetSessions(typeutil.IndexNodeRole)
	if err != nil {
		log.Warn("DataCoord get QueryCoord session failed", zap.Error(err))
		return err
	}
	if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
		if err = s.indexNodeManager.AddNode(Params.DataCoordCfg.IndexNodeID.GetAsInt64(), Params.DataCoordCfg.IndexNodeAddress.GetValue()); err != nil {
			log.Error("add indexNode fail", zap.Int64("ServerID", Params.DataCoordCfg.IndexNodeID.GetAsInt64()),
				zap.String("address", Params.DataCoordCfg.IndexNodeAddress.GetValue()), zap.Error(err))
			return err
		}
		log.Info("add indexNode success", zap.String("IndexNode address", Params.DataCoordCfg.IndexNodeAddress.GetValue()),
			zap.Int64("nodeID", Params.DataCoordCfg.IndexNodeID.GetAsInt64()))
	} else {
		for _, session := range inSessions {
			if err := s.indexNodeManager.AddNode(session.ServerID, session.Address); err != nil {
				return err
			}
		}
	}
	s.inEventCh = s.session.WatchServices(typeutil.IndexNodeRole, inRevision+1, nil)

	s.indexEngineVersionManager = newIndexEngineVersionManager()
	qnSessions, qnRevision, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		log.Warn("DataCoord get QueryNode sessions failed", zap.Error(err))
		return err
	}
	s.indexEngineVersionManager.Startup(qnSessions)
	s.qnEventCh = s.session.WatchServicesWithVersionRange(typeutil.QueryNodeRole, r, qnRevision+1, nil)

	return nil
}

func (s *Server) initSegmentManager() error {
	if s.segmentManager == nil {
		manager, err := newSegmentManager(s.meta, s.allocator)
		if err != nil {
			return err
		}
		s.segmentManager = manager
	}
	return nil
}

func (s *Server) initMeta(chunkManager storage.ChunkManager) error {
	if s.meta != nil {
		return nil
	}
	s.watchClient = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
		etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	log.Info("data coordinator connecting to metadata store", zap.String("metaType", metaType))
	metaRootPath := ""
	if metaType == util.MetaStoreTypeTiKV {
		metaRootPath = Params.TiKVCfg.MetaRootPath.GetValue()
		s.kv = tikv.NewTiKV(s.tikvCli, metaRootPath,
			tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else if metaType == util.MetaStoreTypeEtcd {
		metaRootPath = Params.EtcdCfg.MetaRootPath.GetValue()
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else {
		return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", metaType))
	}
	log.Info("data coordinator successfully connected to metadata store", zap.String("metaType", metaType))

	reloadEtcdFn := func() error {
		var err error
		catalog := datacoord.NewCatalog(s.kv, chunkManager.RootPath(), metaRootPath)
		s.meta, err = newMeta(s.ctx, catalog, chunkManager)
		if err != nil {
			return err
		}

		// Load collection information asynchronously
		// HINT: please make sure this is the last step in the `reloadEtcdFn` function !!!
		go func() {
			_ = retry.Do(s.ctx, func() error {
				return s.meta.reloadCollectionsFromRootcoord(s.ctx, s.broker)
			}, retry.Sleep(time.Second), retry.Attempts(connMetaMaxRetryTime))
		}()
		return nil
	}
	return retry.Do(s.ctx, reloadEtcdFn, retry.Attempts(connMetaMaxRetryTime))
}

func (s *Server) initTaskScheduler(manager storage.ChunkManager) {
	if s.taskScheduler == nil {
		s.taskScheduler = newTaskScheduler(s.ctx, s.meta, s.indexNodeManager, manager, s.indexEngineVersionManager, s.handler, s.allocator)
	}
}

func (s *Server) initIndexNodeManager() {
	if s.indexNodeManager == nil {
		s.indexNodeManager = session.NewNodeManager(s.ctx, s.indexNodeCreator)
	}
}

func (s *Server) initCompaction() {
	s.compactionHandler = newCompactionPlanHandler(s.cluster, s.sessionManager, s.channelManager, s.meta, s.allocator, s.taskScheduler, s.handler)
	s.compactionTriggerManager = NewCompactionTriggerManager(s.allocator, s.handler, s.compactionHandler, s.meta)
	s.compactionTrigger = newCompactionTrigger(s.meta, s.compactionHandler, s.allocator, s.handler, s.indexEngineVersionManager)
}

func (s *Server) stopCompaction() {
	if s.compactionTrigger != nil {
		s.compactionTrigger.stop()
	}
	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Stop()
	}

	if s.compactionHandler != nil {
		s.compactionHandler.stop()
	}
}

func (s *Server) startCompaction() {
	if s.compactionHandler != nil {
		s.compactionHandler.start()
	}

	if s.compactionTrigger != nil {
		s.compactionTrigger.start()
	}

	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Start()
	}
}

func (s *Server) startServerLoop() {
	if Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		s.startCompaction()
	}

	s.serverLoopWg.Add(2)
	s.startWatchService(s.serverLoopCtx)
	s.startFlushLoop(s.serverLoopCtx)
	go s.importScheduler.Start()
	go s.importChecker.Start()
	s.garbageCollector.start()

	if !streamingutil.IsStreamingServiceEnabled() {
		s.syncSegmentsScheduler.Start()
	}
}

func (s *Server) startTaskScheduler() {
	s.taskScheduler.Start()

	s.startIndexService(s.serverLoopCtx)
	s.startStatsTasksCheckLoop(s.serverLoopCtx)
}

func (s *Server) updateSegmentStatistics(stats []*commonpb.SegmentStats) {
	for _, stat := range stats {
		segment := s.meta.GetSegment(stat.GetSegmentID())
		if segment == nil {
			log.Warn("skip updating row number for not exist segment",
				zap.Int64("segmentID", stat.GetSegmentID()),
				zap.Int64("new value", stat.GetNumRows()))
			continue
		}

		if isFlushState(segment.GetState()) {
			log.Warn("skip updating row number for flushed segment",
				zap.Int64("segmentID", stat.GetSegmentID()),
				zap.Int64("new value", stat.GetNumRows()))
			continue
		}

		// Log if # of rows is updated.
		if segment.currRows < stat.GetNumRows() {
			log.Debug("Updating segment number of rows",
				zap.Int64("segmentID", stat.GetSegmentID()),
				zap.Int64("old value", s.meta.GetSegment(stat.GetSegmentID()).GetNumOfRows()),
				zap.Int64("new value", stat.GetNumRows()),
			)
			s.meta.SetCurrentRows(stat.GetSegmentID(), stat.GetNumRows())
		}
	}
}

func (s *Server) getFlushableSegmentsInfo(flushableIDs []int64) []*SegmentInfo {
	res := make([]*SegmentInfo, 0, len(flushableIDs))
	for _, id := range flushableIDs {
		sinfo := s.meta.GetHealthySegment(id)
		if sinfo == nil {
			log.Error("get segment from meta error", zap.Int64("id", id))
			continue
		}
		res = append(res, sinfo)
	}
	return res
}

func (s *Server) setLastFlushTime(segments []*SegmentInfo) {
	for _, sinfo := range segments {
		s.meta.SetLastFlushTime(sinfo.GetID(), time.Now())
	}
}

// start a goroutine wto watch services
func (s *Server) startWatchService(ctx context.Context) {
	go s.watchService(ctx)
}

func (s *Server) stopServiceWatch() {
	// ErrCompacted is handled inside SessionWatcher, which means there is some other error occurred, closing server.
	logutil.Logger(s.ctx).Error("watch service channel closed", zap.Int64("serverID", paramtable.GetNodeID()))
	go s.Stop()
	if s.session.IsTriggerKill() {
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			p.Signal(syscall.SIGINT)
		}
	}
}

// watchService watches services.
func (s *Server) watchService(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Info("watch service shutdown")
			return
		case event, ok := <-s.dnEventCh:
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.DataNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						log.Warn("DataCoord server stop error", zap.Error(err))
					}
				}()
				return
			}
		case event, ok := <-s.inEventCh:
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.IndexNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						log.Warn("DataCoord server stop error", zap.Error(err))
					}
				}()
				return
			}
		case event, ok := <-s.qnEventCh:
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.QueryNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						log.Warn("DataCoord server stop error", zap.Error(err))
					}
				}()
				return
			}
		}
	}
}

// handles session events - DataNodes Add/Del
func (s *Server) handleSessionEvent(ctx context.Context, role string, event *sessionutil.SessionEvent) error {
	if event == nil {
		return nil
	}
	log := log.Ctx(ctx)
	switch role {
	case typeutil.DataNodeRole:
		info := &datapb.DataNodeInfo{
			Address:  event.Session.Address,
			Version:  event.Session.ServerID,
			Channels: []*datapb.ChannelStatus{},
		}
		node := &session.NodeInfo{
			NodeID:  event.Session.ServerID,
			Address: event.Session.Address,
		}
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			log.Info("received datanode register",
				zap.String("address", info.Address),
				zap.Int64("serverID", info.Version))
			if err := s.cluster.Register(node); err != nil {
				log.Warn("failed to register node", zap.Int64("id", node.NodeID), zap.String("address", node.Address), zap.Error(err))
				return err
			}
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
		case sessionutil.SessionDelEvent:
			log.Info("received datanode unregister",
				zap.String("address", info.Address),
				zap.Int64("serverID", info.Version))
			if err := s.cluster.UnRegister(node); err != nil {
				log.Warn("failed to deregister node", zap.Int64("id", node.NodeID), zap.String("address", node.Address), zap.Error(err))
				return err
			}
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
		default:
			log.Warn("receive unknown service event type",
				zap.Any("type", event.EventType))
		}
	case typeutil.IndexNodeRole:
		if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
			log.Info("receive indexnode session event, but adding indexnode by bind mode, skip it",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID),
				zap.String("event type", event.EventType.String()))
			return nil
		}
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			log.Info("received indexnode register",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID))
			return s.indexNodeManager.AddNode(event.Session.ServerID, event.Session.Address)
		case sessionutil.SessionDelEvent:
			log.Info("received indexnode unregister",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID))
			s.indexNodeManager.RemoveNode(event.Session.ServerID)
		case sessionutil.SessionUpdateEvent:
			serverID := event.Session.ServerID
			log.Info("received indexnode SessionUpdateEvent", zap.Int64("serverID", serverID))
			s.indexNodeManager.StoppingNode(serverID)
		default:
			log.Warn("receive unknown service event type",
				zap.Any("type", event.EventType))
		}
	case typeutil.QueryNodeRole:
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			log.Info("received querynode register",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID))
			s.indexEngineVersionManager.AddNode(event.Session)
		case sessionutil.SessionDelEvent:
			log.Info("received querynode unregister",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID))
			s.indexEngineVersionManager.RemoveNode(event.Session)
		case sessionutil.SessionUpdateEvent:
			serverID := event.Session.ServerID
			log.Info("received querynode SessionUpdateEvent", zap.Int64("serverID", serverID))
			s.indexEngineVersionManager.Update(event.Session)
		default:
			log.Warn("receive unknown service event type",
				zap.Any("type", event.EventType))
		}
	}

	return nil
}

// startFlushLoop starts a goroutine to handle post func process
// which is to notify `RootCoord` that this segment is flushed
func (s *Server) startFlushLoop(ctx context.Context) {
	go func() {
		defer logutil.LogPanic()
		defer s.serverLoopWg.Done()
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		// send `Flushing` segments
		go s.handleFlushingSegments(ctx2)
		for {
			select {
			case <-ctx.Done():
				logutil.Logger(s.ctx).Info("flush loop shutdown")
				return
			case segmentID := <-s.flushCh:
				// Ignore return error
				log.Info("flush successfully", zap.Any("segmentID", segmentID))
				err := s.postFlush(ctx, segmentID)
				if err != nil {
					log.Warn("failed to do post flush", zap.Int64("segmentID", segmentID), zap.Error(err))
				}
			}
		}
	}()
}

// post function after flush is done
// 1. check segment id is valid
// 2. notify RootCoord segment is flushed
// 3. change segment state to `Flushed` in meta
func (s *Server) postFlush(ctx context.Context, segmentID UniqueID) error {
	log := log.Ctx(ctx)
	segment := s.meta.GetHealthySegment(segmentID)
	if segment == nil {
		return merr.WrapErrSegmentNotFound(segmentID, "segment not found, might be a faked segment, ignore post flush")
	}
	// set segment to SegmentState_Flushed
	if err := s.meta.SetState(segmentID, commonpb.SegmentState_Flushed); err != nil {
		log.Error("flush segment complete failed", zap.Error(err))
		return err
	}
	select {
	case s.statsCh <- segmentID:
	default:
	}

	insertFileNum := 0
	for _, fieldBinlog := range segment.GetBinlogs() {
		insertFileNum += len(fieldBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

	statFileNum := 0
	for _, fieldBinlog := range segment.GetStatslogs() {
		statFileNum += len(fieldBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

	deleteFileNum := 0
	for _, filedBinlog := range segment.GetDeltalogs() {
		deleteFileNum += len(filedBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))

	log.Info("flush segment complete", zap.Int64("id", segmentID))
	return nil
}

// recovery logic, fetch all Segment in `Flushing` state and do Flush notification logic
func (s *Server) handleFlushingSegments(ctx context.Context) {
	segments := s.meta.GetFlushingSegments()
	for _, segment := range segments {
		select {
		case <-ctx.Done():
			return
		case s.flushCh <- segment.ID:
		}
	}
}

func (s *Server) initRootCoordClient() error {
	var err error
	if s.rootCoordClient == nil {
		if s.rootCoordClient, err = s.rootCoordClientCreator(s.ctx); err != nil {
			return err
		}
	}
	return nil
}

// Stop do the Server finalize processes
// it checks the server status is healthy, if not, just quit
// if Server is healthy, set server state to stopped, release etcd session,
//
//	stop message stream client and stop server loops
func (s *Server) Stop() error {
	if !s.stateCode.CompareAndSwap(commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal) {
		return nil
	}
	logutil.Logger(s.ctx).Info("datacoord server shutdown")
	s.garbageCollector.close()
	logutil.Logger(s.ctx).Info("datacoord garbage collector stopped")

	if s.streamingCoord != nil {
		log.Info("StreamingCoord stoping...")
		s.streamingCoord.Stop()
		log.Info("StreamingCoord stopped")
	}

	s.stopServerLoop()

	s.importScheduler.Close()
	s.importChecker.Close()
	s.syncSegmentsScheduler.Stop()

	s.stopCompaction()
	logutil.Logger(s.ctx).Info("datacoord compaction stopped")

	s.taskScheduler.Stop()
	logutil.Logger(s.ctx).Info("datacoord index builder stopped")

	s.cluster.Close()
	logutil.Logger(s.ctx).Info("datacoord cluster stopped")

	if s.session != nil {
		s.session.Stop()
	}

	if s.icSession != nil {
		s.icSession.Stop()
	}

	s.stopServerLoop()
	logutil.Logger(s.ctx).Info("datacoord serverloop stopped")
	logutil.Logger(s.ctx).Warn("datacoord stop successful")
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	log.Debug("clean meta", zap.Any("kv", s.kv))
	err := s.kv.RemoveWithPrefix("")
	err2 := s.watchClient.RemoveWithPrefix("")
	if err2 != nil {
		if err != nil {
			err = fmt.Errorf("Failed to CleanMeta[metadata cleanup error: %w][watchdata cleanup error: %v]", err, err2)
		} else {
			err = err2
		}
	}
	return err
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

// func (s *Server) validateAllocRequest(collID UniqueID, partID UniqueID, channelName string) error {
//	if !s.meta.HasCollection(collID) {
//		return fmt.Errorf("can not find collection %d", collID)
//	}
//	if !s.meta.HasPartition(collID, partID) {
//		return fmt.Errorf("can not find partition %d", partID)
//	}
//	for _, name := range s.insertChannels {
//		if name == channelName {
//			return nil
//		}
//	}
//	return fmt.Errorf("can not find channel %s", channelName)
// }

// loadCollectionFromRootCoord communicates with RootCoord and asks for collection information.
// collection information will be added to server meta info.
func (s *Server) loadCollectionFromRootCoord(ctx context.Context, collectionID int64) error {
	has, err := s.broker.HasCollection(ctx, collectionID)
	if err != nil {
		return err
	}
	if !has {
		return merr.WrapErrCollectionNotFound(collectionID)
	}

	resp, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return err
	}
	partitionIDs, err := s.broker.ShowPartitionsInternal(ctx, collectionID)
	if err != nil {
		return err
	}

	properties := make(map[string]string)
	for _, pair := range resp.Properties {
		properties[pair.GetKey()] = pair.GetValue()
	}

	collInfo := &collectionInfo{
		ID:             resp.CollectionID,
		Schema:         resp.Schema,
		Partitions:     partitionIDs,
		StartPositions: resp.GetStartPositions(),
		Properties:     properties,
		CreatedAt:      resp.GetCreatedTimestamp(),
		DatabaseName:   resp.GetDbName(),
		DatabaseID:     resp.GetDbId(),
		VChannelNames:  resp.GetVirtualChannelNames(),
	}
	s.meta.AddCollection(collInfo)
	return nil
}

func (s *Server) updateBalanceConfigLoop(ctx context.Context) {
	success := s.updateBalanceConfig()
	if success {
		return
	}

	s.serverLoopWg.Add(1)
	go func() {
		defer s.serverLoopWg.Done()
		ticker := time.NewTicker(Params.DataCoordCfg.CheckAutoBalanceConfigInterval.GetAsDuration(time.Second))
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
	r := semver.MustParseRange("<2.3.0")
	sessions, _, err := s.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		log.Warn("check data node version occur error on etcd", zap.Error(err))
		return false
	}

	if len(sessions) == 0 {
		// only balance channel when all data node's version > 2.3.0
		Params.Reset(Params.DataCoordCfg.AutoBalance.Key)
		log.Info("all old data node down, enable auto balance!")
		return true
	}

	Params.Save(Params.DataCoordCfg.AutoBalance.Key, "false")
	log.RatedDebug(10, "old data node exist", zap.Strings("sessions", lo.Keys(sessions)))
	return false
}
