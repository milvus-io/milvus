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
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	globalIDAllocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	indexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	etcdCli        *clientv3.Client
	tikvCli        *txnkv.Client
	address        string
	watchClient    kv.WatchKV
	kv             kv.MetaKv
	metaRootPath   string
	meta           *meta
	segmentManager Manager
	allocator      allocator.Allocator
	// self host id allocator, to avoid get unique id from rootcoord
	idAllocator      *globalIDAllocator.GlobalIDAllocator
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
	jobManager    StatsJobManager

	// manage ways that data coord access other coord
	broker broker.Broker

	metricsRequest *metricsinfo.MetricsRequest
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
		notifyIndexChan:        make(chan UniqueID, 1024),
		dataNodeCreator:        defaultDataNodeCreatorFunc,
		indexNodeCreator:       defaultIndexNodeCreatorFunc,
		rootCoordClientCreator: defaultRootCoordCreatorFunc,
		metricsCacheManager:    metricsinfo.NewMetricsCacheManager(),
		enableActiveStandBy:    Params.DataCoordCfg.EnableActiveStandby.GetAsBool(),
		metricsRequest:         metricsinfo.NewMetricsRequest(),
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
	return coordclient.GetRootCoordClient(ctx), nil
}

// QuitSignal returns signal when server quits
func (s *Server) QuitSignal() <-chan struct{} {
	return s.quitCh
}

// Register registers data service at etcd
func (s *Server) Register() error {
	log := log.Ctx(s.ctx)
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
	log := log.Ctx(s.ctx)
	var err error
	s.registerMetricsRequest()
	s.factory.Init(Params)
	if err = s.initSession(); err != nil {
		return err
	}
	if err := s.initKV(); err != nil {
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
			return nil
		}
		s.UpdateStateCode(commonpb.StateCode_StandBy)
		log.Info("DataCoord enter standby mode successfully")
		return nil
	}

	return s.initDataCoord()
}

func (s *Server) initDataCoord() error {
	log := log.Ctx(s.ctx)
	// wait for master init or healthy
	log.Info("DataCoord try to wait for RootCoord ready")
	if err := s.initRootCoordClient(); err != nil {
		return err
	}
	log.Info("init rootcoord client done")
	err := componentutil.WaitForComponentHealthy(s.ctx, s.rootCoordClient, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Error("DataCoord wait for RootCoord ready failed", zap.Error(err))
		return err
	}
	log.Info("DataCoord report RootCoord ready")

	s.UpdateStateCode(commonpb.StateCode_Initializing)

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

	// init id allocator after init meta
	s.idAllocator = globalIDAllocator.NewGlobalIDAllocator("idTimestamp", s.kv)
	err = s.idAllocator.Initialize()
	if err != nil {
		log.Error("data coordinator id allocator initialize failed", zap.Error(err))
		return err
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

	s.importMeta, err = NewImportMeta(s.ctx, s.meta.catalog)
	if err != nil {
		return err
	}
	s.initCompaction()
	log.Info("init compaction done")

	s.initTaskScheduler(storageCli)
	log.Info("init task scheduler done")

	s.initJobManager()
	log.Info("init statsJobManager done")

	if err = s.initSegmentManager(); err != nil {
		return err
	}
	log.Info("init segment manager done")

	s.initGarbageCollection(storageCli)

	s.importScheduler = NewImportScheduler(s.meta, s.cluster, s.allocator, s.importMeta)
	s.importChecker = NewImportChecker(s.meta, s.broker, s.cluster, s.allocator, s.importMeta, s.jobManager, s.compactionTriggerManager)

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
	log := log.Ctx(s.ctx)
	if !s.enableActiveStandBy {
		s.startDataCoord()
		log.Info("DataCoord startup successfully")
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
	s.UpdateStateCode(commonpb.StateCode_Healthy)
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
	channelManagerOpts := []ChannelmanagerOpt{withCheckerV2()}
	if streamingutil.IsStreamingServiceEnabled() {
		channelManagerOpts = append(channelManagerOpts, withEmptyPolicyFactory())
	}
	s.channelManager, err = NewChannelManager(s.watchClient, s.handler, s.sessionManager, s.idAllocator, channelManagerOpts...)
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
	log := log.Ctx(s.ctx)
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

func (s *Server) initKV() error {
	if s.kv != nil {
		return nil
	}
	s.watchClient = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
		etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	log.Info("data coordinator connecting to metadata store", zap.String("metaType", metaType))
	if metaType == util.MetaStoreTypeTiKV {
		s.metaRootPath = Params.TiKVCfg.MetaRootPath.GetValue()
		s.kv = tikv.NewTiKV(s.tikvCli, s.metaRootPath,
			tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else if metaType == util.MetaStoreTypeEtcd {
		s.metaRootPath = Params.EtcdCfg.MetaRootPath.GetValue()
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, s.metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else {
		return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", metaType))
	}
	log.Info("data coordinator successfully connected to metadata store", zap.String("metaType", metaType))
	return nil
}

func (s *Server) initMeta(chunkManager storage.ChunkManager) error {
	if s.meta != nil {
		return nil
	}
	reloadEtcdFn := func() error {
		var err error
		catalog := datacoord.NewCatalog(s.kv, chunkManager.RootPath(), s.metaRootPath)
		s.meta, err = newMeta(s.ctx, catalog, chunkManager, s.broker)
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
		s.taskScheduler = newTaskScheduler(s.ctx, s.meta, s.indexNodeManager, manager, s.indexEngineVersionManager, s.handler, s.allocator, s.compactionHandler)
		s.compactionHandler.setTaskScheduler(s.taskScheduler)
	}
}

func (s *Server) initJobManager() {
	if s.jobManager == nil {
		s.jobManager = newJobManager(s.ctx, s.meta, s.taskScheduler, s.allocator)
	}
}

func (s *Server) initIndexNodeManager() {
	if s.indexNodeManager == nil {
		s.indexNodeManager = session.NewNodeManager(s.ctx, s.indexNodeCreator)
	}
}

func (s *Server) initCompaction() {
	cph := newCompactionPlanHandler(s.cluster, s.sessionManager, s.meta, s.allocator, s.handler)
	cph.loadMeta()
	s.compactionHandler = cph
	s.compactionTriggerManager = NewCompactionTriggerManager(s.allocator, s.handler, s.compactionHandler, s.meta, s.importMeta)
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

	if !(streamingutil.IsStreamingServiceEnabled() || paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool()) {
		s.syncSegmentsScheduler.Start()
	}
}

func (s *Server) startTaskScheduler() {
	s.taskScheduler.Start()
	s.jobManager.Start()

	s.startIndexService(s.serverLoopCtx)
}

func (s *Server) updateSegmentStatistics(ctx context.Context, stats []*commonpb.SegmentStats) {
	log := log.Ctx(ctx)
	for _, stat := range stats {
		segment := s.meta.GetSegment(ctx, stat.GetSegmentID())
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
				zap.Int64("old value", s.meta.GetSegment(ctx, stat.GetSegmentID()).GetNumOfRows()),
				zap.Int64("new value", stat.GetNumRows()),
			)
			s.meta.SetCurrentRows(stat.GetSegmentID(), stat.GetNumRows())
		}
	}
}

func (s *Server) getFlushableSegmentsInfo(ctx context.Context, flushableIDs []int64) []*SegmentInfo {
	log := log.Ctx(ctx)
	res := make([]*SegmentInfo, 0, len(flushableIDs))
	for _, id := range flushableIDs {
		sinfo := s.meta.GetHealthySegment(ctx, id)
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
	log.Ctx(s.ctx).Error("watch service channel closed", zap.Int64("serverID", paramtable.GetNodeID()))
	go s.Stop()
	if s.session.IsTriggerKill() {
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			p.Signal(syscall.SIGINT)
		}
	}
}

// watchService watches services.
func (s *Server) watchService(ctx context.Context) {
	log := log.Ctx(ctx)
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
				log.Ctx(s.ctx).Info("flush loop shutdown")
				return
			case segmentID := <-s.flushCh:
				// Ignore return error
				log.Ctx(ctx).Info("flush successfully", zap.Any("segmentID", segmentID))
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
	segment := s.meta.GetHealthySegment(ctx, segmentID)
	if segment == nil {
		return merr.WrapErrSegmentNotFound(segmentID, "segment not found, might be a faked segment, ignore post flush")
	}
	// set segment to SegmentState_Flushed
	var operators []UpdateOperator
	operators = append(operators, SetSegmentIsInvisible(segmentID, true))
	operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed))
	err := s.meta.UpdateSegmentsInfo(ctx, operators...)
	if err != nil {
		log.Warn("flush segment complete failed", zap.Error(err))
		return err
	}
	select {
	case getStatsTaskChSingleton() <- segmentID:
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
	log := log.Ctx(s.ctx)
	if !s.stateCode.CompareAndSwap(commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal) {
		return nil
	}
	log.Info("datacoord server shutdown")
	s.garbageCollector.close()
	log.Info("datacoord garbage collector stopped")

	s.stopServerLoop()

	s.importScheduler.Close()
	s.importChecker.Close()
	s.syncSegmentsScheduler.Stop()

	s.stopCompaction()
	log.Info("datacoord compaction stopped")

	s.jobManager.Stop()
	log.Info("datacoord statsJobManager stopped")

	s.taskScheduler.Stop()
	log.Info("datacoord index builder stopped")

	s.cluster.Close()
	log.Info("datacoord cluster stopped")

	if s.session != nil {
		s.session.Stop()
	}

	if s.icSession != nil {
		s.icSession.Stop()
	}

	s.stopServerLoop()
	log.Info("datacoord serverloop stopped")
	log.Warn("datacoord stop successful")
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	log.Ctx(s.ctx).Debug("clean meta", zap.Any("kv", s.kv))
	err := s.kv.RemoveWithPrefix(s.ctx, "")
	err2 := s.watchClient.RemoveWithPrefix(s.ctx, "")
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

func (s *Server) registerMetricsRequest() {
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSystemInfoMetrics(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.DistKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getDistJSON(ctx, req), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ImportTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.importMeta.TaskStatsJSON(ctx), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.CompactionTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.meta.compactionTaskMeta.TaskStatsJSON(), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.BuildIndexTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.meta.indexMeta.TaskStatsJSON(), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SyncTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSyncTaskJSON(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SegmentKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSegmentsJSON(ctx, req, jsonReq)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ChannelKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getChannelsJSON(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.IndexKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
			return s.meta.indexMeta.GetIndexJSON(collectionID), nil
		})
	log.Ctx(s.ctx).Info("register metrics actions finished")
}

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
				log.Ctx(ctx).Info("update balance config loop exit!")
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
	log := log.Ctx(s.ctx)
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
