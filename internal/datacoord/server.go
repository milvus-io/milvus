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
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
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

type mixCoordCreatorFunc func(ctx context.Context) (types.MixCoord, error)

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
	sessionManager   session.DataNodeManager // TODO: sheep, remove sessionManager and cluster
	nodeManager      session.NodeManager
	cluster2         session.Cluster
	channelManager   ChannelManager
	mixCoord         types.MixCoord
	garbageCollector *garbageCollector
	gcOpt            GcOption
	handler          Handler
	importMeta       ImportMeta
	importInspector  ImportInspector
	importChecker    ImportChecker

	compactionTrigger        trigger
	compactionInspector      CompactionInspector
	compactionTriggerManager TriggerManager

	syncSegmentsScheduler *SyncSegmentsScheduler
	metricsCacheManager   *metricsinfo.MetricsCacheManager

	flushCh         chan UniqueID
	notifyIndexChan chan UniqueID
	factory         dependency.Factory

	session   sessionutil.SessionInterface
	icSession sessionutil.SessionInterface
	dnEventCh <-chan *sessionutil.SessionEvent
	// qcEventCh <-chan *sessionutil.SessionEvent
	qnEventCh <-chan *sessionutil.SessionEvent

	enableActiveStandBy bool
	activateFunc        func() error

	dataNodeCreator session.DataNodeCreatorFunc
	mixCoordCreator mixCoordCreatorFunc
	// indexCoord             types.IndexCoord

	// segReferManager  *SegmentReferenceManager
	indexEngineVersionManager IndexEngineVersionManager

	statsInspector   *statsInspector
	indexInspector   *indexInspector
	analyzeInspector *analyzeInspector
	globalScheduler  task.GlobalScheduler

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

func WithMixCoordCreator(creator mixCoordCreatorFunc) Option {
	return func(svr *Server) {
		svr.mixCoordCreator = creator
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
		ctx:                 ctx,
		quitCh:              make(chan struct{}),
		factory:             factory,
		flushCh:             make(chan UniqueID, 1024),
		notifyIndexChan:     make(chan UniqueID, 1024),
		dataNodeCreator:     defaultDataNodeCreatorFunc,
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		metricsRequest:      metricsinfo.NewMetricsRequest(),
	}

	for _, opt := range opts {
		opt(s)
	}
	expr.Register("datacoord", s)
	return s
}

func defaultDataNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
	return datanodeclient.NewClient(ctx, addr, nodeID, Params.DataCoordCfg.WithCredential.GetAsBool())
}

// QuitSignal returns signal when server quits
func (s *Server) QuitSignal() <-chan struct{} {
	return s.quitCh
}

// Register registers data service at etcd
func (s *Server) Register() error {
	return nil
}

// Init change server state to Initializing
func (s *Server) Init() error {
	s.registerMetricsRequest()
	s.factory.Init(Params)
	if err := s.initSession(); err != nil {
		return err
	}
	if err := s.initKV(); err != nil {
		return err
	}

	return s.initDataCoord()
}

func (s *Server) initDataCoord() error {
	log := log.Ctx(s.ctx)

	log.Info("DataCoord try to wait for MixCoord ready")
	if err := s.initMixCoord(); err != nil {
		return err
	}

	s.UpdateStateCode(commonpb.StateCode_Initializing)

	s.broker = broker.NewCoordinatorBroker(s.mixCoord)
	s.allocator = allocator.NewRootCoordAllocator(s.mixCoord)

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

	if err = s.initServiceDiscovery(); err != nil {
		return err
	}
	log.Info("init service discovery done")

	s.globalScheduler = task.NewGlobalTaskScheduler(s.ctx, s.cluster2)

	s.importMeta, err = NewImportMeta(s.ctx, s.meta.catalog, s.allocator, s.meta)
	if err != nil {
		return err
	}
	s.initCompaction()
	log.Info("init compaction done")

	s.initAnalyzeInspector()
	log.Info("init analyze inspector done")

	s.initIndexInspector(storageCli)
	log.Info("init task scheduler done")

	s.initStatsInspector()
	log.Info("init statsJobManager done")

	if err = s.initSegmentManager(); err != nil {
		return err
	}
	log.Info("init segment manager done")

	s.initGarbageCollection(storageCli)

	s.importInspector = NewImportInspector(s.meta, s.importMeta, s.globalScheduler)

	s.importChecker = NewImportChecker(s.meta, s.broker, s.allocator, s.importMeta, s.statsInspector, s.compactionTriggerManager)

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
	s.startDataCoord()
	log.Info("DataCoord startup successfully")

	return nil
}

func (s *Server) startDataCoord() {
	s.startTaskScheduler()
	s.startServerLoop()

	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.MixCoordRole, s.session.GetServerID())
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
	s.nodeManager = session.NewNodeManager(s.dataNodeCreator)
	s.cluster2 = session.NewCluster(s.nodeManager)
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

func (s *Server) SetMixCoord(mixCoord types.MixCoord) {
	s.mixCoord = mixCoord
}

func (s *Server) SetDataNodeCreator(f func(context.Context, string, int64) (types.DataNodeClient, error)) {
	s.dataNodeCreator = f
}

func (s *Server) SetSession(session sessionutil.SessionInterface) error {
	s.session = session
	s.icSession = session
	if s.session == nil {
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	return nil
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
	if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
		log.Info("initServiceDiscovery adding datanode with bind mode",
			zap.Int64("nodeID", Params.DataCoordCfg.IndexNodeID.GetAsInt64()),
			zap.String("address", Params.DataCoordCfg.IndexNodeAddress.GetValue()))
		if err := s.nodeManager.AddNode(Params.DataCoordCfg.IndexNodeID.GetAsInt64(),
			Params.DataCoordCfg.IndexNodeAddress.GetValue()); err != nil {
			log.Warn("DataCoord failed to add datanode", zap.Error(err))
			return err
		}
	} else {
		for _, ss := range sessions {
			info := &session.NodeInfo{
				NodeID:  ss.ServerID,
				Address: ss.Address,
			}

			if ss.Version.LTE(legacyVersion) {
				info.IsLegacy = true
			}

			datanodes = append(datanodes, info)
			if err := s.nodeManager.AddNode(info.NodeID, info.Address); err != nil {
				log.Warn("DataCoord failed to add datanode", zap.Error(err))
				return err
			}
		}

		log.Info("DataCoord Cluster Manager start up")
		if err := s.cluster.Startup(s.ctx, datanodes); err != nil {
			log.Warn("DataCoord Cluster Manager failed to start up", zap.Error(err))
			return err
		}
		log.Info("DataCoord Cluster Manager start up successfully")

		// TODO implement rewatch logic
		s.dnEventCh = s.session.WatchServicesWithVersionRange(typeutil.DataNodeRole, r, rev+1, nil)
	}

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

func (s *Server) initSession() error {
	if s.icSession == nil {
		s.icSession = sessionutil.NewSession(s.ctx)
		s.icSession.Init(typeutil.IndexCoordRole, s.address, true, true)
		s.icSession.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	if s.session == nil {
		s.session = sessionutil.NewSession(s.ctx)

		s.session.Init(typeutil.DataCoordRole, s.address, true, true)
		s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
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

func (s *Server) initAnalyzeInspector() {
	if s.analyzeInspector == nil {
		s.analyzeInspector = newAnalyzeInspector(s.ctx, s.meta, s.globalScheduler)
	}
}

func (s *Server) initIndexInspector(storageCli storage.ChunkManager) {
	if s.indexInspector == nil {
		s.indexInspector = newIndexInspector(s.ctx, s.notifyIndexChan, s.meta, s.globalScheduler, s.allocator, s.handler, storageCli, s.indexEngineVersionManager)
	}
}

func (s *Server) initStatsInspector() {
	if s.statsInspector == nil {
		s.statsInspector = newStatsInspector(s.ctx, s.meta, s.globalScheduler, s.allocator, s.handler, s.compactionInspector, s.indexEngineVersionManager)
	}
}

func (s *Server) initCompaction() {
	cph := newCompactionInspector(s.meta, s.allocator, s.handler, s.globalScheduler)
	cph.loadMeta()
	s.compactionInspector = cph
	s.compactionTriggerManager = NewCompactionTriggerManager(s.allocator, s.handler, s.compactionInspector, s.meta, s.importMeta)
	s.compactionTrigger = newCompactionTrigger(s.meta, s.compactionInspector, s.allocator, s.handler, s.indexEngineVersionManager)
}

func (s *Server) stopCompaction() {
	if s.compactionTrigger != nil {
		s.compactionTrigger.stop()
	}
	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Stop()
	}

	if s.compactionInspector != nil {
		s.compactionInspector.stop()
	}
}

func (s *Server) startCompaction() {
	if s.compactionInspector != nil {
		s.compactionInspector.start()
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
	s.globalScheduler.Start()
	go s.importInspector.Start()
	go s.importChecker.Start()
	s.garbageCollector.start()

	if !(streamingutil.IsStreamingServiceEnabled() || paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool()) {
		s.syncSegmentsScheduler.Start()
	}
}

func (s *Server) startCollectMetaMetrics(ctx context.Context) {
	s.serverLoopWg.Add(1)
	go s.collectMetaMetrics(ctx)
}

func (s *Server) collectMetaMetrics(ctx context.Context) {
	defer s.serverLoopWg.Done()

	ticker := time.NewTicker(time.Second * 120)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Ctx(s.ctx).Warn("collectMetaMetrics ctx done")
			return
		case <-ticker.C:
			s.meta.statsTaskMeta.updateMetrics()
			s.meta.indexMeta.updateIndexTasksMetrics()
		}
	}
}

func (s *Server) startTaskScheduler() {
	s.statsInspector.Start()
	s.indexInspector.Start()
	s.analyzeInspector.Start()
	s.startCollectMetaMetrics(s.serverLoopCtx)
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
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				log.Info("receive datanode session event, but adding datanode by bind mode, skip it",
					zap.String("address", event.Session.Address),
					zap.Int64("serverID", event.Session.ServerID),
					zap.String("event type", event.EventType.String()))
				return nil
			}
			return s.nodeManager.AddNode(event.Session.ServerID, event.Session.Address)
		case sessionutil.SessionDelEvent:
			log.Info("received datanode unregister",
				zap.String("address", info.Address),
				zap.Int64("serverID", info.Version))
			if err := s.cluster.UnRegister(node); err != nil {
				log.Warn("failed to deregister node", zap.Int64("id", node.NodeID), zap.String("address", node.Address), zap.Error(err))
				return err
			}
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				log.Info("receive datanode session event, but adding datanode by bind mode, skip it",
					zap.String("address", event.Session.Address),
					zap.Int64("serverID", event.Session.ServerID),
					zap.String("event type", event.EventType.String()))
				return nil
			}
			s.nodeManager.RemoveNode(event.Session.ServerID)
		default:
			log.Warn("receive unknown service event type",
				zap.Any("type", event.EventType))
		}
	case typeutil.QueryNodeRole:
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			log.Info("received querynode register",
				zap.String("address", event.Session.Address),
				zap.Int64("serverID", event.Session.ServerID),
				zap.Bool("indexNonEncoding", event.Session.IndexNonEncoding))
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
	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		operators = append(operators, SetSegmentIsInvisible(segmentID, true))
	}
	operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed))
	err := s.meta.UpdateSegmentsInfo(ctx, operators...)
	if err != nil {
		log.Warn("flush segment complete failed", zap.Error(err))
		return err
	}

	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		select {
		case getStatsTaskChSingleton() <- segmentID:
		default:
		}
	} else {
		select {
		case getBuildIndexChSingleton() <- segmentID:
		default:
		}
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

func (s *Server) initMixCoord() error {
	var err error
	if s.mixCoord == nil {
		if s.mixCoord, err = s.mixCoordCreator(s.ctx); err != nil {
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
	log.Info("datacoord stopServerLoop stopped")

	s.globalScheduler.Stop()
	s.importInspector.Close()
	s.importChecker.Close()
	s.syncSegmentsScheduler.Stop()

	s.stopCompaction()
	log.Info("datacoord compaction stopped")

	s.statsInspector.Stop()
	log.Info("datacoord stats inspector stopped")

	s.indexInspector.Stop()
	log.Info("datacoord index inspector stopped")

	s.analyzeInspector.Stop()
	log.Info("datacoord analyze inspector stopped")

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
