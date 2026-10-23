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
	"time"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	globalIDAllocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/analyze"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/externalrefresh"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
	"github.com/milvus-io/milvus/internal/datacoord/segment"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/datacoord/stats"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/dataview"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	etcdCli                             *clientv3.Client
	tikvCli                             *txnkv.Client
	address                             string
	watchClient                         kv.WatchKV
	kv                                  kv.MetaKv
	metaRootPath                        string
	meta                                *meta.Meta
	dataViewManager                     DataViewManager
	metaStore                           metacache.MetaStore
	catalog                             metastore.DataCoordCatalog
	dataViewCollectionRecoveryValidator dataview.CollectionRecoveryValidator
	segmentManager                      segment.Manager
	allocator                           allocator.Allocator
	// self host id allocator, to avoid get unique id from rootcoord
	idAllocator      *globalIDAllocator.GlobalIDAllocator
	nodeManager      session.NodeManager
	cluster2         session.Cluster
	mixCoord         types.MixCoord
	garbageCollector *gc.GarbageCollector
	gcOpt            gc.GcOption
	handler          meta.Handler
	importMeta       bulkimport.ImportMeta
	importInspector  bulkimport.ImportInspector
	importChecker    bulkimport.ImportChecker
	importCheckerV3  bulkimport.ImportChecker
	importJobLock    *lock.KeyLock[int64]

	domainMetas

	copySegmentMeta      copysegment.CopySegmentMeta
	copySegmentInspector copysegment.CopySegmentInspector
	copySegmentChecker   copysegment.CopySegmentChecker

	snapshotManager       snapshot.SnapshotManager
	snapshotExportManager *snapshot.SnapshotExportManager

	compactionTrigger        compaction.Trigger
	compactionInspector      compaction.CompactionInspector
	compactionTriggerManager compaction.TriggerManager

	metricsCacheManager *metricsinfo.MetricsCacheManager

	flushCh         chan UniqueID
	notifyIndexChan chan UniqueID
	factory         dependency.Factory

	session          sessionutil.SessionInterface
	icSession        sessionutil.SessionInterface
	dnSessionWatcher sessionutil.SessionWatcher
	qnSessionWatcher sessionutil.SessionWatcher

	enableActiveStandBy bool
	activateFunc        func() error

	dataNodeCreator session.DataNodeCreatorFunc
	mixCoordCreator mixCoordCreatorFunc
	// indexCoord             types.IndexCoord

	// segReferManager  *SegmentReferenceManager
	indexEngineVersionManager index.IndexEngineVersionManager

	statsInspector                   *stats.Inspector
	indexInspector                   *index.IndexInspector
	analyzeInspector                 *analyze.AnalyzeInspector
	talonPrewarmInspector            *TalonPrewarmInspector
	segmentChangeInspector           *segmentChangeInspector
	externalCollectionRefreshManager externalrefresh.ExternalCollectionRefreshManager
	globalScheduler                  task.GlobalScheduler

	// manage ways that data coord access other coord
	broker broker.Broker

	ddl *ddl.Callbacks

	metricsRequest *metricsinfo.MetricsRequest

	// file resource
	fileResourceObserver FileResourceObserver
}

type FileResourceObserver interface {
	InitDataCoord(manager session.NodeManager)
	IsEmpty() bool
	Notify()
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

// WithDataNodeCreator returns an `Option` setting DataNode create function
func WithDataNodeCreator(creator session.DataNodeCreatorFunc) Option {
	return func(svr *Server) {
		svr.dataNodeCreator = creator
	}
}

// WithSegmentManager returns an Option to set SegmentManager
func WithSegmentManager(manager segment.Manager) Option {
	return func(svr *Server) {
		svr.segmentManager = manager
	}
}

// WithMetaStore returns an Option to set the shared MetaStore for datacoord
// to use as the primary segment and collection storage.
func WithMetaStore(store metacache.MetaStore) Option {
	return func(svr *Server) {
		svr.metaStore = store
	}
}

// WithCatalog returns an Option to set the DataCoordCatalog for sub-meta
// initialization (indexMeta, analyzeMeta, importMeta, etc.).
func WithCatalog(catalog metastore.DataCoordCatalog) Option {
	return func(svr *Server) {
		svr.catalog = catalog
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
		importJobLock:       lock.NewKeyLock[int64](),
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		metricsRequest:      metricsinfo.NewMetricsRequest(),
	}

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func defaultDataNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
	return datanodeclient.NewClient(ctx, addr, nodeID, Params.DataCoordCfg.WithCredential.GetAsBool())
}

func (s *Server) SetFileResourceObserver(observer FileResourceObserver) {
	s.fileResourceObserver = observer
}

// QuitSignal returns signal when server quits
func (s *Server) QuitSignal() <-chan struct{} {
	return s.quitCh
}

// Register registers data service at etcd
func (s *Server) Register() error {
	return nil
}

func (s *Server) ServerExist(serverID int64) bool {
	sessions, _, err := s.session.GetSessions(s.ctx, typeutil.DataNodeRole)
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

func (s *Server) initDataCoord() (initErr error) {
	defer func() {
		if initErr != nil && s.meta != nil {
			s.meta.CloseManifestCommitExecutor()
		}
	}()
	mlog.Info(s.ctx, "DataCoord try to wait for MixCoord ready")
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
	mlog.Info(s.ctx, "init chunk manager factory done")

	if err = s.initMeta(storageCli); err != nil {
		return err
	}

	// init id allocator after init meta
	s.idAllocator = globalIDAllocator.NewGlobalIDAllocator("idTimestamp", s.kv)
	err = s.idAllocator.Initialize()
	if err != nil {
		mlog.Error(s.ctx, "data coordinator id allocator initialize failed", mlog.Err(err))
		return err
	}

	s.handler = newServerHandler(s)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err = s.initCluster(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "init datanode cluster done")

	if err = s.initServiceDiscovery(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "init service discovery done")

	s.globalScheduler = task.NewGlobalTaskScheduler(s.ctx, s.cluster2)

	s.importMeta, err = bulkimport.NewImportMeta(s.ctx, s.catalog, s.allocator, s.meta)
	if err != nil {
		return err
	}
	s.initCompaction()
	mlog.Info(s.ctx, "init compaction done")

	s.initAnalyzeInspector()
	mlog.Info(s.ctx, "init analyze inspector done")

	s.initIndexInspector(storageCli)
	mlog.Info(s.ctx, "init task scheduler done")

	s.initStatsInspector()
	mlog.Info(s.ctx, "init statsJobManager done")

	s.initSegmentChangeInspector()
	mlog.Info(s.ctx, "init segment change inspector done")

	s.initExternalCollectionInspector(storageCli)
	mlog.Info(s.ctx, "init external collection inspector done")

	if err = s.initSegmentManager(); err != nil {
		return err
	}
	mlog.Info(s.ctx, "init segment manager done")

	s.initGarbageCollection(storageCli)

	s.importChecker = bulkimport.NewImportChecker(s.ctx, s.meta, s.indexMeta, s.broker, s.allocator, s.importMeta, s.compactionInspector, s.handler, bulkimport.ImportCheckerHooks{
		CommitImport:        s.broadcastCommitImportMessage,
		RollbackImport:      s.broadcastRollbackImportMessage,
		AssignImportIDRange: s.broadcastUpdateImportMessage,
		GetReplicationRole:  ddl.GetReplicationRole,
	})
	s.importCheckerV3 = bulkimport.NewImportCheckerV3(s.ctx, s.meta, s.indexMeta, s.broker, s.allocator, s.importMeta, s.cluster2, bulkimport.ImportCheckerHooks{
		CommitImport:        s.broadcastCommitImportMessage,
		RollbackImport:      s.broadcastRollbackImportMessage,
		AssignImportIDRange: s.broadcastUpdateImportMessage,
		GetReplicationRole:  ddl.GetReplicationRole,
	})

	// init file resource observer
	if s.fileResourceObserver != nil {
		s.fileResourceObserver.InitDataCoord(s.nodeManager)
	}

	// Initialize copy segment meta and components
	var snapshotSource copysegment.SnapshotSource
	if s.snapshotMeta != nil {
		snapshotSource = s.snapshotMeta
	}
	s.copySegmentMeta, err = copysegment.NewCopySegmentMeta(s.ctx, s.meta.Catalog, s.meta, s.indexMeta, snapshotSource, s.allocator)
	if err != nil {
		return err
	}

	s.importInspector = bulkimport.NewImportInspector(s.ctx, s.meta, s.importMeta, s.globalScheduler, s.copySegmentMeta)
	s.copySegmentInspector = copysegment.NewCopySegmentInspector(
		s.ctx,
		s.meta,
		s.copySegmentMeta,
		s.globalScheduler,
	)

	s.copySegmentChecker = copysegment.NewCopySegmentChecker(
		s.ctx,
		s.meta,
		s.broker,
		s.allocator,
		s.copySegmentMeta,
	)
	mlog.Info(s.ctx, "init copy segment inspector and checker done")

	// Initialize snapshot manager
	snapshotManager := snapshot.NewSnapshotManager(
		s.meta,
		s.snapshotMeta,
		s.copySegmentMeta,
		s.allocator,
		s.handler,
		s.broker,
		s.getChannelsByCollectionID,
		s.indexEngineVersionManager,
	)
	s.snapshotManager = snapshotManager
	snapshotExportMeta, err := snapshot.NewSnapshotExportMeta(s.ctx, s.meta.Catalog)
	if err != nil {
		return err
	}
	s.snapshotExportManager = snapshot.NewSnapshotExportManager(s.ctx, snapshotExportMeta, snapshotManager)
	snapshotManager.ExportManager = s.snapshotExportManager
	mlog.Info(s.ctx, "init snapshot manager done")

	// s.ddl must be built after every ddl.Deps source field is set and before RegisterDDLCallbacks.
	s.ddl = newDDLCallbacks(s)

	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	if err := s.initTalonPrewarm(); err != nil {
		return err
	}

	mlog.Info(s.ctx, "init datacoord done", mlog.FieldNodeID(paramtable.GetNodeID()), mlog.String("Address", s.address))

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
	s.startDataCoord()
	mlog.Info(s.ctx, "DataCoord startup successfully")

	return nil
}

func (s *Server) startDataCoord() {
	s.startTaskScheduler()
	// Reconcile orphan import segments and re-enqueue in-flight import tasks
	// before the checkers start and before the state turns Healthy: the scan
	// must not race producers that persist a segment before its owner record
	// (snapshot restore pre-registration, V2 import segment allocation).
	s.importInspector.Reload()
	s.startServerLoop()
	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	// The first prewarm round reads the recovered metadata through a healthy
	// DataCoord; starting it earlier could skip that round as not ready.
	if s.talonPrewarmInspector != nil {
		s.talonPrewarmInspector.Start()
	}
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
	if s.nodeManager == nil {
		s.nodeManager = session.NewNodeManager(s.dataNodeCreator)
	}
	if s.cluster2 == nil {
		s.cluster2 = session.NewCluster(s.nodeManager)
	}
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

func (s *Server) SetDataViewCollectionRecoveryValidator(validator dataview.CollectionRecoveryValidator) {
	s.dataViewCollectionRecoveryValidator = validator
}

func (s *Server) SetDataNodeCreator(f func(context.Context, string, int64) (types.DataNodeClient, error)) {
	s.dataNodeCreator = f
}

func (s *Server) SetSession(session sessionutil.SessionInterface) error {
	s.session = session
	s.icSession = session
	if s.session == nil {
		return merr.WrapErrServiceNotReadyMsg("session is nil, the etcd client connection may have failed")
	}
	return nil
}

func (s *Server) newChunkManagerFactory() (storage.ChunkManager, error) {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(s.ctx)
	if err != nil {
		mlog.Error(s.ctx, "chunk manager init failed", mlog.Err(err))
		return nil, err
	}
	return cli, err
}

func (s *Server) initGarbageCollection(cli storage.ChunkManager) {
	s.garbageCollector = gc.NewGarbageCollector(s.meta, s.handler, gc.GcOption{
		Cli:              cli,
		Broker:           s.broker,
		Enabled:          Params.DataCoordCfg.EnableGarbageCollection.GetAsBool(),
		CheckInterval:    Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second),
		ScanInterval:     Params.DataCoordCfg.GCScanIntervalInHour.GetAsDuration(time.Hour),
		MissingTolerance: Params.DataCoordCfg.GCMissingTolerance.GetAsDuration(time.Second),
		DropTolerance:    Params.DataCoordCfg.GCDropTolerance.GetAsDuration(time.Second),
		DataViewGC:       s.dataViewManager,
		IndexMeta:        s.indexMeta,
		AnalyzeMeta:      s.analyzeMeta,
		SnapshotMeta:     s.snapshotMeta,
		ImportJobAlive: func(ctx context.Context, jobID int64) bool {
			return s.importMeta.GetJob(ctx, jobID) != nil
		},
	})
}

func (s *Server) initServiceDiscovery() error {
	r := semver.MustParseRange(">=2.2.3")
	if s.indexEngineVersionManager == nil {
		s.indexEngineVersionManager = index.NewIndexEngineVersionManager()
	}
	sessions, rev, err := s.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		mlog.Warn(s.ctx, "DataCoord failed to init service discovery", mlog.Err(err))
		return err
	}
	mlog.Info(s.ctx, "DataCoord success to get DataNode sessions", mlog.Any("sessions", sessions))

	if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
		mlog.Info(s.ctx, "initServiceDiscovery adding datanode with bind mode",
			mlog.FieldNodeID(Params.DataCoordCfg.IndexNodeID.GetAsInt64()),
			mlog.String("address", Params.DataCoordCfg.IndexNodeAddress.GetValue()))
		if err := s.nodeManager.AddNode(Params.DataCoordCfg.IndexNodeID.GetAsInt64(),
			Params.DataCoordCfg.IndexNodeAddress.GetValue()); err != nil {
			mlog.Warn(s.ctx, "DataCoord failed to add datanode", mlog.Err(err))
			return err
		}
		s.dnSessionWatcher = sessionutil.EmptySessionWatcher()
	} else {
		err := s.rewatchDataNodes(sessions)
		if err != nil {
			mlog.Warn(s.ctx, "DataCoord failed to rewatch datanode", mlog.Err(err))
			return err
		}
		mlog.Info(s.ctx, "DataCoord Cluster Manager start up successfully")

		s.dnSessionWatcher = s.session.WatchServicesWithVersionRange(typeutil.DataNodeRole, r, rev+1, s.rewatchDataNodes)
	}

	qnSessions, qnRevision, err := s.session.GetSessions(s.ctx, typeutil.QueryNodeRole)
	if err != nil {
		mlog.Warn(s.ctx, "DataCoord get QueryNode sessions failed", mlog.Err(err))
		return err
	}
	s.rewatchQueryNodes(qnSessions)
	s.qnSessionWatcher = s.session.WatchServicesWithVersionRange(typeutil.QueryNodeRole, r, qnRevision+1, s.rewatchQueryNodes)

	return nil
}

// rewatchQueryNodes is used to rewatch query nodes when datacoord is started or reconnected to etcd
// Note: may apply same node multiple times, so rewatchQueryNodes must be idempotent
func (s *Server) rewatchQueryNodes(sessions map[string]*sessionutil.Session) error {
	s.indexEngineVersionManager.Startup(sessions)
	return nil
}

// rewatchDataNodes is used to rewatch data nodes when datacoord is started or reconnected to etcd
// Note: may apply same node multiple times, so rewatchDataNodes must be idempotent
func (s *Server) rewatchDataNodes(sessions map[string]*sessionutil.Session) error {
	if s.indexEngineVersionManager == nil {
		s.indexEngineVersionManager = index.NewIndexEngineVersionManager()
	}
	legacyVersion, err := semver.Parse(paramtable.Get().DataCoordCfg.LegacyVersionWithoutRPCWatch.GetValue())
	if err != nil {
		mlog.Warn(s.ctx, "DataCoord failed to init service discovery", mlog.Err(err))
		return err
	}

	datanodes := make([]*session.NodeInfo, 0, len(sessions))
	for _, ss := range sessions {
		info := &session.NodeInfo{
			NodeID:  ss.ServerID,
			Address: ss.Address,
		}

		if ss.Version.LTE(legacyVersion) {
			info.IsLegacy = true
		}

		datanodes = append(datanodes, info)
	}

	if err := s.nodeManager.Startup(s.ctx, datanodes); err != nil {
		mlog.Warn(s.ctx, "DataCoord failed to add datanode", mlog.Err(err))
		return err
	}
	return nil
}

func (s *Server) initSegmentManager() error {
	if s.segmentManager == nil {
		manager, err := segment.NewSegmentManager(s.meta, s.allocator)
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
		s.icSession.Init(typeutil.IndexCoordRole, s.address, true)
		s.icSession.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	if s.session == nil {
		s.session = sessionutil.NewSession(s.ctx)

		s.session.Init(typeutil.DataCoordRole, s.address, true)
		s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	return nil
}

func (s *Server) initKV() error {
	if s.kv != nil {
		return nil
	}
	s.watchClient = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
		etcdkv.WithRequestTimeout(paramtable.Get().EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	mlog.Info(s.ctx, "data coordinator connecting to metadata store", mlog.String("metaType", metaType))
	switch metaType {
	case util.MetaStoreTypeTiKV:
		s.metaRootPath = Params.TiKVCfg.MetaRootPath.GetValue()
		s.kv = tikv.NewTiKV(s.tikvCli, s.metaRootPath,
			tikv.WithRequestTimeout(paramtable.Get().TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	case util.MetaStoreTypeEtcd:
		s.metaRootPath = Params.EtcdCfg.MetaRootPath.GetValue()
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, s.metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	default:
		return retry.Unrecoverable(merr.WrapErrServiceInternalMsg("unsupported meta store: %s", metaType))
	}
	mlog.Info(s.ctx, "data coordinator successfully connected to metadata store", mlog.String("metaType", metaType))
	return nil
}

// domainMetas holds the per-domain metas that Server owns.
type domainMetas struct {
	indexMeta                     *index.IndexMeta
	analyzeMeta                   *analyze.AnalyzeMeta
	partitionStatsMeta            *partitionstats.PartitionStatsMeta
	compactionTaskMeta            *ctask.CompactionTaskMeta
	compactionTargetMeta          *targets.CompactionTargetMeta
	statsTaskMeta                 *stats.StatsTaskMeta
	externalCollectionRefreshMeta *externalrefresh.ExternalCollectionRefreshMeta
	snapshotMeta                  *snapshot.SnapshotMeta
}

// loadDomainMetas returns a DomainMetaLoader that builds every domain meta in parallel into out and returns the index catalog and the compaction protection.
func loadDomainMetas(catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager, out *domainMetas) meta.DomainMetaLoader {
	return func(ctx context.Context, collectionIDs []int64) (meta.SegmentIndexCatalog, meta.CompactionProtection, error) {
		g, _ := errgroup.WithContext(ctx)

		g.Go(func() error {
			var err error
			out.indexMeta, err = index.NewIndexMeta(ctx, catalog, collectionIDs)
			return err
		})

		g.Go(func() error {
			var err error
			out.analyzeMeta, err = analyze.NewAnalyzeMeta(ctx, catalog)
			return err
		})

		g.Go(func() error {
			var err error
			out.partitionStatsMeta, err = partitionstats.NewPartitionStatsMeta(ctx, catalog)
			return err
		})

		g.Go(func() error {
			var err error
			out.compactionTaskMeta, err = ctask.NewCompactionTaskMeta(ctx, catalog)
			return err
		})

		if Params.DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() {
			g.Go(func() error {
				var err error
				out.compactionTargetMeta, err = targets.NewCompactionTargetMeta(ctx, catalog)
				return err
			})
		}

		g.Go(func() error {
			var err error
			out.statsTaskMeta, err = stats.NewStatsTaskMeta(ctx, catalog)
			return err
		})

		g.Go(func() error {
			var err error
			out.externalCollectionRefreshMeta, err = externalrefresh.NewExternalCollectionRefreshMeta(
				ctx,
				catalog,
				externalrefresh.WithExternalCollectionRefreshResultStore(
					externalrefresh.NewExternalCollectionRefreshResultStore(chunkManager),
				),
			)
			return err
		})

		g.Go(func() error {
			var err error
			out.snapshotMeta, err = snapshot.NewSnapshotMeta(ctx, catalog, chunkManager)
			return err
		})

		if err := g.Wait(); err != nil {
			return nil, nil, err
		}
		var protection meta.CompactionProtection
		if out.snapshotMeta != nil {
			protection = out.snapshotMeta
		}
		return out.indexMeta, protection, nil
	}
}

func (s *Server) initMeta(chunkManager storage.ChunkManager) error {
	// MixCoord injects the shared catalog and MetaStore. A standalone
	// DataCoord (e.g. in tests) builds its own over the local KV.
	if s.catalog == nil {
		s.catalog = datacoord.NewCatalog(s.kv, chunkManager.RootPath(), s.metaRootPath)
	}
	if s.metaStore == nil {
		s.metaStore = metacache.NewMetaStore(s.catalog)
	}
	if s.meta != nil {
		return nil
	}
	// DataView persistence lives on the concrete KV catalog, not on the
	// DataCoordCatalog interface.
	catalog, ok := s.catalog.(dataview.Catalog)
	if !ok {
		return merr.WrapErrServiceInternalMsg("datacoord catalog does not support DataView persistence")
	}
	var (
		recoveredMeta        *meta.Meta
		recoveredDomainMetas domainMetas
	)
	reloadEtcdFn := func() error {
		var (
			err error
			dm  domainMetas
		)
		recoveredMeta, err = meta.NewMeta(s.ctx, s.catalog, chunkManager, s.metaStore, s.broker, loadDomainMetas(s.catalog, chunkManager, &dm))
		recoveredDomainMetas = dm
		return err
	}
	if err := retry.Do(s.ctx, reloadEtcdFn, retry.Attempts(connMetaMaxRetryTime)); err != nil {
		return err
	}
	defer func() {
		if s.meta != recoveredMeta {
			recoveredMeta.CloseManifestCommitExecutor()
		}
	}()
	// Retry each recovery phase independently: a later metadata error must
	// not repeat newMeta's successful object-storage manifest scan.
	if err := retry.Do(s.ctx, func() error {
		return recoveredMeta.ReloadCollectionsFromRootcoord(s.ctx, s.broker)
	}, retry.Attempts(connMetaMaxRetryTime)); err != nil {
		return err
	}

	collections := recoveredMeta.GetCollections()
	collectionIDs := lo.Map(collections, func(c *meta.CollectionInfo, _ int) int64 { return c.ID })
	collectionVChannels := lo.SliceToMap(collections, func(c *meta.CollectionInfo) (int64, []string) {
		return c.ID, c.VChannelNames
	})
	var manager dataview.Manager
	if err := retry.Do(s.ctx, func() error {
		var err error
		manager, err = dataview.RecoverManager(
			s.ctx,
			catalog,
			s.dataViewCollectionRecoveryValidator,
			recoveredMeta.LoadableProjection,
			collectionIDs,
			collectionVChannels,
		)
		return err
	}, retry.Attempts(connMetaMaxRetryTime)); err != nil {
		return err
	}
	// Publish only fully recovered metadata. A failed later phase must not
	// make a subsequent initMeta call return early with partial state.
	recoveredMeta.DataViewManager = manager
	s.meta = recoveredMeta
	s.domainMetas = recoveredDomainMetas
	s.dataViewManager = manager
	return nil
}

func (s *Server) initAnalyzeInspector() {
	if s.analyzeInspector == nil {
		s.analyzeInspector = analyze.NewAnalyzeInspector(s.ctx, s.meta, s.analyzeMeta, s.globalScheduler)
	}
}

func (s *Server) initIndexInspector(storageCli storage.ChunkManager) {
	if s.indexInspector == nil {
		s.indexInspector = index.NewIndexInspector(s.ctx, s.notifyIndexChan, s.meta, s.indexMeta, s.globalScheduler, s.allocator, s.handler, storageCli, s.indexEngineVersionManager)
	}
}

func (s *Server) initStatsInspector() {
	if s.statsInspector == nil {
		s.statsInspector = stats.NewStatsInspector(s.ctx, s.meta, s.statsTaskMeta, s.globalScheduler, s.allocator, s.handler, s.compactionInspector, s.indexEngineVersionManager)
	}
}

func (s *Server) initSegmentChangeInspector() {
	if s.segmentChangeInspector == nil {
		s.segmentChangeInspector = newSegmentChangeInspector(s.ctx, s.meta, s.allocator)
	}
}

func (s *Server) initExternalCollectionInspector(storageCli storage.ChunkManager) {
	// Initialize Manager (handles job submission, query, and internal inspector/checker)
	if s.externalCollectionRefreshManager == nil {
		s.externalCollectionRefreshManager = externalrefresh.NewExternalCollectionRefreshManager(
			s.ctx, s.meta, s.indexMeta, s.globalScheduler, s.allocator, s.externalCollectionRefreshMeta, s.cluster2, s.handler.GetCollection, s.updateExternalSchemaViaWAL, storageCli)
	}
}

func (s *Server) initCompaction() {
	partitionStatsCleaner := partitionstats.NewPartitionStatsCleaner(s.meta.ChunkManager, s.catalog, s.analyzeMeta, s.partitionStatsMeta)
	cph := compaction.NewCompactionInspector(s.meta, s.compactionTaskMeta, s.indexMeta, s.analyzeMeta, s.partitionStatsMeta, partitionStatsCleaner, s.allocator, s.handler, s.globalScheduler, s.globalScheduler, s.indexEngineVersionManager)
	cph.LoadMeta()
	s.compactionInspector = cph
	s.compactionTriggerManager = compaction.NewCompactionTriggerManager(s.allocator, s.handler, s.compactionInspector, s.meta, s.indexMeta, s.compactionTaskMeta, s.partitionStatsMeta, s.compactionTargetMeta, s.indexEngineVersionManager)
	s.compactionTriggerManager.InitForceMergeMemoryQuerier(s.nodeManager, s.mixCoord, s.session)
	s.compactionTrigger = compaction.NewCompactionTrigger(s.meta, s.indexMeta, s.compactionInspector, s.allocator, s.handler, s.indexEngineVersionManager)
}

func (s *Server) stopCompaction() {
	if s.compactionTrigger != nil {
		s.compactionTrigger.Stop()
	}
	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Stop()
	}

	if s.compactionInspector != nil {
		s.compactionInspector.Stop()
	}
}

func (s *Server) startCompaction() {
	if s.compactionInspector != nil {
		s.compactionInspector.Start()
	}

	if s.compactionTrigger != nil {
		s.compactionTrigger.Start()
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
	go s.importCheckerV3.Start()

	// Start copy segment inspector and checker
	go s.copySegmentInspector.Start()
	go s.copySegmentChecker.Start()

	// Start external collection refresh manager (includes inspector and checker)
	s.externalCollectionRefreshManager.Start()
	if s.snapshotExportManager != nil {
		s.snapshotExportManager.Start()
	}

	// The DataView recompute worker was already started at RecoverManager
	// construction (bounded by s.ctx); nothing to do here.

	s.garbageCollector.Start()

	s.statsTaskMeta.StartCleanupDeprecatedSortTasks(s.serverLoopCtx, &s.serverLoopWg)
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
			mlog.Warn(s.ctx, "collectMetaMetrics ctx done")
			return
		case <-ticker.C:
			s.statsTaskMeta.UpdateMetrics()
			s.indexMeta.UpdateIndexTasksMetrics()
		}
	}
}

func (s *Server) startTaskScheduler() {
	s.statsInspector.Start()
	s.indexInspector.Start()
	s.analyzeInspector.Start()
	s.segmentChangeInspector.Start()
	// Note: externalCollectionInspector.Start() is called in startServerLoop as a goroutine
	s.startCollectMetaMetrics(s.serverLoopCtx)
}

func (s *Server) getFlushableSegmentsInfo(ctx context.Context, flushableIDs []int64) []*meta.SegmentInfo {
	res := make([]*meta.SegmentInfo, 0, len(flushableIDs))
	for _, id := range flushableIDs {
		sinfo := s.meta.GetHealthySegment(ctx, id)
		if sinfo == nil {
			mlog.Error(ctx, "get segment from meta error", mlog.Int64("id", id))
			continue
		}
		res = append(res, sinfo)
	}
	return res
}

func (s *Server) setLastFlushTime(segments []*meta.SegmentInfo) {
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
	mlog.Error(s.ctx, "watch service channel closed", mlog.Int64("serverID", paramtable.GetNodeID()))
	if s.ctx.Err() == nil {
		// ctx is still active, meaning this is not a normal shutdown but a genuine watch failure.
		// Force exit so the process can be restarted by the orchestrator (e.g. K8s).
		mlog.Error(s.ctx, "force exit due to unexpected watch service failure")
		mlog.Cleanup()
		os.Exit(sessionutil.ExitCodeEtcd)
	}
}

// watchService watches services.
func (s *Server) watchService(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "watch service shutdown")
			return
		case event, ok := <-s.dnSessionWatcher.EventChannel():
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.DataNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						mlog.Warn(ctx, "DataCoord server stop error", mlog.Err(err))
					}
				}()
				return
			}
		case event, ok := <-s.qnSessionWatcher.EventChannel():
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.QueryNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						mlog.Warn(ctx, "DataCoord server stop error", mlog.Err(err))
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

	switch role {
	case typeutil.DataNodeRole:
		info := &datapb.DataNodeInfo{
			Address:  event.Session.Address,
			Version:  event.Session.ServerID,
			Channels: []*datapb.ChannelStatus{},
		}
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			mlog.Info(ctx, "received datanode register",
				mlog.String("address", info.Address),
				mlog.Int64("serverID", info.Version))
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				mlog.Info(ctx, "receive datanode session event, but adding datanode by bind mode, skip it",
					mlog.String("address", event.Session.Address),
					mlog.Int64("serverID", event.Session.ServerID),
					mlog.String("event type", event.EventType.String()))
				return nil
			}
			err := s.nodeManager.AddNode(event.Session.ServerID, event.Session.Address)
			if err != nil {
				return err
			}

			// notify file manager sync file resource to new node
			if s.fileResourceObserver != nil && !s.fileResourceObserver.IsEmpty() {
				s.fileResourceObserver.Notify()
			}
		case sessionutil.SessionDelEvent:
			mlog.Info(ctx, "received datanode unregister",
				mlog.String("address", info.Address),
				mlog.Int64("serverID", info.Version))
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				mlog.Info(ctx, "receive datanode session event, but adding datanode by bind mode, skip it",
					mlog.String("address", event.Session.Address),
					mlog.Int64("serverID", event.Session.ServerID),
					mlog.String("event type", event.EventType.String()))
				return nil
			}
			s.nodeManager.RemoveNode(event.Session.ServerID)
		case sessionutil.SessionUpdateEvent:
			mlog.Info(ctx, "received datanode SessionUpdateEvent",
				mlog.String("address", info.Address),
				mlog.Int64("serverID", info.Version))
		default:
			mlog.Warn(ctx, "receive unknown service event type",
				mlog.Any("type", event.EventType))
		}
	case typeutil.QueryNodeRole:
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			mlog.Info(ctx, "received querynode register",
				mlog.String("address", event.Session.Address),
				mlog.Int64("serverID", event.Session.ServerID),
				mlog.Bool("indexNonEncoding", event.Session.IndexNonEncoding))
			s.indexEngineVersionManager.AddNode(event.Session)
		case sessionutil.SessionDelEvent:
			mlog.Info(ctx, "received querynode unregister",
				mlog.String("address", event.Session.Address),
				mlog.Int64("serverID", event.Session.ServerID))
			s.indexEngineVersionManager.RemoveNode(event.Session)
		case sessionutil.SessionUpdateEvent:
			serverID := event.Session.ServerID
			mlog.Info(ctx, "received querynode SessionUpdateEvent", mlog.Int64("serverID", serverID))
			s.indexEngineVersionManager.Update(event.Session)
		default:
			mlog.Warn(ctx, "receive unknown service event type",
				mlog.Any("type", event.EventType))
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
				mlog.Info(s.ctx, "flush loop shutdown")
				return
			case segmentID := <-s.flushCh:
				// Ignore return error
				mlog.Info(ctx, "flush successfully", mlog.Any("segmentID", segmentID))
				err := s.postFlush(ctx, segmentID)
				if err != nil {
					mlog.Warn(ctx, "failed to do post flush", mlog.FieldSegmentID(segmentID), mlog.Err(err))
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
	seg := s.meta.GetHealthySegment(ctx, segmentID)
	if seg == nil {
		return merr.WrapErrSegmentNotFound(segmentID, "segment not found, might be a faked segment, ignore post flush")
	}

	if meta.EnableSortCompaction() {
		select {
		case meta.GetStatsTaskChSingleton() <- segmentID:
		default:
		}
	} else {
		select {
		case meta.GetBuildIndexChSingleton() <- segmentID:
		default:
		}
	}

	// Insert / delta counts come from Statistics. Stats array is iterated
	// because Statistics doesn't carry a per-segment stat-file count — V3
	// reports 0, matching the manifest-driven layout.
	st := seg.EnsureStats()
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(st.GetInsertBinlogCount()))
	statFileNum := 0
	for _, fieldBinlog := range seg.GetStatslogs() {
		statFileNum += len(fieldBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(st.GetDeltaBinlogCount()))

	mlog.Info(ctx, "flush segment complete", mlog.Int64("id", segmentID))
	return nil
}

// recovery logic, fetch all Segment in `Flushing` state and do Flush notification logic
func (s *Server) handleFlushingSegments(ctx context.Context) {
	segments := s.meta.GetFlushingSegments()
	for _, seg := range segments {
		// The old flushing segment may not be flushed, so we need to flush it again.
		// It should be retry until success
		if err := s.flushFlushingSegment(ctx, seg.ID); err != nil {
			mlog.Warn(ctx, "flush flushing segment failed", mlog.FieldSegmentID(seg.ID), mlog.Err(err))
			return
		}
		mlog.Info(ctx, "flush flushing segment success", mlog.FieldSegmentID(seg.ID))
		select {
		case <-ctx.Done():
			return
		case s.flushCh <- seg.ID:
		}
	}
}

// flushFlushingSegment flushes a segment to `Flushed` state
func (s *Server) flushFlushingSegment(ctx context.Context, segmentID UniqueID) error {
	return retry.Do(ctx, func() error {
		// set segment to SegmentState_Flushed
		var operators []meta.UpdateOperator
		if meta.EnableSortCompaction() {
			operators = append(operators, meta.SetSegmentIsInvisible(segmentID, true))
		}
		operators = append(operators, meta.UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed))
		if err := s.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			mlog.Warn(ctx, "flush segment complete failed", mlog.FieldSegmentID(segmentID), mlog.Err(err))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// underlying etcd may return context canceled, so we need to return a error to retry.
			return merr.WrapErrServiceInternalMsg("flush segment complete failed")
		}
		return nil
	}, retry.AttemptAlways())
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
	// Init can create the client without reaching Start. Release it even when
	// the state guard below skips the rest of component shutdown.
	if s.talonPrewarmInspector != nil {
		s.talonPrewarmInspector.Stop()
	}
	if !s.stateCode.CompareAndSwap(commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal) {
		return nil
	}
	mlog.Info(s.ctx, "datacoord server shutdown")
	s.garbageCollector.Close()
	mlog.Info(s.ctx, "datacoord garbage collector stopped")
	if s.snapshotExportManager != nil {
		s.snapshotExportManager.Close()
		mlog.Info(s.ctx, "datacoord snapshot export manager stopped")
	}

	if s.snapshotMeta != nil {
		s.snapshotMeta.Close()
		mlog.Info(s.ctx, "datacoord snapshot meta closed")
	}

	s.stopServerLoop()
	mlog.Info(s.ctx, "datacoord stopServerLoop stopped")

	s.globalScheduler.Stop()
	s.importInspector.Close()
	s.importChecker.Close()
	s.importCheckerV3.Close()

	// Stop copy segment components
	s.copySegmentInspector.Close()
	s.copySegmentChecker.Close()
	mlog.Info(s.ctx, "datacoord copy segment inspector and checker stopped")

	s.stopCompaction()
	mlog.Info(s.ctx, "datacoord compaction stopped")

	s.statsInspector.Stop()
	mlog.Info(s.ctx, "datacoord stats inspector stopped")

	s.segmentChangeInspector.Stop()
	mlog.Info(s.ctx, "datacoord segment change inspector stopped")

	s.indexInspector.Stop()
	mlog.Info(s.ctx, "datacoord index inspector stopped")

	s.analyzeInspector.Stop()
	mlog.Info(s.ctx, "datacoord analyze inspector stopped")

	if s.dnSessionWatcher != nil {
		s.dnSessionWatcher.Stop()
	}

	if s.qnSessionWatcher != nil {
		s.qnSessionWatcher.Stop()
	}
	s.externalCollectionRefreshManager.Stop()
	mlog.Info(s.ctx, "datacoord external collection refresh manager stopped")

	if s.session != nil {
		s.session.Stop()
	}

	if s.icSession != nil {
		s.icSession.Stop()
	}

	s.stopServerLoop()
	mlog.Info(s.ctx, "datacoord serverloop stopped")
	if s.meta != nil {
		s.meta.CloseManifestCommitExecutor()
	}
	mlog.Warn(s.ctx, "datacoord stop successful")
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	mlog.Debug(s.ctx, "clean meta", mlog.Any("kv", s.kv))
	err := s.kv.RemoveWithPrefix(s.ctx, "")
	err2 := s.watchClient.RemoveWithPrefix(s.ctx, "")
	if err2 != nil {
		if err != nil {
			err = merr.Wrapf(err, "failed to clean meta (watchdata cleanup error: %v)", err2)
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
			return s.compactionTaskMeta.TaskStatsJSON(), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.BuildIndexTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.indexMeta.TaskStatsJSON(), nil
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
			return s.indexMeta.GetIndexJSON(collectionID), nil
		})
	mlog.Info(s.ctx, "register metrics actions finished")
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

	s.meta.AddCollection(&meta.CollectionInfo{
		ID:             resp.CollectionID,
		Schema:         resp.Schema,
		Partitions:     partitionIDs,
		StartPositions: resp.GetStartPositions(),
		Properties:     properties,
		CreatedAt:      resp.GetCreatedTimestamp(),
		DatabaseName:   resp.GetDbName(),
		DatabaseID:     resp.GetDbId(),
		VChannelNames:  resp.GetVirtualChannelNames(),
	})
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
				mlog.Info(ctx, "update balance config loop exit!")
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
		mlog.Warn(s.ctx, "check data node version occur error on etcd", mlog.Err(err))
		return false
	}

	if len(sessions) == 0 {
		// only balance channel when all data node's version > 2.3.0
		Params.Reset(Params.DataCoordCfg.AutoBalance.Key)
		mlog.Info(s.ctx, "all old data node down, enable auto balance!")
		return true
	}

	Params.Save(Params.DataCoordCfg.AutoBalance.Key, "false")
	mlog.RatedDebug(s.ctx, rate.Limit(10), "old data node exist", mlog.Strings("sessions", lo.Keys(sessions)))
	return false
}

func (s *Server) listLoadedSegments(ctx context.Context) ([]int64, error) {
	req := &querypb.ListLoadedSegmentsRequest{}
	resp, err := s.mixCoord.ListLoadedSegments(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}

	return resp.SegmentIDs, nil
}
