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

package rootcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"

	"github.com/milvus-io/milvus/internal/metastore/db/rootcoord"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dao"
	"github.com/milvus-io/milvus/internal/metastore/db/dbcore"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// UniqueID is an alias of typeutil.UniqueID.
type UniqueID = typeutil.UniqueID

// Timestamp is an alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

const InvalidCollectionID = UniqueID(0)

// ------------------ struct -----------------------

var Params paramtable.ComponentParam

type Opt func(*Core)

type metaKVCreator func(root string) (kv.MetaKv, error)

func defaultMetaKVCreator(etcdCli *clientv3.Client) metaKVCreator {
	return func(root string) (kv.MetaKv, error) {
		return etcdkv.NewEtcdKV(etcdCli, root), nil
	}
}

// Core root coordinator core
type Core struct {
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	etcdCli          *clientv3.Client
	meta             IMetaTable
	scheduler        IScheduler
	broker           Broker
	garbageCollector GarbageCollector

	metaKVCreator metaKVCreator

	proxyCreator       proxyCreator
	proxyManager       *proxyManager
	proxyClientManager *proxyClientManager

	metricsCacheManager *metricsinfo.MetricsCacheManager

	chanTimeTick *timetickSync

	idAllocator  allocator.GIDAllocator
	tsoAllocator tso.Allocator

	dataCoord  types.DataCoord
	queryCoord types.QueryCoord
	indexCoord types.IndexCoord

	quotaCenter *QuotaCenter

	stateCode atomic.Value
	initOnce  sync.Once
	startOnce sync.Once
	session   *sessionutil.Session

	factory dependency.Factory

	importManager *importManager
}

// --------------------- function --------------------------

// NewCore creates a new rootcoord core
func NewCore(c context.Context, factory dependency.Factory) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	core := &Core{
		ctx:     ctx,
		cancel:  cancel,
		factory: factory,
	}
	core.UpdateStateCode(internalpb.StateCode_Abnormal)
	return core, nil
}

// UpdateStateCode update state code
func (c *Core) UpdateStateCode(code internalpb.StateCode) {
	c.stateCode.Store(code)
}

func (c *Core) checkHealthy() (internalpb.StateCode, bool) {
	code := c.stateCode.Load().(internalpb.StateCode)
	ok := code == internalpb.StateCode_Healthy
	return code, ok
}

func (c *Core) sendTimeTick(t Timestamp, reason string) error {
	pc := c.chanTimeTick.listDmlChannels()
	pt := make([]uint64, len(pc))
	for i := 0; i < len(pt); i++ {
		pt[i] = t
	}
	ttMsg := internalpb.ChannelTimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			Timestamp: t,
			SourceID:  c.session.ServerID,
		},
		ChannelNames:     pc,
		Timestamps:       pt,
		DefaultTimestamp: t,
	}
	return c.chanTimeTick.updateTimeTick(&ttMsg, reason)
}

func (c *Core) startTimeTickLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(Params.ProxyCfg.TimeTickInterval)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if ts, err := c.tsoAllocator.GenerateTSO(1); err == nil {
				err := c.sendTimeTick(ts, "timetick loop")
				if err != nil {
					log.Warn("failed to send timetick", zap.Error(err))
				}
			}
		}
	}
}

func (c *Core) tsLoop() {
	defer c.wg.Done()
	tsoTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for {
		select {
		case <-tsoTicker.C:
			if err := c.tsoAllocator.UpdateTSO(); err != nil {
				log.Warn("failed to update timestamp: ", zap.Error(err))
				continue
			}
			ts := c.tsoAllocator.GetLastSavedTime()
			metrics.RootCoordTimestampSaved.Set(float64(ts.Unix()))
			if err := c.tsoAllocator.UpdateTSO(); err != nil {
				log.Warn("failed to update id: ", zap.Error(err))
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Core) SetNewProxyClient(f func(sess *sessionutil.Session) (types.Proxy, error)) {
	c.proxyCreator = f
}

func (c *Core) SetDataCoord(ctx context.Context, s types.DataCoord) error {
	if err := s.Init(); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	c.dataCoord = s
	return nil
}

func (c *Core) SetIndexCoord(s types.IndexCoord) error {
	if err := s.Init(); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	c.indexCoord = s
	return nil
}

func (c *Core) SetQueryCoord(s types.QueryCoord) error {
	if err := s.Init(); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	c.queryCoord = s
	return nil
}

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, collNames []string, collectionID UniqueID, ts typeutil.Timestamp) error {
	// if collectionID is specified, invalidate all the collection meta cache with the specified collectionID and return
	if collectionID != InvalidCollectionID {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionID: collectionID,
		}
		return c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)
	}

	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionName: collName,
		}
		err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)
		if err != nil {
			// TODO: try to expire all or directly return err?
			return err
		}
	}
	return nil
}

// Register register rootcoord at etcd
func (c *Core) Register() error {
	c.session.Register()
	go c.session.LivenessCheck(c.ctx, func() {
		log.Error("Root Coord disconnected from etcd, process will exit", zap.Int64("Server Id", c.session.ServerID))
		if err := c.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if c.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	c.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

// SetEtcdClient sets the etcdCli of Core
func (c *Core) SetEtcdClient(etcdClient *clientv3.Client) {
	c.etcdCli = etcdClient
}

func (c *Core) initSession() error {
	c.session = sessionutil.NewSession(c.ctx, Params.EtcdCfg.MetaRootPath, c.etcdCli)
	if c.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	c.session.Init(typeutil.RootCoordRole, Params.RootCoordCfg.Address, true, true)
	Params.SetLogger(c.session.ServerID)
	return nil
}

func (c *Core) initKVCreator() {
	if c.metaKVCreator == nil {
		c.metaKVCreator = defaultMetaKVCreator(c.etcdCli)
	}
}

func (c *Core) initMetaTable() error {
	fn := func() error {
		var catalog metastore.RootCoordCatalog
		var err error

		switch Params.MetaStoreCfg.MetaStoreType {
		case util.MetaStoreTypeEtcd:
			var metaKV kv.MetaKv
			var ss *kvmetestore.SuffixSnapshot
			var err error

			if metaKV, err = c.metaKVCreator(Params.EtcdCfg.MetaRootPath); err != nil {
				return err
			}

			if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, snapshotsSep, Params.EtcdCfg.MetaRootPath, snapshotPrefix); err != nil {
				return err
			}

			catalog = &kvmetestore.Catalog{Txn: metaKV, Snapshot: ss}
		case util.MetaStoreTypeMysql:
			// connect to database
			err := dbcore.Connect(&Params.DBCfg)
			if err != nil {
				return err
			}

			catalog = rootcoord.NewTableCatalog(dbcore.NewTxImpl(), dao.NewMetaDomain())
		default:
			return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", Params.MetaStoreCfg.MetaStoreType))
		}

		if c.meta, err = NewMetaTable(c.ctx, catalog); err != nil {
			return err
		}

		return nil
	}

	return retry.Do(c.ctx, fn, retry.Attempts(10))
}

func (c *Core) initIDAllocator() error {
	tsoKV := tsoutil.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath, globalIDAllocatorSubPath)
	idAllocator := allocator.NewGlobalIDAllocator(globalIDAllocatorKey, tsoKV)
	if err := idAllocator.Initialize(); err != nil {
		return err
	}
	c.idAllocator = idAllocator
	return nil
}

func (c *Core) initTSOAllocator() error {
	tsoKV := tsoutil.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath, globalTSOAllocatorSubPath)
	tsoAllocator := tso.NewGlobalTSOAllocator(globalTSOAllocatorKey, tsoKV)
	if err := tsoAllocator.Initialize(); err != nil {
		return err
	}
	c.tsoAllocator = tsoAllocator

	return nil
}

func (c *Core) initImportManager() error {
	impTaskKv, err := c.metaKVCreator(Params.EtcdCfg.KvRootPath)
	if err != nil {
		return err
	}

	f := NewImportFactory(c)
	c.importManager = newImportManager(
		c.ctx,
		impTaskKv,
		f.NewIDAllocator(),
		f.NewImportFunc(),
		f.NewGetCollectionNameFunc(),
	)
	c.importManager.init(c.ctx)

	return nil
}

func (c *Core) initInternal() error {
	if err := c.initSession(); err != nil {
		return err
	}

	c.initKVCreator()

	if err := c.initMetaTable(); err != nil {
		return err
	}

	if err := c.initIDAllocator(); err != nil {
		return err
	}

	if err := c.initTSOAllocator(); err != nil {
		return err
	}

	c.scheduler = newScheduler(c.ctx, c.idAllocator, c.tsoAllocator)

	c.factory.Init(&Params)

	chanMap := c.meta.ListCollectionPhysicalChannels()
	c.chanTimeTick = newTimeTickSync(c.ctx, c.session.ServerID, c.factory, chanMap)
	c.chanTimeTick.addSession(c.session)
	c.proxyClientManager = newProxyClientManager(c.proxyCreator)

	c.broker = newServerBroker(c)
	c.garbageCollector = newGarbageCollectorCtx(c)

	c.proxyManager = newProxyManager(
		c.ctx,
		c.etcdCli,
		c.chanTimeTick.initSessions,
		c.proxyClientManager.GetProxyClients,
	)
	c.proxyManager.AddSessionFunc(c.chanTimeTick.addSession, c.proxyClientManager.AddProxyClient)
	c.proxyManager.DelSessionFunc(c.chanTimeTick.delSession, c.proxyClientManager.DelProxyClient)

	c.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	c.quotaCenter = NewQuotaCenter(c.proxyClientManager, c.queryCoord, c.dataCoord, c.tsoAllocator)
	log.Debug("RootCoord init QuotaCenter done")

	if err := c.initImportManager(); err != nil {
		return err
	}

	if err := c.initCredentials(); err != nil {
		return err
	}

	if err := c.initRbac(); err != nil {
		return err
	}

	return nil
}

// Init initialize routine
func (c *Core) Init() error {
	var initError error
	c.initOnce.Do(func() {
		initError = c.initInternal()
	})
	return initError
}

func (c *Core) initCredentials() error {
	credInfo, _ := c.meta.GetCredential(util.UserRoot)
	if credInfo == nil {
		log.Debug("RootCoord init user root")
		encryptedRootPassword, _ := crypto.PasswordEncrypt(util.DefaultRootPassword)
		err := c.meta.AddCredential(&internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword})
		return err
	}
	return nil
}

func (c *Core) initRbac() (initError error) {
	// create default roles, including admin, public
	for _, role := range util.DefaultRoles {
		if initError = c.meta.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: role}); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}

	// grant privileges for the public role
	globalPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
	}
	collectionPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
	}

	for _, globalPrivilege := range globalPrivileges {
		if initError = c.meta.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			ObjectName: util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: globalPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}
	for _, collectionPrivilege := range collectionPrivileges {
		if initError = c.meta.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: collectionPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant); initError != nil {
			if common.IsIgnorableError(initError) {
				initError = nil
				continue
			}
			return
		}
	}
	return nil
}

func (c *Core) restore(ctx context.Context) error {
	colls, err := c.meta.ListAbnormalCollections(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	for _, coll := range colls {
		ts, err := c.tsoAllocator.GenerateTSO(1)
		if err != nil {
			return err
		}

		switch coll.State {
		case pb.CollectionState_CollectionDropping:
			go c.garbageCollector.ReDropCollection(coll.Clone(), ts)
		case pb.CollectionState_CollectionCreating:
			go c.garbageCollector.RemoveCreatingCollection(coll.Clone())
		default:
		}
	}

	colls, err = c.meta.ListCollections(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	for _, coll := range colls {
		for _, part := range coll.Partitions {
			ts, err := c.tsoAllocator.GenerateTSO(1)
			if err != nil {
				return err
			}

			switch part.State {
			case pb.PartitionState_PartitionDropping:
				go c.garbageCollector.ReDropPartition(coll.PhysicalChannelNames, part.Clone(), ts)
			default:
			}
		}
	}
	return nil
}

func (c *Core) startInternal() error {
	if err := c.proxyManager.WatchProxy(); err != nil {
		log.Fatal("rootcoord failed to watch proxy", zap.Error(err))
		// you can not just stuck here,
		panic(err)
	}

	if err := c.restore(c.ctx); err != nil {
		panic(err)
	}

	c.wg.Add(5)
	go c.tsLoop()
	go c.startTimeTickLoop()
	go c.chanTimeTick.startWatch(&c.wg)
	go c.importManager.expireOldTasksLoop(&c.wg, c.broker.ReleaseSegRefLock)
	go c.importManager.sendOutTasksLoop(&c.wg)

	if Params.QuotaConfig.EnableQuotaAndLimits {
		go c.quotaCenter.run()
	}

	c.scheduler.Start()

	Params.RootCoordCfg.CreatedTime = time.Now()
	Params.RootCoordCfg.UpdatedTime = time.Now()

	return nil
}

// Start starts RootCoord.
func (c *Core) Start() error {
	var err error
	c.startOnce.Do(func() {
		err = c.startInternal()
	})
	return err
}

// Stop stops rootCoord.
func (c *Core) Stop() error {
	c.UpdateStateCode(internalpb.StateCode_Abnormal)

	c.cancel()
	c.wg.Wait()
	// wait at most one second to revoke
	c.session.Revoke(time.Second)
	return nil
}

// GetComponentStates get states of components
func (c *Core) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	code := c.stateCode.Load().(internalpb.StateCode)

	nodeID := common.NotRegisteredID
	if c.session != nil && c.session.Registered() {
		nodeID = c.session.ServerID
	}

	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			// NodeID:    c.session.ServerID, // will race with Core.Register()
			NodeID:    nodeID,
			Role:      typeutil.RootCoordRole,
			StateCode: code,
			ExtraInfo: nil,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		SubcomponentStates: []*internalpb.ComponentInfo{
			{
				NodeID:    nodeID,
				Role:      typeutil.RootCoordRole,
				StateCode: code,
				ExtraInfo: nil,
			},
		},
	}, nil
}

// GetTimeTickChannel get timetick channel name
func (c *Core) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.RootCoordTimeTick,
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.RootCoordStatistics,
	}, nil
}

// CreateCollection create collection
func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateCollection")

	log.Info("received request to create collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &createCollectionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to create collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to create collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Inc()

	log.Info("done to create collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropCollection")

	log.Info("received request to drop collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &dropCollectionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to drop collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to drop collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Dec()

	log.Info("done to drop collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Value:  false,
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasCollection")

	log.Info("received request to has collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &hasCollectionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
		Rsp: &milvuspb.BoolResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to has collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection name", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasCollection failed: "+err.Error()),
			Value:  false,
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to enqueue request to has collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection name", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasCollection failed: "+err.Error()),
			Value:  false,
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to has collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()),
		zap.Bool("exist", t.Rsp.GetValue()))
	return t.Rsp, nil
}

// DescribeCollection return collection info
func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode"+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DescribeCollection")

	log.Info("received request to describe collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.GetCollectionName()), zap.Int64("id", in.GetCollectionID()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &describeCollectionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
		Rsp: &milvuspb.DescribeCollectionResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to describe collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection name", in.GetCollectionName()), zap.Int64("id", in.GetCollectionID()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeCollection failed: "+err.Error()),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to describe collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection name", in.GetCollectionName()), zap.Int64("id", in.GetCollectionID()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "DescribeCollection failed: "+err.Error()),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to describe collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection name", in.GetCollectionName()), zap.Int64("id", in.GetCollectionID()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return t.Rsp, nil
}

// ShowCollections list all collection names
func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowCollections")

	log.Info("received request to show collections", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbname", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &showCollectionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
		Rsp: &milvuspb.ShowCollectionsResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to show collections", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbname", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowCollections failed: "+err.Error()),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to show collections", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbname", in.GetDbName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowCollections failed: "+err.Error()),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowCollections").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to show collections", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbname", in.GetDbName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()),
		zap.Int("num of collections", len(t.Rsp.GetCollectionNames()))) // maybe very large, print number instead.
	return t.Rsp, nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreatePartition")

	log.Info("received request to create partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &createPartitionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to create partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to create partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreatePartition").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to create partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropPartition")

	log.Info("received request to drop partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &dropPartitionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to drop partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}
	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to drop partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to drop partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Value:  false,
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasPartition")

	log.Info("received request to has partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &hasPartitionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
		Rsp: &milvuspb.BoolResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to has partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasPartition failed: "+err.Error()),
			Value:  false,
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to has partition", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "HasPartition failed: "+err.Error()),
			Value:  false,
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to has partition", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()),
		zap.Bool("exist", t.Rsp.GetValue()))
	return t.Rsp, nil
}

// ShowPartitions list all partition names
func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowPartitions")

	log.Info("received request to show partitions", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &showPartitionTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
		Rsp: &milvuspb.ShowPartitionsResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to show partitions", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowPartitions failed: "+err.Error()),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to show partitions", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ShowPartitions failed: "+err.Error()),
		}, nil
	}
	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowPartitions").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to show partitions", zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()),
		zap.Strings("partitions", t.Rsp.GetPartitionNames()))
	return t.Rsp, nil
}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	// ShowSegments Only used in GetPersistentSegmentInfo, it's already deprecated for a long time.
	// Though we continue to keep current logic, it's not right enough since RootCoord only contains indexed segments.
	return &milvuspb.ShowSegmentsResponse{Status: succStatus()}, nil
}

// AllocTimestamp alloc timestamp
func (c *Core) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocTimestampResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	ts, err := c.tsoAllocator.GenerateTSO(in.GetCount())
	if err != nil {
		log.Error("failed to allocate timestamp", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		return &rootcoordpb.AllocTimestampResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "AllocTimestamp failed: "+err.Error()),
		}, nil
	}

	// return first available timestamp
	ts = ts - uint64(in.GetCount()) + 1
	metrics.RootCoordTimestamp.Set(float64(ts))
	return &rootcoordpb.AllocTimestampResponse{
		Status:    succStatus(),
		Timestamp: ts,
		Count:     in.GetCount(),
	}, nil
}

// AllocID alloc ids
func (c *Core) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocIDResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	start, _, err := c.idAllocator.Alloc(in.Count)
	if err != nil {
		log.Error("failed to allocate id", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		return &rootcoordpb.AllocIDResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "AllocID failed: "+err.Error()),
			Count:  in.Count,
		}, nil
	}

	metrics.RootCoordIDAllocCounter.Add(float64(in.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: succStatus(),
		ID:     start,
		Count:  in.Count,
	}, nil
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Core) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		log.Warn("failed to updateTimeTick because rootcoord is not healthy", zap.Any("state", code))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	if in.Base.MsgType != commonpb.MsgType_TimeTick {
		log.Warn("failed to updateTimeTick because base messasge is not timetick, state", zap.Any("base message type", in.Base.MsgType))
		msgTypeName := commonpb.MsgType_name[int32(in.Base.GetMsgType())]
		return failStatus(commonpb.ErrorCode_UnexpectedError, "invalid message type "+msgTypeName), nil
	}
	err := c.chanTimeTick.updateTimeTick(in, "gRPC")
	if err != nil {
		log.Warn("failed to updateTimeTick", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		return failStatus(commonpb.ErrorCode_UnexpectedError, "UpdateTimeTick failed: "+err.Error()), nil
	}
	return succStatus(), nil
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Core) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, in)
	if err != nil {
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}
	return succStatus(), nil
}

//ShowConfigurations returns the configurations of RootCoord matching req.Pattern
func (c *Core) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &internalpb.ShowConfigurationsResponse{
			Status:        failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics get metrics
func (c *Core) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(in.Request)
	if err != nil {
		log.Warn("ParseMetricType failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("node_id", c.session.ServerID), zap.String("req", in.Request), zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status:   failStatus(commonpb.ErrorCode_UnexpectedError, "ParseMetricType failed: "+err.Error()),
			Response: "",
		}, nil
	}

	log.Debug("GetMetrics success", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}

		log.Warn("GetSystemInfoMetrics from cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Error(err))

		systemInfoMetrics, err := c.getSystemInfoMetrics(ctx, in)
		if err != nil {
			log.Warn("GetSystemInfoMetrics failed", zap.String("role", typeutil.RootCoordRole),
				zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status:   failStatus(commonpb.ErrorCode_UnexpectedError, fmt.Sprintf("getSystemInfoMetrics failed: %s", err.Error())),
				Response: "",
			}, nil
		}

		c.metricsCacheManager.UpdateSystemInfoMetrics(systemInfoMetrics)
		return systemInfoMetrics, err
	}

	log.Warn("GetMetrics failed, metric type not implemented", zap.String("role", typeutil.RootCoordRole),
		zap.String("metric_type", metricType), zap.Int64("msgID", in.GetBase().GetMsgID()))

	return &milvuspb.GetMetricsResponse{
		Status:   failStatus(commonpb.ErrorCode_UnexpectedError, metricsinfo.MsgUnimplementedMetric),
		Response: "",
	}, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateAlias")

	log.Info("received request to create alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &createAliasTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to create alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to create alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to create alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropAlias")

	log.Info("received request to drop alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &dropAliasTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to drop alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to drop alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to drop alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("AlterAlias")

	log.Info("received request to alter alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &alterAliasTask{
		baseTaskV2: baseTaskV2{
			ctx:  ctx,
			core: c,
			done: make(chan error, 1),
		},
		Req: in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Error("failed to enqueue request to alter alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Error("failed to alter alias", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("AlterAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to alter alias", zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()), zap.String("collection", in.GetCollectionName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

// Import imports large files (json, numpy, etc.) on MinIO/S3 storage into Milvus storage.
func (c *Core) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ImportResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	// Get collection/partition ID from collection/partition name.
	var cID UniqueID
	var err error
	if cID, err = c.meta.GetCollectionIDByName(req.GetCollectionName()); err != nil {
		log.Error("failed to find collection ID from its name",
			zap.String("collection name", req.GetCollectionName()),
			zap.Error(err))
		return nil, err
	}
	var pID UniqueID
	if pID, err = c.meta.GetPartitionByName(cID, req.GetPartitionName(), typeutil.MaxTimestamp); err != nil {
		log.Error("failed to get partition ID from its name",
			zap.String("partition name", req.GetPartitionName()),
			zap.Error(err))
		return nil, err
	}
	log.Info("RootCoord receive import request",
		zap.String("collection name", req.GetCollectionName()),
		zap.Int64("collection ID", cID),
		zap.String("partition name", req.GetPartitionName()),
		zap.Int64("partition ID", pID),
		zap.Int("# of files = ", len(req.GetFiles())),
		zap.Bool("row-based", req.GetRowBased()),
	)
	resp := c.importManager.importJob(ctx, req, cID, pID)
	return resp, nil
}

// GetImportState returns the current state of an import task.
func (c *Core) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetImportStateResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}
	return c.importManager.getTaskState(req.GetTask()), nil
}

// ListImportTasks returns id array of all import tasks.
func (c *Core) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ListImportTasksResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]),
		}, nil
	}

	resp := &milvuspb.ListImportTasksResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Tasks: c.importManager.listAllTasks(),
	}
	return resp, nil
}

// ReportImport reports import task state to RootCoord.
func (c *Core) ReportImport(ctx context.Context, ir *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	log.Info("RootCoord receive import state report",
		zap.Int64("task ID", ir.GetTaskId()),
		zap.Any("import state", ir.GetState()))
	if code, ok := c.checkHealthy(); !ok {
		return failStatus(commonpb.ErrorCode_UnexpectedError, "StateCode="+internalpb.StateCode_name[int32(code)]), nil
	}
	// Special case for ImportState_ImportAllocSegment state, where we shall only add segment ref lock and do no other
	// operations.
	// TODO: This is inelegant and must get re-structured.
	if ir.GetState() == commonpb.ImportState_ImportAllocSegment {
		// Lock the segments, so we don't lose track of them when compaction happens.
		// Note that these locks will be unlocked in c.postImportPersistLoop() -> checkSegmentLoadedLoop().
		if err := c.broker.AddSegRefLock(ctx, ir.GetTaskId(), ir.GetSegments()); err != nil {
			log.Error("failed to acquire segment ref lock", zap.Error(err))
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("failed to acquire segment ref lock %s", err.Error()),
			}, nil
		}
		// Update task store with new segments.
		c.importManager.appendTaskSegments(ir.GetTaskId(), ir.GetSegments())
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}
	// Upon receiving ReportImport request, update the related task's state in task store.
	ti, err := c.importManager.updateTaskState(ir)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UpdateImportTaskFailure,
			Reason:    err.Error(),
		}, nil
	}

	// This method update a busy node to idle node, and send import task to idle node
	resendTaskFunc := func() {
		func() {
			c.importManager.busyNodesLock.Lock()
			defer c.importManager.busyNodesLock.Unlock()
			delete(c.importManager.busyNodes, ir.GetDatanodeId())
			log.Info("DataNode is no longer busy",
				zap.Int64("dataNode ID", ir.GetDatanodeId()),
				zap.Int64("task ID", ir.GetTaskId()))

		}()
		c.importManager.sendOutTasks(c.importManager.ctx)
	}

	// If task failed, send task to idle datanode
	if ir.GetState() == commonpb.ImportState_ImportFailed {
		// Release segments when task fails.
		log.Info("task failed, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.broker.ReleaseSegRefLock(ctx, ir.GetTaskId(), ir.GetSegments())
		}, retry.Attempts(100))
		if err != nil {
			log.Error("failed to release lock, about to panic!")
			panic(err)
		}
		resendTaskFunc()
	}

	// So much for reporting, unless the task just reached `ImportPersisted` state.
	if ir.GetState() != commonpb.ImportState_ImportPersisted {
		log.Debug("non import-persisted state received, return immediately",
			zap.Any("task ID", ir.GetTaskId()),
			zap.Any("import state", ir.GetState()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	// Look up collection name on collection ID.
	var colName string
	var colMeta *model.Collection
	if colMeta, err = c.meta.GetCollectionByID(ctx, ti.GetCollectionId(), typeutil.MaxTimestamp); err != nil {
		log.Error("failed to get collection name",
			zap.Int64("collection ID", ti.GetCollectionId()),
			zap.Error(err))
		// In some unexpected cases, user drop collection when bulkload task still in pending list, the datanode become idle.
		// If we directly return, the pending tasks will remain in pending list. So we call resendTaskFunc() to push next pending task to idle datanode.
		resendTaskFunc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound,
			Reason:    "failed to get collection name for collection ID" + strconv.FormatInt(ti.GetCollectionId(), 10),
		}, nil
	}
	colName = colMeta.Name

	// When DataNode has done its thing, remove it from the busy node list. And send import task again
	resendTaskFunc()

	// Flush all import data segments.
	c.broker.Flush(ctx, ti.GetCollectionId(), ir.GetSegments())
	// Check if data are "queryable" and if indices are built on all segments.
	go c.postImportPersistLoop(c.ctx, ir.GetTaskId(), ti.GetCollectionId(), colName, ir.GetSegments())

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// CountCompleteIndex checks indexing status of the given segments.
// It returns an error if error occurs. It also returns a boolean indicating whether indexing is done (or if no index
// is needed).
func (c *Core) CountCompleteIndex(ctx context.Context, collectionName string, collectionID UniqueID,
	allSegmentIDs []UniqueID) (bool, error) {
	// Note: Index name is always Params.CommonCfg.DefaultIndexName in current Milvus designs as of today.
	indexName := Params.CommonCfg.DefaultIndexName

	states, err := c.broker.GetSegmentIndexState(ctx, collectionID, indexName, allSegmentIDs)
	if err != nil {
		log.Error("failed to get index state in checkSegmentIndexStates", zap.Error(err))
		return false, err
	}

	// Count the # of segments with finished index.
	ct := 0
	for _, s := range states {
		if s.State == commonpb.IndexState_Finished {
			ct++
		}
	}
	log.Info("segment indexing state checked",
		//zap.Int64s("segments checked", seg2Check),
		//zap.Int("# of checked segment", len(seg2Check)),
		zap.Int("# of segments with complete index", ct),
		zap.String("collection name", collectionName),
		zap.Int64("collection ID", collectionID),
	)
	return len(allSegmentIDs) == ct, nil
}

func (c *Core) postImportPersistLoop(ctx context.Context, taskID int64, colID int64, colName string, segIDs []UniqueID) {
	// Loop and check if segments are loaded in queryNodes.
	c.wg.Add(1)
	go c.checkSegmentLoadedLoop(ctx, taskID, colID, segIDs)
	// Check if collection has any indexed fields. If so, start a loop to check segments' index states.
	if _, err := c.meta.GetCollectionByID(ctx, colID, typeutil.MaxTimestamp); err != nil {
		log.Error("failed to find meta for collection",
			zap.Int64("collection ID", colID),
			zap.Error(err))
	} else {
		log.Info("start checking index state", zap.Int64("collection ID", colID))
		c.wg.Add(1)
		go c.checkCompleteIndexLoop(ctx, taskID, colID, colName, segIDs)
	}
}

// checkSegmentLoadedLoop loops and checks if all segments in `segIDs` are loaded in queryNodes.
func (c *Core) checkSegmentLoadedLoop(ctx context.Context, taskID int64, colID int64, segIDs []UniqueID) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportSegmentStateCheckInterval*1000) * time.Millisecond)
	defer ticker.Stop()
	expireTicker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportSegmentStateWaitLimit*1000) * time.Millisecond)
	defer expireTicker.Stop()
	defer func() {
		log.Info("we are done checking segment loading state, release segment ref locks")
		err := retry.Do(ctx, func() error {
			return c.broker.ReleaseSegRefLock(ctx, taskID, segIDs)
		}, retry.Attempts(100))
		if err != nil {
			log.Error("failed to release lock, about to panic!")
			panic(err)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("(in check segment loaded loop) context done, exiting checkSegmentLoadedLoop")
			return
		case <-ticker.C:
			resp, err := c.broker.GetQuerySegmentInfo(ctx, colID, segIDs)
			log.Debug("(in check segment loaded loop)",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs expected", segIDs),
				zap.Int("# of segments found", len(resp.GetInfos())))
			if err != nil {
				log.Warn("(in check segment loaded loop) failed to call get segment info on queryCoord",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
			} else if len(resp.GetInfos()) == len(segIDs) {
				// Check if all segment info are loaded in queryNodes.
				log.Info("(in check segment loaded loop) all import data segments loaded in queryNodes",
					zap.Int64("task ID", taskID),
					zap.Int64("collection ID", colID),
					zap.Int64s("segment IDs", segIDs))
				c.importManager.setTaskDataQueryable(taskID)
				return
			}
		case <-expireTicker.C:
			log.Warn("(in check segment loaded loop) segments still not loaded after max wait time",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs", segIDs))
			return
		}
	}
}

// checkCompleteIndexLoop loops and checks if all indices are built for an import task's segments.
func (c *Core) checkCompleteIndexLoop(ctx context.Context, taskID int64, colID int64, colName string, segIDs []UniqueID) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexCheckInterval*1000) * time.Millisecond)
	defer ticker.Stop()
	expireTicker := time.NewTicker(time.Duration(Params.RootCoordCfg.ImportIndexWaitLimit*1000) * time.Millisecond)
	defer expireTicker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("(in check complete index loop) context done, exiting checkCompleteIndexLoop")
			return
		case <-ticker.C:
			if done, err := c.CountCompleteIndex(ctx, colName, colID, segIDs); err == nil && done {
				log.Info("(in check complete index loop) indices are built or no index needed",
					zap.Int64("task ID", taskID))
				c.importManager.setTaskDataIndexed(taskID)
				return
			} else if err != nil {
				log.Error("(in check complete index loop) an error occurs",
					zap.Error(err))
			}
		case <-expireTicker.C:
			log.Warn("(in check complete index loop) indexing is taken too long",
				zap.Int64("task ID", taskID),
				zap.Int64("collection ID", colID),
				zap.Int64s("segment IDs", segIDs))
			return
		}
	}
}

// ExpireCredCache will call invalidate credential cache
func (c *Core) ExpireCredCache(ctx context.Context, username string) error {
	req := proxypb.InvalidateCredCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:  0, //TODO, msg type
			MsgID:    0, //TODO, msg id
			SourceID: c.session.ServerID,
		},
		Username: username,
	}
	return c.proxyClientManager.InvalidateCredentialCache(ctx, &req)
}

// UpdateCredCache will call update credential cache
func (c *Core) UpdateCredCache(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	req := proxypb.UpdateCredCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:  0, //TODO, msg type
			MsgID:    0, //TODO, msg id
			SourceID: c.session.ServerID,
		},
		Username: credInfo.Username,
		Password: credInfo.Sha256Password,
	}
	return c.proxyClientManager.UpdateCredentialCache(ctx, &req)
}

// CreateCredential create new user and password
// 	1. decode ciphertext password to raw password
// 	2. encrypt raw password
// 	3. save in to etcd
func (c *Core) CreateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	method := "CreateCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("CreateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	// insert to db
	err := c.meta.AddCredential(credInfo)
	if err != nil {
		log.Error("CreateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_CreateCredentialFailure, "CreateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Warn("CreateCredential add cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
	}
	log.Debug("CreateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Inc()
	return succStatus(), nil
}

// GetCredential get credential by username
func (c *Core) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	method := "GetCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("GetCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	credInfo, err := c.meta.GetCredential(in.Username)
	if err != nil {
		log.Error("GetCredential query credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &rootcoordpb.GetCredentialResponse{
			Status: failStatus(commonpb.ErrorCode_GetCredentialFailure, "GetCredential failed: "+err.Error()),
		}, err
	}
	log.Debug("GetCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &rootcoordpb.GetCredentialResponse{
		Status:   succStatus(),
		Username: credInfo.Username,
		Password: credInfo.EncryptedPassword,
	}, nil
}

// UpdateCredential update password for a user
func (c *Core) UpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	method := "UpdateCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	log.Debug("UpdateCredential", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))
	// update data on storage
	err := c.meta.AlterCredential(credInfo)
	if err != nil {
		log.Error("UpdateCredential save credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Error("UpdateCredential update cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UpdateCredentialFailure, "UpdateCredential failed: "+err.Error()), nil
	}
	log.Debug("UpdateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// DeleteCredential delete a user
func (c *Core) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	method := "DeleteCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	// delete data on storage
	err := c.meta.DeleteCredential(in.Username)
	if err != nil {
		log.Error("DeleteCredential remove credential failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), err
	}
	// invalidate proxy's local cache
	err = c.ExpireCredCache(ctx, in.Username)
	if err != nil {
		log.Error("DeleteCredential expire credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_DeleteCredentialFailure, "DeleteCredential failed: "+err.Error()), nil
	}
	log.Debug("DeleteCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Dec()
	return succStatus(), nil
}

// ListCredUsers list all usernames
func (c *Core) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	method := "ListCredUsers"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	credInfo, err := c.meta.ListCredentialUsernames()
	if err != nil {
		log.Error("ListCredUsers query usernames failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("msgID", in.Base.MsgID), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListCredUsersResponse{
			Status: failStatus(commonpb.ErrorCode_ListCredUsersFailure, "ListCredUsers failed: "+err.Error()),
		}, err
	}
	log.Debug("ListCredUsers success", zap.String("role", typeutil.RootCoordRole))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    succStatus(),
		Usernames: credInfo.Usernames,
	}, nil
}

// CreateRole create role
// - check the node health
// - check if the role is existed
// - check if the role num has reached the limit
// - create the role by the meta api
func (c *Core) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	method := "CreateRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	entity := in.Entity

	err := c.meta.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name})
	if err != nil {
		errMsg := "fail to create role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_CreateRoleFailure, errMsg), nil
	}

	logger.Debug(method+" success", zap.String("role_name", entity.Name))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Inc()

	return succStatus(), nil
}

// DropRole drop role
// - check the node health
// - check if the role name is existed
// - check if the role has some grant info
// - get all role mapping of this role
// - drop these role mappings
// - drop the role by the meta api
func (c *Core) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	method := "DropRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	if _, err := c.meta.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "the role isn't existed"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}

	grantEntities, err := c.meta.SelectGrant(util.DefaultTenant, &milvuspb.GrantEntity{
		Role: &milvuspb.RoleEntity{Name: in.RoleName},
	})
	if len(grantEntities) != 0 {
		errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	roleResults, err := c.meta.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, true)
	if err != nil {
		errMsg := "fail to select a role by role name"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	logger.Debug("role to user info", zap.Int("counter", len(roleResults)))
	for _, roleResult := range roleResults {
		for index, userEntity := range roleResult.Users {
			if err = c.meta.OperateUserRole(util.DefaultTenant,
				&milvuspb.UserEntity{Name: userEntity.Name},
				&milvuspb.RoleEntity{Name: roleResult.Role.Name}, milvuspb.OperateUserRoleType_RemoveUserFromRole); err != nil {
				if common.IsIgnorableError(err) {
					continue
				}
				errMsg := "fail to remove user from role"
				log.Error(errMsg, zap.Any("in", in), zap.String("role_name", roleResult.Role.Name), zap.String("username", userEntity.Name), zap.Int("current_index", index), zap.Error(err))
				return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
			}
		}
	}
	if err = c.meta.DropGrant(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}); err != nil {
		errMsg := "fail to drop the grant"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}
	if err = c.meta.DropRole(util.DefaultTenant, in.RoleName); err != nil {
		errMsg := "fail to drop the role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_DropRoleFailure, errMsg), nil
	}

	logger.Debug(method+" success", zap.String("role_name", in.RoleName))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Dec()
	return succStatus(), nil
}

// OperateUserRole operate the relationship between a user and a role
// - check the node health
// - check if the role is valid
// - check if the user is valid
// - operate the user-role by the meta api
// - update the policy cache
func (c *Core) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	method := "OperateUserRole-" + in.Type.String()
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}

	if _, err := c.meta.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "fail to check the role name"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
	}
	if _, err := c.meta.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, false); err != nil {
		errMsg := "fail to check the username"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
	}
	updateCache := true
	if err := c.meta.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, &milvuspb.RoleEntity{Name: in.RoleName}, in.Type); err != nil {
		if !common.IsIgnorableError(err) {
			errMsg := "fail to operate user to role"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
		updateCache = false
	}

	if updateCache {
		var opType int32
		switch in.Type {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			opType = int32(typeutil.CacheAddUserToRole)
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			opType = int32(typeutil.CacheRemoveUserFromRole)
		default:
			errMsg := "invalid operate type for the OperateUserRole api"
			log.Error(errMsg, zap.Any("in", in))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
		if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.EncodeUserRoleCache(in.Username, in.RoleName),
		}); err != nil {
			errMsg := "fail to refresh policy info cache"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// SelectRole select role
// - check the node health
// - check if the role is valid when this param is provided
// - select role by the meta api
func (c *Core) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	method := "SelectRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectRoleResponse{Status: errorutil.UnhealthyStatus(code)}, errorutil.UnhealthyError()
	}

	if in.Role != nil {
		if _, err := c.meta.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: in.Role.Name}, false); err != nil {
			if common.IsKeyNotExistError(err) {
				return &milvuspb.SelectRoleResponse{
					Status: succStatus(),
				}, nil
			}
			errMsg := "fail to select the role to check the role name"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectRoleResponse{
				Status: failStatus(commonpb.ErrorCode_SelectRoleFailure, errMsg),
			}, nil
		}
	}
	roleResults, err := c.meta.SelectRole(util.DefaultTenant, in.Role, in.IncludeUserInfo)
	if err != nil {
		errMsg := "fail to select the role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectRoleResponse{
			Status: failStatus(commonpb.ErrorCode_SelectRoleFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectRoleResponse{
		Status:  succStatus(),
		Results: roleResults,
	}, nil
}

// SelectUser select user
// - check the node health
// - check if the user is valid when this param is provided
// - select user by the meta api
func (c *Core) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	method := "SelectUser"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectUserResponse{Status: errorutil.UnhealthyStatus(code)}, errorutil.UnhealthyError()
	}

	if in.User != nil {
		if _, err := c.meta.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: in.User.Name}, false); err != nil {
			if common.IsKeyNotExistError(err) {
				return &milvuspb.SelectUserResponse{
					Status: succStatus(),
				}, nil
			}
			errMsg := "fail to select the user to check the username"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectUserResponse{
				Status: failStatus(commonpb.ErrorCode_SelectUserFailure, errMsg),
			}, nil
		}
	}
	userResults, err := c.meta.SelectUser(util.DefaultTenant, in.User, in.IncludeRoleInfo)
	if err != nil {
		errMsg := "fail to select the user"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectUserResponse{
			Status: failStatus(commonpb.ErrorCode_SelectUserFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectUserResponse{
		Status:  succStatus(),
		Results: userResults,
	}, nil
}

func (c *Core) isValidRole(entity *milvuspb.RoleEntity) error {
	if entity == nil {
		return errors.New("the role entity is nil")
	}
	if entity.Name == "" {
		return errors.New("the name in the role entity is empty")
	}
	if _, err := c.meta.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name}, false); err != nil {
		return err
	}
	return nil
}

func (c *Core) isValidObject(entity *milvuspb.ObjectEntity) error {
	if entity == nil {
		return errors.New("the object entity is nil")
	}
	if _, ok := commonpb.ObjectType_value[entity.Name]; !ok {
		return fmt.Errorf("the object type in the object entity[name: %s] is invalid", entity.Name)
	}
	return nil
}

func (c *Core) isValidGrantor(entity *milvuspb.GrantorEntity, object string) error {
	if entity == nil {
		return errors.New("the grantor entity is nil")
	}
	if entity.User == nil {
		return errors.New("the user entity in the grantor entity is nil")
	}
	if entity.User.Name == "" {
		return errors.New("the name in the user entity of the grantor entity is empty")
	}
	if _, err := c.meta.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: entity.User.Name}, false); err != nil {
		return err
	}
	if entity.Privilege == nil {
		return errors.New("the privilege entity in the grantor entity is nil")
	}
	if util.IsAnyWord(entity.Privilege.Name) {
		return nil
	}
	if privilegeName := util.PrivilegeNameForMetastore(entity.Privilege.Name); privilegeName == "" {
		return fmt.Errorf("the privilege name[%s] in the privilege entity is invalid", entity.Privilege.Name)
	}
	privileges, ok := util.ObjectPrivileges[object]
	if !ok {
		return fmt.Errorf("the object type[%s] is invalid", object)
	}
	for _, privilege := range privileges {
		if privilege == entity.Privilege.Name {
			return nil
		}
	}
	return fmt.Errorf("the privilege name[%s] is invalid", entity.Privilege.Name)
}

// OperatePrivilege operate the privilege, including grant and revoke
// - check the node health
// - check if the operating type is valid
// - check if the entity is nil
// - check if the params, including the resource entity, the principal entity, the grantor entity, is valid
// - operate the privilege by the meta api
// - update the policy cache
func (c *Core) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	method := "OperatePrivilege"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return errorutil.UnhealthyStatus(code), errorutil.UnhealthyError()
	}
	if in.Type != milvuspb.OperatePrivilegeType_Grant && in.Type != milvuspb.OperatePrivilegeType_Revoke {
		errMsg := fmt.Sprintf("invalid operate privilege type, current type: %s, valid value: [%s, %s]", in.Type, milvuspb.OperatePrivilegeType_Grant, milvuspb.OperatePrivilegeType_Revoke)
		log.Error(errMsg, zap.Any("in", in))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		log.Error(errMsg, zap.Any("in", in))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
	}
	if err := c.isValidObject(in.Entity.Object); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}
	if err := c.isValidGrantor(in.Entity.Grantor, in.Entity.Object.Name); err != nil {
		log.Error("", zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, err.Error()), nil
	}

	logger.Debug("before PrivilegeNameForMetastore", zap.String("privilege", in.Entity.Grantor.Privilege.Name))
	if !util.IsAnyWord(in.Entity.Grantor.Privilege.Name) {
		in.Entity.Grantor.Privilege.Name = util.PrivilegeNameForMetastore(in.Entity.Grantor.Privilege.Name)
	}
	logger.Debug("after PrivilegeNameForMetastore", zap.String("privilege", in.Entity.Grantor.Privilege.Name))
	if in.Entity.Object.Name == commonpb.ObjectType_Global.String() {
		in.Entity.ObjectName = util.AnyWord
	}
	updateCache := true
	if err := c.meta.OperatePrivilege(util.DefaultTenant, in.Entity, in.Type); err != nil {
		if !common.IsIgnorableError(err) {
			errMsg := "fail to operate the privilege"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
		updateCache = false
	}

	if updateCache {
		var opType int32
		switch in.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			opType = int32(typeutil.CacheGrantPrivilege)
		case milvuspb.OperatePrivilegeType_Revoke:
			opType = int32(typeutil.CacheRevokePrivilege)
		default:
			errMsg := "invalid operate type for the OperatePrivilege api"
			log.Error(errMsg, zap.Any("in", in))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
		if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.PolicyForPrivilege(in.Entity.Role.Name, in.Entity.Object.Name, in.Entity.ObjectName, in.Entity.Grantor.Privilege.Name),
		}); err != nil {
			errMsg := "fail to refresh policy info cache"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return succStatus(), nil
}

// SelectGrant select grant
// - check the node health
// - check if the principal entity is valid
// - check if the resource entity which is provided by the user is valid
// - select grant by the meta api
func (c *Core) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	method := "SelectGrant"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.SelectGrantResponse{
			Status: errorutil.UnhealthyStatus(code),
		}, errorutil.UnhealthyError()
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		log.Error(errMsg, zap.Any("in", in))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, errMsg),
		}, nil
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		log.Error("", zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, err.Error()),
		}, nil
	}
	if in.Entity.Object != nil {
		if err := c.isValidObject(in.Entity.Object); err != nil {
			log.Error("", zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectGrantResponse{
				Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, err.Error()),
			}, nil
		}
	}

	grantEntities, err := c.meta.SelectGrant(util.DefaultTenant, in.Entity)
	if common.IsKeyNotExistError(err) {
		return &milvuspb.SelectGrantResponse{
			Status: succStatus(),
		}, nil
	}
	if err != nil {
		errMsg := "fail to select the grant"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: failStatus(commonpb.ErrorCode_SelectGrantFailure, errMsg),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectGrantResponse{
		Status:   succStatus(),
		Entities: grantEntities,
	}, nil
}

func (c *Core) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	method := "PolicyList"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	logger.Debug(method, zap.Any("in", in))

	if code, ok := c.checkHealthy(); !ok {
		return &internalpb.ListPolicyResponse{
			Status: errorutil.UnhealthyStatus(code),
		}, errorutil.UnhealthyError()
	}

	policies, err := c.meta.ListPolicy(util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list policy"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: failStatus(commonpb.ErrorCode_ListPolicyFailure, errMsg),
		}, nil
	}
	userRoles, err := c.meta.ListUserRole(util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list user-role"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: failStatus(commonpb.ErrorCode_ListPolicyFailure, "fail to list user-role"),
		}, nil
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &internalpb.ListPolicyResponse{
		Status:      succStatus(),
		PolicyInfos: policies,
		UserRoles:   userRoles,
	}, nil
}

func (c *Core) InvalidateSoFile(ctx context.Context, in *milvuspb.InvalidateSoFileRequest) (*commonpb.Status, error) {
	method := "InvalidateSoFile"
	logger.Debug(method, zap.Any("in", in))

	if err := c.proxyClientManager.InvalidateSoFile(ctx); err != nil {
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}
	logger.Debug(method + " success")
	return succStatus(), nil
}
