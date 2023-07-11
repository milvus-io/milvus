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
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dao"
	"github.com/milvus-io/milvus/internal/metastore/db/dbcore"
	"github.com/milvus-io/milvus/internal/metastore/db/rootcoord"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	tso2 "github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	tsoutil2 "github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/errorutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is an alias of typeutil.UniqueID.
type UniqueID = typeutil.UniqueID

// Timestamp is an alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

const InvalidCollectionID = UniqueID(0)

var Params *paramtable.ComponentParam = paramtable.Get()

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
	address          string
	meta             IMetaTable
	scheduler        IScheduler
	broker           Broker
	ddlTsLockManager DdlTsLockManager
	garbageCollector GarbageCollector
	stepExecutor     StepExecutor

	metaKVCreator metaKVCreator

	proxyCreator       proxyCreator
	proxyManager       *proxyManager
	proxyClientManager *proxyClientManager

	metricsCacheManager *metricsinfo.MetricsCacheManager

	chanTimeTick *timetickSync

	idAllocator  allocator.Interface
	tsoAllocator tso2.Allocator

	dataCoord  types.DataCoord
	queryCoord types.QueryCoord

	quotaCenter *QuotaCenter

	stateCode atomic.Value
	initOnce  sync.Once
	startOnce sync.Once
	session   *sessionutil.Session

	factory dependency.Factory

	importManager *importManager

	enableActiveStandBy bool
	activateFunc        func() error
}

// --------------------- function --------------------------

// NewCore creates a new rootcoord core
func NewCore(c context.Context, factory dependency.Factory) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	rand.Seed(time.Now().UnixNano())
	core := &Core{
		ctx:                 ctx,
		cancel:              cancel,
		factory:             factory,
		enableActiveStandBy: Params.RootCoordCfg.EnableActiveStandby.GetAsBool(),
	}

	core.UpdateStateCode(commonpb.StateCode_Abnormal)
	core.SetProxyCreator(DefaultProxyCreator)

	return core, nil
}

// UpdateStateCode update state code
func (c *Core) UpdateStateCode(code commonpb.StateCode) {
	c.stateCode.Store(code)
	log.Info("update rootcoord state", zap.String("state", code.String()))
}

func (c *Core) checkHealthy() (commonpb.StateCode, bool) {
	code := c.stateCode.Load().(commonpb.StateCode)
	ok := code == commonpb.StateCode_Healthy
	return code, ok
}

func (c *Core) sendTimeTick(t Timestamp, reason string) error {
	pc := c.chanTimeTick.listDmlChannels()
	pt := make([]uint64, len(pc))
	for i := 0; i < len(pt); i++ {
		pt[i] = t
	}
	ttMsg := internalpb.ChannelTimeTickMsg{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
			commonpbutil.WithTimeStamp(t),
			commonpbutil.WithSourceID(ddlSourceID),
		),
		ChannelNames:     pc,
		Timestamps:       pt,
		DefaultTimestamp: t,
	}
	return c.chanTimeTick.updateTimeTick(&ttMsg, reason)
}

func (c *Core) sendMinDdlTsAsTt() {
	code := c.stateCode.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		log.Warn("rootCoord is not healthy, skip send timetick")
		return
	}
	minBgDdlTs := c.ddlTsLockManager.GetMinDdlTs()
	minNormalDdlTs := c.scheduler.GetMinDdlTs()
	minDdlTs := funcutil.Min(minBgDdlTs, minNormalDdlTs)

	// zero	-> ddlTsLockManager and scheduler not started.
	if minDdlTs == typeutil.ZeroTimestamp {
		log.Warn("zero ts was met, this should be only occurred in starting state", zap.Uint64("minBgDdlTs", minBgDdlTs), zap.Uint64("minNormalDdlTs", minNormalDdlTs))
		return
	}

	// max	-> abnormal case, impossible.
	if minDdlTs == typeutil.MaxTimestamp {
		log.Warn("ddl ts is abnormal, max ts was met", zap.Uint64("minBgDdlTs", minBgDdlTs), zap.Uint64("minNormalDdlTs", minNormalDdlTs))
		return
	}

	if err := c.sendTimeTick(minDdlTs, "timetick loop"); err != nil {
		log.Warn("failed to send timetick", zap.Error(err))
	}
}

func (c *Core) startTimeTickLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("rootcoord's timetick loop quit!")
			return
		case <-ticker.C:
			c.sendMinDdlTsAsTt()
		}
	}
}

func (c *Core) tsLoop() {
	defer c.wg.Done()
	tsoTicker := time.NewTicker(tso2.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for {
		select {
		case <-tsoTicker.C:
			if err := c.tsoAllocator.UpdateTSO(); err != nil {
				log.Warn("failed to update tso", zap.Error(err))
				continue
			}
			ts := c.tsoAllocator.GetLastSavedTime()
			metrics.RootCoordTimestampSaved.Set(float64(ts.Unix()))

		case <-ctx.Done():
			log.Info("rootcoord's ts loop quit!")
			return
		}
	}
}

func (c *Core) SetProxyCreator(f func(ctx context.Context, addr string) (types.Proxy, error)) {
	c.proxyCreator = f
}

func (c *Core) SetDataCoord(s types.DataCoord) error {
	if s == nil {
		return errors.New("null DataCoord interface")
	}
	c.dataCoord = s
	return nil
}

func (c *Core) SetQueryCoord(s types.QueryCoord) error {
	if s == nil {
		return errors.New("null QueryCoord interface")
	}
	c.queryCoord = s
	return nil
}

// Register register rootcoord at etcd
func (c *Core) Register() error {
	c.session.Register()
	if c.enableActiveStandBy {
		if err := c.session.ProcessActiveStandBy(c.activateFunc); err != nil {
			return err
		}
	}
	metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.RootCoordRole).Inc()
	log.Info("RootCoord Register Finished")
	c.session.LivenessCheck(c.ctx, func() {
		log.Error("Root Coord disconnected from etcd, process will exit", zap.Int64("Server Id", c.session.ServerID))
		if err := c.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.RootCoordRole).Dec()
		// manually send signal to starter goroutine
		if c.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	return nil
}

func (c *Core) SetAddress(address string) {
	c.address = address
}

// SetEtcdClient sets the etcdCli of Core
func (c *Core) SetEtcdClient(etcdClient *clientv3.Client) {
	c.etcdCli = etcdClient
}

func (c *Core) initSession() error {
	c.session = sessionutil.NewSession(c.ctx, Params.EtcdCfg.MetaRootPath.GetValue(), c.etcdCli)
	if c.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	c.session.Init(typeutil.RootCoordRole, c.address, true, true)
	c.session.SetEnableActiveStandBy(c.enableActiveStandBy)
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

		switch Params.MetaStoreCfg.MetaStoreType.GetValue() {
		case util.MetaStoreTypeEtcd:
			var metaKV kv.MetaKv
			var ss *kvmetestore.SuffixSnapshot
			var err error

			if metaKV, err = c.metaKVCreator(Params.EtcdCfg.MetaRootPath.GetValue()); err != nil {
				return err
			}

			if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, kvmetestore.SnapshotsSep, Params.EtcdCfg.MetaRootPath.GetValue(), kvmetestore.SnapshotPrefix); err != nil {
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
			return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", Params.MetaStoreCfg.MetaStoreType.GetValue()))
		}

		if c.meta, err = NewMetaTable(c.ctx, catalog, c.tsoAllocator); err != nil {
			return err
		}

		return nil
	}

	return retry.Do(c.ctx, fn, retry.Attempts(10))
}

func (c *Core) initIDAllocator() error {
	tsoKV := tsoutil2.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), globalIDAllocatorSubPath)
	idAllocator := allocator.NewGlobalIDAllocator(globalIDAllocatorKey, tsoKV)
	if err := idAllocator.Initialize(); err != nil {
		return err
	}
	c.idAllocator = idAllocator

	log.Info("id allocator initialized",
		zap.String("root_path", Params.EtcdCfg.KvRootPath.GetValue()),
		zap.String("sub_path", globalIDAllocatorSubPath),
		zap.String("key", globalIDAllocatorKey))

	return nil
}

func (c *Core) initTSOAllocator() error {
	tsoKV := tsoutil2.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), globalTSOAllocatorSubPath)
	tsoAllocator := tso2.NewGlobalTSOAllocator(globalTSOAllocatorKey, tsoKV)
	if err := tsoAllocator.Initialize(); err != nil {
		return err
	}
	c.tsoAllocator = tsoAllocator

	log.Info("tso allocator initialized",
		zap.String("root_path", Params.EtcdCfg.KvRootPath.GetValue()),
		zap.String("sub_path", globalIDAllocatorSubPath),
		zap.String("key", globalIDAllocatorKey))

	return nil
}

func (c *Core) initImportManager() error {
	impTaskKv, err := c.metaKVCreator(Params.EtcdCfg.KvRootPath.GetValue())
	if err != nil {
		return err
	}

	f := NewImportFactory(c)
	c.importManager = newImportManager(
		c.ctx,
		impTaskKv,
		f.NewIDAllocator(),
		f.NewImportFunc(),
		f.NewGetSegmentStatesFunc(),
		f.NewGetCollectionNameFunc(),
		f.NewUnsetIsImportingStateFunc(),
	)
	c.importManager.init(c.ctx)

	return nil
}

func (c *Core) initInternal() error {
	c.UpdateStateCode(commonpb.StateCode_Initializing)
	c.initKVCreator()

	if err := c.initIDAllocator(); err != nil {
		return err
	}

	if err := c.initTSOAllocator(); err != nil {
		return err
	}

	if err := c.initMetaTable(); err != nil {
		return err
	}

	c.scheduler = newScheduler(c.ctx, c.idAllocator, c.tsoAllocator)

	c.factory.Init(Params)
	chanMap := c.meta.ListCollectionPhysicalChannels()
	c.chanTimeTick = newTimeTickSync(c.ctx, c.session.ServerID, c.factory, chanMap)
	c.proxyClientManager = newProxyClientManager(c.proxyCreator)

	c.broker = newServerBroker(c)
	c.ddlTsLockManager = newDdlTsLockManager(c.tsoAllocator)
	c.garbageCollector = newBgGarbageCollector(c)
	c.stepExecutor = newBgStepExecutor(c.ctx)

	c.proxyManager = newProxyManager(
		c.ctx,
		c.etcdCli,
		c.chanTimeTick.initSessions,
		c.proxyClientManager.GetProxyClients,
	)
	c.proxyManager.AddSessionFunc(c.chanTimeTick.addSession, c.proxyClientManager.AddProxyClient)
	c.proxyManager.DelSessionFunc(c.chanTimeTick.delSession, c.proxyClientManager.DelProxyClient)

	c.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	c.quotaCenter = NewQuotaCenter(c.proxyClientManager, c.queryCoord, c.dataCoord, c.tsoAllocator, c.meta)
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
	c.factory.Init(Params)
	if err := c.initSession(); err != nil {
		return err
	}

	if c.enableActiveStandBy {
		c.activateFunc = func() error {
			log.Info("RootCoord switch from standby to active, activating")

			var err error
			c.initOnce.Do(func() {
				if err = c.initInternal(); err != nil {
					log.Error("RootCoord init failed", zap.Error(err))
				}
			})
			if err != nil {
				return err
			}
			c.startOnce.Do(func() {
				if err = c.startInternal(); err != nil {
					log.Error("RootCoord start failed", zap.Error(err))
				}
			})
			log.Info("RootCoord startup success")
			return err
		}
		c.UpdateStateCode(commonpb.StateCode_StandBy)
		log.Info("RootCoord enter standby mode successfully")
	} else {
		c.initOnce.Do(func() {
			initError = c.initInternal()
		})
	}

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

func (c *Core) initRbac() error {
	var err error
	// create default roles, including admin, public
	for _, role := range util.DefaultRoles {
		err = c.meta.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: role})
		if err != nil && !common.IsIgnorableError(err) {
			return errors.Wrap(err, "failed to create role")
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
		err = c.meta.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: globalPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant)
		if err != nil && !common.IsIgnorableError(err) {
			return errors.Wrap(err, "failed to grant global privilege")
		}
	}
	for _, collectionPrivilege := range collectionPrivileges {
		err = c.meta.OperatePrivilege(util.DefaultTenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: util.RolePublic},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: collectionPrivilege},
			},
		}, milvuspb.OperatePrivilegeType_Grant)
		if err != nil && !common.IsIgnorableError(err) {
			return errors.Wrap(err, "failed to grant collection privilege")
		}
	}
	return nil
}

func (c *Core) restore(ctx context.Context) error {
	dbs, err := c.meta.ListDatabases(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		colls, err := c.meta.ListCollections(ctx, db.Name, typeutil.MaxTimestamp, false)
		if err != nil {
			return err
		}
		for _, coll := range colls {
			for _, part := range coll.Partitions {
				ts, err := c.tsoAllocator.GenerateTSO(1)
				if err != nil {
					return err
				}

				if coll.Available() {
					switch part.State {
					case pb.PartitionState_PartitionDropping:
						go c.garbageCollector.ReDropPartition(coll.DBID, coll.PhysicalChannelNames, part.Clone(), ts)
					case pb.PartitionState_PartitionCreating:
						go c.garbageCollector.RemoveCreatingPartition(coll.DBID, part.Clone(), ts)
					default:
					}
				} else {
					switch coll.State {
					case pb.CollectionState_CollectionDropping:
						go c.garbageCollector.ReDropCollection(coll.Clone(), ts)
					case pb.CollectionState_CollectionCreating:
						go c.garbageCollector.RemoveCreatingCollection(coll.Clone())
					default:
					}
				}
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

	if Params.QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		go c.quotaCenter.run()
	}

	c.scheduler.Start()
	c.stepExecutor.Start()

	c.startServerLoop()
	c.UpdateStateCode(commonpb.StateCode_Healthy)
	logutil.Logger(c.ctx).Info("rootcoord startup successfully")

	return nil
}

func (c *Core) startServerLoop() {
	c.wg.Add(6)
	go c.startTimeTickLoop()
	go c.tsLoop()
	go c.chanTimeTick.startWatch(&c.wg)
	go c.importManager.cleanupLoop(&c.wg)
	go c.importManager.sendOutTasksLoop(&c.wg)
	go c.importManager.flipTaskStateLoop(&c.wg)
}

// Start starts RootCoord.
func (c *Core) Start() error {
	var err error
	if !c.enableActiveStandBy {
		c.startOnce.Do(func() {
			err = c.startInternal()
		})
	}

	return err
}

func (c *Core) stopExecutor() {
	if c.stepExecutor != nil {
		c.stepExecutor.Stop()
		log.Info("stop rootcoord executor")
	}
}

func (c *Core) stopScheduler() {
	if c.scheduler != nil {
		c.scheduler.Stop()
		log.Info("stop rootcoord scheduler")
	}
}

func (c *Core) cancelIfNotNil() {
	if c.cancel != nil {
		c.cancel()
		log.Info("cancel rootcoord goroutines")
	}
}

func (c *Core) revokeSession() {
	if c.session != nil {
		// wait at most one second to revoke
		c.session.Stop()
		log.Info("revoke rootcoord session")
	}
}

// Stop stops rootCoord.
func (c *Core) Stop() error {
	c.UpdateStateCode(commonpb.StateCode_Abnormal)
	c.stopExecutor()
	c.stopScheduler()
	c.cancelIfNotNil()
	if c.quotaCenter != nil {
		c.quotaCenter.stop()
	}
	c.wg.Wait()
	c.revokeSession()
	return nil
}

// GetComponentStates get states of components
func (c *Core) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	code := c.stateCode.Load().(commonpb.StateCode)

	nodeID := common.NotRegisteredID
	if c.session != nil && c.session.Registered() {
		nodeID = c.session.ServerID
	}

	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    c.session.ServerID, // will race with Core.Register()
			NodeID:    nodeID,
			Role:      typeutil.RootCoordRole,
			StateCode: code,
			ExtraInfo: nil,
		},
		Status: merr.Status(nil),
		SubcomponentStates: []*milvuspb.ComponentInfo{
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
		Status: merr.Status(nil),
		Value:  Params.CommonCfg.RootCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (c *Core) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(nil),
		Value:  Params.CommonCfg.RootCoordStatistics.GetValue(),
	}, nil
}

func (c *Core) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	method := "CreateDatabase"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateDatabase")

	log.Ctx(ctx).Info("received request to create database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &createDatabaseTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to create database",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to create database",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfDatabases.Inc()
	log.Ctx(ctx).Info("done to create database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

func (c *Core) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	method := "DropDatabase"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropDatabase")

	log.Ctx(ctx).Info("received request to drop database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

	t := &dropDatabaseTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to drop database", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to drop database", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return failStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfDatabases.Dec()
	log.Ctx(ctx).Info("done to drop database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Uint64("ts", t.GetTs()))
	return succStatus(), nil
}

func (c *Core) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		ret := &milvuspb.ListDatabasesResponse{Status: merr.Status(merr.WrapErrServiceNotReady(code.String()))}
		return ret, nil
	}
	method := "ListDatabases"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ListDatabases")

	log := log.Ctx(ctx).With(zap.Int64("msgID", in.GetBase().GetMsgID()))
	log.Info("received request to list databases")

	t := &listDatabaseTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
		Resp:     &milvuspb.ListDatabasesResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to list databases", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListDatabasesResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ListDatabases failed: "+err.Error()),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to list databases", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListDatabasesResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "ListDatabases failed: "+err.Error()),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Info("done to list databases", zap.Int("num of databases", len(t.Resp.GetDbNames())))
	return t.Resp, nil
}

// CreateCollection create collection
func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateCollection")

	log.Ctx(ctx).Info("received request to create collection",
		zap.String("dbName", in.GetDbName()),
		zap.String("name", in.GetCollectionName()),
		zap.String("role", typeutil.RootCoordRole))

	t := &createCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to create collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to create collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Inc()
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("CreateCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to create collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropCollection")

	log.Ctx(ctx).Info("received request to drop collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.String("name", in.GetCollectionName()))

	t := &dropCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to drop collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to drop collection", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Dec()
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DropCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to drop collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasCollection")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("collection name", in.GetCollectionName()),
		zap.Uint64("ts", ts))

	log.Info("received request to has collection")

	t := &hasCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
		Rsp:      &milvuspb.BoolResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to has collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to has collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("HasCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to has collection", zap.Bool("exist", t.Rsp.GetValue()))

	return t.Rsp, nil
}

func (c *Core) describeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, allowUnavailable bool) (*model.Collection, error) {
	ts := getTravelTs(in)
	if in.GetCollectionName() != "" {
		return c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), ts)
	}
	return c.meta.GetCollectionByID(ctx, in.GetDbName(), in.GetCollectionID(), ts, allowUnavailable)
}

func convertModelToDesc(collInfo *model.Collection, aliases []string) *milvuspb.DescribeCollectionResponse {
	resp := &milvuspb.DescribeCollectionResponse{Status: merr.Status(nil)}

	resp.Schema = &schemapb.CollectionSchema{
		Name:               collInfo.Name,
		Description:        collInfo.Description,
		AutoID:             collInfo.AutoID,
		Fields:             model.MarshalFieldModels(collInfo.Fields),
		EnableDynamicField: collInfo.EnableDynamicField,
	}
	resp.CollectionID = collInfo.CollectionID
	resp.VirtualChannelNames = collInfo.VirtualChannelNames
	resp.PhysicalChannelNames = collInfo.PhysicalChannelNames
	if collInfo.ShardsNum == 0 {
		collInfo.ShardsNum = int32(len(collInfo.VirtualChannelNames))
	}
	resp.ShardsNum = collInfo.ShardsNum
	resp.ConsistencyLevel = collInfo.ConsistencyLevel

	resp.CreatedTimestamp = collInfo.CreateTime
	createdPhysicalTime, _ := tsoutil.ParseHybridTs(collInfo.CreateTime)
	resp.CreatedUtcTimestamp = uint64(createdPhysicalTime)
	resp.Aliases = aliases
	resp.StartPositions = collInfo.StartPositions
	resp.CollectionName = resp.Schema.Name
	resp.Properties = collInfo.Properties
	resp.NumPartitions = int64(len(collInfo.Partitions))
	return resp
}

func (c *Core) describeCollectionImpl(ctx context.Context, in *milvuspb.DescribeCollectionRequest, allowUnavailable bool) (*milvuspb.DescribeCollectionResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DescribeCollection")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("collection name", in.GetCollectionName()),
		zap.String("dbName", in.GetDbName()),
		zap.Int64("id", in.GetCollectionID()),
		zap.Uint64("ts", ts),
		zap.Bool("allowUnavailable", allowUnavailable))

	// TODO(longjiquan): log may be very frequent here.

	log.Info("received request to describe collection")

	t := &describeCollectionTask{
		baseTask:         newBaseTask(ctx, c),
		Req:              in,
		Rsp:              &milvuspb.DescribeCollectionResponse{Status: merr.Status(nil)},
		allowUnavailable: allowUnavailable,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to describe collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			// TODO: use commonpb.ErrorCode_CollectionNotExists. SDK use commonpb.ErrorCode_UnexpectedError now.
			Status: merr.Status(err),
			// Status: common.StatusFromError(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to describe collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			// TODO: use commonpb.ErrorCode_CollectionNotExists. SDK use commonpb.ErrorCode_UnexpectedError now.
			Status: merr.Status(err),
			// Status: common.StatusFromError(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DescribeCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to describe collection", zap.Int64("collection_id", t.Rsp.GetCollectionID()))

	return t.Rsp, nil
}

// DescribeCollection return collection info
func (c *Core) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return c.describeCollectionImpl(ctx, in, false)
}

// DescribeCollectionInternal same to DescribeCollection, but will return unavailable collections and
// only used in internal RPC.
// When query cluster tried to do recovery, it'll be healthy until all collections' targets were recovered,
// so during this time, releasing request generated by rootcoord's recovery won't succeed. So in theory, rootcoord goes
// to be healthy, querycoord recovers all collections' targets, and then querycoord serves the releasing request sent
// by rootcoord, eventually, the dropping collections will be released.
func (c *Core) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return c.describeCollectionImpl(ctx, in, true)
}

// ShowCollections list all collection names
func (c *Core) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowCollections")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("dbname", in.GetDbName()),
		zap.Uint64("ts", ts))

	log.Info("received request to show collections")

	t := &showCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
		Rsp:      &milvuspb.ShowCollectionsResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to show collections", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to show collections", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.FailLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowCollections").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("ShowCollections").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to show collections", zap.Int("num of collections", len(t.Rsp.GetCollectionNames()))) // maybe very large, print number instead.

	return t.Rsp, nil
}

func (c *Core) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("AlterCollection")

	log.Ctx(ctx).Info("received request to alter collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()))

	t := &alterCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Warn("failed to enqueue request to alter collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to alter collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("AlterCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCollections.Dec()
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("AlterCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to alter collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreatePartition")

	log.Ctx(ctx).Info("received request to create partition",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()))

	t := &createPartitionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to create partition",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()),
			zap.String("partition", in.GetPartitionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to create partition",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()),
			zap.String("partition", in.GetPartitionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreatePartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreatePartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("CreatePartition").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to create partition",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropPartition")

	log.Ctx(ctx).Info("received request to drop partition",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()))

	t := &dropPartitionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to drop partition",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()),
			zap.String("partition", in.GetPartitionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to drop partition",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("collection", in.GetCollectionName()),
			zap.String("partition", in.GetPartitionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DropPartition").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to drop partition",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.BoolResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasPartition")

	// TODO(longjiquan): why HasPartitionRequest doesn't contain Timestamp but other requests do.
	ts := typeutil.MaxTimestamp
	log := log.Ctx(ctx).With(zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()),
		zap.Uint64("ts", ts))

	log.Info("received request to has partition")

	t := &hasPartitionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
		Rsp:      &milvuspb.BoolResponse{},
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to has partition", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to has partition", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("HasPartition").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("HasPartition").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to has partition", zap.Bool("exist", t.Rsp.GetValue()))

	return t.Rsp, nil
}

func (c *Core) showPartitionsImpl(ctx context.Context, in *milvuspb.ShowPartitionsRequest, allowUnavailable bool) (*milvuspb.ShowPartitionsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowPartitions")

	log := log.Ctx(ctx).With(zap.String("collection", in.GetCollectionName()),
		zap.Int64("collection_id", in.GetCollectionID()),
		zap.Strings("partitions", in.GetPartitionNames()),
		zap.Bool("allowUnavailable", allowUnavailable))

	log.Info("received request to show partitions")

	t := &showPartitionTask{
		baseTask:         newBaseTask(ctx, c),
		Req:              in,
		Rsp:              &milvuspb.ShowPartitionsResponse{},
		allowUnavailable: allowUnavailable,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to show partitions", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
			// Status: common.StatusFromError(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to show partitions", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.FailLabel).Inc()
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
			// Status: common.StatusFromError(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("ShowPartitions").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("ShowPartitions").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to show partitions", zap.Strings("partitions", t.Rsp.GetPartitionNames()))

	return t.Rsp, nil
}

// ShowPartitions list all partition names
func (c *Core) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return c.showPartitionsImpl(ctx, in, false)
}

// ShowPartitionsInternal same to ShowPartitions, only used in internal RPC.
func (c *Core) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return c.showPartitionsImpl(ctx, in, true)
}

// ShowSegments list all segments
func (c *Core) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	// ShowSegments Only used in GetPersistentSegmentInfo, it's already deprecated for a long time.
	// Though we continue to keep current logic, it's not right enough since RootCoord only contains indexed segments.
	return &milvuspb.ShowSegmentsResponse{Status: merr.Status(nil)}, nil
}

// AllocTimestamp alloc timestamp
func (c *Core) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocTimestampResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	ts, err := c.tsoAllocator.GenerateTSO(in.GetCount())
	if err != nil {
		log.Ctx(ctx).Error("failed to allocate timestamp", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err))

		return &rootcoordpb.AllocTimestampResponse{
			Status: merr.Status(err),
		}, nil
	}

	// return first available timestamp
	ts = ts - uint64(in.GetCount()) + 1
	metrics.RootCoordTimestamp.Set(float64(ts))
	return &rootcoordpb.AllocTimestampResponse{
		Status:    merr.Status(nil),
		Timestamp: ts,
		Count:     in.GetCount(),
	}, nil
}

// AllocID alloc ids
func (c *Core) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &rootcoordpb.AllocIDResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}
	start, _, err := c.idAllocator.Alloc(in.Count)
	if err != nil {
		log.Ctx(ctx).Error("failed to allocate id",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err))

		return &rootcoordpb.AllocIDResponse{
			Status: merr.Status(err),
			Count:  in.Count,
		}, nil
	}

	metrics.RootCoordIDAllocCounter.Add(float64(in.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: merr.Status(nil),
		ID:     start,
		Count:  in.Count,
	}, nil
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Core) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if code, ok := c.checkHealthy(); !ok {
		log.Warn("failed to updateTimeTick because rootcoord is not healthy", zap.Any("state", code))
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}
	if in.Base.MsgType != commonpb.MsgType_TimeTick {
		log.Warn("failed to updateTimeTick because base messasge is not timetick, state", zap.Any("base message type", in.Base.MsgType))
		return merr.Status(merr.WrapErrParameterInvalid(commonpb.MsgType_TimeTick.String(), in.Base.MsgType.String(), "invalid message type")), nil
	}
	err := c.chanTimeTick.updateTimeTick(in, "gRPC")
	if err != nil {
		log.Warn("failed to updateTimeTick",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Status(nil), nil
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Core) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}
	err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, in)
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Status(nil), nil
}

// ShowConfigurations returns the configurations of RootCoord matching req.Pattern
func (c *Core) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(merr.WrapErrServiceNotReady(code.String())),
			Configuations: nil,
		}, nil
	}

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("rootcoord", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}, nil
}

// GetMetrics get metrics
func (c *Core) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(merr.WrapErrServiceNotReady(code.String())),
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(in.Request)
	if err != nil {
		log.Warn("ParseMetricType failed", zap.String("role", typeutil.RootCoordRole),
			zap.Int64("nodeID", c.session.ServerID), zap.String("req", in.Request), zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := c.metricsCacheManager.GetSystemInfoMetrics()
		if err != nil {
			metrics, err = c.getSystemInfoMetrics(ctx, in)
		}

		if err != nil {
			log.Warn("GetSystemInfoMetrics failed",
				zap.String("role", typeutil.RootCoordRole),
				zap.String("metricType", metricType),
				zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status:   merr.Status(err),
				Response: "",
			}, nil
		}

		c.metricsCacheManager.UpdateSystemInfoMetrics(metrics)
		return metrics, err
	}

	log.RatedWarn(60, "GetMetrics failed, metric type not implemented", zap.String("role", typeutil.RootCoordRole),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status:   merr.Status(merr.WrapErrMetricNotFound(metricType)),
		Response: "",
	}, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("CreateAlias")

	log.Ctx(ctx).Info("received request to create alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.String("collection", in.GetCollectionName()))

	t := &createAliasTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to create alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.String("collection", in.GetCollectionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to create alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.String("collection", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("CreateAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("CreateAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("CreateAlias").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to create alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.String("collection", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DropAlias")

	log.Ctx(ctx).Info("received request to drop alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()))

	t := &dropAliasTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to drop alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to drop alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DropAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DropAlias").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to drop alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DropAlias", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("AlterAlias")

	log.Ctx(ctx).Info("received request to alter alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.String("collection", in.GetCollectionName()))

	t := &alterAliasTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Ctx(ctx).Info("failed to enqueue request to alter alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.String("collection", in.GetCollectionName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to alter alias",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("alias", in.GetAlias()),
			zap.String("collection", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterAlias", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("AlterAlias").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("AlterAlias").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to alter alias",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("alias", in.GetAlias()),
		zap.String("collection", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

// Import imports large files (json, numpy, etc.) on MinIO/S3 storage into Milvus storage.
func (c *Core) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ImportResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	// Get collection/partition ID from collection/partition name.
	var colInfo *model.Collection
	var err error
	if colInfo, err = c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp); err != nil {
		log.Error("failed to find collection ID from its name",
			zap.String("collection name", req.GetCollectionName()),
			zap.Error(err))
		return nil, err
	}

	isBackUp := importutil.IsBackup(req.GetOptions())
	if isBackUp {
		if len(req.GetPartitionName()) == 0 {
			log.Info("partition name not specified when backup recovery",
				zap.String("collection name", req.GetCollectionName()))
			ret := &milvuspb.ImportResponse{
				Status: failStatus(commonpb.ErrorCode_UnexpectedError,
					"partition name not specified when backup"),
			}
			return ret, nil
		}
	}

	cID := colInfo.CollectionID
	req.ChannelNames = c.meta.GetCollectionVirtualChannels(cID)

	hasPartitionKey := false
	for _, field := range colInfo.Fields {
		if field.IsPartitionKey {
			hasPartitionKey = true
			break
		}
	}
	var pID UniqueID
	if !hasPartitionKey {
		if req.GetPartitionName() == "" {
			req.PartitionName = Params.CommonCfg.DefaultPartitionName.GetValue()
		}
		if pID, err = c.meta.GetPartitionByName(cID, req.GetPartitionName(), typeutil.MaxTimestamp); err != nil {
			log.Error("failed to get partition ID from its name",
				zap.String("partition name", req.GetPartitionName()),
				zap.Error(err))
			return nil, err
		}
	}

	log.Info("RootCoord receive import request",
		zap.String("collection name", req.GetCollectionName()),
		zap.Int64("collection ID", cID),
		zap.String("partition name", req.GetPartitionName()),
		zap.Strings("virtual channel names", req.GetChannelNames()),
		zap.Int64("partition ID", pID),
		zap.Int("# of files = ", len(req.GetFiles())),
	)
	importJobResp := c.importManager.importJob(ctx, req, cID, pID)
	return importJobResp, nil
}

// GetImportState returns the current state of an import task.
func (c *Core) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.GetImportStateResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}
	return c.importManager.getTaskState(req.GetTask()), nil
}

// ListImportTasks returns id array of all import tasks.
func (c *Core) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	if code, ok := c.checkHealthy(); !ok {
		return &milvuspb.ListImportTasksResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady(code.String())),
		}, nil
	}

	colID := int64(-1)
	collectionName := req.GetCollectionName()
	if len(collectionName) != 0 {
		// if the collection name is specified but not found, user may input a wrong name, the collection doesn't exist or has been dropped.
		// we will return error to notify user the name is incorrect.
		colInfo, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
		if err != nil {
			err = fmt.Errorf("failed to find collection ID from its name: '%s', error: %w", req.GetCollectionName(), err)
			log.Error("ListImportTasks failed", zap.Error(err))
			status := merr.Status(err)
			status.ErrorCode = commonpb.ErrorCode_IllegalCollectionName
			return &milvuspb.ListImportTasksResponse{
				Status: status,
			}, nil
		}
		colID = colInfo.CollectionID
	}

	// if the collection name is not specified, the colID is -1, listAllTasks will return all tasks
	tasks, err := c.importManager.listAllTasks(colID, req.GetLimit())
	if err != nil {
		err = fmt.Errorf("failed to list import tasks, collection name: '%s', error: %w", req.GetCollectionName(), err)
		log.Error("ListImportTasks failed", zap.Error(err))
		return &milvuspb.ListImportTasksResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.ListImportTasksResponse{
		Status: merr.Status(nil),
		Tasks:  tasks,
	}
	return resp, nil
}

// ReportImport reports import task state to RootCoord.
func (c *Core) ReportImport(ctx context.Context, ir *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	log.Info("RootCoord receive import state report",
		zap.Int64("task ID", ir.GetTaskId()),
		zap.Any("import state", ir.GetState()))
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	// This method update a busy node to idle node, and send import task to idle node
	resendTaskFunc := func() {
		func() {
			c.importManager.busyNodesLock.Lock()
			defer c.importManager.busyNodesLock.Unlock()
			delete(c.importManager.busyNodes, ir.GetDatanodeId())
			log.Info("a DataNode is no longer busy after processing task",
				zap.Int64("dataNode ID", ir.GetDatanodeId()),
				zap.Int64("task ID", ir.GetTaskId()))

		}()
		err := c.importManager.sendOutTasks(c.importManager.ctx)
		if err != nil {
			log.Error("fail to send out import task to datanodes")
		}
	}

	// If setting ImportState_ImportCompleted, simply update the state and return directly.
	if ir.GetState() == commonpb.ImportState_ImportCompleted {
		log.Warn("this should not be called!")
	}
	// Upon receiving ReportImport request, update the related task's state in task store.
	ti, err := c.importManager.updateTaskInfo(ir)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UpdateImportTaskFailure,
			Reason:    err.Error(),
			Code:      merr.Code(err),
		}, nil
	}

	// If task failed, send task to idle datanode
	if ir.GetState() == commonpb.ImportState_ImportFailed {
		// When a DataNode failed importing, remove this DataNode from the busy node list and send out import tasks again.
		log.Info("an import task has failed, marking DataNode available and resending import task",
			zap.Int64("task ID", ir.GetTaskId()))
		resendTaskFunc()
	} else if ir.GetState() == commonpb.ImportState_ImportCompleted {
		// When a DataNode completes importing, remove this DataNode from the busy node list and send out import tasks again.
		log.Info("an import task has completed, marking DataNode available and resending import task",
			zap.Int64("task ID", ir.GetTaskId()))
		resendTaskFunc()
	} else if ir.GetState() == commonpb.ImportState_ImportPersisted {
		// Here ir.GetState() == commonpb.ImportState_ImportPersisted
		// Seal these import segments, so they can be auto-flushed later.
		log.Info("an import task turns to persisted state, flush segments to be sealed",
			zap.Any("task ID", ir.GetTaskId()), zap.Any("segments", ir.GetSegments()))
		if err := c.broker.Flush(ctx, ti.GetCollectionId(), ir.GetSegments()); err != nil {
			log.Error("failed to call Flush on bulk insert segments",
				zap.Int64("task ID", ir.GetTaskId()))
			return merr.Status(err), nil
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// ExpireCredCache will call invalidate credential cache
func (c *Core) ExpireCredCache(ctx context.Context, username string) error {
	req := proxypb.InvalidateCredCacheRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0), //TODO, msg type
			commonpbutil.WithMsgID(0),   //TODO, msg id
			commonpbutil.WithSourceID(c.session.ServerID),
		),
		Username: username,
	}
	return c.proxyClientManager.InvalidateCredentialCache(ctx, &req)
}

// UpdateCredCache will call update credential cache
func (c *Core) UpdateCredCache(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	req := proxypb.UpdateCredCacheRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0), //TODO, msg type
			commonpbutil.WithMsgID(0),   //TODO, msg id
			commonpbutil.WithSourceID(c.session.ServerID),
		),
		Username: credInfo.Username,
		Password: credInfo.Sha256Password,
	}
	return c.proxyClientManager.UpdateCredentialCache(ctx, &req)
}

// CreateCredential create new user and password
//  1. decode ciphertext password to raw password
//  2. encrypt raw password
//  3. save in to etcd
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

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_CreateCredentialFailure
		return status, nil
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
	return merr.Status(nil), nil
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

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_GetCredentialFailure
		return &rootcoordpb.GetCredentialResponse{
			Status: status,
		}, err
	}
	log.Debug("GetCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &rootcoordpb.GetCredentialResponse{
		Status:   merr.Status(nil),
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

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_UpdateCredentialFailure
		return status, nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		log.Error("UpdateCredential update cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", credInfo.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_UpdateCredentialFailure
		return status, nil
	}
	log.Debug("UpdateCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", credInfo.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Status(nil), nil
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

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_DeleteCredentialFailure
		return status, nil
	}
	// invalidate proxy's local cache
	err = c.ExpireCredCache(ctx, in.Username)
	if err != nil {
		log.Error("DeleteCredential expire credential cache failed", zap.String("role", typeutil.RootCoordRole),
			zap.String("username", in.Username), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_DeleteCredentialFailure
		return status, nil
	}
	log.Debug("DeleteCredential success", zap.String("role", typeutil.RootCoordRole),
		zap.String("username", in.Username))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Dec()
	return merr.Status(nil), nil
}

// ListCredUsers list all usernames
func (c *Core) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	method := "ListCredUsers"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	credInfo, err := c.meta.ListCredentialUsernames()
	if err != nil {
		log.Ctx(ctx).Error("ListCredUsers query usernames failed",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()

		status := merr.Status(err)
		status.ErrorCode = commonpb.ErrorCode_ListCredUsersFailure
		return &milvuspb.ListCredUsersResponse{Status: status}, nil
	}
	log.Debug("ListCredUsers success", zap.String("role", typeutil.RootCoordRole))

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Status(nil),
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
		return failStatus(commonpb.ErrorCode_CreateRoleFailure, fmt.Sprintf("%s, error: %s", errMsg, err.Error())), nil
	}

	logger.Debug(method+" success", zap.String("role_name", entity.Name))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Inc()

	return merr.Status(nil), nil
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
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
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
		errMsg := "fail to find the role by role name, maybe the role isn't existed or internal system error"
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
	return merr.Status(nil), nil
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
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		log.Error(errMsg, zap.Any("in", in), zap.Error(err))
		return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
	}
	if in.Type != milvuspb.OperateUserRoleType_RemoveUserFromRole {
		if _, err := c.meta.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, false); err != nil {
			errMsg := "not found the user, maybe the user isn't existed or internal system error"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperateUserRoleFailure, errMsg), nil
		}
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
	return merr.Status(nil), nil
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
					Status: merr.Status(nil),
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
		Status:  merr.Status(nil),
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
					Status: merr.Status(nil),
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
		Status:  merr.Status(nil),
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
		log.Warn("fail to select the role", zap.String("role_name", entity.Name), zap.Error(err))
		return errors.New("not found the role, maybe the role isn't existed or internal system error")
	}
	return nil
}

func (c *Core) isValidObject(entity *milvuspb.ObjectEntity) error {
	if entity == nil {
		return errors.New("the object entity is nil")
	}
	if _, ok := commonpb.ObjectType_value[entity.Name]; !ok {
		return fmt.Errorf("not found the object type[name: %s], supported the object types: %v", entity.Name, lo.Keys(commonpb.ObjectType_value))
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
		log.Warn("fail to select the user", zap.String("username", entity.User.Name), zap.Error(err))
		return errors.New("not found the user, maybe the user isn't existed or internal system error")
	}
	if entity.Privilege == nil {
		return errors.New("the privilege entity in the grantor entity is nil")
	}
	if util.IsAnyWord(entity.Privilege.Name) {
		return nil
	}
	if privilegeName := util.PrivilegeNameForMetastore(entity.Privilege.Name); privilegeName == "" {
		return fmt.Errorf("not found the privilege name[%s]", entity.Privilege.Name)
	}
	privileges, ok := util.ObjectPrivileges[object]
	if !ok {
		return fmt.Errorf("not found the object type[name: %s], supported the object types: %v", object, lo.Keys(commonpb.ObjectType_value))
	}
	for _, privilege := range privileges {
		if privilege == entity.Privilege.Name {
			return nil
		}
	}
	return fmt.Errorf("not found the privilege name[%s]", entity.Privilege.Name)
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
			OpKey:  funcutil.PolicyForPrivilege(in.Entity.Role.Name, in.Entity.Object.Name, in.Entity.ObjectName, in.Entity.Grantor.Privilege.Name, in.Entity.DbName),
		}); err != nil {
			errMsg := "fail to refresh policy info cache"
			log.Error(errMsg, zap.Any("in", in), zap.Error(err))
			return failStatus(commonpb.ErrorCode_OperatePrivilegeFailure, errMsg), nil
		}
	}

	logger.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Status(nil), nil
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
			Status: merr.Status(nil),
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
		Status:   merr.Status(nil),
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
		Status:      merr.Status(nil),
		PolicyInfos: policies,
		UserRoles:   userRoles,
	}, nil
}

func (c *Core) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	if code, ok := c.checkHealthy(); !ok {
		return merr.Status(merr.WrapErrServiceNotReady(code.String())), nil
	}

	log := log.Ctx(ctx).With(zap.String("oldCollectionName", req.GetOldName()), zap.String("newCollectionName", req.GetNewName()))
	log.Info("received request to rename collection")

	metrics.RootCoordDDLReqCounter.WithLabelValues("RenameCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("RenameCollection")
	t := &renameCollectionTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Warn("failed to enqueue request to rename collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("RenameCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to rename collection", zap.Uint64("ts", t.GetTs()), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("RenameCollection", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("RenameCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("RenameCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to rename collection", zap.Uint64("ts", t.GetTs()))
	return merr.Status(nil), nil
}

func (c *Core) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if _, ok := c.checkHealthy(); !ok {
		reason := errorutil.UnHealthReason("rootcoord", c.session.ServerID, "rootcoord is unhealthy")
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: []string{reason}}, nil
	}

	mu := &sync.Mutex{}
	group, ctx := errgroup.WithContext(ctx)
	errReasons := make([]string, 0, len(c.proxyClientManager.proxyClient))

	for nodeID, proxyClient := range c.proxyClientManager.proxyClient {
		nodeID := nodeID
		proxyClient := proxyClient
		group.Go(func() error {
			sta, err := proxyClient.GetComponentStates(ctx)
			isHealthy, reason := errorutil.UnHealthReasonWithComponentStatesOrErr("proxy", nodeID, sta, err)
			if !isHealthy {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, reason)
			}
			return err
		})
	}

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: errReasons}, nil
	}

	return &milvuspb.CheckHealthResponse{IsHealthy: true, Reasons: errReasons}, nil
}
