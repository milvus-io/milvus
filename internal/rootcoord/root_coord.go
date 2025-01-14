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
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	streamingcoord "github.com/milvus-io/milvus/internal/streamingcoord/server"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	tso2 "github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	tsoutil2 "github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
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

type metaKVCreator func() kv.MetaKv

// Core root coordinator core
type Core struct {
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	etcdCli          *clientv3.Client
	tikvCli          *txnkv.Client
	address          string
	meta             IMetaTable
	scheduler        IScheduler
	broker           Broker
	ddlTsLockManager DdlTsLockManager
	garbageCollector GarbageCollector
	stepExecutor     StepExecutor

	metaKVCreator metaKVCreator

	proxyCreator       proxyutil.ProxyCreator
	proxyWatcher       *proxyutil.ProxyWatcher
	proxyClientManager proxyutil.ProxyClientManagerInterface

	metricsCacheManager *metricsinfo.MetricsCacheManager

	chanTimeTick *timetickSync

	idAllocator  allocator.Interface
	tsoAllocator tso2.Allocator

	dataCoord  types.DataCoordClient
	queryCoord types.QueryCoordClient

	quotaCenter *QuotaCenter

	stateCode atomic.Int32
	initOnce  sync.Once
	startOnce sync.Once
	session   *sessionutil.Session

	factory dependency.Factory

	enableActiveStandBy bool
	activateFunc        func() error

	metricsRequest *metricsinfo.MetricsRequest

	streamingCoord *streamingcoord.Server
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
		metricsRequest:      metricsinfo.NewMetricsRequest(),
	}

	core.UpdateStateCode(commonpb.StateCode_Abnormal)
	core.SetProxyCreator(proxyutil.DefaultProxyCreator)

	expr.Register("rootcoord", core)
	return core, nil
}

// UpdateStateCode update state code
func (c *Core) UpdateStateCode(code commonpb.StateCode) {
	c.stateCode.Store(int32(code))
	log.Ctx(c.ctx).Info("update rootcoord state", zap.String("state", code.String()))
}

func (c *Core) GetStateCode() commonpb.StateCode {
	return commonpb.StateCode(c.stateCode.Load())
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
	if !paramtable.Get().CommonCfg.TTMsgEnabled.GetAsBool() {
		return
	}
	log := log.Ctx(c.ctx)
	code := c.GetStateCode()
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
	log := log.Ctx(c.ctx)
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
	log := log.Ctx(c.ctx)
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

func (c *Core) SetProxyCreator(f func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error)) {
	c.proxyCreator = f
}

func (c *Core) SetDataCoordClient(s types.DataCoordClient) error {
	if s == nil {
		return errors.New("null DataCoord interface")
	}
	c.dataCoord = s
	return nil
}

func (c *Core) SetQueryCoordClient(s types.QueryCoordClient) error {
	if s == nil {
		return errors.New("null QueryCoord interface")
	}
	c.queryCoord = s
	return nil
}

// Register register rootcoord at etcd
func (c *Core) Register() error {
	log := log.Ctx(c.ctx)
	c.session.Register()
	afterRegister := func() {
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.RootCoordRole).Inc()
		log.Info("RootCoord Register Finished")
		c.session.LivenessCheck(c.ctx, func() {
			log.Error("Root Coord disconnected from etcd, process will exit", zap.Int64("Server Id", c.session.ServerID))
			os.Exit(1)
		})
	}
	if c.enableActiveStandBy {
		go func() {
			if err := c.session.ProcessActiveStandBy(c.activateFunc); err != nil {
				log.Warn("failed to activate standby rootcoord server", zap.Error(err))
				panic(err)
			}
			afterRegister()
		}()
	} else {
		afterRegister()
	}

	return nil
}

func (c *Core) SetAddress(address string) {
	c.address = address
}

// SetEtcdClient sets the etcdCli of Core
func (c *Core) SetEtcdClient(etcdClient *clientv3.Client) {
	c.etcdCli = etcdClient
}

// SetTiKVClient sets the tikvCli of Core
func (c *Core) SetTiKVClient(client *txnkv.Client) {
	c.tikvCli = client
}

func (c *Core) initSession() error {
	c.session = sessionutil.NewSession(c.ctx)
	if c.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	c.session.Init(typeutil.RootCoordRole, c.address, true, true)
	c.session.SetEnableActiveStandBy(c.enableActiveStandBy)
	return nil
}

func (c *Core) initKVCreator() {
	if c.metaKVCreator == nil {
		if Params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
			c.metaKVCreator = func() kv.MetaKv {
				return tikv.NewTiKV(c.tikvCli, Params.TiKVCfg.MetaRootPath.GetValue(),
					tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
			}
		} else {
			c.metaKVCreator = func() kv.MetaKv {
				return etcdkv.NewEtcdKV(c.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
					etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
			}
		}
	}
}

func (c *Core) initStreamingCoord() {
	c.streamingCoord = streamingcoord.NewServerBuilder().
		WithETCD(c.etcdCli).
		WithMetaKV(c.metaKVCreator()).
		WithSession(c.session).
		WithRootCoordClient(coordclient.MustGetLocalRootCoordClientFuture()).
		Build()
}

func (c *Core) initMetaTable(initCtx context.Context) error {
	fn := func() error {
		var catalog metastore.RootCoordCatalog
		var err error

		switch Params.MetaStoreCfg.MetaStoreType.GetValue() {
		case util.MetaStoreTypeEtcd:
			log.Ctx(initCtx).Info("Using etcd as meta storage.")
			var ss *kvmetestore.SuffixSnapshot
			var err error

			metaKV := c.metaKVCreator()
			if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, kvmetestore.SnapshotsSep, Params.EtcdCfg.MetaRootPath.GetValue(), kvmetestore.SnapshotPrefix); err != nil {
				return err
			}
			catalog = &kvmetestore.Catalog{Txn: metaKV, Snapshot: ss}
		case util.MetaStoreTypeTiKV:
			log.Ctx(initCtx).Info("Using tikv as meta storage.")
			var ss *kvmetestore.SuffixSnapshot
			var err error

			metaKV := c.metaKVCreator()
			if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, kvmetestore.SnapshotsSep, Params.TiKVCfg.MetaRootPath.GetValue(), kvmetestore.SnapshotPrefix); err != nil {
				return err
			}
			catalog = &kvmetestore.Catalog{Txn: metaKV, Snapshot: ss}
		default:
			return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", Params.MetaStoreCfg.MetaStoreType.GetValue()))
		}

		if c.meta, err = NewMetaTable(c.ctx, catalog, c.tsoAllocator); err != nil {
			return err
		}

		return nil
	}

	return retry.Do(initCtx, fn, retry.Attempts(10))
}

func (c *Core) initIDAllocator(initCtx context.Context) error {
	var tsoKV kv.TxnKV
	var kvPath string
	if Params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		kvPath = Params.TiKVCfg.KvRootPath.GetValue()
		tsoKV = tsoutil2.NewTSOTiKVBase(c.tikvCli, kvPath, globalIDAllocatorSubPath)
	} else {
		kvPath = Params.EtcdCfg.KvRootPath.GetValue()
		tsoKV = tsoutil2.NewTSOKVBase(c.etcdCli, kvPath, globalIDAllocatorSubPath)
	}
	idAllocator := allocator.NewGlobalIDAllocator(globalIDAllocatorKey, tsoKV)
	if err := idAllocator.Initialize(); err != nil {
		return err
	}
	c.idAllocator = idAllocator

	log.Ctx(initCtx).Info("id allocator initialized",
		zap.String("root_path", kvPath),
		zap.String("sub_path", globalIDAllocatorSubPath),
		zap.String("key", globalIDAllocatorKey))

	return nil
}

func (c *Core) initTSOAllocator(initCtx context.Context) error {
	var tsoKV kv.TxnKV
	var kvPath string
	if Params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		kvPath = Params.TiKVCfg.KvRootPath.GetValue()
		tsoKV = tsoutil2.NewTSOTiKVBase(c.tikvCli, Params.TiKVCfg.KvRootPath.GetValue(), globalIDAllocatorSubPath)
	} else {
		kvPath = Params.EtcdCfg.KvRootPath.GetValue()
		tsoKV = tsoutil2.NewTSOKVBase(c.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), globalIDAllocatorSubPath)
	}
	tsoAllocator := tso2.NewGlobalTSOAllocator(globalTSOAllocatorKey, tsoKV)
	if err := tsoAllocator.Initialize(); err != nil {
		return err
	}
	c.tsoAllocator = tsoAllocator

	log.Ctx(initCtx).Info("tso allocator initialized",
		zap.String("root_path", kvPath),
		zap.String("sub_path", globalIDAllocatorSubPath),
		zap.String("key", globalIDAllocatorKey))

	return nil
}

func (c *Core) initInternal() error {
	initCtx, initSpan := log.NewIntentContext(typeutil.RootCoordRole, "initInternal")
	defer initSpan.End()
	log := log.Ctx(initCtx)

	c.UpdateStateCode(commonpb.StateCode_Initializing)

	if err := c.initIDAllocator(initCtx); err != nil {
		return err
	}

	if err := c.initTSOAllocator(initCtx); err != nil {
		return err
	}

	if err := c.initMetaTable(initCtx); err != nil {
		return err
	}

	c.scheduler = newScheduler(c.ctx, c.idAllocator, c.tsoAllocator)

	c.factory.Init(Params)
	chanMap := c.meta.ListCollectionPhysicalChannels(c.ctx)
	c.chanTimeTick = newTimeTickSync(initCtx, c.ctx, c.session.ServerID, c.factory, chanMap)
	log.Info("create TimeTick sync done")

	c.proxyClientManager = proxyutil.NewProxyClientManager(c.proxyCreator)

	c.broker = newServerBroker(c)
	c.ddlTsLockManager = newDdlTsLockManager(c.tsoAllocator)
	c.garbageCollector = newBgGarbageCollector(c)
	c.stepExecutor = newBgStepExecutor(c.ctx)

	if err := c.streamingCoord.Start(c.ctx); err != nil {
		log.Info("start streaming coord failed", zap.Error(err))
		return err
	}
	if !streamingutil.IsStreamingServiceEnabled() {
		c.proxyWatcher = proxyutil.NewProxyWatcher(
			c.etcdCli,
			c.chanTimeTick.initSessions,
			c.proxyClientManager.AddProxyClients,
		)
		c.proxyWatcher.AddSessionFunc(c.chanTimeTick.addSession, c.proxyClientManager.AddProxyClient)
		c.proxyWatcher.DelSessionFunc(c.chanTimeTick.delSession, c.proxyClientManager.DelProxyClient)
	} else {
		c.proxyWatcher = proxyutil.NewProxyWatcher(
			c.etcdCli,
			c.proxyClientManager.AddProxyClients,
		)
		c.proxyWatcher.AddSessionFunc(c.proxyClientManager.AddProxyClient)
		c.proxyWatcher.DelSessionFunc(c.proxyClientManager.DelProxyClient)
	}
	log.Info("init proxy manager done")

	c.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	c.quotaCenter = NewQuotaCenter(c.proxyClientManager, c.queryCoord, c.dataCoord, c.tsoAllocator, c.meta)
	log.Debug("RootCoord init QuotaCenter done")

	if err := c.initCredentials(initCtx); err != nil {
		return err
	}
	log.Info("init credentials done")

	if err := c.initRbac(initCtx); err != nil {
		return err
	}

	log.Info("init rootcoord done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", c.address))
	return nil
}

func (c *Core) registerMetricsRequest() {
	c.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return c.getSystemInfoMetrics(ctx, req)
		})
	log.Ctx(c.ctx).Info("register metrics actions finished")
}

// Init initialize routine
func (c *Core) Init() error {
	log := log.Ctx(c.ctx)
	var initError error
	c.registerMetricsRequest()
	c.factory.Init(Params)
	if err := c.initSession(); err != nil {
		return err
	}
	c.initKVCreator()
	c.initStreamingCoord()

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
			log.Info("RootCoord startup success", zap.String("address", c.session.Address))
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

func (c *Core) initCredentials(initCtx context.Context) error {
	credInfo, _ := c.meta.GetCredential(initCtx, util.UserRoot)
	if credInfo == nil {
		encryptedRootPassword, err := crypto.PasswordEncrypt(Params.CommonCfg.DefaultRootPassword.GetValue())
		if err != nil {
			log.Ctx(initCtx).Warn("RootCoord init user root failed", zap.Error(err))
			return err
		}
		log.Ctx(initCtx).Info("RootCoord init user root")
		err = c.meta.AddCredential(initCtx, &internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword})
		return err
	}
	return nil
}

func (c *Core) initRbac(initCtx context.Context) error {
	var err error
	// create default roles, including admin, public
	for _, role := range util.DefaultRoles {
		err = c.meta.CreateRole(initCtx, util.DefaultTenant, &milvuspb.RoleEntity{Name: role})
		if err != nil && !common.IsIgnorableError(err) {
			return errors.Wrap(err, "failed to create role")
		}
	}

	if Params.ProxyCfg.EnablePublicPrivilege.GetAsBool() {
		err = c.initPublicRolePrivilege(initCtx)
		if err != nil {
			return err
		}
	}

	if Params.RoleCfg.Enabled.GetAsBool() {
		return c.initBuiltinRoles()
	}
	return nil
}

func (c *Core) initPublicRolePrivilege(initCtx context.Context) error {
	// grant privileges for the public role
	globalPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeListAliases.String(),
	}
	collectionPrivileges := []string{
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
	}

	var err error
	for _, globalPrivilege := range globalPrivileges {
		err = c.meta.OperatePrivilege(initCtx, util.DefaultTenant, &milvuspb.GrantEntity{
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
		err = c.meta.OperatePrivilege(initCtx, util.DefaultTenant, &milvuspb.GrantEntity{
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

func (c *Core) initBuiltinRoles() error {
	log := log.Ctx(c.ctx)
	rolePrivilegesMap := Params.RoleCfg.Roles.GetAsRoleDetails()
	for role, privilegesJSON := range rolePrivilegesMap {
		err := c.meta.CreateRole(c.ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: role})
		if err != nil && !common.IsIgnorableError(err) {
			log.Error("create a builtin role fail", zap.String("roleName", role), zap.Error(err))
			return errors.Wrapf(err, "failed to create a builtin role: %s", role)
		}
		for _, privilege := range privilegesJSON[util.RoleConfigPrivileges] {
			privilegeName := privilege[util.RoleConfigPrivilege]
			if !util.IsAnyWord(privilege[util.RoleConfigPrivilege]) {
				dbPrivName, err := c.getMetastorePrivilegeName(c.ctx, privilege[util.RoleConfigPrivilege])
				if err != nil {
					return errors.Wrapf(err, "failed to get metastore privilege name for: %s", privilege[util.RoleConfigPrivilege])
				}
				privilegeName = dbPrivName
			}
			err := c.meta.OperatePrivilege(c.ctx, util.DefaultTenant, &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: role},
				Object:     &milvuspb.ObjectEntity{Name: privilege[util.RoleConfigObjectType]},
				ObjectName: privilege[util.RoleConfigObjectName],
				DbName:     privilege[util.RoleConfigDBName],
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: util.UserRoot},
					Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				},
			}, milvuspb.OperatePrivilegeType_Grant)
			if err != nil && !common.IsIgnorableError(err) {
				log.Error("grant privilege to builtin role fail", zap.String("roleName", role), zap.Any("privilege", privilege), zap.Error(err))
				return errors.Wrapf(err, "failed to grant privilege: <%s, %s, %s> of db: %s to role: %s", privilege[util.RoleConfigObjectType], privilege[util.RoleConfigObjectName], privilege[util.RoleConfigPrivilege], privilege[util.RoleConfigDBName], role)
			}
		}
		util.BuiltinRoles = append(util.BuiltinRoles, role)
		log.Info("init a builtin role successfully", zap.String("roleName", role))
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
			ts, err := c.tsoAllocator.GenerateTSO(1)
			if err != nil {
				return err
			}
			if coll.Available() {
				for _, part := range coll.Partitions {
					switch part.State {
					case pb.PartitionState_PartitionDropping:
						go c.garbageCollector.ReDropPartition(coll.DBID, coll.PhysicalChannelNames, coll.VirtualChannelNames, part.Clone(), ts)
					case pb.PartitionState_PartitionCreating:
						go c.garbageCollector.RemoveCreatingPartition(coll.DBID, part.Clone(), ts)
					default:
					}
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
	return nil
}

func (c *Core) startInternal() error {
	log := log.Ctx(c.ctx)
	if err := c.proxyWatcher.WatchProxy(c.ctx); err != nil {
		log.Fatal("rootcoord failed to watch proxy", zap.Error(err))
		// you can not just stuck here,
		panic(err)
	}

	if err := c.restore(c.ctx); err != nil {
		panic(err)
	}

	if Params.QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		c.quotaCenter.Start()
	}

	c.scheduler.Start()
	c.stepExecutor.Start()
	go func() {
		// refresh rbac cache
		if err := retry.Do(c.ctx, func() error {
			if err := c.proxyClientManager.RefreshPolicyInfoCache(c.ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: int32(typeutil.CacheRefresh),
			}); err != nil {
				log.RatedWarn(60, "fail to refresh policy info cache", zap.Error(err))
				return err
			}
			return nil
		}, retry.Attempts(100), retry.Sleep(time.Second)); err != nil {
			log.Warn("fail to refresh policy info cache", zap.Error(err))
		}
	}()

	c.startServerLoop()
	c.UpdateStateCode(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.RootCoordRole, c.session.ServerID)
	log.Info("rootcoord startup successfully")

	// regster the core as a appendoperator for broadcast service.
	// TODO: should be removed at 2.6.0.
	// Add the wal accesser to the broadcaster registry for making broadcast operation.
	registry.Register(registry.AppendOperatorTypeMsgstream, newMsgStreamAppendOperator(c))
	return nil
}

func (c *Core) startServerLoop() {
	c.wg.Add(1)
	go c.tsLoop()
	if !streamingutil.IsStreamingServiceEnabled() {
		c.wg.Add(2)
		go c.startTimeTickLoop()
		go c.chanTimeTick.startWatch(&c.wg)
	}
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
		log.Ctx(c.ctx).Info("stop rootcoord executor")
	}
}

func (c *Core) stopScheduler() {
	if c.scheduler != nil {
		c.scheduler.Stop()
		log.Ctx(c.ctx).Info("stop rootcoord scheduler")
	}
}

func (c *Core) cancelIfNotNil() {
	if c.cancel != nil {
		c.cancel()
		log.Ctx(c.ctx).Info("cancel rootcoord goroutines")
	}
}

func (c *Core) revokeSession() {
	if c.session != nil {
		// wait at most one second to revoke
		c.session.Stop()
		log.Ctx(c.ctx).Info("rootcoord session stop")
	}
}

// Stop stops rootCoord.
func (c *Core) Stop() error {
	c.UpdateStateCode(commonpb.StateCode_Abnormal)
	c.stopExecutor()
	c.stopScheduler()

	if c.streamingCoord != nil {
		c.streamingCoord.Stop()
	}
	if c.proxyWatcher != nil {
		c.proxyWatcher.Stop()
	}
	if c.quotaCenter != nil {
		c.quotaCenter.stop()
	}

	c.revokeSession()
	c.cancelIfNotNil()
	c.wg.Wait()
	return nil
}

// GetComponentStates get states of components
func (c *Core) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	code := c.GetStateCode()
	log.Ctx(ctx).Debug("RootCoord current state", zap.String("StateCode", code.String()))

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
		Status: merr.Success(),
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
func (c *Core) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.RootCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (c *Core) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.RootCoordStatistics.GetValue(),
	}, nil
}

func (c *Core) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to create database",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Ctx(ctx).Info("done to create database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

func (c *Core) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Ctx(ctx).Info("failed to drop database", zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("dbName", in.GetDbName()),
			zap.Int64("msgID", in.GetBase().GetMsgID()), zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.CleanupRootCoordDBMetrics(in.GetDbName())
	log.Ctx(ctx).Info("done to drop database", zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()), zap.Int64("msgID", in.GetBase().GetMsgID()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

func (c *Core) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		ret := &milvuspb.ListDatabasesResponse{Status: merr.Status(err)}
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
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Info("failed to list databases", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &milvuspb.ListDatabasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Info("done to list databases", zap.Int("num of databases", len(t.Resp.GetDbNames())))
	return t.Resp, nil
}

// CreateCollection create collection
func (c *Core) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("CreateCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to create collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

// DropCollection drop collection
func (c *Core) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DropCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to drop collection", zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

// HasCollection check collection existence
func (c *Core) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasCollection")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("collectionName", in.GetCollectionName()),
		zap.Uint64("ts", ts))

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

	return t.Rsp, nil
}

// getCollectionIDStr get collectionID string to avoid the alias name
func (c *Core) getCollectionIDStr(ctx context.Context, db, collectionName string, collectionID int64) string {
	// When neither the collection name nor the collectionID exists, no error is returned at this point.
	// An error will be returned during the execution phase.
	if collectionID != 0 {
		return strconv.FormatInt(collectionID, 10)
	}

	coll, err := c.meta.GetCollectionByName(ctx, db, collectionName, typeutil.MaxTimestamp)
	if err != nil {
		return "-1"
	}
	return strconv.FormatInt(coll.CollectionID, 10)
}

func (c *Core) describeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, allowUnavailable bool) (*model.Collection, error) {
	ts := getTravelTs(in)
	if in.GetCollectionName() != "" {
		return c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), ts)
	}
	return c.meta.GetCollectionByID(ctx, in.GetDbName(), in.GetCollectionID(), ts, allowUnavailable)
}

func convertModelToDesc(collInfo *model.Collection, aliases []string, dbName string) *milvuspb.DescribeCollectionResponse {
	resp := &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		DbName: dbName,
	}

	resp.Schema = &schemapb.CollectionSchema{
		Name:               collInfo.Name,
		Description:        collInfo.Description,
		AutoID:             collInfo.AutoID,
		Fields:             model.MarshalFieldModels(collInfo.Fields),
		Functions:          model.MarshalFunctionModels(collInfo.Functions),
		EnableDynamicField: collInfo.EnableDynamicField,
		Properties:         collInfo.Properties,
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
	resp.DbId = collInfo.DBID
	return resp
}

func (c *Core) describeCollectionImpl(ctx context.Context, in *milvuspb.DescribeCollectionRequest, allowUnavailable bool) (*milvuspb.DescribeCollectionResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DescribeCollection")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("collectionName", in.GetCollectionName()),
		zap.String("dbName", in.GetDbName()),
		zap.Int64("id", in.GetCollectionID()),
		zap.Uint64("ts", ts),
		zap.Bool("allowUnavailable", allowUnavailable))

	t := &describeCollectionTask{
		baseTask:         newBaseTask(ctx, c),
		Req:              in,
		Rsp:              &milvuspb.DescribeCollectionResponse{Status: merr.Success()},
		allowUnavailable: allowUnavailable,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Info("failed to enqueue request to describe collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to describe collection", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.FailLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeCollection", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeCollection").Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("DescribeCollection").Observe(float64(t.queueDur.Milliseconds()))

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
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowCollections", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowCollections")

	ts := getTravelTs(in)
	log := log.Ctx(ctx).With(zap.String("dbname", in.GetDbName()),
		zap.Uint64("ts", ts))

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

	return t.Rsp, nil
}

func (c *Core) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollection", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("AlterCollection")

	log.Ctx(ctx).Info("received request to alter collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Any("props", in.Properties),
		zap.Any("delete_keys", in.DeleteKeys),
	)

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
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues("AlterCollection").Observe(float64(t.queueDur.Milliseconds()))

	log.Info("done to alter collection",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

func (c *Core) AlterCollectionField(ctx context.Context, in *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollectionField", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("AlterCollectionField")

	log.Ctx(ctx).Info("received request to alter collection field",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.String("fieldName", in.GetFieldName()),
		zap.Any("props", in.Properties),
	)

	t := &alterCollectionFieldTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Warn("failed to enqueue request to alter collection field",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.String("fieldName", in.GetFieldName()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollectionField", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to alter collection",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetCollectionName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollectionField", metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("AlterCollectionField", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("AlterCollectionField").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to alter collection field",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetCollectionName()),
		zap.String("fieldName", in.GetFieldName()))
	return merr.Success(), nil
}

func (c *Core) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "AlterDatabase"

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	log.Ctx(ctx).Info("received request to alter database",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetDbName()),
		zap.Any("props", in.Properties))

	t := &alterDatabaseTask{
		baseTask: newBaseTask(ctx, c),
		Req:      in,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Warn("failed to enqueue request to alter database",
			zap.String("role", typeutil.RootCoordRole),
			zap.String("name", in.GetDbName()),
			zap.Error(err))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to alter database",
			zap.String("role", typeutil.RootCoordRole),
			zap.Error(err),
			zap.String("name", in.GetDbName()),
			zap.Uint64("ts", t.GetTs()))

		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordDDLReqLatencyInQueue.WithLabelValues(method).Observe(float64(t.queueDur.Milliseconds()))

	log.Ctx(ctx).Info("done to alter database",
		zap.String("role", typeutil.RootCoordRole),
		zap.String("name", in.GetDbName()),
		zap.Uint64("ts", t.GetTs()))
	return merr.Success(), nil
}

// CreatePartition create partition
func (c *Core) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// DropPartition drop partition
func (c *Core) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// HasPartition check partition existence
func (c *Core) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("HasPartition", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("HasPartition")

	// TODO(longjiquan): why HasPartitionRequest doesn't contain Timestamp but other requests do.
	ts := typeutil.MaxTimestamp
	log := log.Ctx(ctx).With(zap.String("collection", in.GetCollectionName()),
		zap.String("partition", in.GetPartitionName()),
		zap.Uint64("ts", ts))

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

	return t.Rsp, nil
}

func (c *Core) showPartitionsImpl(ctx context.Context, in *milvuspb.ShowPartitionsRequest, allowUnavailable bool) (*milvuspb.ShowPartitionsResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("ShowPartitions", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("ShowPartitions")

	log := log.Ctx(ctx).With(zap.String("collection", in.GetCollectionName()),
		zap.Int64("collection_id", in.GetCollectionID()),
		zap.Strings("partitions", in.GetPartitionNames()),
		zap.Bool("allowUnavailable", allowUnavailable))

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
	return &milvuspb.ShowSegmentsResponse{Status: merr.Success()}, nil
}

// GetPChannelInfo get pchannel info.
func (c *Core) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.GetPChannelInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	return c.meta.GetPChannelInfo(ctx, in.GetPchannel()), nil
}

// AllocTimestamp alloc timestamp
func (c *Core) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.AllocTimestampResponse{
			Status: merr.Status(err),
		}, nil
	}

	if in.BlockTimestamp > 0 {
		blockTime, _ := tsoutil.ParseTS(in.BlockTimestamp)
		lastTime := c.tsoAllocator.GetLastSavedTime()
		deltaDuration := blockTime.Sub(lastTime)
		if deltaDuration > 0 {
			log.Info("wait for block timestamp",
				zap.Time("blockTime", blockTime),
				zap.Time("lastTime", lastTime),
				zap.Duration("delta", deltaDuration))
			time.Sleep(deltaDuration + time.Millisecond*200)
		}
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
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     in.GetCount(),
	}, nil
}

// AllocID alloc ids
func (c *Core) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.AllocIDResponse{
			Status: merr.Status(err),
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
		Status: merr.Success(),
		ID:     start,
		Count:  in.Count,
	}, nil
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Core) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		log.Warn("failed to updateTimeTick because rootcoord is not healthy", zap.Error(err))
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Core) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, in)
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

// ShowConfigurations returns the configurations of RootCoord matching req.Pattern
func (c *Core) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(err),
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
		Status:        merr.Success(),
		Configuations: configList,
	}, nil
}

// GetMetrics get metrics
func (c *Core) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}

	resp := &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, paramtable.GetNodeID()),
	}

	ret, err := c.metricsRequest.ExecuteMetricsRequest(ctx, in)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
	return resp, nil
}

// CreateAlias create collection alias
func (c *Core) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// DropAlias drop collection alias
func (c *Core) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// AlterAlias alter collection alias
func (c *Core) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// DescribeAlias describe collection alias
func (c *Core) DescribeAlias(ctx context.Context, in *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(err),
		}, nil
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("db", in.GetDbName()),
		zap.String("alias", in.GetAlias()))
	method := "DescribeAlias"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DescribeAlias")

	log.Info("received request to describe alias")

	if in.GetAlias() == "" {
		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(merr.WrapErrParameterMissing("alias", "no input alias")),
		}, nil
	}

	collectionName, err := c.meta.DescribeAlias(ctx, in.GetDbName(), in.GetAlias(), 0)
	if err != nil {
		log.Warn("fail to DescribeAlias", zap.Error(err))
		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(err),
		}, nil
	}
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to describe alias")
	return &milvuspb.DescribeAliasResponse{
		Status:     merr.Status(nil),
		DbName:     in.GetDbName(),
		Alias:      in.GetAlias(),
		Collection: collectionName,
	}, nil
}

// ListAliases list aliases
func (c *Core) ListAliases(ctx context.Context, in *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListAliasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	method := "ListAliases"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("db", in.GetDbName()),
		zap.String("collectionName", in.GetCollectionName()))
	log.Info("received request to list aliases")

	aliases, err := c.meta.ListAliases(ctx, in.GetDbName(), in.GetCollectionName(), 0)
	if err != nil {
		log.Warn("fail to ListAliases", zap.Error(err))
		return &milvuspb.ListAliasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to list aliases")
	return &milvuspb.ListAliasesResponse{
		Status:         merr.Status(nil),
		DbName:         in.GetDbName(),
		CollectionName: in.GetCollectionName(),
		Aliases:        aliases,
	}, nil
}

// ExpireCredCache will call invalidate credential cache
func (c *Core) ExpireCredCache(ctx context.Context, username string) error {
	req := proxypb.InvalidateCredCacheRequest{
		Base: commonpbutil.NewMsgBase(
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.String("username", credInfo.Username))
	ctxLog.Debug(method)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	// insert to db
	err := c.meta.AddCredential(ctx, credInfo)
	if err != nil {
		ctxLog.Warn("CreateCredential save credential failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_CreateCredentialFailure), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		ctxLog.Warn("CreateCredential add cache failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
	}
	ctxLog.Debug("CreateCredential success")

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Inc()
	return merr.Success(), nil
}

// GetCredential get credential by username
func (c *Core) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	method := "GetCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.String("username", in.Username))
	ctxLog.Debug(method)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.GetCredentialResponse{Status: merr.Status(err)}, nil
	}

	credInfo, err := c.meta.GetCredential(ctx, in.Username)
	if err != nil {
		ctxLog.Warn("GetCredential query credential failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return &rootcoordpb.GetCredentialResponse{
			Status: merr.StatusWithErrorCode(err, commonpb.ErrorCode_GetCredentialFailure),
		}, nil
	}
	ctxLog.Debug("GetCredential success")

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &rootcoordpb.GetCredentialResponse{
		Status:   merr.Success(),
		Username: credInfo.Username,
		Password: credInfo.EncryptedPassword,
	}, nil
}

// UpdateCredential update password for a user
func (c *Core) UpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) (*commonpb.Status, error) {
	method := "UpdateCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.String("username", credInfo.Username))
	ctxLog.Debug(method)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	// update data on storage
	err := c.meta.AlterCredential(ctx, credInfo)
	if err != nil {
		ctxLog.Warn("UpdateCredential save credential failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_UpdateCredentialFailure), nil
	}
	// update proxy's local cache
	err = c.UpdateCredCache(ctx, credInfo)
	if err != nil {
		ctxLog.Warn("UpdateCredential update cache failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_UpdateCredentialFailure), nil
	}
	ctxLog.Debug("UpdateCredential success")

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

// DeleteCredential delete a user
func (c *Core) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	method := "DeleteCredential"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.String("username", in.Username))
	ctxLog.Debug(method)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	var status *commonpb.Status
	defer func() {
		if status.Code != 0 {
			metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()
		}
	}()

	err := executeDeleteCredentialTaskSteps(ctx, c, in.Username)
	if err != nil {
		errMsg := "fail to execute task when deleting the user"
		ctxLog.Warn(errMsg, zap.Error(err))
		status = merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_DeleteCredentialFailure)
		return status, nil
	}
	ctxLog.Debug("DeleteCredential success")

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfCredentials.Dec()
	status = merr.Success()
	return status, nil
}

// ListCredUsers list all usernames
func (c *Core) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	method := "ListCredUsers"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole))
	ctxLog.Debug(method)
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListCredUsersResponse{Status: merr.Status(err)}, nil
	}

	credInfo, err := c.meta.ListCredentialUsernames(ctx)
	if err != nil {
		ctxLog.Warn("ListCredUsers query usernames failed", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.FailLabel).Inc()

		status := merr.Status(err)
		return &milvuspb.ListCredUsersResponse{Status: status}, nil
	}
	ctxLog.Debug("ListCredUsers success")

	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Success(),
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method + " begin")

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	entity := in.Entity

	err := c.meta.CreateRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name})
	if err != nil {
		errMsg := "fail to create role"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_CreateRoleFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Inc()

	return merr.Success(), nil
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.String("role_name", in.RoleName))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	for util.IsBuiltinRole(in.GetRoleName()) {
		err := merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be dropped", in.GetRoleName())
		return merr.Status(err), nil
	}
	if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_DropRoleFailure), nil
	}

	if !in.ForceDrop {
		grantEntities, err := c.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
			Role:   &milvuspb.RoleEntity{Name: in.RoleName},
			DbName: "*",
		})
		if len(grantEntities) != 0 {
			errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
			ctxLog.Warn(errMsg, zap.Any("grants", grantEntities), zap.Error(err))
			return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_DropRoleFailure), nil
		}
	}
	err := executeDropRoleTaskSteps(ctx, c, in.RoleName, in.ForceDrop)
	if err != nil {
		errMsg := "fail to execute task when dropping the role"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_DropRoleFailure), nil
	}

	ctxLog.Debug(method+" success", zap.String("role_name", in.RoleName))
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfRoles.Dec()
	return merr.Success(), nil
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_OperateUserRoleFailure), nil
	}
	if in.Type != milvuspb.OperateUserRoleType_RemoveUserFromRole {
		if _, err := c.meta.SelectUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, false); err != nil {
			errMsg := "not found the user, maybe the user isn't existed or internal system error"
			ctxLog.Warn(errMsg, zap.Error(err))
			return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_OperateUserRoleFailure), nil
		}
	}

	err := executeOperateUserRoleTaskSteps(ctx, c, in)
	if err != nil {
		errMsg := "fail to execute task when operate the user and role"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_OperateUserRoleFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

// SelectRole select role
// - check the node health
// - check if the role is valid when this param is provided
// - select role by the meta api
func (c *Core) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	method := "SelectRole"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.SelectRoleResponse{Status: merr.Status(err)}, nil
	}

	if in.Role != nil {
		if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.Role.Name}, false); err != nil {
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				return &milvuspb.SelectRoleResponse{
					Status: merr.Success(),
				}, nil
			}
			errMsg := "fail to select the role to check the role name"
			ctxLog.Warn(errMsg, zap.Error(err))
			return &milvuspb.SelectRoleResponse{
				Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectRoleFailure),
			}, nil
		}
	}
	roleResults, err := c.meta.SelectRole(ctx, util.DefaultTenant, in.Role, in.IncludeUserInfo)
	if err != nil {
		errMsg := "fail to select the role"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &milvuspb.SelectRoleResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectRoleFailure),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectRoleResponse{
		Status:  merr.Success(),
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.SelectUserResponse{Status: merr.Status(err)}, nil
	}

	if in.User != nil {
		if _, err := c.meta.SelectUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: in.User.Name}, false); err != nil {
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				return &milvuspb.SelectUserResponse{
					Status: merr.Success(),
				}, nil
			}
			errMsg := "fail to select the user to check the username"
			ctxLog.Warn(errMsg, zap.Any("in", in), zap.Error(err))
			return &milvuspb.SelectUserResponse{
				Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectUserFailure),
			}, nil
		}
	}
	userResults, err := c.meta.SelectUser(ctx, util.DefaultTenant, in.User, in.IncludeRoleInfo)
	if err != nil {
		errMsg := "fail to select the user"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &milvuspb.SelectUserResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectUserFailure),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectUserResponse{
		Status:  merr.Success(),
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
	if _, err := c.meta.SelectRole(c.ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.Name}, false); err != nil {
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

func (c *Core) isValidPrivilege(ctx context.Context, privilegeName string, object string) error {
	if util.IsAnyWord(privilegeName) {
		return nil
	}
	customPrivGroup, err := c.meta.IsCustomPrivilegeGroup(ctx, privilegeName)
	if err != nil {
		return err
	}
	if customPrivGroup {
		return fmt.Errorf("can not operate the custom privilege group [%s]", privilegeName)
	}
	if lo.Contains(Params.RbacConfig.GetDefaultPrivilegeGroupNames(), privilegeName) {
		return fmt.Errorf("can not operate the built-in privilege group [%s]", privilegeName)
	}
	// check object privileges for built-in privileges
	if util.IsPrivilegeNameDefined(privilegeName) {
		privileges, ok := util.ObjectPrivileges[object]
		if !ok {
			return fmt.Errorf("not found the object type[name: %s], supported the object types: %v", object, lo.Keys(commonpb.ObjectType_value))
		}
		for _, privilege := range privileges {
			if privilege == privilegeName {
				return nil
			}
		}
	}
	return fmt.Errorf("not found the privilege name[%s] in object[%s]", privilegeName, object)
}

func (c *Core) isValidPrivilegeV2(ctx context.Context, privilegeName string) error {
	if util.IsAnyWord(privilegeName) {
		return nil
	}
	customPrivGroup, err := c.meta.IsCustomPrivilegeGroup(ctx, privilegeName)
	if err != nil {
		return err
	}
	if customPrivGroup {
		return nil
	}
	if util.IsPrivilegeNameDefined(privilegeName) {
		return nil
	}
	return fmt.Errorf("not found the privilege name[%s]", privilegeName)
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := c.operatePrivilegeCommonCheck(ctx, in); err != nil {
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
	}

	privName := in.Entity.Grantor.Privilege.Name
	switch in.Version {
	case "v2":
		if err := c.isValidPrivilegeV2(ctx, privName); err != nil {
			ctxLog.Error("", zap.Error(err))
			return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
		}
		if err := c.validatePrivilegeGroupParams(ctx, privName, in.Entity.DbName, in.Entity.ObjectName); err != nil {
			ctxLog.Error("", zap.Error(err))
			return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
		}
		// set up object type for metastore, to be compatible with v1 version
		in.Entity.Object.Name = util.GetObjectType(privName)
	default:
		if err := c.isValidPrivilege(ctx, privName, in.Entity.Object.Name); err != nil {
			ctxLog.Error("", zap.Error(err))
			return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
		}
		// set up object name if it is global object type and not built in privilege group
		if in.Entity.Object.Name == commonpb.ObjectType_Global.String() && !util.IsBuiltinPrivilegeGroup(in.Entity.Grantor.Privilege.Name) {
			in.Entity.ObjectName = util.AnyWord
		}
	}

	err := executeOperatePrivilegeTaskSteps(ctx, c, in)
	if err != nil {
		errMsg := "fail to execute task when operating the privilege"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

func (c *Core) operatePrivilegeCommonCheck(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) error {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return err
	}
	if in.Type != milvuspb.OperatePrivilegeType_Grant && in.Type != milvuspb.OperatePrivilegeType_Revoke {
		errMsg := fmt.Sprintf("invalid operate privilege type, current type: %s, valid value: [%s, %s]", in.Type, milvuspb.OperatePrivilegeType_Grant, milvuspb.OperatePrivilegeType_Revoke)
		return errors.New(errMsg)
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		return errors.New(errMsg)
	}
	if err := c.isValidObject(in.Entity.Object); err != nil {
		return errors.New("the object entity in the request is nil or invalid")
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		return err
	}
	entity := in.Entity.Grantor
	if entity == nil {
		return errors.New("the grantor entity is nil")
	}
	if entity.User == nil || entity.User.Name == "" {
		return errors.New("the user entity in the grantor entity is nil or empty")
	}
	if _, err := c.meta.SelectUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: entity.User.Name}, false); err != nil {
		log.Ctx(ctx).Warn("fail to select the user", zap.String("username", entity.User.Name), zap.Error(err))
		return errors.New("not found the user, maybe the user isn't existed or internal system error")
	}
	if entity.Privilege == nil {
		return errors.New("the privilege entity in the grantor entity is nil")
	}
	return nil
}

func (c *Core) validatePrivilegeGroupParams(ctx context.Context, entity string, dbName string, collectionName string) error {
	allGroups, err := c.getDefaultAndCustomPrivilegeGroups(ctx)
	if err != nil {
		return err
	}
	groups := lo.SliceToMap(allGroups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
		return group.GroupName, group.Privileges
	})
	privs, exists := groups[entity]
	if !exists || len(privs) == 0 {
		// it is a privilege, no need to check with other params
		return nil
	}
	// since all privileges are same level in a group, just check the first privilege
	level := util.GetPrivilegeLevel(privs[0].GetName())
	switch level {
	case milvuspb.PrivilegeLevel_Cluster.String():
		if !util.IsAnyWord(dbName) || !util.IsAnyWord(collectionName) {
			return merr.WrapErrParameterInvalidMsg("dbName and collectionName should be * for the cluster level privilege: %s", entity)
		}
		return nil
	case milvuspb.PrivilegeLevel_Database.String():
		if collectionName != "" && collectionName != util.AnyWord {
			return merr.WrapErrParameterInvalidMsg("collectionName should be * for the database level privilege: %s", entity)
		}
		return nil
	case milvuspb.PrivilegeLevel_Collection.String():
		if util.IsAnyWord(dbName) && !util.IsAnyWord(collectionName) && collectionName != "" {
			return merr.WrapErrParameterInvalidMsg("please specify database name for the collection level privilege: %s", entity)
		}
		return nil
	default:
		return errors.New("not found the privilege level")
	}
}

func (c *Core) getMetastorePrivilegeName(ctx context.Context, privName string) (string, error) {
	// if it is built-in privilege, return the privilege name directly
	if util.IsPrivilegeNameDefined(privName) {
		return util.PrivilegeNameForMetastore(privName), nil
	}
	// return the privilege group name if it is a custom privilege group
	customGroup, err := c.meta.IsCustomPrivilegeGroup(ctx, privName)
	if err != nil {
		return "", err
	}
	if customGroup {
		return util.PrivilegeGroupNameForMetastore(privName), nil
	}
	return "", errors.New("not found the privilege name")
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
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.SelectGrantResponse{
			Status: merr.Status(err),
		}, nil
	}
	if in.Entity == nil {
		errMsg := "the grant entity in the request is nil"
		ctxLog.Warn(errMsg)
		return &milvuspb.SelectGrantResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectGrantFailure),
		}, nil
	}
	if err := c.isValidRole(in.Entity.Role); err != nil {
		ctxLog.Warn("", zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: merr.StatusWithErrorCode(err, commonpb.ErrorCode_SelectGrantFailure),
		}, nil
	}
	if in.Entity.Object != nil {
		if err := c.isValidObject(in.Entity.Object); err != nil {
			ctxLog.Warn("", zap.Error(err))
			return &milvuspb.SelectGrantResponse{
				Status: merr.StatusWithErrorCode(err, commonpb.ErrorCode_SelectGrantFailure),
			}, nil
		}
	}

	grantEntities, err := c.meta.SelectGrant(ctx, util.DefaultTenant, in.Entity)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return &milvuspb.SelectGrantResponse{
			Status:   merr.Success(),
			Entities: grantEntities,
		}, nil
	}
	if err != nil {
		errMsg := "fail to select the grant"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_SelectGrantFailure),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.SelectGrantResponse{
		Status:   merr.Success(),
		Entities: grantEntities,
	}, nil
}

func (c *Core) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	method := "PolicyList"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &internalpb.ListPolicyResponse{
			Status: merr.Status(err),
		}, nil
	}

	policies, err := c.meta.ListPolicy(ctx, util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list policy"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_ListPolicyFailure),
		}, nil
	}
	// expand privilege groups and turn to policies
	allGroups, err := c.getDefaultAndCustomPrivilegeGroups(ctx)
	if err != nil {
		errMsg := "fail to get privilege groups"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_ListPolicyFailure),
		}, nil
	}
	groups := lo.SliceToMap(allGroups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
		return group.GroupName, group.Privileges
	})
	expandGrants, err := c.expandPrivilegeGroups(ctx, policies, groups)
	if err != nil {
		errMsg := "fail to expand privilege groups"
		ctxLog.Warn(errMsg, zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_ListPolicyFailure),
		}, nil
	}
	expandPolicies := lo.Map(expandGrants, func(r *milvuspb.GrantEntity, _ int) string {
		return funcutil.PolicyForPrivilege(r.Role.Name, r.Object.Name, r.ObjectName, r.Grantor.Privilege.Name, r.DbName)
	})

	userRoles, err := c.meta.ListUserRole(ctx, util.DefaultTenant)
	if err != nil {
		errMsg := "fail to list user-role"
		ctxLog.Warn(errMsg, zap.Any("in", in), zap.Error(err))
		return &internalpb.ListPolicyResponse{
			Status: merr.StatusWithErrorCode(errors.New(errMsg), commonpb.ErrorCode_ListPolicyFailure),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &internalpb.ListPolicyResponse{
		Status:          merr.Success(),
		PolicyInfos:     expandPolicies,
		UserRoles:       userRoles,
		PrivilegeGroups: allGroups,
	}, nil
}

func (c *Core) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	method := "BackupRBAC"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.BackupRBACMetaResponse{
			Status: merr.Status(err),
		}, nil
	}

	rbacMeta, err := c.meta.BackupRBAC(ctx, util.DefaultTenant)
	if err != nil {
		return &milvuspb.BackupRBACMetaResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return &milvuspb.BackupRBACMetaResponse{
		Status:   merr.Success(),
		RBACMeta: rbacMeta,
	}, nil
}

func (c *Core) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	method := "RestoreRBAC"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	err := executeRestoreRBACTaskSteps(ctx, c, in)
	if err != nil {
		errMsg := "fail to execute task when restore rbac meta data"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

func (c *Core) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

func (c *Core) DescribeDatabase(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.DescribeDatabaseResponse{Status: merr.Status(err)}, nil
	}

	log := log.Ctx(ctx).With(zap.String("dbName", req.GetDbName()))
	log.Info("received request to describe database ")

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeDatabase", metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("DescribeDatabase")
	t := &describeDBTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := c.scheduler.AddTask(t); err != nil {
		log.Warn("failed to enqueue request to describe database", zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeDatabase", metrics.FailLabel).Inc()
		return &rootcoordpb.DescribeDatabaseResponse{Status: merr.Status(err)}, nil
	}

	if err := t.WaitToFinish(); err != nil {
		log.Warn("failed to describe database", zap.Uint64("ts", t.GetTs()), zap.Error(err))
		metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeDatabase", metrics.FailLabel).Inc()
		return &rootcoordpb.DescribeDatabaseResponse{Status: merr.Status(err)}, nil
	}

	metrics.RootCoordDDLReqCounter.WithLabelValues("DescribeDatabase", metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues("DescribeDatabase").Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Info("done to describe database", zap.Uint64("ts", t.GetTs()))
	return t.Rsp, nil
}

func (c *Core) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.CheckHealthResponse{
			Status:    merr.Status(err),
			IsHealthy: false,
			Reasons:   []string{fmt.Sprintf("serverID=%d: %v", c.session.ServerID, err)},
		}, nil
	}

	group, ctx := errgroup.WithContext(ctx)
	errs := typeutil.NewConcurrentSet[error]()

	proxyClients := c.proxyClientManager.GetProxyClients()
	proxyClients.Range(func(key int64, value types.ProxyClient) bool {
		nodeID := key
		proxyClient := value
		group.Go(func() error {
			sta, err := proxyClient.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			if err != nil {
				errs.Insert(err)
				return err
			}

			err = merr.AnalyzeState("Proxy", nodeID, sta)
			if err != nil {
				errs.Insert(err)
			}

			return err
		})
		return true
	})

	maxDelay := Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second)
	if maxDelay > 0 {
		group.Go(func() error {
			err := CheckTimeTickLagExceeded(ctx, c.queryCoord, c.dataCoord, maxDelay)
			if err != nil {
				errs.Insert(err)
			}
			return err
		})
	}

	err := group.Wait()
	if err != nil {
		return &milvuspb.CheckHealthResponse{
			Status:    merr.Success(),
			IsHealthy: false,
			Reasons: lo.Map(errs.Collect(), func(e error, i int) string {
				return err.Error()
			}),
		}, nil
	}

	return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: true, Reasons: []string{}}, nil
}

func (c *Core) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	method := "CreatePrivilegeGroup"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_CreatePrivilegeGroupFailure), nil
	}

	if err := c.meta.CreatePrivilegeGroup(ctx, in.GroupName); err != nil {
		ctxLog.Warn("fail to create privilege group", zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_CreatePrivilegeGroupFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPrivilegeGroups.Inc()
	return merr.Success(), nil
}

func (c *Core) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	method := "DropPrivilegeGroup"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_DropPrivilegeGroupFailure), nil
	}

	if err := c.meta.DropPrivilegeGroup(ctx, in.GroupName); err != nil {
		ctxLog.Warn("fail to drop privilege group", zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_DropPrivilegeGroupFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.RootCoordNumOfPrivilegeGroups.Desc()
	return merr.Success(), nil
}

func (c *Core) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	method := "ListPrivilegeGroups"
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListPrivilegeGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	privGroups, err := c.meta.ListPrivilegeGroups(ctx)
	if err != nil {
		ctxLog.Warn("fail to list privilege group", zap.Error(err))
		return &milvuspb.ListPrivilegeGroupsResponse{
			Status: merr.StatusWithErrorCode(err, commonpb.ErrorCode_ListPrivilegeGroupsFailure),
		}, nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	// append built in privilege groups
	privGroups = append(privGroups, Params.RbacConfig.GetDefaultPrivilegeGroups()...)
	return &milvuspb.ListPrivilegeGroupsResponse{
		Status:          merr.Success(),
		PrivilegeGroups: privGroups,
	}, nil
}

func (c *Core) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	method := "OperatePrivilegeGroup-" + in.Type.String()
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder(method)
	ctxLog := log.Ctx(ctx).With(zap.String("role", typeutil.RootCoordRole), zap.Any("in", in))
	ctxLog.Debug(method)

	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	err := executeOperatePrivilegeGroupTaskSteps(ctx, c, in)
	if err != nil {
		errMsg := "fail to execute task when operate privilege group"
		ctxLog.Warn(errMsg, zap.Error(err))
		return merr.StatusWithErrorCode(err, commonpb.ErrorCode_OperatePrivilegeGroupFailure), nil
	}

	ctxLog.Debug(method + " success")
	metrics.RootCoordDDLReqCounter.WithLabelValues(method, metrics.SuccessLabel).Inc()
	metrics.RootCoordDDLReqLatency.WithLabelValues(method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

func (c *Core) expandPrivilegeGroups(ctx context.Context, grants []*milvuspb.GrantEntity, groups map[string][]*milvuspb.PrivilegeEntity) ([]*milvuspb.GrantEntity, error) {
	newGrants := []*milvuspb.GrantEntity{}
	createGrantEntity := func(grant *milvuspb.GrantEntity, privilegeName string) (*milvuspb.GrantEntity, error) {
		metaName, err := c.getMetastorePrivilegeName(ctx, privilegeName)
		if err != nil {
			return nil, err
		}
		objectType := &milvuspb.ObjectEntity{
			Name: util.GetObjectType(privilegeName),
		}
		objectName := grant.ObjectName
		if objectType.Name == commonpb.ObjectType_Global.String() {
			objectName = util.AnyWord
		}
		return &milvuspb.GrantEntity{
			Role:       grant.Role,
			Object:     objectType,
			ObjectName: objectName,
			Grantor: &milvuspb.GrantorEntity{
				User: grant.Grantor.User,
				Privilege: &milvuspb.PrivilegeEntity{
					Name: metaName,
				},
			},
			DbName: grant.DbName,
		}, nil
	}

	for _, grant := range grants {
		privName := grant.Grantor.Privilege.Name
		privGroup, exists := groups[privName]
		if !exists {
			privGroup = []*milvuspb.PrivilegeEntity{{Name: privName}}
		}
		for _, priv := range privGroup {
			newGrant, err := createGrantEntity(grant, priv.Name)
			if err != nil {
				return nil, err
			}
			newGrants = append(newGrants, newGrant)
		}
	}
	// uniq by role + object + object name + grantor user + privilege name + db name
	return lo.UniqBy(newGrants, func(g *milvuspb.GrantEntity) string {
		return fmt.Sprintf("%s-%s-%s-%s-%s-%s", g.Role, g.Object, g.ObjectName, g.Grantor.User, g.Grantor.Privilege.Name, g.DbName)
	}), nil
}

// getDefaultAndCustomPrivilegeGroups returns default privilege groups and user-defined privilege groups.
func (c *Core) getDefaultAndCustomPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	allGroups, err := c.meta.ListPrivilegeGroups(ctx)
	allGroups = append(allGroups, Params.RbacConfig.GetDefaultPrivilegeGroups()...)
	if err != nil {
		return nil, err
	}
	return allGroups, nil
}

// RegisterStreamingCoordGRPCService registers the grpc service of streaming coordinator.
func (s *Core) RegisterStreamingCoordGRPCService(server *grpc.Server) {
	s.streamingCoord.RegisterGRPCService(server)
}
