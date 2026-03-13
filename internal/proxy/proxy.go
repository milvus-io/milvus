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

package proxy

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

// Timestamp is alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

// const sendTimeTickMsgInterval = 200 * time.Millisecond
// const channelMgrTickerInterval = 100 * time.Millisecond

// make sure Proxy implements types.Proxy
var _ types.Proxy = (*Proxy)(nil)

var (
	Params  = paramtable.Get()
	rateCol *ratelimitutil.RateCollector
)

// Proxy of milvus
type Proxy struct {
	milvuspb.UnimplementedMilvusServiceServer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Int32

	address  string
	mixCoord types.MixCoordClient

	simpleLimiter *SimpleLimiter

	chMgr channelsMgr

	sched *taskScheduler

	rowIDAllocator *allocator.IDAllocator
	tsoAllocator   *timestampAllocator

	metricsCacheManager *metricsinfo.MetricsCacheManager

	session  *sessionutil.Session
	shardMgr shardclient.ShardClientMgr

	searchResultCh chan *internalpb.SearchResults

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	// for load balance in replicas
	lbPolicy shardclient.LBPolicy

	// resource manager
	resourceManager resource.Manager

	// materialized view
	enableMaterializedView bool

	// delete rate limiter
	enableComplexDeleteLimit bool

	slowQueries *expirable.LRU[Timestamp, *metricsinfo.SlowQuery]
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, _ dependency.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	n := 1024 // better to be configurable
	resourceManager := resource.NewManager(10*time.Second, 20*time.Second, make(map[string]time.Duration))
	node := &Proxy{
		ctx:            ctx1,
		cancel:         cancel,
		searchResultCh: make(chan *internalpb.SearchResults, n),
		// shardMgr:        mgr,
		simpleLimiter: NewSimpleLimiter(Params.QuotaConfig.AllocWaitInterval.GetAsDuration(time.Millisecond), Params.QuotaConfig.AllocRetryTimes.GetAsUint()),
		// lbPolicy:        lbPolicy,
		resourceManager: resourceManager,
		slowQueries:     expirable.NewLRU[Timestamp, *metricsinfo.SlowQuery](20, nil, time.Minute*15),
	}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	expr.Register("proxy", node)
	hookutil.InitOnceHook()
	logutil.Logger(ctx).Debug("create a new Proxy instance", zap.Any("state", node.stateCode.Load()))
	return node, nil
}

// UpdateStateCode updates the state code of Proxy.
func (node *Proxy) UpdateStateCode(code commonpb.StateCode) {
	node.stateCode.Store(int32(code))
}

func (node *Proxy) GetStateCode() commonpb.StateCode {
	return commonpb.StateCode(node.stateCode.Load())
}

// Register registers proxy at etcd
func (node *Proxy) Register() error {
	node.session.Register()
	metrics.NumNodes.WithLabelValues(paramtable.GetStringNodeID(), typeutil.ProxyRole).Inc()
	log.Info("Proxy Register Finished")
	// TODO Reset the logger
	// Params.initLogCfg()
	return nil
}

// initSession initialize the session of Proxy.
func (node *Proxy) initSession() error {
	node.session = sessionutil.NewSession(node.ctx)
	if node.session == nil {
		return errors.New("new session failed, maybe etcd cannot be connected")
	}
	node.session.Init(typeutil.ProxyRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.ProxyRole, node.session.ServerID)
	return nil
}

// initRateCollector creates and starts rateCollector in Proxy.
func (node *Proxy) initRateCollector() error {
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, true)
	if err != nil {
		return err
	}
	rateCol.Register(internalpb.RateType_DMLInsert.String())
	rateCol.Register(internalpb.RateType_DMLDelete.String())
	// TODO: add bulkLoad rate
	rateCol.Register(internalpb.RateType_DQLSearch.String())
	rateCol.Register(internalpb.RateType_DQLQuery.String())
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	log := log.Ctx(node.ctx)
	log.Info("init session for Proxy")
	if err := node.initSession(); err != nil {
		log.Warn("failed to init Proxy's session", zap.Error(err))
		return err
	}
	log.Info("init session for Proxy done")

	err := node.initRateCollector()
	if err != nil {
		return err
	}
	log.Info("Proxy init rateCollector done", zap.Int64("nodeID", paramtable.GetNodeID()))

	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.mixCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create id allocator",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.rowIDAllocator = idAllocator
	log.Debug("create id allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	tsoAllocator, err := newTimestampAllocator(node.mixCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create timestamp allocator",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.tsoAllocator = tsoAllocator
	log.Debug("create timestamp allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.mixCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc)
	node.chMgr = chMgr
	log.Debug("create channels manager done", zap.String("role", typeutil.ProxyRole))

	node.sched, err = newTaskScheduler(node.ctx, node.tsoAllocator)
	if err != nil {
		log.Warn("failed to create task scheduler", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("create task scheduler done", zap.String("role", typeutil.ProxyRole))

	node.enableComplexDeleteLimit = Params.QuotaConfig.ComplexDeleteLimitEnable.GetAsBool()
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	log.Debug("create metrics cache manager done", zap.String("role", typeutil.ProxyRole))

	if err := InitMetaCache(node.ctx, node.mixCoord); err != nil {
		log.Warn("failed to init meta cache", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("init meta cache done", zap.String("role", typeutil.ProxyRole))

	node.shardMgr = shardclient.NewShardClientMgr(node.mixCoord)
	node.lbPolicy = shardclient.NewLBPolicyImpl(node.shardMgr)

	node.enableMaterializedView = Params.CommonCfg.EnableMaterializedView.GetAsBool()

	// Enable internal rand pool for UUIDv4 generation
	// This is NOT thread-safe and should only be called before the service starts and
	// there is no possibility that New or any other UUID V4 generation function will be called concurrently
	// Only proxy generates UUID for now, and one Milvus process only has one proxy
	uuid.EnableRandPool()
	log.Debug("enable rand pool for UUIDv4 generation")

	log.Info("init proxy done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", node.address))
	return nil
}

// Start starts a proxy node.
func (node *Proxy) Start() error {
	log := log.Ctx(node.ctx)

	node.shardMgr.Start()
	log.Debug("start shard client manager done", zap.String("role", typeutil.ProxyRole))

	node.lbPolicy.Start(node.ctx)

	if err := node.sched.Start(); err != nil {
		log.Warn("failed to start task scheduler", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("start task scheduler done", zap.String("role", typeutil.ProxyRole))

	if err := node.rowIDAllocator.Start(); err != nil {
		log.Warn("failed to start id allocator", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("start id allocator done", zap.String("role", typeutil.ProxyRole))

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey: hookutil.OpTypeNodeID,
		hookutil.NodeIDKey: paramtable.GetNodeID(),
	})

	log.Debug("update state code", zap.String("role", typeutil.ProxyRole), zap.String("State", commonpb.StateCode_Healthy.String()))
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	// register devops api
	RegisterMgrRoute(node)

	return nil
}

// Stop stops a proxy node.
func (node *Proxy) Stop() error {
	log := log.Ctx(node.ctx)
	if node.rowIDAllocator != nil {
		node.rowIDAllocator.Close()
		log.Info("close id allocator", zap.String("role", typeutil.ProxyRole))
	}

	if node.sched != nil {
		node.sched.Close()
		log.Info("close scheduler", zap.String("role", typeutil.ProxyRole))
	}

	for _, cb := range node.closeCallbacks {
		cb()
	}

	if node.session != nil {
		node.session.Stop()
	}

	if node.shardMgr != nil {
		node.shardMgr.Close()
	}

	if node.lbPolicy != nil {
		node.lbPolicy.Close()
	}

	if node.resourceManager != nil {
		node.resourceManager.Close()
	}

	node.cancel()
	node.wg.Wait()

	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	connection.GetManager().Stop()
	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (node *Proxy) AddStartCallback(callbacks ...func()) {
	node.startCallbacks = append(node.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (node *Proxy) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

func (node *Proxy) SetAddress(address string) {
	node.address = address
}

func (node *Proxy) GetAddress() string {
	return node.address
}

// SetMixCoordClient sets MixCoord client for proxy.
func (node *Proxy) SetMixCoordClient(cli types.MixCoordClient) {
	node.mixCoord = cli
}

func (node *Proxy) SetQueryNodeCreator(f func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)) {
	node.shardMgr.SetClientCreatorFunc(f)
}

// GetRateLimiter returns the rateLimiter in Proxy.
func (node *Proxy) GetRateLimiter() (types.Limiter, error) {
	if node.simpleLimiter == nil {
		return nil, errors.New("nil rate limiter in Proxy")
	}
	return node.simpleLimiter, nil
}
