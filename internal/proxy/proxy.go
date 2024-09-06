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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	etcdCli    *clientv3.Client
	address    string
	rootCoord  types.RootCoordClient
	dataCoord  types.DataCoordClient
	queryCoord types.QueryCoordClient

	simpleLimiter *SimpleLimiter

	chMgr channelsMgr

	replicateMsgStream msgstream.MsgStream

	sched *taskScheduler

	chTicker channelsTimeTicker

	rowIDAllocator *allocator.IDAllocator
	tsoAllocator   *timestampAllocator
	segAssigner    *segIDAssigner

	metricsCacheManager *metricsinfo.MetricsCacheManager

	session  *sessionutil.Session
	shardMgr shardClientMgr

	factory dependency.Factory

	searchResultCh chan *internalpb.SearchResults

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	// for load balance in replicas
	lbPolicy LBPolicy

	// resource manager
	resourceManager        resource.Manager
	replicateStreamManager *ReplicateStreamManager

	// materialized view
	enableMaterializedView bool

	// delete rate limiter
	enableComplexDeleteLimit bool
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, factory dependency.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	n := 1024 // better to be configurable
	mgr := newShardClientMgr()
	lbPolicy := NewLBPolicyImpl(mgr)
	lbPolicy.Start(ctx)
	resourceManager := resource.NewManager(10*time.Second, 20*time.Second, make(map[string]time.Duration))
	replicateStreamManager := NewReplicateStreamManager(ctx, factory, resourceManager)
	node := &Proxy{
		ctx:                    ctx1,
		cancel:                 cancel,
		factory:                factory,
		searchResultCh:         make(chan *internalpb.SearchResults, n),
		shardMgr:               mgr,
		simpleLimiter:          NewSimpleLimiter(Params.QuotaConfig.AllocWaitInterval.GetAsDuration(time.Millisecond), Params.QuotaConfig.AllocRetryTimes.GetAsUint()),
		lbPolicy:               lbPolicy,
		resourceManager:        resourceManager,
		replicateStreamManager: replicateStreamManager,
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
	metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.ProxyRole).Inc()
	log.Info("Proxy Register Finished")
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Proxy disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		os.Exit(1)
	})
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
	rateCol.Register(internalpb.RateType_DMLUpsert.String())
	rateCol.Register(internalpb.RateType_DMLDelete.String())
	// TODO: add bulkLoad rate
	rateCol.Register(internalpb.RateType_DQLSearch.String())
	rateCol.Register(internalpb.RateType_DQLQuery.String())
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	log.Info("init session for Proxy")
	if err := node.initSession(); err != nil {
		log.Warn("failed to init Proxy's session", zap.Error(err))
		return err
	}
	log.Info("init session for Proxy done")

	node.factory.Init(Params)

	log.Debug("init access log for Proxy done")

	err := node.initRateCollector()
	if err != nil {
		return err
	}
	log.Info("Proxy init rateCollector done", zap.Int64("nodeID", paramtable.GetNodeID()))

	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.rootCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create id allocator",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.rowIDAllocator = idAllocator
	log.Debug("create id allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	tsoAllocator, err := newTimestampAllocator(node.rootCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create timestamp allocator",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.tsoAllocator = tsoAllocator
	log.Debug("create timestamp allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	segAssigner, err := newSegIDAssigner(node.ctx, node.dataCoord, node.lastTick)
	if err != nil {
		log.Warn("failed to create segment id assigner",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = paramtable.GetNodeID()
	log.Debug("create segment id assigner done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.rootCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc, node.factory)
	node.chMgr = chMgr
	log.Debug("create channels manager done", zap.String("role", typeutil.ProxyRole))

	replicateMsgChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	node.replicateMsgStream, err = node.factory.NewMsgStream(node.ctx)
	if err != nil {
		log.Warn("failed to create replicate msg stream",
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()),
			zap.Error(err))
		return err
	}
	node.replicateMsgStream.EnableProduce(true)
	node.replicateMsgStream.AsProducer([]string{replicateMsgChannel})

	node.sched, err = newTaskScheduler(node.ctx, node.tsoAllocator, node.factory)
	if err != nil {
		log.Warn("failed to create task scheduler", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("create task scheduler done", zap.String("role", typeutil.ProxyRole))

	syncTimeTickInterval := Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond) / 2
	node.chTicker = newChannelsTimeTicker(node.ctx, Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond)/2, []string{}, node.sched.getPChanStatistics, tsoAllocator)
	log.Debug("create channels time ticker done", zap.String("role", typeutil.ProxyRole), zap.Duration("syncTimeTickInterval", syncTimeTickInterval))

	node.enableComplexDeleteLimit = Params.QuotaConfig.ComplexDeleteLimitEnable.GetAsBool()
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	log.Debug("create metrics cache manager done", zap.String("role", typeutil.ProxyRole))

	if err := InitMetaCache(node.ctx, node.rootCoord, node.queryCoord, node.shardMgr); err != nil {
		log.Warn("failed to init meta cache", zap.String("role", typeutil.ProxyRole), zap.Error(err))
		return err
	}
	log.Debug("init meta cache done", zap.String("role", typeutil.ProxyRole))

	node.enableMaterializedView = Params.CommonCfg.EnableMaterializedView.GetAsBool()

	log.Info("init proxy done", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("Address", node.address))
	return nil
}

// sendChannelsTimeTickLoop starts a goroutine that synchronizes the time tick information.
func (node *Proxy) sendChannelsTimeTickLoop() {
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()

		ticker := time.NewTicker(Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-node.ctx.Done():
				log.Info("send channels time tick loop exit")
				return
			case <-ticker.C:
				if !Params.CommonCfg.TTMsgEnabled.GetAsBool() {
					continue
				}
				stats, ts, err := node.chTicker.getMinTsStatistics()
				if err != nil {
					log.Warn("sendChannelsTimeTickLoop.getMinTsStatistics", zap.Error(err))
					continue
				}

				if ts == 0 {
					log.Warn("sendChannelsTimeTickLoop.getMinTsStatistics default timestamp equal 0")
					continue
				}

				channels := make([]pChan, 0, len(stats))
				tss := make([]Timestamp, 0, len(stats))

				maxTs := ts
				for channel, ts := range stats {
					channels = append(channels, channel)
					tss = append(tss, ts)
					if ts > maxTs {
						maxTs = ts
					}
				}

				req := &internalpb.ChannelTimeTickMsg{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
						commonpbutil.WithSourceID(node.session.ServerID),
					),
					ChannelNames:     channels,
					Timestamps:       tss,
					DefaultTimestamp: maxTs,
				}

				func() {
					// we should pay more attention to the max lag.
					minTs := maxTs
					minTsOfChannel := "default"

					// find the min ts and the related channel.
					for channel, ts := range stats {
						if ts < minTs {
							minTs = ts
							minTsOfChannel = channel
						}
					}

					sub := tsoutil.SubByNow(minTs)
					metrics.ProxySyncTimeTickLag.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), minTsOfChannel).Set(float64(sub))
				}()

				status, err := node.rootCoord.UpdateChannelTimeTick(node.ctx, req)
				if err != nil {
					log.Warn("sendChannelsTimeTickLoop.UpdateChannelTimeTick", zap.Error(err))
					continue
				}
				if status.GetErrorCode() != 0 {
					log.Warn("sendChannelsTimeTickLoop.UpdateChannelTimeTick",
						zap.Any("ErrorCode", status.ErrorCode),
						zap.Any("Reason", status.Reason))
					continue
				}
			}
		}
	}()
}

// Start starts a proxy node.
func (node *Proxy) Start() error {
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

	if !streamingutil.IsStreamingServiceEnabled() {
		if err := node.segAssigner.Start(); err != nil {
			log.Warn("failed to start segment id assigner", zap.String("role", typeutil.ProxyRole), zap.Error(err))
			return err
		}
		log.Debug("start segment id assigner done", zap.String("role", typeutil.ProxyRole))

		if err := node.chTicker.start(); err != nil {
			log.Warn("failed to start channels time ticker", zap.String("role", typeutil.ProxyRole), zap.Error(err))
			return err
		}
		log.Debug("start channels time ticker done", zap.String("role", typeutil.ProxyRole))

		node.sendChannelsTimeTickLoop()
	}

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
	if node.rowIDAllocator != nil {
		node.rowIDAllocator.Close()
		log.Info("close id allocator", zap.String("role", typeutil.ProxyRole))
	}

	if node.sched != nil {
		node.sched.Close()
		log.Info("close scheduler", zap.String("role", typeutil.ProxyRole))
	}

	if !streamingutil.IsStreamingServiceEnabled() {
		if node.segAssigner != nil {
			node.segAssigner.Close()
			log.Info("close segment id assigner", zap.String("role", typeutil.ProxyRole))
		}

		if node.chTicker != nil {
			err := node.chTicker.close()
			if err != nil {
				return err
			}
			log.Info("close channels time ticker", zap.String("role", typeutil.ProxyRole))
		}
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

	if node.chMgr != nil {
		node.chMgr.removeAllDMLStream()
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

// lastTick returns the last write timestamp of all pchans in this Proxy.
func (node *Proxy) lastTick() Timestamp {
	return node.chTicker.getMinTick()
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

// SetEtcdClient sets etcd client for proxy.
func (node *Proxy) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}

// SetRootCoordClient sets RootCoord client for proxy.
func (node *Proxy) SetRootCoordClient(cli types.RootCoordClient) {
	node.rootCoord = cli
}

// SetDataCoordClient sets DataCoord client for proxy.
func (node *Proxy) SetDataCoordClient(cli types.DataCoordClient) {
	node.dataCoord = cli
}

// SetQueryCoordClient sets QueryCoord client for proxy.
func (node *Proxy) SetQueryCoordClient(cli types.QueryCoordClient) {
	node.queryCoord = cli
}

func (node *Proxy) SetQueryNodeCreator(f func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)) {
	node.shardMgr.SetClientCreatorFunc(f)
}

// GetRateLimiter returns the rateLimiter in Proxy.
func (node *Proxy) GetRateLimiter() (types.Limiter, error) {
	if node.simpleLimiter == nil {
		return nil, fmt.Errorf("nil rate limiter in Proxy")
	}
	return node.simpleLimiter, nil
}
