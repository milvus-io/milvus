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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
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

var Params *paramtable.ComponentParam = paramtable.Get()

// rateCol is global rateCollector in Proxy.
var rateCol *ratelimitutil.RateCollector

// Proxy of milvus
type Proxy struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Value

	etcdCli    *clientv3.Client
	address    string
	rootCoord  types.RootCoord
	dataCoord  types.DataCoord
	queryCoord types.QueryCoord

	multiRateLimiter *MultiRateLimiter

	chMgr channelsMgr

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
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, factory dependency.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	n := 1024 // better to be configurable
	mgr := newShardClientMgr()
	node := &Proxy{
		ctx:              ctx1,
		cancel:           cancel,
		factory:          factory,
		searchResultCh:   make(chan *internalpb.SearchResults, n),
		shardMgr:         mgr,
		multiRateLimiter: NewMultiRateLimiter(),
		lbPolicy:         NewLBPolicyImpl(mgr),
	}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	logutil.Logger(ctx).Debug("create a new Proxy instance", zap.Any("state", node.stateCode.Load()))
	return node, nil
}

// Register registers proxy at etcd
func (node *Proxy) Register() error {
	node.session.Register()
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Proxy disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	// TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

// initSession initialize the session of Proxy.
func (node *Proxy) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.EtcdCfg.MetaRootPath.GetValue(), node.etcdCli)
	if node.session == nil {
		return errors.New("new session failed, maybe etcd cannot be connected")
	}
	node.session.Init(typeutil.ProxyRole, node.address, false, true)
	return nil
}

// initRateCollector creates and starts rateCollector in Proxy.
func (node *Proxy) initRateCollector() error {
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity)
	if err != nil {
		return err
	}
	rateCol.Register(internalpb.RateType_DMLInsert.String())
	rateCol.Register(internalpb.RateType_DMLDelete.String())
	// TODO: add bulkLoad rate
	rateCol.Register(internalpb.RateType_DQLSearch.String())
	rateCol.Register(internalpb.RateType_DQLQuery.String())
	rateCol.Register(metricsinfo.ReadResultThroughput)
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
	log.Debug("init parameters for factory", zap.String("role", typeutil.ProxyRole), zap.Any("parameters", Params.GetAll()))

	accesslog.SetupAccseeLog(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	log.Debug("init access log for Proxy done")

	err := node.initRateCollector()
	if err != nil {
		return err
	}
	log.Info("Proxy init rateCollector done", zap.Int64("nodeID", paramtable.GetNodeID()))

	log.Debug("create id allocator", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.rootCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create id allocator",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
		return err
	}
	node.rowIDAllocator = idAllocator
	log.Debug("create id allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	log.Debug("create timestamp allocator", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
	tsoAllocator, err := newTimestampAllocator(node.rootCoord, paramtable.GetNodeID())
	if err != nil {
		log.Warn("failed to create timestamp allocator",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
		return err
	}
	node.tsoAllocator = tsoAllocator
	log.Debug("create timestamp allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	log.Debug("create segment id assigner", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
	segAssigner, err := newSegIDAssigner(node.ctx, node.dataCoord, node.lastTick)
	if err != nil {
		log.Warn("failed to create segment id assigner",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))
		return err
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = paramtable.GetNodeID()
	log.Debug("create segment id assigner done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", paramtable.GetNodeID()))

	log.Debug("create channels manager", zap.String("role", typeutil.ProxyRole))
	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.rootCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc, node.factory)
	node.chMgr = chMgr
	log.Debug("create channels manager done", zap.String("role", typeutil.ProxyRole))

	log.Debug("create task scheduler", zap.String("role", typeutil.ProxyRole))
	node.sched, err = newTaskScheduler(node.ctx, node.tsoAllocator, node.factory)
	if err != nil {
		log.Warn("failed to create task scheduler", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("create task scheduler done", zap.String("role", typeutil.ProxyRole))

	syncTimeTickInterval := Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond) / 2
	log.Debug("create channels time ticker",
		zap.String("role", typeutil.ProxyRole), zap.Duration("syncTimeTickInterval", syncTimeTickInterval))
	node.chTicker = newChannelsTimeTicker(node.ctx, Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond)/2, []string{}, node.sched.getPChanStatistics, tsoAllocator)
	log.Debug("create channels time ticker done", zap.String("role", typeutil.ProxyRole))

	log.Debug("create metrics cache manager", zap.String("role", typeutil.ProxyRole))
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	log.Debug("create metrics cache manager done", zap.String("role", typeutil.ProxyRole))

	log.Debug("init meta cache", zap.String("role", typeutil.ProxyRole))
	if err := InitMetaCache(node.ctx, node.rootCoord, node.queryCoord, node.shardMgr); err != nil {
		log.Warn("failed to init meta cache", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("init meta cache done", zap.String("role", typeutil.ProxyRole))

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
						commonpbutil.WithMsgType(commonpb.MsgType_TimeTick), // todo
						commonpbutil.WithMsgID(0),                           // todo
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
				if status.ErrorCode != 0 {
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
	log.Debug("start task scheduler", zap.String("role", typeutil.ProxyRole))
	if err := node.sched.Start(); err != nil {
		log.Warn("failed to start task scheduler", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("start task scheduler done", zap.String("role", typeutil.ProxyRole))

	log.Debug("start id allocator", zap.String("role", typeutil.ProxyRole))
	if err := node.rowIDAllocator.Start(); err != nil {
		log.Warn("failed to start id allocator", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("start id allocator done", zap.String("role", typeutil.ProxyRole))

	log.Debug("start segment id assigner", zap.String("role", typeutil.ProxyRole))
	if err := node.segAssigner.Start(); err != nil {
		log.Warn("failed to start segment id assigner", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("start segment id assigner done", zap.String("role", typeutil.ProxyRole))

	log.Debug("start channels time ticker", zap.String("role", typeutil.ProxyRole))
	if err := node.chTicker.start(); err != nil {
		log.Warn("failed to start channels time ticker", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("start channels time ticker done", zap.String("role", typeutil.ProxyRole))

	node.sendChannelsTimeTickLoop()

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	log.Debug("update state code", zap.String("role", typeutil.ProxyRole), zap.String("State", commonpb.StateCode_Healthy.String()))
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	return nil
}

// Stop stops a proxy node.
func (node *Proxy) Stop() error {
	node.cancel()

	if node.rowIDAllocator != nil {
		node.rowIDAllocator.Close()
		log.Info("close id allocator", zap.String("role", typeutil.ProxyRole))
	}

	if node.segAssigner != nil {
		node.segAssigner.Close()
		log.Info("close segment id assigner", zap.String("role", typeutil.ProxyRole))
	}

	if node.sched != nil {
		node.sched.Close()
		log.Info("close scheduler", zap.String("role", typeutil.ProxyRole))
	}

	if node.chTicker != nil {
		err := node.chTicker.close()
		if err != nil {
			return err
		}
		log.Info("close channels time ticker", zap.String("role", typeutil.ProxyRole))
	}

	node.wg.Wait()

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

	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	GetConnectionManager().stop()

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
func (node *Proxy) SetRootCoordClient(cli types.RootCoord) {
	node.rootCoord = cli
}

// SetDataCoordClient sets DataCoord client for proxy.
func (node *Proxy) SetDataCoordClient(cli types.DataCoord) {
	node.dataCoord = cli
}

// SetQueryCoordClient sets QueryCoord client for proxy.
func (node *Proxy) SetQueryCoordClient(cli types.QueryCoord) {
	node.queryCoord = cli
}

func (node *Proxy) SetQueryNodeCreator(f func(ctx context.Context, addr string) (types.QueryNode, error)) {
	node.shardMgr.SetClientCreatorFunc(f)
}

// GetRateLimiter returns the rateLimiter in Proxy.
func (node *Proxy) GetRateLimiter() (types.Limiter, error) {
	if node.multiRateLimiter == nil {
		return nil, fmt.Errorf("nil rate limiter in Proxy")
	}
	return node.multiRateLimiter, nil
}
