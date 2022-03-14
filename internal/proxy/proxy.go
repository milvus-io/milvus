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
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

// Timestamp is alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

// const sendTimeTickMsgInterval = 200 * time.Millisecond
// const channelMgrTickerInterval = 100 * time.Millisecond

// make sure Proxy implements types.Proxy
var _ types.Proxy = (*Proxy)(nil)

var Params paramtable.ComponentParam

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
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord
	dataCoord  types.DataCoord
	queryCoord types.QueryCoord

	chMgr channelsMgr

	sched *taskScheduler

	chTicker channelsTimeTicker

	idAllocator  *allocator.IDAllocator
	tsoAllocator *timestampAllocator
	segAssigner  *segIDAssigner

	metricsCacheManager *metricsinfo.MetricsCacheManager

	session *sessionutil.Session

	msFactory msgstream.Factory

	searchResultCh   chan *internalpb.SearchResults
	retrieveResultCh chan *internalpb.RetrieveResults

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, factory msgstream.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	n := 1024 // better to be configurable
	node := &Proxy{
		ctx:              ctx1,
		cancel:           cancel,
		msFactory:        factory,
		searchResultCh:   make(chan *internalpb.SearchResults, n),
		retrieveResultCh: make(chan *internalpb.RetrieveResults, n),
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	logutil.Logger(ctx).Debug("create a new Proxy instance", zap.Any("state", node.stateCode.Load()))
	return node, nil

}

// Register registers proxy at etcd
func (node *Proxy) Register() error {
	node.session.Register()
	go node.session.LivenessCheck(node.ctx, func() {
		log.Error("Proxy disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		if node.session.TriggerKill {
			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}
	})
	// TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

// initSession initialize the session of Proxy.
func (node *Proxy) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.EtcdCfg.MetaRootPath, node.etcdCli)
	if node.session == nil {
		return errors.New("new session failed, maybe etcd cannot be connected")
	}
	node.session.Init(typeutil.ProxyRole, Params.ProxyCfg.NetworkAddress, false, true)
	Params.ProxyCfg.ProxyID = node.session.ServerID
	Params.SetLogger(Params.ProxyCfg.ProxyID)
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	log.Debug("init session for Proxy")
	if err := node.initSession(); err != nil {
		log.Warn("failed to init Proxy's session", zap.Error(err))
		return err
	}
	log.Debug("init session for Proxy done")

	if node.queryCoord != nil {
		log.Debug("create query channel for Proxy")
		resp, err := node.queryCoord.CreateQueryChannel(node.ctx, &querypb.CreateQueryChannelRequest{})
		if err != nil {
			log.Warn("failed to create query channel for Proxy", zap.Error(err))
			return err
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("failed to create query channel for Proxy",
				zap.String("error_code", resp.Status.ErrorCode.String()),
				zap.String("reason", resp.Status.Reason))
			return errors.New(resp.Status.Reason)
		}

		// TODO SearchResultChannelNames and RetrieveResultChannelNames should not be part in the Param table
		// we should maintain a separate map for search result
		Params.ProxyCfg.SearchResultChannelNames = []string{resp.QueryResultChannel}
		Params.ProxyCfg.RetrieveResultChannelNames = []string{resp.QueryResultChannel}
		log.Debug("Proxy CreateQueryChannel success", zap.Any("SearchResultChannelNames", Params.ProxyCfg.SearchResultChannelNames))
		log.Debug("Proxy CreateQueryChannel success", zap.Any("RetrieveResultChannelNames", Params.ProxyCfg.RetrieveResultChannelNames))
		log.Debug("create query channel for Proxy done", zap.String("QueryResultChannel", resp.QueryResultChannel))
	}

	m := map[string]interface{}{
		"PulsarAddress": Params.PulsarCfg.Address,
		"PulsarBufSize": 1024}
	log.Debug("set parameters for ms factory", zap.String("role", typeutil.ProxyRole), zap.Any("parameters", m))
	if err := node.msFactory.SetParams(m); err != nil {
		log.Warn("failed to set parameters for ms factory",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole),
			zap.Any("parameters", m))
		return err
	}
	log.Debug("set parameters for ms factory done", zap.String("role", typeutil.ProxyRole), zap.Any("parameters", m))

	log.Debug("create id allocator", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.rootCoord, Params.ProxyCfg.ProxyID)
	if err != nil {
		log.Warn("failed to create id allocator",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
		return err
	}
	node.idAllocator = idAllocator
	log.Debug("create id allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))

	log.Debug("create timestamp allocator", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
	tsoAllocator, err := newTimestampAllocator(node.ctx, node.rootCoord, Params.ProxyCfg.ProxyID)
	if err != nil {
		log.Warn("failed to create timestamp allocator",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
		return err
	}
	node.tsoAllocator = tsoAllocator
	log.Debug("create timestamp allocator done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))

	log.Debug("create segment id assigner", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
	segAssigner, err := newSegIDAssigner(node.ctx, node.dataCoord, node.lastTick)
	if err != nil {
		log.Warn("failed to create segment id assigner",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))
		return err
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = Params.ProxyCfg.ProxyID
	log.Debug("create segment id assigner done", zap.String("role", typeutil.ProxyRole), zap.Int64("ProxyID", Params.ProxyCfg.ProxyID))

	log.Debug("create channels manager", zap.String("role", typeutil.ProxyRole))
	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.rootCoord)
	dqlChannelsFunc := getDqlChannelsFunc(node.ctx, node.session.ServerID, node.queryCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc, dqlChannelsFunc, nil, node.msFactory)
	node.chMgr = chMgr
	log.Debug("create channels manager done", zap.String("role", typeutil.ProxyRole))

	log.Debug("create task scheduler", zap.String("role", typeutil.ProxyRole))
	node.sched, err = newTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory,
		schedOptWithSearchResultCh(node.searchResultCh),
		schedOptWithRetrieveResultCh(node.retrieveResultCh))
	if err != nil {
		log.Warn("failed to create task scheduler", zap.Error(err), zap.String("role", typeutil.ProxyRole))
		return err
	}
	log.Debug("create task scheduler done", zap.String("role", typeutil.ProxyRole))

	syncTimeTickInterval := Params.ProxyCfg.TimeTickInterval / 2
	log.Debug("create channels time ticker",
		zap.String("role", typeutil.ProxyRole), zap.Duration("syncTimeTickInterval", syncTimeTickInterval))
	node.chTicker = newChannelsTimeTicker(node.ctx, Params.ProxyCfg.TimeTickInterval/2, []string{}, node.sched.getPChanStatistics, tsoAllocator)
	log.Debug("create channels time ticker done", zap.String("role", typeutil.ProxyRole))

	log.Debug("create metrics cache manager", zap.String("role", typeutil.ProxyRole))
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	log.Debug("create metrics cache manager done", zap.String("role", typeutil.ProxyRole))

	log.Debug("init meta cache", zap.String("role", typeutil.ProxyRole))
	if err := InitMetaCache(node.rootCoord); err != nil {
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

		timer := time.NewTicker(Params.ProxyCfg.TimeTickInterval)

		for {
			select {
			case <-node.ctx.Done():
				log.Info("send channels time tick loop exit")
				return
			case <-timer.C:
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
					metrics.ProxySyncTimeTick.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), channel).Set(float64(ts))
					channels = append(channels, channel)
					tss = append(tss, ts)
					if ts > maxTs {
						maxTs = ts
					}
				}

				req := &internalpb.ChannelTimeTickMsg{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_TimeTick, // todo
						MsgID:     0,                         // todo
						Timestamp: 0,                         // todo
						SourceID:  node.session.ServerID,
					},
					ChannelNames:     channels,
					Timestamps:       tss,
					DefaultTimestamp: maxTs,
				}

				metrics.ProxySyncTimeTick.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), "DefaultTimestamp").Set(float64(maxTs))

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
	if err := node.idAllocator.Start(); err != nil {
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

	now := time.Now()
	Params.ProxyCfg.CreatedTime = now
	Params.ProxyCfg.UpdatedTime = now

	log.Debug("update state code", zap.String("role", typeutil.ProxyRole), zap.String("State", internalpb.StateCode_Healthy.String()))
	node.UpdateStateCode(internalpb.StateCode_Healthy)

	return nil
}

// Stop stops a proxy node.
func (node *Proxy) Stop() error {
	node.cancel()

	if node.idAllocator != nil {
		node.idAllocator.Close()
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

	node.session.Revoke(time.Second)

	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

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

// SetEtcdClient sets etcd client for proxy.
func (node *Proxy) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}

// SetRootCoordClient sets RootCoord client for proxy.
func (node *Proxy) SetRootCoordClient(cli types.RootCoord) {
	node.rootCoord = cli
}

// SetIndexCoordClient sets IndexCoord client for proxy.
func (node *Proxy) SetIndexCoordClient(cli types.IndexCoord) {
	node.indexCoord = cli
}

// SetDataCoordClient sets DataCoord client for proxy.
func (node *Proxy) SetDataCoordClient(cli types.DataCoord) {
	node.dataCoord = cli
}

// SetQueryCoordClient sets QueryCoord client for proxy.
func (node *Proxy) SetQueryCoordClient(cli types.QueryCoord) {
	node.queryCoord = cli
}
