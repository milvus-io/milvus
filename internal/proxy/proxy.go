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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

// Timestamp is alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

const sendTimeTickMsgInterval = 200 * time.Millisecond
const channelMgrTickerInterval = 100 * time.Millisecond

// make sure Proxy implements types.Proxy
var _ types.Proxy = (*Proxy)(nil)

// Proxy of milvus
type Proxy struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Value

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

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, factory msgstream.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &Proxy{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
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
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	})
	// TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

func (node *Proxy) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	if node.session == nil {
		return errors.New("new session failed, maybe etcd cannot be connected")
	}
	node.session.Init(typeutil.ProxyRole, Params.NetworkAddress, false)
	Params.ProxyID = node.session.ServerID
	Params.SetLogger(Params.ProxyID)
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	err := node.initSession()
	if err != nil {
		log.Error("Proxy init session failed", zap.Error(err))
		return err
	}
	Params.initProxySubName()
	// wait for datacoord state changed to Healthy
	if node.dataCoord != nil {
		log.Debug("Proxy wait for DataCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.dataCoord, "DataCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for DataCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy DataCoord is ready")
	}

	// wait for queryCoord state changed to Healthy
	if node.queryCoord != nil {
		log.Debug("Proxy wait for QueryCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.queryCoord, "QueryCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for QueryCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy QueryCoord is ready")
	}

	// wait for indexcoord state changed to Healthy
	if node.indexCoord != nil {
		log.Debug("Proxy wait for IndexCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.indexCoord, "IndexCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for IndexCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy IndexCoord is ready")
	}

	if node.queryCoord != nil {
		resp, err := node.queryCoord.CreateQueryChannel(node.ctx, &querypb.CreateQueryChannelRequest{})
		if err != nil {
			log.Debug("Proxy CreateQueryChannel failed", zap.Error(err))
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Debug("Proxy CreateQueryChannel failed", zap.String("reason", resp.Status.Reason))

			return errors.New(resp.Status.Reason)
		}
		log.Debug("Proxy CreateQueryChannel success")

		// TODO SearchResultChannelNames and RetrieveResultChannelNames should not be part in the Param table
		// we should maintain a separate map for search result
		Params.SearchResultChannelNames = []string{resp.QueryResultChannel}
		Params.RetrieveResultChannelNames = []string{resp.QueryResultChannel}
		log.Debug("Proxy CreateQueryChannel success", zap.Any("SearchResultChannelNames", Params.SearchResultChannelNames))
		log.Debug("Proxy CreateQueryChannel success", zap.Any("RetrieveResultChannelNames", Params.RetrieveResultChannelNames))
	}

	m := map[string]interface{}{
		"PulsarAddress": Params.PulsarAddress,
		"PulsarBufSize": 1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.rootCoord, Params.ProxyID)
	if err != nil {
		return err
	}

	node.idAllocator = idAllocator

	tsoAllocator, err := newTimestampAllocator(node.ctx, node.rootCoord, Params.ProxyID)
	if err != nil {
		return err
	}
	node.tsoAllocator = tsoAllocator

	segAssigner, err := newSegIDAssigner(node.ctx, node.dataCoord, node.lastTick)
	if err != nil {
		panic(err)
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = Params.ProxyID

	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.rootCoord)
	dqlChannelsFunc := getDqlChannelsFunc(node.ctx, node.session.ServerID, node.queryCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc, dqlChannelsFunc, nil, node.msFactory)
	node.chMgr = chMgr

	node.sched, err = newTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory)
	if err != nil {
		return err
	}

	node.chTicker = newChannelsTimeTicker(node.ctx, channelMgrTickerInterval, []string{}, node.sched.getPChanStatistics, tsoAllocator)

	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	return nil
}

// sendChannelsTimeTickLoop starts a goroutine that synchronize the time tick information.
func (node *Proxy) sendChannelsTimeTickLoop() {
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()

		// TODO(dragondriver): read this from config
		timer := time.NewTicker(sendTimeTickMsgInterval)

		for {
			select {
			case <-node.ctx.Done():
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

				for idx, channel := range channels {
					ts := tss[idx]
					metrics.ProxyDmlChannelTimeTick.WithLabelValues(channel).Set(float64(ts))
				}
				metrics.ProxyDmlChannelTimeTick.WithLabelValues("DefaultTimestamp").Set(float64(maxTs))

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
	err := InitMetaCache(node.rootCoord)
	if err != nil {
		return err
	}
	log.Debug("init global meta cache ...")

	if err := node.sched.Start(); err != nil {
		return err
	}
	log.Debug("start scheduler ...")

	if err := node.idAllocator.Start(); err != nil {
		return err
	}
	log.Debug("start id allocator ...")

	if err := node.segAssigner.Start(); err != nil {
		return err
	}
	log.Debug("start seg assigner ...")

	err = node.chTicker.start()
	if err != nil {
		return err
	}
	log.Debug("start channelsTimeTicker")

	node.sendChannelsTimeTickLoop()

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	Params.CreatedTime = time.Now()
	Params.UpdatedTime = time.Now()

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("Proxy", zap.Any("State", node.stateCode.Load()))

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

func (node *Proxy) lastTick() Timestamp {
	return node.chTicker.getMinTick()
}

// AddCloseCallback adds a callback in the Close phase.
func (node *Proxy) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

// SetRootCoordClient sets rootcoord client for proxy.
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
