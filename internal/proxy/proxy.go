// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

const sendTimeTickMsgInterval = 200 * time.Millisecond
const channelMgrTickerInterval = 100 * time.Millisecond

// make sure Proxy implements types.Proxy
var _ types.Proxy = (*Proxy)(nil)

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
	log.Debug("Proxy", zap.Any("State", node.stateCode.Load()))
	return node, nil

}

// Register register proxy at etcd
func (node *Proxy) Register() error {
	node.session = sessionutil.NewSession(node.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	node.session.Init(typeutil.ProxyRole, Params.NetworkAddress, false)
	Params.ProxyID = node.session.ServerID
	Params.SetLogger(Params.ProxyID)
	Params.initProxySubName()
	// TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	// wait for datacoord state changed to Healthy
	if node.dataCoord != nil {
		log.Debug("Proxy wait for dataCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.dataCoord, "DataCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for dataCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy dataCoord is ready")
	}

	// wait for queryCoord state changed to Healthy
	if node.queryCoord != nil {
		log.Debug("Proxy wait for queryCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.queryCoord, "QueryCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for queryCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy queryCoord is ready")
	}

	// wait for indexcoord state changed to Healthy
	if node.indexCoord != nil {
		log.Debug("Proxy wait for indexCoord ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.indexCoord, "IndexCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("Proxy wait for indexCoord ready failed", zap.Error(err))
			return err
		}
		log.Debug("Proxy indexCoord is ready")
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
		Params.SearchResultChannelNames = []string{resp.ResultChannel}
		Params.RetrieveResultChannelNames = []string{resp.ResultChannel}
		log.Debug("Proxy CreateQueryChannel success", zap.Any("SearchResultChannelNames", Params.SearchResultChannelNames))
		log.Debug("Proxy CreateQueryChannel success", zap.Any("RetrieveResultChannelNames", Params.RetrieveResultChannelNames))
	}

	m := map[string]interface{}{
		"PulsarAddress": Params.PulsarAddress,
		"PulsarBufSize": 1024}
	err := node.msFactory.SetParams(m)
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
				ts, err := node.tsoAllocator.AllocOne()
				if err != nil {
					log.Warn("Failed to get timestamp from tso", zap.Error(err))
					continue
				}

				stats, err := node.chTicker.getMinTsStatistics()
				if err != nil {
					log.Warn("sendChannelsTimeTickLoop.getMinTsStatistics", zap.Error(err))
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
	}
	if node.segAssigner != nil {
		node.segAssigner.Close()
	}
	if node.sched != nil {
		node.sched.Close()
	}
	if node.chTicker != nil {
		err := node.chTicker.close()
		if err != nil {
			return err
		}
	}

	node.wg.Wait()

	for _, cb := range node.closeCallbacks {
		cb()
	}

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

// SetRootCoordClient set rootcoord client for proxy.
func (node *Proxy) SetRootCoordClient(cli types.RootCoord) {
	node.rootCoord = cli
}

func (node *Proxy) SetIndexCoordClient(cli types.IndexCoord) {
	node.indexCoord = cli
}

func (node *Proxy) SetDataCoordClient(cli types.DataCoord) {
	node.dataCoord = cli
}

func (node *Proxy) SetQueryCoordClient(cli types.QueryCoord) {
	node.queryCoord = cli
}
