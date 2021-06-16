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

package proxynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
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

type ProxyNode struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Value

	masterService types.MasterService
	indexService  types.IndexService
	dataService   types.DataService
	queryService  types.QueryService

	chMgr channelsMgr

	sched *TaskScheduler
	tick  *timeTick

	chTicker channelsTimeTicker

	idAllocator  *allocator.IDAllocator
	tsoAllocator *TimestampAllocator
	segAssigner  *SegIDAssigner

	session *sessionutil.Session

	queryMsgStream msgstream.MsgStream
	msFactory      msgstream.Factory

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func NewProxyNode(ctx context.Context, factory msgstream.Factory) (*ProxyNode, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &ProxyNode{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("ProxyNode", zap.Any("State", node.stateCode.Load()))
	return node, nil

}

// Register register proxy node at etcd
func (node *ProxyNode) Register() error {
	node.session = sessionutil.NewSession(node.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	node.session.Init(typeutil.ProxyNodeRole, Params.NetworkAddress, false)
	Params.ProxyID = node.session.ServerID
	return nil
}

func (node *ProxyNode) Init() error {
	// wait for dataservice state changed to Healthy
	if node.dataService != nil {
		log.Debug("ProxyNode wait for dataService ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.dataService, "DataService", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("ProxyNode wait for dataService ready failed", zap.Error(err))
			return err
		}
		log.Debug("ProxyNode dataService is ready")
	}

	// wait for queryService state changed to Healthy
	if node.queryService != nil {
		log.Debug("ProxyNode wait for queryService ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.queryService, "QueryService", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("ProxyNode wait for queryService ready failed", zap.Error(err))
			return err
		}
		log.Debug("ProxyNode queryService is ready")
	}

	// wait for indexservice state changed to Healthy
	if node.indexService != nil {
		log.Debug("ProxyNode wait for indexService ready")
		err := funcutil.WaitForComponentHealthy(node.ctx, node.indexService, "IndexService", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("ProxyNode wait for indexService ready failed", zap.Error(err))
			return err
		}
		log.Debug("ProxyNode indexService is ready")
	}

	if node.queryService != nil {
		resp, err := node.queryService.CreateQueryChannel(node.ctx, &querypb.CreateQueryChannelRequest{})
		if err != nil {
			log.Debug("ProxyNode CreateQueryChannel failed", zap.Error(err))
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Debug("ProxyNode CreateQueryChannel failed", zap.String("reason", resp.Status.Reason))

			return errors.New(resp.Status.Reason)
		}
		log.Debug("ProxyNode CreateQueryChannel success")

		Params.SearchChannelNames = []string{resp.RequestChannel}
		Params.SearchResultChannelNames = []string{resp.ResultChannel}
		Params.RetrieveChannelNames = []string{resp.RequestChannel}
		Params.RetrieveResultChannelNames = []string{resp.ResultChannel}
		log.Debug("ProxyNode CreateQueryChannel success", zap.Any("SearchChannelNames", Params.SearchChannelNames))
		log.Debug("ProxyNode CreateQueryChannel success", zap.Any("SearchResultChannelNames", Params.SearchResultChannelNames))
		log.Debug("ProxyNode CreateQueryChannel success", zap.Any("RetrieveChannelNames", Params.RetrieveChannelNames))
		log.Debug("ProxyNode CreateQueryChannel success", zap.Any("RetrieveResultChannelNames", Params.RetrieveResultChannelNames))
	}

	// todo
	//Params.InsertChannelNames, err = node.dataService.GetInsertChannels()
	//if err != nil {
	//	return err
	//}

	m := map[string]interface{}{
		"PulsarAddress": Params.PulsarAddress,
		"PulsarBufSize": 1024}
	err := node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	node.queryMsgStream, _ = node.msFactory.NewQueryMsgStream(node.ctx)
	node.queryMsgStream.AsProducer(Params.SearchChannelNames)
	// FIXME(wxyu): use log.Debug instead
	log.Debug("proxynode", zap.Strings("proxynode AsProducer:", Params.SearchChannelNames))
	log.Debug("create query message stream ...")

	idAllocator, err := allocator.NewIDAllocator(node.ctx, Params.MetaRootPath, Params.EtcdEndpoints)

	if err != nil {
		return err
	}
	node.idAllocator = idAllocator
	node.idAllocator.PeerID = Params.ProxyID

	tsoAllocator, err := NewTimestampAllocator(node.ctx, node.masterService, Params.ProxyID)
	if err != nil {
		return err
	}
	node.tsoAllocator = tsoAllocator

	segAssigner, err := NewSegIDAssigner(node.ctx, node.dataService, node.lastTick)
	if err != nil {
		panic(err)
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = Params.ProxyID

	getDmlChannelsFunc := func(collectionID UniqueID) (map[vChan]pChan, error) {
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     0, // todo
				Timestamp: 0, // todo
				SourceID:  0, // todo
			},
			DbName:         "", // todo
			CollectionName: "", // todo
			CollectionID:   collectionID,
			TimeStamp:      0, // todo
		}
		resp, err := node.masterService.DescribeCollection(node.ctx, req)
		if err != nil {
			log.Warn("DescribeCollection", zap.Error(err))
			return nil, err
		}
		if resp.Status.ErrorCode != 0 {
			log.Warn("DescribeCollection",
				zap.Any("ErrorCode", resp.Status.ErrorCode),
				zap.Any("Reason", resp.Status.Reason))
			return nil, err
		}
		if len(resp.VirtualChannelNames) != len(resp.PhysicalChannelNames) {
			err := fmt.Errorf(
				"len(VirtualChannelNames): %v, len(PhysicalChannelNames): %v",
				len(resp.VirtualChannelNames),
				len(resp.PhysicalChannelNames))
			log.Warn("GetDmlChannels", zap.Error(err))
			return nil, err
		}

		ret := make(map[vChan]pChan)
		for idx, name := range resp.VirtualChannelNames {
			if _, ok := ret[name]; ok {
				err := fmt.Errorf(
					"duplicated virtual channel found, vchan: %v, pchan: %v",
					name,
					resp.PhysicalChannelNames[idx])
				return nil, err
			}
			ret[name] = resp.PhysicalChannelNames[idx]
		}

		return ret, nil
	}
	mockQueryService := newMockGetChannelsService()

	chMgr := newChannelsMgr(getDmlChannelsFunc, mockQueryService.GetChannels, node.msFactory)
	node.chMgr = chMgr

	node.sched, err = NewTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory)
	if err != nil {
		return err
	}

	node.tick = newTimeTick(node.ctx, node.tsoAllocator, time.Millisecond*200, node.sched.TaskDoneTest, node.msFactory)

	node.chTicker = newChannelsTimeTicker(node.ctx, channelMgrTickerInterval, []string{}, node.sched.getPChanStatistics, tsoAllocator)

	return nil
}

func (node *ProxyNode) sendChannelsTimeTickLoop() {
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
				stats, err := node.chTicker.getMinTsStatistics()
				if err != nil {
					log.Warn("sendChannelsTimeTickLoop.getMinTsStatistics", zap.Error(err))
					continue
				}

				channels := make([]pChan, 0, len(stats))
				tss := make([]Timestamp, 0, len(stats))

				for channel, ts := range stats {
					channels = append(channels, channel)
					tss = append(tss, ts)
				}
				log.Debug("send timestamp statistics of pchan", zap.Any("channels", channels), zap.Any("tss", tss))

				req := &internalpb.ChannelTimeTickMsg{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_TimeTick, // todo
						MsgID:     0,                         // todo
						Timestamp: 0,                         // todo
						SourceID:  node.session.ServerID,
					},
					ChannelNames: channels,
					Timestamps:   tss,
				}

				status, err := node.masterService.UpdateChannelTimeTick(node.ctx, req)
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

func (node *ProxyNode) Start() error {
	err := InitMetaCache(node.masterService)
	if err != nil {
		return err
	}
	log.Debug("init global meta cache ...")

	initGlobalInsertChannelsMap(node)
	log.Debug("init global insert channels map ...")

	node.queryMsgStream.Start()
	log.Debug("start query message stream ...")

	node.sched.Start()
	log.Debug("start scheduler ...")

	node.idAllocator.Start()
	log.Debug("start id allocator ...")

	node.segAssigner.Start()
	log.Debug("start seg assigner ...")

	node.tick.Start()
	log.Debug("start time tick ...")

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

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("ProxyNode", zap.Any("State", node.stateCode.Load()))

	return nil
}

func (node *ProxyNode) Stop() error {
	node.cancel()

	globalInsertChannelsMap.CloseAllMsgStream()
	node.idAllocator.Close()
	node.segAssigner.Close()
	node.sched.Close()
	node.queryMsgStream.Close()
	node.tick.Close()
	err := node.chTicker.close()
	if err != nil {
		return err
	}

	node.wg.Wait()

	for _, cb := range node.closeCallbacks {
		cb()
	}

	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (node *ProxyNode) AddStartCallback(callbacks ...func()) {
	node.startCallbacks = append(node.startCallbacks, callbacks...)
}

func (node *ProxyNode) lastTick() Timestamp {
	return node.tick.LastTick()
}

// AddCloseCallback adds a callback in the Close phase.
func (node *ProxyNode) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

func (node *ProxyNode) SetMasterClient(cli types.MasterService) {
	node.masterService = cli
}

func (node *ProxyNode) SetIndexServiceClient(cli types.IndexService) {
	node.indexService = cli
}

func (node *ProxyNode) SetDataServiceClient(cli types.DataService) {
	node.dataService = cli
}

func (node *ProxyNode) SetQueryServiceClient(cli types.QueryService) {
	node.queryService = cli
}
