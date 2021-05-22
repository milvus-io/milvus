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
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

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
	proxyService  types.ProxyService
	queryService  types.QueryService

	sched *TaskScheduler
	tick  *timeTick

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
	log.Debug("proxynode",
		zap.Any("state of proxynode", internalpb.StateCode_Abnormal))
	return node, nil

}

// Register register proxy node at etcd
func (node *ProxyNode) Register() error {
	node.session = sessionutil.NewSession(node.ctx, []string{Params.EtcdAddress})
	node.session.Init(typeutil.ProxyNodeRole, Params.NetworkAddress, false)
	Params.ProxyID = node.session.ServerID
	return nil
}

func (node *ProxyNode) Init() error {
	// todo wait for proxyservice state changed to Healthy
	ctx := context.Background()

	err := funcutil.WaitForComponentHealthy(ctx, node.proxyService, "ProxyService", 1000000, time.Millisecond*200)
	if err != nil {
		return err
	}
	log.Debug("service was ready ...")

	request := &proxypb.RegisterNodeRequest{
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.NetworkPort),
		},
	}

	response, err := node.proxyService.RegisterNode(ctx, request)
	if err != nil {
		return err
	}
	if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(response.Status.Reason)
	}

	err = Params.LoadConfigFromInitParams(response.InitParams)
	if err != nil {
		return err
	}

	// wait for dataservice state changed to Healthy
	if node.dataService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.dataService, "DataService", 1000000, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	// wait for queryService state changed to Healthy
	if node.queryService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.queryService, "QueryService", 1000000, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	// wait for indexservice state changed to Healthy
	if node.indexService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.indexService, "IndexService", 1000000, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	if node.queryService != nil {
		resp, err := node.queryService.CreateQueryChannel(ctx)
		if err != nil {
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		Params.SearchChannelNames = []string{resp.RequestChannel}
		Params.SearchResultChannelNames = []string{resp.ResultChannel}
	}

	// todo
	//Params.InsertChannelNames, err = node.dataService.GetInsertChannels()
	//if err != nil {
	//	return err
	//}

	m := map[string]interface{}{
		"PulsarAddress": Params.PulsarAddress,
		"PulsarBufSize": 1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	node.queryMsgStream, _ = node.msFactory.NewQueryMsgStream(node.ctx)
	node.queryMsgStream.AsProducer(Params.SearchChannelNames)
	// FIXME(wxyu): use log.Debug instead
	log.Debug("proxynode", zap.Strings("proxynode AsProducer:", Params.SearchChannelNames))
	log.Debug("create query message stream ...")

	masterAddr := Params.MasterAddress
	idAllocator, err := allocator.NewIDAllocator(node.ctx, masterAddr, []string{Params.EtcdAddress})

	if err != nil {
		return err
	}
	node.idAllocator = idAllocator
	node.idAllocator.PeerID = Params.ProxyID

	tsoAllocator, err := NewTimestampAllocator(node.masterService, Params.ProxyID)
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

	node.sched, err = NewTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory)
	if err != nil {
		return err
	}

	node.tick = newTimeTick(node.ctx, node.tsoAllocator, time.Millisecond*200, node.sched.TaskDoneTest, node.msFactory)

	return nil
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

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("proxynode",
		zap.Any("state of proxynode", internalpb.StateCode_Healthy))
	log.Debug("proxy node is healthy ...")

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

func (node *ProxyNode) SetProxyServiceClient(cli types.ProxyService) {
	node.proxyService = cli
}

func (node *ProxyNode) SetQueryServiceClient(cli types.QueryService) {
	node.queryService = cli
}
