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

// Package datanode implements data persistence logic.
//
// Data node persists insert logs into persistent storage like minIO/S3.
package datanode

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	// ConnectEtcdMaxRetryTime is used to limit the max retry time for connection etcd
	ConnectEtcdMaxRetryTime = 100
)

var getFlowGraphServiceAttempts = uint(50)

// makes sure DataNode implements types.DataNode
var _ types.DataNode = (*DataNode)(nil)

// Params from config.yaml
var Params *paramtable.ComponentParam = paramtable.Get()

// DataNode communicates with outside services and unioun all
// services in datanode package.
//
// DataNode implements `types.Component`, `types.DataNode` interfaces.
//
//	`etcdCli`   is a connection of etcd
//	`rootCoord` is a grpc client of root coordinator.
//	`dataCoord` is a grpc client of data service.
//	`stateCode` is current statement of this data node, indicating whether it's healthy.
//
//	`clearSignal` is a signal channel for releasing the flowgraph resources.
//	`segmentCache` stores all flushing and flushed segments.
type DataNode struct {
	ctx              context.Context
	cancel           context.CancelFunc
	Role             string
	stateCode        atomic.Value // commonpb.StateCode_Initializing
	flowgraphManager *flowgraphManager
	eventManagerMap  *typeutil.ConcurrentMap[string, *channelEventManager]

	clearSignal        chan string // vchannel name
	segmentCache       *Cache
	compactionExecutor *compactionExecutor
	timeTickSender     *timeTickSender

	etcdCli   *clientv3.Client
	address   string
	rootCoord types.RootCoordClient
	dataCoord types.DataCoordClient
	broker    broker.Broker

	// call once
	initOnce     sync.Once
	startOnce    sync.Once
	stopOnce     sync.Once
	stopWaiter   sync.WaitGroup
	sessionMu    sync.Mutex // to fix data race
	session      *sessionutil.Session
	watchKv      kv.WatchKV
	chunkManager storage.ChunkManager
	allocator    allocator.Allocator

	closer io.Closer

	dispClient msgdispatcher.Client
	factory    dependency.Factory

	reportImportRetryTimes uint // unitest set this value to 1 to save time, default is 10
}

// NewDataNode will return a DataNode with abnormal state.
func NewDataNode(ctx context.Context, factory dependency.Factory) *DataNode {
	rand.Seed(time.Now().UnixNano())
	ctx2, cancel2 := context.WithCancel(ctx)
	node := &DataNode{
		ctx:    ctx2,
		cancel: cancel2,
		Role:   typeutil.DataNodeRole,

		rootCoord:          nil,
		dataCoord:          nil,
		factory:            factory,
		segmentCache:       newCache(),
		compactionExecutor: newCompactionExecutor(),

		eventManagerMap:  typeutil.NewConcurrentMap[string, *channelEventManager](),
		flowgraphManager: newFlowgraphManager(),
		clearSignal:      make(chan string, 100),

		reportImportRetryTimes: 10,
	}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	return node
}

func (node *DataNode) SetAddress(address string) {
	node.address = address
}

func (node *DataNode) GetAddress() string {
	return node.address
}

// SetEtcdClient sets etcd client for DataNode
func (node *DataNode) SetEtcdClient(etcdCli *clientv3.Client) {
	node.etcdCli = etcdCli
}

// SetRootCoordClient sets RootCoord's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetRootCoordClient(rc types.RootCoordClient) error {
	switch {
	case rc == nil, node.rootCoord != nil:
		return errors.New("nil parameter or repeatedly set")
	default:
		node.rootCoord = rc
		return nil
	}
}

// SetDataCoordClient sets data service's grpc client, error is returned if repeatedly set.
func (node *DataNode) SetDataCoordClient(ds types.DataCoordClient) error {
	switch {
	case ds == nil, node.dataCoord != nil:
		return errors.New("nil parameter or repeatedly set")
	default:
		node.dataCoord = ds
		return nil
	}
}

// Register register datanode to etcd
func (node *DataNode) Register() error {
	node.session.Register()

	metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.DataNodeRole).Inc()
	log.Info("DataNode Register Finished")
	// Start liveness check
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Data Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.GetSession().ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.DataNodeRole).Dec()
		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	return nil
}

func (node *DataNode) initSession() error {
	node.session = sessionutil.NewSession(node.ctx, Params.EtcdCfg.MetaRootPath.GetValue(), node.etcdCli)
	if node.session == nil {
		return errors.New("failed to initialize session")
	}
	node.session.Init(typeutil.DataNodeRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.DataNodeRole, node.session.ServerID)
	return nil
}

// initRateCollector creates and starts rateCollector in QueryNode.
func (node *DataNode) initRateCollector() error {
	err := initGlobalRateCollector()
	if err != nil {
		return err
	}
	rateCol.Register(metricsinfo.InsertConsumeThroughput)
	rateCol.Register(metricsinfo.DeleteConsumeThroughput)
	return nil
}

func (node *DataNode) Init() error {
	var initError error
	node.initOnce.Do(func() {
		logutil.Logger(node.ctx).Info("DataNode server initializing",
			zap.String("TimeTickChannelName", Params.CommonCfg.DataCoordTimeTick.GetValue()),
		)
		if err := node.initSession(); err != nil {
			log.Error("DataNode server init session failed", zap.Error(err))
			initError = err
			return
		}

		node.broker = broker.NewCoordBroker(node.rootCoord, node.dataCoord)

		err := node.initRateCollector()
		if err != nil {
			log.Error("DataNode server init rateCollector failed", zap.Int64("node ID", paramtable.GetNodeID()), zap.Error(err))
			initError = err
			return
		}
		log.Info("DataNode server init rateCollector done", zap.Int64("node ID", paramtable.GetNodeID()))

		node.dispClient = msgdispatcher.NewClient(node.factory, typeutil.DataNodeRole, paramtable.GetNodeID())
		log.Info("DataNode server init dispatcher client done", zap.Int64("node ID", paramtable.GetNodeID()))

		alloc, err := allocator.New(context.Background(), node.rootCoord, paramtable.GetNodeID())
		if err != nil {
			log.Error("failed to create id allocator",
				zap.Error(err),
				zap.String("role", typeutil.DataNodeRole), zap.Int64("DataNode ID", paramtable.GetNodeID()))
			initError = err
			return
		}
		node.allocator = alloc

		node.factory.Init(Params)
		log.Info("DataNode server init succeeded",
			zap.String("MsgChannelSubName", Params.CommonCfg.DataNodeSubName.GetValue()))
	})
	return initError
}

// handleChannelEvt handles event from kv watch event
func (node *DataNode) handleChannelEvt(evt *clientv3.Event) {
	var e *event
	switch evt.Type {
	case clientv3.EventTypePut: // datacoord shall put channels needs to be watched here
		e = &event{
			eventType: putEventType,
			version:   evt.Kv.Version,
		}

	case clientv3.EventTypeDelete:
		e = &event{
			eventType: deleteEventType,
			version:   evt.Kv.Version,
		}
	}
	node.handleWatchInfo(e, string(evt.Kv.Key), evt.Kv.Value)
}

// tryToReleaseFlowgraph tries to release a flowgraph
func (node *DataNode) tryToReleaseFlowgraph(vChanName string) {
	log.Info("try to release flowgraph", zap.String("vChanName", vChanName))
	node.flowgraphManager.release(vChanName)
}

// BackGroundGC runs in background to release datanode resources
// GOOSE TODO: remove background GC, using ToRelease for drop-collection after #15846
func (node *DataNode) BackGroundGC(vChannelCh <-chan string) {
	defer node.stopWaiter.Done()
	log.Info("DataNode Background GC Start")
	for {
		select {
		case vchanName := <-vChannelCh:
			node.tryToReleaseFlowgraph(vchanName)
		case <-node.ctx.Done():
			log.Warn("DataNode context done, exiting background GC")
			return
		}
	}
}

// Start will update DataNode state to HEALTHY
func (node *DataNode) Start() error {
	var startErr error
	node.startOnce.Do(func() {
		if err := node.allocator.Start(); err != nil {
			log.Error("failed to start id allocator", zap.Error(err), zap.String("role", typeutil.DataNodeRole))
			startErr = err
			return
		}
		log.Info("start id allocator done", zap.String("role", typeutil.DataNodeRole))

		/*
			rep, err := node.rootCoord.AllocTimestamp(node.ctx, &rootcoordpb.AllocTimestampRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
					commonpbutil.WithMsgID(0),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				Count: 1,
			})
			if err != nil || rep.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				log.Warn("fail to alloc timestamp", zap.Any("rep", rep), zap.Error(err))
				startErr = errors.New("DataNode fail to alloc timestamp")
				return
			}*/

		connectEtcdFn := func() error {
			etcdKV := etcdkv.NewEtcdKV(node.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
			node.watchKv = etcdKV
			return nil
		}
		err := retry.Do(node.ctx, connectEtcdFn, retry.Attempts(ConnectEtcdMaxRetryTime))
		if err != nil {
			startErr = errors.New("DataNode fail to connect etcd")
			return
		}

		chunkManager, err := node.factory.NewPersistentStorageChunkManager(node.ctx)
		if err != nil {
			startErr = err
			return
		}

		node.chunkManager = chunkManager

		node.stopWaiter.Add(1)
		go node.BackGroundGC(node.clearSignal)

		go node.compactionExecutor.start(node.ctx)

		if Params.DataNodeCfg.DataNodeTimeTickByRPC.GetAsBool() {
			node.timeTickSender = newTimeTickSender(node.broker, node.session.ServerID)
			go node.timeTickSender.start(node.ctx)
		}

		node.stopWaiter.Add(1)
		// Start node watch node
		go node.StartWatchChannels(node.ctx)

		node.stopWaiter.Add(1)
		go node.flowgraphManager.start(&node.stopWaiter)

		node.UpdateStateCode(commonpb.StateCode_Healthy)
	})
	return startErr
}

// UpdateStateCode updates datanode's state code
func (node *DataNode) UpdateStateCode(code commonpb.StateCode) {
	node.stateCode.Store(code)
}

// GetStateCode return datanode's state code
func (node *DataNode) GetStateCode() commonpb.StateCode {
	return node.stateCode.Load().(commonpb.StateCode)
}

func (node *DataNode) isHealthy() bool {
	return node.GetStateCode() == commonpb.StateCode_Healthy
}

// ReadyToFlush tells whether DataNode is ready for flushing
func (node *DataNode) ReadyToFlush() error {
	if !node.isHealthy() {
		return errors.New("DataNode not in HEALTHY state")
	}
	return nil
}

// Stop will release DataNode resources and shutdown datanode
func (node *DataNode) Stop() error {
	node.stopOnce.Do(func() {
		// https://github.com/milvus-io/milvus/issues/12282
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.flowgraphManager.close()
		// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed the flow graph
		node.cancel()

		node.eventManagerMap.Range(func(_ string, m *channelEventManager) bool {
			m.Close()
			return true
		})

		if node.allocator != nil {
			log.Info("close id allocator", zap.String("role", typeutil.DataNodeRole))
			node.allocator.Close()
		}

		if node.closer != nil {
			node.closer.Close()
		}

		if node.session != nil {
			node.session.Stop()
		}

		node.stopWaiter.Wait()
	})
	return nil
}

// to fix data race
func (node *DataNode) SetSession(session *sessionutil.Session) {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	node.session = session
}

// to fix data race
func (node *DataNode) GetSession() *sessionutil.Session {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	return node.session
}
