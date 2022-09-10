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

package querynode

/*
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// make sure QueryNode implements types.QueryNode
var _ types.QueryNode = (*QueryNode)(nil)

// make sure QueryNode implements types.QueryNodeComponent
var _ types.QueryNodeComponent = (*QueryNode)(nil)

var Params paramtable.ComponentParam

// QueryNode communicates with outside services and union all
// services in querynode package.
//
// QueryNode implements `types.Component`, `types.QueryNode` interfaces.
//  `rootCoord` is a grpc client of root coordinator.
//  `indexCoord` is a grpc client of index coordinator.
//  `stateCode` is current statement of this query node, indicating whether it's healthy.
type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	wg sync.WaitGroup

	stateCode atomic.Value

	//call once
	initOnce sync.Once

	// internal components
	metaReplica ReplicaInterface

	// tSafeReplica
	tSafeReplica TSafeReplicaInterface

	// dataSyncService
	dataSyncService *dataSyncService

	// segment loader
	loader *segmentLoader

	// etcd client
	etcdCli *clientv3.Client

	factory   dependency.Factory
	scheduler *taskScheduler

	session *sessionutil.Session
	eventCh <-chan *sessionutil.SessionEvent

	vectorStorage storage.ChunkManager
	cacheStorage  storage.ChunkManager
	etcdKV        *etcdkv.EtcdKV

	// shard cluster service, handle shard leader functions
	ShardClusterService *ShardClusterService
	//shard query service, handles shard-level query & search
	queryShardService *queryShardService

	// cgoPool is the worker pool to control concurrency of cgo call
	cgoPool *concurrency.Pool
}

// NewQueryNode will return a QueryNode with abnormal state.
func NewQueryNode(ctx context.Context, factory dependency.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		factory:             factory,
	}

	node.tSafeReplica = newTSafeReplica()
	node.scheduler = newTaskScheduler(ctx1, node.tSafeReplica)
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	return node
}

func (node *QueryNode) initSession() error {
	node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath, node.etcdCli)
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, Params.QueryNodeCfg.QueryNodeIP+":"+strconv.FormatInt(Params.QueryNodeCfg.QueryNodePort, 10), false, true)
	Params.QueryNodeCfg.SetNodeID(node.session.ServerID)
	Params.SetLogger(Params.QueryNodeCfg.GetNodeID())
	log.Info("QueryNode init session", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	node.session.Register()
	// start liveness check
	go node.session.LivenessCheck(node.queryNodeLoopCtx, func() {
		log.Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	//TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

// InitSegcore set init params of segCore, such as chunckRows, SIMD type...
func (node *QueryNode) InitSegcore() {
	cEasyloggingYaml := C.CString(path.Join(Params.BaseTable.GetConfigDir(), paramtable.DefaultEasyloggingYaml))
	C.SegcoreInit(cEasyloggingYaml)
	C.free(unsafe.Pointer(cEasyloggingYaml))

	// override segcore chunk size
	cChunkRows := C.int64_t(Params.QueryNodeCfg.ChunkRows)
	C.SegcoreSetChunkRows(cChunkRows)

	nlist := C.int64_t(Params.QueryNodeCfg.SmallIndexNlist)
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(Params.QueryNodeCfg.SmallIndexNProbe)
	C.SegcoreSetNprobe(nprobe)

	// override segcore SIMD type
	cSimdType := C.CString(Params.CommonCfg.SimdType)
	cRealSimdType := C.SegcoreSetSimdType(cSimdType)
	Params.CommonCfg.SimdType = C.GoString(cRealSimdType)
	C.free(unsafe.Pointer(cRealSimdType))
	C.free(unsafe.Pointer(cSimdType))

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(Params.CommonCfg.IndexSliceSize)
	C.SegcoreSetIndexSliceSize(cIndexSliceSize)
}

// Init function init historical and streaming module to manage segments
func (node *QueryNode) Init() error {
	var initError error = nil
	node.initOnce.Do(func() {
		//ctx := context.Background()
		log.Info("QueryNode session info", zap.String("metaPath", Params.EtcdCfg.MetaRootPath))
		err := node.initSession()
		if err != nil {
			log.Error("QueryNode init session failed", zap.Error(err))
			initError = err
			return
		}

		node.factory.Init(&Params)

		node.vectorStorage, err = node.factory.NewVectorStorageChunkManager(node.queryNodeLoopCtx)
		if err != nil {
			log.Error("QueryNode init vector storage failed", zap.Error(err))
			initError = err
			return
		}

		node.cacheStorage, err = node.factory.NewCacheStorageChunkManager(node.queryNodeLoopCtx)
		if err != nil {
			log.Error("QueryNode init cache storage failed", zap.Error(err))
			initError = err
			return
		}

		node.etcdKV = etcdkv.NewEtcdKV(node.etcdCli, Params.EtcdCfg.MetaRootPath)
		log.Info("queryNode try to connect etcd success", zap.Any("MetaRootPath", Params.EtcdCfg.MetaRootPath))

		cpuNum := runtime.GOMAXPROCS(0)
		node.cgoPool, err = concurrency.NewPool(cpuNum, ants.WithPreAlloc(true))
		if err != nil {
			log.Error("QueryNode init cgo pool failed", zap.Error(err))
			initError = err
			return
		}

		// ensure every cgopool go routine is locked with a OS thread
		// so openmp in knowhere won't create too much request
		sig := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(cpuNum)
		for i := 0; i < cpuNum; i++ {
			node.cgoPool.Submit(func() (interface{}, error) {
				runtime.LockOSThread()
				wg.Done()
				<-sig
				return nil, nil
			})
		}
		wg.Wait()
		close(sig)

		node.metaReplica = newCollectionReplica(node.cgoPool)

		node.loader = newSegmentLoader(
			node.metaReplica,
			node.etcdKV,
			node.vectorStorage,
			node.factory,
			node.cgoPool)

		node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.metaReplica, node.tSafeReplica, node.factory)

		node.InitSegcore()

		log.Info("query node init successfully",
			zap.Any("queryNodeID", Params.QueryNodeCfg.GetNodeID()),
			zap.Any("IP", Params.QueryNodeCfg.QueryNodeIP),
			zap.Any("Port", Params.QueryNodeCfg.QueryNodePort),
		)
	})

	return initError
}

// Start mainly start QueryNode's query service.
func (node *QueryNode) Start() error {
	// start task scheduler
	go node.scheduler.Start()

	// start services
	go node.watchChangeInfo()

	// create shardClusterService for shardLeader functions.
	node.ShardClusterService = newShardClusterService(node.etcdCli, node.session, node)
	// create shard-level query service
	node.queryShardService = newQueryShardService(node.queryNodeLoopCtx, node.metaReplica, node.tSafeReplica,
		node.ShardClusterService, node.factory, node.scheduler)

	Params.QueryNodeCfg.CreatedTime = time.Now()
	Params.QueryNodeCfg.UpdatedTime = time.Now()

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Info("query node start successfully",
		zap.Any("queryNodeID", Params.QueryNodeCfg.GetNodeID()),
		zap.Any("IP", Params.QueryNodeCfg.QueryNodeIP),
		zap.Any("Port", Params.QueryNodeCfg.QueryNodePort),
	)
	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	log.Warn("Query node stop..")
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	node.queryNodeLoopCancel()

	// close services
	if node.dataSyncService != nil {
		node.dataSyncService.close()
	}

	if node.metaReplica != nil {
		node.metaReplica.freeAll()
	}

	if node.queryShardService != nil {
		node.queryShardService.close()
	}

	node.session.Revoke(time.Second)
	node.wg.Wait()
	return nil
}

// UpdateStateCode updata the state of query node, which can be initializing, healthy, and abnormal
func (node *QueryNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (node *QueryNode) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}

func (node *QueryNode) watchChangeInfo() {
	log.Info("query node watchChangeInfo start")
	watchChan := node.etcdKV.WatchWithPrefix(util.ChangeInfoMetaPrefix)
	for {
		select {
		case <-node.queryNodeLoopCtx.Done():
			log.Info("query node watchChangeInfo close")
			return
		case resp, ok := <-watchChan:
			if !ok {
				log.Warn("querynode failed to watch channel, return")
				return
			}

			if err := resp.Err(); err != nil {
				log.Warn("query watch channel canceled", zap.Error(resp.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				if resp.Err() == v3rpc.ErrCompacted {
					go node.watchChangeInfo()
					return
				}
				// if watch loop return due to event canceled, the datanode is not functional anymore
				log.Panic("querynoe3 is not functional for event canceled", zap.Error(err))
				return
			}
			for _, event := range resp.Events {
				switch event.Type {
				case mvccpb.PUT:
					infoID, err := strconv.ParseInt(filepath.Base(string(event.Kv.Key)), 10, 64)
					if err != nil {
						log.Warn("Parse SealedSegmentsChangeInfo id failed", zap.Any("error", err.Error()))
						continue
					}
					log.Info("get SealedSegmentsChangeInfo from etcd",
						zap.Any("infoID", infoID),
					)
					info := &querypb.SealedSegmentsChangeInfo{}
					err = proto.Unmarshal(event.Kv.Value, info)
					if err != nil {
						log.Warn("Unmarshal SealedSegmentsChangeInfo failed", zap.Any("error", err.Error()))
						continue
					}
					go node.handleSealedSegmentsChangeInfo(info)
				default:
					// do nothing
				}
			}
		}
	}
}

func (node *QueryNode) handleSealedSegmentsChangeInfo(info *querypb.SealedSegmentsChangeInfo) {
	for _, line := range info.GetInfos() {
		result := splitSegmentsChange(line)

		for vchannel, changeInfo := range result {
			err := node.ShardClusterService.HandoffVChannelSegments(vchannel, changeInfo)
			if err != nil {
				log.Warn("failed to handle vchannel segments", zap.String("vchannel", vchannel))
			}
		}
	}
}

// splitSegmentsChange returns rearranged segmentChangeInfo in vchannel dimension
func splitSegmentsChange(changeInfo *querypb.SegmentChangeInfo) map[string]*querypb.SegmentChangeInfo {
	result := make(map[string]*querypb.SegmentChangeInfo)

	for _, segment := range changeInfo.GetOnlineSegments() {
		dmlChannel := segment.GetDmChannel()
		info, has := result[dmlChannel]
		if !has {
			info = &querypb.SegmentChangeInfo{
				OnlineNodeID:  changeInfo.OnlineNodeID,
				OfflineNodeID: changeInfo.OfflineNodeID,
			}

			result[dmlChannel] = info
		}

		info.OnlineSegments = append(info.OnlineSegments, segment)
	}

	for _, segment := range changeInfo.GetOfflineSegments() {
		dmlChannel := segment.GetDmChannel()
		info, has := result[dmlChannel]
		if !has {
			info = &querypb.SegmentChangeInfo{
				OnlineNodeID:  changeInfo.OnlineNodeID,
				OfflineNodeID: changeInfo.OfflineNodeID,
			}

			result[dmlChannel] = info
		}

		info.OfflineSegments = append(info.OfflineSegments, segment)
	}

	return result
}
