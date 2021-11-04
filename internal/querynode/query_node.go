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

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"

*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const changeInfoMetaPrefix = "queryCoord-sealedSegmentChangeInfo"

// make sure QueryNode implements types.QueryNode
var _ types.QueryNode = (*QueryNode)(nil)

// make sure QueryNode implements types.QueryNodeComponent
var _ types.QueryNodeComponent = (*QueryNode)(nil)

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

	stateCode atomic.Value

	//call once
	initOnce sync.Once

	// internal components
	historical *historical
	streaming  *streaming

	// tSafeReplica
	tSafeReplica TSafeReplicaInterface

	// dataSyncService
	dataSyncService *dataSyncService

	// internal services
	queryService *queryService

	// clients
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	msFactory msgstream.Factory
	scheduler *taskScheduler

	session *sessionutil.Session

	minioKV kv.BaseKV // minio minioKV
	etcdKV  *etcdkv.EtcdKV
}

// NewQueryNode will return a QueryNode with abnormal state.
func NewQueryNode(ctx context.Context, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		queryService:        nil,
		msFactory:           factory,
	}

	node.scheduler = newTaskScheduler(ctx1)
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	return node
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	log.Debug("query node session info", zap.String("metaPath", Params.MetaRootPath), zap.Strings("etcdEndPoints", Params.EtcdEndpoints))
	node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	node.session.Init(typeutil.QueryNodeRole, Params.QueryNodeIP+":"+strconv.FormatInt(Params.QueryNodePort, 10), false)
	// start liveness check
	go node.session.LivenessCheck(node.queryNodeLoopCtx, func() {
		log.Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
	})

	Params.QueryNodeID = node.session.ServerID
	Params.SetLogger(Params.QueryNodeID)
	log.Debug("query nodeID", zap.Int64("nodeID", Params.QueryNodeID))
	log.Debug("query node address", zap.String("address", node.session.Address))

	// This param needs valid QueryNodeID
	Params.initMsgChannelSubName()
	//TODO Reset the logger
	//Params.initLogCfg()
	return nil
}

// InitSegcore set init params of segCore, such as chunckRows, SIMD type...
func (node *QueryNode) InitSegcore() {
	C.SegcoreInit()

	// override segcore chunk size
	cChunkRows := C.int64_t(Params.ChunkRows)
	C.SegcoreSetChunkRows(cChunkRows)

	// override segcore SIMD type
	cSimdType := C.CString(Params.SimdType)
	cRealSimdType := C.SegcoreSetSimdType(cSimdType)
	Params.SimdType = C.GoString(cRealSimdType)
	C.free(unsafe.Pointer(cRealSimdType))
	C.free(unsafe.Pointer(cSimdType))
}

// Init function init historical and streaming module to manage segments
func (node *QueryNode) Init() error {
	var initError error = nil
	node.initOnce.Do(func() {
		//ctx := context.Background()
		connectEtcdFn := func() error {
			etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
			if err != nil {
				return err
			}
			node.etcdKV = etcdKV
			return err
		}
		log.Debug("queryNode try to connect etcd",
			zap.Any("EtcdEndpoints", Params.EtcdEndpoints),
			zap.Any("MetaRootPath", Params.MetaRootPath),
		)
		err := retry.Do(node.queryNodeLoopCtx, connectEtcdFn, retry.Attempts(300))
		if err != nil {
			log.Debug("queryNode try to connect etcd failed", zap.Error(err))
			initError = err
			return
		}
		log.Debug("queryNode try to connect etcd success",
			zap.Any("EtcdEndpoints", Params.EtcdEndpoints),
			zap.Any("MetaRootPath", Params.MetaRootPath),
		)
		node.tSafeReplica = newTSafeReplica()

		streamingReplica := newCollectionReplica(node.etcdKV)
		historicalReplica := newCollectionReplica(node.etcdKV)

		node.historical = newHistorical(node.queryNodeLoopCtx,
			historicalReplica,
			node.rootCoord,
			node.indexCoord,
			node.msFactory,
			node.etcdKV,
			node.tSafeReplica,
		)
		node.streaming = newStreaming(node.queryNodeLoopCtx,
			streamingReplica,
			node.msFactory,
			node.etcdKV,
			node.tSafeReplica,
		)

		node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, streamingReplica, historicalReplica, node.tSafeReplica, node.msFactory)

		node.InitSegcore()

		if node.rootCoord == nil {
			log.Error("null root coordinator detected")
		}

		if node.indexCoord == nil {
			log.Error("null index coordinator detected")
		}
	})

	return initError
}

// Start mainly start QueryNode's query service.
func (node *QueryNode) Start() error {
	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	// init services and manager
	// TODO: pass node.streaming.replica to search service
	node.queryService = newQueryService(node.queryNodeLoopCtx,
		node.historical,
		node.streaming,
		node.msFactory)

	// start task scheduler
	go node.scheduler.Start()

	// start services
	go node.historical.start()
	go node.watchChangeInfo()

	Params.CreatedTime = time.Now()
	Params.UpdatedTime = time.Now()

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	node.queryNodeLoopCancel()

	// close services
	if node.dataSyncService != nil {
		node.dataSyncService.close()
	}
	if node.historical != nil {
		node.historical.close()
	}
	if node.streaming != nil {
		node.streaming.close()
	}
	if node.queryService != nil {
		node.queryService.close()
	}
	return nil
}

// UpdateStateCode updata the state of query node, which can be initializing, healthy, and abnormal
func (node *QueryNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

// SetRootCoord assigns parameter rc to its member rootCoord.
func (node *QueryNode) SetRootCoord(rc types.RootCoord) error {
	if rc == nil {
		return errors.New("null root coordinator interface")
	}
	node.rootCoord = rc
	return nil
}

// SetIndexCoord assigns parameter index to its member indexCoord.
func (node *QueryNode) SetIndexCoord(index types.IndexCoord) error {
	if index == nil {
		return errors.New("null index coordinator interface")
	}
	node.indexCoord = index
	return nil
}

func (node *QueryNode) watchChangeInfo() {
	log.Debug("query node watchChangeInfo start")
	watchChan := node.etcdKV.WatchWithPrefix(changeInfoMetaPrefix)

	for {
		select {
		case <-node.queryNodeLoopCtx.Done():
			log.Debug("query node watchChangeInfo close")
			return
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case mvccpb.PUT:
					infoID, err := strconv.ParseInt(filepath.Base(string(event.Kv.Key)), 10, 64)
					if err != nil {
						log.Warn("Parse SealedSegmentsChangeInfo id failed", zap.Any("error", err.Error()))
						continue
					}
					log.Debug("get SealedSegmentsChangeInfo from etcd",
						zap.Any("infoID", infoID),
					)
					info := &querypb.SealedSegmentsChangeInfo{}
					err = proto.Unmarshal(event.Kv.Value, info)
					if err != nil {
						log.Warn("Unmarshal SealedSegmentsChangeInfo failed", zap.Any("error", err.Error()))
						continue
					}
					go func() {
						err = node.adjustByChangeInfo(info)
						if err != nil {
							log.Warn("adjustByChangeInfo failed", zap.Any("error", err.Error()))
						}
					}()
				default:
					// do nothing
				}
			}
		}
	}
}

func (node *QueryNode) waitChangeInfo(segmentChangeInfos *querypb.SealedSegmentsChangeInfo) error {
	fn := func() error {
		for _, info := range segmentChangeInfos.Infos {
			canDoLoadBalance := true
			// Check online segments:
			for _, segmentInfo := range info.OnlineSegments {
				if node.queryService.hasQueryCollection(segmentInfo.CollectionID) {
					qc, err := node.queryService.getQueryCollection(segmentInfo.CollectionID)
					if err != nil {
						canDoLoadBalance = false
						break
					}
					if info.OnlineNodeID == Params.QueryNodeID && !qc.globalSegmentManager.hasGlobalSegment(segmentInfo.SegmentID) {
						canDoLoadBalance = false
						break
					}
				}
			}
			// Check offline segments:
			for _, segmentInfo := range info.OfflineSegments {
				if node.queryService.hasQueryCollection(segmentInfo.CollectionID) {
					qc, err := node.queryService.getQueryCollection(segmentInfo.CollectionID)
					if err != nil {
						canDoLoadBalance = false
						break
					}
					if info.OfflineNodeID == Params.QueryNodeID && qc.globalSegmentManager.hasGlobalSegment(segmentInfo.SegmentID) {
						canDoLoadBalance = false
						break
					}
				}
			}
			if canDoLoadBalance {
				return nil
			}
			return errors.New(fmt.Sprintln("waitChangeInfo failed, infoID = ", segmentChangeInfos.Base.GetMsgID()))
		}

		return nil
	}

	return retry.Do(context.TODO(), fn, retry.Attempts(10))
}

func (node *QueryNode) adjustByChangeInfo(segmentChangeInfos *querypb.SealedSegmentsChangeInfo) error {
	err := node.waitChangeInfo(segmentChangeInfos)
	if err != nil {
		log.Error("waitChangeInfo failed", zap.Any("error", err.Error()))
		return err
	}

	node.streaming.replica.queryLock()
	node.historical.replica.queryLock()
	defer node.streaming.replica.queryUnlock()
	defer node.historical.replica.queryUnlock()
	for _, info := range segmentChangeInfos.Infos {
		// For online segments:
		for _, segmentInfo := range info.OnlineSegments {
			// delete growing segment because these segments are loaded in historical.
			hasGrowingSegment := node.streaming.replica.hasSegment(segmentInfo.SegmentID)
			if hasGrowingSegment {
				err := node.streaming.replica.removeSegment(segmentInfo.SegmentID)
				if err != nil {

					return err
				}
				log.Debug("remove growing segment in adjustByChangeInfo",
					zap.Any("collectionID", segmentInfo.CollectionID),
					zap.Any("segmentID", segmentInfo.SegmentID),
					zap.Any("infoID", segmentChangeInfos.Base.GetMsgID()),
				)
			}
		}

		// For offline segments:
		for _, segment := range info.OfflineSegments {
			// load balance or compaction, remove old sealed segments.
			if info.OfflineNodeID == Params.QueryNodeID {
				err := node.historical.replica.removeSegment(segment.SegmentID)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
