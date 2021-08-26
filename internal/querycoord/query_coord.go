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

package querycoord

import (
	"context"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

type queryChannelInfo struct {
	requestChannel  string
	responseChannel string
}

type QueryCoord struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc
	loopWg     sync.WaitGroup
	kvClient   *etcdkv.EtcdKV

	queryCoordID uint64
	meta         Meta
	cluster      *queryNodeCluster
	newNodeFn    newQueryNodeFn
	scheduler    *TaskScheduler

	dataCoordClient types.DataCoord
	rootCoordClient types.RootCoord

	session   *sessionutil.Session
	eventChan <-chan *sessionutil.SessionEvent

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool

	msFactory msgstream.Factory
}

// Register register query service at etcd
func (qc *QueryCoord) Register() error {
	log.Debug("query coord session info", zap.String("metaPath", Params.MetaRootPath), zap.Strings("etcdEndPoints", Params.EtcdEndpoints), zap.String("address", Params.Address))
	qc.session = sessionutil.NewSession(qc.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	qc.session.Init(typeutil.QueryCoordRole, Params.Address, true)
	Params.NodeID = uint64(qc.session.ServerID)
	return nil
}

func (qc *QueryCoord) Init() error {
	connectEtcdFn := func() error {
		etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		if err != nil {
			return err
		}
		qc.kvClient = etcdKV
		return nil
	}
	log.Debug("query coordinator try to connect etcd")
	err := retry.Do(qc.loopCtx, connectEtcdFn, retry.Attempts(300))
	if err != nil {
		log.Debug("query coordinator try to connect etcd failed", zap.Error(err))
		return err
	}
	log.Debug("query coordinator try to connect etcd success")
	qc.meta, err = newMeta(qc.kvClient)
	if err != nil {
		log.Error("query coordinator init meta failed", zap.Error(err))
		return err
	}

	qc.cluster, err = newQueryNodeCluster(qc.meta, qc.kvClient, qc.newNodeFn)
	if err != nil {
		log.Error("query coordinator init cluster failed", zap.Error(err))
		return err
	}

	qc.scheduler, err = NewTaskScheduler(qc.loopCtx, qc.meta, qc.cluster, qc.kvClient, qc.rootCoordClient, qc.dataCoordClient)
	if err != nil {
		log.Error("query coordinator init task scheduler failed", zap.Error(err))
		return err
	}

	return nil
}

func (qc *QueryCoord) Start() error {
	qc.scheduler.Start()
	log.Debug("start scheduler ...")
	qc.UpdateStateCode(internalpb.StateCode_Healthy)

	qc.loopWg.Add(1)
	go qc.watchNodeLoop()

	qc.loopWg.Add(1)
	go qc.watchMetaLoop()

	return nil
}

func (qc *QueryCoord) Stop() error {
	qc.scheduler.Close()
	log.Debug("close scheduler ...")
	qc.loopCancel()
	qc.UpdateStateCode(internalpb.StateCode_Abnormal)

	qc.loopWg.Wait()
	return nil
}

func (qc *QueryCoord) UpdateStateCode(code internalpb.StateCode) {
	qc.stateCode.Store(code)
}

func NewQueryCoord(ctx context.Context, factory msgstream.Factory) (*QueryCoord, error) {
	rand.Seed(time.Now().UnixNano())
	queryChannels := make([]*queryChannelInfo, 0)
	channelID := len(queryChannels)
	searchPrefix := Params.SearchChannelPrefix
	searchResultPrefix := Params.SearchResultChannelPrefix
	allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(int64(channelID), 10)

	queryChannels = append(queryChannels, &queryChannelInfo{
		requestChannel:  allocatedQueryChannel,
		responseChannel: allocatedQueryResultChannel,
	})

	ctx1, cancel := context.WithCancel(ctx)
	service := &QueryCoord{
		loopCtx:    ctx1,
		loopCancel: cancel,
		msFactory:  factory,
		newNodeFn:  newQueryNode,
	}

	service.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("query coordinator", zap.Any("queryChannels", queryChannels))
	return service, nil
}

func (qc *QueryCoord) SetRootCoord(rootCoord types.RootCoord) {
	qc.rootCoordClient = rootCoord
}

func (qc *QueryCoord) SetDataCoord(dataCoord types.DataCoord) {
	qc.dataCoordClient = dataCoord
}

func (qc *QueryCoord) watchNodeLoop() {
	ctx, cancel := context.WithCancel(qc.loopCtx)
	defer cancel()
	defer qc.loopWg.Done()
	log.Debug("query coordinator start watch node loop")

	clusterStartSession, version, _ := qc.session.GetSessions(typeutil.QueryNodeRole)
	sessionMap := make(map[int64]*sessionutil.Session)
	for _, session := range clusterStartSession {
		nodeID := session.ServerID
		sessionMap[nodeID] = session
	}
	for nodeID, session := range sessionMap {
		if _, ok := qc.cluster.nodes[nodeID]; !ok {
			serverID := session.ServerID
			log.Debug("start add a queryNode to cluster", zap.Any("nodeID", serverID))
			err := qc.cluster.registerNode(ctx, session, serverID)
			if err != nil {
				log.Error("query node failed to register", zap.Int64("nodeID", serverID), zap.String("error info", err.Error()))
			}
		}
	}
	for nodeID := range qc.cluster.nodes {
		if _, ok := sessionMap[nodeID]; !ok {
			qc.cluster.stopNode(nodeID)
			loadBalanceSegment := &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_LoadBalanceSegments,
					SourceID: qc.session.ServerID,
				},
				SourceNodeIDs: []int64{nodeID},
			}

			loadBalanceTask := &LoadBalanceTask{
				BaseTask: BaseTask{
					ctx:              qc.loopCtx,
					Condition:        NewTaskCondition(qc.loopCtx),
					triggerCondition: querypb.TriggerCondition_nodeDown,
				},
				LoadBalanceRequest: loadBalanceSegment,
				rootCoord:          qc.rootCoordClient,
				dataCoord:          qc.dataCoordClient,
				cluster:            qc.cluster,
				meta:               qc.meta,
			}
			qc.scheduler.Enqueue([]task{loadBalanceTask})
		}
	}

	qc.eventChan = qc.session.WatchServices(typeutil.QueryNodeRole, version+1)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-qc.eventChan:
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				log.Debug("start add a queryNode to cluster", zap.Any("nodeID", serverID))
				err := qc.cluster.registerNode(ctx, event.Session, serverID)
				if err != nil {
					log.Error("query node failed to register", zap.Int64("nodeID", serverID), zap.String("error info", err.Error()))
				}
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Debug("get a del event after queryNode down", zap.Int64("nodeID", serverID))
				_, err := qc.cluster.getNodeByID(serverID)
				if err != nil {
					log.Error("queryNode not exist", zap.Int64("nodeID", serverID))
					continue
				}

				qc.cluster.stopNode(serverID)
				loadBalanceSegment := &querypb.LoadBalanceRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_LoadBalanceSegments,
						SourceID: qc.session.ServerID,
					},
					SourceNodeIDs: []int64{serverID},
					BalanceReason: querypb.TriggerCondition_nodeDown,
				}

				loadBalanceTask := &LoadBalanceTask{
					BaseTask: BaseTask{
						ctx:              qc.loopCtx,
						Condition:        NewTaskCondition(qc.loopCtx),
						triggerCondition: querypb.TriggerCondition_nodeDown,
					},
					LoadBalanceRequest: loadBalanceSegment,
					rootCoord:          qc.rootCoordClient,
					dataCoord:          qc.dataCoordClient,
					cluster:            qc.cluster,
					meta:               qc.meta,
				}
				qc.scheduler.Enqueue([]task{loadBalanceTask})
			}
		}
	}
}

func (qc *QueryCoord) watchMetaLoop() {
	ctx, cancel := context.WithCancel(qc.loopCtx)

	defer cancel()
	defer qc.loopWg.Done()
	log.Debug("query coordinator start watch MetaReplica loop")

	watchChan := qc.kvClient.WatchWithPrefix("queryNode-segmentMeta")

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-watchChan:
			log.Debug("segment MetaReplica updated.")
			for _, event := range resp.Events {
				segmentID, err := strconv.ParseInt(filepath.Base(string(event.Kv.Key)), 10, 64)
				if err != nil {
					log.Error("watch MetaReplica loop error when get segmentID", zap.Any("error", err.Error()))
				}
				segmentInfo := &querypb.SegmentInfo{}
				err = proto.UnmarshalText(string(event.Kv.Value), segmentInfo)
				if err != nil {
					log.Error("watch MetaReplica loop error when unmarshal", zap.Any("error", err.Error()))
				}
				switch event.Type {
				case mvccpb.PUT:
					//TODO::
					qc.meta.setSegmentInfo(segmentID, segmentInfo)
				case mvccpb.DELETE:
					//TODO::
				}
			}
		}
	}

}
