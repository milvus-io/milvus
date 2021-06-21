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

package queryservice

import (
	"context"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
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

type QueryService struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc
	loopWg     sync.WaitGroup
	kvClient   *etcdkv.EtcdKV

	queryServiceID uint64
	meta           *meta
	cluster        *queryNodeCluster
	scheduler      *TaskScheduler

	dataServiceClient   types.DataService
	masterServiceClient types.MasterService

	session   *sessionutil.Session
	eventChan <-chan *sessionutil.SessionEvent

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool

	msFactory msgstream.Factory
}

// Register register query service at etcd
func (qs *QueryService) Register() error {
	qs.session = sessionutil.NewSession(qs.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	qs.session.Init(typeutil.QueryServiceRole, Params.Address, true)
	Params.NodeID = uint64(qs.session.ServerID)
	return nil
}

func (qs *QueryService) Init() error {
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
		if err != nil {
			return err
		}
		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		qs.kvClient = etcdKV
		metaKV, err := newMeta(etcdKV)
		if err != nil {
			return err
		}
		qs.meta = metaKV
		qs.cluster, err = newQueryNodeCluster(metaKV, etcdKV)
		if err != nil {
			return err
		}

		qs.scheduler, err = NewTaskScheduler(qs.loopCtx, metaKV, qs.cluster, etcdKV, qs.masterServiceClient, qs.dataServiceClient)
		return err
	}
	log.Debug("queryService try to connect etcd")
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		log.Debug("queryService try to connect etcd failed", zap.Error(err))
		return err
	}
	log.Debug("queryService try to connect etcd success")
	return nil
}

func (qs *QueryService) Start() error {
	qs.scheduler.Start()
	log.Debug("start scheduler ...")
	qs.UpdateStateCode(internalpb.StateCode_Healthy)

	qs.loopWg.Add(1)
	go qs.watchNodeLoop()

	qs.loopWg.Add(1)
	go qs.watchMetaLoop()

	return nil
}

func (qs *QueryService) Stop() error {
	qs.scheduler.Close()
	log.Debug("close scheduler ...")
	qs.loopCancel()
	qs.UpdateStateCode(internalpb.StateCode_Abnormal)

	qs.loopWg.Wait()
	return nil
}

func (qs *QueryService) UpdateStateCode(code internalpb.StateCode) {
	qs.stateCode.Store(code)
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
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
	service := &QueryService{
		loopCtx:    ctx1,
		loopCancel: cancel,
		msFactory:  factory,
	}

	service.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("QueryService", zap.Any("queryChannels", queryChannels))
	return service, nil
}

func (qs *QueryService) SetMasterService(masterService types.MasterService) {
	qs.masterServiceClient = masterService
}

func (qs *QueryService) SetDataService(dataService types.DataService) {
	qs.dataServiceClient = dataService
}

func (qs *QueryService) watchNodeLoop() {
	ctx, cancel := context.WithCancel(qs.loopCtx)
	defer cancel()
	defer qs.loopWg.Done()
	log.Debug("QueryService start watch node loop")

	clusterStartSession, version, _ := qs.session.GetSessions(typeutil.QueryNodeRole)
	sessionMap := make(map[int64]*sessionutil.Session)
	for _, session := range clusterStartSession {
		nodeID := session.ServerID
		sessionMap[nodeID] = session
	}
	for nodeID, session := range sessionMap {
		if _, ok := qs.cluster.nodes[nodeID]; !ok {
			serverID := session.ServerID
			err := qs.cluster.RegisterNode(session, serverID)
			if err != nil {
				log.Error("register queryNode error", zap.Any("error", err.Error()))
			}
			log.Debug("QueryService", zap.Any("Add QueryNode, session serverID", serverID))
		}
	}
	for nodeID := range qs.cluster.nodes {
		if _, ok := sessionMap[nodeID]; !ok {
			qs.cluster.nodes[nodeID].setNodeState(false)
			loadBalanceSegment := &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_LoadBalanceSegments,
					SourceID: qs.session.ServerID,
				},
				SourceNodeIDs: []int64{nodeID},
			}

			loadBalanceTask := &LoadBalanceTask{
				BaseTask: BaseTask{
					ctx:              qs.loopCtx,
					Condition:        NewTaskCondition(qs.loopCtx),
					triggerCondition: querypb.TriggerCondition_nodeDown,
				},
				LoadBalanceRequest: loadBalanceSegment,
				master:             qs.masterServiceClient,
				dataService:        qs.dataServiceClient,
				cluster:            qs.cluster,
				meta:               qs.meta,
			}
			qs.scheduler.Enqueue([]task{loadBalanceTask})
		}
	}

	qs.eventChan = qs.session.WatchServices(typeutil.QueryNodeRole, version+1)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-qs.eventChan:
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				err := qs.cluster.RegisterNode(event.Session, serverID)
				if err != nil {
					log.Error(err.Error())
				}
				log.Debug("QueryService", zap.Any("Add QueryNode, session serverID", serverID))
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Debug("QueryService", zap.Any("The QueryNode crashed with ID", serverID))
				qs.cluster.nodes[serverID].setNodeState(false)
				loadBalanceSegment := &querypb.LoadBalanceRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_LoadBalanceSegments,
						SourceID: qs.session.ServerID,
					},
					SourceNodeIDs: []int64{serverID},
					BalanceReason: querypb.TriggerCondition_nodeDown,
				}

				loadBalanceTask := &LoadBalanceTask{
					BaseTask: BaseTask{
						ctx:              qs.loopCtx,
						Condition:        NewTaskCondition(qs.loopCtx),
						triggerCondition: querypb.TriggerCondition_nodeDown,
					},
					LoadBalanceRequest: loadBalanceSegment,
					master:             qs.masterServiceClient,
					dataService:        qs.dataServiceClient,
					cluster:            qs.cluster,
					meta:               qs.meta,
				}
				qs.scheduler.Enqueue([]task{loadBalanceTask})
				err := loadBalanceTask.WaitToFinish()
				if err != nil {
					log.Error(err.Error())
				}
				log.Debug("load balance done after queryNode down", zap.Int64s("nodeIDs", loadBalanceTask.SourceNodeIDs))
				//TODO::remove nodeInfo and clear etcd
			}
		}
	}
}

func (qs *QueryService) watchMetaLoop() {
	ctx, cancel := context.WithCancel(qs.loopCtx)

	defer cancel()
	defer qs.loopWg.Done()
	log.Debug("QueryService start watch meta loop")

	watchChan := qs.meta.client.WatchWithPrefix("queryNode-segmentMeta")

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-watchChan:
			log.Debug("segment meta updated.")
			for _, event := range resp.Events {
				segmentID, err := strconv.ParseInt(filepath.Base(string(event.Kv.Key)), 10, 64)
				if err != nil {
					log.Error("watch meta loop error when get segmentID", zap.Any("error", err.Error()))
				}
				segmentInfo := &querypb.SegmentInfo{}
				err = proto.UnmarshalText(string(event.Kv.Value), segmentInfo)
				if err != nil {
					log.Error("watch meta loop error when unmarshal", zap.Any("error", err.Error()))
				}
				switch event.Type {
				case mvccpb.PUT:
					//TODO::
					qs.meta.setSegmentInfo(segmentID, segmentInfo)
				case mvccpb.DELETE:
					//TODO::
				}
			}
		}
	}

}
