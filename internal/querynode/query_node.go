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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/clientv3"
)

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID UniqueID
	stateCode   atomic.Value

	replica ReplicaInterface

	// internal services
	metaService      *metaService
	searchService    *searchService
	loadService      *loadService
	statsService     *statsService
	dsServicesMu     sync.Mutex // guards dataSyncServices
	dataSyncServices map[UniqueID]*dataSyncService

	// clients
	masterService types.MasterService
	queryService  types.QueryService
	indexService  types.IndexService
	dataService   types.DataService

	etcdKV  *etcdkv.EtcdKV
	session struct {
		NodeName string
		IP       string
		LeaseID  clientv3.LeaseID
	}

	msFactory msgstream.Factory
	scheduler *taskScheduler
}

func NewQueryNode(ctx context.Context, queryNodeID UniqueID, factory msgstream.Factory) *QueryNode {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		dataSyncServices: make(map[UniqueID]*dataSyncService),
		metaService:      nil,
		searchService:    nil,
		statsService:     nil,

		msFactory: factory,
	}

	node.scheduler = newTaskScheduler(ctx1)
	node.replica = newCollectionReplica()
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

func NewQueryNodeWithoutID(ctx context.Context, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,

		dataSyncServices: make(map[UniqueID]*dataSyncService),
		metaService:      nil,
		searchService:    nil,
		statsService:     nil,

		msFactory: factory,
	}

	node.scheduler = newTaskScheduler(ctx1)
	node.replica = newCollectionReplica()
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	return node
}

func (node *QueryNode) Init() error {
	ctx := context.Background()

	connectEtcdFn := func() error {
		etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		node.etcdKV = etcdkv.NewEtcdKV(etcdCli, Params.MetaRootPath)
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return err
	}

	ch, err := node.registerService(fmt.Sprintf("querynode-%d", Params.QueryNodeID), Params.QueryNodeIP)
	if err != nil {
		return err
	}
	go func() {
		for {
			for range ch {
				//TODO process lesase response
			}
		}
	}()

	C.SegcoreInit()
	registerReq := &queryPb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			SourceID: Params.QueryNodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.QueryNodeIP,
			Port: Params.QueryNodePort,
		},
	}

	resp, err := node.queryService.RegisterNode(ctx, registerReq)
	if err != nil {
		panic(err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		panic(resp.Status.Reason)
	}

	for _, kv := range resp.InitParams.StartParams {
		switch kv.Key {
		case "StatsChannelName":
			Params.StatsChannelName = kv.Value
		case "TimeTickChannelName":
			Params.QueryTimeTickChannelName = kv.Value
		case "SearchChannelName":
			Params.SearchChannelNames = append(Params.SearchChannelNames, kv.Value)
		case "SearchResultChannelName":
			Params.SearchResultChannelNames = append(Params.SearchResultChannelNames, kv.Value)
		default:
			return fmt.Errorf("Invalid key: %v", kv.Key)
		}
	}

	log.Debug("", zap.Int64("QueryNodeID", Params.QueryNodeID))

	if node.masterService == nil {
		log.Error("null master service detected")
	}

	if node.indexService == nil {
		log.Error("null index service detected")
	}

	if node.dataService == nil {
		log.Error("null data service detected")
	}

	return nil
}

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
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica, node.msFactory)
	node.loadService = newLoadService(node.queryNodeLoopCtx, node.masterService, node.dataService, node.indexService, node.replica)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadService.segLoader.indexLoader.fieldStatsChan, node.msFactory)

	// start task scheduler
	go node.scheduler.Start()

	// start services
	go node.searchService.start()
	go node.loadService.start()
	go node.statsService.start()
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

func (node *QueryNode) Stop() error {
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	node.queryNodeLoopCancel()

	// free collectionReplica
	node.replica.freeAll()

	// close services
	for _, dsService := range node.dataSyncServices {
		if dsService != nil {
			dsService.close()
		}
	}
	if node.searchService != nil {
		node.searchService.close()
	}
	if node.loadService != nil {
		node.loadService.close()
	}
	if node.statsService != nil {
		node.statsService.close()
	}
	return nil
}

func (node *QueryNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

func (node *QueryNode) SetMasterService(master types.MasterService) error {
	if master == nil {
		return errors.New("null master service interface")
	}
	node.masterService = master
	return nil
}

func (node *QueryNode) SetQueryService(query types.QueryService) error {
	if query == nil {
		return errors.New("null query service interface")
	}
	node.queryService = query
	return nil
}

func (node *QueryNode) SetIndexService(index types.IndexService) error {
	if index == nil {
		return errors.New("null index service interface")
	}
	node.indexService = index
	return nil
}

func (node *QueryNode) SetDataService(data types.DataService) error {
	if data == nil {
		return errors.New("null data service interface")
	}
	node.dataService = data
	return nil
}

func (node *QueryNode) getDataSyncService(collectionID UniqueID) (*dataSyncService, error) {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	ds, ok := node.dataSyncServices[collectionID]
	if !ok {
		return nil, errors.New("cannot found dataSyncService, collectionID =" + fmt.Sprintln(collectionID))
	}
	return ds, nil
}

func (node *QueryNode) addDataSyncService(collectionID UniqueID, ds *dataSyncService) error {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	if _, ok := node.dataSyncServices[collectionID]; ok {
		return errors.New("dataSyncService has been existed, collectionID =" + fmt.Sprintln(collectionID))
	}
	node.dataSyncServices[collectionID] = ds
	return nil
}

func (node *QueryNode) removeDataSyncService(collectionID UniqueID) {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	delete(node.dataSyncServices, collectionID)
}

func (node *QueryNode) registerService(nodeName string, ip string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	respID, err := node.etcdKV.Grant(5)
	if err != nil {
		fmt.Printf("grant error %s\n", err)
		return nil, err
	}
	node.session.NodeName = nodeName
	node.session.IP = ip
	node.session.LeaseID = respID

	sessionJSON, err := json.Marshal(node.session)
	if err != nil {
		return nil, err
	}

	err = node.etcdKV.SaveWithLease(fmt.Sprintf("/node/%s", nodeName), string(sessionJSON), respID)
	if err != nil {
		fmt.Printf("put lease error %s\n", err)
		return nil, err
	}

	ch, err := node.etcdKV.KeepAlive(respID)
	if err != nil {
		fmt.Printf("keep alive error %s\n", err)
		return nil, err
	}
	return ch, nil
}
