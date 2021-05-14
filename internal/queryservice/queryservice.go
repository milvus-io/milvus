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
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
)

type Timestamp = typeutil.Timestamp

type queryChannelInfo struct {
	requestChannel  string
	responseChannel string
}

type QueryService struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc

	queryServiceID uint64
	replica        Replica
	sched          *TaskScheduler

	dataServiceClient   types.DataService
	masterServiceClient types.MasterService
	queryNodes          map[int64]*queryNodeInfo
	queryChannels       []*queryChannelInfo
	qcMutex             *sync.Mutex

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool

	etcdKV  *etcdkv.EtcdKV
	session struct {
		NodeName string
		IP       string
		LeaseID  clientv3.LeaseID
	}

	msFactory msgstream.Factory
}

func (qs *QueryService) Init() error {
	connectEtcdFn := func() error {
		etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		qs.etcdKV = etcdkv.NewEtcdKV(etcdCli, Params.MetaRootPath)
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return err
	}

	ch, err := qs.registerService(fmt.Sprintf("queryservice-%d", Params.QueryServiceID), Params.Address)
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

	return nil
}

func (qs *QueryService) Start() error {
	qs.sched.Start()
	log.Debug("start scheduler ...")
	qs.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

func (qs *QueryService) Stop() error {
	qs.sched.Close()
	log.Debug("close scheduler ...")
	qs.loopCancel()
	qs.UpdateStateCode(internalpb.StateCode_Abnormal)
	return nil
}

func (qs *QueryService) UpdateStateCode(code internalpb.StateCode) {
	qs.stateCode.Store(code)
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
	rand.Seed(time.Now().UnixNano())
	nodes := make(map[int64]*queryNodeInfo)
	queryChannels := make([]*queryChannelInfo, 0)
	ctx1, cancel := context.WithCancel(ctx)
	replica := newMetaReplica()
	scheduler := NewTaskScheduler(ctx1)
	service := &QueryService{
		loopCtx:       ctx1,
		loopCancel:    cancel,
		replica:       replica,
		sched:         scheduler,
		queryNodes:    nodes,
		queryChannels: queryChannels,
		qcMutex:       &sync.Mutex{},
		msFactory:     factory,
	}

	service.UpdateStateCode(internalpb.StateCode_Abnormal)
	return service, nil
}

func (qs *QueryService) SetMasterService(masterService types.MasterService) {
	qs.masterServiceClient = masterService
}

func (qs *QueryService) SetDataService(dataService types.DataService) {
	qs.dataServiceClient = dataService
}

func (qs *QueryService) registerService(nodeName string, ip string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	respID, err := qs.etcdKV.Grant(5)
	if err != nil {
		fmt.Printf("grant error %s\n", err)
		return nil, err
	}
	qs.session.NodeName = nodeName
	qs.session.IP = ip
	qs.session.LeaseID = respID

	sessionJSON, err := json.Marshal(qs.session)
	if err != nil {
		return nil, err
	}

	err = qs.etcdKV.SaveWithLease(fmt.Sprintf("/node/%s", nodeName), string(sessionJSON), respID)
	if err != nil {
		fmt.Printf("put lease error %s\n", err)
		return nil, err
	}

	ch, err := qs.etcdKV.KeepAlive(respID)
	if err != nil {
		fmt.Printf("keep alive error %s\n", err)
		return nil, err
	}
	return ch, nil
}
