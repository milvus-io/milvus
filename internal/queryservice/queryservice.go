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
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
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

	queryServiceID uint64
	replica        Replica
	sched          *TaskScheduler

	dataServiceClient   types.DataService
	masterServiceClient types.MasterService
	queryNodes          map[int64]*queryNodeInfo
	queryChannels       []*queryChannelInfo
	qcMutex             *sync.Mutex

	session *sessionutil.Session

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool

	msFactory msgstream.Factory
}

// Register register query service at etcd
func (qs *QueryService) Register() error {
	qs.session = sessionutil.NewSession(qs.loopCtx, []string{Params.EtcdAddress})
	qs.session.Init(typeutil.QueryServiceRole, Params.Address, true)
	Params.NodeID = uint64(qs.session.ServerID)
	return nil
}

func (qs *QueryService) Init() error {
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
