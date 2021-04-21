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

package proxyservice

import (
	"context"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type ProxyService struct {
	allocator nodeIDAllocator
	sched     *taskScheduler
	tick      *TimeTick
	nodeInfos *globalNodeInfoTable
	stateCode internalpb.StateCode

	//subStates *internalpb.ComponentStates

	nodeStartParams []*commonpb.KeyValuePair

	ctx    context.Context
	cancel context.CancelFunc

	msFactory msgstream.Factory
}

func NewProxyService(ctx context.Context, factory msgstream.Factory) (*ProxyService, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	s := &ProxyService{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}

	s.allocator = newNodeIDAllocator()
	s.sched = newTaskScheduler(ctx1)
	s.nodeInfos = newGlobalNodeInfoTable()
	s.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("proxyservice", zap.Any("state of proxyservice: ", internalpb.StateCode_Abnormal))

	return s, nil
}
