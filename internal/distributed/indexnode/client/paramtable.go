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

package grpcindexnodeclient

import (
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	ClientMaxSendSize int
	ClientMaxRecvSize int
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()

		pt.initClientMaxSendSize()
		pt.initClientMaxRecvSize()
	})
}

func (pt *ParamTable) initClientMaxSendSize() {
	var err error
	pt.ClientMaxSendSize, err = pt.ParseIntWithErr("indexNode.grpc.clientMaxSendSize")
	if err != nil {
		pt.ClientMaxSendSize = grpcconfigs.DefaultClientMaxSendSize
		log.Debug("indexNode.grpc.clientMaxSendSize not set, set to default")
	}
	log.Debug("initClientMaxSendSize",
		zap.Int("indexNode.grpc.clientMaxSendSize", pt.ClientMaxSendSize))
}

func (pt *ParamTable) initClientMaxRecvSize() {
	var err error
	pt.ClientMaxRecvSize, err = pt.ParseIntWithErr("indexNode.grpc.clientMaxRecvSize")
	if err != nil {
		pt.ClientMaxRecvSize = grpcconfigs.DefaultClientMaxRecvSize
		log.Debug("indexNode.grpc.clientMaxRecvSize not set, set to default")
	}
	log.Debug("initClientMaxRecvSize",
		zap.Int("indexNode.grpc.clientMaxRecvSize", pt.ClientMaxRecvSize))
}
