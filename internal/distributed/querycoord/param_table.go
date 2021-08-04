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

package grpcquerycoord

import (
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable
	Port int

	RootCoordAddress string
	DataCoordAddress string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initPort()
		pt.initRootCoordAddress()
		pt.initDataCoordAddress()

		pt.initServerMaxSendSize()
		pt.initServerMaxRecvSize()
	})
}

func (pt *ParamTable) initRootCoordAddress() {
	ret, err := pt.Load("_RootCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.RootCoordAddress = ret
}

func (pt *ParamTable) initDataCoordAddress() {
	ret, err := pt.Load("_DataCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.DataCoordAddress = ret
}

func (pt *ParamTable) initPort() {
	pt.Port = pt.ParseInt("queryCoord.port")
}

func (pt *ParamTable) initServerMaxSendSize() {
	var err error
	pt.ServerMaxSendSize, err = pt.ParseIntWithErr("queryCoord.grpc.serverMaxSendSize")
	if err != nil {
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
		log.Debug("queryCoord.grpc.serverMaxSendSize not set, set to default")
	}
	log.Debug("initServerMaxSendSize",
		zap.Int("queryCoord.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error
	pt.ServerMaxRecvSize, err = pt.ParseIntWithErr("queryCoord.grpc.serverMaxRecvSize")
	if err != nil {
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
		log.Debug("queryCoord.grpc.serverMaxRecvSize not set, set to default")
	}
	log.Debug("initServerMaxRecvSize",
		zap.Int("queryCoord.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
