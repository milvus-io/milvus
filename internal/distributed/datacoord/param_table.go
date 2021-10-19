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

package grpcdatacoordclient

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable is a derived struct of paramtable.BaseTable. It achieves Composition by
// embedding paramtable.BaseTable. It is used to quickly and easily access the system configuration.
type ParamTable struct {
	paramtable.BaseTable

	IP               string
	Port             int
	RootCoordAddress string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initPort()
		pt.initParams()
		pt.loadFromEnv()

		pt.initServerMaxSendSize()
		pt.initServerMaxRecvSize()
	})
}

func (pt *ParamTable) initParams() {
	pt.initRootCoordAddress()
	pt.initDataCoordAddress()
}

func (pt *ParamTable) loadFromEnv() {

}

func (pt *ParamTable) initPort() {
	pt.Port = pt.ParseInt("dataCoord.port")
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
	pt.IP = ret
}

func (pt *ParamTable) initServerMaxSendSize() {
	var err error

	valueStr, err := pt.Load("dataCoord.grpc.serverMaxSendSize")
	if err != nil { // not set
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse dataCoord.grpc.serverMaxSendSize, set to default",
			zap.String("dataCoord.grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	} else {
		pt.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.Int("dataCoord.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error

	valueStr, err := pt.Load("dataCoord.grpc.serverMaxRecvSize")
	if err != nil { // not set
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse dataCoord.grpc.serverMaxRecvSize, set to default",
			zap.String("dataCoord.grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	} else {
		pt.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.Int("dataCoord.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
