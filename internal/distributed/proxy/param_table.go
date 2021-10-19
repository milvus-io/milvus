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

package grpcproxy

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	RootCoordAddress  string
	IndexCoordAddress string
	DataCoordAddress  string
	QueryCoordAddress string

	IP      string
	Port    int
	Address string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()

		pt.LoadFromEnv()
		pt.LoadFromArgs()
		pt.Address = pt.IP + ":" + strconv.FormatInt(int64(pt.Port), 10)

		pt.initServerMaxSendSize()
		pt.initServerMaxRecvSize()
	})
}

func (pt *ParamTable) LoadFromArgs() {

}

func (pt *ParamTable) LoadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initParams() {
	pt.initPort()
	pt.initRootCoordAddress()
	pt.initIndexCoordAddress()
	pt.initDataCoordAddress()
	pt.initQueryCoordAddress()
}

// todo remove and use load from env
func (pt *ParamTable) initIndexCoordAddress() {
	ret, err := pt.Load("_IndexCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.IndexCoordAddress = ret
}

// todo remove and use load from env
func (pt *ParamTable) initRootCoordAddress() {
	ret, err := pt.Load("_RootCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.RootCoordAddress = ret
}

// todo remove and use load from env
func (pt *ParamTable) initDataCoordAddress() {
	ret, err := pt.Load("_DataCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.DataCoordAddress = ret
}

// todo remove and use load from env
func (pt *ParamTable) initQueryCoordAddress() {
	ret, err := pt.Load("_QueryCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.QueryCoordAddress = ret
}

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("proxy.port")
	pt.Port = port
}

func (pt *ParamTable) initServerMaxSendSize() {
	var err error

	valueStr, err := pt.Load("proxy.grpc.serverMaxSendSize")
	if err != nil { // not set
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse proxy.grpc.serverMaxSendSize, set to default",
			zap.String("proxy.grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	} else {
		pt.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.Int("proxy.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error

	valueStr, err := pt.Load("proxy.grpc.serverMaxRecvSize")
	if err != nil { // not set
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse proxy.grpc.serverMaxRecvSize, set to default",
			zap.String("proxy.grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	} else {
		pt.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.Int("proxy.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
