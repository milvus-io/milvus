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

package grpcindexnode

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable is used to record configuration items.
type ParamTable struct {
	paramtable.BaseTable

	IndexCoordAddress string

	IP      string
	Port    int
	Address string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

// Params is an alias for ParamTable.
var Params ParamTable
var once sync.Once

// Init is used to initialize configuration items.
func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()

		pt.initServerMaxSendSize()
		pt.initServerMaxRecvSize()

		if !funcutil.CheckPortAvailable(pt.Port) {
			pt.Port = funcutil.GetAvailablePort()
			log.Warn("IndexNode init", zap.Any("Port", pt.Port))
		}
		pt.LoadFromEnv()
		pt.LoadFromArgs()
	})
}

// LoadFromArgs is used to initialize configuration items from args.
func (pt *ParamTable) LoadFromArgs() {

}

// LoadFromEnv is used to initialize configuration items from env.
func (pt *ParamTable) LoadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initParams() {
	pt.initPort()
	pt.initIndexCoordAddress()
}

// todo remove and use load from env
func (pt *ParamTable) initIndexCoordAddress() {
	ret, err := pt.Load("_IndexCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.IndexCoordAddress = ret
}

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("indexNode.port")
	pt.Port = port
}

func (pt *ParamTable) initServerMaxSendSize() {
	var err error

	valueStr, err := pt.Load("indexNode.grpc.serverMaxSendSize")
	if err != nil { // not set
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse indexNode.grpc.serverMaxSendSize, set to default",
			zap.String("indexNode.grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	} else {
		pt.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.Int("indexNode.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error

	valueStr, err := pt.Load("indexNode.grpc.serverMaxRecvSize")
	if err != nil { // not set
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse indexNode.grpc.serverMaxRecvSize, set to default",
			zap.String("indexNode.grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	} else {
		pt.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.Int("indexNode.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
