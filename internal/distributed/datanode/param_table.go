// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcdatanode

import (
	"net"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// ParamTable is a derived struct of paramtable.BaseTable. It achieves Composition by
// embedding paramtable.BaseTable. It is used to quickly and easily access the system configuration.
type ParamTable struct {
	paramtable.BaseTable

	IP       string
	Port     int
	Address  string
	listener net.Listener

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()
		pt.Address = pt.IP + ":" + strconv.FormatInt(int64(pt.Port), 10)

		listener, err := net.Listen("tcp", pt.Address)
		if err != nil {
			panic(err)
		}
		pt.listener = listener
	})
}

// initParams initializes params of the configuration items.
func (pt *ParamTable) initParams() {
	pt.loadFromEnv()
	pt.loadFromArgs()

	pt.initPort()
	pt.initServerMaxSendSize()
	pt.initServerMaxRecvSize()
}

func (pt *ParamTable) loadFromArgs() {

}

func (pt *ParamTable) loadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("dataNode.port")
	pt.Port = port
	if !funcutil.CheckPortAvailable(pt.Port) {
		pt.Port = funcutil.GetAvailablePort()
		log.Warn("DataNode init", zap.Any("Port", pt.Port))
	}
}

func (pt *ParamTable) initServerMaxSendSize() {
	var err error

	valueStr, err := pt.Load("dataNode.grpc.serverMaxSendSize")
	if err != nil { // not set
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse dataNode.grpc.serverMaxSendSize, set to default",
			zap.String("dataNode.grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	} else {
		pt.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.Int("dataNode.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error

	valueStr, err := pt.Load("dataNode.grpc.serverMaxRecvSize")
	if err != nil { // not set
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // invalid format
		log.Warn("Failed to parse dataNode.grpc.serverMaxRecvSize, set to default",
			zap.String("dataNode.grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	} else {
		pt.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.Int("dataNode.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
