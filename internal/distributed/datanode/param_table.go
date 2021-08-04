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

package grpcdatanode

import (
	"net"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	IP       string
	Port     int
	listener net.Listener

	RootCoordAddress string
	DataCoordAddress string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initRootCoordAddress()
		pt.initDataCoordAddress()
		pt.initPort()

		pt.initServerMaxSendSize()
		pt.initServerMaxRecvSize()
	})
}

func (pt *ParamTable) LoadFromArgs() {

}

func (pt *ParamTable) LoadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initPort() {

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	pt.Port = listener.Addr().(*net.TCPAddr).Port
	pt.listener = listener
	log.Info("DataNode", zap.Int("port", pt.Port))
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

func (pt *ParamTable) initServerMaxSendSize() {
	var err error
	pt.ServerMaxSendSize, err = pt.ParseIntWithErr("dataNode.grpc.serverMaxSendSize")
	if err != nil {
		pt.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
		log.Debug("dataNode.grpc.serverMaxSendSize not set, set to default")
	}
	log.Debug("initServerMaxSendSize",
		zap.Int("dataNode.grpc.serverMaxSendSize", pt.ServerMaxSendSize))
}

func (pt *ParamTable) initServerMaxRecvSize() {
	var err error
	pt.ServerMaxRecvSize, err = pt.ParseIntWithErr("dataNode.grpc.serverMaxRecvSize")
	if err != nil {
		pt.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
		log.Debug("dataNode.grpc.serverMaxRecvSize not set, set to default")
	}
	log.Debug("initServerMaxRecvSize",
		zap.Int("dataNode.grpc.serverMaxRecvSize", pt.ServerMaxRecvSize))
}
