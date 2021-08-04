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

package grpcrootcoord

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

	Address string // ip:port
	Port    int

	IndexCoordAddress string
	QueryCoordAddress string
	DataCoordAddress  string

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/root_coord.yaml")
		if err != nil {
			panic(err)
		}
		p.initAddress()
		p.initPort()
		p.initIndexCoordAddress()
		p.initQueryCoordAddress()
		p.initDataCoordAddress()

		p.initServerMaxSendSize()
		p.initServerMaxRecvSize()
	})
}

func (p *ParamTable) initAddress() {
	ret, err := p.Load("_RootCoordAddress")
	if err != nil {
		panic(err)
	}
	p.Address = ret
}

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("rootCoord.port")
}

func (p *ParamTable) initIndexCoordAddress() {
	ret, err := p.Load("_IndexCoordAddress")
	if err != nil {
		panic(err)
	}
	p.IndexCoordAddress = ret
}

func (p *ParamTable) initQueryCoordAddress() {
	ret, err := p.Load("_QueryCoordAddress")
	if err != nil {
		panic(err)
	}
	p.QueryCoordAddress = ret
}

func (p *ParamTable) initDataCoordAddress() {
	ret, err := p.Load("_DataCoordAddress")
	if err != nil {
		panic(err)
	}
	p.DataCoordAddress = ret
}

func (p *ParamTable) initServerMaxSendSize() {
	var err error
	p.ServerMaxSendSize, err = p.ParseIntWithErr("rootCoord.grpc.serverMaxSendSize")
	if err != nil {
		p.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
		log.Debug("rootCoord.grpc.serverMaxSendSize not set, set to default")
	}
	log.Debug("initServerMaxSendSize",
		zap.Int("rootCoord.grpc.serverMaxSendSize", p.ServerMaxSendSize))
}

func (p *ParamTable) initServerMaxRecvSize() {
	var err error
	p.ServerMaxRecvSize, err = p.ParseIntWithErr("rootCoord.grpc.serverMaxRecvSize")
	if err != nil {
		p.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
		log.Debug("rootCoord.grpc.serverMaxRecvSize not set, set to default")
	}
	log.Debug("initServerMaxRecvSize",
		zap.Int("rootCoord.grpc.serverMaxRecvSize", p.ServerMaxRecvSize))
}
