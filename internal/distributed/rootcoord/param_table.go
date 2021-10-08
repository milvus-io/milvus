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
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// ParamTable is a derived struct of paramtable.BaseTable. It achieves Composition by
// embedding paramtable.BaseTable. It is used to quickly and easily access the system configuration.
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

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
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

	valueStr, err := p.Load("rootCoord.grpc.serverMaxSendSize")
	if err != nil { // not set
		p.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse rootCoord.grpc.serverMaxSendSize, set to default",
			zap.String("rootCoord.grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		p.ServerMaxSendSize = grpcconfigs.DefaultServerMaxSendSize
	} else {
		p.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.Int("rootCoord.grpc.serverMaxSendSize", p.ServerMaxSendSize))
}

func (p *ParamTable) initServerMaxRecvSize() {
	var err error

	valueStr, err := p.Load("rootCoord.grpc.serverMaxRecvSize")
	if err != nil { // not set
		p.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil { // not in valid format
		log.Warn("Failed to parse rootCoord.grpc.serverMaxRecvSize, set to default",
			zap.String("rootCoord.grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		p.ServerMaxRecvSize = grpcconfigs.DefaultServerMaxRecvSize
	} else {
		p.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.Int("rootCoord.grpc.serverMaxRecvSize", p.ServerMaxRecvSize))
}
