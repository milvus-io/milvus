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

package grpcmasterservice

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	Address string // ip:port
	Port    int

	IndexServiceAddress string
	QueryServiceAddress string
	DataServiceAddress  string
}

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/master.yaml")
		if err != nil {
			panic(err)
		}
		p.initAddress()
		p.initPort()
		p.initIndexServiceAddress()
		p.initQueryServiceAddress()
		p.initDataServiceAddress()

	})
}

func (p *ParamTable) initAddress() {
	ret, err := p.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	p.Address = ret
}

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("master.port")
}

func (p *ParamTable) initIndexServiceAddress() {
	ret, err := p.Load("IndexServiceAddress")
	if err != nil {
		panic(err)
	}
	p.IndexServiceAddress = ret
}

func (p *ParamTable) initQueryServiceAddress() {
	ret, err := p.Load("_QueryServiceAddress")
	if err != nil {
		panic(err)
	}
	p.QueryServiceAddress = ret
}

func (p *ParamTable) initDataServiceAddress() {
	ret, err := p.Load("_DataServiceAddress")
	if err != nil {
		panic(err)
	}
	p.DataServiceAddress = ret
}
