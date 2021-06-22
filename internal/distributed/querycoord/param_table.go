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

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable
	Port int

	IndexCoordAddress string
	RootCoordAddress  string
	DataCoordAddress  string
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initPort()
		pt.initRootCoordAddress()
		pt.initIndexCoordAddress()
		pt.initDataCoordAddress()
	})
}

func (pt *ParamTable) initRootCoordAddress() {
	ret, err := pt.Load("_RootCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.RootCoordAddress = ret
}

func (pt *ParamTable) initIndexCoordAddress() {
	ret, err := pt.Load("IndexCoordAddress")
	if err != nil {
		panic(err)
	}
	pt.IndexCoordAddress = ret
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
