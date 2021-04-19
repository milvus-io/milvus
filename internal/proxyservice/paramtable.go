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

package proxyservice

import (
	"path"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	PulsarAddress           string
	MasterAddress           string
	NodeTimeTickChannel     []string
	ServiceTimeTickChannel  string
	DataServiceAddress      string
	InsertChannelPrefixName string
	InsertChannelNum        int64

	Log log.Config
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()

		if err := pt.LoadYaml("advanced/data_service.yaml"); err != nil {
			panic(err)
		}

		pt.initPulsarAddress()
		pt.initMasterAddress()
		pt.initNodeTimeTickChannel()
		pt.initServiceTimeTickChannel()
		pt.initDataServiceAddress()
		pt.initInsertChannelPrefixName()
		pt.initInsertChannelNum()
		pt.initLogCfg()
	})
}

func (pt *ParamTable) initPulsarAddress() {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	pt.PulsarAddress = ret
}

func (pt *ParamTable) initMasterAddress() {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = ret
}

func (pt *ParamTable) initNodeTimeTickChannel() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		log.Error("proxyservice", zap.Error(err))
	}
	prefix += "-0"
	pt.NodeTimeTickChannel = []string{prefix}
}

func (pt *ParamTable) initServiceTimeTickChannel() {
	ch, err := pt.Load("msgChannel.chanNamePrefix.proxyServiceTimeTick")
	if err != nil {
		log.Error("proxyservice", zap.Error(err))
	}
	pt.ServiceTimeTickChannel = ch
}

func (pt *ParamTable) initDataServiceAddress() {
	// NOT USED NOW
	pt.DataServiceAddress = "TODO: read from config"
}

func (pt *ParamTable) initInsertChannelNum() {
	pt.InsertChannelNum = pt.ParseInt64("dataservice.insertChannelNum")
}

func (pt *ParamTable) initInsertChannelPrefixName() {
	var err error
	pt.InsertChannelPrefixName, err = pt.Load("msgChannel.chanNamePrefix.dataServiceInsertChannel")
	if err != nil {
		panic(err)
	}
}

func (pt *ParamTable) initLogCfg() {
	pt.Log = log.Config{}
	format, err := pt.Load("log.format")
	if err != nil {
		panic(err)
	}
	pt.Log.Format = format
	level, err := pt.Load("log.level")
	if err != nil {
		panic(err)
	}
	pt.Log.Level = level
	devStr, err := pt.Load("log.dev")
	if err != nil {
		panic(err)
	}
	dev, err := strconv.ParseBool(devStr)
	if err != nil {
		panic(err)
	}
	pt.Log.Development = dev
	pt.Log.File.MaxSize = pt.ParseInt("log.file.maxSize")
	pt.Log.File.MaxBackups = pt.ParseInt("log.file.maxBackups")
	pt.Log.File.MaxDays = pt.ParseInt("log.file.maxAge")
	rootPath, err := pt.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		pt.Log.File.Filename = path.Join(rootPath, "proxyservice.log")
	} else {
		pt.Log.File.Filename = ""
	}
}
