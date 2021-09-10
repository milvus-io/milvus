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

package rootcoord

import (
	"path"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int

	PulsarAddress     string
	EtcdEndpoints     []string
	MetaRootPath      string
	KvRootPath        string
	MsgChannelSubName string
	TimeTickChannel   string
	StatisticsChannel string
	DmlChannelName    string

	DmlChannelNum               int64
	MaxPartitionNum             int64
	DefaultPartitionName        string
	DefaultIndexName            string
	MinSegmentSizeToEnableIndex int64

	Timeout          int
	TimeTickInterval int

	Log log.Config

	RoleName string
}

func (p *ParamTable) Init() {
	once.Do(func() {
		// load yaml
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/root_coord.yaml")
		if err != nil {
			panic(err)
		}

		p.initPulsarAddress()
		p.initEtcdEndpoints()
		p.initMetaRootPath()
		p.initKvRootPath()

		p.initMsgChannelSubName()
		p.initTimeTickChannel()
		p.initStatisticsChannelName()

		p.initMaxPartitionNum()
		p.initMinSegmentSizeToEnableIndex()
		p.initDefaultPartitionName()
		p.initDefaultIndexName()

		p.initTimeout()
		p.initTimeTickInterval()

		p.initLogCfg()
		p.initRoleName()
	})
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (p *ParamTable) initMetaRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initKvRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	p.MsgChannelSubName = name
}

func (p *ParamTable) initTimeTickChannel() {
	channel, err := p.Load("msgChannel.chanNamePrefix.rootCoordTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannel = channel
}

func (p *ParamTable) initStatisticsChannelName() {
	channel, err := p.Load("msgChannel.chanNamePrefix.rootCoordStatistics")
	if err != nil {
		panic(err)
	}
	p.StatisticsChannel = channel
}

func (p *ParamTable) initMaxPartitionNum() {
	p.MaxPartitionNum = p.ParseInt64("rootcoord.maxPartitionNum")
}

func (p *ParamTable) initMinSegmentSizeToEnableIndex() {
	p.MinSegmentSizeToEnableIndex = p.ParseInt64("rootcoord.minSegmentSizeToEnableIndex")
}

func (p *ParamTable) initDefaultPartitionName() {
	name, err := p.Load("common.defaultPartitionName")
	if err != nil {
		panic(err)
	}
	p.DefaultPartitionName = name
}

func (p *ParamTable) initDefaultIndexName() {
	name, err := p.Load("common.defaultIndexName")
	if err != nil {
		panic(err)
	}
	p.DefaultIndexName = name
}

func (p *ParamTable) initTimeout() {
	p.Timeout = p.ParseInt("rootcoord.timeout")
}

func (p *ParamTable) initTimeTickInterval() {
	p.TimeTickInterval = p.ParseInt("rootcoord.timeTickInterval")
}

func (p *ParamTable) initLogCfg() {
	p.Log = log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.Log.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.Log.Level = level
	p.Log.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.Log.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.Log.File.MaxDays = p.ParseInt("log.file.maxAge")
	rootPath, err := p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		p.Log.File.Filename = path.Join(rootPath, "rootcoord.log")
	} else {
		p.Log.File.Filename = ""
	}
}

func (p *ParamTable) initRoleName() {
	p.RoleName = "RootCoord"
}
