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

package rootcoord

import (
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// Params globle params
var Params ParamTable
var once sync.Once

// ParamTable structure stores all params
type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int

	PulsarAddress string
	EtcdEndpoints []string
	MetaRootPath  string
	KvRootPath    string

	ClusterChannelPrefix string
	MsgChannelSubName    string
	TimeTickChannel      string
	StatisticsChannel    string
	DmlChannelName       string
	DeltaChannelName     string

	DmlChannelNum               int64
	MaxPartitionNum             int64
	DefaultPartitionName        string
	DefaultIndexName            string
	MinSegmentSizeToEnableIndex int64

	Timeout          int
	TimeTickInterval int

	CreatedTime time.Time
	UpdatedTime time.Time
}

// InitOnce initialize once
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

// Init initialize param table
func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()

	p.initPulsarAddress()
	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initKvRootPath()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initMsgChannelSubName()
	p.initTimeTickChannel()
	p.initStatisticsChannelName()
	p.initDmlChannelName()
	p.initDeltaChannelName()

	p.initDmlChannelNum()
	p.initMaxPartitionNum()
	p.initMinSegmentSizeToEnableIndex()
	p.initDefaultPartitionName()
	p.initDefaultIndexName()

	p.initTimeout()
	p.initTimeTickInterval()

	p.initRoleName()
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

func (p *ParamTable) initClusterMsgChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *ParamTable) initMsgChannelSubName() {
	config, err := p.Load("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannel() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannel = strings.Join(s, "-")
}

func (p *ParamTable) initStatisticsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordStatistics")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatisticsChannel = strings.Join(s, "-")
}

func (p *ParamTable) initDmlChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initDeltaChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initDmlChannelNum() {
	p.DmlChannelNum = p.ParseInt64WithDefault("rootCoord.dmlChannelNum", 256)
}

func (p *ParamTable) initMaxPartitionNum() {
	p.MaxPartitionNum = p.ParseInt64WithDefault("rootCoord.maxPartitionNum", 4096)
}

func (p *ParamTable) initMinSegmentSizeToEnableIndex() {
	p.MinSegmentSizeToEnableIndex = p.ParseInt64WithDefault("rootCoord.minSegmentSizeToEnableIndex", 1024)
}

func (p *ParamTable) initDefaultPartitionName() {
	name := p.LoadWithDefault("common.defaultPartitionName", "_default")
	p.DefaultPartitionName = name
}

func (p *ParamTable) initDefaultIndexName() {
	name := p.LoadWithDefault("common.defaultIndexName", "_default_idx")
	p.DefaultIndexName = name
}

func (p *ParamTable) initTimeout() {
	p.Timeout = p.ParseIntWithDefault("rootCoord.timeout", 3600)
}

func (p *ParamTable) initTimeTickInterval() {
	p.TimeTickInterval = p.ParseIntWithDefault("rootCoord.timeTickInterval", 200)
}

func (p *ParamTable) initRoleName() {
	p.RoleName = "rootcoord"
}
