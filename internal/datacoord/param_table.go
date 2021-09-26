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

package datacoord

import (
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	NodeID int64

	IP   string
	Port int

	// --- ETCD ---
	EtcdEndpoints           []string
	MetaRootPath            string
	KvRootPath              string
	SegmentBinlogSubPath    string
	CollectionBinlogSubPath string

	// --- Pulsar ---
	PulsarAddress string

	// --- Rocksmq ---
	RocksmqPath string

	FlushStreamPosSubPath string
	StatsStreamPosSubPath string

	// --- SEGMENTS ---
	SegmentMaxSize          float64
	SegmentSealProportion   float64
	SegAssignmentExpiration int64

	// --- Channels ---
	ClusterChannelPrefix      string
	InsertChannelPrefixName   string
	StatisticsChannelName     string
	TimeTickChannelName       string
	SegmentInfoChannelName    string
	DataCoordSubscriptionName string

	CreatedTime time.Time
	UpdatedTime time.Time
}

var Params ParamTable
var once sync.Once

/* Init params from base table as well as data coord yaml*/
func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()

	if err := p.LoadYaml("advanced/data_coord.yaml"); err != nil {
		panic(err)
	}

	// set members
	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initSegmentBinlogSubPath()
	p.initCollectionBinlogSubPath()

	p.initPulsarAddress()
	p.initRocksmqPath()

	p.initSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initInsertChannelPrefixName()
	p.initStatisticsChannelName()
	p.initTimeTickChannelName()
	p.initSegmentInfoChannelName()
	p.initDataCoordSubscriptionName()
	p.initLogCfg()

	p.initFlushStreamPosSubPath()
	p.initStatsStreamPosSubPath()
}

// Init once ensure param table is a singleton
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
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

func (p *ParamTable) initSegmentBinlogSubPath() {
	subPath, err := p.Load("etcd.segmentBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.SegmentBinlogSubPath = subPath
}

func (p *ParamTable) initCollectionBinlogSubPath() {
	subPath, err := p.Load("etcd.collectionBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.CollectionBinlogSubPath = subPath
}

func (p *ParamTable) initSegmentMaxSize() {
	p.SegmentMaxSize = p.ParseFloat("datacoord.segment.maxSize")
}

func (p *ParamTable) initSegmentSealProportion() {
	p.SegmentSealProportion = p.ParseFloat("datacoord.segment.sealProportion")
}

func (p *ParamTable) initSegAssignmentExpiration() {
	p.SegAssignmentExpiration = p.ParseInt64("datacoord.segment.assignmentExpiration")
}

func (p *ParamTable) initClusterMsgChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *ParamTable) initInsertChannelPrefixName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordInsertChannel")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.InsertChannelPrefixName = strings.Join(s, "-")
}

func (p *ParamTable) initStatisticsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordStatistic")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatisticsChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initSegmentInfoChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SegmentInfoChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initDataCoordSubscriptionName() {
	config, err := p.Load("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DataCoordSubscriptionName = strings.Join(s, "-")
}

func (p *ParamTable) initLogCfg() {
	p.InitLogCfg("datacoord", 0)
}

func (p *ParamTable) initFlushStreamPosSubPath() {
	subPath, err := p.Load("etcd.flushStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.FlushStreamPosSubPath = subPath
}

func (p *ParamTable) initStatsStreamPosSubPath() {
	subPath, err := p.Load("etcd.statsStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.StatsStreamPosSubPath = subPath
}
