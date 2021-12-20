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

package querycoord

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is an alias for the Int64 type
type UniqueID = typeutil.UniqueID

// ParamTable maintains some environment variables that are required for the QueryCoord runtime
type ParamTable struct {
	paramtable.BaseTable

	NodeID uint64

	Address      string
	Port         int
	QueryCoordID UniqueID

	// stats
	StatsChannelName string

	// timetick
	TimeTickChannelName string

	// channels
	ClusterChannelPrefix      string
	SearchChannelPrefix       string
	SearchResultChannelPrefix string

	// --- etcd ---
	EtcdEndpoints []string
	MetaRootPath  string
	KvRootPath    string

	//--- Minio ---
	MinioEndPoint        string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSLStr       bool
	MinioBucketName      string

	CreatedTime time.Time
	UpdatedTime time.Time

	DmlChannelPrefix   string
	DeltaChannelPrefix string

	// --- Pulsar ---
	PulsarAddress string

	//---- Handoff ---
	AutoHandoff bool

	//---- Balance ---
	AutoBalance                         bool
	OverloadedMemoryThresholdPercentage float64
	BalanceIntervalSeconds              int64
	MemoryUsageMaxDifferencePercentage  float64
}

// Params are variables of the ParamTable type
var Params ParamTable
var once sync.Once

// InitOnce guarantees that variables are initialized only once
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

//Init is used to initialize params
func (p *ParamTable) Init() {
	p.BaseTable.Init()

	p.initRoleName()

	// --- Channels ---
	p.initClusterMsgChannelPrefix()
	p.initSearchChannelPrefix()
	p.initSearchResultChannelPrefix()
	p.initStatsChannelName()
	p.initTimeTickChannelName()

	// --- etcd ---
	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initKvRootPath()

	//--- Minio ----
	p.initMinioEndPoint()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSLStr()
	p.initMinioBucketName()

	//--- Pulsar ----
	p.initPulsarAddress()

	//---- Handoff ---
	p.initAutoHandoff()

	p.initDmlChannelName()
	p.initDeltaChannelName()

	//---- Balance ---
	p.initAutoBalance()
	p.initOverloadedMemoryThresholdPercentage()
	p.initBalanceIntervalSeconds()
	p.initMemoryUsageMaxDifferencePercentage()
}

func (p *ParamTable) initClusterMsgChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *ParamTable) initSearchChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Error(err.Error())
	}

	s := []string{p.ClusterChannelPrefix, config}
	p.SearchChannelPrefix = strings.Join(s, "-")
}

func (p *ParamTable) initSearchResultChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		log.Error(err.Error())
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SearchResultChannelPrefix = strings.Join(s, "-")
}

func (p *ParamTable) initStatsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatsChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
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
	p.MetaRootPath = path.Join(rootPath, subPath)
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
	p.KvRootPath = path.Join(rootPath, subPath)
}

func (p *ParamTable) initMinioEndPoint() {
	url, err := p.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioEndPoint = url
}

func (p *ParamTable) initMinioAccessKeyID() {
	id, err := p.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = id
}

func (p *ParamTable) initMinioSecretAccessKey() {
	key, err := p.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *ParamTable) initMinioUseSSLStr() {
	ssl, err := p.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	sslBoolean, err := strconv.ParseBool(ssl)
	if err != nil {
		panic(err)
	}
	p.MinioUseSSLStr = sslBoolean
}

func (p *ParamTable) initMinioBucketName() {
	bucketName, err := p.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *ParamTable) initRoleName() {
	p.RoleName = "querycoord"
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initAutoHandoff() {
	handoff, err := p.Load("queryCoord.autoHandoff")
	if err != nil {
		panic(err)
	}
	p.AutoHandoff, err = strconv.ParseBool(handoff)
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initAutoBalance() {
	balanceStr := p.LoadWithDefault("queryCoord.autoBalance", "false")
	autoBalance, err := strconv.ParseBool(balanceStr)
	if err != nil {
		panic(err)
	}
	p.AutoBalance = autoBalance
}

func (p *ParamTable) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}

func (p *ParamTable) initBalanceIntervalSeconds() {
	balanceInterval := p.LoadWithDefault("queryCoord.balanceIntervalSeconds", "60")
	interval, err := strconv.ParseInt(balanceInterval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.BalanceIntervalSeconds = interval
}

func (p *ParamTable) initMemoryUsageMaxDifferencePercentage() {
	maxDiff := p.LoadWithDefault("queryCoord.memoryUsageMaxDifferencePercentage", "30")
	diffPercentage, err := strconv.ParseInt(maxDiff, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MemoryUsageMaxDifferencePercentage = float64(diffPercentage) / 100
}

func (p *ParamTable) initDmlChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		config = "rootcoord-dml"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelPrefix = strings.Join(s, "-")
}

func (p *ParamTable) initDeltaChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelPrefix = strings.Join(s, "-")
}
