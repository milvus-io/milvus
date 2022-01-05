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

package paramtable

import (
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-basic/ipv4"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	// DefaultServerMaxSendSize defines the maximum size of data per grpc request can send by server side.
	DefaultServerMaxSendSize = math.MaxInt32

	// DefaultServerMaxRecvSize defines the maximum size of data per grpc request can receive by server side.
	DefaultServerMaxRecvSize = math.MaxInt32

	// DefaultClientMaxSendSize defines the maximum size of data per grpc request can send by client side.
	DefaultClientMaxSendSize = 100 * 1024 * 1024

	// DefaultClientMaxRecvSize defines the maximum size of data per grpc request can receive by client side.
	DefaultClientMaxRecvSize = 100 * 1024 * 1024

	// SuggestPulsarMaxMessageSize defines the maximum size of Pulsar message.
	SuggestPulsarMaxMessageSize = 5 * 1024 * 1024

	// DefaultRetentionDuration defines the default duration for retention which is 5 days in seconds.
	DefaultRetentionDuration = 3600 * 24 * 5
)

// GlobalParamTable is a derived struct of BaseParamTable.
// It is used to quickly and easily access global system configuration.
type GlobalParamTable struct {
	once       sync.Once
	BaseParams BaseParamTable

	//CommonCfg     commonConfig
	//KnowhereCfg   knowhereConfig
	//MsgChannelCfg msgChannelConfig

	RootCoordCfg  rootCoordConfig
	ProxyCfg      proxyConfig
	QueryCoordCfg queryCoordConfig
	QueryNodeCfg  queryNodeConfig
	DataCoordCfg  dataCoordConfig
	DataNodeCfg   dataNodeConfig
	IndexCoordCfg indexCoordConfig
	IndexNodeCfg  indexNodeConfig
}

// InitOnce initialize once
func (p *GlobalParamTable) InitOnce() {
	p.once.Do(func() {
		p.Init()
	})
}

func (p *GlobalParamTable) Init() {
	p.BaseParams.Init()

	//p.CommonCfg.init(&p.BaseParams)
	//p.KnowhereCfg.init(&p.BaseParams)
	//p.MsgChannelCfg.init(&p.BaseParams)

	p.RootCoordCfg.init(&p.BaseParams)
	p.ProxyCfg.init(&p.BaseParams)
	p.QueryCoordCfg.init(&p.BaseParams)
	p.QueryNodeCfg.init(&p.BaseParams)
	p.DataCoordCfg.init(&p.BaseParams)
	p.DataNodeCfg.init(&p.BaseParams)
	p.IndexCoordCfg.init(&p.BaseParams)
	p.IndexNodeCfg.init(&p.BaseParams)
}

func (p *GlobalParamTable) SetLogConfig(role string) {
	p.BaseParams.RoleName = role
	p.BaseParams.SetLogConfig()
}

///////////////////////////////////////////////////////////////////////////////
// --- common ---
//type commonConfig struct {
//	BaseParams *BaseParamTable
//
//	DefaultPartitionName string
//	DefaultIndexName     string
//}
//
//func (p *commonConfig) init(bp *BaseParamTable) {
//	p.BaseParams = bp
//	p.initDefaultPartitionName()
//	p.initDefaultIndexName()
//}
//
//func (p *commonConfig) initDefaultPartitionName() {
//	name := p.BaseParams.LoadWithDefault("common.defaultPartitionName", "_default")
//	p.DefaultPartitionName = name
//}
//
//func (p *commonConfig) initDefaultIndexName() {
//	name := p.BaseParams.LoadWithDefault("common.defaultIndexName", "_default_idx")
//	p.DefaultIndexName = name
//}

///////////////////////////////////////////////////////////////////////////////
// --- knowhere ---
//type knowhereConfig struct {
//	BaseParams *BaseParamTable
//}
//
//func (p *knowhereConfig) init(bp *BaseParamTable) {
//	p.BaseParams = bp
//}

///////////////////////////////////////////////////////////////////////////////
// --- msgChannel ---
//type msgChannelConfig struct {
//	BaseParams *BaseParamTable
//
//	ClusterPrefix          string
//	RootCoordTimeTick      string
//	RootCoordStatistics    string
//	RootCoordDml           string
//	RootCoordDelta         string
//	QueryCoordSearch       string
//	QueryCoordSearchResult string
//	ProxyTimeTick          string
//	QueryTimeTick          string
//	QueryNodeStats         string
//	Cmd                    string
//	DataCoordInsertChannel string
//	DataCoordStatistic     string
//	DataCoordTimeTick      string
//	DataCoordSegmentInfo   string
//
//	SkipQueryChannelRecovery string
//
//	RootCoordSubNamePrefix string
//	ProxySubNamePrefix     string
//	QueryNodeSubNamePrefix string
//	DataNodeSubNamePrefix  string
//	DataCoordSubNamePrefix string
//}
//
//func (p *msgChannelConfig) init(bp *BaseParamTable) {
//	p.BaseParams = bp
//
//	// must init cluster prefix first
//	p.initClusterPrefix()
//	p.initRootCoordTimeTick()
//	p.initRootCoordStatistics()
//	p.initRootCoordDml()
//	p.initRootCoordDelta()
//	p.initQueryCoordSearch()
//	p.initQueryCoordSearchResult()
//	p.initProxyTimeTick()
//	p.initQueryTimeTick()
//	p.initQueryNodeStats()
//	p.initMsgChannelCmd()
//	p.initDataCoordInsertChannel()
//	p.initDataCoordStatistic()
//	p.initDataCoordTimeTick()
//	p.initDataCoordSegmentInfo()
//
//	p.initRootCoordSubNamePrefix()
//	p.initProxySubNamePrefix()
//	p.initQueryNodeSubNamePrefix()
//	p.initDataNodeSubNamePrefix()
//	p.initDataCoordSubNamePrefix()
//}
//
//func (p *msgChannelConfig) initClusterPrefix() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
//	if err != nil {
//		panic(err)
//	}
//	p.ClusterPrefix = config
//}
//
//func (p *msgChannelConfig) initChanNamePrefix(cfg string) string {
//	value, err := p.BaseParams.Load(cfg)
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterPrefix, value}
//	return strings.Join(s, "-")
//}
//
//// --- msgChannel.chanNamePrefix ---
//func (p *msgChannelConfig) initRootCoordTimeTick() {
//	p.RootCoordTimeTick = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordTimeTick")
//}
//
//func (p *msgChannelConfig) initRootCoordStatistics() {
//	p.RootCoordStatistics = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordStatistics")
//}
//
//func (p *msgChannelConfig) initRootCoordDml() {
//	p.RootCoordDml = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordDml")
//}
//
//func (p *msgChannelConfig) initRootCoordDelta() {
//	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordDelta")
//}
//
//func (p *msgChannelConfig) initQueryCoordSearch() {
//	p.QueryCoordSearch = p.initChanNamePrefix("msgChannel.chanNamePrefix.search")
//}
//
//func (p *msgChannelConfig) initQueryCoordSearchResult() {
//	p.QueryCoordSearchResult = p.initChanNamePrefix("msgChannel.chanNamePrefix.searchResult")
//}
//
//func (p *msgChannelConfig) initProxyTimeTick() {
//	p.ProxyTimeTick = p.initChanNamePrefix("msgChannel.chanNamePrefix.proxyTimeTick")
//}
//
//func (p *msgChannelConfig) initQueryTimeTick() {
//	p.QueryTimeTick = p.initChanNamePrefix("msgChannel.chanNamePrefix.queryTimeTick")
//}
//
//func (p *msgChannelConfig) initQueryNodeStats() {
//	p.QueryNodeStats = p.initChanNamePrefix("msgChannel.chanNamePrefix.queryNodeStats")
//}
//
//func (p *msgChannelConfig) initMsgChannelCmd() {
//	p.Cmd = p.initChanNamePrefix("msgChannel.chanNamePrefix.cmd")
//}
//
//func (p *msgChannelConfig) initDataCoordInsertChannel() {
//	p.DataCoordInsertChannel = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordInsertChannel")
//}
//
//func (p *msgChannelConfig) initDataCoordStatistic() {
//	p.DataCoordStatistic = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordStatistic")
//}
//
//func (p *msgChannelConfig) initDataCoordTimeTick() {
//	p.DataCoordTimeTick = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordTimeTick")
//}
//
//func (p *msgChannelConfig) initDataCoordSegmentInfo() {
//	p.DataCoordSegmentInfo = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
//}
//
//// --- msgChannel.subNamePrefix ---
//func (p *msgChannelConfig) initRootCoordSubNamePrefix() {
//	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
//}
//
//func (p *msgChannelConfig) initProxySubNamePrefix() {
//	p.ProxySubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.proxySubNamePrefix")
//}
//
//func (p *msgChannelConfig) initQueryNodeSubNamePrefix() {
//	p.QueryNodeSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
//}
//
//func (p *msgChannelConfig) initDataNodeSubNamePrefix() {
//	p.DataNodeSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
//}
//
//func (p *msgChannelConfig) initDataCoordSubNamePrefix() {
//	p.DataCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
//}

///////////////////////////////////////////////////////////////////////////////
// --- rootcoord ---
type rootCoordConfig struct {
	BaseParams *BaseParamTable

	Address string
	Port    int

	PulsarAddress string
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

	Timeout int

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *rootCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initPulsarAddress()
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
}

func (p *rootCoordConfig) initPulsarAddress() {
	addr, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *rootCoordConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *rootCoordConfig) initKvRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *rootCoordConfig) initClusterMsgChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *rootCoordConfig) initMsgChannelSubName() {
	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *rootCoordConfig) initTimeTickChannel() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannel = strings.Join(s, "-")
}

func (p *rootCoordConfig) initStatisticsChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordStatistics")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatisticsChannel = strings.Join(s, "-")
}

func (p *rootCoordConfig) initDmlChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelName = strings.Join(s, "-")
}

func (p *rootCoordConfig) initDeltaChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelName = strings.Join(s, "-")
}

func (p *rootCoordConfig) initDmlChannelNum() {
	p.DmlChannelNum = p.BaseParams.ParseInt64WithDefault("rootCoord.dmlChannelNum", 256)
}

func (p *rootCoordConfig) initMaxPartitionNum() {
	p.MaxPartitionNum = p.BaseParams.ParseInt64WithDefault("rootCoord.maxPartitionNum", 4096)
}

func (p *rootCoordConfig) initMinSegmentSizeToEnableIndex() {
	p.MinSegmentSizeToEnableIndex = p.BaseParams.ParseInt64WithDefault("rootCoord.minSegmentSizeToEnableIndex", 1024)
}

func (p *rootCoordConfig) initDefaultPartitionName() {
	name := p.BaseParams.LoadWithDefault("common.defaultPartitionName", "_default")
	p.DefaultPartitionName = name
}

func (p *rootCoordConfig) initDefaultIndexName() {
	name := p.BaseParams.LoadWithDefault("common.defaultIndexName", "_default_idx")
	p.DefaultIndexName = name
}

func (p *rootCoordConfig) initTimeout() {
	p.Timeout = p.BaseParams.ParseIntWithDefault("rootCoord.timeout", 3600)
}

///////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type proxyConfig struct {
	BaseParams *BaseParamTable

	// NetworkPort & IP are not used
	NetworkPort int
	IP          string

	NetworkAddress string

	Alias string

	MetaRootPath  string
	PulsarAddress string

	RocksmqPath string // not used in Proxy

	ProxyID                  UniqueID
	TimeTickInterval         time.Duration
	MsgStreamTimeTickBufSize int64
	MaxNameLength            int64
	MaxFieldNum              int64
	MaxShardNum              int32
	MaxDimension             int64
	DefaultPartitionName     string
	DefaultIndexName         string
	BufFlagExpireTime        time.Duration
	BufFlagCleanupInterval   time.Duration

	// --- Channels ---
	ClusterChannelPrefix      string
	ProxyTimeTickChannelNames []string
	ProxySubName              string

	// required from query coord
	SearchResultChannelNames   []string
	RetrieveResultChannelNames []string

	MaxTaskNum int64

	PulsarMaxMessageSize int

	RetentionDuration int64

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *proxyConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initMetaRootPath()
	p.initPulsarAddress()
	p.initRocksmqPath()
	p.initTimeTickInterval()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initProxySubName()
	p.initProxyTimeTickChannelNames()
	p.initMsgStreamTimeTickBufSize()
	p.initMaxNameLength()
	p.initMaxFieldNum()
	p.initMaxShardNum()
	p.initMaxDimension()
	p.initDefaultPartitionName()
	p.initDefaultIndexName()

	p.initPulsarMaxMessageSize()

	p.initMaxTaskNum()
	p.initBufFlagExpireTime()
	p.initBufFlagCleanupInterval()
	p.initRetentionDuration()
}

// Refresh is called after session init
func (p *proxyConfig) Refresh() {
	p.initProxySubName()
}

// InitAlias initialize Alias member.
func (p *proxyConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *proxyConfig) initPulsarAddress() {
	ret, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = ret
}

func (p *proxyConfig) initRocksmqPath() {
	path, err := p.BaseParams.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *proxyConfig) initTimeTickInterval() {
	interval := p.BaseParams.ParseIntWithDefault("proxy.timeTickInterval", 200)
	p.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

func (p *proxyConfig) initClusterMsgChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *proxyConfig) initProxySubName() {
	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config, strconv.FormatInt(p.ProxyID, 10)}
	p.ProxySubName = strings.Join(s, "-")
}

func (p *proxyConfig) initProxyTimeTickChannelNames() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config, "0"}
	prefix := strings.Join(s, "-")
	p.ProxyTimeTickChannelNames = []string{prefix}
}

func (p *proxyConfig) initMsgStreamTimeTickBufSize() {
	p.MsgStreamTimeTickBufSize = p.BaseParams.ParseInt64WithDefault("proxy.msgStream.timeTick.bufSize", 512)
}

func (p *proxyConfig) initMaxNameLength() {
	str := p.BaseParams.LoadWithDefault("proxy.maxNameLength", "255")
	maxNameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxNameLength = maxNameLength
}

func (p *proxyConfig) initMaxShardNum() {
	str := p.BaseParams.LoadWithDefault("proxy.maxShardNum", "256")
	maxShardNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxShardNum = int32(maxShardNum)
}

func (p *proxyConfig) initMaxFieldNum() {
	str := p.BaseParams.LoadWithDefault("proxy.maxFieldNum", "64")
	maxFieldNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxFieldNum = maxFieldNum
}

func (p *proxyConfig) initMaxDimension() {
	str := p.BaseParams.LoadWithDefault("proxy.maxDimension", "32768")
	maxDimension, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxDimension = maxDimension
}

func (p *proxyConfig) initDefaultPartitionName() {
	name := p.BaseParams.LoadWithDefault("common.defaultPartitionName", "_default")
	p.DefaultPartitionName = name
}

func (p *proxyConfig) initDefaultIndexName() {
	name := p.BaseParams.LoadWithDefault("common.defaultIndexName", "_default_idx")
	p.DefaultIndexName = name
}

func (p *proxyConfig) initPulsarMaxMessageSize() {
	maxMessageSizeStr, err := p.BaseParams.Load("pulsar.maxMessageSize")
	if err != nil {
		p.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
	} else {
		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
		if err != nil {
			p.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
		} else {
			p.PulsarMaxMessageSize = maxMessageSize
		}
	}
}

func (p *proxyConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *proxyConfig) initMaxTaskNum() {
	p.MaxTaskNum = p.BaseParams.ParseInt64WithDefault("proxy.maxTaskNum", 1024)
}

func (p *proxyConfig) initBufFlagExpireTime() {
	expireTime := p.BaseParams.ParseInt64WithDefault("proxy.bufFlagExpireTime", 3600)
	p.BufFlagExpireTime = time.Duration(expireTime) * time.Second
}

func (p *proxyConfig) initBufFlagCleanupInterval() {
	interval := p.BaseParams.ParseInt64WithDefault("proxy.bufFlagCleanupInterval", 600)
	p.BufFlagCleanupInterval = time.Duration(interval) * time.Second
}

func (p *proxyConfig) initRetentionDuration() {
	p.RetentionDuration = p.BaseParams.ParseInt64WithDefault("common.retentionDuration", DefaultRetentionDuration)
}

///////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	BaseParams *BaseParamTable

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
	MetaRootPath string
	KvRootPath   string

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

func (p *queryCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	// --- Channels ---
	p.initClusterMsgChannelPrefix()
	p.initSearchChannelPrefix()
	p.initSearchResultChannelPrefix()
	p.initStatsChannelName()
	p.initTimeTickChannelName()

	// --- etcd ---
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

func (p *queryCoordConfig) initClusterMsgChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *queryCoordConfig) initSearchChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Error(err.Error())
	}

	s := []string{p.ClusterChannelPrefix, config}
	p.SearchChannelPrefix = strings.Join(s, "-")
}

func (p *queryCoordConfig) initSearchResultChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		log.Error(err.Error())
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SearchResultChannelPrefix = strings.Join(s, "-")
}

func (p *queryCoordConfig) initStatsChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatsChannelName = strings.Join(s, "-")
}

func (p *queryCoordConfig) initTimeTickChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *queryCoordConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *queryCoordConfig) initKvRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = path.Join(rootPath, subPath)
}

func (p *queryCoordConfig) initMinioEndPoint() {
	url, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioEndPoint = url
}

func (p *queryCoordConfig) initMinioAccessKeyID() {
	id, err := p.BaseParams.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = id
}

func (p *queryCoordConfig) initMinioSecretAccessKey() {
	key, err := p.BaseParams.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *queryCoordConfig) initMinioUseSSLStr() {
	ssl, err := p.BaseParams.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	sslBoolean, err := strconv.ParseBool(ssl)
	if err != nil {
		panic(err)
	}
	p.MinioUseSSLStr = sslBoolean
}

func (p *queryCoordConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *queryCoordConfig) initPulsarAddress() {
	addr, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *queryCoordConfig) initAutoHandoff() {
	handoff, err := p.BaseParams.Load("queryCoord.autoHandoff")
	if err != nil {
		panic(err)
	}
	p.AutoHandoff, err = strconv.ParseBool(handoff)
	if err != nil {
		panic(err)
	}
}

func (p *queryCoordConfig) initAutoBalance() {
	balanceStr := p.BaseParams.LoadWithDefault("queryCoord.autoBalance", "false")
	autoBalance, err := strconv.ParseBool(balanceStr)
	if err != nil {
		panic(err)
	}
	p.AutoBalance = autoBalance
}

func (p *queryCoordConfig) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.BaseParams.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}

func (p *queryCoordConfig) initBalanceIntervalSeconds() {
	balanceInterval := p.BaseParams.LoadWithDefault("queryCoord.balanceIntervalSeconds", "60")
	interval, err := strconv.ParseInt(balanceInterval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.BalanceIntervalSeconds = interval
}

func (p *queryCoordConfig) initMemoryUsageMaxDifferencePercentage() {
	maxDiff := p.BaseParams.LoadWithDefault("queryCoord.memoryUsageMaxDifferencePercentage", "30")
	diffPercentage, err := strconv.ParseInt(maxDiff, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MemoryUsageMaxDifferencePercentage = float64(diffPercentage) / 100
}

func (p *queryCoordConfig) initDmlChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		config = "rootcoord-dml"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelPrefix = strings.Join(s, "-")
}

func (p *queryCoordConfig) initDeltaChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelPrefix = strings.Join(s, "-")
}

///////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	BaseParams *BaseParamTable

	PulsarAddress string
	RocksmqPath   string
	MetaRootPath  string

	Alias         string
	QueryNodeIP   string
	QueryNodePort int64
	QueryNodeID   UniqueID
	// TODO: remove cacheSize
	CacheSize int64 // deprecated

	// channel prefix
	ClusterChannelPrefix     string
	QueryTimeTickChannelName string
	StatsChannelName         string
	MsgChannelSubName        string

	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32

	// minio
	MinioEndPoint        string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSLStr       bool
	MinioBucketName      string

	// search
	SearchChannelNames         []string
	SearchResultChannelNames   []string
	SearchReceiveBufSize       int64
	SearchPulsarBufSize        int64
	SearchResultReceiveBufSize int64

	// Retrieve
	RetrieveChannelNames         []string
	RetrieveResultChannelNames   []string
	RetrieveReceiveBufSize       int64
	RetrievePulsarBufSize        int64
	RetrieveResultReceiveBufSize int64

	// stats
	StatsPublishInterval int

	GracefulTime int64
	SliceIndex   int

	// segcore
	ChunkRows int64
	SimdType  string

	CreatedTime time.Time
	UpdatedTime time.Time

	// recovery
	SkipQueryChannelRecovery bool

	// memory limit
	OverloadedMemoryThresholdPercentage float64
}

func (p *queryNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initCacheSize()

	p.initMinioEndPoint()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSLStr()
	p.initMinioBucketName()

	p.initPulsarAddress()
	p.initRocksmqPath()
	p.initMetaRootPath()

	p.initGracefulTime()

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()

	p.initSearchReceiveBufSize()
	p.initSearchPulsarBufSize()
	p.initSearchResultReceiveBufSize()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initQueryTimeTickChannelName()
	p.initStatsChannelName()
	p.initMsgChannelSubName()

	p.initStatsPublishInterval()

	p.initSegcoreChunkRows()
	p.initKnowhereSimdType()

	p.initSkipQueryChannelRecovery()
	p.initOverloadedMemoryThresholdPercentage()
}

// InitAlias initializes an alias for the QueryNode role.
func (p *queryNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

// Refresh is called after session init
func (p *queryNodeConfig) Refresh() {
	p.initMsgChannelSubName()
}

func (p *queryNodeConfig) initCacheSize() {
	defer log.Debug("init cacheSize", zap.Any("cacheSize (GB)", p.CacheSize))

	const defaultCacheSize = 32 // GB
	p.CacheSize = defaultCacheSize

	var err error
	cacheSize := os.Getenv("CACHE_SIZE")
	if cacheSize == "" {
		cacheSize, err = p.BaseParams.Load("queryNode.cacheSize")
		if err != nil {
			return
		}
	}
	value, err := strconv.ParseInt(cacheSize, 10, 64)
	if err != nil {
		return
	}
	p.CacheSize = value
}

// ---------------------------------------------------------- minio
func (p *queryNodeConfig) initMinioEndPoint() {
	url, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioEndPoint = url
}

func (p *queryNodeConfig) initMinioAccessKeyID() {
	id, err := p.BaseParams.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = id
}

func (p *queryNodeConfig) initMinioSecretAccessKey() {
	key, err := p.BaseParams.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *queryNodeConfig) initMinioUseSSLStr() {
	ssl, err := p.BaseParams.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	sslBoolean, err := strconv.ParseBool(ssl)
	if err != nil {
		panic(err)
	}
	p.MinioUseSSLStr = sslBoolean
}

func (p *queryNodeConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *queryNodeConfig) initPulsarAddress() {
	url, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *queryNodeConfig) initRocksmqPath() {
	path, err := p.BaseParams.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

// advanced params
// stats
func (p *queryNodeConfig) initStatsPublishInterval() {
	p.StatsPublishInterval = p.BaseParams.ParseIntWithDefault("queryNode.stats.publishInterval", 1000)
}

// dataSync:
func (p *queryNodeConfig) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.BaseParams.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *queryNodeConfig) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.BaseParams.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxParallelism", 1024)
}

// msgStream
func (p *queryNodeConfig) initSearchReceiveBufSize() {
	p.SearchReceiveBufSize = p.BaseParams.ParseInt64WithDefault("queryNode.msgStream.search.recvBufSize", 512)
}

func (p *queryNodeConfig) initSearchPulsarBufSize() {
	p.SearchPulsarBufSize = p.BaseParams.ParseInt64WithDefault("queryNode.msgStream.search.pulsarBufSize", 512)
}

func (p *queryNodeConfig) initSearchResultReceiveBufSize() {
	p.SearchResultReceiveBufSize = p.BaseParams.ParseInt64WithDefault("queryNode.msgStream.searchResult.recvBufSize", 64)
}

// ------------------------  channel names
func (p *queryNodeConfig) initClusterMsgChannelPrefix() {
	name, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = name
}

func (p *queryNodeConfig) initQueryTimeTickChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		log.Warn(err.Error())
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.QueryTimeTickChannelName = strings.Join(s, "-")
}

func (p *queryNodeConfig) initMsgChannelSubName() {
	namePrefix, err := p.BaseParams.Load("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
	if err != nil {
		log.Warn(err.Error())
	}

	s := []string{p.ClusterChannelPrefix, namePrefix, strconv.FormatInt(p.QueryNodeID, 10)}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *queryNodeConfig) initStatsChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatsChannelName = strings.Join(s, "-")
}

func (p *queryNodeConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *queryNodeConfig) initGracefulTime() {
	p.GracefulTime = p.BaseParams.ParseInt64("queryNode.gracefulTime")
	log.Debug("query node init gracefulTime", zap.Any("gracefulTime", p.GracefulTime))
}

func (p *queryNodeConfig) initSegcoreChunkRows() {
	p.ChunkRows = p.BaseParams.ParseInt64WithDefault("queryNode.segcore.chunkRows", 32768)
}

func (p *queryNodeConfig) initKnowhereSimdType() {
	simdType := p.BaseParams.LoadWithDefault("knowhere.simdType", "auto")
	p.SimdType = simdType
	log.Debug("initialize the knowhere simd type", zap.String("simd_type", p.SimdType))
}

func (p *queryNodeConfig) initSkipQueryChannelRecovery() {
	p.SkipQueryChannelRecovery = p.BaseParams.ParseBool("msgChannel.skipQueryChannelRecovery", false)
}

func (p *queryNodeConfig) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.BaseParams.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}

///////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	BaseParams *BaseParamTable

	NodeID int64

	IP      string
	Port    int
	Address string

	// --- ETCD ---
	MetaRootPath        string
	KvRootPath          string
	ChannelWatchSubPath string

	// --- MinIO ---
	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string
	MinioRootPath        string

	// --- Pulsar ---
	PulsarAddress string

	// --- Rocksmq ---
	RocksmqPath string

	// --- SEGMENTS ---
	SegmentMaxSize          float64
	SegmentSealProportion   float64
	SegAssignmentExpiration int64

	// --- Channels ---
	ClusterChannelPrefix      string
	InsertChannelPrefixName   string
	TimeTickChannelName       string
	SegmentInfoChannelName    string
	DataCoordSubscriptionName string

	CreatedTime time.Time
	UpdatedTime time.Time

	EnableCompaction        bool
	EnableGarbageCollection bool

	RetentionDuration    int64
	EnableAutoCompaction bool

	// Garbage Collection
	GCInterval         time.Duration
	GCMissingTolerance time.Duration
	GCDropTolerance    time.Duration
}

func (p *dataCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initMetaRootPath()
	p.initKvRootPath()
	p.initChannelWatchPrefix()

	p.initPulsarAddress()
	p.initRocksmqPath()

	p.initSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initInsertChannelPrefixName()
	p.initTimeTickChannelName()
	p.initSegmentInfoChannelName()
	p.initDataCoordSubscriptionName()

	p.initEnableCompaction()

	p.initMinioAddress()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSL()
	p.initMinioBucketName()
	p.initMinioRootPath()

	p.initRetentionDuration()
	p.initEnableAutoCompaction()

	p.initEnableGarbageCollection()
	p.initGCInterval()
	p.initGCMissingTolerance()
	p.initGCDropTolerance()
}

func (p *dataCoordConfig) initPulsarAddress() {
	addr, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *dataCoordConfig) initRocksmqPath() {
	path, err := p.BaseParams.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *dataCoordConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *dataCoordConfig) initKvRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *dataCoordConfig) initSegmentMaxSize() {
	p.SegmentMaxSize = p.BaseParams.ParseFloatWithDefault("dataCoord.segment.maxSize", 512.0)
}

func (p *dataCoordConfig) initSegmentSealProportion() {
	p.SegmentSealProportion = p.BaseParams.ParseFloatWithDefault("dataCoord.segment.sealProportion", 0.75)
}

func (p *dataCoordConfig) initSegAssignmentExpiration() {
	p.SegAssignmentExpiration = p.BaseParams.ParseInt64WithDefault("dataCoord.segment.assignmentExpiration", 2000)
}

func (p *dataCoordConfig) initClusterMsgChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *dataCoordConfig) initInsertChannelPrefixName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.dataCoordInsertChannel")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.InsertChannelPrefixName = strings.Join(s, "-")
}

func (p *dataCoordConfig) initTimeTickChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *dataCoordConfig) initSegmentInfoChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SegmentInfoChannelName = strings.Join(s, "-")
}

func (p *dataCoordConfig) initDataCoordSubscriptionName() {
	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DataCoordSubscriptionName = strings.Join(s, "-")
}

func (p *dataCoordConfig) initChannelWatchPrefix() {
	// WARN: this value should not be put to milvus.yaml. It's a default value for channel watch path.
	// This will be removed after we reconstruct our config module.
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataCoordConfig) initEnableCompaction() {
	p.EnableCompaction = p.BaseParams.ParseBool("dataCoord.enableCompaction", false)
}

// -- GC --

func (p *dataCoordConfig) initEnableGarbageCollection() {
	p.EnableGarbageCollection = p.BaseParams.ParseBool("dataCoord.enableGarbageCollection", false)
}

func (p *dataCoordConfig) initGCInterval() {
	p.GCInterval = time.Duration(p.BaseParams.ParseInt64WithDefault("dataCoord.gc.interval", 60*60)) * time.Second
}

func (p *dataCoordConfig) initGCMissingTolerance() {
	p.GCMissingTolerance = time.Duration(p.BaseParams.ParseInt64WithDefault("dataCoord.gc.missingTolerance", 24*60*60)) * time.Second
}

func (p *dataCoordConfig) initGCDropTolerance() {
	p.GCDropTolerance = time.Duration(p.BaseParams.ParseInt64WithDefault("dataCoord.gc.dropTolerance", 24*60*60)) * time.Second
}

// --- MinIO ---
func (p *dataCoordConfig) initMinioAddress() {
	endpoint, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioAddress = endpoint
}

func (p *dataCoordConfig) initMinioAccessKeyID() {
	keyID, err := p.BaseParams.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = keyID
}

func (p *dataCoordConfig) initMinioSecretAccessKey() {
	key, err := p.BaseParams.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *dataCoordConfig) initMinioUseSSL() {
	usessl, err := p.BaseParams.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	p.MinioUseSSL, _ = strconv.ParseBool(usessl)
}

func (p *dataCoordConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *dataCoordConfig) initMinioRootPath() {
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.MinioRootPath = rootPath
}

func (p *dataCoordConfig) initRetentionDuration() {
	p.RetentionDuration = p.BaseParams.ParseInt64WithDefault("common.retentionDuration", DefaultRetentionDuration)
}

func (p *dataCoordConfig) initEnableAutoCompaction() {
	p.EnableAutoCompaction = p.BaseParams.ParseBool("dataCoord.compaction.enableAutoCompaction", false)
}

///////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	BaseParams *BaseParamTable

	// ID of the current DataNode
	NodeID UniqueID

	// IP of the current DataNode
	IP string

	// Port of the current DataNode
	Port                    int
	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32
	FlushInsertBufferSize   int64
	InsertBinlogRootPath    string
	StatsBinlogRootPath     string
	DeleteBinlogRootPath    string
	Alias                   string // Different datanode in one machine

	// Channel Name
	DmlChannelName   string
	DeltaChannelName string

	// Pulsar address
	PulsarAddress string

	// Rocksmq path
	RocksmqPath string

	// Cluster channels
	ClusterChannelPrefix string

	// Timetick channel
	TimeTickChannelName string

	// Channel subscribition name -
	MsgChannelSubName string

	// etcd
	MetaRootPath        string
	ChannelWatchSubPath string

	// MinIO
	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *dataNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initInsertBinlogRootPath()
	p.initStatsBinlogRootPath()
	p.initDeleteBinlogRootPath()

	p.initPulsarAddress()
	p.initRocksmqPath()

	// Must init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initTimeTickChannelName()
	p.initMsgChannelSubName()

	p.initMetaRootPath()
	p.initChannelWatchPath()

	p.initMinioAddress()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSL()
	p.initMinioBucketName()

	p.initDmlChannelName()
	p.initDeltaChannelName()
}

// Refresh is called after session init
func (p *dataNodeConfig) Refresh() {
	p.initMsgChannelSubName()
}

// InitAlias init this DataNode alias
func (p *dataNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *dataNodeConfig) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.BaseParams.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *dataNodeConfig) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.BaseParams.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxParallelism", 1024)
}

func (p *dataNodeConfig) initFlushInsertBufferSize() {
	p.FlushInsertBufferSize = p.BaseParams.ParseInt64("_DATANODE_INSERTBUFSIZE")
}

func (p *dataNodeConfig) initInsertBinlogRootPath() {
	// GOOSE TODO: rootPath change to  TenentID
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.InsertBinlogRootPath = path.Join(rootPath, "insert_log")
}

func (p *dataNodeConfig) initStatsBinlogRootPath() {
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.StatsBinlogRootPath = path.Join(rootPath, "stats_log")
}

func (p *dataNodeConfig) initDeleteBinlogRootPath() {
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.DeleteBinlogRootPath = path.Join(rootPath, "delta_log")
}

func (p *dataNodeConfig) initPulsarAddress() {
	url, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *dataNodeConfig) initRocksmqPath() {
	path, err := p.BaseParams.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *dataNodeConfig) initClusterMsgChannelPrefix() {
	name, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = name
}

func (p *dataNodeConfig) initTimeTickChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *dataNodeConfig) initMsgChannelSubName() {
	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config, strconv.FormatInt(p.NodeID, 10)}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *dataNodeConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *dataNodeConfig) initChannelWatchPath() {
	p.ChannelWatchSubPath = "channelwatch"
}

// --- MinIO ---
func (p *dataNodeConfig) initMinioAddress() {
	endpoint, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioAddress = endpoint
}

func (p *dataNodeConfig) initMinioAccessKeyID() {
	keyID, err := p.BaseParams.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = keyID
}

func (p *dataNodeConfig) initMinioSecretAccessKey() {
	key, err := p.BaseParams.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *dataNodeConfig) initMinioUseSSL() {
	usessl, err := p.BaseParams.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	p.MinioUseSSL, _ = strconv.ParseBool(usessl)
}

func (p *dataNodeConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *dataNodeConfig) initDmlChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelName = strings.Join(s, "-")
}

func (p *dataNodeConfig) initDeltaChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelName = strings.Join(s, "-")
}

///////////////////////////////////////////////////////////////////////////////
// --- indexcoord ---
type indexCoordConfig struct {
	BaseParams *BaseParamTable

	Address string
	Port    int

	KvRootPath           string
	MetaRootPath         string
	IndexStorageRootPath string

	MinIOAddress         string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinioBucketName      string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *indexCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initMetaRootPath()
	p.initKvRootPath()
	p.initMinIOAddress()
	p.initMinIOAccessKeyID()
	p.initMinIOSecretAccessKey()
	p.initMinIOUseSSL()
	p.initMinioBucketName()
	p.initIndexStorageRootPath()
}

func (p *indexCoordConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *indexCoordConfig) initKvRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

// initMinIOAddress initializes init the minio address of configuration items.
func (p *indexCoordConfig) initMinIOAddress() {
	ret, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinIOAddress = ret
}

// initMinIOAccessKeyID initializes the minio access key of configuration items.
func (p *indexCoordConfig) initMinIOAccessKeyID() {
	ret, err := p.BaseParams.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinIOAccessKeyID = ret
}

// initMinIOSecretAccessKey initializes the minio secret access key.
func (p *indexCoordConfig) initMinIOSecretAccessKey() {
	ret, err := p.BaseParams.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinIOSecretAccessKey = ret
}

// initMinIOUseSSL initializes the minio use SSL of configuration items.
func (p *indexCoordConfig) initMinIOUseSSL() {
	ret, err := p.BaseParams.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	p.MinIOUseSSL, err = strconv.ParseBool(ret)
	if err != nil {
		panic(err)
	}
}

// initMinioBucketName initializes the minio bucket name of configuration items.
func (p *indexCoordConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

// initIndexStorageRootPath initializes the root path of index files.
func (p *indexCoordConfig) initIndexStorageRootPath() {
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.IndexStorageRootPath = path.Join(rootPath, "index_files")
}

///////////////////////////////////////////////////////////////////////////////
// --- indexnode ---
type indexNodeConfig struct {
	BaseParams *BaseParamTable

	IP      string
	Address string
	Port    int

	NodeID int64
	Alias  string

	MetaRootPath         string
	IndexStorageRootPath string

	MinIOAddress         string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinioBucketName      string

	SimdType string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *indexNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initMinIOAddress()
	p.initMinIOAccessKeyID()
	p.initMinIOSecretAccessKey()
	p.initMinIOUseSSL()
	p.initMinioBucketName()
	p.initMetaRootPath()
	p.initIndexStorageRootPath()
	p.initKnowhereSimdType()
}

// InitAlias initializes an alias for the IndexNode role.
func (p *indexNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *indexNodeConfig) initMinIOAddress() {
	ret, err := p.BaseParams.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinIOAddress = ret
}

func (p *indexNodeConfig) initMinIOAccessKeyID() {
	ret, err := p.BaseParams.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinIOAccessKeyID = ret
}

func (p *indexNodeConfig) initMinIOSecretAccessKey() {
	ret, err := p.BaseParams.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinIOSecretAccessKey = ret
}

func (p *indexNodeConfig) initMinIOUseSSL() {
	ret, err := p.BaseParams.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	p.MinIOUseSSL, err = strconv.ParseBool(ret)
	if err != nil {
		panic(err)
	}
}

func (p *indexNodeConfig) initMetaRootPath() {
	rootPath, err := p.BaseParams.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *indexNodeConfig) initIndexStorageRootPath() {
	rootPath, err := p.BaseParams.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.IndexStorageRootPath = path.Join(rootPath, "index_files")
}

func (p *indexNodeConfig) initMinioBucketName() {
	bucketName, err := p.BaseParams.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *indexNodeConfig) initKnowhereSimdType() {
	simdType := p.BaseParams.LoadWithDefault("knowhere.simdType", "auto")
	p.SimdType = simdType
	log.Debug("initialize the knowhere simd type", zap.String("simd_type", p.SimdType))
}

///////////////////////////////////////////////////////////////////////////////
// --- grpc ---
type grpcConfig struct {
	BaseParamTable

	once     sync.Once
	Domain   string
	IP       string
	Port     int
	Listener net.Listener
}

func (p *grpcConfig) init(domain string) {
	p.BaseParamTable.Init()
	p.Domain = domain

	p.LoadFromEnv()
	p.LoadFromArgs()
	p.initPort()
	p.initListener()
}

// LoadFromEnv is used to initialize configuration items from env.
func (p *grpcConfig) LoadFromEnv() {
	p.IP = ipv4.LocalIP()
}

// LoadFromArgs is used to initialize configuration items from args.
func (p *grpcConfig) LoadFromArgs() {

}

func (p *grpcConfig) initPort() {
	p.Port = p.ParseInt(p.Domain + ".port")

	if p.Domain == typeutil.ProxyRole || p.Domain == typeutil.DataNodeRole || p.Domain == typeutil.IndexNodeRole || p.Domain == typeutil.QueryNodeRole {
		if !CheckPortAvailable(p.Port) {
			p.Port = GetAvailablePort()
			log.Warn("get available port when init", zap.String("Domain", p.Domain), zap.Int("Port", p.Port))
		}
	}
}

// GetAddress return grpc address
func (p *grpcConfig) GetAddress() string {
	return p.IP + ":" + strconv.Itoa(p.Port)
}

func (p *grpcConfig) initListener() {
	if p.Domain == typeutil.DataNodeRole {
		listener, err := net.Listen("tcp", p.GetAddress())
		if err != nil {
			panic(err)
		}
		p.Listener = listener
	}
}

type GrpcServerConfig struct {
	grpcConfig

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

// InitOnce initialize grpc server config once
func (p *GrpcServerConfig) InitOnce(domain string) {
	p.once.Do(func() {
		p.init(domain)
	})
}

func (p *GrpcServerConfig) init(domain string) {
	p.grpcConfig.init(domain)

	p.initServerMaxSendSize()
	p.initServerMaxRecvSize()
}

func (p *GrpcServerConfig) initServerMaxSendSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.serverMaxSendSize")
	if err != nil {
		p.ServerMaxSendSize = DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.serverMaxSendSize, set to default",
			zap.String("rol", p.Domain), zap.String("grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		p.ServerMaxSendSize = DefaultServerMaxSendSize
	} else {
		p.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxSendSize", p.ServerMaxSendSize))
}

func (p *GrpcServerConfig) initServerMaxRecvSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.serverMaxRecvSize")
	if err != nil {
		p.ServerMaxRecvSize = DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.serverMaxRecvSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		p.ServerMaxRecvSize = DefaultServerMaxRecvSize
	} else {
		p.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxRecvSize", p.ServerMaxRecvSize))
}

type GrpcClientConfig struct {
	grpcConfig

	ClientMaxSendSize int
	ClientMaxRecvSize int
}

// InitOnce initialize grpc client config once
func (p *GrpcClientConfig) InitOnce(domain string) {
	p.once.Do(func() {
		p.init(domain)
	})
}

func (p *GrpcClientConfig) init(domain string) {
	p.grpcConfig.init(domain)

	p.initClientMaxSendSize()
	p.initClientMaxRecvSize()
}

func (p *GrpcClientConfig) initClientMaxSendSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.clientMaxSendSize")
	if err != nil {
		p.ClientMaxSendSize = DefaultClientMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.clientMaxSendSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.clientMaxSendSize", valueStr),
			zap.Error(err))

		p.ClientMaxSendSize = DefaultClientMaxSendSize
	} else {
		p.ClientMaxSendSize = value
	}

	log.Debug("initClientMaxSendSize",
		zap.String("role", p.Domain), zap.Int("grpc.clientMaxSendSize", p.ClientMaxSendSize))
}

func (p *GrpcClientConfig) initClientMaxRecvSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.clientMaxRecvSize")
	if err != nil {
		p.ClientMaxRecvSize = DefaultClientMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.clientMaxRecvSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.clientMaxRecvSize", valueStr),
			zap.Error(err))

		p.ClientMaxRecvSize = DefaultClientMaxRecvSize
	} else {
		p.ClientMaxRecvSize = value
	}

	log.Debug("initClientMaxRecvSize",
		zap.String("role", p.Domain), zap.Int("grpc.clientMaxRecvSize", p.ClientMaxRecvSize))
}

// CheckPortAvailable check if a port is available to be listened on
func CheckPortAvailable(port int) bool {
	addr := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", addr)
	if listener != nil {
		listener.Close()
	}
	return err == nil
}

// GetAvailablePort return an available port that can be listened on
func GetAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
