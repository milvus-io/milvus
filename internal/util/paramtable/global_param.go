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
)

// GlobalParamTable is a derived struct of BaseParamTable.
// It is used to quickly and easily access global system configuration.
type GlobalParamTable struct {
	once       sync.Once
	BaseParams BaseParamTable

	EtcdCfg       etcdConfig
	MinioCfg      minioConfig
	PulsarCfg     pulsarConfig
	RocksdbCfg    rocksdbConfig
	CommonCfg     commonConfig
	KnowhereCfg   knowhereConfig
	MsgChannelCfg msgChannelConfig

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
	//p.BaseParams.RoleName = "rootcoord"

	p.EtcdCfg.init(&p.BaseParams)
	p.MinioCfg.init(&p.BaseParams)
	p.PulsarCfg.init(&p.BaseParams)
	p.RocksdbCfg.init(&p.BaseParams)
	p.CommonCfg.init(&p.BaseParams)
	p.KnowhereCfg.init(&p.BaseParams)
	p.MsgChannelCfg.init(&p.BaseParams)

	p.RootCoordCfg.init(&p.BaseParams)
	p.ProxyCfg.init(&p.BaseParams)
	p.QueryCoordCfg.init(&p.BaseParams)
	p.QueryNodeCfg.init(&p.BaseParams)
	p.DataCoordCfg.init(&p.BaseParams)
	p.DataNodeCfg.init(&p.BaseParams)
	p.IndexCoordCfg.init(&p.BaseParams)
	p.IndexNodeCfg.init(&p.BaseParams)
}

func (p *GlobalParamTable) InitProxyAlias(alias string) {
	p.ProxyCfg.initAlias(alias)
}

func (p *GlobalParamTable) InitDataNodeAlias(alias string) {
	//p.DataNodeCfg.initAlias(alias)
}

func (p *GlobalParamTable) InitIndexNodeAlias(alias string) {
	//p.IndexNodeCfg.initAlias(alias)
}

func (p *GlobalParamTable) InitProxyNodeAlias(alias string) {
	//p.QueryNodeCfg.initAlias(alias)
}

///////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type etcdConfig struct {
	BaseParams *BaseParamTable
}

func (p *etcdConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
type minioConfig struct {
	BaseParams *BaseParamTable
}

func (p *minioConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type pulsarConfig struct {
	BaseParams *BaseParamTable

	Address string

	MaxMessageSize int
}

func (p *pulsarConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initPulsarAddress()
	p.initMaxMessageSize()
}

func (p *pulsarConfig) initPulsarAddress() {
	addr, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.Address = addr
}

func (p *pulsarConfig) initMaxMessageSize() {
	maxMessageSizeStr, err := p.BaseParams.Load("pulsar.maxMessageSize")
	if err != nil {
		p.MaxMessageSize = SuggestPulsarMaxMessageSize
	} else {
		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
		if err != nil {
			p.MaxMessageSize = SuggestPulsarMaxMessageSize
		} else {
			p.MaxMessageSize = maxMessageSize
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// --- rocksdb ---
type rocksdbConfig struct {
	BaseParams *BaseParamTable

	RocksmqPath string
}

func (p *rocksdbConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

func (p *rocksdbConfig) initRocksmqPath() {
	path, err := p.BaseParams.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	BaseParams *BaseParamTable

	DefaultPartitionName string
	DefaultIndexName     string
}

func (p *commonConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
	p.initDefaultPartitionName()
	p.initDefaultIndexName()
}

func (p *commonConfig) initDefaultPartitionName() {
	name := p.BaseParams.LoadWithDefault("common.defaultPartitionName", "_default")
	p.DefaultPartitionName = name
}

func (p *commonConfig) initDefaultIndexName() {
	name := p.BaseParams.LoadWithDefault("common.defaultIndexName", "_default_idx")
	p.DefaultIndexName = name
}

///////////////////////////////////////////////////////////////////////////////
// --- knowhere ---
type knowhereConfig struct {
	BaseParams *BaseParamTable
}

func (p *knowhereConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- msgChannel ---
type msgChannelConfig struct {
	BaseParams *BaseParamTable

	ClusterPrefix          string
	RootCoordTimeTick      string
	RootCoordStatistics    string
	RootCoordDml           string
	RootCoordDelta         string
	Search                 string
	SearchResult           string
	ProxyTimeTick          string
	QueryTimeTick          string
	QueryNodeStats         string
	Cmd                    string
	DataCoordInsertChannel string
	DataCoordStatistic     string
	DataCoordTimeTick      string
	DataCoordSegmentInfo   string

	SkipQueryChannelRecovery string

	RootCoordSubNamePrefix string
	ProxySubNamePrefix     string
	QueryNodeSubNamePrefix string
	DataNodeSubNamePrefix  string
	DataCoordSubNamePrefix string
}

func (p *msgChannelConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	// must init cluster prefix first
	p.initClusterPrefix()
	p.initRootCoordTimeTick()
	p.initRootCoordStatistics()
	p.initRootCoordDml()
	p.initRootCoordDelta()
	p.initMsgChannelSearch()
	p.initMsgChannelSearchResult()
	p.initProxyTimeTick()
	p.initQueryTimeTick()
	p.initQueryNodeStats()
	p.initMsgChannelCmd()
	p.initDataCoordInsertChannel()
	p.initDataCoordStatistic()
	p.initDataCoordTimeTick()
	p.initDataCoordSegmentInfo()

	p.initRootCoordSubNamePrefix()
	p.initProxySubNamePrefix()
	p.initQueryNodeSubNamePrefix()
	p.initDataNodeSubNamePrefix()
	p.initDataCoordSubNamePrefix()
}

func (p *msgChannelConfig) initClusterPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterPrefix = config
}

func (p *msgChannelConfig) initChanNamePrefix(cfg string) string {
	value, err := p.BaseParams.Load(cfg)
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterPrefix, value}
	return strings.Join(s, "-")
}

// --- msgChannel.chanNamePrefix ---
func (p *msgChannelConfig) initRootCoordTimeTick() {
	p.RootCoordTimeTick = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordTimeTick")
}

func (p *msgChannelConfig) initRootCoordStatistics() {
	p.RootCoordStatistics = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordStatistics")
}

func (p *msgChannelConfig) initRootCoordDml() {
	p.RootCoordDml = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordDml")
}

func (p *msgChannelConfig) initRootCoordDelta() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.rootCoordDelta")
}

func (p *msgChannelConfig) initMsgChannelSearch() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.search")
}

func (p *msgChannelConfig) initMsgChannelSearchResult() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.searchResult")
}

func (p *msgChannelConfig) initProxyTimeTick() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.proxyTimeTick")
}

func (p *msgChannelConfig) initQueryTimeTick() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.queryTimeTick")
}

func (p *msgChannelConfig) initQueryNodeStats() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.queryNodeStats")
}

func (p *msgChannelConfig) initMsgChannelCmd() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.cmd")
}

func (p *msgChannelConfig) initDataCoordInsertChannel() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordInsertChannel")
}

func (p *msgChannelConfig) initDataCoordStatistic() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordStatistic")
}

func (p *msgChannelConfig) initDataCoordTimeTick() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordTimeTick")
}

func (p *msgChannelConfig) initDataCoordSegmentInfo() {
	p.RootCoordDelta = p.initChanNamePrefix("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
}

// --- msgChannel.subNamePrefix ---
func (p *msgChannelConfig) initRootCoordSubNamePrefix() {
	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
}

func (p *msgChannelConfig) initProxySubNamePrefix() {
	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.proxySubNamePrefix")
}

func (p *msgChannelConfig) initQueryNodeSubNamePrefix() {
	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
}

func (p *msgChannelConfig) initDataNodeSubNamePrefix() {
	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
}

func (p *msgChannelConfig) initDataCoordSubNamePrefix() {
	p.RootCoordSubNamePrefix = p.initChanNamePrefix("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
}

///////////////////////////////////////////////////////////////////////////////
// --- rootcoord ---
type rootCoordConfig struct {
	BaseParams *BaseParamTable

	Port    int
	Address string
	//PulsarAddress string
	//EtcdEndpoints []string
	//MetaRootPath  string
	//KvRootPath    string

	//ClusterChannelPrefix string
	//MsgChannelSubName    string
	//TimeTickChannel      string
	//StatisticsChannel    string
	//DmlChannelName       string
	//DeltaChannelName     string

	DmlChannelNum   int64
	MaxPartitionNum int64
	//DefaultPartitionName        string
	//DefaultIndexName            string
	MinSegmentSizeToEnableIndex int64

	Timeout          int
	TimeTickInterval int

	CreatedTime time.Time
	UpdatedTime time.Time
}

// InitOnce initialize once
//func (p *rootCoordConfig) InitOnce() {
//	once.Do(func() {
//		p.Init()
//	})
//}

func (p *rootCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	//p.initPulsarAddress()
	//p.initEtcdEndpoints()
	//p.initMetaRootPath()
	//p.initKvRootPath()

	// Has to init global msgchannel prefix before other channel names
	//p.initClusterMsgChannelPrefix()
	//p.initMsgChannelSubName()
	//p.initTimeTickChannel()
	//p.initStatisticsChannelName()
	//p.initDmlChannelName()
	//p.initDeltaChannelName()

	p.initDmlChannelNum()
	p.initMaxPartitionNum()
	p.initMinSegmentSizeToEnableIndex()
	//p.initDefaultPartitionName()
	//p.initDefaultIndexName()

	p.initTimeout()
	p.initTimeTickInterval()

	//p.initRoleName()
}

//func (p *rootCoordConfig) initPulsarAddress() {
//	addr, err := p.BaseParams.Load("_PulsarAddress")
//	if err != nil {
//		panic(err)
//	}
//	p.PulsarAddress = addr
//}

//func (p *rootCoordConfig) initEtcdEndpoints() {
//	endpoints, err := p.BaseParams.Load("_EtcdEndpoints")
//	if err != nil {
//		panic(err)
//	}
//	p.EtcdEndpoints = strings.Split(endpoints, ",")
//}

//func (p *rootCoordConfig) initMetaRootPath() {
//	rootPath, err := p.BaseParams.Load("etcd.rootPath")
//	if err != nil {
//		panic(err)
//	}
//	subPath, err := p.BaseParams.Load("etcd.metaSubPath")
//	if err != nil {
//		panic(err)
//	}
//	p.MetaRootPath = rootPath + "/" + subPath
//}

//func (p *rootCoordConfig) initKvRootPath() {
//	rootPath, err := p.BaseParams.Load("etcd.rootPath")
//	if err != nil {
//		panic(err)
//	}
//	subPath, err := p.BaseParams.Load("etcd.kvSubPath")
//	if err != nil {
//		panic(err)
//	}
//	p.KvRootPath = rootPath + "/" + subPath
//}

//func (p *rootCoordConfig) initClusterMsgChannelPrefix() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
//	if err != nil {
//		panic(err)
//	}
//	p.ClusterChannelPrefix = config
//}
//
//func (p *rootCoordConfig) initMsgChannelSubName() {
//	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config}
//	p.MsgChannelSubName = strings.Join(s, "-")
//}
//
//func (p *rootCoordConfig) initTimeTickChannel() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordTimeTick")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config}
//	p.TimeTickChannel = strings.Join(s, "-")
//}
//
//func (p *rootCoordConfig) initStatisticsChannelName() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordStatistics")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config}
//	p.StatisticsChannel = strings.Join(s, "-")
//}
//
//func (p *rootCoordConfig) initDmlChannelName() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDml")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config}
//	p.DmlChannelName = strings.Join(s, "-")
//}
//
//func (p *rootCoordConfig) initDeltaChannelName() {
//	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDelta")
//	if err != nil {
//		config = "rootcoord-delta"
//	}
//	s := []string{p.ClusterChannelPrefix, config}
//	p.DeltaChannelName = strings.Join(s, "-")
//}

func (p *rootCoordConfig) initDmlChannelNum() {
	p.DmlChannelNum = p.BaseParams.ParseInt64WithDefault("rootCoord.dmlChannelNum", 256)
}

func (p *rootCoordConfig) initMaxPartitionNum() {
	p.MaxPartitionNum = p.BaseParams.ParseInt64WithDefault("rootCoord.maxPartitionNum", 4096)
}

func (p *rootCoordConfig) initMinSegmentSizeToEnableIndex() {
	p.MinSegmentSizeToEnableIndex = p.BaseParams.ParseInt64WithDefault("rootCoord.minSegmentSizeToEnableIndex", 1024)
}

//func (p *rootCoordConfig) initDefaultPartitionName() {
//	name := p.LoadWithDefault("common.defaultPartitionName", "_default")
//	p.DefaultPartitionName = name
//}
//
//func (p *rootCoordConfig) initDefaultIndexName() {
//	name := p.LoadWithDefault("common.defaultIndexName", "_default_idx")
//	p.DefaultIndexName = name
//}

func (p *rootCoordConfig) initTimeout() {
	p.Timeout = p.BaseParams.ParseIntWithDefault("rootCoord.timeout", 3600)
}

func (p *rootCoordConfig) initTimeTickInterval() {
	p.TimeTickInterval = p.BaseParams.ParseIntWithDefault("rootCoord.timeTickInterval", 200)
}

//func (p *rootCoordConfig) initRoleName() {
//	p.RoleName = "rootcoord"
//}

///////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type proxyConfig struct {
	BaseParams *BaseParamTable

	// NetworkPort & IP are not used
	IP      string
	Port    int
	Address string

	Alias string

	//EtcdEndpoints []string
	//MetaRootPath  string
	//PulsarAddress string

	RocksmqPath string // not used in Proxy

	ProxyID                  UniqueID
	TimeTickInterval         time.Duration
	MsgStreamTimeTickBufSize int64
	MaxNameLength            int64
	MaxFieldNum              int64
	MaxShardNum              int32
	MaxDimension             int64
	//DefaultPartitionName     string
	//DefaultIndexName         string
	BufFlagExpireTime      time.Duration
	BufFlagCleanupInterval time.Duration

	// --- Channels ---
	//ClusterChannelPrefix      string
	//ProxyTimeTickChannelNames []string
	//ProxySubName              string

	// required from query coord
	SearchResultChannelNames   []string
	RetrieveResultChannelNames []string

	MaxTaskNum int64

	PulsarMaxMessageSize int

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *proxyConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp

	//p.initEtcdEndpoints()
	//p.initMetaRootPath()
	//p.initPulsarAddress()
	//p.initRocksmqPath()
	p.initTimeTickInterval()
	// Has to init global msgchannel prefix before other channel names
	//p.initClusterMsgChannelPrefix()
	//p.initProxySubName()
	//p.initProxyTimeTickChannelNames()
	p.initMsgStreamTimeTickBufSize()
	p.initMaxNameLength()
	p.initMaxFieldNum()
	p.initMaxShardNum()
	p.initMaxDimension()
	//p.initDefaultPartitionName()
	//p.initDefaultIndexName()

	//p.initPulsarMaxMessageSize()

	p.initMaxTaskNum()
	p.initBufFlagExpireTime()
	p.initBufFlagCleanupInterval()
}

func (p *proxyConfig) initAlias(alias string) {
	p.Alias = alias
}

//func (p *proxyConfig) initPulsarAddress() {
//	ret, err := p.Load("_PulsarAddress")
//	if err != nil {
//		panic(err)
//	}
//	p.PulsarAddress = ret
//}

//func (p *proxyConfig) initRocksmqPath() {
//	path, err := p.Load("_RocksmqPath")
//	if err != nil {
//		panic(err)
//	}
//	p.RocksmqPath = path
//}

func (p *proxyConfig) initTimeTickInterval() {
	interval := p.BaseParams.ParseIntWithDefault("proxy.timeTickInterval", 200)
	p.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

//func (p *proxyConfig) initClusterMsgChannelPrefix() {
//	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
//	if err != nil {
//		panic(err)
//	}
//	p.ClusterChannelPrefix = config
//}

//func (p *proxyConfig) initProxySubName() {
//	config, err := p.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config, strconv.FormatInt(p.ProxyID, 10)}
//	p.ProxySubName = strings.Join(s, "-")
//}

//func (p *proxyConfig) initProxyTimeTickChannelNames() {
//	config, err := p.Load("msgChannel.chanNamePrefix.proxyTimeTick")
//	if err != nil {
//		panic(err)
//	}
//	s := []string{p.ClusterChannelPrefix, config, "0"}
//	prefix := strings.Join(s, "-")
//	p.ProxyTimeTickChannelNames = []string{prefix}
//}

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

//func (p *proxyConfig) initDefaultPartitionName() {
//	name := p.LoadWithDefault("common.defaultPartitionName", "_default")
//	p.DefaultPartitionName = name
//}

//func (p *proxyConfig) initDefaultIndexName() {
//	name := p.LoadWithDefault("common.defaultIndexName", "_default_idx")
//	p.DefaultIndexName = name
//}

//func (p *proxyConfig) initPulsarMaxMessageSize() {
//	maxMessageSizeStr, err := p.Load("pulsar.maxMessageSize")
//	if err != nil {
//		p.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
//	} else {
//		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
//		if err != nil {
//			p.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
//		} else {
//			p.PulsarMaxMessageSize = maxMessageSize
//		}
//	}
//}

//func (p *proxyConfig) initRoleName() {
//	p.RoleName = "proxy"
//}

//func (p *proxyConfig) initEtcdEndpoints() {
//	endpoints, err := p.Load("_EtcdEndpoints")
//	if err != nil {
//		panic(err)
//	}
//	p.EtcdEndpoints = strings.Split(endpoints, ",")
//}

//func (p *proxyConfig) initMetaRootPath() {
//	rootPath, err := p.Load("etcd.rootPath")
//	if err != nil {
//		panic(err)
//	}
//	subPath, err := p.Load("etcd.metaSubPath")
//	if err != nil {
//		panic(err)
//	}
//	p.MetaRootPath = path.Join(rootPath, subPath)
//}

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

///////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *queryCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *queryNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *dataCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *dataNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- indexcoord ---
type indexCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *indexCoordConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- indexnode ---
type indexNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *indexNodeConfig) init(bp *BaseParamTable) {
	p.BaseParams = bp
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
