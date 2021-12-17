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
)

// GlobalParamTable is a derived struct of BaseParamTable.
// It is used to quickly and easily access global system configuration.
type GlobalParamTable struct {
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
	once.Do(func() {
		p.Init()
	})
}

func (p *GlobalParamTable) Init() {
	p.BaseParams.Init()
	p.BaseParams.RoleName = "rootcoord"

	p.EtcdCfg.Init(&p.BaseParams)
	p.MinioCfg.Init(&p.BaseParams)
	p.PulsarCfg.Init(&p.BaseParams)
	p.RocksdbCfg.Init(&p.BaseParams)
	p.CommonCfg.Init(&p.BaseParams)
	p.KnowhereCfg.Init(&p.BaseParams)
	p.MsgChannelCfg.Init(&p.BaseParams)

	p.RootCoordCfg.Init(&p.BaseParams)
	p.ProxyCfg.Init(&p.BaseParams)
	p.QueryCoordCfg.Init(&p.BaseParams)
	p.QueryNodeCfg.Init(&p.BaseParams)
	p.DataCoordCfg.Init(&p.BaseParams)
	p.DataNodeCfg.Init(&p.BaseParams)
	p.IndexCoordCfg.Init(&p.BaseParams)
	p.IndexNodeCfg.Init(&p.BaseParams)
}

///////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type etcdConfig struct {
	BaseParams *BaseParamTable
}

func (p *etcdConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
type minioConfig struct {
	BaseParams *BaseParamTable
}

func (p *minioConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type pulsarConfig struct {
	BaseParams *BaseParamTable

	Address string
}

func (p *pulsarConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp

	p.initPulsarAddress()
}

func (p *pulsarConfig) initPulsarAddress() {
	addr, err := p.BaseParams.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.Address = addr
}

///////////////////////////////////////////////////////////////////////////////
// --- rocksdb ---
type rocksdbConfig struct {
	BaseParams *BaseParamTable
}

func (p *rocksdbConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	BaseParams *BaseParamTable

	DefaultPartitionName string
	DefaultIndexName     string
}

func (p *commonConfig) Init(bp *BaseParamTable) {
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

func (p *knowhereConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// --- msgChannel ---
type msgChannelConfig struct {
	BaseParams *BaseParamTable

	ClusterChannelPrefix string
	MsgChannelSubName    string
	TimeTickChannel      string
	StatisticsChannel    string
	DmlChannelName       string
	DeltaChannelName     string
}

func (p *msgChannelConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initMsgChannelSubName()
	p.initTimeTickChannel()
	p.initStatisticsChannelName()
	p.initDmlChannelName()
	p.initDeltaChannelName()
}

func (p *msgChannelConfig) initClusterMsgChannelPrefix() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *msgChannelConfig) initMsgChannelSubName() {
	config, err := p.BaseParams.Load("msgChannel.subNamePrefix.rootCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *msgChannelConfig) initTimeTickChannel() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannel = strings.Join(s, "-")
}

func (p *msgChannelConfig) initStatisticsChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordStatistics")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatisticsChannel = strings.Join(s, "-")
}

func (p *msgChannelConfig) initDmlChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDml")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DmlChannelName = strings.Join(s, "-")
}

func (p *msgChannelConfig) initDeltaChannelName() {
	config, err := p.BaseParams.Load("msgChannel.chanNamePrefix.rootCoordDelta")
	if err != nil {
		config = "rootcoord-delta"
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DeltaChannelName = strings.Join(s, "-")
}

///////////////////////////////////////////////////////////////////////////////
// -- rootcoord ---
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

// Init initialize param table
func (p *rootCoordConfig) Init(bp *BaseParamTable) {
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
// -- proxy ---
type proxyConfig struct {
	BaseParams *BaseParamTable
}

func (p *proxyConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- querycoord ---
type queryCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *queryCoordConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- querynode ---
type queryNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *queryNodeConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- datacoord ---
type dataCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *dataCoordConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- datanode ---
type dataNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *dataNodeConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- indexcoord ---
type indexCoordConfig struct {
	BaseParams *BaseParamTable
}

func (p *indexCoordConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- indexnode ---
type indexNodeConfig struct {
	BaseParams *BaseParamTable
}

func (p *indexNodeConfig) Init(bp *BaseParamTable) {
	p.BaseParams = bp
}

///////////////////////////////////////////////////////////////////////////////
// -- grpc ---
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
