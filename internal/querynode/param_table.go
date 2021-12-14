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

package querynode

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable is used to record configuration items.
type ParamTable struct {
	paramtable.BaseTable

	PulsarAddress string
	RocksmqPath   string
	EtcdEndpoints []string
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
	skipQueryChannelRecovery bool

	// memory limit
	OverloadedMemoryThresholdPercentage float64
}

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// InitAlias initializes an alias for the QueryNode role.
func (p *ParamTable) InitAlias(alias string) {
	p.Alias = alias
}

// InitOnce is used to initialize configuration items, and it will only be called once.
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

// Init is used to initialize configuration items.
func (p *ParamTable) Init() {
	p.BaseTable.Init()

	p.initCacheSize()

	p.initMinioEndPoint()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSLStr()
	p.initMinioBucketName()

	p.initPulsarAddress()
	p.initRocksmqPath()
	p.initEtcdEndpoints()
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

	p.initRoleName()

	p.initSkipQueryChannelRecovery()
	p.initOverloadedMemoryThresholdPercentage()
}

func (p *ParamTable) initCacheSize() {
	defer log.Debug("init cacheSize", zap.Any("cacheSize (GB)", p.CacheSize))

	const defaultCacheSize = 32 // GB
	p.CacheSize = defaultCacheSize

	var err error
	cacheSize := os.Getenv("CACHE_SIZE")
	if cacheSize == "" {
		cacheSize, err = p.Load("queryNode.cacheSize")
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

func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

// advanced params
// stats
func (p *ParamTable) initStatsPublishInterval() {
	p.StatsPublishInterval = p.ParseIntWithDefault("queryNode.stats.publishInterval", 1000)
}

// dataSync:
func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxParallelism", 1024)
}

// msgStream
func (p *ParamTable) initSearchReceiveBufSize() {
	p.SearchReceiveBufSize = p.ParseInt64WithDefault("queryNode.msgStream.search.recvBufSize", 512)
}

func (p *ParamTable) initSearchPulsarBufSize() {
	p.SearchPulsarBufSize = p.ParseInt64WithDefault("queryNode.msgStream.search.pulsarBufSize", 512)
}

func (p *ParamTable) initSearchResultReceiveBufSize() {
	p.SearchResultReceiveBufSize = p.ParseInt64WithDefault("queryNode.msgStream.searchResult.recvBufSize", 64)
}

// ------------------------  channel names
func (p *ParamTable) initClusterMsgChannelPrefix() {
	name, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = name
}

func (p *ParamTable) initQueryTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		log.Warn(err.Error())
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.QueryTimeTickChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initMsgChannelSubName() {
	namePrefix, err := p.Load("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
	if err != nil {
		log.Warn(err.Error())
	}

	s := []string{p.ClusterChannelPrefix, namePrefix, strconv.FormatInt(p.QueryNodeID, 10)}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *ParamTable) initStatsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatsChannelName = strings.Join(s, "-")
}

// ETCD configs
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

func (p *ParamTable) initGracefulTime() {
	p.GracefulTime = p.ParseInt64("queryNode.gracefulTime")
	log.Debug("query node init gracefulTime", zap.Any("gracefulTime", p.GracefulTime))
}

func (p *ParamTable) initSegcoreChunkRows() {
	p.ChunkRows = p.ParseInt64WithDefault("queryNode.segcore.chunkRows", 32768)
}

func (p *ParamTable) initKnowhereSimdType() {
	simdType := p.LoadWithDefault("knowhere.simdType", "auto")
	p.SimdType = simdType
	log.Debug("initialize the knowhere simd type", zap.String("simd_type", p.SimdType))
}

func (p *ParamTable) initRoleName() {
	p.RoleName = "querynode"
}

func (p *ParamTable) initSkipQueryChannelRecovery() {
	p.skipQueryChannelRecovery = p.ParseBool("msgChannel.skipQueryChannelRecovery", false)
}

func (p *ParamTable) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}
