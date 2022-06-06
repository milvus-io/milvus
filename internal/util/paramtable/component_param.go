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
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	// DefaultRetentionDuration defines the default duration for retention which is 5 days in seconds.
	DefaultRetentionDuration = 3600 * 24 * 5

	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize = 16
	DefaultGracefulTime   = 5000 //ms
)

// ComponentParam is used to quickly and easily access all components' configurations.
type ComponentParam struct {
	ServiceParam
	once sync.Once

	CommonCfg commonConfig

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
func (p *ComponentParam) InitOnce() {
	p.once.Do(func() {
		p.Init()
	})
}

// Init initialize the global param table
func (p *ComponentParam) Init() {
	p.ServiceParam.Init()

	p.CommonCfg.init(&p.BaseTable)

	p.RootCoordCfg.init(&p.BaseTable)
	p.ProxyCfg.init(&p.BaseTable)
	p.QueryCoordCfg.init(&p.BaseTable)
	p.QueryNodeCfg.init(&p.BaseTable)
	p.DataCoordCfg.init(&p.BaseTable)
	p.DataNodeCfg.init(&p.BaseTable)
	p.IndexCoordCfg.init(&p.BaseTable)
	p.IndexNodeCfg.init(&p.BaseTable)
}

// SetLogConfig set log config with given role
func (p *ComponentParam) SetLogConfig(role string) {
	p.BaseTable.RoleName = role
	p.BaseTable.SetLogConfig()
}

func (p *ComponentParam) RocksmqEnable() bool {
	return p.RocksmqCfg.Path != ""
}

func (p *ComponentParam) PulsarEnable() bool {
	return p.PulsarCfg.Address != ""
}

func (p *ComponentParam) KafkaEnable() bool {
	return p.KafkaCfg.Address != ""
}

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	Base *BaseTable

	ClusterPrefix string

	ProxySubName string

	RootCoordTimeTick   string
	RootCoordStatistics string
	RootCoordDml        string
	RootCoordDelta      string
	RootCoordSubName    string

	QueryCoordSearch       string
	QueryCoordSearchResult string
	QueryCoordTimeTick     string
	QueryNodeStats         string
	QueryNodeSubName       string

	DataCoordStatistic   string
	DataCoordTimeTick    string
	DataCoordSegmentInfo string
	DataCoordSubName     string
	DataNodeSubName      string

	DefaultPartitionName string
	DefaultIndexName     string
	RetentionDuration    int64
	EntityExpirationTTL  time.Duration

	IndexSliceSize int64
	GracefulTime   int64

	StorageType string
	SimdType    string

	AuthorizationEnabled bool
}

func (p *commonConfig) init(base *BaseTable) {
	p.Base = base

	// must init cluster prefix first
	p.initClusterPrefix()
	p.initProxySubName()

	p.initRootCoordTimeTick()
	p.initRootCoordStatistics()
	p.initRootCoordDml()
	p.initRootCoordDelta()
	p.initRootCoordSubName()

	p.initQueryCoordSearch()
	p.initQueryCoordSearchResult()
	p.initQueryCoordTimeTick()
	p.initQueryNodeStats()
	p.initQueryNodeSubName()

	p.initDataCoordStatistic()
	p.initDataCoordTimeTick()
	p.initDataCoordSegmentInfo()
	p.initDataCoordSubName()
	p.initDataNodeSubName()

	p.initDefaultPartitionName()
	p.initDefaultIndexName()
	p.initRetentionDuration()
	p.initEntityExpiration()

	p.initSimdType()
	p.initIndexSliceSize()
	p.initGracefulTime()
	p.initStorageType()

	p.initEnableAuthorization()
}

func (p *commonConfig) initClusterPrefix() {
	keys := []string{
		"common.chanNamePrefix.cluster",
		"msgChannel.chanNamePrefix.cluster",
	}
	str, err := p.Base.Load2(keys)
	if err != nil {
		panic(err)
	}
	p.ClusterPrefix = str
}

func (p *commonConfig) initChanNamePrefix(keys []string) string {
	value, err := p.Base.Load2(keys)
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterPrefix, value}
	return strings.Join(s, "-")
}

// --- proxy ---
func (p *commonConfig) initProxySubName() {
	keys := []string{
		"common.subNamePrefix.proxySubNamePrefix",
		"msgChannel.subNamePrefix.proxySubNamePrefix",
	}
	p.ProxySubName = p.initChanNamePrefix(keys)
}

// --- rootcoord ---
// Deprecate
func (p *commonConfig) initRootCoordTimeTick() {
	keys := []string{
		"common.chanNamePrefix.rootCoordTimeTick",
		"msgChannel.chanNamePrefix.rootCoordTimeTick",
	}
	p.RootCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordStatistics() {
	keys := []string{
		"common.chanNamePrefix.rootCoordStatistics",
		"msgChannel.chanNamePrefix.rootCoordStatistics",
	}
	p.RootCoordStatistics = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDml() {
	keys := []string{
		"common.chanNamePrefix.rootCoordDml",
		"msgChannel.chanNamePrefix.rootCoordDml",
	}
	p.RootCoordDml = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDelta() {
	keys := []string{
		"common.chanNamePrefix.rootCoordDelta",
		"msgChannel.chanNamePrefix.rootCoordDelta",
	}
	p.RootCoordDelta = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordSubName() {
	keys := []string{
		"common.subNamePrefix.rootCoordSubNamePrefix",
		"msgChannel.subNamePrefix.rootCoordSubNamePrefix",
	}
	p.RootCoordSubName = p.initChanNamePrefix(keys)
}

// --- querycoord ---
func (p *commonConfig) initQueryCoordSearch() {
	keys := []string{
		"common.chanNamePrefix.search",
		"msgChannel.chanNamePrefix.search",
	}
	p.QueryCoordSearch = p.initChanNamePrefix(keys)
}

// Deprecated, search result use grpc instead of a result channel
func (p *commonConfig) initQueryCoordSearchResult() {
	keys := []string{
		"common.chanNamePrefix.searchResult",
		"msgChannel.chanNamePrefix.searchResult",
	}
	p.QueryCoordSearchResult = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initQueryCoordTimeTick() {
	keys := []string{
		"common.chanNamePrefix.queryTimeTick",
		"msgChannel.chanNamePrefix.queryTimeTick",
	}
	p.QueryCoordTimeTick = p.initChanNamePrefix(keys)
}

// --- querynode ---
func (p *commonConfig) initQueryNodeStats() {
	keys := []string{
		"common.chanNamePrefix.queryNodeStats",
		"msgChannel.chanNamePrefix.queryNodeStats",
	}
	p.QueryNodeStats = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initQueryNodeSubName() {
	keys := []string{
		"common.subNamePrefix.queryNodeSubNamePrefix",
		"msgChannel.subNamePrefix.queryNodeSubNamePrefix",
	}
	p.QueryNodeSubName = p.initChanNamePrefix(keys)
}

// --- datacoord ---
func (p *commonConfig) initDataCoordStatistic() {
	keys := []string{
		"common.chanNamePrefix.dataCoordStatistic",
		"msgChannel.chanNamePrefix.dataCoordStatistic",
	}
	p.DataCoordStatistic = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initDataCoordTimeTick() {
	keys := []string{
		"common.chanNamePrefix.dataCoordTimeTick",
		"msgChannel.chanNamePrefix.dataCoordTimeTick",
	}
	p.DataCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSegmentInfo() {
	keys := []string{
		"common.chanNamePrefix.dataCoordSegmentInfo",
		"msgChannel.chanNamePrefix.dataCoordSegmentInfo",
	}
	p.DataCoordSegmentInfo = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSubName() {
	keys := []string{
		"common.subNamePrefix.dataCoordSubNamePrefix",
		"msgChannel.subNamePrefix.dataCoordSubNamePrefix",
	}
	p.DataCoordSubName = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataNodeSubName() {
	keys := []string{
		"common.subNamePrefix.dataNodeSubNamePrefix",
		"msgChannel.subNamePrefix.dataNodeSubNamePrefix",
	}
	p.DataNodeSubName = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDefaultPartitionName() {
	p.DefaultPartitionName = p.Base.LoadWithDefault("common.defaultPartitionName", "_default")
}

func (p *commonConfig) initDefaultIndexName() {
	p.DefaultIndexName = p.Base.LoadWithDefault("common.defaultIndexName", "_default_idx")
}

func (p *commonConfig) initRetentionDuration() {
	p.RetentionDuration = p.Base.ParseInt64WithDefault("common.retentionDuration", DefaultRetentionDuration)
}

func (p *commonConfig) initEntityExpiration() {
	ttl := p.Base.ParseInt64WithDefault("common.entityExpiration", -1)
	if ttl < 0 {
		p.EntityExpirationTTL = -1
		return
	}

	// make sure ttl is larger than retention duration to ensure time travel works
	if ttl > p.RetentionDuration {
		p.EntityExpirationTTL = time.Duration(ttl) * time.Second
	} else {
		p.EntityExpirationTTL = time.Duration(p.RetentionDuration) * time.Second
	}
}

func (p *commonConfig) initSimdType() {
	keys := []string{
		"common.simdType",
		"knowhere.simdType",
	}
	p.SimdType = p.Base.LoadWithDefault2(keys, "auto")
}

func (p *commonConfig) initIndexSliceSize() {
	p.IndexSliceSize = p.Base.ParseInt64WithDefault("common.indexSliceSize", DefaultIndexSliceSize)
}

func (p *commonConfig) initGracefulTime() {
	p.GracefulTime = p.Base.ParseInt64WithDefault("common.gracefulTime", DefaultGracefulTime)
}

func (p *commonConfig) initStorageType() {
	p.StorageType = p.Base.LoadWithDefault("common.storageType", "minio")
}

func (p *commonConfig) initEnableAuthorization() {
	p.AuthorizationEnabled = p.Base.ParseBool("common.security.authorizationEnabled", false)
}

///////////////////////////////////////////////////////////////////////////////
// --- rootcoord ---
type rootCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int

	DmlChannelNum                   int64
	MaxPartitionNum                 int64
	MinSegmentSizeToEnableIndex     int64
	ImportTaskExpiration            float64
	ImportTaskRetention             float64
	ImportSegmentStateCheckInterval float64
	ImportSegmentStateWaitLimit     float64
	ImportIndexCheckInterval        float64
	ImportIndexWaitLimit            float64

	// --- ETCD Path ---
	ImportTaskSubPath string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *rootCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.DmlChannelNum = p.Base.ParseInt64WithDefault("rootCoord.dmlChannelNum", 256)
	p.MaxPartitionNum = p.Base.ParseInt64WithDefault("rootCoord.maxPartitionNum", 4096)
	p.MinSegmentSizeToEnableIndex = p.Base.ParseInt64WithDefault("rootCoord.minSegmentSizeToEnableIndex", 1024)
	p.ImportTaskExpiration = p.Base.ParseFloatWithDefault("rootCoord.importTaskExpiration", 3600)
	p.ImportTaskRetention = p.Base.ParseFloatWithDefault("rootCoord.importTaskRetention", 3600*24)
	p.ImportSegmentStateCheckInterval = p.Base.ParseFloatWithDefault("rootCoord.importSegmentStateCheckInterval", 10)
	p.ImportSegmentStateWaitLimit = p.Base.ParseFloatWithDefault("rootCoord.importSegmentStateWaitLimit", 60)
	p.ImportIndexCheckInterval = p.Base.ParseFloatWithDefault("rootCoord.importIndexCheckInterval", 60*5)
	p.ImportIndexWaitLimit = p.Base.ParseFloatWithDefault("rootCoord.importIndexWaitLimit", 60*20)
	p.ImportTaskSubPath = "importtask"
}

///////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type proxyConfig struct {
	Base *BaseTable

	// NetworkPort & IP are not used
	NetworkPort    int
	IP             string
	NetworkAddress string

	Alias string

	NodeID                   atomic.Value
	TimeTickInterval         time.Duration
	MsgStreamTimeTickBufSize int64
	MaxNameLength            int64
	MaxUsernameLength        int64
	MinPasswordLength        int64
	MaxPasswordLength        int64
	MaxFieldNum              int64
	MaxShardNum              int32
	MaxDimension             int64
	GinLogging               bool

	// required from QueryCoord
	SearchResultChannelNames   []string
	RetrieveResultChannelNames []string

	MaxTaskNum int64

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *proxyConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initTimeTickInterval()

	p.initMsgStreamTimeTickBufSize()
	p.initMaxNameLength()
	p.initMinPasswordLength()
	p.initMaxUsernameLength()
	p.initMaxPasswordLength()
	p.initMaxFieldNum()
	p.initMaxShardNum()
	p.initMaxDimension()

	p.initMaxTaskNum()
	p.initGinLogging()
}

// InitAlias initialize Alias member.
func (p *proxyConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *proxyConfig) initTimeTickInterval() {
	interval := p.Base.ParseIntWithDefault("proxy.timeTickInterval", 200)
	p.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

func (p *proxyConfig) initMsgStreamTimeTickBufSize() {
	p.MsgStreamTimeTickBufSize = p.Base.ParseInt64WithDefault("proxy.msgStream.timeTick.bufSize", 512)
}

func (p *proxyConfig) initMaxNameLength() {
	str := p.Base.LoadWithDefault("proxy.maxNameLength", "255")
	maxNameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxNameLength = maxNameLength
}

func (p *proxyConfig) initMaxUsernameLength() {
	str := p.Base.LoadWithDefault("proxy.maxUsernameLength", "32")
	maxUsernameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxUsernameLength = maxUsernameLength
}

func (p *proxyConfig) initMinPasswordLength() {
	str := p.Base.LoadWithDefault("proxy.minPasswordLength", "6")
	minPasswordLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MinPasswordLength = minPasswordLength
}

func (p *proxyConfig) initMaxPasswordLength() {
	str := p.Base.LoadWithDefault("proxy.maxPasswordLength", "256")
	maxPasswordLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxPasswordLength = maxPasswordLength
}

func (p *proxyConfig) initMaxShardNum() {
	str := p.Base.LoadWithDefault("proxy.maxShardNum", "256")
	maxShardNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxShardNum = int32(maxShardNum)
}

func (p *proxyConfig) initMaxFieldNum() {
	str := p.Base.LoadWithDefault("proxy.maxFieldNum", "64")
	maxFieldNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxFieldNum = maxFieldNum
}

func (p *proxyConfig) initMaxDimension() {
	str := p.Base.LoadWithDefault("proxy.maxDimension", "32768")
	maxDimension, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxDimension = maxDimension
}

func (p *proxyConfig) initMaxTaskNum() {
	p.MaxTaskNum = p.Base.ParseInt64WithDefault("proxy.maxTaskNum", 1024)
}

func (p *proxyConfig) initGinLogging() {
	// Gin logging is on by default.
	p.GinLogging = p.Base.ParseBool("proxy.ginLogging", true)
}

func (p *proxyConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *proxyConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int
	NodeID  atomic.Value

	CreatedTime time.Time
	UpdatedTime time.Time

	//---- Handoff ---
	AutoHandoff bool

	//---- Balance ---
	AutoBalance                         bool
	OverloadedMemoryThresholdPercentage float64
	BalanceIntervalSeconds              int64
	MemoryUsageMaxDifferencePercentage  float64
}

func (p *queryCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	//---- Handoff ---
	p.initAutoHandoff()

	//---- Balance ---
	p.initAutoBalance()
	p.initOverloadedMemoryThresholdPercentage()
	p.initBalanceIntervalSeconds()
	p.initMemoryUsageMaxDifferencePercentage()
}

func (p *queryCoordConfig) initAutoHandoff() {
	handoff, err := p.Base.Load("queryCoord.autoHandoff")
	if err != nil {
		panic(err)
	}
	p.AutoHandoff, err = strconv.ParseBool(handoff)
	if err != nil {
		panic(err)
	}
}

func (p *queryCoordConfig) initAutoBalance() {
	balanceStr := p.Base.LoadWithDefault("queryCoord.autoBalance", "false")
	autoBalance, err := strconv.ParseBool(balanceStr)
	if err != nil {
		panic(err)
	}
	p.AutoBalance = autoBalance
}

func (p *queryCoordConfig) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.Base.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}

func (p *queryCoordConfig) initBalanceIntervalSeconds() {
	balanceInterval := p.Base.LoadWithDefault("queryCoord.balanceIntervalSeconds", "60")
	interval, err := strconv.ParseInt(balanceInterval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.BalanceIntervalSeconds = interval
}

func (p *queryCoordConfig) initMemoryUsageMaxDifferencePercentage() {
	maxDiff := p.Base.LoadWithDefault("queryCoord.memoryUsageMaxDifferencePercentage", "30")
	diffPercentage, err := strconv.ParseInt(maxDiff, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MemoryUsageMaxDifferencePercentage = float64(diffPercentage) / 100
}

func (p *queryCoordConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *queryCoordConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	Base *BaseTable

	Alias         string
	QueryNodeIP   string
	QueryNodePort int64
	NodeID        atomic.Value
	// TODO: remove cacheSize
	CacheSize int64 // deprecated

	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32

	// stats
	StatsPublishInterval int

	SliceIndex int

	// segcore
	ChunkRows        int64
	SmallIndexNlist  int64
	SmallIndexNProbe int64

	CreatedTime time.Time
	UpdatedTime time.Time

	// memory limit
	OverloadedMemoryThresholdPercentage float64

	// cache limit
	CacheEnabled     bool
	CacheMemoryLimit int64

	GroupEnabled         bool
	MaxReceiveChanSize   int32
	MaxUnsolvedQueueSize int32
	MaxGroupNQ           int64
	TopKMergeRatio       float64
}

func (p *queryNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initCacheSize()

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()

	p.initStatsPublishInterval()

	p.initSmallIndexParams()

	p.initOverloadedMemoryThresholdPercentage()

	p.initCacheMemoryLimit()
	p.initCacheEnabled()

	p.initGroupEnabled()
	p.initMaxReceiveChanSize()
	p.initMaxUnsolvedQueueSize()
	p.initMaxGroupNQ()
	p.initTopKMergeRatio()
}

// InitAlias initializes an alias for the QueryNode role.
func (p *queryNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *queryNodeConfig) initCacheSize() {
	defer log.Debug("init cacheSize", zap.Any("cacheSize (GB)", p.CacheSize))

	const defaultCacheSize = 32 // GB
	p.CacheSize = defaultCacheSize

	var err error
	cacheSize := os.Getenv("CACHE_SIZE")
	if cacheSize == "" {
		cacheSize, err = p.Base.Load("queryNode.cacheSize")
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

// advanced params
// stats
func (p *queryNodeConfig) initStatsPublishInterval() {
	p.StatsPublishInterval = p.Base.ParseIntWithDefault("queryNode.stats.publishInterval", 1000)
}

// dataSync:
func (p *queryNodeConfig) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.Base.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *queryNodeConfig) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.Base.ParseInt32WithDefault("queryNode.dataSync.flowGraph.maxParallelism", 1024)
}

func (p *queryNodeConfig) initSmallIndexParams() {
	p.ChunkRows = p.Base.ParseInt64WithDefault("queryNode.segcore.chunkRows", 32768)
	if p.ChunkRows < 1024 {
		log.Warn("chunk rows can not be less than 1024, force set to 1024", zap.Any("current", p.ChunkRows))
		p.ChunkRows = 1024
	}

	// default NList is the first nlist
	var defaultNList int64
	for i := int64(0); i < p.ChunkRows; i++ {
		if math.Pow(2.0, float64(i)) > math.Sqrt(float64(p.ChunkRows)) {
			defaultNList = int64(math.Pow(2, float64(i)))
			break
		}
	}

	p.SmallIndexNlist = p.Base.ParseInt64WithDefault("queryNode.segcore.smallIndex.nlist", defaultNList)
	if p.SmallIndexNlist > p.ChunkRows/8 {
		log.Warn("small index nlist must smaller than chunkRows/8, force set to", zap.Any("nliit", p.ChunkRows/8))
		p.SmallIndexNlist = p.ChunkRows / 8
	}

	defaultNprobe := p.SmallIndexNlist / 16
	p.SmallIndexNProbe = p.Base.ParseInt64WithDefault("queryNode.segcore.smallIndex.nprobe", defaultNprobe)
	if p.SmallIndexNProbe > p.SmallIndexNlist {
		log.Warn("small index nprobe must smaller than nlist, force set to", zap.Any("nprobe", p.SmallIndexNlist))
		p.SmallIndexNProbe = p.SmallIndexNlist
	}
}

func (p *queryNodeConfig) initOverloadedMemoryThresholdPercentage() {
	overloadedMemoryThresholdPercentage := p.Base.LoadWithDefault("queryCoord.overloadedMemoryThresholdPercentage", "90")
	thresholdPercentage, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.OverloadedMemoryThresholdPercentage = float64(thresholdPercentage) / 100
}

func (p *queryNodeConfig) initCacheMemoryLimit() {
	overloadedMemoryThresholdPercentage := p.Base.LoadWithDefault("queryNode.cache.memoryLimit", "2147483648")
	cacheMemoryLimit, err := strconv.ParseInt(overloadedMemoryThresholdPercentage, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CacheMemoryLimit = cacheMemoryLimit
}

func (p *queryNodeConfig) initCacheEnabled() {
	var err error
	cacheEnabled := p.Base.LoadWithDefault("queryNode.cache.enabled", "true")
	p.CacheEnabled, err = strconv.ParseBool(cacheEnabled)
	if err != nil {
		panic(err)
	}
}

func (p *queryNodeConfig) initGroupEnabled() {
	p.GroupEnabled = p.Base.ParseBool("queryNode.grouping.enabled", true)
}

func (p *queryNodeConfig) initMaxReceiveChanSize() {
	p.MaxReceiveChanSize = p.Base.ParseInt32WithDefault("queryNode.grouping.receiveChanSize", 10240)
}

func (p *queryNodeConfig) initMaxUnsolvedQueueSize() {
	p.MaxUnsolvedQueueSize = p.Base.ParseInt32WithDefault("queryNode.grouping.unsolvedQueueSize", 10240)
}

func (p *queryNodeConfig) initMaxGroupNQ() {
	p.MaxGroupNQ = p.Base.ParseInt64WithDefault("queryNode.grouping.maxNQ", 1000)
}

func (p *queryNodeConfig) initTopKMergeRatio() {
	p.TopKMergeRatio = p.Base.ParseFloatWithDefault("queryNode.grouping.topKMergeRatio", 10.0)
}

func (p *queryNodeConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *queryNodeConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	Base *BaseTable

	NodeID atomic.Value

	IP      string
	Port    int
	Address string

	// --- ETCD ---
	ChannelWatchSubPath string

	// --- SEGMENTS ---
	SegmentMaxSize          float64
	SegmentSealProportion   float64
	SegAssignmentExpiration int64
	SegmentMaxLifetime      time.Duration

	CreatedTime time.Time
	UpdatedTime time.Time

	EnableCompaction        bool
	EnableAutoCompaction    atomic.Value
	EnableGarbageCollection bool

	// Garbage Collection
	GCInterval         time.Duration
	GCMissingTolerance time.Duration
	GCDropTolerance    time.Duration
}

func (p *dataCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.initChannelWatchPrefix()

	p.initSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()
	p.initSegmentMaxLifetime()

	p.initEnableCompaction()
	p.initEnableAutoCompaction()

	p.initEnableGarbageCollection()
	p.initGCInterval()
	p.initGCMissingTolerance()
	p.initGCDropTolerance()
}

func (p *dataCoordConfig) initSegmentMaxSize() {
	p.SegmentMaxSize = p.Base.ParseFloatWithDefault("dataCoord.segment.maxSize", 512.0)
}

func (p *dataCoordConfig) initSegmentSealProportion() {
	p.SegmentSealProportion = p.Base.ParseFloatWithDefault("dataCoord.segment.sealProportion", 0.25)
}

func (p *dataCoordConfig) initSegAssignmentExpiration() {
	p.SegAssignmentExpiration = p.Base.ParseInt64WithDefault("dataCoord.segment.assignmentExpiration", 2000)
}

func (p *dataCoordConfig) initSegmentMaxLifetime() {
	p.SegmentMaxLifetime = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.segment.maxLife", 24*60*60)) * time.Second
}

func (p *dataCoordConfig) initChannelWatchPrefix() {
	// WARN: this value should not be put to milvus.yaml. It's a default value for channel watch path.
	// This will be removed after we reconstruct our config module.
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataCoordConfig) initEnableCompaction() {
	p.EnableCompaction = p.Base.ParseBool("dataCoord.enableCompaction", false)
}

// -- GC --
func (p *dataCoordConfig) initEnableGarbageCollection() {
	p.EnableGarbageCollection = p.Base.ParseBool("dataCoord.enableGarbageCollection", false)
}

func (p *dataCoordConfig) initGCInterval() {
	p.GCInterval = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.gc.interval", 60*60)) * time.Second
}

func (p *dataCoordConfig) initGCMissingTolerance() {
	p.GCMissingTolerance = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.gc.missingTolerance", 24*60*60)) * time.Second
}

func (p *dataCoordConfig) initGCDropTolerance() {
	p.GCDropTolerance = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.gc.dropTolerance", 24*60*60)) * time.Second
}

func (p *dataCoordConfig) initEnableAutoCompaction() {
	p.EnableAutoCompaction.Store(p.Base.ParseBool("dataCoord.compaction.enableAutoCompaction", false))
}

func (p *dataCoordConfig) SetEnableAutoCompaction(enable bool) {
	p.EnableAutoCompaction.Store(enable)
}

func (p *dataCoordConfig) GetEnableAutoCompaction() bool {
	enable := p.EnableAutoCompaction.Load()
	if enable != nil {
		return enable.(bool)
	}
	return false
}

func (p *dataCoordConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *dataCoordConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	Base *BaseTable

	// ID of the current node
	//NodeID atomic.Value
	NodeID atomic.Value
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

	// etcd
	ChannelWatchSubPath string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initInsertBinlogRootPath()
	p.initStatsBinlogRootPath()
	p.initDeleteBinlogRootPath()

	p.initChannelWatchPath()
}

// InitAlias init this DataNode alias
func (p *dataNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *dataNodeConfig) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.Base.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *dataNodeConfig) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.Base.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxParallelism", 1024)
}

func (p *dataNodeConfig) initFlushInsertBufferSize() {
	p.FlushInsertBufferSize = p.Base.ParseInt64("_DATANODE_INSERTBUFSIZE")
}

func (p *dataNodeConfig) initInsertBinlogRootPath() {
	// GOOSE TODO: rootPath change to TenentID
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.InsertBinlogRootPath = path.Join(rootPath, "insert_log")
}

func (p *dataNodeConfig) initStatsBinlogRootPath() {
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.StatsBinlogRootPath = path.Join(rootPath, "stats_log")
}

func (p *dataNodeConfig) initDeleteBinlogRootPath() {
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.DeleteBinlogRootPath = path.Join(rootPath, "delta_log")
}

func (p *dataNodeConfig) initChannelWatchPath() {
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataNodeConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *dataNodeConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// --- indexcoord ---
type indexCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int

	IndexStorageRootPath string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *indexCoordConfig) init(base *BaseTable) {
	p.Base = base

	p.initIndexStorageRootPath()
}

// initIndexStorageRootPath initializes the root path of index files.
func (p *indexCoordConfig) initIndexStorageRootPath() {
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.IndexStorageRootPath = path.Join(rootPath, "index_files")
}

///////////////////////////////////////////////////////////////////////////////
// --- indexnode ---
type indexNodeConfig struct {
	Base *BaseTable

	IP      string
	Address string
	Port    int

	NodeID atomic.Value

	Alias string

	IndexStorageRootPath string

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *indexNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initIndexStorageRootPath()
}

// InitAlias initializes an alias for the IndexNode role.
func (p *indexNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *indexNodeConfig) initIndexStorageRootPath() {
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.IndexStorageRootPath = path.Join(rootPath, "index_files")
}

func (p *indexNodeConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *indexNodeConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}
