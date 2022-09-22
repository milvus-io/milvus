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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/disk"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	// DefaultRetentionDuration defines the default duration for retention which is 5 days in seconds.
	DefaultRetentionDuration = 3600 * 24

	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize = 16
	DefaultGracefulTime   = 5000 //ms
)

// ComponentParam is used to quickly and easily access all components' configurations.
type ComponentParam struct {
	ServiceParam
	once sync.Once

	CommonCfg   commonConfig
	QuotaConfig quotaConfig

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
	p.QuotaConfig.init(&p.BaseTable)

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

	ClusterName string
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

	p.initClusterName()
}

func (p *commonConfig) initClusterPrefix() {
	keys := []string{
		"msgChannel.chanNamePrefix.cluster",
		"common.chanNamePrefix.cluster",
	}
	str, err := p.Base.LoadWithPriority(keys)
	if err != nil {
		panic(err)
	}
	p.ClusterPrefix = str
}

func (p *commonConfig) initChanNamePrefix(keys []string) string {
	value, err := p.Base.LoadWithPriority(keys)
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterPrefix, value}
	return strings.Join(s, "-")
}

// --- proxy ---
func (p *commonConfig) initProxySubName() {
	keys := []string{
		"msgChannel.subNamePrefix.proxySubNamePrefix",
		"common.subNamePrefix.proxySubNamePrefix",
	}
	p.ProxySubName = p.initChanNamePrefix(keys)
}

// --- rootcoord ---
// Deprecate
func (p *commonConfig) initRootCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordTimeTick",
		"common.chanNamePrefix.rootCoordTimeTick",
	}
	p.RootCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordStatistics() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordStatistics",
		"common.chanNamePrefix.rootCoordStatistics",
	}
	p.RootCoordStatistics = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDml() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordDml",
		"common.chanNamePrefix.rootCoordDml",
	}
	p.RootCoordDml = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordDelta() {
	keys := []string{
		"msgChannel.chanNamePrefix.rootCoordDelta",
		"common.chanNamePrefix.rootCoordDelta",
	}
	p.RootCoordDelta = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initRootCoordSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.rootCoordSubNamePrefix",
		"common.subNamePrefix.rootCoordSubNamePrefix",
	}
	p.RootCoordSubName = p.initChanNamePrefix(keys)
}

// --- querycoord ---
func (p *commonConfig) initQueryCoordSearch() {
	keys := []string{
		"msgChannel.chanNamePrefix.search",
		"common.chanNamePrefix.search",
	}
	p.QueryCoordSearch = p.initChanNamePrefix(keys)
}

// Deprecated, search result use grpc instead of a result channel
func (p *commonConfig) initQueryCoordSearchResult() {
	keys := []string{
		"msgChannel.chanNamePrefix.searchResult",
		"common.chanNamePrefix.searchResult",
	}
	p.QueryCoordSearchResult = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initQueryCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.queryTimeTick",
		"common.chanNamePrefix.queryTimeTick",
	}
	p.QueryCoordTimeTick = p.initChanNamePrefix(keys)
}

// --- querynode ---
func (p *commonConfig) initQueryNodeSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.queryNodeSubNamePrefix",
		"common.subNamePrefix.queryNodeSubNamePrefix",
	}
	p.QueryNodeSubName = p.initChanNamePrefix(keys)
}

// --- datacoord ---
func (p *commonConfig) initDataCoordStatistic() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordStatistic",
		"common.chanNamePrefix.dataCoordStatistic",
	}
	p.DataCoordStatistic = p.initChanNamePrefix(keys)
}

// Deprecate
func (p *commonConfig) initDataCoordTimeTick() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordTimeTick",
		"common.chanNamePrefix.dataCoordTimeTick",
	}
	p.DataCoordTimeTick = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSegmentInfo() {
	keys := []string{
		"msgChannel.chanNamePrefix.dataCoordSegmentInfo",
		"common.chanNamePrefix.dataCoordSegmentInfo",
	}
	p.DataCoordSegmentInfo = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataCoordSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.dataCoordSubNamePrefix",
		"common.subNamePrefix.dataCoordSubNamePrefix",
	}
	p.DataCoordSubName = p.initChanNamePrefix(keys)
}

func (p *commonConfig) initDataNodeSubName() {
	keys := []string{
		"msgChannel.subNamePrefix.dataNodeSubNamePrefix",
		"common.subNamePrefix.dataNodeSubNamePrefix",
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

func (p *commonConfig) initClusterName() {
	p.ClusterName = p.Base.LoadWithDefault("common.cluster.name", "")
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
	p.ImportTaskExpiration = p.Base.ParseFloatWithDefault("rootCoord.importTaskExpiration", 15*60)
	p.ImportTaskRetention = p.Base.ParseFloatWithDefault("rootCoord.importTaskRetention", 24*60*60)
	p.ImportSegmentStateCheckInterval = p.Base.ParseFloatWithDefault("rootCoord.importSegmentStateCheckInterval", 10)
	p.ImportSegmentStateWaitLimit = p.Base.ParseFloatWithDefault("rootCoord.importSegmentStateWaitLimit", 60)
	p.ImportIndexCheckInterval = p.Base.ParseFloatWithDefault("rootCoord.importIndexCheckInterval", 10)
	p.ImportIndexWaitLimit = p.Base.ParseFloatWithDefault("rootCoord.importIndexWaitLimit", 10*60)
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
	MaxUserNum               int
	MaxRoleNum               int

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
	p.initMaxUserNum()
	p.initMaxRoleNum()
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

func (p *proxyConfig) initMaxUserNum() {
	str := p.Base.LoadWithDefault("proxy.maxUserNum", "100")
	maxUserNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxUserNum = int(maxUserNum)
}

func (p *proxyConfig) initMaxRoleNum() {
	str := p.Base.LoadWithDefault("proxy.maxRoleNum", "10")
	maxRoleNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxRoleNum = int(maxRoleNum)
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

	//---- Task ---
	RetryNum      int32
	RetryInterval int64

	//---- Handoff ---
	AutoHandoff bool

	//---- Balance ---
	AutoBalance                         bool
	OverloadedMemoryThresholdPercentage float64
	BalanceIntervalSeconds              int64
	MemoryUsageMaxDifferencePercentage  float64
	CheckInterval                       time.Duration
	ChannelTaskTimeout                  time.Duration
	SegmentTaskTimeout                  time.Duration
	DistPullInterval                    time.Duration
	LoadTimeoutSeconds                  time.Duration
	CheckHandoffInterval                time.Duration
}

func (p *queryCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))

	//---- Task ---
	p.initTaskRetryNum()
	p.initTaskRetryInterval()

	//---- Handoff ---
	p.initAutoHandoff()

	//---- Balance ---
	p.initAutoBalance()
	p.initOverloadedMemoryThresholdPercentage()
	p.initBalanceIntervalSeconds()
	p.initMemoryUsageMaxDifferencePercentage()
	p.initCheckInterval()
	p.initChannelTaskTimeout()
	p.initSegmentTaskTimeout()
	p.initDistPullInterval()
	p.initLoadTimeoutSeconds()
	p.initCheckHandoffInterval()
}

func (p *queryCoordConfig) initTaskRetryNum() {
	p.RetryNum = p.Base.ParseInt32WithDefault("queryCoord.task.retrynum", 5)
}

func (p *queryCoordConfig) initTaskRetryInterval() {
	p.RetryInterval = p.Base.ParseInt64WithDefault("queryCoord.task.retryinterval", int64(10*time.Second))
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

func (p *queryCoordConfig) initCheckInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkInterval", "1000")
	checkInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckInterval = time.Duration(checkInterval) * time.Millisecond
}

func (p *queryCoordConfig) initChannelTaskTimeout() {
	timeout := p.Base.LoadWithDefault("queryCoord.channelTaskTimeout", "60000")
	taskTimeout, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		panic(err)
	}
	p.ChannelTaskTimeout = time.Duration(taskTimeout) * time.Millisecond
}

func (p *queryCoordConfig) initSegmentTaskTimeout() {
	timeout := p.Base.LoadWithDefault("queryCoord.segmentTaskTimeout", "120000")
	taskTimeout, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		panic(err)
	}
	p.SegmentTaskTimeout = time.Duration(taskTimeout) * time.Millisecond
}

func (p *queryCoordConfig) initDistPullInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.distPullInterval", "500")
	pullInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.DistPullInterval = time.Duration(pullInterval) * time.Millisecond
}

func (p *queryCoordConfig) initLoadTimeoutSeconds() {
	timeout := p.Base.LoadWithDefault("queryCoord.loadTimeoutSeconds", "600")
	loadTimeout, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		panic(err)
	}
	p.LoadTimeoutSeconds = time.Duration(loadTimeout) * time.Second
}

func (p *queryCoordConfig) initCheckHandoffInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkHandoffInterval", "5000")
	checkHandoffInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckHandoffInterval = time.Duration(checkHandoffInterval) * time.Millisecond
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
	LoadMemoryUsageFactor               float64
	OverloadedMemoryThresholdPercentage float64

	// enable disk
	EnableDisk             bool
	DiskCapacityLimit      int64
	MaxDiskUsagePercentage float64

	// cache limit
	CacheEnabled     bool
	CacheMemoryLimit int64

	GroupEnabled         bool
	MaxReceiveChanSize   int32
	MaxUnsolvedQueueSize int32
	MaxReadConcurrency   int32
	MaxGroupNQ           int64
	TopKMergeRatio       float64
	CPURatio             float64
}

func (p *queryNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()

	p.initStatsPublishInterval()

	p.initSmallIndexParams()

	p.initLoadMemoryUsageFactor()
	p.initOverloadedMemoryThresholdPercentage()

	p.initCacheMemoryLimit()
	p.initCacheEnabled()

	p.initGroupEnabled()
	p.initMaxReceiveChanSize()
	p.initMaxReadConcurrency()
	p.initMaxUnsolvedQueueSize()
	p.initMaxGroupNQ()
	p.initTopKMergeRatio()
	p.initCPURatio()
	p.initEnableDisk()
	p.initDiskCapacity()
	p.initMaxDiskUsagePercentage()
}

// InitAlias initializes an alias for the QueryNode role.
func (p *queryNodeConfig) InitAlias(alias string) {
	p.Alias = alias
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
	p.ChunkRows = p.Base.ParseInt64WithDefault("queryNode.segcore.chunkRows", 1024)
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

func (p *queryNodeConfig) initLoadMemoryUsageFactor() {
	loadMemoryUsageFactor := p.Base.LoadWithDefault("queryNode.loadMemoryUsageFactor", "3")
	factor, err := strconv.ParseFloat(loadMemoryUsageFactor, 64)
	if err != nil {
		panic(err)
	}
	p.LoadMemoryUsageFactor = factor
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
	p.MaxReceiveChanSize = p.Base.ParseInt32WithDefault("queryNode.scheduler.receiveChanSize", 10240)
}

func (p *queryNodeConfig) initMaxUnsolvedQueueSize() {
	p.MaxUnsolvedQueueSize = p.Base.ParseInt32WithDefault("queryNode.scheduler.unsolvedQueueSize", 10240)
}

func (p *queryNodeConfig) initCPURatio() {
	p.CPURatio = p.Base.ParseFloatWithDefault("queryNode.scheduler.cpuRatio", 10.0)
}

func (p *queryNodeConfig) initMaxReadConcurrency() {
	readConcurrencyRatio := p.Base.ParseFloatWithDefault("queryNode.scheduler.maxReadConcurrentRatio", 2.0)
	cpuNum := int32(runtime.GOMAXPROCS(0))
	p.MaxReadConcurrency = int32(float64(cpuNum) * readConcurrencyRatio)
	if p.MaxReadConcurrency < 1 {
		p.MaxReadConcurrency = 1 // MaxReadConcurrency must >= 1
	} else if p.MaxReadConcurrency > cpuNum*100 {
		p.MaxReadConcurrency = cpuNum * 100 // MaxReadConcurrency must <= 100*cpuNum
	}
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

func (p *queryNodeConfig) initEnableDisk() {
	var err error
	enableDisk := p.Base.LoadWithDefault("queryNode.enableDisk", "false")
	p.EnableDisk, err = strconv.ParseBool(enableDisk)
	if err != nil {
		panic(err)
	}
}

func (p *queryNodeConfig) initMaxDiskUsagePercentage() {
	maxDiskUsagePercentageStr := p.Base.LoadWithDefault("queryNode.maxDiskUsagePercentage", "95")
	maxDiskUsagePercentage, err := strconv.ParseInt(maxDiskUsagePercentageStr, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxDiskUsagePercentage = float64(maxDiskUsagePercentage) / 100
}

func (p *queryNodeConfig) initDiskCapacity() {
	diskSizeStr := os.Getenv("LOCAL_STORAGE_SIZE")
	if len(diskSizeStr) == 0 {
		diskUsage, err := disk.Usage("/")
		if err != nil {
			panic(err)
		}
		p.DiskCapacityLimit = int64(diskUsage.Total)

		return
	}

	diskSize, err := strconv.ParseInt(diskSizeStr, 10, 64)
	if err != nil {
		panic(err)
	}
	p.DiskCapacityLimit = diskSize * 1024 * 1024 * 1024
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
	SegmentMaxSize                 float64
	SegmentSealProportion          float64
	SegAssignmentExpiration        int64
	SegmentMaxLifetime             time.Duration
	SegmentMaxIdleTime             time.Duration
	SegmentMinSizeFromIdleToSealed float64

	CreatedTime time.Time
	UpdatedTime time.Time

	// compaction
	EnableCompaction     bool
	EnableAutoCompaction atomic.Value

	MinSegmentToMerge                 int
	MaxSegmentToMerge                 int
	SegmentSmallProportion            float64
	CompactionTimeoutInSeconds        int32
	CompactionCheckIntervalInSeconds  int64
	SingleCompactionRatioThreshold    float32
	SingleCompactionDeltaLogMaxSize   int64
	SingleCompactionExpiredLogMaxSize int64
	SingleCompactionBinlogMaxNum      int64
	GlobalCompactionInterval          time.Duration

	// Garbage Collection
	EnableGarbageCollection bool
	GCInterval              time.Duration
	GCMissingTolerance      time.Duration
	GCDropTolerance         time.Duration
}

func (p *dataCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.initChannelWatchPrefix()

	p.initSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()
	p.initSegmentMaxLifetime()
	p.initSegmentMaxIdleTime()
	p.initSegmentMinSizeFromIdleToSealed()

	p.initEnableCompaction()
	p.initEnableAutoCompaction()

	p.initCompactionMinSegment()
	p.initCompactionMaxSegment()
	p.initSegmentSmallProportion()
	p.initCompactionTimeoutInSeconds()
	p.initCompactionCheckIntervalInSeconds()
	p.initSingleCompactionRatioThreshold()
	p.initSingleCompactionDeltaLogMaxSize()
	p.initSingleCompactionExpiredLogMaxSize()
	p.initSingleCompactionBinlogMaxNum()
	p.initGlobalCompactionInterval()

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

func (p *dataCoordConfig) initSegmentMaxIdleTime() {
	p.SegmentMaxIdleTime = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.segment.maxIdleTime", 60*60)) * time.Second
	log.Info("init segment max idle time", zap.String("value", p.SegmentMaxIdleTime.String()))
}

func (p *dataCoordConfig) initSegmentMinSizeFromIdleToSealed() {
	p.SegmentMinSizeFromIdleToSealed = p.Base.ParseFloatWithDefault("dataCoord.segment.minSizeFromIdleToSealed", 16.0)
	log.Info("init segment min size from idle to sealed", zap.Float64("value", p.SegmentMinSizeFromIdleToSealed))
}

func (p *dataCoordConfig) initChannelWatchPrefix() {
	// WARN: this value should not be put to milvus.yaml. It's a default value for channel watch path.
	// This will be removed after we reconstruct our config module.
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataCoordConfig) initEnableCompaction() {
	p.EnableCompaction = p.Base.ParseBool("dataCoord.enableCompaction", false)
}

func (p *dataCoordConfig) initEnableAutoCompaction() {
	p.EnableAutoCompaction.Store(p.Base.ParseBool("dataCoord.compaction.enableAutoCompaction", false))
}

func (p *dataCoordConfig) initCompactionMinSegment() {
	p.MinSegmentToMerge = p.Base.ParseIntWithDefault("dataCoord.compaction.min.segment", 4)
}

func (p *dataCoordConfig) initCompactionMaxSegment() {
	p.MaxSegmentToMerge = p.Base.ParseIntWithDefault("dataCoord.compaction.max.segment", 30)
}

func (p *dataCoordConfig) initSegmentSmallProportion() {
	p.SegmentSmallProportion = p.Base.ParseFloatWithDefault("dataCoord.segment.smallProportion", 0.5)
}

// compaction execution timeout
func (p *dataCoordConfig) initCompactionTimeoutInSeconds() {
	p.CompactionTimeoutInSeconds = p.Base.ParseInt32WithDefault("dataCoord.compaction.timeout", 60*3)
}

func (p *dataCoordConfig) initCompactionCheckIntervalInSeconds() {
	p.CompactionCheckIntervalInSeconds = p.Base.ParseInt64WithDefault("dataCoord.compaction.check.interval", 10)
}

// if total delete entities is large than a ratio of total entities, trigger single compaction.
func (p *dataCoordConfig) initSingleCompactionRatioThreshold() {
	p.SingleCompactionRatioThreshold = float32(p.Base.ParseFloatWithDefault("dataCoord.compaction.single.ratio.threshold", 0.2))
}

// if total delta file size > SingleCompactionDeltaLogMaxSize, trigger single compaction
func (p *dataCoordConfig) initSingleCompactionDeltaLogMaxSize() {
	p.SingleCompactionDeltaLogMaxSize = p.Base.ParseInt64WithDefault("dataCoord.compaction.single.deltalog.maxsize", 2*1024*1024)
}

// if total expired file size > SingleCompactionExpiredLogMaxSize, trigger single compaction
func (p *dataCoordConfig) initSingleCompactionExpiredLogMaxSize() {
	p.SingleCompactionExpiredLogMaxSize = p.Base.ParseInt64WithDefault("dataCoord.compaction.single.expiredlog.maxsize", 10*1024*1024)
}

// if total binlog number > SingleCompactionBinlogMaxNum, trigger single compaction to ensure binlog number per segment is limited
func (p *dataCoordConfig) initSingleCompactionBinlogMaxNum() {
	p.SingleCompactionBinlogMaxNum = p.Base.ParseInt64WithDefault("dataCoord.compaction.single.binlog.maxnum", 1000)
}

// interval we check and trigger global compaction
func (p *dataCoordConfig) initGlobalCompactionInterval() {
	p.GlobalCompactionInterval = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.compaction.global.interval", int64(60*time.Second)))
}

// -- GC --
func (p *dataCoordConfig) initEnableGarbageCollection() {
	p.EnableGarbageCollection = p.Base.ParseBool("dataCoord.enableGarbageCollection", true)
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

	Alias string // Different datanode in one machine

	// etcd
	ChannelWatchSubPath string

	// io concurrency to fetch stats logs
	IOConcurrency int

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initIOConcurrency()

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
	bufferSize := p.Base.LoadWithDefault2([]string{"DATA_NODE_IBUFSIZE", "datanode.flush.insertBufSize"}, "0")
	bs, err := strconv.ParseInt(bufferSize, 10, 64)
	if err != nil {
		panic(err)
	}
	p.FlushInsertBufferSize = bs
}

func (p *dataNodeConfig) initChannelWatchPath() {
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataNodeConfig) initIOConcurrency() {
	p.IOConcurrency = p.Base.ParseIntWithDefault("dataNode.dataSync.ioConcurrency", 10)
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

	MinSegmentNumRowsToEnableIndex int64

	GCInterval time.Duration

	CreatedTime time.Time
	UpdatedTime time.Time
}

func (p *indexCoordConfig) init(base *BaseTable) {
	p.Base = base

	p.initGCInterval()
	p.initMinSegmentNumRowsToEnableIndex()
}

func (p *indexCoordConfig) initMinSegmentNumRowsToEnableIndex() {
	p.MinSegmentNumRowsToEnableIndex = p.Base.ParseInt64WithDefault("indexCoord.minSegmentNumRowsToEnableIndex", 1024)
}

func (p *indexCoordConfig) initGCInterval() {
	p.GCInterval = time.Duration(p.Base.ParseInt64WithDefault("indexCoord.gc.interval", 60*10)) * time.Second
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

	BuildParallel int

	CreatedTime time.Time
	UpdatedTime time.Time

	// enable disk
	EnableDisk             bool
	DiskCapacityLimit      int64
	MaxDiskUsagePercentage float64
}

func (p *indexNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initBuildParallel()
	p.initEnableDisk()
	p.initDiskCapacity()
	p.initMaxDiskUsagePercentage()
}

// InitAlias initializes an alias for the IndexNode role.
func (p *indexNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *indexNodeConfig) initBuildParallel() {
	p.BuildParallel = p.Base.ParseIntWithDefault("indexNode.scheduler.buildParallel", 1)
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

func (p *indexNodeConfig) initEnableDisk() {
	var err error
	enableDisk := p.Base.LoadWithDefault("indexNode.enableDisk", "false")
	p.EnableDisk, err = strconv.ParseBool(enableDisk)
	if err != nil {
		panic(err)
	}
}

func (p *indexNodeConfig) initDiskCapacity() {
	diskSizeStr := os.Getenv("LOCAL_STORAGE_SIZE")
	if len(diskSizeStr) == 0 {
		diskUsage, err := disk.Usage("/")
		if err != nil {
			panic(err)
		}

		p.DiskCapacityLimit = int64(diskUsage.Total)
		return
	}

	diskSize, err := strconv.ParseInt(diskSizeStr, 10, 64)
	if err != nil {
		panic(err)
	}
	p.DiskCapacityLimit = diskSize * 1024 * 1024 * 1024
}

func (p *indexNodeConfig) initMaxDiskUsagePercentage() {
	maxDiskUsagePercentageStr := p.Base.LoadWithDefault("indexNode.maxDiskUsagePercentage", "95")
	maxDiskUsagePercentage, err := strconv.ParseInt(maxDiskUsagePercentageStr, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxDiskUsagePercentage = float64(maxDiskUsagePercentage) / 100
}
