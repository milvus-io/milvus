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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/shirou/gopsutil/v3/disk"
	"go.uber.org/zap"
)

const (
	// DefaultRetentionDuration defines the default duration for retention which is 1 days in seconds.
	DefaultRetentionDuration = 0

	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize        = 16
	DefaultGracefulTime          = 5000 //ms
	DefaultGracefulStopTimeout   = 30   // s
	DefaultThreadCoreCoefficient = 10

	DefaultSessionTTL        = 20 //s
	DefaultSessionRetryTimes = 30

	DefaultMaxDegree                = 56
	DefaultSearchListSize           = 100
	DefaultPQCodeBudgetGBRatio      = 0.125
	DefaultBuildNumThreadsRatio     = 1.0
	DefaultSearchCacheBudgetGBRatio = 0.10
	DefaultLoadNumThreadRatio       = 8.0
	DefaultBeamWidthRatio           = 4.0

	DefaultGrpcRetryTimes = 5
)

// ComponentParam is used to quickly and easily access all components' configurations.
type ComponentParam struct {
	ServiceParam
	once sync.Once

	CommonCfg       commonConfig
	QuotaConfig     quotaConfig
	AutoIndexConfig autoIndexConfig

	RootCoordCfg  rootCoordConfig
	ProxyCfg      proxyConfig
	QueryCoordCfg queryCoordConfig
	QueryNodeCfg  queryNodeConfig
	DataCoordCfg  dataCoordConfig
	DataNodeCfg   dataNodeConfig
	IndexCoordCfg indexCoordConfig
	IndexNodeCfg  indexNodeConfig
	HookCfg       HookConfig
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
	p.AutoIndexConfig.init(&p.BaseTable)

	p.RootCoordCfg.init(&p.BaseTable)
	p.ProxyCfg.init(&p.BaseTable)
	p.QueryCoordCfg.init(&p.BaseTable)
	p.QueryNodeCfg.init(&p.BaseTable)
	p.DataCoordCfg.init(&p.BaseTable)
	p.DataNodeCfg.init(&p.BaseTable)
	p.IndexCoordCfg.init(&p.BaseTable)
	p.IndexNodeCfg.init(&p.BaseTable)
	p.HookCfg.init()
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

// /////////////////////////////////////////////////////////////////////////////
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

	IndexSliceSize           int64
	ThreadCoreCoefficient    int64
	MaxDegree                int64
	SearchListSize           int64
	PQCodeBudgetGBRatio      float64
	BuildNumThreadsRatio     float64
	SearchCacheBudgetGBRatio float64
	LoadNumThreadRatio       float64
	BeamWidthRatio           float64
	GracefulTime             int64
	GracefulStopTimeout      int64 // unit: s
	// Search limit, which applies on:
	// maximum # of results to return (topK), and
	// maximum # of search requests (nq).
	// Check https://milvus.io/docs/limitations.md for more details.
	TopKLimit int64

	StorageType string
	SimdType    string

	AuthorizationEnabled bool
	SuperUsers           []string

	ClusterName string

	SessionTTL        int64
	SessionRetryTimes int64

	GrpcRetryTimes uint
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
	p.initMaxDegree()
	p.initTopKLimit()
	p.initSearchListSize()
	p.initPQCodeBudgetGBRatio()
	p.initBuildNumThreadsRatio()
	p.initSearchCacheBudgetGBRatio()
	p.initLoadNumThreadRatio()
	p.initBeamWidthRatio()
	p.initGracefulTime()
	p.initGracefulStopTimeout()
	p.initStorageType()
	p.initThreadCoreCoefficient()

	p.initEnableAuthorization()
	p.initSuperUsers()

	p.initClusterName()

	p.initSessionTTL()
	p.initSessionRetryTimes()
	p.initGrpcRetryTimes()
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

func (p *commonConfig) initThreadCoreCoefficient() {
	p.ThreadCoreCoefficient = p.Base.ParseInt64WithDefault("common.threadCoreCoefficient", DefaultThreadCoreCoefficient)
}

func (p *commonConfig) initPQCodeBudgetGBRatio() {
	p.PQCodeBudgetGBRatio = p.Base.ParseFloatWithDefault("common.DiskIndex.PQCodeBudgetGBRatio", DefaultPQCodeBudgetGBRatio)
}

func (p *commonConfig) initBuildNumThreadsRatio() {
	p.BuildNumThreadsRatio = p.Base.ParseFloatWithDefault("common.DiskIndex.BuildNumThreadsRatio", DefaultBuildNumThreadsRatio)
}

func (p *commonConfig) initSearchCacheBudgetGBRatio() {
	p.SearchCacheBudgetGBRatio = p.Base.ParseFloatWithDefault("common.DiskIndex.SearchCacheBudgetGBRatio", DefaultSearchCacheBudgetGBRatio)
}

func (p *commonConfig) initLoadNumThreadRatio() {
	p.LoadNumThreadRatio = p.Base.ParseFloatWithDefault("common.DiskIndex.LoadNumThreadRatio", DefaultLoadNumThreadRatio)
}

func (p *commonConfig) initBeamWidthRatio() {
	p.BeamWidthRatio = p.Base.ParseFloatWithDefault("common.DiskIndex.BeamWidthRatio", DefaultBeamWidthRatio)
}

func (p *commonConfig) initMaxDegree() {
	p.MaxDegree = p.Base.ParseInt64WithDefault("common.DiskIndex.MaxDegree", DefaultMaxDegree)
}

func (p *commonConfig) initTopKLimit() {
	p.TopKLimit = p.Base.ParseInt64WithDefault("common.topKLimit", 16384)
}

func (p *commonConfig) initSearchListSize() {
	p.SearchListSize = p.Base.ParseInt64WithDefault("common.DiskIndex.SearchListSize", DefaultSearchListSize)
}

func (p *commonConfig) initGracefulTime() {
	p.GracefulTime = p.Base.ParseInt64WithDefault("common.gracefulTime", DefaultGracefulTime)
}

func (p *commonConfig) initGracefulStopTimeout() {
	p.GracefulStopTimeout = p.Base.ParseInt64WithDefault("common.gracefulStopTimeout", DefaultGracefulStopTimeout)
}

func (p *commonConfig) initStorageType() {
	p.StorageType = p.Base.LoadWithDefault("common.storageType", "minio")
}

func (p *commonConfig) initEnableAuthorization() {
	p.AuthorizationEnabled = p.Base.ParseBool("common.security.authorizationEnabled", false)
}

func (p *commonConfig) initSuperUsers() {
	users := p.Base.LoadWithDefault("common.security.superUsers", "")
	if users == "" {
		p.SuperUsers = []string{}
		return
	}
	p.SuperUsers = strings.Split(users, ",")
}

func (p *commonConfig) initClusterName() {
	p.ClusterName = p.Base.LoadWithDefault("common.cluster.name", "")
}

func (p *commonConfig) initSessionTTL() {
	p.SessionTTL = p.Base.ParseInt64WithDefault("common.session.ttl", 20)
}

func (p *commonConfig) initSessionRetryTimes() {
	p.SessionRetryTimes = p.Base.ParseInt64WithDefault("common.session.retryTimes", 30)
}

func (p *commonConfig) initGrpcRetryTimes() {
	p.GrpcRetryTimes = uint(p.Base.ParseIntWithDefault("grpc.server.retryTimes", DefaultGrpcRetryTimes))
}

// /////////////////////////////////////////////////////////////////////////////
// --- rootcoord ---
type rootCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int

	NodeID atomic.Value

	DmlChannelNum               int64
	MaxDatabaseNum              int64
	MaxPartitionNum             int64
	MinSegmentSizeToEnableIndex int64

	// IMPORT
	ImportMaxPendingTaskCount int
	ImportTaskExpiration      float64
	ImportTaskRetention       float64

	// --- ETCD Path ---
	ImportTaskSubPath string

	CreatedTime time.Time
	UpdatedTime time.Time

	EnableActiveStandby bool
}

func (p *rootCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.DmlChannelNum = p.Base.ParseInt64WithDefault("rootCoord.dmlChannelNum", 256)
	p.MaxDatabaseNum = p.Base.ParseInt64WithDefault("rootCoord.maxDatabaseNum", 64)
	p.MaxPartitionNum = p.Base.ParseInt64WithDefault("rootCoord.maxPartitionNum", 4096)
	p.MinSegmentSizeToEnableIndex = p.Base.ParseInt64WithDefault("rootCoord.minSegmentSizeToEnableIndex", 1024)
	p.ImportTaskExpiration = p.Base.ParseFloatWithDefault("rootCoord.importTaskExpiration", 15*60)
	p.ImportTaskRetention = p.Base.ParseFloatWithDefault("rootCoord.importTaskRetention", 24*60*60)
	p.ImportMaxPendingTaskCount = p.Base.ParseIntWithDefault("rootCoord.importMaxPendingTaskCount", 65536)
	p.ImportTaskSubPath = p.Base.LoadWithDefault("rootCoord.importTaskSubPath", "importtask")
	p.EnableActiveStandby = p.Base.ParseBool("rootCoord.enableActiveStandby", false)
	p.NodeID.Store(UniqueID(0))
}

func (p *rootCoordConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *rootCoordConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

// /////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type proxyConfig struct {
	Base *BaseTable

	// NetworkPort & IP are not used
	NetworkPort    int
	IP             string
	NetworkAddress string

	Alias  string
	SoPath string

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

	CreatedTime              time.Time
	UpdatedTime              time.Time
	ShardLeaderCacheInterval atomic.Value
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

	p.initSoPath()
	p.initShardLeaderCacheInterval()
}

// InitAlias initialize Alias member.
func (p *proxyConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *proxyConfig) initSoPath() {
	p.SoPath = p.Base.LoadWithDefault("proxy.soPath", "")
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
	str := p.Base.LoadWithDefault("proxy.maxShardNum", "64")
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

func (p *proxyConfig) initShardLeaderCacheInterval() {
	interval := p.Base.ParseIntWithDefault("proxy.shardLeaderCacheInterval", 30)
	p.ShardLeaderCacheInterval.Store(time.Duration(interval) * time.Second)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int
	NodeID  atomic.Value

	CreatedTime time.Time
	UpdatedTime time.Time

	//---- Task ---
	RetryNum         int32
	RetryInterval    int64
	TaskMergeCap     int32
	TaskExecutionCap int32

	//---- Handoff ---
	AutoHandoff bool

	//---- Balance ---
	AutoBalance                         bool
	Balancer                            string
	GlobalRowCountFactor                float64
	ScoreUnbalanceTolerationFactor      float64
	ReverseUnbalanceTolerationFactor    float64
	OverloadedMemoryThresholdPercentage float64
	BalanceIntervalSeconds              int64
	MemoryUsageMaxDifferencePercentage  float64
	CheckInterval                       time.Duration
	ChannelTaskTimeout                  time.Duration
	SegmentTaskTimeout                  time.Duration
	DistPullInterval                    time.Duration
	LoadTimeoutSeconds                  time.Duration
	CheckHandoffInterval                time.Duration
	EnableActiveStandby                 bool

	NextTargetSurviveTime      time.Duration
	UpdateNextTargetInterval   time.Duration
	CheckNodeInReplicaInterval time.Duration
	CheckResourceGroupInterval time.Duration
	EnableRGAutoRecover        bool
	CheckHealthInterval        time.Duration
	CheckHealthRPCTimeout      time.Duration
}

func (p *queryCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))

	//---- Task ---
	p.initTaskRetryNum()
	p.initTaskRetryInterval()
	p.initTaskMergeCap()
	p.initTaskExecutionCap()

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
	p.initEnableActiveStandby()
	p.initNextTargetSurviveTime()
	p.initUpdateNextTargetInterval()
	p.initCheckNodeInReplicaInterval()
	p.initBalancer()
	p.initGlobalRowCountFactor()
	p.initScoreUnbalanceTolerationFactor()
	p.initReverseUnbalanceTolerationFactor()
	p.initCheckResourceGroupInterval()
	p.initEnableRGAutoRecover()

	// Check QN Health
	p.initCheckHealthInterval()
	p.initCheckHealthRPCTimeout()
}

func (p *queryCoordConfig) initTaskRetryNum() {
	p.RetryNum = p.Base.ParseInt32WithDefault("queryCoord.task.retrynum", 5)
}

func (p *queryCoordConfig) initTaskRetryInterval() {
	p.RetryInterval = p.Base.ParseInt64WithDefault("queryCoord.task.retryinterval", int64(10*time.Second))
}

func (p *queryCoordConfig) initTaskMergeCap() {
	p.TaskMergeCap = p.Base.ParseInt32WithDefault("queryCoord.taskMergeCap", 16)
}

func (p *queryCoordConfig) initTaskExecutionCap() {
	p.TaskExecutionCap = p.Base.ParseInt32WithDefault("queryCoord.taskExecutionCap", 256)
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

func (p *queryCoordConfig) initEnableActiveStandby() {
	p.EnableActiveStandby = p.Base.ParseBool("queryCoord.enableActiveStandby", false)
}

func (p *queryCoordConfig) initCheckInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkInterval", "10000")
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
	timeout := p.Base.LoadWithDefault("queryCoord.loadTimeoutSeconds", "1800")
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

func (p *queryCoordConfig) initNextTargetSurviveTime() {
	interval := p.Base.LoadWithDefault("queryCoord.NextTargetSurviveTime", "300")
	nextTargetSurviveTime, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.NextTargetSurviveTime = time.Duration(nextTargetSurviveTime) * time.Second
}

func (p *queryCoordConfig) initUpdateNextTargetInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.UpdateNextTargetInterval", "30")
	updateNextTargetInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.UpdateNextTargetInterval = time.Duration(updateNextTargetInterval) * time.Second
}

func (p *queryCoordConfig) initCheckNodeInReplicaInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkNodeInReplicaInterval", "60")
	checkNodeInReplicaInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckNodeInReplicaInterval = time.Duration(checkNodeInReplicaInterval) * time.Second
}

func (p *queryCoordConfig) initBalancer() {
	balancer := p.Base.LoadWithDefault("queryCoord.balancer", "ScoreBasedBalancer")
	p.Balancer = balancer
}

func (p *queryCoordConfig) initGlobalRowCountFactor() {
	factorStr := p.Base.LoadWithDefault("queryCoord.globalRowCountFactor", "0.1")
	globalRowCountFactor, err := strconv.ParseFloat(factorStr, 64)
	if err != nil {
		panic(err)
	}
	if globalRowCountFactor > 1.0 {
		log.Warn("globalRowCountFactor should not be more than 1.0, force set to 1.0")
		globalRowCountFactor = 1.0
	}
	if globalRowCountFactor <= 0 {
		log.Warn("globalRowCountFactor should not be less than 0, force set to 0.1")
		globalRowCountFactor = 0.1
	}
	p.GlobalRowCountFactor = globalRowCountFactor
}

func (p *queryCoordConfig) initScoreUnbalanceTolerationFactor() {
	factorStr := p.Base.LoadWithDefault("queryCoord.scoreUnbalanceTolerationFactor", "0.05")
	scoreUnbalanceTolerationFactor, err := strconv.ParseFloat(factorStr, 64)
	if err != nil {
		panic(err)
	}
	if scoreUnbalanceTolerationFactor < 0 {
		log.Warn("scoreUnbalanceTolerationFactor should not be less than 0, force set to 0.05")
		scoreUnbalanceTolerationFactor = 0.05
	}
	p.ScoreUnbalanceTolerationFactor = scoreUnbalanceTolerationFactor
}

func (p *queryCoordConfig) initReverseUnbalanceTolerationFactor() {
	factorStr := p.Base.LoadWithDefault("queryCoord.reverseUnBalanceTolerationFactor", "1.3")
	reverseToleration, err := strconv.ParseFloat(factorStr, 64)
	if err != nil {
		panic(err)
	}
	if reverseToleration > 2.0 {
		log.Warn("reverseToleration should not be more than 2.0, force set to 2.0")
		reverseToleration = 2.0
	}
	if reverseToleration < 1.1 {
		log.Warn("reverseToleration should not be less than 1.1, force set to 1.1")
		reverseToleration = 1.1
	}
	p.ReverseUnbalanceTolerationFactor = reverseToleration
}

func (p *queryCoordConfig) initCheckResourceGroupInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkResourceGroupInterval", "10")
	checkResourceGroupInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckResourceGroupInterval = time.Duration(checkResourceGroupInterval) * time.Second
}

func (p *queryCoordConfig) initEnableRGAutoRecover() {
	p.EnableRGAutoRecover = p.Base.ParseBool("queryCoord.enableRGAutoRecover", true)
}

func (p *queryCoordConfig) initCheckHealthInterval() {
	interval := p.Base.LoadWithDefault("queryCoord.checkHealthInterval", "3000")
	checkHealthInterval, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckHealthInterval = time.Duration(checkHealthInterval) * time.Millisecond
}

func (p *queryCoordConfig) initCheckHealthRPCTimeout() {
	interval := p.Base.LoadWithDefault("queryCoord.checkHealthRPCTimeout", "100")
	checkHealthRPCTimeout, err := strconv.ParseInt(interval, 10, 64)
	if err != nil {
		panic(err)
	}
	p.CheckHealthRPCTimeout = time.Duration(checkHealthRPCTimeout) * time.Millisecond
}

// /////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	Base *BaseTable

	SoPath        string
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
	ChunkRows              int64
	SmallIndexNlist        int64
	SmallIndexNProbe       int64
	KnowhereThreadPoolSize uint32

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
	MaxTimestampLag      time.Duration

	GCHelperEnabled   bool
	MinimumGOGCConfig int
	MaximumGOGCConfig int

	GracefulStopTimeout int64
}

func (p *queryNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))

	p.initSoPath()
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

	p.initGCTunerEnbaled()
	p.initMaximumGOGC()
	p.initMinimumGOGC()

	p.initGracefulStopTimeout()
	p.initMaxTimestampLag()
	p.initKnowhereThreadPoolSize()
}

// InitAlias initializes an alias for the QueryNode role.
func (p *queryNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *queryNodeConfig) initSoPath() {
	p.SoPath = p.Base.LoadWithDefault("queryNode.soPath", "")
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

func (p *queryNodeConfig) initKnowhereThreadPoolSize() {
	cpuNum := uint32(runtime.GOMAXPROCS(0))
	if p.EnableDisk {
		threadRate := p.Base.ParseFloatWithDefault("queryNode.segcore.knowhereThreadPoolNumRatio", 1)
		p.KnowhereThreadPoolSize = uint32(threadRate * float64(cpuNum))
	} else {
		p.KnowhereThreadPoolSize = cpuNum
	}
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
	p.MaxGroupNQ = p.Base.ParseInt64WithDefault("queryNode.grouping.maxNQ", 50000)
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

func (p *queryNodeConfig) initGCTunerEnbaled() {
	p.GCHelperEnabled = p.Base.ParseBool("queryNode.gchelper.enabled", true)
}

func (p *queryNodeConfig) initMinimumGOGC() {
	p.MinimumGOGCConfig = p.Base.ParseIntWithDefault("queryNode.gchelper.minimumGoGC", 30)
}

func (p *queryNodeConfig) initMaximumGOGC() {
	p.MaximumGOGCConfig = p.Base.ParseIntWithDefault("queryNode.gchelper.maximumGoGC", 200)
}

func (p *queryNodeConfig) initGracefulStopTimeout() {
	timeout := p.Base.LoadWithDefault2([]string{"queryNode.gracefulStopTimeout", "common.gracefulStopTimeout"},
		strconv.FormatInt(DefaultGracefulStopTimeout, 10))
	var err error
	p.GracefulStopTimeout, err = strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		panic(err)
	}
}

func (p *queryNodeConfig) initMaxTimestampLag() {
	maxTsLagSeconds := p.Base.ParseIntWithDefault("queryNode.scheduler.maxTimestampLag", 86400)
	p.MaxTimestampLag = time.Duration(maxTsLagSeconds) * time.Second
}

// /////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	Base *BaseTable

	NodeID atomic.Value

	IP      string
	Port    int
	Address string

	// --- ETCD ---
	ChannelWatchSubPath string

	// --- CHANNEL ---
	WatchTimeoutInterval         time.Duration
	ChannelBalanceSilentDuration time.Duration
	ChannelBalanceInterval       time.Duration

	// --- SEGMENTS ---
	SegmentMaxSize                 float64
	DiskSegmentMaxSize             float64
	SegmentSealProportion          float64
	SegAssignmentExpiration        int64
	SegmentMaxLifetime             time.Duration
	SegmentMaxIdleTime             time.Duration
	SegmentMinSizeFromIdleToSealed float64
	SegmentMaxBinlogFileNumber     int

	CreatedTime time.Time
	UpdatedTime time.Time

	// compaction
	EnableCompaction     bool
	EnableAutoCompaction atomic.Value

	MinSegmentToMerge                 int
	MaxSegmentToMerge                 int
	SegmentSmallProportion            float64
	SegmentCompactableProportion      float64
	SegmentExpansionRate              float64
	CompactionTimeoutInSeconds        int32
	CompactionCheckIntervalInSeconds  int64
	SingleCompactionRatioThreshold    float32
	SingleCompactionDeltaLogMaxSize   int64
	SingleCompactionExpiredLogMaxSize int64
	SingleCompactionDeltalogMaxNum    int64
	GlobalCompactionInterval          time.Duration

	// Garbage Collection
	EnableGarbageCollection bool
	GCInterval              time.Duration
	GCMissingTolerance      time.Duration
	GCDropTolerance         time.Duration
	EnableActiveStandby     bool
}

func (p *dataCoordConfig) init(base *BaseTable) {
	p.Base = base
	p.initChannelWatchPrefix()

	p.initWatchTimeoutInterval()
	p.initChannelBalanceSilentDuration()
	p.initChannelBalanceInterval()

	p.initSegmentMaxSize()
	p.initDiskSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()
	p.initSegmentMaxLifetime()
	p.initSegmentMaxIdleTime()
	p.initSegmentMinSizeFromIdleToSealed()
	p.initSegmentMaxBinlogFileNumber()

	p.initEnableCompaction()
	p.initEnableAutoCompaction()

	p.initCompactionMinSegment()
	p.initCompactionMaxSegment()
	p.initSegmentProportion()
	p.initSegmentExpansionRate()
	p.initCompactionTimeoutInSeconds()
	p.initCompactionCheckIntervalInSeconds()
	p.initSingleCompactionRatioThreshold()
	p.initSingleCompactionDeltaLogMaxSize()
	p.initSingleCompactionExpiredLogMaxSize()
	p.initSingleCompactionDeltalogMaxNum()
	p.initGlobalCompactionInterval()

	p.initEnableGarbageCollection()
	p.initGCInterval()
	p.initGCMissingTolerance()
	p.initGCDropTolerance()
	p.initEnableActiveStandby()
}

func (p *dataCoordConfig) initWatchTimeoutInterval() {
	p.WatchTimeoutInterval = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.channel.watchTimeoutInterval", 120)) * time.Second
}

func (p *dataCoordConfig) initChannelBalanceSilentDuration() {
	p.ChannelBalanceSilentDuration = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.channel.balanceSilentDuration", 300)) * time.Second
}

func (p *dataCoordConfig) initChannelBalanceInterval() {
	p.ChannelBalanceInterval = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.channel.balanceInterval", 360)) * time.Second
}

func (p *dataCoordConfig) initSegmentMaxSize() {
	p.SegmentMaxSize = p.Base.ParseFloatWithDefault("dataCoord.segment.maxSize", 512.0)
}

func (p *dataCoordConfig) initDiskSegmentMaxSize() {
	p.DiskSegmentMaxSize = p.Base.ParseFloatWithDefault("dataCoord.segment.diskSegmentMaxSize", 512.0*4)
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

func (p *dataCoordConfig) initSegmentExpansionRate() {
	p.SegmentExpansionRate = p.Base.ParseFloatWithDefault("dataCoord.segment.expansionRate", 1.25)
	log.Info("init segment expansion rate", zap.Float64("value", p.SegmentExpansionRate))

}

func (p *dataCoordConfig) initSegmentMaxBinlogFileNumber() {
	p.SegmentMaxBinlogFileNumber = p.Base.ParseIntWithDefault("dataCoord.segment.maxBinlogFileNumber", 32)
	log.Info("init segment max binlog file to sealed", zap.Int("value", p.SegmentMaxBinlogFileNumber))
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
	p.EnableAutoCompaction.Store(p.Base.ParseBool("dataCoord.compaction.enableAutoCompaction", true))
}

func (p *dataCoordConfig) initCompactionMinSegment() {
	p.MinSegmentToMerge = p.Base.ParseIntWithDefault("dataCoord.compaction.min.segment", 3)
}

func (p *dataCoordConfig) initCompactionMaxSegment() {
	p.MaxSegmentToMerge = p.Base.ParseIntWithDefault("dataCoord.compaction.max.segment", 30)
}

func (p *dataCoordConfig) initSegmentSmallProportion() {
	p.SegmentSmallProportion = p.Base.ParseFloatWithDefault("dataCoord.segment.smallProportion", 0.5)
}

func (p *dataCoordConfig) initSegmentCompactableProportion() {
	p.SegmentCompactableProportion = p.Base.ParseFloatWithDefault("dataCoord.segment.compactableProportion", 0.85)
}

func (p *dataCoordConfig) initSegmentProportion() {
	p.initSegmentSmallProportion()
	p.initSegmentCompactableProportion()

	// avoid invalid single compaction
	if p.SegmentCompactableProportion < p.SegmentSmallProportion {
		p.SegmentCompactableProportion = p.SegmentSmallProportion
	}
}

// compaction execution timeout
func (p *dataCoordConfig) initCompactionTimeoutInSeconds() {
	p.CompactionTimeoutInSeconds = p.Base.ParseInt32WithDefault("dataCoord.compaction.timeout", 60*5)
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

// if total delta log number > SingleCompactionDeltalogMaxNum, trigger single compaction to ensure delta number per segment is limited
func (p *dataCoordConfig) initSingleCompactionDeltalogMaxNum() {
	p.SingleCompactionDeltalogMaxNum = p.Base.ParseInt64WithDefault("dataCoord.compaction.single.deltalog.maxnum", 200)
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
	p.GCDropTolerance = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.gc.dropTolerance", 3600)) * time.Second
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

func (p *dataCoordConfig) initEnableActiveStandby() {
	p.EnableActiveStandby = p.Base.ParseBool("dataCoord.enableActiveStandby", false)
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

// /////////////////////////////////////////////////////////////////////////////
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

	// segment
	FlushInsertBufferSize  int64
	FlushDeleteBufferBytes int64
	BinLogMaxSize          int64
	SyncPeriod             time.Duration
	CpLagPeriod            time.Duration

	Alias string // Different datanode in one machine

	// etcd
	ChannelWatchSubPath string

	// watchEvent
	WatchEventTicklerInterval time.Duration

	// io concurrency to fetch stats logs
	IOConcurrency int

	// datanote send timetick interval per channel
	DataNodeTimeTickInterval int

	SkipBFStatsLoad bool

	CreatedTime time.Time
	UpdatedTime time.Time

	// memory management
	MemoryForceSyncEnable     bool
	MemoryForceSyncSegmentNum int
	MemoryWatermark           float64
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initWatchEventTicklerInterval()
	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initFlushDeleteBufferSize()
	p.initBinlogMaxSize()
	p.initSyncPeriod()
	p.initCpLagPeriod()
	p.initIOConcurrency()
	p.initDataNodeTimeTickInterval()

	p.initSkipBFStatsLoad()

	p.initChannelWatchPath()
	p.initMemoryForceSyncEnable()
	p.initMemoryWatermark()
	p.initMemoryForceSyncSegmentNum()
}

// InitAlias init this DataNode alias
func (p *dataNodeConfig) InitAlias(alias string) {
	p.Alias = alias
}

func (p *dataNodeConfig) initWatchEventTicklerInterval() {
	p.WatchEventTicklerInterval = time.Duration(p.Base.ParseInt64WithDefault("dataCoord.channel.watchEventTicklerInterval", 15)) * time.Second
}

func (p *dataNodeConfig) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.Base.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxQueueLength", 1024)
}

func (p *dataNodeConfig) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.Base.ParseInt32WithDefault("dataNode.dataSync.flowGraph.maxParallelism", 1024)
}

func (p *dataNodeConfig) initFlushInsertBufferSize() {
	bufferSize := p.Base.LoadWithDefault2([]string{"DATA_NODE_IBUFSIZE", "datanode.segment.insertBufSize"}, "16777216")
	bs, err := strconv.ParseInt(bufferSize, 10, 64)
	if err != nil {
		panic(err)
	}
	p.FlushInsertBufferSize = bs
}

func (p *dataNodeConfig) initFlushDeleteBufferSize() {
	deleteBufBytes := p.Base.ParseInt64WithDefault("datanode.segment.deleteBufBytes",
		64*1024*1024)
	p.FlushDeleteBufferBytes = deleteBufBytes
}

func (p *dataNodeConfig) initBinlogMaxSize() {
	size := p.Base.LoadWithDefault("datanode.segment.binlog.maxsize", "67108864")
	bs, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	p.BinLogMaxSize = bs
}

func (p *dataNodeConfig) initSyncPeriod() {
	syncPeriodInSeconds := p.Base.ParseInt64WithDefault("datanode.segment.syncPeriod", 600)
	p.SyncPeriod = time.Duration(syncPeriodInSeconds) * time.Second
}

func (p *dataNodeConfig) initCpLagPeriod() {
	cpLagPeriod := p.Base.ParseInt64WithDefault("datanode.segment.cpLagPeriod", 600)
	p.CpLagPeriod = time.Duration(cpLagPeriod) * time.Second
}

func (p *dataNodeConfig) initChannelWatchPath() {
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *dataNodeConfig) initIOConcurrency() {
	p.IOConcurrency = p.Base.ParseIntWithDefault("dataNode.dataSync.ioConcurrency", 10)
}

func (p *dataNodeConfig) initDataNodeTimeTickInterval() {
	p.DataNodeTimeTickInterval = p.Base.ParseIntWithDefault("datanode.timetick.interval", 500)
}

func (p *dataNodeConfig) initSkipBFStatsLoad() {
	p.SkipBFStatsLoad = p.Base.ParseBool("dataNode.skip.BFStats.Load", false)
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

func (p *dataNodeConfig) initMemoryForceSyncEnable() {
	p.MemoryForceSyncEnable = p.Base.ParseBool("datanode.memory.forceSyncEnable", true)
}

func (p *dataNodeConfig) initMemoryWatermark() {
	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		p.MemoryWatermark = p.Base.ParseFloatWithDefault("datanode.memory.watermarkStandalone", 0.2)
		return
	}
	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.ClusterDeployMode {
		p.MemoryWatermark = p.Base.ParseFloatWithDefault("datanode.memory.watermarkCluster", 0.5)
		return
	}
	log.Warn("DeployModeEnv is not set, use default", zap.Float64("default", 0.5))
	p.MemoryWatermark = 0.5
}

func (p *dataNodeConfig) initMemoryForceSyncSegmentNum() {
	p.MemoryForceSyncSegmentNum = p.Base.ParseIntWithDefault("datanode.memory.forceSyncSegmentNum", 1)
	if p.MemoryForceSyncSegmentNum < 1 {
		p.MemoryForceSyncSegmentNum = 1
	}
}

// /////////////////////////////////////////////////////////////////////////////
// --- indexcoord ---
type indexCoordConfig struct {
	Base *BaseTable

	Address string
	Port    int

	BindIndexNodeMode bool
	IndexNodeAddress  string
	WithCredential    bool
	IndexNodeID       int64

	NodeID atomic.Value

	MinSegmentNumRowsToEnableIndex int64

	SchedulerInterval time.Duration
	GCInterval        time.Duration

	CreatedTime time.Time
	UpdatedTime time.Time

	EnableActiveStandby bool
}

func (p *indexCoordConfig) init(base *BaseTable) {
	p.Base = base

	p.initGCInterval()
	p.initSchedulerInterval()
	p.initMinSegmentNumRowsToEnableIndex()
	p.initBindIndexNodeMode()
	p.initIndexNodeAddress()
	p.initWithCredential()
	p.initIndexNodeID()
	p.initEnableActiveStandby()
	p.NodeID.Store(UniqueID(0))
}

func (p *indexCoordConfig) initMinSegmentNumRowsToEnableIndex() {
	p.MinSegmentNumRowsToEnableIndex = p.Base.ParseInt64WithDefault("indexCoord.minSegmentNumRowsToEnableIndex", 1024)
}

func (p *indexCoordConfig) initGCInterval() {
	p.GCInterval = time.Duration(p.Base.ParseInt64WithDefault("indexCoord.gc.interval", 60*10)) * time.Second
}

func (p *indexCoordConfig) initSchedulerInterval() {
	p.SchedulerInterval = time.Duration(p.Base.ParseInt64WithDefault("indexCoord.scheduler.interval", 1000)) * time.Millisecond
}

func (p *indexCoordConfig) initBindIndexNodeMode() {
	p.BindIndexNodeMode = p.Base.ParseBool("indexCoord.bindIndexNodeMode.enable", false)
}

func (p *indexCoordConfig) initIndexNodeAddress() {
	p.IndexNodeAddress = p.Base.LoadWithDefault("indexCoord.bindIndexNodeMode.address", "localhost:22930")
}

func (p *indexCoordConfig) initWithCredential() {
	p.WithCredential = p.Base.ParseBool("indexCoord.bindIndexNodeMode.withCred", false)
}

func (p *indexCoordConfig) initIndexNodeID() {
	p.IndexNodeID = p.Base.ParseInt64WithDefault("indexCoord.bindIndexNodeMode.nodeID", 0)
}

func (p *indexCoordConfig) initEnableActiveStandby() {
	p.EnableActiveStandby = p.Base.ParseBool("indexCoord.enableActiveStandby", false)
}

func (p *indexCoordConfig) SetNodeID(id UniqueID) {
	p.NodeID.Store(id)
}

func (p *indexCoordConfig) GetNodeID() UniqueID {
	val := p.NodeID.Load()
	if val != nil {
		return val.(UniqueID)
	}
	return 0
}

// /////////////////////////////////////////////////////////////////////////////
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

	GracefulStopTimeout int64
}

func (p *indexNodeConfig) init(base *BaseTable) {
	p.Base = base
	p.NodeID.Store(UniqueID(0))
	p.initBuildParallel()
	p.initEnableDisk()
	p.initDiskCapacity()
	p.initMaxDiskUsagePercentage()
	p.initGracefulStopTimeout()
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

func (p *indexNodeConfig) initGracefulStopTimeout() {
	timeout := p.Base.LoadWithDefault2([]string{"indexNode.gracefulStopTimeout", "common.gracefulStopTimeout"},
		strconv.FormatInt(DefaultGracefulStopTimeout, 10))
	var err error
	p.GracefulStopTimeout, err = strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		panic(err)
	}
}
