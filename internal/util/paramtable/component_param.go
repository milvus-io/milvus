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
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/disk"

	config "github.com/milvus-io/milvus/internal/config"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// DefaultRetentionDuration defines the default duration for retention which is 1 days in seconds.
	DefaultRetentionDuration = 0

	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize        = 16
	DefaultGracefulTime          = 5000 //ms
	DefaultGracefulStopTimeout   = 30   // s
	DefaultThreadCoreCoefficient = 10

	DefaultSessionTTL        = 60 //s
	DefaultSessionRetryTimes = 30

	DefaultMaxDegree                = 56
	DefaultSearchListSize           = 100
	DefaultPQCodeBudgetGBRatio      = 0.125
	DefaultBuildNumThreadsRatio     = 1.0
	DefaultSearchCacheBudgetGBRatio = 0.10
	DefaultLoadNumThreadRatio       = 8.0
	DefaultBeamWidthRatio           = 4.0
)

// ComponentParam is used to quickly and easily access all components' configurations.
type ComponentParam struct {
	ServiceParam
	once sync.Once

	CommonCfg       commonConfig
	QuotaConfig     quotaConfig
	AutoIndexConfig autoIndexConfig
	TraceCfg        traceConfig

	RootCoordCfg  rootCoordConfig
	ProxyCfg      proxyConfig
	QueryCoordCfg queryCoordConfig
	QueryNodeCfg  queryNodeConfig
	DataCoordCfg  dataCoordConfig
	DataNodeCfg   dataNodeConfig
	IndexNodeCfg  indexNodeConfig
	HTTPCfg       httpConfig
	HookCfg       hookConfig

	RootCoordGrpcServerCfg  GrpcServerConfig
	ProxyGrpcServerCfg      GrpcServerConfig
	QueryCoordGrpcServerCfg GrpcServerConfig
	QueryNodeGrpcServerCfg  GrpcServerConfig
	DataCoordGrpcServerCfg  GrpcServerConfig
	DataNodeGrpcServerCfg   GrpcServerConfig
	IndexCoordGrpcServerCfg GrpcServerConfig
	IndexNodeGrpcServerCfg  GrpcServerConfig

	RootCoordGrpcClientCfg  GrpcClientConfig
	ProxyGrpcClientCfg      GrpcClientConfig
	QueryCoordGrpcClientCfg GrpcClientConfig
	QueryNodeGrpcClientCfg  GrpcClientConfig
	DataCoordGrpcClientCfg  GrpcClientConfig
	DataNodeGrpcClientCfg   GrpcClientConfig
	IndexCoordGrpcClientCfg GrpcClientConfig
	IndexNodeGrpcClientCfg  GrpcClientConfig

	IntegrationTestCfg integrationTestConfig
}

// Init initialize once
func (p *ComponentParam) Init() {
	p.once.Do(func() {
		p.init()
	})
}

// init initialize the global param table

func (p *ComponentParam) init() {
	p.ServiceParam.init()

	p.CommonCfg.init(&p.BaseTable)
	p.QuotaConfig.init(&p.BaseTable)
	p.AutoIndexConfig.init(&p.BaseTable)
	p.TraceCfg.init(&p.BaseTable)

	p.RootCoordCfg.init(&p.BaseTable)
	p.ProxyCfg.init(&p.BaseTable)
	p.QueryCoordCfg.init(&p.BaseTable)
	p.QueryNodeCfg.init(&p.BaseTable)
	p.DataCoordCfg.init(&p.BaseTable)
	p.DataNodeCfg.init(&p.BaseTable)
	p.IndexNodeCfg.init(&p.BaseTable)
	p.HTTPCfg.init(&p.BaseTable)
	p.HookCfg.init()

	p.RootCoordGrpcServerCfg.Init(typeutil.RootCoordRole, &p.BaseTable)
	p.ProxyGrpcServerCfg.Init(typeutil.ProxyRole, &p.BaseTable)
	p.QueryCoordGrpcServerCfg.Init(typeutil.QueryCoordRole, &p.BaseTable)
	p.QueryNodeGrpcServerCfg.Init(typeutil.QueryNodeRole, &p.BaseTable)
	p.DataCoordGrpcServerCfg.Init(typeutil.DataCoordRole, &p.BaseTable)
	p.DataNodeGrpcServerCfg.Init(typeutil.DataNodeRole, &p.BaseTable)
	p.IndexNodeGrpcServerCfg.Init(typeutil.IndexNodeRole, &p.BaseTable)

	p.RootCoordGrpcClientCfg.Init(typeutil.RootCoordRole, &p.BaseTable)
	p.ProxyGrpcClientCfg.Init(typeutil.ProxyRole, &p.BaseTable)
	p.QueryCoordGrpcClientCfg.Init(typeutil.QueryCoordRole, &p.BaseTable)
	p.QueryNodeGrpcClientCfg.Init(typeutil.QueryNodeRole, &p.BaseTable)
	p.DataCoordGrpcClientCfg.Init(typeutil.DataCoordRole, &p.BaseTable)
	p.DataNodeGrpcClientCfg.Init(typeutil.DataNodeRole, &p.BaseTable)
	p.IndexNodeGrpcClientCfg.Init(typeutil.IndexNodeRole, &p.BaseTable)

	p.IntegrationTestCfg.init(&p.BaseTable)
}

func (p *ComponentParam) GetComponentConfigurations(componentName string, sub string) map[string]string {
	allownPrefixs := append(globalConfigPrefixs(), componentName+".")
	return p.mgr.GetBy(config.WithSubstr(sub), config.WithOneOfPrefixs(allownPrefixs...))
}

func (p *ComponentParam) GetAll() map[string]string {
	return p.mgr.GetConfigs()
}

func (p *ComponentParam) Watch(key string, watcher config.EventHandler) {
	p.mgr.Dispatcher.Register(key, watcher)
}

// /////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	ClusterPrefix ParamItem `refreshable:"false"`

	// Deprecated: do not use it anymore
	ProxySubName ParamItem `refreshable:"true"`

	RootCoordTimeTick   ParamItem `refreshable:"true"`
	RootCoordStatistics ParamItem `refreshable:"true"`
	RootCoordDml        ParamItem `refreshable:"false"`
	RootCoordDelta      ParamItem `refreshable:"false"`
	// Deprecated: do not use it anymore
	RootCoordSubName ParamItem `refreshable:"true"`

	// Deprecated: only used in metrics as ID
	QueryCoordSearch ParamItem `refreshable:"true"`
	// Deprecated: only used in metrics as ID
	QueryCoordSearchResult ParamItem `refreshable:"true"`
	QueryCoordTimeTick     ParamItem `refreshable:"true"`
	QueryNodeSubName       ParamItem `refreshable:"false"`

	// Deprecated: do not use it anymore
	DataCoordStatistic    ParamItem `refreshable:"true"`
	DataCoordTimeTick     ParamItem `refreshable:"false"`
	DataCoordSegmentInfo  ParamItem `refreshable:"true"`
	DataCoordSubName      ParamItem `refreshable:"false"`
	DataCoordWatchSubPath ParamItem `refreshable:"false"`
	DataNodeSubName       ParamItem `refreshable:"false"`

	DefaultPartitionName ParamItem `refreshable:"false"`
	DefaultIndexName     ParamItem `refreshable:"false"`
	RetentionDuration    ParamItem `refreshable:"true"`
	EntityExpirationTTL  ParamItem `refreshable:"true"`

	IndexSliceSize           ParamItem `refreshable:"false"`
	ThreadCoreCoefficient    ParamItem `refreshable:"false"`
	MaxDegree                ParamItem `refreshable:"true"`
	SearchListSize           ParamItem `refreshable:"true"`
	PQCodeBudgetGBRatio      ParamItem `refreshable:"true"`
	BuildNumThreadsRatio     ParamItem `refreshable:"true"`
	SearchCacheBudgetGBRatio ParamItem `refreshable:"true"`
	LoadNumThreadRatio       ParamItem `refreshable:"true"`
	BeamWidthRatio           ParamItem `refreshable:"true"`
	GracefulTime             ParamItem `refreshable:"true"`
	GracefulStopTimeout      ParamItem `refreshable:"true"`

	// Search limit, which applies on:
	// maximum # of results to return (topK), and
	// maximum # of search requests (nq).
	// Check https://milvus.io/docs/limitations.md for more details.
	TopKLimit   ParamItem `refreshable:"true"`
	StorageType ParamItem `refreshable:"false"`
	SimdType    ParamItem `refreshable:"false"`

	AuthorizationEnabled ParamItem `refreshable:"false"`
	SuperUsers           ParamItem `refreshable:"true"`

	ClusterName ParamItem `refreshable:"false"`

	SessionTTL        ParamItem `refreshable:"false"`
	SessionRetryTimes ParamItem `refreshable:"false"`
}

func (p *commonConfig) init(base *BaseTable) {
	// must init cluster prefix first
	p.ClusterPrefix = ParamItem{
		Key:          "msgChannel.chanNamePrefix.cluster",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.cluster"},
		PanicIfEmpty: true,
		Forbidden:    true,
	}
	p.ClusterPrefix.Init(base.mgr)

	chanNamePrefix := func(prefix string) string {
		return strings.Join([]string{p.ClusterPrefix.GetValue(), prefix}, "-")
	}
	p.ProxySubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.proxySubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.proxySubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.ProxySubName.Init(base.mgr)

	// --- rootcoord ---
	p.RootCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.RootCoordTimeTick.Init(base.mgr)

	p.RootCoordStatistics = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordStatistics",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordStatistics"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.RootCoordStatistics.Init(base.mgr)

	p.RootCoordDml = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordDml",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordDml"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Doc:          "It is not refreshable currently",
	}
	p.RootCoordDml.Init(base.mgr)

	p.RootCoordDelta = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordDelta",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordDelta"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Doc:          "It is not refreshable currently",
	}
	p.RootCoordDelta.Init(base.mgr)

	p.RootCoordSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.rootCoordSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.rootCoordSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Doc:          "It is deprecated",
	}
	p.RootCoordSubName.Init(base.mgr)

	p.QueryCoordSearch = ParamItem{
		Key:          "msgChannel.chanNamePrefix.search",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.search"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Doc:          "It is deprecated",
	}
	p.QueryCoordSearch.Init(base.mgr)

	p.QueryCoordSearchResult = ParamItem{
		Key:          "msgChannel.chanNamePrefix.searchResult",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.searchResult"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Doc:          "It is deprecated",
	}
	p.QueryCoordSearchResult.Init(base.mgr)

	p.QueryCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.queryTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.queryTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.QueryCoordTimeTick.Init(base.mgr)

	p.QueryNodeSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.queryNodeSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.queryNodeSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.QueryNodeSubName.Init(base.mgr)

	p.DataCoordStatistic = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordStatistic",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordStatistic"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.DataCoordStatistic.Init(base.mgr)

	p.DataCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.DataCoordTimeTick.Init(base.mgr)

	p.DataCoordSegmentInfo = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordSegmentInfo",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordSegmentInfo"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.DataCoordSegmentInfo.Init(base.mgr)

	p.DataCoordSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataCoordSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.DataCoordSubName.Init(base.mgr)

	p.DataCoordWatchSubPath = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordWatchSubPath",
		Version:      "2.1.0",
		DefaultValue: "channelwatch",
		PanicIfEmpty: true,
	}
	p.DataCoordWatchSubPath.Init(base.mgr)

	p.DataNodeSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataNodeSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataNodeSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
	}
	p.DataNodeSubName.Init(base.mgr)

	p.DefaultPartitionName = ParamItem{
		Key:          "common.defaultPartitionName",
		Version:      "2.0.0",
		DefaultValue: "_default",
		Forbidden:    true,
	}
	p.DefaultPartitionName.Init(base.mgr)

	p.DefaultIndexName = ParamItem{
		Key:          "common.defaultIndexName",
		Version:      "2.0.0",
		DefaultValue: "_default_idx",
		Forbidden:    true,
	}
	p.DefaultIndexName.Init(base.mgr)

	p.RetentionDuration = ParamItem{
		Key:          "common.retentionDuration",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultRetentionDuration),
	}
	p.RetentionDuration.Init(base.mgr)

	p.EntityExpirationTTL = ParamItem{
		Key:          "common.entityExpiration",
		Version:      "2.1.0",
		DefaultValue: "-1",
		Formatter: func(value string) string {
			ttl := getAsInt(value)
			if ttl < 0 {
				return "-1"
			}

			// make sure ttl is larger than retention duration to ensure time travel works
			if ttl > p.RetentionDuration.GetAsInt() {
				return strconv.Itoa(ttl)
			}
			return p.RetentionDuration.GetValue()
		},
	}
	p.EntityExpirationTTL.Init(base.mgr)

	p.SimdType = ParamItem{
		Key:          "common.simdType",
		Version:      "2.1.0",
		DefaultValue: "auto",
		FallbackKeys: []string{"knowhere.simdType"},
	}
	p.SimdType.Init(base.mgr)

	p.IndexSliceSize = ParamItem{
		Key:          "common.indexSliceSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultIndexSliceSize),
	}
	p.IndexSliceSize.Init(base.mgr)

	p.MaxDegree = ParamItem{
		Key:          "common.DiskIndex.MaxDegree",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultMaxDegree),
	}
	p.MaxDegree.Init(base.mgr)

	p.SearchListSize = ParamItem{
		Key:          "common.DiskIndex.SearchListSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultSearchListSize),
	}
	p.SearchListSize.Init(base.mgr)

	p.PQCodeBudgetGBRatio = ParamItem{
		Key:          "common.DiskIndex.PQCodeBudgetGBRatio",
		Version:      "2.0.0",
		DefaultValue: fmt.Sprintf("%f", DefaultPQCodeBudgetGBRatio),
	}
	p.PQCodeBudgetGBRatio.Init(base.mgr)

	p.BuildNumThreadsRatio = ParamItem{
		Key:          "common.DiskIndex.BuildNumThreadsRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultBuildNumThreadsRatio),
	}
	p.BuildNumThreadsRatio.Init(base.mgr)

	p.SearchCacheBudgetGBRatio = ParamItem{
		Key:          "common.DiskIndex.SearchCacheBudgetGBRatio",
		Version:      "2.0.0",
		DefaultValue: fmt.Sprintf("%f", DefaultSearchCacheBudgetGBRatio),
	}
	p.SearchCacheBudgetGBRatio.Init(base.mgr)

	p.LoadNumThreadRatio = ParamItem{
		Key:          "common.DiskIndex.LoadNumThreadRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultLoadNumThreadRatio),
	}
	p.LoadNumThreadRatio.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "common.gracefulStopTimeout",
		Version:      "2.2.1",
		DefaultValue: "30",
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.TopKLimit = ParamItem{
		Key:          "common.topKLimit",
		Version:      "2.2.1",
		DefaultValue: "16384",
	}
	p.TopKLimit.Init(base.mgr)

	p.BeamWidthRatio = ParamItem{
		Key:          "common.DiskIndex.BeamWidthRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultBeamWidthRatio),
	}
	p.BeamWidthRatio.Init(base.mgr)

	p.GracefulTime = ParamItem{
		Key:          "common.gracefulTime",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultGracefulTime),
	}
	p.GracefulTime.Init(base.mgr)

	p.StorageType = ParamItem{
		Key:          "common.storageType",
		Version:      "2.0.0",
		DefaultValue: "minio",
	}
	p.StorageType.Init(base.mgr)

	p.ThreadCoreCoefficient = ParamItem{
		Key:          "common.threadCoreCoefficient",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultThreadCoreCoefficient),
	}
	p.ThreadCoreCoefficient.Init(base.mgr)

	p.AuthorizationEnabled = ParamItem{
		Key:          "common.security.authorizationEnabled",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.AuthorizationEnabled.Init(base.mgr)

	p.SuperUsers = ParamItem{
		Key:     "common.security.superUsers",
		Version: "2.2.1",
	}
	p.SuperUsers.Init(base.mgr)

	p.ClusterName = ParamItem{
		Key:          "common.cluster.name",
		Version:      "2.0.0",
		DefaultValue: "",
	}
	p.ClusterName.Init(base.mgr)

	p.SessionTTL = ParamItem{
		Key:          "common.session.ttl",
		Version:      "2.0.0",
		DefaultValue: "60",
	}
	p.SessionTTL.Init(base.mgr)

	p.SessionRetryTimes = ParamItem{
		Key:          "common.session.retryTimes",
		Version:      "2.0.0",
		DefaultValue: "30",
	}
	p.SessionRetryTimes.Init(base.mgr)

}

type traceConfig struct {
	Exporter       ParamItem `refreshable:"false"`
	SampleFraction ParamItem `refreshable:"false"`
	JaegerURL      ParamItem `refreshable:"false"`
}

func (t *traceConfig) init(base *BaseTable) {
	t.Exporter = ParamItem{
		Key:     "trace.exporter",
		Version: "2.3.0",
	}
	t.Exporter.Init(base.mgr)

	t.SampleFraction = ParamItem{
		Key:          "trace.sampleFraction",
		Version:      "2.3.0",
		DefaultValue: "1",
	}
	t.SampleFraction.Init(base.mgr)

	t.JaegerURL = ParamItem{
		Key:     "trace.jaeger.url",
		Version: "2.3.0",
	}
	t.JaegerURL.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- rootcoord ---
type rootCoordConfig struct {
	DmlChannelNum               ParamItem `refreshable:"false"`
	MaxPartitionNum             ParamItem `refreshable:"true"`
	MinSegmentSizeToEnableIndex ParamItem `refreshable:"true"`
	ImportTaskExpiration        ParamItem `refreshable:"true"`
	ImportTaskRetention         ParamItem `refreshable:"true"`
	ImportMaxPendingTaskCount   ParamItem `refreshable:"true"`
	ImportTaskSubPath           ParamItem `refreshable:"true"`
	EnableActiveStandby         ParamItem `refreshable:"false"`
}

func (p *rootCoordConfig) init(base *BaseTable) {
	p.DmlChannelNum = ParamItem{
		Key:          "rootCoord.dmlChannelNum",
		Version:      "2.0.0",
		DefaultValue: "256",
	}
	p.DmlChannelNum.Init(base.mgr)

	p.MaxPartitionNum = ParamItem{
		Key:          "rootCoord.maxPartitionNum",
		Version:      "2.0.0",
		DefaultValue: "4096",
	}
	p.MaxPartitionNum.Init(base.mgr)

	p.MinSegmentSizeToEnableIndex = ParamItem{
		Key:          "rootCoord.minSegmentSizeToEnableIndex",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.MinSegmentSizeToEnableIndex.Init(base.mgr)

	p.ImportTaskExpiration = ParamItem{
		Key:          "rootCoord.importTaskExpiration",
		Version:      "2.2.0",
		DefaultValue: "900", // 15 * 60 seconds
	}
	p.ImportTaskExpiration.Init(base.mgr)

	p.ImportTaskRetention = ParamItem{
		Key:          "rootCoord.importTaskRetention",
		Version:      "2.2.0",
		DefaultValue: strconv.Itoa(24 * 60 * 60),
	}
	p.ImportTaskRetention.Init(base.mgr)

	p.ImportTaskSubPath = ParamItem{
		Key:          "rootCoord.ImportTaskSubPath",
		Version:      "2.2.0",
		DefaultValue: "importtask",
	}
	p.ImportTaskSubPath.Init(base.mgr)

	p.ImportMaxPendingTaskCount = ParamItem{
		Key:          "rootCoord.importMaxPendingTaskCount",
		Version:      "2.2.2",
		DefaultValue: strconv.Itoa(65535),
	}
	p.ImportMaxPendingTaskCount.Init(base.mgr)

	p.EnableActiveStandby = ParamItem{
		Key:          "rootCoord.enableActiveStandby",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.EnableActiveStandby.Init(base.mgr)

}

// /////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type AccessLogConfig struct {
	// if use access log
	Enable ParamItem `refreshable:"false"`
	// if upload sealed access log file to minio
	MinioEnable ParamItem `refreshable:"false"`
	// Log path
	LocalPath ParamItem `refreshable:"false"`
	// Log filename, leave empty to disable file log.
	Filename ParamItem `refreshable:"false"`
	// Max size for a single file, in MB.
	MaxSize ParamItem `refreshable:"false"`
	// Max time for single access log file in seconds
	RotatedTime ParamItem `refreshable:"false"`
	// Maximum number of old log files to retain.
	MaxBackups ParamItem `refreshable:"false"`
	//File path in minIO
	RemotePath ParamItem `refreshable:"false"`
	//Max time for log file in minIO, in hours
	RemoteMaxTime ParamItem `refreshable:"false"`
}

type proxyConfig struct {
	// Alias  string
	SoPath ParamItem `refreshable:"false"`

	TimeTickInterval         ParamItem `refreshable:"false"`
	MsgStreamTimeTickBufSize ParamItem `refreshable:"true"`
	MaxNameLength            ParamItem `refreshable:"true"`
	MaxUsernameLength        ParamItem `refreshable:"true"`
	MinPasswordLength        ParamItem `refreshable:"true"`
	MaxPasswordLength        ParamItem `refreshable:"true"`
	MaxFieldNum              ParamItem `refreshable:"true"`
	MaxShardNum              ParamItem `refreshable:"true"`
	MaxDimension             ParamItem `refreshable:"true"`
	GinLogging               ParamItem `refreshable:"false"`
	MaxUserNum               ParamItem `refreshable:"true"`
	MaxRoleNum               ParamItem `refreshable:"true"`
	MaxTaskNum               ParamItem `refreshable:"false"`
	AccessLog                AccessLogConfig
}

func (p *proxyConfig) init(base *BaseTable) {
	p.TimeTickInterval = ParamItem{
		Key:          "proxy.timeTickInterval",
		Version:      "2.2.0",
		DefaultValue: "200",
		PanicIfEmpty: true,
	}
	p.TimeTickInterval.Init(base.mgr)

	p.MsgStreamTimeTickBufSize = ParamItem{
		Key:          "proxy.msgStream.timeTick.bufSize",
		Version:      "2.2.0",
		DefaultValue: "512",
		PanicIfEmpty: true,
	}
	p.MsgStreamTimeTickBufSize.Init(base.mgr)

	p.MaxNameLength = ParamItem{
		Key:          "proxy.maxNameLength",
		DefaultValue: "255",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxNameLength.Init(base.mgr)

	p.MinPasswordLength = ParamItem{
		Key:          "proxy.minPasswordLength",
		DefaultValue: "6",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MinPasswordLength.Init(base.mgr)

	p.MaxUsernameLength = ParamItem{
		Key:          "proxy.maxUsernameLength",
		DefaultValue: "32",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxUsernameLength.Init(base.mgr)

	p.MaxPasswordLength = ParamItem{
		Key:          "proxy.maxPasswordLength",
		DefaultValue: "256",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxPasswordLength.Init(base.mgr)

	p.MaxFieldNum = ParamItem{
		Key:          "proxy.maxFieldNum",
		DefaultValue: "64",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxFieldNum.Init(base.mgr)

	p.MaxShardNum = ParamItem{
		Key:          "proxy.maxShardNum",
		DefaultValue: "64",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxShardNum.Init(base.mgr)

	p.MaxDimension = ParamItem{
		Key:          "proxy.maxDimension",
		DefaultValue: "32768",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxDimension.Init(base.mgr)

	p.MaxTaskNum = ParamItem{
		Key:          "proxy.maxTaskNum",
		Version:      "2.2.0",
		DefaultValue: "1024",
	}
	p.MaxTaskNum.Init(base.mgr)

	p.GinLogging = ParamItem{
		Key:          "proxy.ginLogging",
		Version:      "2.2.0",
		DefaultValue: "true",
	}
	p.GinLogging.Init(base.mgr)

	p.MaxUserNum = ParamItem{
		Key:          "proxy.maxUserNum",
		DefaultValue: "100",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxUserNum.Init(base.mgr)

	p.MaxRoleNum = ParamItem{
		Key:          "proxy.maxRoleNum",
		DefaultValue: "10",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.MaxRoleNum.Init(base.mgr)

	p.SoPath = ParamItem{
		Key:          "proxy.soPath",
		Version:      "2.2.0",
		DefaultValue: "",
	}
	p.SoPath.Init(base.mgr)

	p.AccessLog.Enable = ParamItem{
		Key:          "proxy.accessLog.enable",
		Version:      "2.2.0",
		DefaultValue: "true",
	}
	p.AccessLog.Enable.Init(base.mgr)

	p.AccessLog.MinioEnable = ParamItem{
		Key:          "proxy.accessLog.minioEnable",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.AccessLog.MinioEnable.Init(base.mgr)

	p.AccessLog.LocalPath = ParamItem{
		Key:     "proxy.accessLog.localPath",
		Version: "2.2.0",
	}
	p.AccessLog.LocalPath.Init(base.mgr)

	p.AccessLog.Filename = ParamItem{
		Key:          "proxy.accessLog.filename",
		Version:      "2.2.0",
		DefaultValue: "milvus_access_log.log",
	}
	p.AccessLog.Filename.Init(base.mgr)

	p.AccessLog.MaxSize = ParamItem{
		Key:          "proxy.accessLog.maxSize",
		Version:      "2.2.0",
		DefaultValue: "64",
	}
	p.AccessLog.MaxSize.Init(base.mgr)

	p.AccessLog.MaxBackups = ParamItem{
		Key:          "proxy.accessLog.maxBackups",
		Version:      "2.2.0",
		DefaultValue: "8",
	}
	p.AccessLog.MaxBackups.Init(base.mgr)

	p.AccessLog.RotatedTime = ParamItem{
		Key:          "proxy.accessLog.rotatedTime",
		Version:      "2.2.0",
		DefaultValue: "3600",
	}
	p.AccessLog.RotatedTime.Init(base.mgr)

	p.AccessLog.RemotePath = ParamItem{
		Key:          "proxy.accessLog.remotePath",
		Version:      "2.2.0",
		DefaultValue: "access_log/",
	}
	p.AccessLog.RemotePath.Init(base.mgr)

	p.AccessLog.RemoteMaxTime = ParamItem{
		Key:          "proxy.accessLog.remoteMaxTime",
		Version:      "2.2.0",
		DefaultValue: "168",
	}
	p.AccessLog.RemoteMaxTime.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	//Deprecated: Since 2.2.0
	RetryNum ParamItem `refreshable:"true"`
	//Deprecated: Since 2.2.0
	RetryInterval    ParamItem `refreshable:"true"`
	TaskMergeCap     ParamItem `refreshable:"false"`
	TaskExecutionCap ParamItem `refreshable:"true"`

	//---- Handoff ---
	//Deprecated: Since 2.2.2
	AutoHandoff ParamItem `refreshable:"true"`

	//---- Balance ---
	AutoBalance                         ParamItem `refreshable:"true"`
	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"true"`
	BalanceIntervalSeconds              ParamItem `refreshable:"true"`
	MemoryUsageMaxDifferencePercentage  ParamItem `refreshable:"true"`
	CheckInterval                       ParamItem `refreshable:"true"`
	ChannelTaskTimeout                  ParamItem `refreshable:"true"`
	SegmentTaskTimeout                  ParamItem `refreshable:"true"`
	DistPullInterval                    ParamItem `refreshable:"false"`
	HeartbeatAvailableInterval          ParamItem `refreshable:"true"`
	LoadTimeoutSeconds                  ParamItem `refreshable:"true"`
	//Deprecated: Since 2.2.2, QueryCoord do not use HandOff logic anymore
	CheckHandoffInterval ParamItem `refreshable:"true"`
	EnableActiveStandby  ParamItem `refreshable:"false"`

	NextTargetSurviveTime      ParamItem `refreshable:"true"`
	UpdateNextTargetInterval   ParamItem `refreshable:"false"`
	CheckNodeInReplicaInterval ParamItem `refreshable:"false"`
	CheckResourceGroupInterval ParamItem `refreshable:"false"`
	EnableRGAutoRecover        ParamItem `refreshable:"true"`
}

func (p *queryCoordConfig) init(base *BaseTable) {
	//---- Task ---
	p.RetryNum = ParamItem{
		Key:          "queryCoord.task.retrynum",
		Version:      "2.2.0",
		DefaultValue: "5",
	}
	p.RetryNum.Init(base.mgr)

	p.RetryInterval = ParamItem{
		Key:          "queryCoord.task.retryinterval",
		Version:      "2.2.0",
		DefaultValue: strconv.FormatInt(int64(10*time.Second), 10),
	}
	p.RetryInterval.Init(base.mgr)

	p.TaskMergeCap = ParamItem{
		Key:          "queryCoord.taskMergeCap",
		Version:      "2.2.0",
		DefaultValue: "16",
	}
	p.TaskMergeCap.Init(base.mgr)

	p.TaskExecutionCap = ParamItem{
		Key:          "queryCoord.taskExecutionCap",
		Version:      "2.2.0",
		DefaultValue: "256",
	}
	p.TaskExecutionCap.Init(base.mgr)

	p.AutoHandoff = ParamItem{
		Key:          "queryCoord.autoHandoff",
		Version:      "2.0.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
	}
	p.AutoHandoff.Init(base.mgr)

	p.AutoBalance = ParamItem{
		Key:          "queryCoord.autoBalance",
		Version:      "2.0.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.AutoBalance.Init(base.mgr)

	p.OverloadedMemoryThresholdPercentage = ParamItem{
		Key:          "queryCoord.overloadedMemoryThresholdPercentage",
		Version:      "2.0.0",
		DefaultValue: "90",
		PanicIfEmpty: true,
	}
	p.OverloadedMemoryThresholdPercentage.Init(base.mgr)

	p.BalanceIntervalSeconds = ParamItem{
		Key:          "queryCoord.balanceIntervalSeconds",
		Version:      "2.0.0",
		DefaultValue: "60",
		PanicIfEmpty: true,
	}
	p.BalanceIntervalSeconds.Init(base.mgr)

	p.MemoryUsageMaxDifferencePercentage = ParamItem{
		Key:          "queryCoord.memoryUsageMaxDifferencePercentage",
		Version:      "2.0.0",
		DefaultValue: "30",
		PanicIfEmpty: true,
	}
	p.MemoryUsageMaxDifferencePercentage.Init(base.mgr)

	p.CheckInterval = ParamItem{
		Key:          "queryCoord.checkInterval",
		Version:      "2.0.0",
		DefaultValue: "1000",
		PanicIfEmpty: true,
	}
	p.CheckInterval.Init(base.mgr)

	p.ChannelTaskTimeout = ParamItem{
		Key:          "queryCoord.channelTaskTimeout",
		Version:      "2.0.0",
		DefaultValue: "60000",
		PanicIfEmpty: true,
	}
	p.ChannelTaskTimeout.Init(base.mgr)

	p.SegmentTaskTimeout = ParamItem{
		Key:          "queryCoord.segmentTaskTimeout",
		Version:      "2.0.0",
		DefaultValue: "120000",
		PanicIfEmpty: true,
	}
	p.SegmentTaskTimeout.Init(base.mgr)

	p.DistPullInterval = ParamItem{
		Key:          "queryCoord.distPullInterval",
		Version:      "2.0.0",
		DefaultValue: "500",
		PanicIfEmpty: true,
	}
	p.DistPullInterval.Init(base.mgr)

	p.LoadTimeoutSeconds = ParamItem{
		Key:          "queryCoord.loadTimeoutSeconds",
		Version:      "2.0.0",
		DefaultValue: "600",
		PanicIfEmpty: true,
	}
	p.LoadTimeoutSeconds.Init(base.mgr)

	p.HeartbeatAvailableInterval = ParamItem{
		Key:          "queryCoord.heartbeatAvailableInterval",
		Version:      "2.2.1",
		DefaultValue: "10000",
		PanicIfEmpty: true,
	}
	p.HeartbeatAvailableInterval.Init(base.mgr)

	p.CheckHandoffInterval = ParamItem{
		Key:          "queryCoord.checkHandoffInterval",
		DefaultValue: "5000",
		Version:      "2.2.0",
		PanicIfEmpty: true,
	}
	p.CheckHandoffInterval.Init(base.mgr)

	p.EnableActiveStandby = ParamItem{
		Key:          "queryCoord.enableActiveStandby",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.EnableActiveStandby.Init(base.mgr)

	p.NextTargetSurviveTime = ParamItem{
		Key:          "queryCoord.NextTargetSurviveTime",
		Version:      "2.0.0",
		DefaultValue: "300",
		PanicIfEmpty: true,
	}
	p.NextTargetSurviveTime.Init(base.mgr)

	p.UpdateNextTargetInterval = ParamItem{
		Key:          "queryCoord.UpdateNextTargetInterval",
		Version:      "2.0.0",
		DefaultValue: "10",
		PanicIfEmpty: true,
	}
	p.UpdateNextTargetInterval.Init(base.mgr)

	p.CheckNodeInReplicaInterval = ParamItem{
		Key:          "queryCoord.checkNodeInReplicaInterval",
		Version:      "2.2.3",
		DefaultValue: "60",
		PanicIfEmpty: true,
	}
	p.CheckNodeInReplicaInterval.Init(base.mgr)

	p.CheckResourceGroupInterval = ParamItem{
		Key:          "queryCoord.checkResourceGroupInterval",
		Version:      "2.2.3",
		DefaultValue: "10",
		PanicIfEmpty: true,
	}
	p.CheckResourceGroupInterval.Init(base.mgr)

	p.EnableRGAutoRecover = ParamItem{
		Key:          "queryCoord.enableRGAutoRecover",
		Version:      "2.2.3",
		DefaultValue: "true",
		PanicIfEmpty: true,
	}
	p.EnableRGAutoRecover.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`

	// stats
	//Deprecated: Never used
	StatsPublishInterval ParamItem `refreshable:"true"`

	// segcore
	ChunkRows        ParamItem `refreshable:"false"`
	SmallIndexNlist  ParamItem `refreshable:"false"`
	SmallIndexNProbe ParamItem `refreshable:"false"`

	// memory limit
	LoadMemoryUsageFactor               ParamItem `refreshable:"true"`
	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"false"`

	// enable disk
	EnableDisk             ParamItem `refreshable:"true"`
	DiskCapacityLimit      ParamItem `refreshable:"true"`
	MaxDiskUsagePercentage ParamItem `refreshable:"true"`

	// cache limit
	CacheEnabled     ParamItem `refreshable:"false"`
	CacheMemoryLimit ParamItem `refreshable:"false"`

	GroupEnabled         ParamItem `refreshable:"true"`
	MaxReceiveChanSize   ParamItem `refreshable:"false"`
	MaxUnsolvedQueueSize ParamItem `refreshable:"true"`
	MaxReadConcurrency   ParamItem `refreshable:"true"`
	MaxGroupNQ           ParamItem `refreshable:"true"`
	TopKMergeRatio       ParamItem `refreshable:"true"`
	CPURatio             ParamItem `refreshable:"true"`
	MaxTimestampLag      ParamItem `refreshable:"true"`

	GCHelperEnabled     ParamItem `refreshable:"false"`
	MinimumGOGCConfig   ParamItem `refreshable:"false"`
	MaximumGOGCConfig   ParamItem `refreshable:"false"`
	GracefulStopTimeout ParamItem `refreshable:"false"`
}

func (p *queryNodeConfig) init(base *BaseTable) {
	p.FlowGraphMaxQueueLength = ParamItem{
		Key:          "queryNode.dataSync.flowGraph.maxQueueLength",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.FlowGraphMaxQueueLength.Init(base.mgr)

	p.FlowGraphMaxParallelism = ParamItem{
		Key:          "queryNode.dataSync.flowGraph.maxParallelism",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.FlowGraphMaxParallelism.Init(base.mgr)

	p.StatsPublishInterval = ParamItem{
		Key:          "queryNode.stats.publishInterval",
		Version:      "2.0.0",
		DefaultValue: "1000",
	}
	p.StatsPublishInterval.Init(base.mgr)

	p.ChunkRows = ParamItem{
		Key:          "queryNode.segcore.chunkRows",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Formatter: func(v string) string {
			if getAsInt(v) < 1024 {
				return "1024"
			}
			return v
		},
	}
	p.ChunkRows.Init(base.mgr)

	p.SmallIndexNlist = ParamItem{
		Key:     "queryNode.segcore.smallIndex.nlist",
		Version: "2.0.0",
		Formatter: func(v string) string {
			rows := p.ChunkRows.GetAsInt64()
			var defaultNList int64
			for i := int64(0); i < rows; i++ {
				if math.Pow(2.0, float64(i)) > math.Sqrt(float64(rows)) {
					defaultNList = int64(math.Pow(2, float64(i)))
					break
				}
			}

			nlist := getAsInt64(v)
			if nlist == 0 {
				nlist = defaultNList
			}
			if nlist > rows/8 {
				return strconv.FormatInt(rows/8, 10)
			}
			return strconv.FormatInt(nlist, 10)
		},
	}
	p.SmallIndexNlist.Init(base.mgr)

	p.SmallIndexNProbe = ParamItem{
		Key:     "queryNode.segcore.smallIndex.nprobe",
		Version: "2.0.0",
		Formatter: func(v string) string {
			defaultNprobe := p.SmallIndexNlist.GetAsInt64() / 16
			nprobe := getAsInt64(v)
			if nprobe == 0 {
				nprobe = defaultNprobe
			}
			if nprobe > p.SmallIndexNlist.GetAsInt64() {
				return p.SmallIndexNlist.GetValue()
			}
			return strconv.FormatInt(nprobe, 10)
		},
	}
	p.SmallIndexNProbe.Init(base.mgr)

	p.LoadMemoryUsageFactor = ParamItem{
		Key:          "queryNode.loadMemoryUsageFactor",
		Version:      "2.0.0",
		DefaultValue: "3",
		PanicIfEmpty: true,
	}
	p.LoadMemoryUsageFactor.Init(base.mgr)

	p.OverloadedMemoryThresholdPercentage = ParamItem{
		Key:          "queryCoord.overloadedMemoryThresholdPercentage",
		Version:      "2.0.0",
		DefaultValue: "90",
		PanicIfEmpty: true,
		Formatter: func(v string) string {
			return fmt.Sprintf("%f", getAsFloat(v)/100)
		},
	}
	p.OverloadedMemoryThresholdPercentage.Init(base.mgr)

	p.CacheMemoryLimit = ParamItem{
		Key:          "queryNode.cache.memoryLimit",
		Version:      "2.0.0",
		DefaultValue: "2147483648",
		PanicIfEmpty: true,
	}
	p.CacheMemoryLimit.Init(base.mgr)

	p.CacheEnabled = ParamItem{
		Key:          "queryNode.cache.enabled",
		Version:      "2.0.0",
		DefaultValue: "",
	}
	p.CacheEnabled.Init(base.mgr)

	p.GroupEnabled = ParamItem{
		Key:          "queryNode.grouping.enabled",
		Version:      "2.0.0",
		DefaultValue: "true",
	}
	p.GroupEnabled.Init(base.mgr)

	p.MaxReceiveChanSize = ParamItem{
		Key:          "queryNode.scheduler.receiveChanSize",
		Version:      "2.0.0",
		DefaultValue: "10240",
	}
	p.MaxReceiveChanSize.Init(base.mgr)

	p.MaxReadConcurrency = ParamItem{
		Key:          "queryNode.scheduler.maxReadConcurrentRatio",
		Version:      "2.0.0",
		DefaultValue: "2.0",
		Formatter: func(v string) string {
			ratio := getAsFloat(v)
			cpuNum := int64(runtime.GOMAXPROCS(0))
			concurrency := int64(float64(cpuNum) * ratio)
			if concurrency < 1 {
				return "1" // MaxReadConcurrency must >= 1
			} else if concurrency > cpuNum*100 {
				return strconv.FormatInt(cpuNum*100, 10) // MaxReadConcurrency must <= 100*cpuNum
			}
			return strconv.FormatInt(concurrency, 10)
		},
	}
	p.MaxReadConcurrency.Init(base.mgr)

	p.MaxUnsolvedQueueSize = ParamItem{
		Key:          "queryNode.scheduler.unsolvedQueueSize",
		Version:      "2.0.0",
		DefaultValue: "10240",
	}
	p.MaxUnsolvedQueueSize.Init(base.mgr)

	p.MaxGroupNQ = ParamItem{
		Key:          "queryNode.grouping.maxNQ",
		Version:      "2.0.0",
		DefaultValue: "1000",
	}
	p.MaxGroupNQ.Init(base.mgr)

	p.TopKMergeRatio = ParamItem{
		Key:          "queryNode.grouping.topKMergeRatio",
		Version:      "2.0.0",
		DefaultValue: "10.0",
	}
	p.TopKMergeRatio.Init(base.mgr)

	p.CPURatio = ParamItem{
		Key:          "queryNode.scheduler.cpuRatio",
		Version:      "2.0.0",
		DefaultValue: "10",
	}
	p.CPURatio.Init(base.mgr)

	p.EnableDisk = ParamItem{
		Key:          "queryNode.enableDisk",
		Version:      "2.2.0",
		DefaultValue: "false",
	}
	p.EnableDisk.Init(base.mgr)

	p.DiskCapacityLimit = ParamItem{
		Key:     "LOCAL_STORAGE_SIZE",
		Version: "2.2.0",
		Formatter: func(v string) string {
			if len(v) == 0 {
				diskUsage, err := disk.Usage("/")
				if err != nil {
					panic(err)
				}
				return strconv.FormatUint(diskUsage.Total, 10)
			}
			diskSize := getAsInt64(v)
			return strconv.FormatInt(diskSize*1024*1024*1024, 10)
		},
	}
	p.DiskCapacityLimit.Init(base.mgr)

	p.MaxDiskUsagePercentage = ParamItem{
		Key:          "queryNode.maxDiskUsagePercentage",
		Version:      "2.2.0",
		DefaultValue: "95",
		PanicIfEmpty: true,
		Formatter: func(v string) string {
			return fmt.Sprintf("%f", getAsFloat(v)/100)
		},
	}
	p.MaxDiskUsagePercentage.Init(base.mgr)

	p.MaxTimestampLag = ParamItem{
		Key:          "queryNode.scheduler.maxTimestampLag",
		Version:      "2.2.3",
		DefaultValue: "86400",
	}
	p.MaxTimestampLag.Init(base.mgr)

	p.GCHelperEnabled = ParamItem{
		Key:          "queryNode.gchelper.enabled",
		Version:      "2.0.0",
		DefaultValue: "true",
	}
	p.GCHelperEnabled.Init(base.mgr)

	p.MaximumGOGCConfig = ParamItem{
		Key:          "queryNode.gchelper.maximumGoGC",
		Version:      "2.0.0",
		DefaultValue: "200",
	}
	p.MaximumGOGCConfig.Init(base.mgr)

	p.MinimumGOGCConfig = ParamItem{
		Key:          "queryNode.gchelper.minimumGoGC",
		Version:      "2.0.0",
		DefaultValue: "30",
	}
	p.MinimumGOGCConfig.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "queryNode.gracefulStopTimeout",
		Version:      "2.2.1",
		FallbackKeys: []string{"common.gracefulStopTimeout"},
	}
	p.GracefulStopTimeout.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {

	// --- CHANNEL ---
	MaxWatchDuration ParamItem `refreshable:"false"`

	// --- SEGMENTS ---
	SegmentMaxSize                 ParamItem `refreshable:"false"`
	DiskSegmentMaxSize             ParamItem `refreshable:"true"`
	SegmentSealProportion          ParamItem `refreshable:"false"`
	SegAssignmentExpiration        ParamItem `refreshable:"true"`
	SegmentMaxLifetime             ParamItem `refreshable:"false"`
	SegmentMaxIdleTime             ParamItem `refreshable:"false"`
	SegmentMinSizeFromIdleToSealed ParamItem `refreshable:"false"`
	SegmentMaxBinlogFileNumber     ParamItem `refreshable:"false"`

	// compaction
	EnableCompaction     ParamItem `refreshable:"false"`
	EnableAutoCompaction ParamItem `refreshable:"true"`

	MinSegmentToMerge                 ParamItem `refreshable:"true"`
	MaxSegmentToMerge                 ParamItem `refreshable:"true"`
	SegmentSmallProportion            ParamItem `refreshable:"true"`
	SegmentCompactableProportion      ParamItem `refreshable:"true"`
	SegmentExpansionRate              ParamItem `refreshable:"true"`
	CompactionTimeoutInSeconds        ParamItem `refreshable:"true"`
	CompactionCheckIntervalInSeconds  ParamItem `refreshable:"false"`
	SingleCompactionRatioThreshold    ParamItem `refreshable:"true"`
	SingleCompactionDeltaLogMaxSize   ParamItem `refreshable:"true"`
	SingleCompactionExpiredLogMaxSize ParamItem `refreshable:"true"`
	SingleCompactionDeltalogMaxNum    ParamItem `refreshable:"true"`
	GlobalCompactionInterval          ParamItem `refreshable:"false"`

	// Garbage Collection
	EnableGarbageCollection ParamItem `refreshable:"false"`
	GCInterval              ParamItem `refreshable:"false"`
	GCMissingTolerance      ParamItem `refreshable:"false"`
	GCDropTolerance         ParamItem `refreshable:"false"`
	EnableActiveStandby     ParamItem `refreshable:"false"`

	BindIndexNodeMode          ParamItem `refreshable:"false"`
	IndexNodeAddress           ParamItem `refreshable:"false"`
	WithCredential             ParamItem `refreshable:"false"`
	IndexNodeID                ParamItem `refreshable:"false"`
	IndexTaskSchedulerInterval ParamItem `refreshable:"false"`

	MinSegmentNumRowsToEnableIndex ParamItem `refreshable:"true"`
}

func (p *dataCoordConfig) init(base *BaseTable) {

	p.MaxWatchDuration = ParamItem{
		Key:          "dataCoord.channel.maxWatchDuration",
		Version:      "2.2.1",
		DefaultValue: "600",
	}
	p.MaxWatchDuration.Init(base.mgr)

	p.SegmentMaxSize = ParamItem{
		Key:          "dataCoord.segment.maxSize",
		Version:      "2.0.0",
		DefaultValue: "512",
	}
	p.SegmentMaxSize.Init(base.mgr)

	p.DiskSegmentMaxSize = ParamItem{
		Key:          "dataCoord.segment.diskSegmentMaxSize",
		Version:      "2.0.0",
		DefaultValue: "512",
	}
	p.DiskSegmentMaxSize.Init(base.mgr)

	p.SegmentSealProportion = ParamItem{
		Key:          "dataCoord.segment.sealProportion",
		Version:      "2.0.0",
		DefaultValue: "0.25",
	}
	p.SegmentSealProportion.Init(base.mgr)

	p.SegAssignmentExpiration = ParamItem{
		Key:          "dataCoord.segment.assignmentExpiration",
		Version:      "2.0.0",
		DefaultValue: "2000",
	}
	p.SegAssignmentExpiration.Init(base.mgr)

	p.SegmentMaxLifetime = ParamItem{
		Key:          "dataCoord.segment.maxLife",
		Version:      "2.0.0",
		DefaultValue: "86400",
	}
	p.SegmentMaxLifetime.Init(base.mgr)

	p.SegmentMaxIdleTime = ParamItem{
		Key:          "dataCoord.segment.maxIdleTime",
		Version:      "2.0.0",
		DefaultValue: "3600",
	}
	p.SegmentMaxIdleTime.Init(base.mgr)

	p.SegmentMinSizeFromIdleToSealed = ParamItem{
		Key:          "dataCoord.segment.minSizeFromIdleToSealed",
		Version:      "2.0.0",
		DefaultValue: "16.0",
	}
	p.SegmentMinSizeFromIdleToSealed.Init(base.mgr)

	p.SegmentMaxBinlogFileNumber = ParamItem{
		Key:          "dataCoord.segment.maxBinlogFileNumber",
		Version:      "2.2.0",
		DefaultValue: "32",
	}
	p.SegmentMaxBinlogFileNumber.Init(base.mgr)

	p.EnableCompaction = ParamItem{
		Key:          "dataCoord.enableCompaction",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.EnableCompaction.Init(base.mgr)

	p.EnableAutoCompaction = ParamItem{
		Key:          "dataCoord.compaction.enableAutoCompaction",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.EnableAutoCompaction.Init(base.mgr)

	p.MinSegmentToMerge = ParamItem{
		Key:          "dataCoord.compaction.min.segment",
		Version:      "2.0.0",
		DefaultValue: "3",
	}
	p.MinSegmentToMerge.Init(base.mgr)

	p.MaxSegmentToMerge = ParamItem{
		Key:          "dataCoord.compaction.max.segment",
		Version:      "2.0.0",
		DefaultValue: "30",
	}
	p.MaxSegmentToMerge.Init(base.mgr)

	p.SegmentSmallProportion = ParamItem{
		Key:          "dataCoord.segment.smallProportion",
		Version:      "2.0.0",
		DefaultValue: "0.5",
	}
	p.SegmentSmallProportion.Init(base.mgr)

	p.SegmentCompactableProportion = ParamItem{
		Key:          "dataCoord.segment.compactableProportion",
		Version:      "2.2.1",
		DefaultValue: "0.5",
	}
	p.SegmentCompactableProportion.Init(base.mgr)

	p.SegmentExpansionRate = ParamItem{
		Key:          "dataCoord.segment.expansionRate",
		Version:      "2.2.1",
		DefaultValue: "1.25",
	}
	p.SegmentExpansionRate.Init(base.mgr)

	p.CompactionTimeoutInSeconds = ParamItem{
		Key:          "dataCoord.compaction.timeout",
		Version:      "2.0.0",
		DefaultValue: "300",
	}
	p.CompactionTimeoutInSeconds.Init(base.mgr)

	p.CompactionCheckIntervalInSeconds = ParamItem{
		Key:          "dataCoord.compaction.check.interval",
		Version:      "2.0.0",
		DefaultValue: "10",
	}
	p.CompactionCheckIntervalInSeconds.Init(base.mgr)

	p.SingleCompactionRatioThreshold = ParamItem{
		Key:          "dataCoord.compaction.single.ratio.threshold",
		Version:      "2.0.0",
		DefaultValue: "0.2",
	}
	p.SingleCompactionRatioThreshold.Init(base.mgr)

	p.SingleCompactionDeltaLogMaxSize = ParamItem{
		Key:          "dataCoord.compaction.single.deltalog.maxsize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(2 * 1024 * 1024),
	}
	p.SingleCompactionDeltaLogMaxSize.Init(base.mgr)

	p.SingleCompactionExpiredLogMaxSize = ParamItem{
		Key:          "dataCoord.compaction.single.expiredlog.maxsize",
		Version:      "2.0.0",
		DefaultValue: "10485760",
	}
	p.SingleCompactionExpiredLogMaxSize.Init(base.mgr)

	p.SingleCompactionDeltalogMaxNum = ParamItem{
		Key:          "dataCoord.compaction.single.deltalog.maxnum",
		Version:      "2.0.0",
		DefaultValue: "200",
	}
	p.SingleCompactionDeltalogMaxNum.Init(base.mgr)

	p.GlobalCompactionInterval = ParamItem{
		Key:          "dataCoord.compaction.global.interval",
		Version:      "2.0.0",
		DefaultValue: "60",
	}
	p.GlobalCompactionInterval.Init(base.mgr)

	p.EnableGarbageCollection = ParamItem{
		Key:          "dataCoord.enableGarbageCollection",
		Version:      "2.0.0",
		DefaultValue: "true",
	}
	p.EnableGarbageCollection.Init(base.mgr)

	p.GCInterval = ParamItem{
		Key:          "dataCoord.gc.interval",
		Version:      "2.0.0",
		DefaultValue: "3600",
	}
	p.GCInterval.Init(base.mgr)

	p.GCMissingTolerance = ParamItem{
		Key:          "dataCoord.gc.missingTolerance",
		Version:      "2.0.0",
		DefaultValue: "86400",
	}
	p.GCMissingTolerance.Init(base.mgr)

	p.GCDropTolerance = ParamItem{
		Key:          "dataCoord.gc.dropTolerance",
		Version:      "2.0.0",
		DefaultValue: "3600",
	}
	p.GCDropTolerance.Init(base.mgr)

	p.EnableActiveStandby = ParamItem{
		Key:          "dataCoord.enableActiveStandby",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.EnableActiveStandby.Init(base.mgr)

	p.MinSegmentNumRowsToEnableIndex = ParamItem{
		Key:          "indexCoord.segment.minSegmentNumRowsToEnableIndex",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.MinSegmentNumRowsToEnableIndex.Init(base.mgr)

	p.BindIndexNodeMode = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.enable",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.BindIndexNodeMode.Init(base.mgr)

	p.IndexNodeAddress = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.address",
		Version:      "2.0.0",
		DefaultValue: "localhost:22930",
	}
	p.IndexNodeAddress.Init(base.mgr)

	p.WithCredential = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.withCred",
		Version:      "2.0.0",
		DefaultValue: "false",
	}
	p.WithCredential.Init(base.mgr)

	p.IndexNodeID = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.nodeID",
		Version:      "2.0.0",
		DefaultValue: "0",
	}
	p.IndexNodeID.Init(base.mgr)
	p.IndexTaskSchedulerInterval = ParamItem{
		Key:          "indexCoord.scheduler.interval",
		Version:      "2.0.0",
		DefaultValue: "1000",
	}
	p.IndexTaskSchedulerInterval.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`

	// segment
	FlushInsertBufferSize  ParamItem `refreshable:"true"`
	FlushDeleteBufferBytes ParamItem `refreshable:"true"`
	BinLogMaxSize          ParamItem `refreshable:"true"`
	SyncPeriod             ParamItem `refreshable:"true"`

	// io concurrency to fetch stats logs
	IOConcurrency ParamItem `refreshable:"false"`
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.FlowGraphMaxQueueLength = ParamItem{
		Key:          "dataNode.dataSync.flowGraph.maxQueueLength",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.FlowGraphMaxQueueLength.Init(base.mgr)

	p.FlowGraphMaxParallelism = ParamItem{
		Key:          "dataNode.dataSync.flowGraph.maxParallelism",
		Version:      "2.0.0",
		DefaultValue: "1024",
	}
	p.FlowGraphMaxParallelism.Init(base.mgr)

	p.FlushInsertBufferSize = ParamItem{
		Key:          "DATA_NODE_IBUFSIZE",
		Version:      "2.0.0",
		FallbackKeys: []string{"datanode.segment.insertBufSize"},
		DefaultValue: "16777216",
		PanicIfEmpty: true,
	}
	p.FlushInsertBufferSize.Init(base.mgr)

	p.FlushDeleteBufferBytes = ParamItem{
		Key:          "datanode.segment.deleteBufBytes",
		Version:      "2.0.0",
		DefaultValue: "67108864",
	}
	p.FlushDeleteBufferBytes.Init(base.mgr)

	p.BinLogMaxSize = ParamItem{
		Key:          "datanode.segment.binlog.maxsize",
		Version:      "2.0.0",
		DefaultValue: "67108864",
	}
	p.BinLogMaxSize.Init(base.mgr)

	p.SyncPeriod = ParamItem{
		Key:          "datanode.segment.syncPeriod",
		Version:      "2.0.0",
		DefaultValue: "600",
	}
	p.SyncPeriod.Init(base.mgr)

	p.IOConcurrency = ParamItem{
		Key:          "dataNode.dataSync.ioConcurrency",
		Version:      "2.0.0",
		DefaultValue: "10",
	}
	p.IOConcurrency.Init(base.mgr)

}

// /////////////////////////////////////////////////////////////////////////////
// --- indexnode ---
type indexNodeConfig struct {
	BuildParallel ParamItem `refreshable:"false"`
	// enable disk
	EnableDisk             ParamItem `refreshable:"false"`
	DiskCapacityLimit      ParamItem `refreshable:"true"`
	MaxDiskUsagePercentage ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"false"`
}

func (p *indexNodeConfig) init(base *BaseTable) {
	p.BuildParallel = ParamItem{
		Key:          "indexNode.scheduler.buildParallel",
		Version:      "2.0.0",
		DefaultValue: "1",
	}
	p.BuildParallel.Init(base.mgr)

	p.EnableDisk = ParamItem{
		Key:          "indexNode.enableDisk",
		Version:      "2.2.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.EnableDisk.Init(base.mgr)

	p.DiskCapacityLimit = ParamItem{
		Key:     "LOCAL_STORAGE_SIZE",
		Version: "2.2.0",
		Formatter: func(v string) string {
			if len(v) == 0 {
				diskUsage, err := disk.Usage("/")
				if err != nil {
					panic(err)
				}
				return strconv.FormatUint(diskUsage.Total, 10)
			}
			diskSize := getAsInt64(v)
			return strconv.FormatInt(diskSize*1024*1024*1024, 10)
		},
	}
	p.DiskCapacityLimit.Init(base.mgr)

	p.MaxDiskUsagePercentage = ParamItem{
		Key:          "indexNode.maxDiskUsagePercentage",
		Version:      "2.2.0",
		DefaultValue: "95",
		PanicIfEmpty: true,
		Formatter: func(v string) string {
			return fmt.Sprintf("%f", getAsFloat(v)/100)
		},
	}
	p.MaxDiskUsagePercentage.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "indexNode.gracefulStopTimeout",
		Version:      "2.2.1",
		FallbackKeys: []string{"common.gracefulStopTimeout"},
	}
	p.GracefulStopTimeout.Init(base.mgr)
}

type integrationTestConfig struct {
	IntegrationMode ParamItem `refreshable:"false"`
}

func (p *integrationTestConfig) init(base *BaseTable) {
	p.IntegrationMode = ParamItem{
		Key:          "integration.test.mode",
		Version:      "2.2.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.IntegrationMode.Init(base.mgr)
}
