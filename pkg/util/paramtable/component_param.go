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
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

const (
	// DefaultRetentionDuration defines the default duration for retention which is 1 days in seconds.
	DefaultRetentionDuration = 0

	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize        = 16
	DefaultGracefulTime          = 5000 // ms
	DefaultGracefulStopTimeout   = 30   // s
	DefaultThreadCoreCoefficient = 10

	DefaultSessionTTL        = 20 // s
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
	LogCfg        logConfig
	HookCfg       hookConfig

	RootCoordGrpcServerCfg  GrpcServerConfig
	ProxyGrpcServerCfg      GrpcServerConfig
	QueryCoordGrpcServerCfg GrpcServerConfig
	QueryNodeGrpcServerCfg  GrpcServerConfig
	DataCoordGrpcServerCfg  GrpcServerConfig
	DataNodeGrpcServerCfg   GrpcServerConfig
	IndexNodeGrpcServerCfg  GrpcServerConfig

	RootCoordGrpcClientCfg  GrpcClientConfig
	ProxyGrpcClientCfg      GrpcClientConfig
	QueryCoordGrpcClientCfg GrpcClientConfig
	QueryNodeGrpcClientCfg  GrpcClientConfig
	DataCoordGrpcClientCfg  GrpcClientConfig
	DataNodeGrpcClientCfg   GrpcClientConfig
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
	p.LogCfg.init(&p.BaseTable)
	p.HookCfg.init(&p.BaseTable)

	p.RootCoordGrpcServerCfg.Init("rootCoord", &p.BaseTable)
	p.ProxyGrpcServerCfg.Init("proxy", &p.BaseTable)
	p.ProxyGrpcServerCfg.InternalPort.Export = true
	p.QueryCoordGrpcServerCfg.Init("queryCoord", &p.BaseTable)
	p.QueryNodeGrpcServerCfg.Init("queryNode", &p.BaseTable)
	p.DataCoordGrpcServerCfg.Init("dataCoord", &p.BaseTable)
	p.DataNodeGrpcServerCfg.Init("dataNode", &p.BaseTable)
	p.IndexNodeGrpcServerCfg.Init("indexNode", &p.BaseTable)

	p.RootCoordGrpcClientCfg.Init("rootCoord", &p.BaseTable)
	p.ProxyGrpcClientCfg.Init("proxy", &p.BaseTable)
	p.QueryCoordGrpcClientCfg.Init("queryCoord", &p.BaseTable)
	p.QueryNodeGrpcClientCfg.Init("queryNode", &p.BaseTable)
	p.DataCoordGrpcClientCfg.Init("dataCoord", &p.BaseTable)
	p.DataNodeGrpcClientCfg.Init("dataNode", &p.BaseTable)
	p.IndexNodeGrpcClientCfg.Init("indexNode", &p.BaseTable)

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
	DataCoordStatistic ParamItem `refreshable:"true"`
	// Deprecated
	DataCoordTimeTick     ParamItem `refreshable:"false"`
	DataCoordSegmentInfo  ParamItem `refreshable:"true"`
	DataCoordSubName      ParamItem `refreshable:"false"`
	DataCoordWatchSubPath ParamItem `refreshable:"false"`
	DataCoordTicklePath   ParamItem `refreshable:"false"`
	DataNodeSubName       ParamItem `refreshable:"false"`

	DefaultPartitionName ParamItem `refreshable:"false"`
	DefaultIndexName     ParamItem `refreshable:"true"`
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

	StorageType ParamItem `refreshable:"false"`
	SimdType    ParamItem `refreshable:"false"`

	AuthorizationEnabled ParamItem `refreshable:"false"`
	SuperUsers           ParamItem `refreshable:"true"`

	ClusterName ParamItem `refreshable:"false"`

	SessionTTL        ParamItem `refreshable:"false"`
	SessionRetryTimes ParamItem `refreshable:"false"`

	PreCreatedTopicEnabled ParamItem `refreshable:"true"`
	TopicNames             ParamItem `refreshable:"true"`
	TimeTicker             ParamItem `refreshable:"true"`

	JSONMaxLength ParamItem `refreshable:"false"`

	ImportMaxFileSize ParamItem `refreshable:"true"`
}

func (p *commonConfig) init(base *BaseTable) {
	// must init cluster prefix first
	p.ClusterPrefix = ParamItem{
		Key:          "msgChannel.chanNamePrefix.cluster",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.cluster"},
		DefaultValue: "by-dev",
		PanicIfEmpty: true,
		Forbidden:    true,
		Export:       true,
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
		Export:       true,
	}
	p.ProxySubName.Init(base.mgr)

	// --- rootcoord ---
	p.RootCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.RootCoordTimeTick.Init(base.mgr)

	p.RootCoordStatistics = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordStatistics",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordStatistics"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.RootCoordStatistics.Init(base.mgr)

	p.RootCoordDml = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordDml",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordDml"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.RootCoordDml.Init(base.mgr)

	p.RootCoordDelta = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordDelta",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordDelta"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.RootCoordDelta.Init(base.mgr)

	p.RootCoordSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.rootCoordSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.rootCoordSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.RootCoordSubName.Init(base.mgr)

	p.QueryCoordSearch = ParamItem{
		Key:          "msgChannel.chanNamePrefix.search",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.search"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.QueryCoordSearch.Init(base.mgr)

	p.QueryCoordSearchResult = ParamItem{
		Key:          "msgChannel.chanNamePrefix.searchResult",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.searchResult"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.QueryCoordSearchResult.Init(base.mgr)

	p.QueryCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.queryTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.queryTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.QueryCoordTimeTick.Init(base.mgr)

	p.QueryNodeSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.queryNodeSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.queryNodeSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.QueryNodeSubName.Init(base.mgr)

	p.DataCoordStatistic = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordStatistic",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordStatistic"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.DataCoordStatistic.Init(base.mgr)

	p.DataCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.DataCoordTimeTick.Init(base.mgr)

	p.DataCoordSegmentInfo = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordSegmentInfo",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordSegmentInfo"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.DataCoordSegmentInfo.Init(base.mgr)

	p.DataCoordSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataCoordSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.DataCoordSubName.Init(base.mgr)

	p.DataCoordWatchSubPath = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordWatchSubPath",
		Version:      "2.1.0",
		DefaultValue: "channelwatch",
		PanicIfEmpty: true,
	}
	p.DataCoordWatchSubPath.Init(base.mgr)

	p.DataCoordTicklePath = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordWatchSubPath",
		Version:      "2.2.3",
		DefaultValue: "tickle",
		PanicIfEmpty: true,
	}
	p.DataCoordTicklePath.Init(base.mgr)

	p.DataNodeSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataNodeSubNamePrefix",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataNodeSubNamePrefix"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.DataNodeSubName.Init(base.mgr)

	p.DefaultPartitionName = ParamItem{
		Key:          "common.defaultPartitionName",
		Version:      "2.0.0",
		DefaultValue: "_default",
		Forbidden:    true,
		Doc:          "default partition name for a collection",
		Export:       true,
	}
	p.DefaultPartitionName.Init(base.mgr)

	p.DefaultIndexName = ParamItem{
		Key:          "common.defaultIndexName",
		Version:      "2.0.0",
		DefaultValue: "_default_idx",
		Doc:          "default index name",
		Export:       true,
	}
	p.DefaultIndexName.Init(base.mgr)

	p.RetentionDuration = ParamItem{
		Key:          "common.retentionDuration",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultRetentionDuration),
		Doc:          "time travel reserved time, insert/delete will not be cleaned in this period. disable it by default",
		Export:       true,
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
		Doc:    "Entity expiration in seconds, CAUTION make sure entityExpiration >= retentionDuration and -1 means never expire",
		Export: true,
	}
	p.EntityExpirationTTL.Init(base.mgr)

	p.SimdType = ParamItem{
		Key:          "common.simdType",
		Version:      "2.1.0",
		DefaultValue: "auto",
		FallbackKeys: []string{"knowhere.simdType"},
		Doc: `Default value: auto
Valid values: [auto, avx512, avx2, avx, sse4_2]
This configuration is only used by querynode and indexnode, it selects CPU instruction set for Searching and Index-building.`,
		Export: true,
	}
	p.SimdType.Init(base.mgr)

	p.IndexSliceSize = ParamItem{
		Key:          "common.indexSliceSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultIndexSliceSize),
		Doc:          "MB",
		Export:       true,
	}
	p.IndexSliceSize.Init(base.mgr)

	p.MaxDegree = ParamItem{
		Key:          "common.DiskIndex.MaxDegree",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultMaxDegree),
		Export:       true,
	}
	p.MaxDegree.Init(base.mgr)

	p.SearchListSize = ParamItem{
		Key:          "common.DiskIndex.SearchListSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultSearchListSize),
		Export:       true,
	}
	p.SearchListSize.Init(base.mgr)

	p.PQCodeBudgetGBRatio = ParamItem{
		Key:          "common.DiskIndex.PQCodeBudgetGBRatio",
		Version:      "2.0.0",
		DefaultValue: fmt.Sprintf("%f", DefaultPQCodeBudgetGBRatio),
		Export:       true,
	}
	p.PQCodeBudgetGBRatio.Init(base.mgr)

	p.BuildNumThreadsRatio = ParamItem{
		Key:          "common.DiskIndex.BuildNumThreadsRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultBuildNumThreadsRatio),
		Export:       true,
	}
	p.BuildNumThreadsRatio.Init(base.mgr)

	p.SearchCacheBudgetGBRatio = ParamItem{
		Key:          "common.DiskIndex.SearchCacheBudgetGBRatio",
		Version:      "2.0.0",
		DefaultValue: fmt.Sprintf("%f", DefaultSearchCacheBudgetGBRatio),
		Export:       true,
	}
	p.SearchCacheBudgetGBRatio.Init(base.mgr)

	p.LoadNumThreadRatio = ParamItem{
		Key:          "common.DiskIndex.LoadNumThreadRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultLoadNumThreadRatio),
		Export:       true,
	}
	p.LoadNumThreadRatio.Init(base.mgr)

	p.BeamWidthRatio = ParamItem{
		Key:          "common.DiskIndex.BeamWidthRatio",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultBeamWidthRatio),
		Doc:          "",
		Export:       true,
	}
	p.BeamWidthRatio.Init(base.mgr)

	p.GracefulTime = ParamItem{
		Key:          "common.gracefulTime",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultGracefulTime),
		Doc:          "milliseconds. it represents the interval (in ms) by which the request arrival time needs to be subtracted in the case of Bounded Consistency.",
		Export:       true,
	}
	p.GracefulTime.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "common.gracefulStopTimeout",
		Version:      "2.2.1",
		DefaultValue: "30",
		Doc:          "seconds. it will force quit the server if the graceful stop process is not completed during this time.",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.StorageType = ParamItem{
		Key:          "common.storageType",
		Version:      "2.0.0",
		DefaultValue: "minio",
		Doc:          "please adjust in embedded Milvus: local",
		Export:       true,
	}
	p.StorageType.Init(base.mgr)

	p.ThreadCoreCoefficient = ParamItem{
		Key:          "common.threadCoreCoefficient",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultThreadCoreCoefficient),
		Doc:          "This parameter specify how many times the number of threads is the number of cores",
		Export:       true,
	}
	p.ThreadCoreCoefficient.Init(base.mgr)

	p.AuthorizationEnabled = ParamItem{
		Key:          "common.security.authorizationEnabled",
		Version:      "2.0.0",
		DefaultValue: "false",
		Export:       true,
	}
	p.AuthorizationEnabled.Init(base.mgr)

	p.SuperUsers = ParamItem{
		Key:     "common.security.superUsers",
		Version: "2.2.1",
		Doc: `The superusers will ignore some system check processes,
like the old password verification when updating the credential`,
		DefaultValue: "",
		Export:       true,
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
		DefaultValue: "20",
		Doc:          "ttl value when session granting a lease to register service",
		Export:       true,
	}
	p.SessionTTL.Init(base.mgr)

	p.SessionRetryTimes = ParamItem{
		Key:          "common.session.retryTimes",
		Version:      "2.0.0",
		DefaultValue: "30",
		Doc:          "retry times when session sending etcd requests",
		Export:       true,
	}
	p.SessionRetryTimes.Init(base.mgr)

	p.PreCreatedTopicEnabled = ParamItem{
		Key:          "common.preCreatedTopic.enabled",
		Version:      "2.3.0",
		DefaultValue: "false",
	}
	p.PreCreatedTopicEnabled.Init(base.mgr)

	p.TopicNames = ParamItem{
		Key:     "common.preCreatedTopic.names",
		Version: "2.3.0",
	}
	p.TopicNames.Init(base.mgr)

	p.TimeTicker = ParamItem{
		Key:     "common.preCreatedTopic.timeticker",
		Version: "2.3.0",
	}
	p.TimeTicker.Init(base.mgr)

	p.JSONMaxLength = ParamItem{
		Key:          "common.JSONMaxLength",
		Version:      "2.2.9",
		DefaultValue: fmt.Sprint(64 << 10),
	}
	p.JSONMaxLength.Init(base.mgr)

	p.ImportMaxFileSize = ParamItem{
		Key:          "common.ImportMaxFileSize",
		Version:      "2.2.9",
		DefaultValue: fmt.Sprint(16 << 30),
	}
	p.ImportMaxFileSize.Init(base.mgr)
}

type traceConfig struct {
	Exporter       ParamItem `refreshable:"false"`
	SampleFraction ParamItem `refreshable:"false"`
	JaegerURL      ParamItem `refreshable:"false"`
	OtlpEndpoint   ParamItem `refreshable:"false"`
}

func (t *traceConfig) init(base *BaseTable) {
	t.Exporter = ParamItem{
		Key:     "trace.exporter",
		Version: "2.3.0",
		Doc: `trace exporter type, default is stdout,
optional values: ['stdout', 'jaeger']`,
		Export: true,
	}
	t.Exporter.Init(base.mgr)

	t.SampleFraction = ParamItem{
		Key:          "trace.sampleFraction",
		Version:      "2.3.0",
		DefaultValue: "0",
		Doc: `fraction of traceID based sampler,
optional values: [0, 1]
Fractions >= 1 will always sample. Fractions < 0 are treated as zero.`,
		Export: true,
	}
	t.SampleFraction.Init(base.mgr)

	t.JaegerURL = ParamItem{
		Key:     "trace.jaeger.url",
		Version: "2.3.0",
		Doc:     "when exporter is jaeger should set the jaeger's URL",
		Export:  true,
	}
	t.JaegerURL.Init(base.mgr)

	t.OtlpEndpoint = ParamItem{
		Key:     "trace.otlp.endpoint",
		Version: "2.3.0",
	}
	t.OtlpEndpoint.Init(base.mgr)
}

type logConfig struct {
	Level        ParamItem `refreshable:"false"`
	RootPath     ParamItem `refreshable:"false"`
	MaxSize      ParamItem `refreshable:"false"`
	MaxAge       ParamItem `refreshable:"false"`
	MaxBackups   ParamItem `refreshable:"false"`
	Format       ParamItem `refreshable:"false"`
	Stdout       ParamItem `refreshable:"false"`
	GrpcLogLevel ParamItem `refreshable:"false"`
}

func (l *logConfig) init(base *BaseTable) {
	l.Level = ParamItem{
		Key:          "log.level",
		DefaultValue: "info",
		Version:      "2.0.0",
		Doc:          "Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
		Export:       true,
	}
	l.Level.Init(base.mgr)

	l.RootPath = ParamItem{
		Key:     "log.file.rootPath",
		Version: "2.0.0",
		Doc:     "root dir path to put logs, default \"\" means no log file will print. please adjust in embedded Milvus: /tmp/milvus/logs",
		Export:  true,
	}
	l.RootPath.Init(base.mgr)

	l.MaxSize = ParamItem{
		Key:          "log.file.maxSize",
		DefaultValue: "300",
		Version:      "2.0.0",
		Doc:          "MB",
		Export:       true,
	}
	l.MaxSize.Init(base.mgr)

	l.MaxAge = ParamItem{
		Key:          "log.file.maxAge",
		DefaultValue: "10",
		Version:      "2.0.0",
		Doc:          "Maximum time for log retention in day.",
		Export:       true,
	}
	l.MaxAge.Init(base.mgr)

	l.MaxBackups = ParamItem{
		Key:          "log.file.maxBackups",
		DefaultValue: "20",
		Version:      "2.0.0",
		Export:       true,
	}
	l.MaxBackups.Init(base.mgr)

	l.Format = ParamItem{
		Key:          "log.format",
		DefaultValue: "text",
		Version:      "2.0.0",
		Doc:          "text or json",
		Export:       true,
	}
	l.Format.Init(base.mgr)

	l.Stdout = ParamItem{
		Key:          "log.stdout",
		DefaultValue: "true",
		Version:      "2.3.0",
		Doc:          "Stdout enable or not",
		Export:       true,
	}
	l.Stdout.Init(base.mgr)

	l.GrpcLogLevel = ParamItem{
		Key:          "grpc.log.level",
		DefaultValue: "WARNING",
		Version:      "2.0.0",
		Export:       true,
	}
	l.GrpcLogLevel.Init(base.mgr)
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
	MaxDatabaseNum              ParamItem `refreshable:"false"`
}

func (p *rootCoordConfig) init(base *BaseTable) {
	p.DmlChannelNum = ParamItem{
		Key:          "rootCoord.dmlChannelNum",
		Version:      "2.0.0",
		DefaultValue: "16",
		Forbidden:    true,
		Doc:          "The number of dml channels created at system startup",
		Export:       true,
	}
	p.DmlChannelNum.Init(base.mgr)

	p.MaxPartitionNum = ParamItem{
		Key:          "rootCoord.maxPartitionNum",
		Version:      "2.0.0",
		DefaultValue: "4096",
		Doc:          "Maximum number of partitions in a collection",
		Export:       true,
	}
	p.MaxPartitionNum.Init(base.mgr)

	p.MinSegmentSizeToEnableIndex = ParamItem{
		Key:          "rootCoord.minSegmentSizeToEnableIndex",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "It's a threshold. When the segment size is less than this value, the segment will not be indexed",
		Export:       true,
	}
	p.MinSegmentSizeToEnableIndex.Init(base.mgr)

	p.ImportTaskExpiration = ParamItem{
		Key:          "rootCoord.importTaskExpiration",
		Version:      "2.2.0",
		DefaultValue: "900", // 15 * 60 seconds
		Doc:          "(in seconds) Duration after which an import task will expire (be killed). Default 900 seconds (15 minutes).",
		Export:       true,
	}
	p.ImportTaskExpiration.Init(base.mgr)

	p.ImportTaskRetention = ParamItem{
		Key:          "rootCoord.importTaskRetention",
		Version:      "2.2.0",
		DefaultValue: strconv.Itoa(24 * 60 * 60),
		Doc:          "(in seconds) Milvus will keep the record of import tasks for at least `importTaskRetention` seconds. Default 86400, seconds (24 hours).",
		Export:       true,
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
		Export:       true,
	}
	p.EnableActiveStandby.Init(base.mgr)

	p.MaxDatabaseNum = ParamItem{
		Key:          "rootCoord.maxDatabaseNum",
		Version:      "2.3.0",
		DefaultValue: "64",
		Doc:          "Maximum number of database",
		Export:       true,
	}
	p.MaxDatabaseNum.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type AccessLogConfig struct {
	Enable        ParamItem `refreshable:"false"`
	MinioEnable   ParamItem `refreshable:"false"`
	LocalPath     ParamItem `refreshable:"false"`
	Filename      ParamItem `refreshable:"false"`
	MaxSize       ParamItem `refreshable:"false"`
	RotatedTime   ParamItem `refreshable:"false"`
	MaxBackups    ParamItem `refreshable:"false"`
	RemotePath    ParamItem `refreshable:"false"`
	RemoteMaxTime ParamItem `refreshable:"false"`
}

type proxyConfig struct {
	// Alias  string
	SoPath ParamItem `refreshable:"false"`

	TimeTickInterval         ParamItem `refreshable:"false"`
	HealthCheckTimetout      ParamItem `refreshable:"true"`
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
	ShardLeaderCacheInterval ParamItem `refreshable:"false"`
	ReplicaSelectionPolicy   ParamItem `refreshable:"false"`
}

func (p *proxyConfig) init(base *BaseTable) {
	p.TimeTickInterval = ParamItem{
		Key:          "proxy.timeTickInterval",
		Version:      "2.2.0",
		DefaultValue: "200",
		PanicIfEmpty: true,
		Doc:          "ms, the interval that proxy synchronize the time tick",
		Export:       true,
	}
	p.TimeTickInterval.Init(base.mgr)

	p.HealthCheckTimetout = ParamItem{
		Key:          "proxy.healthCheckTimetout",
		Version:      "2.3.0",
		DefaultValue: "500",
		PanicIfEmpty: true,
		Doc:          "ms, the interval that to do component healthy check",
		Export:       true,
	}
	p.HealthCheckTimetout.Init(base.mgr)

	p.MsgStreamTimeTickBufSize = ParamItem{
		Key:          "proxy.msgStream.timeTick.bufSize",
		Version:      "2.2.0",
		DefaultValue: "512",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.MsgStreamTimeTickBufSize.Init(base.mgr)

	p.MaxNameLength = ParamItem{
		Key:          "proxy.maxNameLength",
		DefaultValue: "255",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "Maximum length of name for a collection or alias",
		Export:       true,
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
		Doc: `Maximum number of fields in a collection.
As of today (2.2.0 and after) it is strongly DISCOURAGED to set maxFieldNum >= 64.
So adjust at your risk!`,
		Export: true,
	}
	p.MaxFieldNum.Init(base.mgr)

	p.MaxShardNum = ParamItem{
		Key:          "proxy.maxShardNum",
		DefaultValue: "16",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "Maximum number of shards in a collection",
		Export:       true,
	}
	p.MaxShardNum.Init(base.mgr)

	p.MaxDimension = ParamItem{
		Key:          "proxy.maxDimension",
		DefaultValue: "32768",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "Maximum dimension of a vector",
		Export:       true,
	}
	p.MaxDimension.Init(base.mgr)

	p.MaxTaskNum = ParamItem{
		Key:          "proxy.maxTaskNum",
		Version:      "2.2.0",
		DefaultValue: "1024",
		Doc:          "max task number of proxy task queue",
		Export:       true,
	}
	p.MaxTaskNum.Init(base.mgr)

	p.GinLogging = ParamItem{
		Key:          "proxy.ginLogging",
		Version:      "2.2.0",
		DefaultValue: "true",
		Doc: `Whether to produce gin logs.\n
please adjust in embedded Milvus: false`,
		Export: true,
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
		Doc:          "if use access log",
	}
	p.AccessLog.Enable.Init(base.mgr)

	p.AccessLog.MinioEnable = ParamItem{
		Key:          "proxy.accessLog.minioEnable",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc:          "if upload sealed access log file to minio",
	}
	p.AccessLog.MinioEnable.Init(base.mgr)

	p.AccessLog.LocalPath = ParamItem{
		Key:     "proxy.accessLog.localPath",
		Version: "2.2.0",
		Export:  true,
	}
	p.AccessLog.LocalPath.Init(base.mgr)

	p.AccessLog.Filename = ParamItem{
		Key:          "proxy.accessLog.filename",
		Version:      "2.2.0",
		DefaultValue: "milvus_access_log.log",
		Doc:          "Log filename, leave empty to disable file log.",
		Export:       true,
	}
	p.AccessLog.Filename.Init(base.mgr)

	p.AccessLog.MaxSize = ParamItem{
		Key:          "proxy.accessLog.maxSize",
		Version:      "2.2.0",
		DefaultValue: "64",
		Doc:          "Max size for a single file, in MB.",
	}
	p.AccessLog.MaxSize.Init(base.mgr)

	p.AccessLog.MaxBackups = ParamItem{
		Key:          "proxy.accessLog.maxBackups",
		Version:      "2.2.0",
		DefaultValue: "8",
		Doc:          "Maximum number of old log files to retain.",
	}
	p.AccessLog.MaxBackups.Init(base.mgr)

	p.AccessLog.RotatedTime = ParamItem{
		Key:          "proxy.accessLog.rotatedTime",
		Version:      "2.2.0",
		DefaultValue: "3600",
		Doc:          "Max time for single access log file in seconds",
	}
	p.AccessLog.RotatedTime.Init(base.mgr)

	p.AccessLog.RemotePath = ParamItem{
		Key:          "proxy.accessLog.remotePath",
		Version:      "2.2.0",
		DefaultValue: "access_log/",
		Doc:          "File path in minIO",
	}
	p.AccessLog.RemotePath.Init(base.mgr)

	p.AccessLog.RemoteMaxTime = ParamItem{
		Key:          "proxy.accessLog.remoteMaxTime",
		Version:      "2.2.0",
		DefaultValue: "168",
		Doc:          "Max time for log file in minIO, in hours",
	}
	p.AccessLog.RemoteMaxTime.Init(base.mgr)

	p.ShardLeaderCacheInterval = ParamItem{
		Key:          "proxy.shardLeaderCacheInterval",
		Version:      "2.2.4",
		DefaultValue: "30",
		Doc:          "time interval to update shard leader cache, in seconds",
	}
	p.ShardLeaderCacheInterval.Init(base.mgr)

	p.ReplicaSelectionPolicy = ParamItem{
		Key:          "proxy.replicaSelectionPolicy",
		Version:      "2.3.0",
		DefaultValue: "look_aside",
		Doc:          "replica selection policy in multiple replicas load balancing, support round_robin and look_aside",
	}
	p.ReplicaSelectionPolicy.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	// Deprecated: Since 2.2.0
	RetryNum ParamItem `refreshable:"true"`
	// Deprecated: Since 2.2.0
	RetryInterval    ParamItem `refreshable:"true"`
	TaskMergeCap     ParamItem `refreshable:"false"`
	TaskExecutionCap ParamItem `refreshable:"true"`

	//---- Handoff ---
	//Deprecated: Since 2.2.2
	AutoHandoff ParamItem `refreshable:"true"`

	//---- Balance ---
	AutoBalance                         ParamItem `refreshable:"true"`
	Balancer                            ParamItem `refreshable:"true"`
	GlobalRowCountFactor                ParamItem `refreshable:"true"`
	ScoreUnbalanceTolerationFactor      ParamItem `refreshable:"true"`
	ReverseUnbalanceTolerationFactor    ParamItem `refreshable:"true"`
	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"true"`
	BalanceIntervalSeconds              ParamItem `refreshable:"true"`
	MemoryUsageMaxDifferencePercentage  ParamItem `refreshable:"true"`
	CheckInterval                       ParamItem `refreshable:"true"`
	ChannelTaskTimeout                  ParamItem `refreshable:"true"`
	SegmentTaskTimeout                  ParamItem `refreshable:"true"`
	DistPullInterval                    ParamItem `refreshable:"false"`
	HeartbeatAvailableInterval          ParamItem `refreshable:"true"`
	LoadTimeoutSeconds                  ParamItem `refreshable:"true"`
	// Deprecated: Since 2.2.2, QueryCoord do not use HandOff logic anymore
	CheckHandoffInterval ParamItem `refreshable:"true"`
	EnableActiveStandby  ParamItem `refreshable:"false"`

	NextTargetSurviveTime      ParamItem `refreshable:"true"`
	UpdateNextTargetInterval   ParamItem `refreshable:"false"`
	CheckNodeInReplicaInterval ParamItem `refreshable:"false"`
	CheckResourceGroupInterval ParamItem `refreshable:"false"`
	EnableRGAutoRecover        ParamItem `refreshable:"true"`
	CheckHealthInterval        ParamItem `refreshable:"false"`
	CheckHealthRPCTimeout      ParamItem `refreshable:"true"`
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
		Export:       true,
	}
	p.TaskMergeCap.Init(base.mgr)

	p.TaskExecutionCap = ParamItem{
		Key:          "queryCoord.taskExecutionCap",
		Version:      "2.2.0",
		DefaultValue: "256",
		Export:       true,
	}
	p.TaskExecutionCap.Init(base.mgr)

	p.AutoHandoff = ParamItem{
		Key:          "queryCoord.autoHandoff",
		Version:      "2.0.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
		Doc:          "Enable auto handoff",
		Export:       true,
	}
	p.AutoHandoff.Init(base.mgr)

	p.AutoBalance = ParamItem{
		Key:          "queryCoord.autoBalance",
		Version:      "2.0.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
		Doc:          "Enable auto balance",
		Export:       true,
	}
	p.AutoBalance.Init(base.mgr)

	p.Balancer = ParamItem{
		Key:          "queryCoord.balancer",
		Version:      "2.0.0",
		DefaultValue: "ScoreBasedBalancer",
		PanicIfEmpty: false,
		Doc:          "auto balancer used for segments on queryNodes",
		Export:       true,
	}
	p.Balancer.Init(base.mgr)

	p.GlobalRowCountFactor = ParamItem{
		Key:          "queryCoord.globalRowCountFactor",
		Version:      "2.0.0",
		DefaultValue: "0.1",
		PanicIfEmpty: true,
		Doc:          "the weight used when balancing segments among queryNodes",
		Export:       true,
	}
	p.GlobalRowCountFactor.Init(base.mgr)

	p.ScoreUnbalanceTolerationFactor = ParamItem{
		Key:          "queryCoord.scoreUnbalanceTolerationFactor",
		Version:      "2.0.0",
		DefaultValue: "0.05",
		PanicIfEmpty: true,
		Doc:          "the least value for unbalanced extent between from and to nodes when doing balance",
		Export:       true,
	}
	p.ScoreUnbalanceTolerationFactor.Init(base.mgr)

	p.ReverseUnbalanceTolerationFactor = ParamItem{
		Key:          "queryCoord.reverseUnBalanceTolerationFactor",
		Version:      "2.0.0",
		DefaultValue: "1.3",
		PanicIfEmpty: true,
		Doc:          "the largest value for unbalanced extent between from and to nodes after doing balance",
		Export:       true,
	}
	p.ReverseUnbalanceTolerationFactor.Init(base.mgr)

	p.OverloadedMemoryThresholdPercentage = ParamItem{
		Key:          "queryCoord.overloadedMemoryThresholdPercentage",
		Version:      "2.0.0",
		DefaultValue: "90",
		PanicIfEmpty: true,
		Doc:          "The threshold percentage that memory overload",
		Export:       true,
	}
	p.OverloadedMemoryThresholdPercentage.Init(base.mgr)

	p.BalanceIntervalSeconds = ParamItem{
		Key:          "queryCoord.balanceIntervalSeconds",
		Version:      "2.0.0",
		DefaultValue: "60",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.BalanceIntervalSeconds.Init(base.mgr)

	p.MemoryUsageMaxDifferencePercentage = ParamItem{
		Key:          "queryCoord.memoryUsageMaxDifferencePercentage",
		Version:      "2.0.0",
		DefaultValue: "30",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.MemoryUsageMaxDifferencePercentage.Init(base.mgr)

	p.CheckInterval = ParamItem{
		Key:          "queryCoord.checkInterval",
		Version:      "2.0.0",
		DefaultValue: "10000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.CheckInterval.Init(base.mgr)

	p.ChannelTaskTimeout = ParamItem{
		Key:          "queryCoord.channelTaskTimeout",
		Version:      "2.0.0",
		DefaultValue: "60000",
		PanicIfEmpty: true,
		Doc:          "1 minute",
		Export:       true,
	}
	p.ChannelTaskTimeout.Init(base.mgr)

	p.SegmentTaskTimeout = ParamItem{
		Key:          "queryCoord.segmentTaskTimeout",
		Version:      "2.0.0",
		DefaultValue: "120000",
		PanicIfEmpty: true,
		Doc:          "2 minute",
		Export:       true,
	}
	p.SegmentTaskTimeout.Init(base.mgr)

	p.DistPullInterval = ParamItem{
		Key:          "queryCoord.distPullInterval",
		Version:      "2.0.0",
		DefaultValue: "500",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.DistPullInterval.Init(base.mgr)

	p.LoadTimeoutSeconds = ParamItem{
		Key:          "queryCoord.loadTimeoutSeconds",
		Version:      "2.0.0",
		DefaultValue: "600",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.LoadTimeoutSeconds.Init(base.mgr)

	p.HeartbeatAvailableInterval = ParamItem{
		Key:          "queryCoord.heartbeatAvailableInterval",
		Version:      "2.2.1",
		DefaultValue: "10000",
		PanicIfEmpty: true,
		Doc:          "10s, Only QueryNodes which fetched heartbeats within the duration are available",
		Export:       true,
	}
	p.HeartbeatAvailableInterval.Init(base.mgr)

	p.CheckHandoffInterval = ParamItem{
		Key:          "queryCoord.checkHandoffInterval",
		DefaultValue: "5000",
		Version:      "2.2.0",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.CheckHandoffInterval.Init(base.mgr)

	p.EnableActiveStandby = ParamItem{
		Key:          "queryCoord.enableActiveStandby",
		Version:      "2.2.0",
		DefaultValue: "false",
		Export:       true,
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

	p.CheckHealthInterval = ParamItem{
		Key:          "queryCoord.checkHealthInterval",
		Version:      "2.2.7",
		DefaultValue: "3000",
		PanicIfEmpty: true,
		Doc:          "3s, the interval when query coord try to check health of query node",
		Export:       true,
	}
	p.CheckHealthInterval.Init(base.mgr)

	p.CheckHealthRPCTimeout = ParamItem{
		Key:          "queryCoord.checkHealthRPCTimeout",
		Version:      "2.2.7",
		DefaultValue: "100",
		PanicIfEmpty: true,
		Doc:          "100ms, the timeout of check health rpc to query node",
		Export:       true,
	}
	p.CheckHealthRPCTimeout.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	SoPath ParamItem `refreshable:"false"`

	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`

	// stats
	// Deprecated: Never used
	StatsPublishInterval ParamItem `refreshable:"true"`

	// segcore
	KnowhereThreadPoolSize    ParamItem `refreshable:"false"`
	ChunkRows                 ParamItem `refreshable:"false"`
	EnableGrowingSegmentIndex ParamItem `refreshable:"false"`
	GrowingIndexNlist         ParamItem `refreshable:"false"`
	GrowingIndexNProbe        ParamItem `refreshable:"false"`

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
	MmapDirPath      ParamItem `refreshable:"false"`

	GroupEnabled         ParamItem `refreshable:"true"`
	MaxReceiveChanSize   ParamItem `refreshable:"false"`
	MaxUnsolvedQueueSize ParamItem `refreshable:"true"`
	MaxReadConcurrency   ParamItem `refreshable:"true"`
	MaxGroupNQ           ParamItem `refreshable:"true"`
	TopKMergeRatio       ParamItem `refreshable:"true"`
	CPURatio             ParamItem `refreshable:"true"`
	MaxTimestampLag      ParamItem `refreshable:"true"`
	GCEnabled            ParamItem `refreshable:"true"`

	GCHelperEnabled     ParamItem `refreshable:"false"`
	MinimumGOGCConfig   ParamItem `refreshable:"false"`
	MaximumGOGCConfig   ParamItem `refreshable:"false"`
	GracefulStopTimeout ParamItem `refreshable:"false"`

	// delete buffer
	MaxSegmentDeleteBuffer ParamItem `refreshable:"false"`

	// loader
	IoPoolSize ParamItem `refreshable:"false"`
}

func (p *queryNodeConfig) init(base *BaseTable) {
	p.SoPath = ParamItem{
		Key:          "queryNode.soPath",
		Version:      "2.3.0",
		DefaultValue: "",
	}
	p.SoPath.Init(base.mgr)

	p.FlowGraphMaxQueueLength = ParamItem{
		Key:          "queryNode.dataSync.flowGraph.maxQueueLength",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "Maximum length of task queue in flowgraph",
		Export:       true,
	}
	p.FlowGraphMaxQueueLength.Init(base.mgr)

	p.FlowGraphMaxParallelism = ParamItem{
		Key:          "queryNode.dataSync.flowGraph.maxParallelism",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "Maximum number of tasks executed in parallel in the flowgraph",
		Export:       true,
	}
	p.FlowGraphMaxParallelism.Init(base.mgr)

	p.StatsPublishInterval = ParamItem{
		Key:          "queryNode.stats.publishInterval",
		Version:      "2.0.0",
		DefaultValue: "1000",
		Doc:          "Interval for querynode to report node information (milliseconds)",
		Export:       true,
	}
	p.StatsPublishInterval.Init(base.mgr)

	p.KnowhereThreadPoolSize = ParamItem{
		Key:          "queryNode.segcore.knowhereThreadPoolNumRatio",
		Version:      "2.0.0",
		DefaultValue: "4",
		Formatter: func(v string) string {
			factor := getAsInt64(v)
			if factor <= 0 || !p.EnableDisk.GetAsBool() {
				factor = 1
			} else if factor > 32 {
				factor = 32
			}
			knowhereThreadPoolSize := uint32(runtime.GOMAXPROCS(0)) * uint32(factor)
			return strconv.FormatUint(uint64(knowhereThreadPoolSize), 10)
		},
		Doc:    "The number of threads in knowhere's thread pool. If disk is enabled, the pool size will multiply with knowhereThreadPoolNumRatio([1, 32]).",
		Export: true,
	}
	p.KnowhereThreadPoolSize.Init(base.mgr)

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
		Doc:    "The number of vectors in a chunk.",
		Export: true,
	}
	p.ChunkRows.Init(base.mgr)

	p.EnableGrowingSegmentIndex = ParamItem{
		Key:          "queryNode.segcore.growing.enableIndex",
		Version:      "2.0.0",
		DefaultValue: "false",
		Doc:          "Enable segment growing with index to accelerate vector search.",
		Export:       true,
	}
	p.EnableGrowingSegmentIndex.Init(base.mgr)

	p.GrowingIndexNlist = ParamItem{
		Key:          "queryNode.segcore.growing.nlist",
		Version:      "2.0.0",
		DefaultValue: "128",
		Doc:          "growing index nlist, recommend to set sqrt(chunkRows), must smaller than chunkRows/8",
		Export:       true,
	}
	p.GrowingIndexNlist.Init(base.mgr)

	p.GrowingIndexNProbe = ParamItem{
		Key:     "queryNode.segcore.growing.nprobe",
		Version: "2.0.0",
		Formatter: func(v string) string {
			defaultNprobe := p.GrowingIndexNlist.GetAsInt64() / 8
			nprobe := getAsInt64(v)
			if nprobe == 0 {
				nprobe = defaultNprobe
			}
			if nprobe > p.GrowingIndexNlist.GetAsInt64() {
				return p.GrowingIndexNlist.GetValue()
			}
			return strconv.FormatInt(nprobe, 10)
		},
		Doc:    "nprobe to search small index, based on your accuracy requirement, must smaller than nlist",
		Export: true,
	}
	p.GrowingIndexNProbe.Init(base.mgr)

	p.LoadMemoryUsageFactor = ParamItem{
		Key:          "queryNode.loadMemoryUsageFactor",
		Version:      "2.0.0",
		DefaultValue: "3",
		PanicIfEmpty: true,
		Doc:          "The multiply factor of calculating the memory usage while loading segments",
		Export:       true,
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
		Doc:          "2 GB, 2 * 1024 *1024 *1024",
		Export:       true,
	}
	p.CacheMemoryLimit.Init(base.mgr)

	p.CacheEnabled = ParamItem{
		Key:          "queryNode.cache.enabled",
		Version:      "2.0.0",
		DefaultValue: "",
		Export:       true,
	}
	p.CacheEnabled.Init(base.mgr)

	p.MmapDirPath = ParamItem{
		Key:          "queryNode.mmapDirPath",
		Version:      "2.3.0",
		DefaultValue: "",
		Doc:          "The folder that storing data files for mmap, setting to a path will enable Milvus to load data with mmap",
	}
	p.MmapDirPath.Init(base.mgr)

	p.GroupEnabled = ParamItem{
		Key:          "queryNode.grouping.enabled",
		Version:      "2.0.0",
		DefaultValue: "true",
		Export:       true,
	}
	p.GroupEnabled.Init(base.mgr)

	p.MaxReceiveChanSize = ParamItem{
		Key:          "queryNode.scheduler.receiveChanSize",
		Version:      "2.0.0",
		DefaultValue: "10240",
		Export:       true,
	}
	p.MaxReceiveChanSize.Init(base.mgr)

	p.MaxReadConcurrency = ParamItem{
		Key:          "queryNode.scheduler.maxReadConcurrentRatio",
		Version:      "2.0.0",
		DefaultValue: "1.0",
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
		Doc: `maxReadConcurrentRatio is the concurrency ratio of read task (search task and query task).
Max read concurrency would be the value of ` + "runtime.NumCPU * maxReadConcurrentRatio" + `.
It defaults to 2.0, which means max read concurrency would be the value of runtime.NumCPU * 2.
Max read concurrency must greater than or equal to 1, and less than or equal to runtime.NumCPU * 100.
(0, 100]`,
		Export: true,
	}
	p.MaxReadConcurrency.Init(base.mgr)

	p.MaxUnsolvedQueueSize = ParamItem{
		Key:          "queryNode.scheduler.unsolvedQueueSize",
		Version:      "2.0.0",
		DefaultValue: "10240",
		Export:       true,
	}
	p.MaxUnsolvedQueueSize.Init(base.mgr)

	p.MaxGroupNQ = ParamItem{
		Key:          "queryNode.grouping.maxNQ",
		Version:      "2.0.0",
		DefaultValue: "50000",
		Export:       true,
	}
	p.MaxGroupNQ.Init(base.mgr)

	p.TopKMergeRatio = ParamItem{
		Key:          "queryNode.grouping.topKMergeRatio",
		Version:      "2.0.0",
		DefaultValue: "20.0",
		Export:       true,
	}
	p.TopKMergeRatio.Init(base.mgr)

	p.CPURatio = ParamItem{
		Key:          "queryNode.scheduler.cpuRatio",
		Version:      "2.0.0",
		DefaultValue: "10",
		Doc:          "ratio used to estimate read task cpu usage.",
		Export:       true,
	}
	p.CPURatio.Init(base.mgr)

	p.EnableDisk = ParamItem{
		Key:          "queryNode.enableDisk",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc:          "enable querynode load disk index, and search on disk index",
		Export:       true,
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
		Export: true,
	}
	p.MaxDiskUsagePercentage.Init(base.mgr)

	p.MaxTimestampLag = ParamItem{
		Key:          "queryNode.scheduler.maxTimestampLag",
		Version:      "2.2.3",
		DefaultValue: "86400",
		Export:       true,
	}
	p.MaxTimestampLag.Init(base.mgr)

	p.GCEnabled = ParamItem{
		Key:          "queryNode.gcenabled",
		Version:      "2.3.0",
		DefaultValue: "true",
	}
	p.GCEnabled.Init(base.mgr)

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
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.MaxSegmentDeleteBuffer = ParamItem{
		Key:          "queryNode.maxSegmentDeleteBuffer",
		Version:      "2.3.0",
		DefaultValue: "10000000",
	}
	p.MaxSegmentDeleteBuffer.Init(base.mgr)

	p.IoPoolSize = ParamItem{
		Key:          "queryNode.ioPoolSize",
		Version:      "2.3.0",
		DefaultValue: "0",
		Doc:          "Control how many goroutines will the loader use to pull files, if the given value is non-positive, the value will be set to CpuNum * 8, at least 32, and at most 256",
	}
	p.IoPoolSize.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	// --- CHANNEL ---
	WatchTimeoutInterval         ParamItem `refreshable:"false"`
	ChannelBalanceSilentDuration ParamItem `refreshable:"true"`
	ChannelBalanceInterval       ParamItem `refreshable:"true"`

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
	p.WatchTimeoutInterval = ParamItem{
		Key:          "dataCoord.channel.watchTimeoutInterval",
		Version:      "2.2.3",
		DefaultValue: "120",
		Doc:          "Timeout on watching channels (in seconds). Datanode tickler update watch progress will reset timeout timer.",
		Export:       true,
	}
	p.WatchTimeoutInterval.Init(base.mgr)

	p.ChannelBalanceSilentDuration = ParamItem{
		Key:          "dataCoord.channel.balanceSilentDuration",
		Version:      "2.2.3",
		DefaultValue: "300",
		Doc:          "The duration after which the channel manager start background channel balancing",
		Export:       true,
	}
	p.ChannelBalanceSilentDuration.Init(base.mgr)

	p.ChannelBalanceInterval = ParamItem{
		Key:          "dataCoord.channel.balanceInterval",
		Version:      "2.2.3",
		DefaultValue: "360",
		Doc:          "The interval with which the channel manager check dml channel balance status",
		Export:       true,
	}
	p.ChannelBalanceInterval.Init(base.mgr)

	p.SegmentMaxSize = ParamItem{
		Key:          "dataCoord.segment.maxSize",
		Version:      "2.0.0",
		DefaultValue: "512",
		Doc:          "Maximum size of a segment in MB",
		Export:       true,
	}
	p.SegmentMaxSize.Init(base.mgr)

	p.DiskSegmentMaxSize = ParamItem{
		Key:          "dataCoord.segment.diskSegmentMaxSize",
		Version:      "2.0.0",
		DefaultValue: "512",
		Doc:          "Maximun size of a segment in MB for collection which has Disk index",
		Export:       true,
	}
	p.DiskSegmentMaxSize.Init(base.mgr)

	p.SegmentSealProportion = ParamItem{
		Key:          "dataCoord.segment.sealProportion",
		Version:      "2.0.0",
		DefaultValue: "0.25",
		Export:       true,
	}
	p.SegmentSealProportion.Init(base.mgr)

	p.SegAssignmentExpiration = ParamItem{
		Key:          "dataCoord.segment.assignmentExpiration",
		Version:      "2.0.0",
		DefaultValue: "2000",
		Doc:          "The time of the assignment expiration in ms",
		Export:       true,
	}
	p.SegAssignmentExpiration.Init(base.mgr)

	p.SegmentMaxLifetime = ParamItem{
		Key:          "dataCoord.segment.maxLife",
		Version:      "2.0.0",
		DefaultValue: "86400",
		Doc:          "The max lifetime of segment in seconds, 24*60*60",
		Export:       true,
	}
	p.SegmentMaxLifetime.Init(base.mgr)

	p.SegmentMaxIdleTime = ParamItem{
		Key:          "dataCoord.segment.maxIdleTime",
		Version:      "2.0.0",
		DefaultValue: "3600",
		Doc: `If a segment didn't accept dml records in ` + "maxIdleTime" + ` and the size of segment is greater than
` + "minSizeFromIdleToSealed" + `, Milvus will automatically seal it.
The max idle time of segment in seconds, 10*60.`,
		Export: true,
	}
	p.SegmentMaxIdleTime.Init(base.mgr)

	p.SegmentMinSizeFromIdleToSealed = ParamItem{
		Key:          "dataCoord.segment.minSizeFromIdleToSealed",
		Version:      "2.0.0",
		DefaultValue: "16.0",
		Doc:          "The min size in MB of segment which can be idle from sealed.",
		Export:       true,
	}
	p.SegmentMinSizeFromIdleToSealed.Init(base.mgr)

	p.SegmentMaxBinlogFileNumber = ParamItem{
		Key:          "dataCoord.segment.maxBinlogFileNumber",
		Version:      "2.2.0",
		DefaultValue: "32",
		Doc: `The max number of binlog file for one segment, the segment will be sealed if
the number of binlog file reaches to max value.`,
		Export: true,
	}
	p.SegmentMaxBinlogFileNumber.Init(base.mgr)

	p.EnableCompaction = ParamItem{
		Key:          "dataCoord.enableCompaction",
		Version:      "2.0.0",
		DefaultValue: "false",
		Doc:          "Enable data segment compaction",
		Export:       true,
	}
	p.EnableCompaction.Init(base.mgr)

	p.EnableAutoCompaction = ParamItem{
		Key:          "dataCoord.compaction.enableAutoCompaction",
		Version:      "2.0.0",
		DefaultValue: "true",
		Export:       true,
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
		Doc:          "The segment is considered as \"small segment\" when its # of rows is smaller than",
		Export:       true,
	}
	p.SegmentSmallProportion.Init(base.mgr)

	p.SegmentCompactableProportion = ParamItem{
		Key:          "dataCoord.segment.compactableProportion",
		Version:      "2.2.1",
		DefaultValue: "0.5",
		Doc: `(smallProportion * segment max # of rows).
A compaction will happen on small segments if the segment after compaction will have`,
		Export: true,
	}
	p.SegmentCompactableProportion.Init(base.mgr)

	p.SegmentExpansionRate = ParamItem{
		Key:          "dataCoord.segment.expansionRate",
		Version:      "2.2.1",
		DefaultValue: "1.25",
		Doc: `over (compactableProportion * segment max # of rows) rows.
MUST BE GREATER THAN OR EQUAL TO <smallProportion>!!!
During compaction, the size of segment # of rows is able to exceed segment max # of rows by (expansionRate-1) * 100%. `,
		Export: true,
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
		Doc:          "",
		Export:       true,
	}
	p.EnableGarbageCollection.Init(base.mgr)

	p.GCInterval = ParamItem{
		Key:          "dataCoord.gc.interval",
		Version:      "2.0.0",
		DefaultValue: "3600",
		Doc:          "gc interval in seconds",
		Export:       true,
	}
	p.GCInterval.Init(base.mgr)

	p.GCMissingTolerance = ParamItem{
		Key:          "dataCoord.gc.missingTolerance",
		Version:      "2.0.0",
		DefaultValue: "86400",
		Doc:          "file meta missing tolerance duration in seconds, 60*24",
		Export:       true,
	}
	p.GCMissingTolerance.Init(base.mgr)

	p.GCDropTolerance = ParamItem{
		Key:          "dataCoord.gc.dropTolerance",
		Version:      "2.0.0",
		DefaultValue: "3600",
		Doc:          "file belongs to dropped entity tolerance duration in seconds. 3600",
		Export:       true,
	}
	p.GCDropTolerance.Init(base.mgr)

	p.EnableActiveStandby = ParamItem{
		Key:          "dataCoord.enableActiveStandby",
		Version:      "2.0.0",
		DefaultValue: "false",
		Export:       true,
	}
	p.EnableActiveStandby.Init(base.mgr)

	p.MinSegmentNumRowsToEnableIndex = ParamItem{
		Key:          "indexCoord.segment.minSegmentNumRowsToEnableIndex",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "It's a threshold. When the segment num rows is less than this value, the segment will not be indexed",
		Export:       true,
	}
	p.MinSegmentNumRowsToEnableIndex.Init(base.mgr)

	p.BindIndexNodeMode = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.enable",
		Version:      "2.0.0",
		DefaultValue: "false",
		Export:       true,
	}
	p.BindIndexNodeMode.Init(base.mgr)

	p.IndexNodeAddress = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.address",
		Version:      "2.0.0",
		DefaultValue: "localhost:22930",
		Export:       true,
	}
	p.IndexNodeAddress.Init(base.mgr)

	p.WithCredential = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.withCred",
		Version:      "2.0.0",
		DefaultValue: "false",
		Export:       true,
	}
	p.WithCredential.Init(base.mgr)

	p.IndexNodeID = ParamItem{
		Key:          "indexCoord.bindIndexNodeMode.nodeID",
		Version:      "2.0.0",
		DefaultValue: "0",
		Export:       true,
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
	MaxParallelSyncTaskNum  ParamItem `refreshable:"false"`

	// segment
	FlushInsertBufferSize  ParamItem `refreshable:"true"`
	FlushDeleteBufferBytes ParamItem `refreshable:"true"`
	BinLogMaxSize          ParamItem `refreshable:"true"`
	SyncPeriod             ParamItem `refreshable:"true"`
	CpLagPeriod            ParamItem `refreshable:"true"`

	// watchEvent
	WatchEventTicklerInterval ParamItem `refreshable:"false"`

	// io concurrency to fetch stats logs
	IOConcurrency ParamItem `refreshable:"false"`

	// memory management
	MemoryForceSyncEnable     ParamItem `refreshable:"true"`
	MemoryForceSyncSegmentNum ParamItem `refreshable:"true"`
	MemoryWatermark           ParamItem `refreshable:"true"`

	DataNodeTimeTickByRPC ParamItem `refreshable:"false"`
	// DataNode send timetick interval per collection
	DataNodeTimeTickInterval ParamItem `refreshable:"false"`

	// Skip BF
	SkipBFStatsLoad ParamItem `refreshable:"true"`
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.FlowGraphMaxQueueLength = ParamItem{
		Key:          "dataNode.dataSync.flowGraph.maxQueueLength",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "Maximum length of task queue in flowgraph",
		Export:       true,
	}
	p.FlowGraphMaxQueueLength.Init(base.mgr)

	p.FlowGraphMaxParallelism = ParamItem{
		Key:          "dataNode.dataSync.flowGraph.maxParallelism",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc:          "Maximum number of tasks executed in parallel in the flowgraph",
		Export:       true,
	}
	p.FlowGraphMaxParallelism.Init(base.mgr)

	p.MaxParallelSyncTaskNum = ParamItem{
		Key:          "dataNode.dataSync.maxParallelSyncTaskNum",
		Version:      "2.3.0",
		DefaultValue: "2",
		Doc:          "Maximum number of sync tasks executed in parallel in each flush manager",
		Export:       true,
	}
	p.MaxParallelSyncTaskNum.Init(base.mgr)

	p.FlushInsertBufferSize = ParamItem{
		Key:          "dataNode.segment.insertBufSize",
		Version:      "2.0.0",
		FallbackKeys: []string{"DATA_NODE_IBUFSIZE"},
		DefaultValue: "16777216",
		PanicIfEmpty: true,
		Doc:          "Max buffer size to flush for a single segment.",
		Export:       true,
	}
	p.FlushInsertBufferSize.Init(base.mgr)

	p.MemoryForceSyncEnable = ParamItem{
		Key:          "datanode.memory.forceSyncEnable",
		Version:      "2.2.4",
		DefaultValue: "true",
	}
	p.MemoryForceSyncEnable.Init(base.mgr)

	p.MemoryForceSyncSegmentNum = ParamItem{
		Key:          "datanode.memory.forceSyncSegmentNum",
		Version:      "2.2.4",
		DefaultValue: "1",
	}
	p.MemoryForceSyncSegmentNum.Init(base.mgr)

	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		p.MemoryWatermark = ParamItem{
			Key:          "datanode.memory.watermarkStandalone",
			Version:      "2.2.4",
			DefaultValue: "0.2",
		}
	} else if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.ClusterDeployMode {
		p.MemoryWatermark = ParamItem{
			Key:          "datanode.memory.watermarkCluster",
			Version:      "2.2.4",
			DefaultValue: "0.5",
		}
	} else {
		log.Warn("DeployModeEnv is not set, use default", zap.Float64("default", 0.5))
		p.MemoryWatermark = ParamItem{
			Key:          "datanode.memory.watermarkCluster",
			Version:      "2.2.4",
			DefaultValue: "0.5",
		}
	}
	p.MemoryWatermark.Init(base.mgr)

	p.FlushDeleteBufferBytes = ParamItem{
		Key:          "dataNode.segment.deleteBufBytes",
		Version:      "2.0.0",
		DefaultValue: "67108864",
		Doc:          "Max buffer size to flush del for a single channel",
		Export:       true,
	}
	p.FlushDeleteBufferBytes.Init(base.mgr)

	p.BinLogMaxSize = ParamItem{
		Key:          "dataNode.segment.binlog.maxsize",
		Version:      "2.0.0",
		DefaultValue: "67108864",
	}
	p.BinLogMaxSize.Init(base.mgr)

	p.SyncPeriod = ParamItem{
		Key:          "dataNode.segment.syncPeriod",
		Version:      "2.0.0",
		DefaultValue: "600",
		Doc:          "The period to sync segments if buffer is not empty.",
		Export:       true,
	}
	p.SyncPeriod.Init(base.mgr)

	p.CpLagPeriod = ParamItem{
		Key:          "datanode.segment.cpLagPeriod",
		Version:      "2.2.0",
		DefaultValue: "600",
		Doc:          "The period to sync segments if buffer is not empty.",
		Export:       true,
	}
	p.CpLagPeriod.Init(base.mgr)

	p.WatchEventTicklerInterval = ParamItem{
		Key:          "datanode.segment.watchEventTicklerInterval",
		Version:      "2.2.3",
		DefaultValue: "15",
	}
	p.WatchEventTicklerInterval.Init(base.mgr)

	p.IOConcurrency = ParamItem{
		Key:          "dataNode.dataSync.ioConcurrency",
		Version:      "2.0.0",
		DefaultValue: "10",
	}
	p.IOConcurrency.Init(base.mgr)

	p.DataNodeTimeTickByRPC = ParamItem{
		Key:          "datanode.timetick.byRPC",
		Version:      "2.2.9",
		PanicIfEmpty: false,
		DefaultValue: "true",
	}
	p.DataNodeTimeTickByRPC.Init(base.mgr)

	p.DataNodeTimeTickInterval = ParamItem{
		Key:          "datanode.timetick.interval",
		Version:      "2.2.5",
		PanicIfEmpty: false,
		DefaultValue: "500",
	}
	p.DataNodeTimeTickInterval.Init(base.mgr)

	p.SkipBFStatsLoad = ParamItem{
		Key:          "dataNode.skip.BFStats.Load",
		Version:      "2.2.5",
		PanicIfEmpty: false,
		DefaultValue: "false",
	}
	p.SkipBFStatsLoad.Init(base.mgr)
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
		Export:       true,
	}
	p.BuildParallel.Init(base.mgr)

	p.EnableDisk = ParamItem{
		Key:          "indexNode.enableDisk",
		Version:      "2.2.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
		Doc:          "enable index node build disk vector index",
		Export:       true,
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
		Export: true,
	}
	p.MaxDiskUsagePercentage.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "indexNode.gracefulStopTimeout",
		Version:      "2.2.1",
		FallbackKeys: []string{"common.gracefulStopTimeout"},
		Export:       true,
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
