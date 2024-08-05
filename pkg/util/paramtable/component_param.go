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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

const (
	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize                      = 16
	DefaultConsistencyLevelUsedInDelete        = commonpb.ConsistencyLevel_Bounded
	DefaultGracefulTime                        = 5000 // ms
	DefaultGracefulStopTimeout                 = 1800 // s, for node
	DefaultFlushMgrCleanInterval               = 300  // s
	DefaultProxyGracefulStopTimeout            = 30   // s，for proxy
	DefaultCoordGracefulStopTimeout            = 5    // s，for coord
	DefaultHighPriorityThreadCoreCoefficient   = 10
	DefaultMiddlePriorityThreadCoreCoefficient = 5
	DefaultLowPriorityThreadCoreCoefficient    = 1

	DefaultSessionTTL        = 30 // s
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
	once      sync.Once
	baseTable *BaseTable

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
	RoleCfg       roleConfig

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
func (p *ComponentParam) Init(bt *BaseTable) {
	p.once.Do(func() {
		p.init(bt)
	})
}

// init initialize the global param table

func (p *ComponentParam) init(bt *BaseTable) {
	p.baseTable = bt
	p.ServiceParam.init(bt)

	p.CommonCfg.init(bt)
	p.QuotaConfig.init(bt)
	p.AutoIndexConfig.init(bt)
	p.TraceCfg.init(bt)

	p.RootCoordCfg.init(bt)
	p.ProxyCfg.init(bt)
	p.QueryCoordCfg.init(bt)
	p.QueryNodeCfg.init(bt)
	p.DataCoordCfg.init(bt)
	p.DataNodeCfg.init(bt)
	p.IndexNodeCfg.init(bt)
	p.HTTPCfg.init(bt)
	p.LogCfg.init(bt)
	p.RoleCfg.init(bt)

	p.RootCoordGrpcServerCfg.Init("rootCoord", bt)
	p.ProxyGrpcServerCfg.Init("proxy", bt)
	p.ProxyGrpcServerCfg.InternalPort.Export = true
	p.QueryCoordGrpcServerCfg.Init("queryCoord", bt)
	p.QueryNodeGrpcServerCfg.Init("queryNode", bt)
	p.DataCoordGrpcServerCfg.Init("dataCoord", bt)
	p.DataNodeGrpcServerCfg.Init("dataNode", bt)
	p.IndexNodeGrpcServerCfg.Init("indexNode", bt)

	p.RootCoordGrpcClientCfg.Init("rootCoord", bt)
	p.ProxyGrpcClientCfg.Init("proxy", bt)
	p.QueryCoordGrpcClientCfg.Init("queryCoord", bt)
	p.QueryNodeGrpcClientCfg.Init("queryNode", bt)
	p.DataCoordGrpcClientCfg.Init("dataCoord", bt)
	p.DataNodeGrpcClientCfg.Init("dataNode", bt)
	p.IndexNodeGrpcClientCfg.Init("indexNode", bt)

	p.IntegrationTestCfg.init(bt)
}

func (p *ComponentParam) GetComponentConfigurations(componentName string, sub string) map[string]string {
	allownPrefixs := append(globalConfigPrefixs(), componentName+".")
	return p.baseTable.mgr.GetBy(config.WithSubstr(sub), config.WithOneOfPrefixs(allownPrefixs...))
}

func (p *ComponentParam) GetAll() map[string]string {
	return p.baseTable.mgr.GetConfigs()
}

func (p *ComponentParam) Watch(key string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.Register(key, watcher)
}

func (p *ComponentParam) Unwatch(key string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.Unregister(key, watcher)
}

func (p *ComponentParam) WatchKeyPrefix(keyPrefix string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, watcher)
}

// /////////////////////////////////////////////////////////////////////////////
// --- common ---
type commonConfig struct {
	ClusterPrefix ParamItem `refreshable:"false"`

	RootCoordTimeTick   ParamItem `refreshable:"true"`
	RootCoordStatistics ParamItem `refreshable:"true"`
	RootCoordDml        ParamItem `refreshable:"false"`
	ReplicateMsgChannel ParamItem `refreshable:"false"`

	QueryCoordTimeTick ParamItem `refreshable:"true"`

	// Deprecated
	DataCoordTimeTick     ParamItem `refreshable:"false"`
	DataCoordSegmentInfo  ParamItem `refreshable:"true"`
	DataCoordSubName      ParamItem `refreshable:"false"`
	DataCoordWatchSubPath ParamItem `refreshable:"false"`
	DataCoordTicklePath   ParamItem `refreshable:"false"`
	DataNodeSubName       ParamItem `refreshable:"false"`

	DefaultPartitionName ParamItem `refreshable:"false"`
	DefaultIndexName     ParamItem `refreshable:"true"`
	EntityExpirationTTL  ParamItem `refreshable:"true"`

	IndexSliceSize                      ParamItem `refreshable:"false"`
	HighPriorityThreadCoreCoefficient   ParamItem `refreshable:"false"`
	MiddlePriorityThreadCoreCoefficient ParamItem `refreshable:"false"`
	LowPriorityThreadCoreCoefficient    ParamItem `refreshable:"false"`
	MaxDegree                           ParamItem `refreshable:"true"`
	SearchListSize                      ParamItem `refreshable:"true"`
	PQCodeBudgetGBRatio                 ParamItem `refreshable:"true"`
	BuildNumThreadsRatio                ParamItem `refreshable:"true"`
	SearchCacheBudgetGBRatio            ParamItem `refreshable:"true"`
	LoadNumThreadRatio                  ParamItem `refreshable:"true"`
	BeamWidthRatio                      ParamItem `refreshable:"true"`
	ConsistencyLevelUsedInDelete        ParamItem `refreshable:"true"`
	GracefulTime                        ParamItem `refreshable:"true"`
	GracefulStopTimeout                 ParamItem `refreshable:"true"`

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

	MetricsPort ParamItem `refreshable:"false"`

	// lock related params
	EnableLockMetrics        ParamItem `refreshable:"false"`
	LockSlowLogInfoThreshold ParamItem `refreshable:"true"`
	LockSlowLogWarnThreshold ParamItem `refreshable:"true"`

	TTMsgEnabled ParamItem `refreshable:"true"`
	TraceLogMode ParamItem `refreshable:"true"`

	BloomFilterSize           ParamItem `refreshable:"true"`
	MaxBloomFalsePositive     ParamItem `refreshable:"true"`
	BloomFilterApplyBatchSize ParamItem `refreshable:"true"`
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

	p.ReplicateMsgChannel = ParamItem{
		Key:          "msgChannel.chanNamePrefix.replicateMsg",
		Version:      "2.3.2",
		FallbackKeys: []string{"common.chanNamePrefix.replicateMsg"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.ReplicateMsgChannel.Init(base.mgr)

	p.QueryCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.queryTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.queryTimeTick"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.QueryCoordTimeTick.Init(base.mgr)

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

	p.EntityExpirationTTL = ParamItem{
		Key:          "common.entityExpiration",
		Version:      "2.1.0",
		DefaultValue: "-1",
		Formatter: func(value string) string {
			ttl := getAsInt(value)
			if ttl < 0 {
				return "-1"
			}

			return strconv.Itoa(ttl)
		},
		Doc:    "Entity expiration in seconds, CAUTION -1 means never expire",
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

	p.ConsistencyLevelUsedInDelete = ParamItem{
		Key:          "common.consistencyLevelUsedInDelete",
		Version:      "2.0.0",
		DefaultValue: DefaultConsistencyLevelUsedInDelete.String(),
		Doc:          "Consistency level used in delete by expression",
		Export:       true,
	}
	p.ConsistencyLevelUsedInDelete.Init(base.mgr)

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
		DefaultValue: strconv.Itoa(DefaultGracefulStopTimeout),
		Doc:          "seconds. it will force quit the server if the graceful stop process is not completed during this time.",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.StorageType = ParamItem{
		Key:          "common.storageType",
		Version:      "2.0.0",
		DefaultValue: "remote",
		Doc:          "please adjust in embedded Milvus: local, available values are [local, remote, opendal], value minio is deprecated, use remote instead",
		Export:       true,
	}
	p.StorageType.Init(base.mgr)

	p.HighPriorityThreadCoreCoefficient = ParamItem{
		Key:          "common.threadCoreCoefficient.highPriority",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultHighPriorityThreadCoreCoefficient),
		Doc: "This parameter specify how many times the number of threads " +
			"is the number of cores in high priority pool",
		Export: true,
	}
	p.HighPriorityThreadCoreCoefficient.Init(base.mgr)

	p.MiddlePriorityThreadCoreCoefficient = ParamItem{
		Key:          "common.threadCoreCoefficient.middlePriority",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultMiddlePriorityThreadCoreCoefficient),
		Doc: "This parameter specify how many times the number of threads " +
			"is the number of cores in middle priority pool",
		Export: true,
	}
	p.MiddlePriorityThreadCoreCoefficient.Init(base.mgr)

	p.LowPriorityThreadCoreCoefficient = ParamItem{
		Key:          "common.threadCoreCoefficient.lowPriority",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(DefaultLowPriorityThreadCoreCoefficient),
		Doc: "This parameter specify how many times the number of threads " +
			"is the number of cores in low priority pool",
		Export: true,
	}
	p.LowPriorityThreadCoreCoefficient.Init(base.mgr)

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
		DefaultValue: "60",
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

	p.MetricsPort = ParamItem{
		Key:          "common.MetricsPort",
		Version:      "2.3.0",
		DefaultValue: "9091",
	}
	p.MetricsPort.Init(base.mgr)

	p.EnableLockMetrics = ParamItem{
		Key:          "common.locks.metrics.enable",
		Version:      "2.0.0",
		DefaultValue: "false",
		Doc:          "whether gather statistics for metrics locks",
		Export:       true,
	}
	p.EnableLockMetrics.Init(base.mgr)

	p.LockSlowLogInfoThreshold = ParamItem{
		Key:          "common.locks.threshold.info",
		Version:      "2.0.0",
		DefaultValue: "500",
		Doc:          "minimum milliseconds for printing durations in info level",
		Export:       true,
	}
	p.LockSlowLogInfoThreshold.Init(base.mgr)

	p.LockSlowLogWarnThreshold = ParamItem{
		Key:          "common.locks.threshold.warn",
		Version:      "2.0.0",
		DefaultValue: "1000",
		Doc:          "minimum milliseconds for printing durations in warn level",
		Export:       true,
	}
	p.LockSlowLogWarnThreshold.Init(base.mgr)

	p.TTMsgEnabled = ParamItem{
		Key:          "common.ttMsgEnabled",
		Version:      "2.3.2",
		DefaultValue: "true",
		Doc:          "Whether the instance disable sending ts messages",
	}
	p.TTMsgEnabled.Init(base.mgr)

	p.TraceLogMode = ParamItem{
		Key:          "common.traceLogMode",
		Version:      "2.3.4",
		DefaultValue: "0",
		Doc:          "trace request info",
	}
	p.TraceLogMode.Init(base.mgr)

	p.BloomFilterSize = ParamItem{
		Key:          "common.bloomFilterSize",
		Version:      "2.3.2",
		DefaultValue: "100000",
		Doc:          "bloom filter initial size",
	}
	p.BloomFilterSize.Init(base.mgr)

	p.MaxBloomFalsePositive = ParamItem{
		Key:          "common.maxBloomFalsePositive",
		Version:      "2.3.2",
		DefaultValue: "0.05",
		Doc:          "max false positive rate for bloom filter",
	}
	p.MaxBloomFalsePositive.Init(base.mgr)
	p.BloomFilterApplyBatchSize = ParamItem{
		Key:          "common.bloomFilterApplyBatchSize",
		Version:      "2.3.18",
		DefaultValue: "1000",
		Doc:          "batch size when to apply pk to bloom filter",
		Export:       true,
	}
	p.BloomFilterApplyBatchSize.Init(base.mgr)
}

type traceConfig struct {
	Exporter       ParamItem `refreshable:"false"`
	SampleFraction ParamItem `refreshable:"false"`
	JaegerURL      ParamItem `refreshable:"false"`
	OtlpEndpoint   ParamItem `refreshable:"false"`
	OtlpSecure     ParamItem `refreshable:"false"`
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
		Doc:     "example: \"127.0.0.1:4318\"",
	}
	t.OtlpEndpoint.Init(base.mgr)

	t.OtlpSecure = ParamItem{
		Key:          "trace.otlp.secure",
		Version:      "2.4.0",
		DefaultValue: "true",
	}
	t.OtlpSecure.Init(base.mgr)
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
	MaxGeneralCapacity          ParamItem `refreshable:"true"`
	GracefulStopTimeout         ParamItem `refreshable:"true"`
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

	p.MaxGeneralCapacity = ParamItem{
		Key:          "rootCoord.maxGeneralCapacity",
		Version:      "2.3.5",
		DefaultValue: "65536",
		Doc:          "upper limit for the sum of of product of partitionNumber and shardNumber",
		Export:       true,
		Formatter: func(v string) string {
			if getAsInt(v) < 512 {
				return "512"
			}
			return v
		},
	}
	p.MaxGeneralCapacity.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "rootCoord.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultCoordGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type AccessLogConfig struct {
	Enable        ParamItem  `refreshable:"false"`
	MinioEnable   ParamItem  `refreshable:"false"`
	LocalPath     ParamItem  `refreshable:"false"`
	Filename      ParamItem  `refreshable:"false"`
	MaxSize       ParamItem  `refreshable:"false"`
	CacheSize     ParamItem  `refreshable:"false"`
	RotatedTime   ParamItem  `refreshable:"false"`
	MaxBackups    ParamItem  `refreshable:"false"`
	RemotePath    ParamItem  `refreshable:"false"`
	RemoteMaxTime ParamItem  `refreshable:"false"`
	Formatter     ParamGroup `refreshable:"false"`
}

type proxyConfig struct {
	// Alias  string
	SoPath ParamItem `refreshable:"false"`

	TimeTickInterval             ParamItem `refreshable:"false"`
	HealthCheckTimeout           ParamItem `refreshable:"true"`
	MsgStreamTimeTickBufSize     ParamItem `refreshable:"true"`
	MaxNameLength                ParamItem `refreshable:"true"`
	MaxUsernameLength            ParamItem `refreshable:"true"`
	MinPasswordLength            ParamItem `refreshable:"true"`
	MaxPasswordLength            ParamItem `refreshable:"true"`
	MaxFieldNum                  ParamItem `refreshable:"true"`
	MaxShardNum                  ParamItem `refreshable:"true"`
	MaxDimension                 ParamItem `refreshable:"true"`
	GinLogging                   ParamItem `refreshable:"false"`
	GinLogSkipPaths              ParamItem `refreshable:"false"`
	MaxUserNum                   ParamItem `refreshable:"true"`
	MaxRoleNum                   ParamItem `refreshable:"true"`
	MaxTaskNum                   ParamItem `refreshable:"false"`
	ShardLeaderCacheInterval     ParamItem `refreshable:"false"`
	ReplicaSelectionPolicy       ParamItem `refreshable:"false"`
	CheckQueryNodeHealthInterval ParamItem `refreshable:"false"`
	CostMetricsExpireTime        ParamItem `refreshable:"true"`
	RetryTimesOnReplica          ParamItem `refreshable:"true"`
	RetryTimesOnHealthCheck      ParamItem `refreshable:"true"`
	PartitionNameRegexp          ParamItem `refreshable:"true"`

	SkipAutoIDCheck       ParamItem `refreshable:"true"`
	SkipPartitionKeyCheck ParamItem `refreshable:"true"`

	AccessLog AccessLogConfig

	// connection manager
	ConnectionCheckIntervalSeconds ParamItem `refreshable:"true"`
	ConnectionClientInfoTTLSeconds ParamItem `refreshable:"true"`
	MaxConnectionNum               ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"true"`

	SlowQuerySpanInSeconds ParamItem `refreshable:"true"`
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

	p.HealthCheckTimeout = ParamItem{
		Key:          "proxy.healthCheckTimeout",
		FallbackKeys: []string{"proxy.healthCheckTimetout"},
		Version:      "2.3.0",
		DefaultValue: "3000",
		PanicIfEmpty: true,
		Doc:          "ms, the interval that to do component healthy check",
		Export:       true,
	}
	p.HealthCheckTimeout.Init(base.mgr)

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
		DefaultValue: "10000",
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

	p.GinLogSkipPaths = ParamItem{
		Key:          "proxy.ginLogSkipPaths",
		Version:      "2.3.0",
		DefaultValue: "/",
		Doc:          "skip url path for gin log",
		Export:       true,
	}
	p.GinLogSkipPaths.Init(base.mgr)

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
		DefaultValue: "false",
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
		Key:          "proxy.accessLog.localPath",
		Version:      "2.2.0",
		DefaultValue: "/tmp/milvus_access",
		Export:       true,
	}
	p.AccessLog.LocalPath.Init(base.mgr)

	p.AccessLog.Filename = ParamItem{
		Key:          "proxy.accessLog.filename",
		Version:      "2.2.0",
		DefaultValue: "",
		Doc:          "Log filename, leave empty to use stdout.",
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

	p.AccessLog.CacheSize = ParamItem{
		Key:          "proxy.accessLog.cacheSize",
		Version:      "2.3.2",
		DefaultValue: "10240",
		Doc:          "Size of log of memory cache, in B",
	}
	p.AccessLog.CacheSize.Init(base.mgr)

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
		DefaultValue: "0",
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
		DefaultValue: "0",
		Doc:          "Max time for log file in minIO, in hours",
	}
	p.AccessLog.RemoteMaxTime.Init(base.mgr)

	p.AccessLog.Formatter = ParamGroup{
		KeyPrefix: "proxy.accessLog.formatters.",
		Version:   "2.3.4",
	}
	p.AccessLog.Formatter.Init(base.mgr)

	p.ShardLeaderCacheInterval = ParamItem{
		Key:          "proxy.shardLeaderCacheInterval",
		Version:      "2.2.4",
		DefaultValue: "3",
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

	p.CheckQueryNodeHealthInterval = ParamItem{
		Key:          "proxy.checkQueryNodeHealthInterval",
		Version:      "2.3.0",
		DefaultValue: "1000",
		Doc:          "time interval to check health for query node, in ms",
	}
	p.CheckQueryNodeHealthInterval.Init(base.mgr)

	p.CostMetricsExpireTime = ParamItem{
		Key:          "proxy.costMetricsExpireTime",
		Version:      "2.3.0",
		DefaultValue: "1000",
		Doc:          "expire time for query node cost metrics, in ms",
	}
	p.CostMetricsExpireTime.Init(base.mgr)

	p.RetryTimesOnReplica = ParamItem{
		Key:          "proxy.retryTimesOnReplica",
		Version:      "2.3.0",
		DefaultValue: "2",
		Doc:          "retry times on each replica",
	}
	p.RetryTimesOnReplica.Init(base.mgr)

	p.RetryTimesOnHealthCheck = ParamItem{
		Key:          "proxy.retryTimesOnHealthCheck",
		Version:      "2.3.0",
		DefaultValue: "3",
		Doc:          "set query node unavailable on proxy when heartbeat failures reach this limit",
	}
	p.RetryTimesOnHealthCheck.Init(base.mgr)

	p.PartitionNameRegexp = ParamItem{
		Key:          "proxy.partitionNameRegexp",
		Version:      "2.3.4",
		DefaultValue: "false",
		Doc:          "switch for whether proxy shall use partition name as regexp when searching",
	}
	p.PartitionNameRegexp.Init(base.mgr)

	p.SkipAutoIDCheck = ParamItem{
		Key:          "proxy.skipAutoIDCheck",
		Version:      "2.4.1",
		DefaultValue: "false",
		Doc:          "switch for whether proxy shall skip auto id check when inserting data",
	}
	p.SkipAutoIDCheck.Init(base.mgr)

	p.SkipPartitionKeyCheck = ParamItem{
		Key:          "proxy.skipPartitionKeyCheck",
		Version:      "2.4.1",
		DefaultValue: "false",
		Doc:          "switch for whether proxy shall skip partition key check when inserting data",
	}
	p.SkipPartitionKeyCheck.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "proxy.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultProxyGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.SlowQuerySpanInSeconds = ParamItem{
		Key:          "proxy.slowQuerySpanInSeconds",
		Version:      "2.3.11",
		Doc:          "query whose executed time exceeds the `slowQuerySpanInSeconds` can be considered slow, in seconds.",
		DefaultValue: "5",
		Export:       true,
	}
	p.SlowQuerySpanInSeconds.Init(base.mgr)

	p.ConnectionCheckIntervalSeconds = ParamItem{
		Key:          "proxy.connectionCheckIntervalSeconds",
		Version:      "2.3.11",
		Doc:          "the interval time(in seconds) for connection manager to scan inactive client info",
		DefaultValue: "120",
		Export:       true,
	}
	p.ConnectionCheckIntervalSeconds.Init(base.mgr)

	p.ConnectionClientInfoTTLSeconds = ParamItem{
		Key:          "proxy.connectionClientInfoTTLSeconds",
		Version:      "2.3.11",
		Doc:          "inactive client info TTL duration, in seconds",
		DefaultValue: "86400",
		Export:       true,
	}
	p.ConnectionClientInfoTTLSeconds.Init(base.mgr)

	p.MaxConnectionNum = ParamItem{
		Key:          "proxy.maxConnectionNum",
		Version:      "2.3.11",
		Doc:          "the max client info numbers that proxy should manage, avoid too many client infos",
		DefaultValue: "10000",
		Export:       true,
	}
	p.MaxConnectionNum.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querycoord ---
type queryCoordConfig struct {
	// Deprecated: Since 2.2.0
	RetryNum ParamItem `refreshable:"true"`
	// Deprecated: Since 2.2.0
	RetryInterval ParamItem `refreshable:"true"`
	// Deprecated: Since 2.3.4
	TaskMergeCap ParamItem `refreshable:"false"`

	TaskExecutionCap ParamItem `refreshable:"true"`

	// ---- Handoff ---
	// Deprecated: Since 2.2.2
	AutoHandoff ParamItem `refreshable:"true"`

	// ---- Balance ---
	AutoBalance                         ParamItem `refreshable:"true"`
	AutoBalanceChannel                  ParamItem `refreshable:"true"`
	Balancer                            ParamItem `refreshable:"true"`
	GlobalRowCountFactor                ParamItem `refreshable:"true"`
	ScoreUnbalanceTolerationFactor      ParamItem `refreshable:"true"`
	ReverseUnbalanceTolerationFactor    ParamItem `refreshable:"true"`
	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"true"`
	BalanceIntervalSeconds              ParamItem `refreshable:"true"`
	MemoryUsageMaxDifferencePercentage  ParamItem `refreshable:"true"`
	GrowingRowCountWeight               ParamItem `refreshable:"true"`
	DelegatorMemoryOverloadFactor       ParamItem `refreshable:"true"`
	BalanceCostThreshold                ParamItem `refreshable:"true"`

	SegmentCheckInterval       ParamItem `refreshable:"true"`
	ChannelCheckInterval       ParamItem `refreshable:"true"`
	BalanceCheckInterval       ParamItem `refreshable:"true"`
	IndexCheckInterval         ParamItem `refreshable:"true"`
	ChannelTaskTimeout         ParamItem `refreshable:"true"`
	SegmentTaskTimeout         ParamItem `refreshable:"true"`
	DistPullInterval           ParamItem `refreshable:"false"`
	HeartbeatAvailableInterval ParamItem `refreshable:"true"`
	LoadTimeoutSeconds         ParamItem `refreshable:"true"`

	DistributionRequestTimeout ParamItem `refreshable:"true"`
	HeartBeatWarningLag        ParamItem `refreshable:"true"`

	// Deprecated: Since 2.2.2, QueryCoord do not use HandOff logic anymore
	CheckHandoffInterval ParamItem `refreshable:"true"`
	EnableActiveStandby  ParamItem `refreshable:"false"`

	// Deprecated: Since 2.2.2, use different interval for different checker
	CheckInterval ParamItem `refreshable:"true"`

	NextTargetSurviveTime          ParamItem `refreshable:"true"`
	UpdateNextTargetInterval       ParamItem `refreshable:"false"`
	CheckNodeInReplicaInterval     ParamItem `refreshable:"false"`
	CheckResourceGroupInterval     ParamItem `refreshable:"false"`
	LeaderViewUpdateInterval       ParamItem `refreshable:"false"`
	EnableRGAutoRecover            ParamItem `refreshable:"true"`
	CheckHealthInterval            ParamItem `refreshable:"false"`
	CheckHealthRPCTimeout          ParamItem `refreshable:"true"`
	BrokerTimeout                  ParamItem `refreshable:"false"`
	CollectionRecoverTimesLimit    ParamItem `refreshable:"true"`
	ObserverTaskParallel           ParamItem `refreshable:"false"`
	CheckAutoBalanceConfigInterval ParamItem `refreshable:"false"`
	CheckNodeSessionInterval       ParamItem `refreshable:"false"`
	GracefulStopTimeout            ParamItem `refreshable:"true"`

	CollectionObserverInterval ParamItem `refreshable:"false"`
	CheckExecutedFlagInterval  ParamItem `refreshable:"false"`
}

func (p *queryCoordConfig) init(base *BaseTable) {
	// ---- Task ---
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
		DefaultValue: "1",
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

	p.AutoBalanceChannel = ParamItem{
		Key:          "queryCoord.autoBalanceChannel",
		Version:      "2.3.4",
		DefaultValue: "true",
		PanicIfEmpty: true,
		Doc:          "Enable auto balance channel",
		Export:       true,
	}
	p.AutoBalanceChannel.Init(base.mgr)

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

	p.GrowingRowCountWeight = ParamItem{
		Key:          "queryCoord.growingRowCountWeight",
		Version:      "2.3.5",
		DefaultValue: "4.0",
		PanicIfEmpty: true,
		Doc:          "the memory weight of growing segment row count",
		Export:       true,
	}
	p.GrowingRowCountWeight.Init(base.mgr)

	p.DelegatorMemoryOverloadFactor = ParamItem{
		Key:          "queryCoord.delegatorMemoryOverloadFactor",
		Version:      "2.3.19",
		DefaultValue: "0.1",
		PanicIfEmpty: true,
		Doc:          "the factor of delegator overloaded memory",
		Export:       true,
	}
	p.DelegatorMemoryOverloadFactor.Init(base.mgr)

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

	p.SegmentCheckInterval = ParamItem{
		Key:          "queryCoord.checkSegmentInterval",
		Version:      "2.3.0",
		DefaultValue: "1000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.SegmentCheckInterval.Init(base.mgr)

	p.ChannelCheckInterval = ParamItem{
		Key:          "queryCoord.checkChannelInterval",
		Version:      "2.3.0",
		DefaultValue: "1000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.ChannelCheckInterval.Init(base.mgr)

	p.BalanceCheckInterval = ParamItem{
		Key:          "queryCoord.checkBalanceInterval",
		Version:      "2.3.0",
		DefaultValue: "10000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.BalanceCheckInterval.Init(base.mgr)

	p.IndexCheckInterval = ParamItem{
		Key:          "queryCoord.checkIndexInterval",
		Version:      "2.3.0",
		DefaultValue: "10000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.IndexCheckInterval.Init(base.mgr)

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

	p.LeaderViewUpdateInterval = ParamItem{
		Key:          "queryCoord.leaderViewUpdateInterval",
		Doc:          "the interval duration(in seconds) for LeaderObserver to fetch LeaderView from querynodes",
		Version:      "2.3.4",
		DefaultValue: "1",
		PanicIfEmpty: true,
	}
	p.LeaderViewUpdateInterval.Init(base.mgr)

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
		DefaultValue: "2000",
		PanicIfEmpty: true,
		Doc:          "100ms, the timeout of check health rpc to query node",
		Export:       true,
	}
	p.CheckHealthRPCTimeout.Init(base.mgr)

	p.BrokerTimeout = ParamItem{
		Key:          "queryCoord.brokerTimeout",
		Version:      "2.3.0",
		DefaultValue: "5000",
		PanicIfEmpty: true,
		Doc:          "5000ms, querycoord broker rpc timeout",
		Export:       true,
	}
	p.BrokerTimeout.Init(base.mgr)

	p.CollectionRecoverTimesLimit = ParamItem{
		Key:          "queryCoord.collectionRecoverTimes",
		Version:      "2.3.3",
		DefaultValue: "3",
		PanicIfEmpty: true,
		Doc:          "if collection recover times reach the limit during loading state, release it",
		Export:       true,
	}
	p.CollectionRecoverTimesLimit.Init(base.mgr)

	p.ObserverTaskParallel = ParamItem{
		Key:          "queryCoord.observerTaskParallel",
		Version:      "2.3.2",
		DefaultValue: "16",
		PanicIfEmpty: true,
		Doc:          "the parallel observer dispatcher task number",
		Export:       true,
	}
	p.ObserverTaskParallel.Init(base.mgr)

	p.CheckAutoBalanceConfigInterval = ParamItem{
		Key:          "queryCoord.checkAutoBalanceConfigInterval",
		Version:      "2.3.3",
		DefaultValue: "10",
		PanicIfEmpty: true,
		Doc:          "the interval of check auto balance config",
		Export:       true,
	}
	p.CheckAutoBalanceConfigInterval.Init(base.mgr)

	p.CheckNodeSessionInterval = ParamItem{
		Key:          "queryCoord.checkNodeSessionInterval",
		Version:      "2.3.4",
		DefaultValue: "60",
		PanicIfEmpty: true,
		Doc:          "the interval(in seconds) of check querynode cluster session",
		Export:       true,
	}
	p.CheckNodeSessionInterval.Init(base.mgr)

	p.DistributionRequestTimeout = ParamItem{
		Key:          "queryCoord.distRequestTimeout",
		Version:      "2.3.6",
		DefaultValue: "5000",
		Doc:          "the request timeout for querycoord fetching data distribution from querynodes, in milliseconds",
		Export:       true,
	}
	p.DistributionRequestTimeout.Init(base.mgr)

	p.HeartBeatWarningLag = ParamItem{
		Key:          "queryCoord.heatbeatWarningLag",
		Version:      "2.3.6",
		DefaultValue: "5000",
		Doc:          "the lag value for querycoord report warning when last heatbeat is too old, in milliseconds",
		Export:       true,
	}
	p.HeartBeatWarningLag.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "queryCoord.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultCoordGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.CollectionObserverInterval = ParamItem{
		Key:          "queryCoord.collectionObserverInterval",
		Version:      "2.4.4",
		DefaultValue: "200",
		Doc:          "the interval of collection observer",
		Export:       false,
	}
	p.CollectionObserverInterval.Init(base.mgr)

	p.CheckExecutedFlagInterval = ParamItem{
		Key:          "queryCoord.checkExecutedFlagInterval",
		Version:      "2.4.4",
		DefaultValue: "100",
		Doc:          "the interval of check executed flag to force to pull dist",
		Export:       false,
	}
	p.CheckExecutedFlagInterval.Init(base.mgr)
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
	KnowhereThreadPoolSize        ParamItem `refreshable:"false"`
	ChunkRows                     ParamItem `refreshable:"false"`
	EnableInterimSegmentIndex     ParamItem `refreshable:"false"`
	InterimIndexNlist             ParamItem `refreshable:"false"`
	InterimIndexNProbe            ParamItem `refreshable:"false"`
	InterimIndexMemExpandRate     ParamItem `refreshable:"false"`
	InterimIndexBuildParallelRate ParamItem `refreshable:"false"`

	KnowhereScoreConsistency ParamItem `refreshable:"false"`

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

	// chunk cache
	ReadAheadPolicy     ParamItem `refreshable:"false"`
	ChunkCacheWarmingUp ParamItem `refreshable:"true"`

	GroupEnabled          ParamItem `refreshable:"true"`
	MaxReceiveChanSize    ParamItem `refreshable:"false"`
	MaxUnsolvedQueueSize  ParamItem `refreshable:"true"`
	MaxReadConcurrency    ParamItem `refreshable:"true"`
	MaxGpuReadConcurrency ParamItem `refreshable:"false"`
	MaxGroupNQ            ParamItem `refreshable:"true"`
	TopKMergeRatio        ParamItem `refreshable:"true"`
	CPURatio              ParamItem `refreshable:"true"`
	MaxTimestampLag       ParamItem `refreshable:"true"`
	GCEnabled             ParamItem `refreshable:"true"`

	GCHelperEnabled     ParamItem `refreshable:"false"`
	MinimumGOGCConfig   ParamItem `refreshable:"false"`
	MaximumGOGCConfig   ParamItem `refreshable:"false"`
	GracefulStopTimeout ParamItem `refreshable:"false"`

	// delete buffer
	MaxSegmentDeleteBuffer ParamItem `refreshable:"false"`
	DeleteBufferBlockSize  ParamItem `refreshable:"false"`

	// loader
	IoPoolSize ParamItem `refreshable:"false"`

	// schedule task policy.
	SchedulePolicyName                    ParamItem `refreshable:"false"`
	SchedulePolicyTaskQueueExpire         ParamItem `refreshable:"true"`
	SchedulePolicyEnableCrossUserGrouping ParamItem `refreshable:"true"`
	SchedulePolicyMaxPendingTaskPerUser   ParamItem `refreshable:"true"`

	// CGOPoolSize ratio to MaxReadConcurrency
	CGOPoolSizeRatio ParamItem `refreshable:"false"`

	EnableWorkerSQCostMetrics ParamItem `refreshable:"true"`

	MemoryIndexLoadPredictMemoryUsageFactor ParamItem `refreshable:"true"`
	BloomFilterApplyParallelFactor          ParamItem `refreshable:"true"`
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
		DefaultValue: "16",
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
			knowhereThreadPoolSize := uint32(hardware.GetCPUNum()) * uint32(factor)
			return strconv.FormatUint(uint64(knowhereThreadPoolSize), 10)
		},
		Doc:    "The number of threads in knowhere's thread pool. If disk is enabled, the pool size will multiply with knowhereThreadPoolNumRatio([1, 32]).",
		Export: true,
	}
	p.KnowhereThreadPoolSize.Init(base.mgr)

	p.ChunkRows = ParamItem{
		Key:          "queryNode.segcore.chunkRows",
		Version:      "2.0.0",
		DefaultValue: "128",
		Formatter: func(v string) string {
			if getAsInt(v) < 128 {
				return "128"
			}
			return v
		},
		Doc:    "The number of vectors in a chunk.",
		Export: true,
	}
	p.ChunkRows.Init(base.mgr)

	p.EnableInterimSegmentIndex = ParamItem{
		Key:          "queryNode.segcore.interimIndex.enableIndex",
		Version:      "2.0.0",
		DefaultValue: "false",
		Doc:          "Enable segment build with index to accelerate vector search when segment is in growing or binlog.",
		Export:       true,
	}
	p.EnableInterimSegmentIndex.Init(base.mgr)

	p.KnowhereScoreConsistency = ParamItem{
		Key:          "queryNode.segcore.knowhereScoreConsistency",
		Version:      "2.3.15",
		DefaultValue: "false",
		Doc:          "Enable knowhere strong consistency score computation logic",
		Export:       true,
	}

	p.KnowhereScoreConsistency.Init(base.mgr)

	p.InterimIndexNlist = ParamItem{
		Key:          "queryNode.segcore.interimIndex.nlist",
		Version:      "2.0.0",
		DefaultValue: "128",
		Doc:          "temp index nlist, recommend to set sqrt(chunkRows), must smaller than chunkRows/8",
		Export:       true,
	}
	p.InterimIndexNlist.Init(base.mgr)

	p.InterimIndexMemExpandRate = ParamItem{
		Key:          "queryNode.segcore.interimIndex.memExpansionRate",
		Version:      "2.0.0",
		DefaultValue: "1.15",
		Doc:          "extra memory needed by building interim index",
		Export:       true,
	}
	p.InterimIndexMemExpandRate.Init(base.mgr)

	p.InterimIndexBuildParallelRate = ParamItem{
		Key:          "queryNode.segcore.interimIndex.buildParallelRate",
		Version:      "2.3.19",
		DefaultValue: "0.5",
		Doc:          "the ratio of building interim index parallel matched with cpu num",
		Export:       true,
	}
	p.InterimIndexBuildParallelRate.Init(base.mgr)

	p.InterimIndexNProbe = ParamItem{
		Key:     "queryNode.segcore.interimIndex.nprobe",
		Version: "2.0.0",
		Formatter: func(v string) string {
			defaultNprobe := p.InterimIndexNlist.GetAsInt64() / 8
			nprobe := getAsInt64(v)
			if nprobe == 0 {
				nprobe = defaultNprobe
			}
			if nprobe > p.InterimIndexNlist.GetAsInt64() {
				return p.InterimIndexNlist.GetValue()
			}
			return strconv.FormatInt(nprobe, 10)
		},
		Doc:    "nprobe to search small index, based on your accuracy requirement, must smaller than nlist",
		Export: true,
	}
	p.InterimIndexNProbe.Init(base.mgr)

	p.LoadMemoryUsageFactor = ParamItem{
		Key:          "queryNode.loadMemoryUsageFactor",
		Version:      "2.0.0",
		DefaultValue: "2",
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

	p.ReadAheadPolicy = ParamItem{
		Key:          "queryNode.cache.readAheadPolicy",
		Version:      "2.3.2",
		DefaultValue: "willneed",
		Doc:          "The read ahead policy of chunk cache, options: `normal, random, sequential, willneed, dontneed`",
	}
	p.ReadAheadPolicy.Init(base.mgr)

	p.ChunkCacheWarmingUp = ParamItem{
		Key:          "queryNode.cache.warmup",
		Version:      "2.3.6",
		DefaultValue: "async",
		Doc: `options: async, sync, off. 
Specifies the necessity for warming up the chunk cache. 
1. If set to "sync" or "async," the original vector data will be synchronously/asynchronously loaded into the 
chunk cache during the load process. This approach has the potential to substantially reduce query/search latency
for a specific duration post-load, albeit accompanied by a concurrent increase in disk usage;
2. If set to "off," original vector data will only be loaded into the chunk cache during search/query.`,
	}
	p.ChunkCacheWarmingUp.Init(base.mgr)

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
			cpuNum := int64(hardware.GetCPUNum())
			concurrency := int64(float64(cpuNum) * ratio)
			if concurrency < 1 {
				return "1" // MaxReadConcurrency must >= 1
			} else if concurrency > cpuNum*100 {
				return strconv.FormatInt(cpuNum*100, 10) // MaxReadConcurrency must <= 100*cpuNum
			}
			return strconv.FormatInt(concurrency, 10)
		},
		Doc: `maxReadConcurrentRatio is the concurrency ratio of read task (search task and query task).
Max read concurrency would be the value of ` + "hardware.GetCPUNum * maxReadConcurrentRatio" + `.
It defaults to 2.0, which means max read concurrency would be the value of hardware.GetCPUNum * 2.
Max read concurrency must greater than or equal to 1, and less than or equal to hardware.GetCPUNum * 100.
(0, 100]`,
		Export: true,
	}
	p.MaxReadConcurrency.Init(base.mgr)

	p.MaxGpuReadConcurrency = ParamItem{
		Key:          "queryNode.scheduler.maxGpuReadConcurrency",
		Version:      "2.0.0",
		DefaultValue: "6",
	}
	p.MaxGpuReadConcurrency.Init(base.mgr)

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
		DefaultValue: "1000",
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
				// use local storage path to check correct device
				localStoragePath := base.Get("localStorage.path")
				if _, err := os.Stat(localStoragePath); os.IsNotExist(err) {
					os.MkdirAll(localStoragePath, os.ModePerm)
				}
				diskUsage, err := disk.Usage(localStoragePath)
				if err != nil {
					// panic(err)
					log.Fatal("failed to get disk usage", zap.String("localStoragePath", localStoragePath), zap.Error(err))
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

	p.DeleteBufferBlockSize = ParamItem{
		Key:          "queryNode.deleteBufferBlockSize",
		Version:      "2.3.5",
		Doc:          "delegator delete buffer block size when using list delete buffer",
		DefaultValue: "1048576", // 1MB
	}
	p.DeleteBufferBlockSize.Init(base.mgr)

	p.IoPoolSize = ParamItem{
		Key:          "queryNode.ioPoolSize",
		Version:      "2.3.0",
		DefaultValue: "0",
		Doc:          "Control how many goroutines will the loader use to pull files, if the given value is non-positive, the value will be set to CpuNum * 8, at least 32, and at most 256",
	}
	p.IoPoolSize.Init(base.mgr)

	// schedule read task policy.
	p.SchedulePolicyName = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.name",
		Version:      "2.3.0",
		DefaultValue: "fifo",
		Doc:          "Control how to schedule query/search read task in query node",
	}
	p.SchedulePolicyName.Init(base.mgr)
	p.SchedulePolicyTaskQueueExpire = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.taskQueueExpire",
		Version:      "2.3.0",
		DefaultValue: "60",
		Doc:          "Control how long (many seconds) that queue retains since queue is empty",
	}
	p.SchedulePolicyTaskQueueExpire.Init(base.mgr)
	p.SchedulePolicyEnableCrossUserGrouping = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.enableCrossUserGrouping",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          "Enable Cross user grouping when using user-task-polling policy. (Disable it if user's task can not merge each other)",
	}
	p.SchedulePolicyEnableCrossUserGrouping.Init(base.mgr)
	p.SchedulePolicyMaxPendingTaskPerUser = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.maxPendingTaskPerUser",
		Version:      "2.3.0",
		DefaultValue: "1024",
		Doc:          "Max pending task per user in scheduler",
	}
	p.SchedulePolicyMaxPendingTaskPerUser.Init(base.mgr)

	p.CGOPoolSizeRatio = ParamItem{
		Key:          "queryNode.segcore.cgoPoolSizeRatio",
		Version:      "2.3.0",
		DefaultValue: "2.0",
		Doc:          "cgo pool size ratio to max read concurrency",
	}
	p.CGOPoolSizeRatio.Init(base.mgr)

	p.EnableWorkerSQCostMetrics = ParamItem{
		Key:          "queryNode.enableWorkerSQCostMetrics",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          "whether use worker's cost to measure delegator's workload",
	}
	p.EnableWorkerSQCostMetrics.Init(base.mgr)

	p.MemoryIndexLoadPredictMemoryUsageFactor = ParamItem{
		Key:          "queryNode.memoryIndexLoadPredictMemoryUsageFactor",
		Version:      "2.3.8",
		DefaultValue: "2.5", // HNSW index needs more memory to load.
		Doc:          "memory usage prediction factor for memory index loaded",
	}
	p.MemoryIndexLoadPredictMemoryUsageFactor.Init(base.mgr)
	p.BloomFilterApplyParallelFactor = ParamItem{
		Key:          "queryNode.bloomFilterApplyBatchSize",
		Version:      "2.3.18",
		DefaultValue: "4",
		Doc:          "parallel factor when to apply pk to bloom filter, default to 4*CPU_CORE_NUM",
		Export:       true,
	}
	p.BloomFilterApplyParallelFactor.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	// --- CHANNEL ---
	WatchTimeoutInterval         ParamItem `refreshable:"false"`
	ChannelBalanceSilentDuration ParamItem `refreshable:"true"`
	ChannelBalanceInterval       ParamItem `refreshable:"true"`
	ChannelOperationRPCTimeout   ParamItem `refreshable:"true"`

	// --- SEGMENTS ---
	SegmentMaxSize                 ParamItem `refreshable:"false"`
	DiskSegmentMaxSize             ParamItem `refreshable:"true"`
	SegmentSealProportion          ParamItem `refreshable:"false"`
	SegAssignmentExpiration        ParamItem `refreshable:"false"`
	AllocLatestExpireAttempt       ParamItem `refreshable:"true"`
	SegmentMaxLifetime             ParamItem `refreshable:"false"`
	SegmentMaxIdleTime             ParamItem `refreshable:"false"`
	SegmentMinSizeFromIdleToSealed ParamItem `refreshable:"false"`
	SegmentMaxBinlogFileNumber     ParamItem `refreshable:"false"`
	AutoUpgradeSegmentIndex        ParamItem `refreshable:"true"`

	// compaction
	EnableCompaction     ParamItem `refreshable:"false"`
	EnableAutoCompaction ParamItem `refreshable:"true"`
	IndexBasedCompaction ParamItem `refreshable:"true"`

	CompactionRPCTimeout              ParamItem `refreshable:"true"`
	CompactionMaxParallelTasks        ParamItem `refreshable:"true"`
	CompactionWorkerParalleTasks      ParamItem `refreshable:"true"`
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
	GCRemoveConcurrent      ParamItem `refreshable:"false"`
	GCScanIntervalInHour    ParamItem `refreshable:"false"`
	EnableActiveStandby     ParamItem `refreshable:"false"`

	BindIndexNodeMode          ParamItem `refreshable:"false"`
	IndexNodeAddress           ParamItem `refreshable:"false"`
	WithCredential             ParamItem `refreshable:"false"`
	IndexNodeID                ParamItem `refreshable:"false"`
	IndexTaskSchedulerInterval ParamItem `refreshable:"false"`

	MinSegmentNumRowsToEnableIndex ParamItem `refreshable:"true"`
	// auto balance channel on datanode
	AutoBalance                    ParamItem `refreshable:"true"`
	CheckAutoBalanceConfigInterval ParamItem `refreshable:"false"`

	GracefulStopTimeout ParamItem `refreshable:"true"`
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

	p.ChannelOperationRPCTimeout = ParamItem{
		Key:          "dataCoord.channel.notifyChannelOperationTimeout",
		Version:      "2.2.3",
		DefaultValue: "5",
		Doc:          "Timeout notifing channel operations (in seconds).",
		Export:       true,
	}
	p.ChannelOperationRPCTimeout.Init(base.mgr)

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
		DefaultValue: "0.23",
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

	p.AllocLatestExpireAttempt = ParamItem{
		Key:          "dataCoord.segment.allocLatestExpireAttempt",
		Version:      "2.2.0",
		DefaultValue: "200",
		Doc:          "The time attempting to alloc latest lastExpire from rootCoord after restart",
		Export:       true,
	}
	p.AllocLatestExpireAttempt.Init(base.mgr)

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
		DefaultValue: "600",
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
		DefaultValue: "true",
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

	p.IndexBasedCompaction = ParamItem{
		Key:          "dataCoord.compaction.indexBasedCompaction",
		Version:      "2.0.0",
		DefaultValue: "true",
		Export:       true,
	}
	p.IndexBasedCompaction.Init(base.mgr)

	p.CompactionRPCTimeout = ParamItem{
		Key:          "dataCoord.compaction.rpcTimeout",
		Version:      "2.2.12",
		DefaultValue: "10",
		Export:       true,
	}
	p.CompactionRPCTimeout.Init(base.mgr)

	p.CompactionMaxParallelTasks = ParamItem{
		Key:          "dataCoord.compaction.maxParallelTaskNum",
		Version:      "2.2.12",
		DefaultValue: "10",
		Export:       true,
	}
	p.CompactionMaxParallelTasks.Init(base.mgr)

	p.CompactionWorkerParalleTasks = ParamItem{
		Key:          "dataCoord.compaction.workerMaxParallelTaskNum",
		Version:      "2.3.0",
		DefaultValue: "2",
		Export:       true,
	}
	p.CompactionWorkerParalleTasks.Init(base.mgr)

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
		DefaultValue: "0.85",
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
		DefaultValue: "900",
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

	p.GCScanIntervalInHour = ParamItem{
		Key:          "dataCoord.gc.scanInterval",
		Version:      "2.4.0",
		DefaultValue: "168", // hours, default 7 * 24 hours
		Doc:          "garbage collection scan residue interval in hours",
		Export:       true,
	}
	p.GCScanIntervalInHour.Init(base.mgr)

	// Do not set this to incredible small value, make sure this to be more than 10 minutes at least
	p.GCMissingTolerance = ParamItem{
		Key:          "dataCoord.gc.missingTolerance",
		Version:      "2.0.0",
		DefaultValue: "3600",
		Doc:          "file meta missing tolerance duration in seconds, default to 1hr",
		Export:       true,
	}
	p.GCMissingTolerance.Init(base.mgr)

	p.GCDropTolerance = ParamItem{
		Key:          "dataCoord.gc.dropTolerance",
		Version:      "2.0.0",
		DefaultValue: "10800",
		Doc:          "file belongs to dropped entity tolerance duration in seconds. 3600",
		Export:       true,
	}
	p.GCDropTolerance.Init(base.mgr)

	p.GCRemoveConcurrent = ParamItem{
		Key:          "dataCoord.gc.removeConcurrent",
		Version:      "2.3.4",
		DefaultValue: "32",
		Doc:          "number of concurrent goroutines to remove dropped s3 objects",
		Export:       true,
	}
	p.GCRemoveConcurrent.Init(base.mgr)

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

	p.AutoBalance = ParamItem{
		Key:          "dataCoord.autoBalance",
		Version:      "2.3.3",
		DefaultValue: "true",
		PanicIfEmpty: true,
		Doc:          "Enable auto balance",
		Export:       true,
	}
	p.AutoBalance.Init(base.mgr)

	p.CheckAutoBalanceConfigInterval = ParamItem{
		Key:          "dataCoord.checkAutoBalanceConfigInterval",
		Version:      "2.3.3",
		DefaultValue: "10",
		PanicIfEmpty: true,
		Doc:          "the interval of check auto balance config",
		Export:       true,
	}
	p.CheckAutoBalanceConfigInterval.Init(base.mgr)

	p.AutoUpgradeSegmentIndex = ParamItem{
		Key:          "dataCoord.autoUpgradeSegmentIndex",
		Version:      "2.3.4",
		DefaultValue: "false",
		PanicIfEmpty: true,
		Doc:          "whether auto upgrade segment index to index engine's version",
		Export:       true,
	}
	p.AutoUpgradeSegmentIndex.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "dataCoord.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultCoordGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`
	MaxParallelSyncTaskNum  ParamItem `refreshable:"true"`

	// skip mode
	FlowGraphSkipModeEnable   ParamItem `refreshable:"true"`
	FlowGraphSkipModeSkipNum  ParamItem `refreshable:"true"`
	FlowGraphSkipModeColdTime ParamItem `refreshable:"true"`

	// segment
	FlushInsertBufferSize  ParamItem `refreshable:"true"`
	FlushDeleteBufferBytes ParamItem `refreshable:"true"`
	BinLogMaxSize          ParamItem `refreshable:"true"`
	SyncPeriod             ParamItem `refreshable:"true"`

	// watchEvent
	WatchEventTicklerInterval ParamItem `refreshable:"false"`

	// io concurrency to add segment
	IOConcurrency ParamItem `refreshable:"false"`

	// Concurrency to handle compaction file read
	FileReadConcurrency ParamItem `refreshable:"false"`

	// memory management
	MemoryForceSyncEnable     ParamItem `refreshable:"true"`
	MemoryForceSyncSegmentNum ParamItem `refreshable:"true"`
	MemoryWatermark           ParamItem `refreshable:"true"`

	DataNodeTimeTickByRPC ParamItem `refreshable:"false"`
	// DataNode send timetick interval per collection
	DataNodeTimeTickInterval ParamItem `refreshable:"false"`

	// timeout for bulkinsert
	BulkInsertTimeoutSeconds ParamItem `refreshable:"true"`
	BulkInsertReadBufferSize ParamItem `refreshable:"true"`
	BulkInsertMaxMemorySize  ParamItem `refreshable:"true"`

	// Skip BF
	SkipBFStatsLoad ParamItem `refreshable:"true"`

	// channel
	ChannelWorkPoolSize ParamItem `refreshable:"true"`

	UpdateChannelCheckpointMaxParallel   ParamItem `refreshable:"true"`
	UpdateChannelCheckpointInterval      ParamItem `refreshable:"true"`
	UpdateChannelCheckpointRPCTimeout    ParamItem `refreshable:"true"`
	MaxChannelCheckpointsPerRPC          ParamItem `refreshable:"true"`
	ChannelCheckpointUpdateTickInSeconds ParamItem `refreshable:"true"`

	GracefulStopTimeout            ParamItem `refreshable:"true"`
	FlushMgrCleanInterval          ParamItem `refreshable:"true"`
	BloomFilterApplyParallelFactor ParamItem `refreshable:"true"`
}

func (p *dataNodeConfig) init(base *BaseTable) {
	p.FlowGraphMaxQueueLength = ParamItem{
		Key:          "dataNode.dataSync.flowGraph.maxQueueLength",
		Version:      "2.0.0",
		DefaultValue: "16",
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

	p.FlowGraphSkipModeEnable = ParamItem{
		Key:          "datanode.dataSync.skipMode.enable",
		Version:      "2.3.4",
		DefaultValue: "true",
		PanicIfEmpty: false,
		Doc:          "Support skip some timetick message to reduce CPU usage",
		Export:       true,
	}
	p.FlowGraphSkipModeEnable.Init(base.mgr)

	p.FlowGraphSkipModeSkipNum = ParamItem{
		Key:          "datanode.dataSync.skipMode.skipNum",
		Version:      "2.3.4",
		DefaultValue: "4",
		PanicIfEmpty: false,
		Doc:          "Consume one for every n records skipped",
		Export:       true,
	}
	p.FlowGraphSkipModeSkipNum.Init(base.mgr)

	p.FlowGraphSkipModeColdTime = ParamItem{
		Key:          "datanode.dataSync.skipMode.coldTime",
		Version:      "2.3.4",
		DefaultValue: "60",
		PanicIfEmpty: false,
		Doc:          "Turn on skip mode after there are only timetick msg for x seconds",
		Export:       true,
	}
	p.FlowGraphSkipModeColdTime.Init(base.mgr)

	p.MaxParallelSyncTaskNum = ParamItem{
		Key:          "dataNode.dataSync.maxParallelSyncTaskNum",
		Version:      "2.3.0",
		DefaultValue: "6",
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

	p.WatchEventTicklerInterval = ParamItem{
		Key:          "datanode.segment.watchEventTicklerInterval",
		Version:      "2.2.3",
		DefaultValue: "15",
	}
	p.WatchEventTicklerInterval.Init(base.mgr)

	p.IOConcurrency = ParamItem{
		Key:          "dataNode.dataSync.ioConcurrency",
		Version:      "2.0.0",
		DefaultValue: "16",
	}
	p.IOConcurrency.Init(base.mgr)

	p.FileReadConcurrency = ParamItem{
		Key:          "dataNode.multiRead.concurrency",
		Version:      "2.0.0",
		DefaultValue: "16",
	}
	p.FileReadConcurrency.Init(base.mgr)

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

	p.BulkInsertTimeoutSeconds = ParamItem{
		Key:          "datanode.bulkinsert.timeout.seconds",
		Version:      "2.3.0",
		PanicIfEmpty: false,
		DefaultValue: "18000",
	}
	p.BulkInsertTimeoutSeconds.Init(base.mgr)

	p.BulkInsertReadBufferSize = ParamItem{
		Key:          "datanode.bulkinsert.readBufferSize",
		Version:      "2.3.4",
		PanicIfEmpty: false,
		DefaultValue: "16777216",
	}
	p.BulkInsertReadBufferSize.Init(base.mgr)

	p.BulkInsertMaxMemorySize = ParamItem{
		Key:          "datanode.bulkinsert.maxMemorySize",
		Version:      "2.3.4",
		PanicIfEmpty: false,
		DefaultValue: "6442450944",
	}
	p.BulkInsertMaxMemorySize.Init(base.mgr)

	p.ChannelWorkPoolSize = ParamItem{
		Key:          "datanode.channel.workPoolSize",
		Version:      "2.3.2",
		PanicIfEmpty: false,
		DefaultValue: "-1",
	}
	p.ChannelWorkPoolSize.Init(base.mgr)

	p.UpdateChannelCheckpointMaxParallel = ParamItem{
		Key:          "datanode.channel.updateChannelCheckpointMaxParallel",
		Version:      "2.3.4",
		PanicIfEmpty: false,
		DefaultValue: "10",
	}
	p.UpdateChannelCheckpointMaxParallel.Init(base.mgr)

	p.UpdateChannelCheckpointInterval = ParamItem{
		Key:          "datanode.channel.updateChannelCheckpointInterval",
		Version:      "2.4.0",
		Doc:          "the interval duration(in seconds) for datanode to update channel checkpoint of each channel",
		DefaultValue: "60",
	}
	p.UpdateChannelCheckpointInterval.Init(base.mgr)

	p.UpdateChannelCheckpointRPCTimeout = ParamItem{
		Key:          "datanode.channel.updateChannelCheckpointRPCTimeout",
		Version:      "2.4.0",
		Doc:          "timeout in seconds for UpdateChannelCheckpoint RPC call",
		DefaultValue: "20",
	}
	p.UpdateChannelCheckpointRPCTimeout.Init(base.mgr)

	p.MaxChannelCheckpointsPerRPC = ParamItem{
		Key:          "datanode.channel.maxChannelCheckpointsPerPRC",
		Version:      "2.4.0",
		Doc:          "The maximum number of channel checkpoints per UpdateChannelCheckpoint RPC.",
		DefaultValue: "128",
	}
	p.MaxChannelCheckpointsPerRPC.Init(base.mgr)

	p.ChannelCheckpointUpdateTickInSeconds = ParamItem{
		Key:          "datanode.channel.channelCheckpointUpdateTickInSeconds",
		Version:      "2.4.0",
		Doc:          "The frequency, in seconds, at which the channel checkpoint updater executes updates.",
		DefaultValue: "10",
	}
	p.ChannelCheckpointUpdateTickInSeconds.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "datanode.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.FlushMgrCleanInterval = ParamItem{
		Key:          "datanode.flushMgrCleanInterval",
		Version:      "2.3.16",
		DefaultValue: strconv.Itoa(DefaultFlushMgrCleanInterval),
		Doc:          "seconds. flush manager clean check interval",
		Export:       true,
	}
	p.FlushMgrCleanInterval.Init(base.mgr)
	p.BloomFilterApplyParallelFactor = ParamItem{
		Key:          "datanode.bloomFilterApplyBatchSize",
		Version:      "2.3.18",
		DefaultValue: "4",
		Doc:          "parallel factor when to apply pk to bloom filter, default to 4*CPU_CORE_NUM",
		Export:       true,
	}
	p.BloomFilterApplyParallelFactor.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- indexnode ---
type indexNodeConfig struct {
	BuildParallel ParamItem `refreshable:"false"`
	// enable disk
	EnableDisk             ParamItem `refreshable:"false"`
	DiskCapacityLimit      ParamItem `refreshable:"true"`
	MaxDiskUsagePercentage ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"true"`
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
		Doc:          "seconds. force stop node without graceful stop",
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

func (params *ComponentParam) Save(key string, value string) error {
	return params.baseTable.Save(key, value)
}

func (params *ComponentParam) SaveGroup(group map[string]string) error {
	return params.baseTable.SaveGroup(group)
}

func (params *ComponentParam) Remove(key string) error {
	return params.baseTable.Remove(key)
}

func (params *ComponentParam) Reset(key string) error {
	return params.baseTable.Reset(key)
}

func (params *ComponentParam) GetWithDefault(key string, dft string) string {
	return params.baseTable.GetWithDefault(key, dft)
}
