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

package paramtable

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

const (
	// DefaultIndexSliceSize defines the default slice size of index file when serializing.
	DefaultIndexSliceSize                      = 16
	DefaultGracefulTime                        = 5000 // ms
	DefaultGracefulStopTimeout                 = 1800 // s, for node
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
	AutoIndexConfig AutoIndexConfig
	GpuConfig       gpuConfig
	TraceCfg        traceConfig

	RootCoordCfg   rootCoordConfig
	ProxyCfg       proxyConfig
	QueryCoordCfg  queryCoordConfig
	QueryNodeCfg   queryNodeConfig
	DataCoordCfg   dataCoordConfig
	DataNodeCfg    dataNodeConfig
	IndexNodeCfg   indexNodeConfig
	KnowhereConfig knowhereConfig
	HTTPCfg        httpConfig
	LogCfg         logConfig
	RoleCfg        roleConfig
	RbacConfig     rbacConfig
	StreamingCfg   streamingConfig

	InternalTLSCfg InternalTLSConfig

	RootCoordGrpcServerCfg     GrpcServerConfig
	ProxyGrpcServerCfg         GrpcServerConfig
	QueryCoordGrpcServerCfg    GrpcServerConfig
	QueryNodeGrpcServerCfg     GrpcServerConfig
	DataCoordGrpcServerCfg     GrpcServerConfig
	DataNodeGrpcServerCfg      GrpcServerConfig
	IndexNodeGrpcServerCfg     GrpcServerConfig
	StreamingNodeGrpcServerCfg GrpcServerConfig

	RootCoordGrpcClientCfg      GrpcClientConfig
	ProxyGrpcClientCfg          GrpcClientConfig
	QueryCoordGrpcClientCfg     GrpcClientConfig
	QueryNodeGrpcClientCfg      GrpcClientConfig
	DataCoordGrpcClientCfg      GrpcClientConfig
	DataNodeGrpcClientCfg       GrpcClientConfig
	IndexNodeGrpcClientCfg      GrpcClientConfig
	StreamingCoordGrpcClientCfg GrpcClientConfig
	StreamingNodeGrpcClientCfg  GrpcClientConfig
	IntegrationTestCfg          integrationTestConfig

	RuntimeConfig runtimeConfig
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
	p.StreamingCfg.init(bt)
	p.HTTPCfg.init(bt)
	p.LogCfg.init(bt)
	p.RoleCfg.init(bt)
	p.RbacConfig.init(bt)
	p.GpuConfig.init(bt)
	p.KnowhereConfig.init(bt)

	p.InternalTLSCfg.Init(bt)

	p.RootCoordGrpcServerCfg.Init("rootCoord", bt)
	p.ProxyGrpcServerCfg.Init("proxy", bt)
	p.ProxyGrpcServerCfg.InternalPort.Export = true
	p.QueryCoordGrpcServerCfg.Init("queryCoord", bt)
	p.QueryNodeGrpcServerCfg.Init("queryNode", bt)
	p.DataCoordGrpcServerCfg.Init("dataCoord", bt)
	p.DataNodeGrpcServerCfg.Init("dataNode", bt)
	p.IndexNodeGrpcServerCfg.Init("indexNode", bt)
	p.StreamingNodeGrpcServerCfg.Init("streamingNode", bt)

	p.RootCoordGrpcClientCfg.Init("rootCoord", bt)
	p.ProxyGrpcClientCfg.Init("proxy", bt)
	p.QueryCoordGrpcClientCfg.Init("queryCoord", bt)
	p.QueryNodeGrpcClientCfg.Init("queryNode", bt)
	p.DataCoordGrpcClientCfg.Init("dataCoord", bt)
	p.DataNodeGrpcClientCfg.Init("dataNode", bt)
	p.IndexNodeGrpcClientCfg.Init("indexNode", bt)
	p.StreamingCoordGrpcClientCfg.Init("streamingCoord", bt)
	p.StreamingNodeGrpcClientCfg.Init("streamingNode", bt)

	p.IntegrationTestCfg.init(bt)
}

func (p *ComponentParam) GetComponentConfigurations(componentName string, sub string) map[string]string {
	allownPrefixs := append(globalConfigPrefixs(), componentName+".")
	return p.baseTable.mgr.GetBy(config.WithSubstr(sub), config.WithOneOfPrefixs(allownPrefixs...))
}

func (p *ComponentParam) GetAll() map[string]string {
	return p.baseTable.mgr.GetConfigs()
}

func (p *ComponentParam) GetConfigsView() map[string]string {
	return p.baseTable.mgr.GetConfigsView()
}

func (p *ComponentParam) Watch(key string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.Register(key, watcher)
}

func (p *ComponentParam) WatchKeyPrefix(keyPrefix string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, watcher)
}

func (p *ComponentParam) Unwatch(key string, watcher config.EventHandler) {
	p.baseTable.mgr.Dispatcher.Unregister(key, watcher)
}

// FOR TEST

// clean all config event in dispatcher
func (p *ComponentParam) CleanEvent() {
	p.baseTable.mgr.Dispatcher.Clean()
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
	DataNodeSubName       ParamItem `refreshable:"false"`

	DefaultPartitionName ParamItem `refreshable:"false"`
	DefaultIndexName     ParamItem `refreshable:"true"`
	EntityExpirationTTL  ParamItem `refreshable:"true"`

	IndexSliceSize                      ParamItem `refreshable:"false"`
	HighPriorityThreadCoreCoefficient   ParamItem `refreshable:"false"`
	MiddlePriorityThreadCoreCoefficient ParamItem `refreshable:"false"`
	LowPriorityThreadCoreCoefficient    ParamItem `refreshable:"false"`
	EnableMaterializedView              ParamItem `refreshable:"false"`
	BuildIndexThreadPoolRatio           ParamItem `refreshable:"false"`
	MaxDegree                           ParamItem `refreshable:"true"`
	SearchListSize                      ParamItem `refreshable:"true"`
	PQCodeBudgetGBRatio                 ParamItem `refreshable:"true"`
	BuildNumThreadsRatio                ParamItem `refreshable:"true"`
	SearchCacheBudgetGBRatio            ParamItem `refreshable:"true"`
	LoadNumThreadRatio                  ParamItem `refreshable:"true"`
	BeamWidthRatio                      ParamItem `refreshable:"true"`
	GracefulTime                        ParamItem `refreshable:"true"`
	GracefulStopTimeout                 ParamItem `refreshable:"true"`

	StorageType ParamItem `refreshable:"false"`
	SimdType    ParamItem `refreshable:"false"`

	AuthorizationEnabled  ParamItem `refreshable:"false"`
	SuperUsers            ParamItem `refreshable:"true"`
	DefaultRootPassword   ParamItem `refreshable:"false"`
	RootShouldBindRole    ParamItem `refreshable:"true"`
	EnablePublicPrivilege ParamItem `refreshable:"false"`

	ClusterName ParamItem `refreshable:"false"`

	SessionTTL        ParamItem `refreshable:"false"`
	SessionRetryTimes ParamItem `refreshable:"false"`

	PreCreatedTopicEnabled ParamItem `refreshable:"true"`
	TopicNames             ParamItem `refreshable:"true"`
	TimeTicker             ParamItem `refreshable:"true"`

	JSONMaxLength ParamItem `refreshable:"false"`

	MetricsPort ParamItem `refreshable:"false"`

	// lock related params
	EnableLockMetrics           ParamItem `refreshable:"false"`
	LockSlowLogInfoThreshold    ParamItem `refreshable:"true"`
	LockSlowLogWarnThreshold    ParamItem `refreshable:"true"`
	MaxWLockConditionalWaitTime ParamItem `refreshable:"true"`

	StorageScheme             ParamItem `refreshable:"false"`
	EnableStorageV2           ParamItem `refreshable:"false"`
	StoragePathPrefix         ParamItem `refreshable:"false"`
	TTMsgEnabled              ParamItem `refreshable:"true"`
	TraceLogMode              ParamItem `refreshable:"true"`
	BloomFilterSize           ParamItem `refreshable:"true"`
	BloomFilterType           ParamItem `refreshable:"true"`
	MaxBloomFalsePositive     ParamItem `refreshable:"true"`
	BloomFilterApplyBatchSize ParamItem `refreshable:"true"`
	PanicWhenPluginFail       ParamItem `refreshable:"false"`
	CollectionReplicateEnable ParamItem `refreshable:"true"`

	UsePartitionKeyAsClusteringKey ParamItem `refreshable:"true"`
	UseVectorAsClusteringKey       ParamItem `refreshable:"true"`
	EnableVectorClusteringKey      ParamItem `refreshable:"true"`

	// GC
	GCEnabled         ParamItem `refreshable:"false"`
	GCHelperEnabled   ParamItem `refreshable:"false"`
	MaximumGOGCConfig ParamItem `refreshable:"false"`
	MinimumGOGCConfig ParamItem `refreshable:"false"`

	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"false"`
	ReadOnlyPrivileges                  ParamItem `refreshable:"false"`
	ReadWritePrivileges                 ParamItem `refreshable:"false"`
	AdminPrivileges                     ParamItem `refreshable:"false"`

	// Local RPC enabled for milvus internal communication when mix or standalone mode.
	LocalRPCEnabled ParamItem `refreshable:"false"`

	SyncTaskPoolReleaseTimeoutSeconds ParamItem `refreshable:"true"`

	EnabledOptimizeExpr               ParamItem `refreshable:"true"`
	EnabledJSONKeyStats               ParamItem `refreshable:"true"`
	EnabledGrowingSegmentJSONKeyStats ParamItem `refreshable:"true"`
}

func (p *commonConfig) init(base *BaseTable) {
	// must init cluster prefix first
	p.ClusterPrefix = ParamItem{
		Key:          "msgChannel.chanNamePrefix.cluster",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.cluster"},
		DefaultValue: "by-dev",
		Doc: `Root name prefix of the channel when a message channel is created.
It is recommended to change this parameter before starting Milvus for the first time.
To share a Pulsar instance among multiple Milvus instances, consider changing this to a name rather than the default one for each Milvus instance before you start them.`,
		PanicIfEmpty: true,
		Forbidden:    true,
		Export:       true,
	}
	p.ClusterPrefix.Init(base.mgr)

	pulsarPartitionKeyword := "-partition-"
	chanNamePrefix := func(prefix string) string {
		str := strings.Join([]string{p.ClusterPrefix.GetValue(), prefix}, "-")
		if strings.Contains(str, pulsarPartitionKeyword) {
			// there is a bug in pulsar client go, please refer to https://github.com/milvus-io/milvus/issues/28675
			panic("channel name can not contains '-partition-'")
		}
		return str
	}

	// --- rootcoord ---
	p.RootCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordTimeTick",
		DefaultValue: "rootcoord-timetick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordTimeTick"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the root coord publishes time tick messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.rootCoordTimeTick}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.RootCoordTimeTick.Init(base.mgr)

	p.RootCoordStatistics = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordStatistics",
		DefaultValue: "rootcoord-statistics",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordStatistics"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the root coord publishes its own statistics messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.rootCoordStatistics}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.RootCoordStatistics.Init(base.mgr)

	p.RootCoordDml = ParamItem{
		Key:          "msgChannel.chanNamePrefix.rootCoordDml",
		DefaultValue: "rootcoord-dml",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.rootCoordDml"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the root coord publishes Data Manipulation Language (DML) messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.rootCoordDml}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.RootCoordDml.Init(base.mgr)

	p.ReplicateMsgChannel = ParamItem{
		Key:          "msgChannel.chanNamePrefix.replicateMsg",
		DefaultValue: "replicate-msg",
		Version:      "2.3.2",
		FallbackKeys: []string{"common.chanNamePrefix.replicateMsg"},
		PanicIfEmpty: true,
		Formatter:    chanNamePrefix,
		Export:       true,
	}
	p.ReplicateMsgChannel.Init(base.mgr)

	p.QueryCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.queryTimeTick",
		DefaultValue: "queryTimeTick",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.queryTimeTick"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the query node publishes time tick messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.queryTimeTick}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.QueryCoordTimeTick.Init(base.mgr)

	p.DataCoordTimeTick = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordTimeTick",
		DefaultValue: "datacoord-timetick-channel",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordTimeTick"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the data coord publishes time tick messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.dataCoordTimeTick}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.DataCoordTimeTick.Init(base.mgr)

	p.DataCoordSegmentInfo = ParamItem{
		Key:          "msgChannel.chanNamePrefix.dataCoordSegmentInfo",
		DefaultValue: "segment-info-channel",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.chanNamePrefix.dataCoordSegmentInfo"},
		PanicIfEmpty: true,
		Doc: `Sub-name prefix of the message channel where the data coord publishes segment information messages.
The complete channel name prefix is ${msgChannel.chanNamePrefix.cluster}-${msgChannel.chanNamePrefix.dataCoordSegmentInfo}
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.DataCoordSegmentInfo.Init(base.mgr)

	p.DataCoordSubName = ParamItem{
		Key:          "msgChannel.subNamePrefix.dataCoordSubNamePrefix",
		DefaultValue: "dataCoord",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataCoordSubNamePrefix"},
		PanicIfEmpty: true,
		Doc: `Subscription name prefix of the data coord.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
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
		DefaultValue: "dataNode",
		Version:      "2.1.0",
		FallbackKeys: []string{"common.subNamePrefix.dataNodeSubNamePrefix"},
		PanicIfEmpty: true,
		Doc: `Subscription name prefix of the data node.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Formatter: chanNamePrefix,
		Export:    true,
	}
	p.DataNodeSubName.Init(base.mgr)

	p.DefaultPartitionName = ParamItem{
		Key:          "common.defaultPartitionName",
		Version:      "2.0.0",
		DefaultValue: "_default",
		Forbidden:    true,
		Doc:          "Name of the default partition when a collection is created",
		Export:       true,
	}
	p.DefaultPartitionName.Init(base.mgr)

	p.DefaultIndexName = ParamItem{
		Key:          "common.defaultIndexName",
		Version:      "2.0.0",
		DefaultValue: "_default_idx",
		Doc:          "Name of the index when it is created with name unspecified",
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
		Doc:          "Index slice size in MB",
		Export:       true,
	}
	p.IndexSliceSize.Init(base.mgr)

	p.EnableMaterializedView = ParamItem{
		Key:          "common.materializedView.enabled",
		Version:      "2.4.6",
		DefaultValue: "true", // 2.5.4 version becomes default true
	}
	p.EnableMaterializedView.Init(base.mgr)

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

	p.BuildIndexThreadPoolRatio = ParamItem{
		Key:          "common.buildIndexThreadPoolRatio",
		Version:      "2.4.0",
		DefaultValue: strconv.FormatFloat(DefaultKnowhereThreadPoolNumRatioInBuildOfStandalone, 'f', 2, 64),
		Export:       true,
	}
	p.BuildIndexThreadPoolRatio.Init(base.mgr)

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

	p.DefaultRootPassword = ParamItem{
		Key:          "common.security.defaultRootPassword",
		Version:      "2.4.7",
		Doc:          "default password for root user. The maximum length is 72 characters, and double quotes are required.",
		DefaultValue: "\"Milvus\"",
		Export:       true,
	}
	p.DefaultRootPassword.Init(base.mgr)

	p.RootShouldBindRole = ParamItem{
		Key:          "common.security.rootShouldBindRole",
		Version:      "2.5.4",
		Doc:          "Whether the root user should bind a role when the authorization is enabled.",
		DefaultValue: "false",
		Export:       true,
	}
	p.RootShouldBindRole.Init(base.mgr)

	p.EnablePublicPrivilege = ParamItem{
		Key: "common.security.enablePublicPrivilege",
		Formatter: func(originValue string) string { // compatible with old version
			fallbackValue := base.Get("proxy.enablePublicPrivilege")
			if fallbackValue == "false" {
				return "false"
			}
			return originValue
		},
		Version:      "2.5.4",
		Doc:          "Whether to enable public privilege",
		DefaultValue: "true",
		Export:       true,
	}
	p.EnablePublicPrivilege.Init(base.mgr)

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

	p.MaxWLockConditionalWaitTime = ParamItem{
		Key:          "common.locks.maxWLockConditionalWaitTime",
		Version:      "2.5.4",
		DefaultValue: "600",
		Doc:          "maximum seconds for waiting wlock conditional",
		Export:       true,
	}
	p.MaxWLockConditionalWaitTime.Init(base.mgr)

	p.EnableStorageV2 = ParamItem{
		Key:          "common.storage.enablev2",
		Version:      "2.3.1",
		DefaultValue: "false",
		Export:       true,
	}
	p.EnableStorageV2.Init(base.mgr)

	p.StorageScheme = ParamItem{
		Key:          "common.storage.scheme",
		Version:      "2.3.4",
		DefaultValue: "s3",
		Export:       true,
	}
	p.StorageScheme.Init(base.mgr)

	p.StoragePathPrefix = ParamItem{
		Key:          "common.storage.pathPrefix",
		Version:      "2.3.4",
		DefaultValue: "",
	}
	p.StoragePathPrefix.Init(base.mgr)

	p.TTMsgEnabled = ParamItem{
		Key:          "common.ttMsgEnabled",
		Version:      "2.3.2",
		DefaultValue: "true",
		Doc: `Whether to disable the internal time messaging mechanism for the system. 
If disabled (set to false), the system will not allow DML operations, including insertion, deletion, queries, and searches. 
This helps Milvus-CDC synchronize incremental data`,
		Export: true,
	}
	p.TTMsgEnabled.Init(base.mgr)

	p.CollectionReplicateEnable = ParamItem{
		Key:          "common.collectionReplicateEnable",
		Version:      "2.4.16",
		DefaultValue: "false",
		Doc:          `Whether to enable collection replication.`,
		Export:       true,
	}
	p.CollectionReplicateEnable.Init(base.mgr)

	p.TraceLogMode = ParamItem{
		Key:          "common.traceLogMode",
		Version:      "2.3.4",
		DefaultValue: "0",
		Doc:          "trace request info",
		Export:       true,
	}
	p.TraceLogMode.Init(base.mgr)

	p.BloomFilterSize = ParamItem{
		Key:          "common.bloomFilterSize",
		Version:      "2.3.2",
		DefaultValue: "100000",
		Doc:          "bloom filter initial size",
		Export:       true,
	}
	p.BloomFilterSize.Init(base.mgr)

	p.BloomFilterType = ParamItem{
		Key:          "common.bloomFilterType",
		Version:      "2.4.3",
		DefaultValue: "BlockedBloomFilter",
		Doc:          "bloom filter type, support BasicBloomFilter and BlockedBloomFilter",
		Export:       true,
	}
	p.BloomFilterType.Init(base.mgr)

	p.MaxBloomFalsePositive = ParamItem{
		Key:          "common.maxBloomFalsePositive",
		Version:      "2.3.2",
		DefaultValue: "0.001",
		Doc:          "max false positive rate for bloom filter",
		Export:       true,
	}
	p.MaxBloomFalsePositive.Init(base.mgr)

	p.BloomFilterApplyBatchSize = ParamItem{
		Key:          "common.bloomFilterApplyBatchSize",
		Version:      "2.4.5",
		DefaultValue: "1000",
		Doc:          "batch size when to apply pk to bloom filter",
		Export:       true,
	}
	p.BloomFilterApplyBatchSize.Init(base.mgr)

	p.PanicWhenPluginFail = ParamItem{
		Key:          "common.panicWhenPluginFail",
		Version:      "2.4.2",
		DefaultValue: "true",
		Doc:          "panic or not when plugin fail to init",
	}
	p.PanicWhenPluginFail.Init(base.mgr)

	p.UsePartitionKeyAsClusteringKey = ParamItem{
		Key:          "common.usePartitionKeyAsClusteringKey",
		Version:      "2.4.6",
		Doc:          "if true, do clustering compaction and segment prune on partition key field",
		DefaultValue: "false",
		Export:       true,
	}
	p.UsePartitionKeyAsClusteringKey.Init(base.mgr)

	p.UseVectorAsClusteringKey = ParamItem{
		Key:          "common.useVectorAsClusteringKey",
		Version:      "2.4.6",
		Doc:          "if true, do clustering compaction and segment prune on vector field",
		DefaultValue: "false",
		Export:       true,
	}
	p.UseVectorAsClusteringKey.Init(base.mgr)

	p.EnableVectorClusteringKey = ParamItem{
		Key:          "common.enableVectorClusteringKey",
		Version:      "2.4.6",
		Doc:          "if true, enable vector clustering key and vector clustering compaction",
		DefaultValue: "false",
		Export:       true,
	}
	p.EnableVectorClusteringKey.Init(base.mgr)

	p.GCEnabled = ParamItem{
		Key:          "common.gcenabled",
		FallbackKeys: []string{"queryNode.gcenabled"},
		Version:      "2.4.7",
		DefaultValue: "true",
	}
	p.GCEnabled.Init(base.mgr)

	p.GCHelperEnabled = ParamItem{
		Key:          "common.gchelper.enabled",
		FallbackKeys: []string{"queryNode.gchelper.enabled"},
		Version:      "2.4.7",
		DefaultValue: "true",
	}
	p.GCHelperEnabled.Init(base.mgr)

	p.OverloadedMemoryThresholdPercentage = ParamItem{
		Key:          "common.overloadedMemoryThresholdPercentage",
		Version:      "2.4.7",
		DefaultValue: "90",
		PanicIfEmpty: true,
		Formatter: func(v string) string {
			return fmt.Sprintf("%f", getAsFloat(v)/100)
		},
	}
	p.OverloadedMemoryThresholdPercentage.Init(base.mgr)

	p.MaximumGOGCConfig = ParamItem{
		Key:          "common.gchelper.maximumGoGC",
		FallbackKeys: []string{"queryNode.gchelper.maximumGoGC"},
		Version:      "2.4.7",
		DefaultValue: "200",
	}
	p.MaximumGOGCConfig.Init(base.mgr)

	p.MinimumGOGCConfig = ParamItem{
		Key:          "common.gchelper.minimumGoGC",
		FallbackKeys: []string{"queryNode.gchelper.minimumGoGC"},
		Version:      "2.4.7",
		DefaultValue: "30",
	}
	p.MinimumGOGCConfig.Init(base.mgr)

	p.ReadOnlyPrivileges = ParamItem{
		Key:     "common.security.readonly.privileges",
		Version: "2.4.7",
		Doc:     `use to override the default value of read-only privileges,  example: "PrivilegeQuery,PrivilegeSearch"`,
	}
	p.ReadOnlyPrivileges.Init(base.mgr)

	p.ReadWritePrivileges = ParamItem{
		Key:     "common.security.readwrite.privileges",
		Version: "2.4.7",
		Doc:     `use to override the default value of read-write privileges,  example: "PrivilegeCreateCollection,PrivilegeDropCollection"`,
	}
	p.ReadWritePrivileges.Init(base.mgr)

	p.AdminPrivileges = ParamItem{
		Key:     "common.security.admin.privileges",
		Version: "2.4.7",
		Doc:     `use to override the default value of admin privileges,  example: "PrivilegeCreateOwnership,PrivilegeDropOwnership"`,
	}
	p.AdminPrivileges.Init(base.mgr)

	p.LocalRPCEnabled = ParamItem{
		Key:          "common.localRPCEnabled",
		Version:      "2.4.18",
		DefaultValue: "false",
		Doc:          `enable local rpc for internal communication when mix or standalone mode.`,
		Export:       true,
	}
	p.LocalRPCEnabled.Init(base.mgr)

	p.SyncTaskPoolReleaseTimeoutSeconds = ParamItem{
		Key:          "common.sync.taskPoolReleaseTimeoutSeconds",
		DefaultValue: "60",
		Version:      "2.4.19",
		Doc:          "The maximum time to wait for the task to finish and release resources in the pool",
		Export:       true,
	}
	p.SyncTaskPoolReleaseTimeoutSeconds.Init(base.mgr)

	p.EnabledJSONKeyStats = ParamItem{
		Key:          "common.enabledJSONKeyStats",
		Version:      "2.5.5",
		DefaultValue: "false",
		Doc:          "Indicates sealedsegment whether to enable JSON key stats",
		Export:       true,
	}
	p.EnabledJSONKeyStats.Init(base.mgr)

	p.EnabledOptimizeExpr = ParamItem{
		Key:          "common.enabledOptimizeExpr",
		Version:      "2.5.6",
		DefaultValue: "true",
		Doc:          "Indicates whether to enable optimize expr",
		Export:       true,
	}
	p.EnabledOptimizeExpr.Init(base.mgr)
	p.EnabledGrowingSegmentJSONKeyStats = ParamItem{
		Key:          "common.enabledGrowingSegmentJSONKeyStats",
		Version:      "2.5.5",
		DefaultValue: "false",
		Doc:          "Indicates growingsegment whether to enable JSON key stats",
		Export:       true,
	}
	p.EnabledGrowingSegmentJSONKeyStats.Init(base.mgr)
}

type gpuConfig struct {
	InitSize ParamItem `refreshable:"false"`
	MaxSize  ParamItem `refreshable:"false"`
}

func (t *gpuConfig) init(base *BaseTable) {
	t.InitSize = ParamItem{
		Key:          "gpu.initMemSize",
		Version:      "2.3.4",
		Doc:          `Gpu Memory Pool init size`,
		Export:       true,
		DefaultValue: "2048",
	}
	t.InitSize.Init(base.mgr)

	t.MaxSize = ParamItem{
		Key:          "gpu.maxMemSize",
		Version:      "2.3.4",
		Doc:          `Gpu Memory Pool Max size`,
		Export:       true,
		DefaultValue: "4096",
	}
	t.MaxSize.Init(base.mgr)
}

type traceConfig struct {
	Exporter           ParamItem `refreshable:"false"`
	SampleFraction     ParamItem `refreshable:"false"`
	JaegerURL          ParamItem `refreshable:"false"`
	OtlpEndpoint       ParamItem `refreshable:"false"`
	OtlpMethod         ParamItem `refreshable:"false"`
	OtlpSecure         ParamItem `refreshable:"false"`
	InitTimeoutSeconds ParamItem `refreshable:"false"`
}

func (t *traceConfig) init(base *BaseTable) {
	t.Exporter = ParamItem{
		Key:     "trace.exporter",
		Version: "2.3.0",
		Doc: `trace exporter type, default is stdout,
optional values: ['noop','stdout', 'jaeger', 'otlp']`,
		DefaultValue: "noop",
		Export:       true,
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
		Doc:     `example: "127.0.0.1:4317" for grpc, "127.0.0.1:4318" for http`,
		Export:  true,
	}
	t.OtlpEndpoint.Init(base.mgr)

	t.OtlpMethod = ParamItem{
		Key:          "trace.otlp.method",
		Version:      "2.4.7",
		DefaultValue: "",
		Doc:          `otlp export method, acceptable values: ["grpc", "http"],  using "grpc" by default`,
		Export:       true,
	}
	t.OtlpMethod.Init(base.mgr)

	t.OtlpSecure = ParamItem{
		Key:          "trace.otlp.secure",
		Version:      "2.4.0",
		DefaultValue: "true",
		Export:       true,
	}
	t.OtlpSecure.Init(base.mgr)

	t.InitTimeoutSeconds = ParamItem{
		Key:          "trace.initTimeoutSeconds",
		Version:      "2.4.4",
		DefaultValue: "10",
		Export:       true,
		Doc:          "segcore initialization timeout in seconds, preventing otlp grpc hangs forever",
	}
	t.InitTimeoutSeconds.Init(base.mgr)
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
		Doc: `Milvus log level. Option: debug, info, warn, error, panic, and fatal. 
It is recommended to use debug level under test and development environments, and info level in production environment.`,
		Export: true,
	}
	l.Level.Init(base.mgr)

	l.RootPath = ParamItem{
		Key:     "log.file.rootPath",
		Version: "2.0.0",
		Doc: `Root path to the log files.
The default value is set empty, indicating to output log files to standard output (stdout) and standard error (stderr).
If this parameter is set to a valid local path, Milvus writes and stores log files in this path.
Set this parameter as the path that you have permission to write.`,
		Export: true,
	}
	l.RootPath.Init(base.mgr)

	l.MaxSize = ParamItem{
		Key:          "log.file.maxSize",
		DefaultValue: "300",
		Version:      "2.0.0",
		Doc:          "The maximum size of a log file, unit: MB.",
		Export:       true,
	}
	l.MaxSize.Init(base.mgr)

	l.MaxAge = ParamItem{
		Key:          "log.file.maxAge",
		DefaultValue: "10",
		Version:      "2.0.0",
		Doc:          "The maximum retention time before a log file is automatically cleared, unit: day. The minimum value is 1.",
		Export:       true,
	}
	l.MaxAge.Init(base.mgr)

	l.MaxBackups = ParamItem{
		Key:          "log.file.maxBackups",
		DefaultValue: "20",
		Version:      "2.0.0",
		Doc:          "The maximum number of log files to back up, unit: day. The minimum value is 1.",
		Export:       true,
	}
	l.MaxBackups.Init(base.mgr)

	l.Format = ParamItem{
		Key:          "log.format",
		DefaultValue: "text",
		Version:      "2.0.0",
		Doc:          "Milvus log format. Option: text and JSON",
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
	EnableActiveStandby         ParamItem `refreshable:"false"`
	MaxDatabaseNum              ParamItem `refreshable:"true"`
	MaxGeneralCapacity          ParamItem `refreshable:"true"`
	GracefulStopTimeout         ParamItem `refreshable:"true"`
	UseLockScheduler            ParamItem `refreshable:"true"`
	DefaultDBProperties         ParamItem `refreshable:"false"`
}

func (p *rootCoordConfig) init(base *BaseTable) {
	p.DmlChannelNum = ParamItem{
		Key:          "rootCoord.dmlChannelNum",
		Version:      "2.0.0",
		DefaultValue: "16",
		Forbidden:    true,
		Doc:          "The number of DML-Channels to create at the root coord startup.",
		Export:       true,
	}
	p.DmlChannelNum.Init(base.mgr)

	p.MaxPartitionNum = ParamItem{
		Key:          "rootCoord.maxPartitionNum",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc: `The maximum number of partitions in each collection.
New partitions cannot be created if this parameter is set as 0 or 1.
Range: [0, INT64MAX]`,
		Export: true,
	}
	p.MaxPartitionNum.Init(base.mgr)

	p.MinSegmentSizeToEnableIndex = ParamItem{
		Key:          "rootCoord.minSegmentSizeToEnableIndex",
		Version:      "2.0.0",
		DefaultValue: "1024",
		Doc: `The minimum row count of a segment required for creating index.
Segments with smaller size than this parameter will not be indexed, and will be searched with brute force.`,
		Export: true,
	}
	p.MinSegmentSizeToEnableIndex.Init(base.mgr)

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

	p.UseLockScheduler = ParamItem{
		Key:          "rootCoord.useLockScheduler",
		Version:      "2.4.15",
		DefaultValue: "true",
		Doc:          "use lock to schedule the task",
		Export:       false,
	}
	p.UseLockScheduler.Init(base.mgr)

	p.DefaultDBProperties = ParamItem{
		Key:          "rootCoord.defaultDBProperties",
		Version:      "2.4.16",
		DefaultValue: "{}",
		Doc:          "default db properties, should be a json string",
		Export:       false,
	}
	p.DefaultDBProperties.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- proxy ---
type AccessLogConfig struct {
	Enable        ParamItem  `refreshable:"true"`
	MinioEnable   ParamItem  `refreshable:"false"`
	LocalPath     ParamItem  `refreshable:"false"`
	Filename      ParamItem  `refreshable:"false"`
	MaxSize       ParamItem  `refreshable:"false"`
	RotatedTime   ParamItem  `refreshable:"false"`
	MaxBackups    ParamItem  `refreshable:"false"`
	RemotePath    ParamItem  `refreshable:"false"`
	RemoteMaxTime ParamItem  `refreshable:"false"`
	Formatter     ParamGroup `refreshable:"false"`

	CacheSize          ParamItem `refreshable:"false"`
	CacheFlushInterval ParamItem `refreshable:"false"`
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
	MaxVectorFieldNum            ParamItem `refreshable:"true"`
	MaxShardNum                  ParamItem `refreshable:"true"`
	MaxDimension                 ParamItem `refreshable:"true"`
	GinLogging                   ParamItem `refreshable:"false"`
	GinLogSkipPaths              ParamItem `refreshable:"false"`
	MaxUserNum                   ParamItem `refreshable:"true"`
	MaxRoleNum                   ParamItem `refreshable:"true"`
	MaxTaskNum                   ParamItem `refreshable:"false"`
	DDLConcurrency               ParamItem `refreshable:"true"`
	DCLConcurrency               ParamItem `refreshable:"true"`
	ShardLeaderCacheInterval     ParamItem `refreshable:"false"`
	ReplicaSelectionPolicy       ParamItem `refreshable:"false"`
	CheckQueryNodeHealthInterval ParamItem `refreshable:"false"`
	CostMetricsExpireTime        ParamItem `refreshable:"false"`
	CheckWorkloadRequestNum      ParamItem `refreshable:"false"`
	WorkloadToleranceFactor      ParamItem `refreshable:"false"`
	RetryTimesOnReplica          ParamItem `refreshable:"true"`
	RetryTimesOnHealthCheck      ParamItem `refreshable:"true"`
	PartitionNameRegexp          ParamItem `refreshable:"true"`
	MustUsePartitionKey          ParamItem `refreshable:"true"`
	SkipAutoIDCheck              ParamItem `refreshable:"true"`
	SkipPartitionKeyCheck        ParamItem `refreshable:"true"`
	MaxVarCharLength             ParamItem `refreshable:"false"`

	AccessLog AccessLogConfig

	// connection manager
	ConnectionCheckIntervalSeconds ParamItem `refreshable:"true"`
	ConnectionClientInfoTTLSeconds ParamItem `refreshable:"true"`
	MaxConnectionNum               ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"true"`

	SlowQuerySpanInSeconds ParamItem `refreshable:"true"`
	QueryNodePoolingSize   ParamItem `refreshable:"false"`
}

func (p *proxyConfig) init(base *BaseTable) {
	p.TimeTickInterval = ParamItem{
		Key:          "proxy.timeTickInterval",
		Version:      "2.2.0",
		DefaultValue: "200",
		PanicIfEmpty: true,
		Doc:          "The interval at which proxy synchronizes the time tick, unit: ms.",
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
		Doc:          "The maximum number of messages can be buffered in the timeTick message stream of the proxy when producing messages.",
		Export:       true,
	}
	p.MsgStreamTimeTickBufSize.Init(base.mgr)

	p.MaxNameLength = ParamItem{
		Key:          "proxy.maxNameLength",
		DefaultValue: "255",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "The maximum length of the name or alias that can be created in Milvus, including the collection name, collection alias, partition name, and field name.",
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
		DefaultValue: "72", // bcrypt max length
		Version:      "2.0.0",
		Formatter: func(v string) string {
			n := getAsInt(v)
			if n <= 0 || n > 72 {
				return "72"
			}
			return v
		},
		PanicIfEmpty: true,
	}
	p.MaxPasswordLength.Init(base.mgr)

	p.MaxFieldNum = ParamItem{
		Key:          "proxy.maxFieldNum",
		DefaultValue: "64",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "The maximum number of field can be created when creating in a collection. It is strongly DISCOURAGED to set maxFieldNum >= 64.",
		Export:       true,
	}
	p.MaxFieldNum.Init(base.mgr)

	p.MaxVectorFieldNum = ParamItem{
		Key:          "proxy.maxVectorFieldNum",
		Version:      "2.4.0",
		DefaultValue: "4",
		PanicIfEmpty: true,
		Doc:          "The maximum number of vector fields that can be specified in a collection. Value range: [1, 10].",
		Export:       true,
	}
	p.MaxVectorFieldNum.Init(base.mgr)

	if p.MaxVectorFieldNum.GetAsInt() > 10 || p.MaxVectorFieldNum.GetAsInt() <= 0 {
		panic(fmt.Sprintf("Maximum number of vector fields in a collection should be in (0, 10], not %d", p.MaxVectorFieldNum.GetAsInt()))
	}

	p.MaxShardNum = ParamItem{
		Key:          "proxy.maxShardNum",
		DefaultValue: "16",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "The maximum number of shards can be created when creating in a collection.",
		Export:       true,
	}
	p.MaxShardNum.Init(base.mgr)

	p.MaxDimension = ParamItem{
		Key:          "proxy.maxDimension",
		DefaultValue: "32768",
		Version:      "2.0.0",
		PanicIfEmpty: true,
		Doc:          "The maximum number of dimensions of a vector can have when creating in a collection.",
		Export:       true,
	}
	p.MaxDimension.Init(base.mgr)

	p.MaxTaskNum = ParamItem{
		Key:          "proxy.maxTaskNum",
		Version:      "2.2.0",
		DefaultValue: "1024",
		Doc:          "The maximum number of tasks in the task queue of the proxy.",
		Export:       true,
	}
	p.MaxTaskNum.Init(base.mgr)

	p.DDLConcurrency = ParamItem{
		Key:          "proxy.ddlConcurrency",
		Version:      "2.5.0",
		DefaultValue: "16",
		Doc:          "The concurrent execution number of DDL at proxy.",
		Export:       true,
	}
	p.DDLConcurrency.Init(base.mgr)

	p.DCLConcurrency = ParamItem{
		Key:          "proxy.dclConcurrency",
		Version:      "2.5.0",
		DefaultValue: "16",
		Doc:          "The concurrent execution number of DCL at proxy.",
		Export:       true,
	}
	p.DCLConcurrency.Init(base.mgr)

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
		Doc:          "Whether to enable the access log feature.",
		Export:       true,
	}
	p.AccessLog.Enable.Init(base.mgr)

	p.AccessLog.MinioEnable = ParamItem{
		Key:          "proxy.accessLog.minioEnable",
		Version:      "2.2.0",
		DefaultValue: "false",
		Doc:          "Whether to upload local access log files to MinIO. This parameter can be specified when proxy.accessLog.filename is not empty.",
		Export:       true,
	}
	p.AccessLog.MinioEnable.Init(base.mgr)

	p.AccessLog.LocalPath = ParamItem{
		Key:          "proxy.accessLog.localPath",
		Version:      "2.2.0",
		DefaultValue: "/tmp/milvus_access",
		Doc:          "The local folder path where the access log file is stored. This parameter can be specified when proxy.accessLog.filename is not empty.",
		Export:       true,
	}
	p.AccessLog.LocalPath.Init(base.mgr)

	p.AccessLog.Filename = ParamItem{
		Key:          "proxy.accessLog.filename",
		Version:      "2.2.0",
		DefaultValue: "",
		Doc:          "The name of the access log file. If you leave this parameter empty, access logs will be printed to stdout.",
		Export:       true,
	}
	p.AccessLog.Filename.Init(base.mgr)

	p.AccessLog.MaxSize = ParamItem{
		Key:          "proxy.accessLog.maxSize",
		Version:      "2.2.0",
		DefaultValue: "64",
		Doc:          "The maximum size allowed for a single access log file. If the log file size reaches this limit, a rotation process will be triggered. This process seals the current access log file, creates a new log file, and clears the contents of the original log file. Unit: MB.",
		Export:       true,
	}
	p.AccessLog.MaxSize.Init(base.mgr)

	p.AccessLog.CacheSize = ParamItem{
		Key:          "proxy.accessLog.cacheSize",
		Version:      "2.3.2",
		DefaultValue: "0",
		Doc:          "Size of log of write cache, in byte. (Close write cache if size was 0)",
		Export:       true,
	}
	p.AccessLog.CacheSize.Init(base.mgr)

	p.AccessLog.CacheFlushInterval = ParamItem{
		Key:          "proxy.accessLog.cacheFlushInterval",
		Version:      "2.4.0",
		DefaultValue: "3",
		Doc:          "time interval of auto flush write cache, in seconds. (Close auto flush if interval was 0)",
		Export:       true,
	}
	p.AccessLog.CacheFlushInterval.Init(base.mgr)

	p.AccessLog.MaxBackups = ParamItem{
		Key:          "proxy.accessLog.maxBackups",
		Version:      "2.2.0",
		DefaultValue: "8",
		Doc:          "The maximum number of sealed access log files that can be retained. If the number of sealed access log files exceeds this limit, the oldest one will be deleted.",
	}
	p.AccessLog.MaxBackups.Init(base.mgr)

	p.AccessLog.RotatedTime = ParamItem{
		Key:          "proxy.accessLog.rotatedTime",
		Version:      "2.2.0",
		DefaultValue: "0",
		Doc:          "The maximum time interval allowed for rotating a single access log file. Upon reaching the specified time interval, a rotation process is triggered, resulting in the creation of a new access log file and sealing of the previous one. Unit: seconds",
		Export:       true,
	}
	p.AccessLog.RotatedTime.Init(base.mgr)

	p.AccessLog.RemotePath = ParamItem{
		Key:          "proxy.accessLog.remotePath",
		Version:      "2.2.0",
		DefaultValue: "access_log/",
		Doc:          "The path of the object storage for uploading access log files.",
		Export:       true,
	}
	p.AccessLog.RemotePath.Init(base.mgr)

	p.AccessLog.RemoteMaxTime = ParamItem{
		Key:          "proxy.accessLog.remoteMaxTime",
		Version:      "2.2.0",
		DefaultValue: "0",
		Doc:          "The time interval allowed for uploading access log files. If the upload time of a log file exceeds this interval, the file will be deleted. Setting the value to 0 disables this feature.",
		Export:       true,
	}
	p.AccessLog.RemoteMaxTime.Init(base.mgr)

	p.AccessLog.Formatter = ParamGroup{
		KeyPrefix: "proxy.accessLog.formatters.",
		Version:   "2.3.4",
		Export:    true,
		Doc:       "Access log formatters for specified methods, if not set, use the base formatter.",
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

	p.CheckWorkloadRequestNum = ParamItem{
		Key:          "proxy.checkWorkloadRequestNum",
		Version:      "2.4.12",
		DefaultValue: "10",
		Doc:          "after every requestNum requests has been assigned, try to check workload for query node",
	}
	p.CheckWorkloadRequestNum.Init(base.mgr)

	p.WorkloadToleranceFactor = ParamItem{
		Key:          "proxy.workloadToleranceFactor",
		Version:      "2.4.12",
		DefaultValue: "0.1",
		Doc: `tolerance factor for query node workload difference, default to 10%, which means if query node's workload diff is higher than this factor, 
		proxy will compute each querynode's workload score, and assign request to the lowest workload node; otherwise, it will assign request to the node by round robin`,
	}
	p.WorkloadToleranceFactor.Init(base.mgr)

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

	p.MustUsePartitionKey = ParamItem{
		Key:          "proxy.mustUsePartitionKey",
		Version:      "2.4.1",
		DefaultValue: "false",
		Doc:          "switch for whether proxy must use partition key for the collection",
		Export:       true,
	}
	p.MustUsePartitionKey.Init(base.mgr)

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

	p.MaxVarCharLength = ParamItem{
		Key:          "proxy.maxVarCharLength",
		Version:      "2.4.19",            // hotfix
		DefaultValue: strconv.Itoa(65535), // 64K
		Doc:          "maximum number of characters for a varchar field; this value is overridden by the value in a pre-existing schema if applicable",
	}
	p.MaxVarCharLength.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "proxy.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultProxyGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

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

	p.SlowQuerySpanInSeconds = ParamItem{
		Key:          "proxy.slowQuerySpanInSeconds",
		Version:      "2.3.11",
		Doc:          "query whose executed time exceeds the `slowQuerySpanInSeconds` can be considered slow, in seconds.",
		DefaultValue: "5",
		Export:       true,
	}
	p.SlowQuerySpanInSeconds.Init(base.mgr)

	p.QueryNodePoolingSize = ParamItem{
		Key:          "proxy.queryNodePooling.size",
		Version:      "2.4.7",
		Doc:          "the size for shardleader(querynode) client pool",
		DefaultValue: "10",
		Export:       true,
	}
	p.QueryNodePoolingSize.Init(base.mgr)
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
	RowCountFactor                      ParamItem `refreshable:"true"`
	SegmentCountFactor                  ParamItem `refreshable:"true"`
	GlobalSegmentCountFactor            ParamItem `refreshable:"true"`
	CollectionChannelCountFactor        ParamItem `refreshable:"true"`
	SegmentCountMaxSteps                ParamItem `refreshable:"true"`
	RowCountMaxSteps                    ParamItem `refreshable:"true"`
	RandomMaxSteps                      ParamItem `refreshable:"true"`
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
	EnableStoppingBalance          ParamItem `refreshable:"true"`
	ChannelExclusiveNodeFactor     ParamItem `refreshable:"true"`

	CollectionObserverInterval         ParamItem `refreshable:"false"`
	CheckExecutedFlagInterval          ParamItem `refreshable:"false"`
	CollectionBalanceSegmentBatchSize  ParamItem `refreshable:"true"`
	CollectionBalanceChannelBatchSize  ParamItem `refreshable:"true"`
	UpdateCollectionLoadStatusInterval ParamItem `refreshable:"false"`
	ClusterLevelLoadReplicaNumber      ParamItem `refreshable:"true"`
	ClusterLevelLoadResourceGroups     ParamItem `refreshable:"true"`
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
		Doc: `Switch value to control if to automatically replace a growing segment with the corresponding indexed sealed segment when the growing segment reaches the sealing threshold.
If this parameter is set false, Milvus simply searches the growing segments with brute force.`,
		Export: true,
	}
	p.AutoHandoff.Init(base.mgr)

	p.AutoBalance = ParamItem{
		Key:          "queryCoord.autoBalance",
		Version:      "2.0.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
		Doc:          "Switch value to control if to automatically balance the memory usage among query nodes by distributing segment loading and releasing operations evenly.",
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

	p.RowCountFactor = ParamItem{
		Key:          "queryCoord.rowCountFactor",
		Version:      "2.3.0",
		DefaultValue: "0.4",
		PanicIfEmpty: true,
		Doc:          "the row count weight used when balancing segments among queryNodes",
		Export:       true,
	}
	p.RowCountFactor.Init(base.mgr)

	p.SegmentCountFactor = ParamItem{
		Key:          "queryCoord.segmentCountFactor",
		Version:      "2.3.0",
		DefaultValue: "0.4",
		PanicIfEmpty: true,
		Doc:          "the segment count weight used when balancing segments among queryNodes",
		Export:       true,
	}
	p.SegmentCountFactor.Init(base.mgr)

	p.GlobalSegmentCountFactor = ParamItem{
		Key:          "queryCoord.globalSegmentCountFactor",
		Version:      "2.3.0",
		DefaultValue: "0.1",
		PanicIfEmpty: true,
		Doc:          "the segment count weight used when balancing segments among queryNodes",
		Export:       true,
	}
	p.GlobalSegmentCountFactor.Init(base.mgr)

	p.CollectionChannelCountFactor = ParamItem{
		Key:          "queryCoord.collectionChannelCountFactor",
		Version:      "2.4.18",
		DefaultValue: "10",
		PanicIfEmpty: true,
		Doc: `the channel count weight used when balancing channels among queryNodes, 
		A higher value reduces the likelihood of assigning channels from the same collection to the same QueryNode. Set to 1 to disable this feature.`,
		Export: true,
	}
	p.CollectionChannelCountFactor.Init(base.mgr)

	p.SegmentCountMaxSteps = ParamItem{
		Key:          "queryCoord.segmentCountMaxSteps",
		Version:      "2.3.0",
		DefaultValue: "50",
		PanicIfEmpty: true,
		Doc:          "segment count based plan generator max steps",
		Export:       true,
	}
	p.SegmentCountMaxSteps.Init(base.mgr)

	p.RowCountMaxSteps = ParamItem{
		Key:          "queryCoord.rowCountMaxSteps",
		Version:      "2.3.0",
		DefaultValue: "50",
		PanicIfEmpty: true,
		Doc:          "segment count based plan generator max steps",
		Export:       true,
	}
	p.RowCountMaxSteps.Init(base.mgr)

	p.RandomMaxSteps = ParamItem{
		Key:          "queryCoord.randomMaxSteps",
		Version:      "2.3.0",
		DefaultValue: "10",
		PanicIfEmpty: true,
		Doc:          "segment count based plan generator max steps",
		Export:       true,
	}
	p.RandomMaxSteps.Init(base.mgr)

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
		Doc:          "The threshold of memory usage (in percentage) in a query node to trigger the sealed segment balancing.",
		Export:       true,
	}
	p.OverloadedMemoryThresholdPercentage.Init(base.mgr)

	p.BalanceIntervalSeconds = ParamItem{
		Key:          "queryCoord.balanceIntervalSeconds",
		Version:      "2.0.0",
		DefaultValue: "60",
		PanicIfEmpty: true,
		Doc:          "The interval at which query coord balances the memory usage among query nodes.",
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

	p.BalanceCostThreshold = ParamItem{
		Key:          "queryCoord.balanceCostThreshold",
		Version:      "2.4.0",
		DefaultValue: "0.001",
		PanicIfEmpty: true,
		Doc:          "the threshold of balance cost, if the difference of cluster's cost after executing the balance plan is less than this value, the plan will not be executed",
		Export:       true,
	}
	p.BalanceCostThreshold.Init(base.mgr)

	p.MemoryUsageMaxDifferencePercentage = ParamItem{
		Key:          "queryCoord.memoryUsageMaxDifferencePercentage",
		Version:      "2.0.0",
		DefaultValue: "30",
		PanicIfEmpty: true,
		Doc:          "The threshold of memory usage difference (in percentage) between any two query nodes to trigger the sealed segment balancing.",
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
		DefaultValue: "3000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.SegmentCheckInterval.Init(base.mgr)

	p.ChannelCheckInterval = ParamItem{
		Key:          "queryCoord.checkChannelInterval",
		Version:      "2.3.0",
		DefaultValue: "3000",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.ChannelCheckInterval.Init(base.mgr)

	p.BalanceCheckInterval = ParamItem{
		Key:          "queryCoord.checkBalanceInterval",
		Version:      "2.3.0",
		DefaultValue: "3000",
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

	p.UpdateCollectionLoadStatusInterval = ParamItem{
		Key:          "queryCoord.updateCollectionLoadStatusInterval",
		Version:      "2.4.7",
		DefaultValue: "5",
		PanicIfEmpty: true,
		Doc:          "5m, max interval of updating collection loaded status for check health",
		Export:       true,
	}

	p.UpdateCollectionLoadStatusInterval.Init(base.mgr)

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

	p.EnableStoppingBalance = ParamItem{
		Key:          "queryCoord.enableStoppingBalance",
		Version:      "2.3.13",
		DefaultValue: "true",
		Doc:          "whether enable stopping balance",
		Export:       true,
	}
	p.EnableStoppingBalance.Init(base.mgr)

	p.ChannelExclusiveNodeFactor = ParamItem{
		Key:          "queryCoord.channelExclusiveNodeFactor",
		Version:      "2.4.2",
		DefaultValue: "4",
		Doc:          "the least node number for enable channel's exclusive mode",
		Export:       true,
	}
	p.ChannelExclusiveNodeFactor.Init(base.mgr)

	p.CollectionObserverInterval = ParamItem{
		Key:          "queryCoord.collectionObserverInterval",
		Version:      "2.4.4",
		DefaultValue: "200",
		Doc:          "the interval of collection observer",
		Export:       true,
	}
	p.CollectionObserverInterval.Init(base.mgr)

	p.CheckExecutedFlagInterval = ParamItem{
		Key:          "queryCoord.checkExecutedFlagInterval",
		Version:      "2.4.4",
		DefaultValue: "100",
		Doc:          "the interval of check executed flag to force to pull dist",
		Export:       true,
	}
	p.CheckExecutedFlagInterval.Init(base.mgr)

	p.CollectionBalanceSegmentBatchSize = ParamItem{
		Key:          "queryCoord.collectionBalanceSegmentBatchSize",
		Version:      "2.4.7",
		DefaultValue: "5",
		Doc:          "the max balance task number for collection at each round",
		Export:       false,
	}
	p.CollectionBalanceSegmentBatchSize.Init(base.mgr)

	p.CollectionBalanceChannelBatchSize = ParamItem{
		Key:          "queryCoord.collectionBalanceChannelBatchSize",
		Version:      "2.4.18",
		DefaultValue: "1",
		Doc:          "the max balance task number for channel at each round",
		Export:       false,
	}
	p.CollectionBalanceChannelBatchSize.Init(base.mgr)

	p.ClusterLevelLoadReplicaNumber = ParamItem{
		Key:          "queryCoord.clusterLevelLoadReplicaNumber",
		Version:      "2.4.7",
		DefaultValue: "0",
		Doc:          "the cluster level default value for load replica number",
		Export:       false,
	}
	p.ClusterLevelLoadReplicaNumber.Init(base.mgr)

	p.ClusterLevelLoadResourceGroups = ParamItem{
		Key:          "queryCoord.clusterLevelLoadResourceGroups",
		Version:      "2.4.7",
		DefaultValue: "",
		Doc:          "resource group names for load collection should be at least equal to queryCoord.clusterLevelLoadReplicaNumber, separate with commas",
		Export:       false,
	}
	p.ClusterLevelLoadResourceGroups.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- querynode ---
type queryNodeConfig struct {
	SoPath ParamItem `refreshable:"false"`

	// stats
	// Deprecated: Never used
	StatsPublishInterval ParamItem `refreshable:"true"`

	// segcore
	KnowhereThreadPoolSize        ParamItem `refreshable:"false"`
	ChunkRows                     ParamItem `refreshable:"false"`
	EnableInterminSegmentIndex    ParamItem `refreshable:"false"`
	InterimIndexNlist             ParamItem `refreshable:"false"`
	InterimIndexNProbe            ParamItem `refreshable:"false"`
	InterimIndexSubDim            ParamItem `refreshable:"false"`
	InterimIndexRefineRatio       ParamItem `refreshable:"false"`
	InterimIndexRefineQuantType   ParamItem `refreshable:"false"`
	InterimIndexRefineWithQuant   ParamItem `refreshable:"false"`
	DenseVectorInterminIndexType  ParamItem `refreshable:"false"`
	InterimIndexMemExpandRate     ParamItem `refreshable:"false"`
	InterimIndexBuildParallelRate ParamItem `refreshable:"false"`
	MultipleChunkedEnable         ParamItem `refreshable:"false"`

	KnowhereScoreConsistency ParamItem `refreshable:"false"`

	// memory limit
	LoadMemoryUsageFactor               ParamItem `refreshable:"true"`
	OverloadedMemoryThresholdPercentage ParamItem `refreshable:"false"`

	// enable disk
	EnableDisk             ParamItem `refreshable:"true"`
	DiskCapacityLimit      ParamItem `refreshable:"true"`
	MaxDiskUsagePercentage ParamItem `refreshable:"true"`
	DiskCacheCapacityLimit ParamItem `refreshable:"true"`

	// cache limit
	CacheMemoryLimit ParamItem `refreshable:"false"`
	MmapDirPath      ParamItem `refreshable:"false"`
	// Deprecated: Since 2.4.7, use `MmapVectorField`/`MmapVectorIndex`/`MmapScalarField`/`MmapScalarIndex` instead
	MmapEnabled                         ParamItem `refreshable:"false"`
	MmapVectorField                     ParamItem `refreshable:"false"`
	MmapVectorIndex                     ParamItem `refreshable:"false"`
	MmapScalarField                     ParamItem `refreshable:"false"`
	MmapScalarIndex                     ParamItem `refreshable:"false"`
	MmapChunkCache                      ParamItem `refreshable:"false"`
	GrowingMmapEnabled                  ParamItem `refreshable:"false"`
	FixedFileSizeForMmapManager         ParamItem `refreshable:"false"`
	MaxMmapDiskPercentageForMmapManager ParamItem `refreshable:"false"`

	LazyLoadEnabled                      ParamItem `refreshable:"false"`
	LazyLoadWaitTimeout                  ParamItem `refreshable:"true"`
	LazyLoadRequestResourceTimeout       ParamItem `refreshable:"true"`
	LazyLoadRequestResourceRetryInterval ParamItem `refreshable:"true"`
	LazyLoadMaxRetryTimes                ParamItem `refreshable:"true"`
	LazyLoadMaxEvictPerRetry             ParamItem `refreshable:"true"`

	IndexOffsetCacheEnabled ParamItem `refreshable:"true"`

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
	GracefulStopTimeout   ParamItem `refreshable:"false"`

	// delete buffer
	MaxSegmentDeleteBuffer ParamItem `refreshable:"false"`
	DeleteBufferBlockSize  ParamItem `refreshable:"false"`

	// delta forward
	LevelZeroForwardPolicy      ParamItem `refreshable:"true"`
	StreamingDeltaForwardPolicy ParamItem `refreshable:"true"`

	// loader
	IoPoolSize             ParamItem `refreshable:"false"`
	DeltaDataExpansionRate ParamItem `refreshable:"true"`
	DiskSizeFetchInterval  ParamItem `refreshable:"false"`

	// schedule task policy.
	SchedulePolicyName                    ParamItem `refreshable:"false"`
	SchedulePolicyTaskQueueExpire         ParamItem `refreshable:"true"`
	SchedulePolicyEnableCrossUserGrouping ParamItem `refreshable:"true"`
	SchedulePolicyMaxPendingTaskPerUser   ParamItem `refreshable:"true"`

	// CGOPoolSize ratio to MaxReadConcurrency
	CGOPoolSizeRatio ParamItem `refreshable:"true"`

	EnableWorkerSQCostMetrics ParamItem `refreshable:"true"`

	ExprEvalBatchSize ParamItem `refreshable:"false"`

	// pipeline
	CleanExcludeSegInterval ParamItem `refreshable:"false"`
	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`

	MemoryIndexLoadPredictMemoryUsageFactor ParamItem `refreshable:"true"`
	EnableSegmentPrune                      ParamItem `refreshable:"true"`
	DefaultSegmentFilterRatio               ParamItem `refreshable:"true"`
	UseStreamComputing                      ParamItem `refreshable:"false"`
	QueryStreamBatchSize                    ParamItem `refreshable:"false"`
	QueryStreamMaxBatchSize                 ParamItem `refreshable:"false"`

	// BF
	SkipGrowingSegmentBF           ParamItem `refreshable:"true"`
	BloomFilterApplyParallelFactor ParamItem `refreshable:"true"`

	// worker
	WorkerPoolingSize ParamItem `refreshable:"false"`

	// Json Key Stats
	JSONKeyStatsCommitInterval        ParamItem `refreshable:"false"`
	EnabledGrowingSegmentJSONKeyStats ParamItem `refreshable:"false"`
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
		Doc:          "The maximum size of task queue cache in flow graph in query node.",
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
		Doc:          "The interval that query node publishes the node statistics information, including segment status, cpu usage, memory usage, health status, etc. Unit: ms.",
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
		Doc:    "Row count by which Segcore divides a segment into chunks.",
		Export: true,
	}
	p.ChunkRows.Init(base.mgr)

	p.EnableInterminSegmentIndex = ParamItem{
		Key:          "queryNode.segcore.interimIndex.enableIndex",
		Version:      "2.0.0",
		DefaultValue: "false",
		Doc: `Whether to create a temporary index for growing segments and sealed segments not yet indexed, improving search performance.
Milvus will eventually seals and indexes all segments, but enabling this optimizes search performance for immediate queries following data insertion.
This defaults to true, indicating that Milvus creates temporary index for growing segments and the sealed segments that are not indexed upon searches.`,
		Export: true,
	}
	p.EnableInterminSegmentIndex.Init(base.mgr)

	p.DenseVectorInterminIndexType = ParamItem{
		Key:          "queryNode.segcore.interimIndex.denseVectorIndexType",
		Version:      "2.5.4",
		DefaultValue: "IVF_FLAT_CC",
		Doc:          `Dense vector intermin index type`,
		Export:       true,
	}
	p.DenseVectorInterminIndexType.Init(base.mgr)

	p.InterimIndexRefineQuantType = ParamItem{
		Key:          "queryNode.segcore.interimIndex.refineQuantType",
		Version:      "2.5.6",
		DefaultValue: "DATA_VIEW",
		Doc:          `Data representation of SCANN_DVR index, options: 'NONE', 'FLOAT16', 'BFLOAT16' and 'UINT8'`,
		Export:       true,
	}
	p.InterimIndexRefineQuantType.Init(base.mgr)

	p.InterimIndexRefineWithQuant = ParamItem{
		Key:          "queryNode.segcore.interimIndex.refineWithQuant",
		Version:      "2.5.6",
		DefaultValue: "true",
		Doc:          `whether to use refineQuantType to refine for fatser but loss a little precision`,
		Export:       true,
	}
	p.InterimIndexRefineWithQuant.Init(base.mgr)

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
		Doc:          "interim index nlist, recommend to set sqrt(chunkRows), must smaller than chunkRows/8",
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
		Version:      "2.0.0",
		DefaultValue: "0.5",
		Doc:          "the ratio of building interim index parallel matched with cpu num",
		Export:       true,
	}
	p.InterimIndexBuildParallelRate.Init(base.mgr)

	p.MultipleChunkedEnable = ParamItem{
		Key:          "queryNode.segcore.multipleChunkedEnable",
		Version:      "2.0.0",
		DefaultValue: "true",
		Doc:          "Enable multiple chunked search",
		Export:       true,
	}
	p.MultipleChunkedEnable.Init(base.mgr)

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

	p.InterimIndexSubDim = ParamItem{
		Key:          "queryNode.segcore.interimIndex.subDim",
		Version:      "2.5.4",
		DefaultValue: "2",
		Doc:          "interim index sub dim, recommend to (subDim % vector dim == 0)",
		Export:       true,
	}
	p.InterimIndexSubDim.Init(base.mgr)

	p.InterimIndexRefineRatio = ParamItem{
		Key:     "queryNode.segcore.interimIndex.refineRatio",
		Version: "2.5.4",
		Formatter: func(v string) string {
			if getAsFloat(v) < 1.0 {
				return "1.0"
			}
			return v
		},
		DefaultValue: "2.0",
		Doc:          "interim index parameters, should set to be >= 1.0",
		Export:       true,
	}
	p.InterimIndexRefineRatio.Init(base.mgr)

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

	p.MmapDirPath = ParamItem{
		Key:          "queryNode.mmap.mmapDirPath",
		Version:      "2.3.0",
		DefaultValue: "",
		FallbackKeys: []string{"queryNode.mmapDirPath"},
		Doc:          "The folder that storing data files for mmap, setting to a path will enable Milvus to load data with mmap",
		Formatter: func(v string) string {
			if len(v) == 0 {
				return path.Join(base.Get("localStorage.path"), "mmap")
			}
			return v
		},
	}
	p.MmapDirPath.Init(base.mgr)

	p.MmapEnabled = ParamItem{
		Key:          "queryNode.mmap.mmapEnabled",
		Version:      "2.4.0",
		DefaultValue: "false",
		FallbackKeys: []string{"queryNode.mmapEnabled"},
		Doc:          "Deprecated: Enable mmap for loading data, including vector/scalar data and index",
		Export:       false,
	}
	p.MmapEnabled.Init(base.mgr)

	p.MmapVectorField = ParamItem{
		Key:          "queryNode.mmap.vectorField",
		Version:      "2.4.7",
		DefaultValue: "false",
		Formatter: func(originValue string) string {
			if p.MmapEnabled.GetAsBool() {
				return "true"
			}
			return originValue
		},
		Doc:    "Enable mmap for loading vector data",
		Export: true,
	}
	p.MmapVectorField.Init(base.mgr)

	p.MmapVectorIndex = ParamItem{
		Key:          "queryNode.mmap.vectorIndex",
		Version:      "2.4.7",
		DefaultValue: "false",
		Formatter: func(originValue string) string {
			if p.MmapEnabled.GetAsBool() {
				return "true"
			}
			return originValue
		},
		Doc:    "Enable mmap for loading vector index",
		Export: true,
	}
	p.MmapVectorIndex.Init(base.mgr)

	p.MmapScalarField = ParamItem{
		Key:          "queryNode.mmap.scalarField",
		Version:      "2.4.7",
		DefaultValue: "false",
		Formatter: func(originValue string) string {
			if p.MmapEnabled.GetAsBool() {
				return "true"
			}
			return originValue
		},
		Doc:    "Enable mmap for loading scalar data",
		Export: true,
	}
	p.MmapScalarField.Init(base.mgr)

	p.MmapScalarIndex = ParamItem{
		Key:          "queryNode.mmap.scalarIndex",
		Version:      "2.4.7",
		DefaultValue: "false",
		Formatter: func(originValue string) string {
			if p.MmapEnabled.GetAsBool() {
				return "true"
			}
			return originValue
		},
		Doc:    "Enable mmap for loading scalar index",
		Export: true,
	}
	p.MmapScalarIndex.Init(base.mgr)

	p.MmapChunkCache = ParamItem{
		Key:          "queryNode.mmap.chunkCache",
		Version:      "2.4.12",
		DefaultValue: "true",
		Doc:          "Enable mmap for chunk cache (raw vector retrieving).",
		Export:       true,
	}
	p.MmapChunkCache.Init(base.mgr)

	p.GrowingMmapEnabled = ParamItem{
		Key:          "queryNode.mmap.growingMmapEnabled",
		Version:      "2.4.6",
		DefaultValue: "false",
		FallbackKeys: []string{"queryNode.growingMmapEnabled"},
		Doc: `Enable memory mapping (mmap) to optimize the handling of growing raw data. 
By activating this feature, the memory overhead associated with newly added or modified data will be significantly minimized. 
However, this optimization may come at the cost of a slight decrease in query latency for the affected data segments.`,
		Export: true,
	}
	p.GrowingMmapEnabled.Init(base.mgr)

	p.FixedFileSizeForMmapManager = ParamItem{
		Key:          "queryNode.mmap.fixedFileSizeForMmapAlloc",
		Version:      "2.4.6",
		DefaultValue: "64",
		Doc:          "tmp file size for mmap chunk manager",
		Export:       true,
	}
	p.FixedFileSizeForMmapManager.Init(base.mgr)

	p.MaxMmapDiskPercentageForMmapManager = ParamItem{
		Key:          "queryNode.mmap.maxDiskUsagePercentageForMmapAlloc",
		Version:      "2.4.6",
		DefaultValue: "20",
		Doc:          "disk percentage used in mmap chunk manager",
		Export:       true,
	}
	p.MaxMmapDiskPercentageForMmapManager.Init(base.mgr)

	p.LazyLoadEnabled = ParamItem{
		Key:          "queryNode.lazyload.enabled",
		Version:      "2.4.2",
		DefaultValue: "false",
		Doc:          "Enable lazyload for loading data",
		Export:       true,
	}
	p.LazyLoadEnabled.Init(base.mgr)
	p.LazyLoadWaitTimeout = ParamItem{
		Key:          "queryNode.lazyload.waitTimeout",
		Version:      "2.4.2",
		DefaultValue: "30000",
		Doc:          "max wait timeout duration in milliseconds before start to do lazyload search and retrieve",
		Export:       true,
	}
	p.LazyLoadWaitTimeout.Init(base.mgr)
	p.LazyLoadRequestResourceTimeout = ParamItem{
		Key:          "queryNode.lazyload.requestResourceTimeout",
		Version:      "2.4.2",
		DefaultValue: "5000",
		Doc:          "max timeout in milliseconds for waiting request resource for lazy load, 5s by default",
		Export:       true,
	}
	p.LazyLoadRequestResourceTimeout.Init(base.mgr)
	p.LazyLoadRequestResourceRetryInterval = ParamItem{
		Key:          "queryNode.lazyload.requestResourceRetryInterval",
		Version:      "2.4.2",
		DefaultValue: "2000",
		Doc:          "retry interval in milliseconds for waiting request resource for lazy load, 2s by default",
		Export:       true,
	}
	p.LazyLoadRequestResourceRetryInterval.Init(base.mgr)

	p.LazyLoadMaxRetryTimes = ParamItem{
		Key:          "queryNode.lazyload.maxRetryTimes",
		Version:      "2.4.2",
		DefaultValue: "1",
		Doc:          "max retry times for lazy load, 1 by default",
		Export:       true,
	}
	p.LazyLoadMaxRetryTimes.Init(base.mgr)

	p.LazyLoadMaxEvictPerRetry = ParamItem{
		Key:          "queryNode.lazyload.maxEvictPerRetry",
		Version:      "2.4.2",
		DefaultValue: "1",
		Doc:          "max evict count for lazy load, 1 by default",
		Export:       true,
	}
	p.LazyLoadMaxEvictPerRetry.Init(base.mgr)

	p.ReadAheadPolicy = ParamItem{
		Key:          "queryNode.cache.readAheadPolicy",
		Version:      "2.3.2",
		DefaultValue: "willneed",
		Doc:          "The read ahead policy of chunk cache, options: `normal, random, sequential, willneed, dontneed`",
		Export:       true,
	}
	p.ReadAheadPolicy.Init(base.mgr)

	p.ChunkCacheWarmingUp = ParamItem{
		Key:          "queryNode.cache.warmup",
		Version:      "2.3.6",
		DefaultValue: "disable",
		Doc: `options: async, sync, disable. 
Specifies the necessity for warming up the chunk cache. 
1. If set to "sync" or "async" the original vector data will be synchronously/asynchronously loaded into the 
chunk cache during the load process. This approach has the potential to substantially reduce query/search latency
for a specific duration post-load, albeit accompanied by a concurrent increase in disk usage;
2. If set to "disable" original vector data will only be loaded into the chunk cache during search/query.`,
		Export: true,
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

	p.IndexOffsetCacheEnabled = ParamItem{
		Key:          "queryNode.indexOffsetCacheEnabled",
		Version:      "2.5.0",
		DefaultValue: "false",
		Doc: "enable index offset cache for some scalar indexes, now is just for bitmap index," +
			" enable this param can improve performance for retrieving raw data from index",
		Export: true,
	}
	p.IndexOffsetCacheEnabled.Init(base.mgr)

	p.DiskCapacityLimit = ParamItem{
		Key:     "LOCAL_STORAGE_SIZE",
		Version: "2.2.0",
		Formatter: func(v string) string {
			if len(v) == 0 {
				// use local storage path to check correct device
				localStoragePath := base.Get("localStorage.path")
				if _, err := os.Stat(localStoragePath); os.IsNotExist(err) {
					if err := os.MkdirAll(localStoragePath, os.ModePerm); err != nil {
						log.Fatal("failed to mkdir", zap.String("localStoragePath", localStoragePath), zap.Error(err))
					}
				}
				diskUsage, err := disk.Usage(localStoragePath)
				if err != nil {
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

	p.DiskCacheCapacityLimit = ParamItem{
		Key:     "queryNode.diskCacheCapacityLimit",
		Version: "2.4.1",
		Formatter: func(v string) string {
			if len(v) == 0 {
				return strconv.FormatInt(int64(float64(p.DiskCapacityLimit.GetAsInt64())*p.MaxDiskUsagePercentage.GetAsFloat()), 10)
			}
			return v
		},
	}
	p.DiskCacheCapacityLimit.Init(base.mgr)

	p.MaxTimestampLag = ParamItem{
		Key:          "queryNode.scheduler.maxTimestampLag",
		Version:      "2.2.3",
		DefaultValue: "86400",
		Export:       true,
	}
	p.MaxTimestampLag.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "queryNode.gracefulStopTimeout",
		Version:      "2.2.1",
		FallbackKeys: []string{"common.gracefulStopTimeout"},
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

	p.LevelZeroForwardPolicy = ParamItem{
		Key:          "queryNode.levelZeroForwardPolicy",
		Version:      "2.4.12",
		Doc:          "delegator level zero deletion forward policy, possible option[\"FilterByBF\", \"RemoteLoad\"]",
		DefaultValue: "FilterByBF",
		Export:       true,
	}
	p.LevelZeroForwardPolicy.Init(base.mgr)

	p.StreamingDeltaForwardPolicy = ParamItem{
		Key:          "queryNode.streamingDeltaForwardPolicy",
		Version:      "2.4.12",
		Doc:          "delegator streaming deletion forward policy, possible option[\"FilterByBF\", \"Direct\"]",
		DefaultValue: "FilterByBF",
		Export:       true,
	}
	p.StreamingDeltaForwardPolicy.Init(base.mgr)

	p.IoPoolSize = ParamItem{
		Key:          "queryNode.ioPoolSize",
		Version:      "2.3.0",
		DefaultValue: "0",
		Doc:          "Control how many goroutines will the loader use to pull files, if the given value is non-positive, the value will be set to CpuNum * 8, at least 32, and at most 256",
	}
	p.IoPoolSize.Init(base.mgr)

	p.DeltaDataExpansionRate = ParamItem{
		Key:          "querynode.deltaDataExpansionRate",
		Version:      "2.4.0",
		DefaultValue: "50",
		Doc:          "the expansion rate for deltalog physical size to actual memory usage",
	}
	p.DeltaDataExpansionRate.Init(base.mgr)

	p.DiskSizeFetchInterval = ParamItem{
		Key:          "querynode.diskSizeFetchInterval",
		Version:      "2.5.0",
		DefaultValue: "60",
		Doc:          "The time interval in seconds for retrieving disk usage.",
	}
	p.DiskSizeFetchInterval.Init(base.mgr)

	// schedule read task policy.
	p.SchedulePolicyName = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.name",
		Version:      "2.3.0",
		DefaultValue: "fifo",
		Doc: `fifo: A FIFO queue support the schedule.
user-task-polling:
	The user's tasks will be polled one by one and scheduled.
	Scheduling is fair on task granularity.
	The policy is based on the username for authentication.
	And an empty username is considered the same user.
	When there are no multi-users, the policy decay into FIFO"`,
		Export: true,
	}
	p.SchedulePolicyName.Init(base.mgr)
	p.SchedulePolicyTaskQueueExpire = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.taskQueueExpire",
		Version:      "2.3.0",
		DefaultValue: "60",
		Doc:          "Control how long (many seconds) that queue retains since queue is empty",
		Export:       true,
	}
	p.SchedulePolicyTaskQueueExpire.Init(base.mgr)
	p.SchedulePolicyEnableCrossUserGrouping = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.enableCrossUserGrouping",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          "Enable Cross user grouping when using user-task-polling policy. (Disable it if user's task can not merge each other)",
		Export:       true,
	}
	p.SchedulePolicyEnableCrossUserGrouping.Init(base.mgr)
	p.SchedulePolicyMaxPendingTaskPerUser = ParamItem{
		Key:          "queryNode.scheduler.scheduleReadPolicy.maxPendingTaskPerUser",
		Version:      "2.3.0",
		DefaultValue: "1024",
		Doc:          "Max pending task per user in scheduler",
		Export:       true,
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

	p.ExprEvalBatchSize = ParamItem{
		Key:          "queryNode.segcore.exprEvalBatchSize",
		Version:      "2.3.4",
		DefaultValue: "8192",
		Doc:          "expr eval batch size for getnext interface",
	}
	p.ExprEvalBatchSize.Init(base.mgr)

	p.JSONKeyStatsCommitInterval = ParamItem{
		Key:          "queryNode.segcore.jsonKeyStatsCommitInterval",
		Version:      "2.5.0",
		DefaultValue: "200",
		Doc:          "the commit interval for the JSON key Stats to commit",
		Export:       true,
	}
	p.JSONKeyStatsCommitInterval.Init(base.mgr)

	p.CleanExcludeSegInterval = ParamItem{
		Key:          "queryCoord.cleanExcludeSegmentInterval",
		Version:      "2.4.0",
		DefaultValue: "60",
		Doc:          "the time duration of clean pipeline exclude segment which used for filter invalid data, in seconds",
		Export:       true,
	}
	p.CleanExcludeSegInterval.Init(base.mgr)

	p.MemoryIndexLoadPredictMemoryUsageFactor = ParamItem{
		Key:          "queryNode.memoryIndexLoadPredictMemoryUsageFactor",
		Version:      "2.3.8",
		DefaultValue: "2.5", // HNSW index needs more memory to load.
		Doc:          "memory usage prediction factor for memory index loaded",
	}
	p.MemoryIndexLoadPredictMemoryUsageFactor.Init(base.mgr)

	p.EnableSegmentPrune = ParamItem{
		Key:          "queryNode.enableSegmentPrune",
		Version:      "2.3.4",
		DefaultValue: "false",
		Doc:          "use partition stats to prune data in search/query on shard delegator",
		Export:       true,
	}
	p.EnableSegmentPrune.Init(base.mgr)
	p.DefaultSegmentFilterRatio = ParamItem{
		Key:          "queryNode.defaultSegmentFilterRatio",
		Version:      "2.4.0",
		DefaultValue: "2",
		Doc:          "filter ratio used for pruning segments when searching",
	}
	p.DefaultSegmentFilterRatio.Init(base.mgr)
	p.UseStreamComputing = ParamItem{
		Key:          "queryNode.useStreamComputing",
		Version:      "2.4.0",
		DefaultValue: "false",
		Doc:          "use stream search mode when searching or querying",
	}
	p.UseStreamComputing.Init(base.mgr)

	p.QueryStreamBatchSize = ParamItem{
		Key:          "queryNode.queryStreamBatchSize",
		Version:      "2.4.1",
		DefaultValue: "4194304",
		Doc:          "return min batch size of stream query",
		Export:       true,
	}
	p.QueryStreamBatchSize.Init(base.mgr)

	p.QueryStreamMaxBatchSize = ParamItem{
		Key:          "queryNode.queryStreamMaxBatchSize",
		Version:      "2.4.10",
		DefaultValue: "134217728",
		Doc:          "return max batch size of stream query",
		Export:       true,
	}
	p.QueryStreamMaxBatchSize.Init(base.mgr)

	p.BloomFilterApplyParallelFactor = ParamItem{
		Key:          "queryNode.bloomFilterApplyParallelFactor",
		FallbackKeys: []string{"queryNode.bloomFilterApplyBatchSize"},
		Version:      "2.4.5",
		DefaultValue: "4",
		Doc:          "parallel factor when to apply pk to bloom filter, default to 4*CPU_CORE_NUM",
		Export:       true,
	}
	p.BloomFilterApplyParallelFactor.Init(base.mgr)

	p.SkipGrowingSegmentBF = ParamItem{
		Key:          "queryNode.skipGrowingSegmentBF",
		Version:      "2.5",
		DefaultValue: "true",
		Doc:          "indicates whether skipping the creation, maintenance, or checking of Bloom Filters for growing segments",
	}
	p.SkipGrowingSegmentBF.Init(base.mgr)

	p.WorkerPoolingSize = ParamItem{
		Key:          "queryNode.workerPooling.size",
		Version:      "2.4.7",
		Doc:          "the size for worker querynode client pool",
		DefaultValue: "10",
		Export:       true,
	}
	p.WorkerPoolingSize.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datacoord ---
type dataCoordConfig struct {
	// --- CHANNEL ---
	WatchTimeoutInterval         ParamItem `refreshable:"false"`
	LegacyVersionWithoutRPCWatch ParamItem `refreshable:"false"`
	ChannelBalanceSilentDuration ParamItem `refreshable:"true"`
	ChannelBalanceInterval       ParamItem `refreshable:"true"`
	ChannelCheckInterval         ParamItem `refreshable:"true"`
	ChannelOperationRPCTimeout   ParamItem `refreshable:"true"`

	// --- SEGMENTS ---
	SegmentMaxSize                 ParamItem `refreshable:"false"`
	DiskSegmentMaxSize             ParamItem `refreshable:"true"`
	SegmentSealProportion          ParamItem `refreshable:"false"`
	SegmentSealProportionJitter    ParamItem `refreshable:"true"`
	SegAssignmentExpiration        ParamItem `refreshable:"false"`
	AllocLatestExpireAttempt       ParamItem `refreshable:"true"`
	SegmentMaxLifetime             ParamItem `refreshable:"false"`
	SegmentMaxIdleTime             ParamItem `refreshable:"false"`
	SegmentMinSizeFromIdleToSealed ParamItem `refreshable:"false"`
	SegmentMaxBinlogFileNumber     ParamItem `refreshable:"false"`
	GrowingSegmentsMemSizeInMB     ParamItem `refreshable:"true"`
	AutoUpgradeSegmentIndex        ParamItem `refreshable:"true"`
	SegmentFlushInterval           ParamItem `refreshable:"true"`
	BlockingL0EntryNum             ParamItem `refreshable:"true"`
	BlockingL0SizeInMB             ParamItem `refreshable:"true"`

	// compaction
	EnableCompaction            ParamItem `refreshable:"false"`
	EnableAutoCompaction        ParamItem `refreshable:"true"`
	IndexBasedCompaction        ParamItem `refreshable:"true"`
	CompactionTaskPrioritizer   ParamItem `refreshable:"true"`
	CompactionTaskQueueCapacity ParamItem `refreshable:"false"`

	CompactionRPCTimeout             ParamItem `refreshable:"true"`
	CompactionMaxParallelTasks       ParamItem `refreshable:"true"`
	CompactionWorkerParalleTasks     ParamItem `refreshable:"true"`
	MinSegmentToMerge                ParamItem `refreshable:"true"`
	SegmentSmallProportion           ParamItem `refreshable:"true"`
	SegmentCompactableProportion     ParamItem `refreshable:"true"`
	SegmentExpansionRate             ParamItem `refreshable:"true"`
	CompactionTimeoutInSeconds       ParamItem `refreshable:"true"`
	CompactionDropToleranceInSeconds ParamItem `refreshable:"true"`
	CompactionGCIntervalInSeconds    ParamItem `refreshable:"true"`
	CompactionCheckIntervalInSeconds ParamItem `refreshable:"false"` // deprecated
	CompactionScheduleInterval       ParamItem `refreshable:"false"`
	MixCompactionTriggerInterval     ParamItem `refreshable:"false"`
	L0CompactionTriggerInterval      ParamItem `refreshable:"false"`
	GlobalCompactionInterval         ParamItem `refreshable:"false"`

	SingleCompactionRatioThreshold    ParamItem `refreshable:"true"`
	SingleCompactionDeltaLogMaxSize   ParamItem `refreshable:"true"`
	SingleCompactionExpiredLogMaxSize ParamItem `refreshable:"true"`
	SingleCompactionDeltalogMaxNum    ParamItem `refreshable:"true"`

	ChannelCheckpointMaxLag ParamItem `refreshable:"true"`
	SyncSegmentsInterval    ParamItem `refreshable:"false"`

	// Index related configuration
	IndexMemSizeEstimateMultiplier ParamItem `refreshable:"true"`

	// Clustering Compaction
	ClusteringCompactionEnable                 ParamItem `refreshable:"true"`
	ClusteringCompactionAutoEnable             ParamItem `refreshable:"true"`
	ClusteringCompactionTriggerInterval        ParamItem `refreshable:"true"`
	ClusteringCompactionMinInterval            ParamItem `refreshable:"true"`
	ClusteringCompactionMaxInterval            ParamItem `refreshable:"true"`
	ClusteringCompactionNewDataSizeThreshold   ParamItem `refreshable:"true"`
	ClusteringCompactionPreferSegmentSizeRatio ParamItem `refreshable:"true"`
	ClusteringCompactionMaxSegmentSizeRatio    ParamItem `refreshable:"true"`
	ClusteringCompactionMaxTrainSizeRatio      ParamItem `refreshable:"true"`
	ClusteringCompactionTimeoutInSeconds       ParamItem `refreshable:"true"`
	ClusteringCompactionMaxCentroidsNum        ParamItem `refreshable:"true"`
	ClusteringCompactionMinCentroidsNum        ParamItem `refreshable:"true"`
	ClusteringCompactionMinClusterSizeRatio    ParamItem `refreshable:"true"`
	ClusteringCompactionMaxClusterSizeRatio    ParamItem `refreshable:"true"`
	ClusteringCompactionMaxClusterSize         ParamItem `refreshable:"true"`

	// LevelZero Segment
	LevelZeroCompactionTriggerMinSize        ParamItem `refreshable:"true"`
	LevelZeroCompactionTriggerMaxSize        ParamItem `refreshable:"true"`
	LevelZeroCompactionTriggerDeltalogMinNum ParamItem `refreshable:"true"`
	LevelZeroCompactionTriggerDeltalogMaxNum ParamItem `refreshable:"true"`

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
	TaskSlowThreshold          ParamItem `refreshable:"true"`

	MinSegmentNumRowsToEnableIndex ParamItem `refreshable:"true"`
	BrokerTimeout                  ParamItem `refreshable:"false"`

	// auto balance channel on datanode
	AutoBalance                    ParamItem `refreshable:"true"`
	CheckAutoBalanceConfigInterval ParamItem `refreshable:"false"`

	// import
	FilesPerPreImportTask    ParamItem `refreshable:"true"`
	ImportTaskRetention      ParamItem `refreshable:"true"`
	MaxSizeInMBPerImportTask ParamItem `refreshable:"true"`
	ImportScheduleInterval   ParamItem `refreshable:"true"`
	ImportCheckIntervalHigh  ParamItem `refreshable:"true"`
	ImportCheckIntervalLow   ParamItem `refreshable:"true"`
	MaxFilesPerImportReq     ParamItem `refreshable:"true"`
	MaxImportJobNum          ParamItem `refreshable:"true"`
	WaitForIndex             ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"true"`

	ClusteringCompactionSlotUsage ParamItem `refreshable:"true"`
	MixCompactionSlotUsage        ParamItem `refreshable:"true"`
	L0DeleteCompactionSlotUsage   ParamItem `refreshable:"true"`

	EnableStatsTask                   ParamItem `refreshable:"true"`
	TaskCheckInterval                 ParamItem `refreshable:"true"`
	StatsTaskTriggerCount             ParamItem `refreshable:"true"`
	JSONStatsTriggerCount             ParamItem `refreshable:"true"`
	JSONStatsTriggerInterval          ParamItem `refreshable:"true"`
	EnabledJSONKeyStatsInSort         ParamItem `refreshable:"true"`
	JSONKeyStatsMemoryBudgetInTantivy ParamItem `refreshable:"false"`

	RequestTimeoutSeconds ParamItem `refreshable:"true"`
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

	p.LegacyVersionWithoutRPCWatch = ParamItem{
		Key:          "dataCoord.channel.legacyVersionWithoutRPCWatch",
		Version:      "2.4.1",
		DefaultValue: "2.4.1",
		Doc:          "Datanodes <= this version are considered as legacy nodes, which doesn't have rpc based watch(). This is only used during rolling upgrade where legacy nodes won't get new channels",
		Export:       true,
	}
	p.LegacyVersionWithoutRPCWatch.Init(base.mgr)

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

	p.ChannelCheckInterval = ParamItem{
		Key:          "dataCoord.channel.checkInterval",
		Version:      "2.4.0",
		DefaultValue: "1",
		Doc:          "The interval in seconds with which the channel manager advances channel states",
		Export:       true,
	}
	p.ChannelCheckInterval.Init(base.mgr)

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
		DefaultValue: "1024",
		Doc:          "The maximum size of a segment, unit: MB. datacoord.segment.maxSize and datacoord.segment.sealProportion together determine if a segment can be sealed.",
		Export:       true,
	}
	p.SegmentMaxSize.Init(base.mgr)

	p.DiskSegmentMaxSize = ParamItem{
		Key:          "dataCoord.segment.diskSegmentMaxSize",
		Version:      "2.0.0",
		DefaultValue: "2048",
		Doc:          "Maximun size of a segment in MB for collection which has Disk index",
		Export:       true,
	}
	p.DiskSegmentMaxSize.Init(base.mgr)

	p.SegmentSealProportion = ParamItem{
		Key:          "dataCoord.segment.sealProportion",
		Version:      "2.0.0",
		DefaultValue: "0.12",
		Doc:          "The minimum proportion to datacoord.segment.maxSize to seal a segment. datacoord.segment.maxSize and datacoord.segment.sealProportion together determine if a segment can be sealed.",
		Export:       true,
	}
	p.SegmentSealProportion.Init(base.mgr)

	p.SegmentSealProportionJitter = ParamItem{
		Key:          "dataCoord.segment.sealProportionJitter",
		Version:      "2.4.6",
		DefaultValue: "0.1",
		Doc:          "segment seal proportion jitter ratio, default value 0.1(10%), if seal proportion is 12%, with jitter=0.1, the actuall applied ratio will be 10.8~12%",
		Export:       true,
	}
	p.SegmentSealProportionJitter.Init(base.mgr)

	p.SegAssignmentExpiration = ParamItem{
		Key:          "dataCoord.segment.assignmentExpiration",
		Version:      "2.0.0",
		DefaultValue: "2000",
		Doc:          "Expiration time of the segment assignment, unit: ms",
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
		Doc: `The max number of binlog (which is equal to the binlog file num of primary key) for one segment, 
the segment will be sealed if the number of binlog file reaches to max value.`,
		Export: true,
	}
	p.SegmentMaxBinlogFileNumber.Init(base.mgr)

	p.GrowingSegmentsMemSizeInMB = ParamItem{
		Key:          "dataCoord.sealPolicy.channel.growingSegmentsMemSize",
		Version:      "2.4.6",
		DefaultValue: "4096",
		Doc: `The size threshold in MB, if the total size of growing segments of each shard 
exceeds this threshold, the largest growing segment will be sealed.`,
		Export: true,
	}
	p.GrowingSegmentsMemSizeInMB.Init(base.mgr)

	p.BlockingL0EntryNum = ParamItem{
		Key:          "dataCoord.sealPolicy.channel.blockingL0EntryNum",
		Version:      "2.5.7",
		DefaultValue: "5000000",
		Doc: `If the total entry number of l0 logs of each shard 
exceeds this threshold, the earliest growing segments will be sealed.`,
		Export: true,
	}
	p.BlockingL0EntryNum.Init(base.mgr)

	p.BlockingL0SizeInMB = ParamItem{
		Key:          "dataCoord.sealPolicy.channel.blockingL0SizeInMB",
		Version:      "2.5.7",
		DefaultValue: "64",
		Doc: `The size threshold in MB, if the total entry number of l0 logs of each shard 
exceeds this threshold, the earliest growing segments will be sealed.`,
		Export: true,
	}
	p.BlockingL0SizeInMB.Init(base.mgr)

	p.EnableCompaction = ParamItem{
		Key:          "dataCoord.enableCompaction",
		Version:      "2.0.0",
		DefaultValue: "true",
		Doc: `Switch value to control if to enable segment compaction. 
Compaction merges small-size segments into a large segment, and clears the entities deleted beyond the rentention duration of Time Travel.`,
		Export: true,
	}
	p.EnableCompaction.Init(base.mgr)

	p.EnableAutoCompaction = ParamItem{
		Key:          "dataCoord.compaction.enableAutoCompaction",
		Version:      "2.0.0",
		DefaultValue: "true",
		Doc: `Switch value to control if to enable automatic segment compaction during which data coord locates and merges compactable segments in the background.
This configuration takes effect only when dataCoord.enableCompaction is set as true.`,
		Export: true,
	}
	p.EnableAutoCompaction.Init(base.mgr)

	p.IndexBasedCompaction = ParamItem{
		Key:          "dataCoord.compaction.indexBasedCompaction",
		Version:      "2.0.0",
		DefaultValue: "true",
		Export:       true,
	}
	p.IndexBasedCompaction.Init(base.mgr)

	p.CompactionTaskPrioritizer = ParamItem{
		Key:          "dataCoord.compaction.taskPrioritizer",
		Version:      "2.5.0",
		DefaultValue: "default",
		Doc: `compaction task prioritizer, options: [default, level, mix]. 
default is FIFO.
level is prioritized by level: L0 compactions first, then mix compactions, then clustering compactions.
mix is prioritized by level: mix compactions first, then L0 compactions, then clustering compactions.`,
		Export: true,
	}
	p.CompactionTaskPrioritizer.Init(base.mgr)

	p.CompactionTaskQueueCapacity = ParamItem{
		Key:          "dataCoord.compaction.taskQueueCapacity",
		Version:      "2.5.0",
		DefaultValue: "100000",
		Doc:          `compaction task queue size`,
		Export:       true,
	}
	p.CompactionTaskQueueCapacity.Init(base.mgr)

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

	p.MinSegmentToMerge = ParamItem{
		Key:          "dataCoord.compaction.min.segment",
		Version:      "2.0.0",
		DefaultValue: "3",
	}
	p.MinSegmentToMerge.Init(base.mgr)

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

	p.CompactionDropToleranceInSeconds = ParamItem{
		Key:          "dataCoord.compaction.dropTolerance",
		Version:      "2.4.2",
		Doc:          "Compaction task will be cleaned after finish longer than this time(in seconds)",
		DefaultValue: "86400",
		Export:       true,
	}
	p.CompactionDropToleranceInSeconds.Init(base.mgr)

	p.CompactionGCIntervalInSeconds = ParamItem{
		Key:          "dataCoord.compaction.gcInterval",
		Version:      "2.4.7",
		Doc:          "The time interval in seconds for compaction gc",
		DefaultValue: "1800",
		Export:       true,
	}
	p.CompactionGCIntervalInSeconds.Init(base.mgr)

	p.CompactionCheckIntervalInSeconds = ParamItem{
		Key:          "dataCoord.compaction.check.interval",
		Version:      "2.0.0",
		DefaultValue: "3",
	}
	p.CompactionCheckIntervalInSeconds.Init(base.mgr)

	p.CompactionScheduleInterval = ParamItem{
		Key:          "dataCoord.compaction.scheduleInterval",
		Version:      "2.4.21",
		DefaultValue: "500",
		Export:       true,
		Formatter: func(value string) string {
			ms := getAsInt64(value)
			if ms < 100 {
				ms = 100
			}
			return strconv.FormatInt(ms, 10)
		},
		Doc: "The time interval in milliseconds for scheduling compaction tasks. If the configuration setting is below 100ms, it will be ajusted upwards to 100ms",
	}
	p.CompactionScheduleInterval.Init(base.mgr)

	p.SingleCompactionRatioThreshold = ParamItem{
		Key:          "dataCoord.compaction.single.ratio.threshold",
		Version:      "2.0.0",
		DefaultValue: "0.2",
		Doc:          "The ratio threshold of a segment to trigger a single compaction, default as 0.2",
		Export:       true,
	}
	p.SingleCompactionRatioThreshold.Init(base.mgr)

	p.SingleCompactionDeltaLogMaxSize = ParamItem{
		Key:          "dataCoord.compaction.single.deltalog.maxsize",
		Version:      "2.0.0",
		DefaultValue: "16777216",
		Doc:          "The deltalog size of a segment to trigger a single compaction, default as 16MB",
		Export:       true,
	}
	p.SingleCompactionDeltaLogMaxSize.Init(base.mgr)

	p.SingleCompactionExpiredLogMaxSize = ParamItem{
		Key:          "dataCoord.compaction.single.expiredlog.maxsize",
		Version:      "2.0.0",
		DefaultValue: "10485760",
		Doc:          "The expired log size of a segment to trigger a compaction, default as 10MB",
		Export:       true,
	}
	p.SingleCompactionExpiredLogMaxSize.Init(base.mgr)

	p.SingleCompactionDeltalogMaxNum = ParamItem{
		Key:          "dataCoord.compaction.single.deltalog.maxnum",
		Version:      "2.0.0",
		DefaultValue: "200",
		Doc:          "The deltalog count of a segment to trigger a compaction, default as 200",
		Export:       true,
	}
	p.SingleCompactionDeltalogMaxNum.Init(base.mgr)

	p.GlobalCompactionInterval = ParamItem{
		Key:          "dataCoord.compaction.global.interval",
		Version:      "2.0.0",
		DefaultValue: "60",
		Doc:          "deprecated",
	}
	p.GlobalCompactionInterval.Init(base.mgr)

	p.MixCompactionTriggerInterval = ParamItem{
		Key:          "dataCoord.compaction.mix.triggerInterval",
		Version:      "2.4.15",
		Doc:          "The time interval in seconds to trigger mix compaction",
		DefaultValue: "60",
		Export:       true,
	}
	p.MixCompactionTriggerInterval.Init(base.mgr)

	p.L0CompactionTriggerInterval = ParamItem{
		Key:          "dataCoord.compaction.levelzero.triggerInterval",
		Version:      "2.4.15",
		Doc:          "The time interval in seconds for trigger L0 compaction",
		DefaultValue: "10",
		Export:       true,
	}
	p.L0CompactionTriggerInterval.Init(base.mgr)

	p.ChannelCheckpointMaxLag = ParamItem{
		Key:          "dataCoord.compaction.channelMaxCPLag",
		Version:      "2.4.0",
		Doc:          "max tolerable channel checkpoint lag(in seconds) to execute compaction",
		DefaultValue: "900", // 15 * 60 seconds
	}
	p.ChannelCheckpointMaxLag.Init(base.mgr)

	p.SyncSegmentsInterval = ParamItem{
		Key:          "dataCoord.syncSegmentsInterval",
		Version:      "2.4.6",
		Doc:          "The time interval for regularly syncing segments",
		DefaultValue: "300", // 5 * 60 seconds
		Export:       true,
	}
	p.SyncSegmentsInterval.Init(base.mgr)

	p.LevelZeroCompactionTriggerMinSize = ParamItem{
		Key:          "dataCoord.compaction.levelzero.forceTrigger.minSize",
		Version:      "2.4.0",
		Doc:          "The minmum size in bytes to force trigger a LevelZero Compaction, default as 8MB",
		DefaultValue: "8388608",
		Export:       true,
	}
	p.LevelZeroCompactionTriggerMinSize.Init(base.mgr)

	p.LevelZeroCompactionTriggerMaxSize = ParamItem{
		Key:          "dataCoord.compaction.levelzero.forceTrigger.maxSize",
		Version:      "2.4.0",
		Doc:          "The maxmum size in bytes to force trigger a LevelZero Compaction, default as 64MB",
		DefaultValue: "67108864",
		Export:       true,
	}
	p.LevelZeroCompactionTriggerMaxSize.Init(base.mgr)

	p.LevelZeroCompactionTriggerDeltalogMinNum = ParamItem{
		Key:          "dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum",
		Version:      "2.4.0",
		Doc:          "The minimum number of deltalog files to force trigger a LevelZero Compaction",
		DefaultValue: "10",
		Export:       true,
	}
	p.LevelZeroCompactionTriggerDeltalogMinNum.Init(base.mgr)

	p.LevelZeroCompactionTriggerDeltalogMaxNum = ParamItem{
		Key:          "dataCoord.compaction.levelzero.forceTrigger.deltalogMaxNum",
		Version:      "2.4.0",
		Doc:          "The maxmum number of deltalog files to force trigger a LevelZero Compaction, default as 30",
		DefaultValue: "30",
		Export:       true,
	}
	p.LevelZeroCompactionTriggerDeltalogMaxNum.Init(base.mgr)

	p.IndexMemSizeEstimateMultiplier = ParamItem{
		Key:          "dataCoord.index.memSizeEstimateMultiplier",
		Version:      "2.4.19",
		DefaultValue: "2",
		Doc:          "When the memory size is not setup by index procedure, multiplier to estimate the memory size of index data",
		Export:       true,
	}
	p.IndexMemSizeEstimateMultiplier.Init(base.mgr)

	p.ClusteringCompactionEnable = ParamItem{
		Key:          "dataCoord.compaction.clustering.enable",
		Version:      "2.4.7",
		DefaultValue: "false",
		Doc:          "Enable clustering compaction",
		Export:       true,
	}
	p.ClusteringCompactionEnable.Init(base.mgr)

	p.ClusteringCompactionAutoEnable = ParamItem{
		Key:          "dataCoord.compaction.clustering.autoEnable",
		Version:      "2.4.7",
		DefaultValue: "false",
		Doc:          "Enable auto clustering compaction",
		Export:       true,
	}
	p.ClusteringCompactionAutoEnable.Init(base.mgr)

	p.ClusteringCompactionTriggerInterval = ParamItem{
		Key:          "dataCoord.compaction.clustering.triggerInterval",
		Version:      "2.4.7",
		DefaultValue: "600",
		Doc:          "clustering compaction trigger interval in seconds",
		Export:       true,
	}
	p.ClusteringCompactionTriggerInterval.Init(base.mgr)

	p.ClusteringCompactionMinInterval = ParamItem{
		Key:          "dataCoord.compaction.clustering.minInterval",
		Version:      "2.4.7",
		Doc:          "The minimum interval between clustering compaction executions of one collection, to avoid redundant compaction",
		DefaultValue: "3600",
		Export:       true,
	}
	p.ClusteringCompactionMinInterval.Init(base.mgr)

	p.ClusteringCompactionMaxInterval = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxInterval",
		Version:      "2.4.7",
		Doc:          "If a collection haven't been clustering compacted for longer than maxInterval, force compact",
		DefaultValue: "86400",
		Export:       true,
	}
	p.ClusteringCompactionMaxInterval.Init(base.mgr)

	p.ClusteringCompactionNewDataSizeThreshold = ParamItem{
		Key:          "dataCoord.compaction.clustering.newDataSizeThreshold",
		Version:      "2.4.7",
		Doc:          "If new data size is large than newDataSizeThreshold, execute clustering compaction",
		DefaultValue: "512m",
		Export:       true,
	}
	p.ClusteringCompactionNewDataSizeThreshold.Init(base.mgr)

	p.ClusteringCompactionTimeoutInSeconds = ParamItem{
		Key:          "dataCoord.compaction.clustering.timeout",
		Version:      "2.4.7",
		DefaultValue: "3600",
	}
	p.ClusteringCompactionTimeoutInSeconds.Init(base.mgr)

	p.ClusteringCompactionPreferSegmentSizeRatio = ParamItem{
		Key:          "dataCoord.compaction.clustering.preferSegmentSizeRatio",
		Version:      "2.4.7",
		DefaultValue: "0.8",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ClusteringCompactionPreferSegmentSizeRatio.Init(base.mgr)

	p.ClusteringCompactionMaxSegmentSizeRatio = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxSegmentSizeRatio",
		Version:      "2.4.7",
		DefaultValue: "1.0",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ClusteringCompactionMaxSegmentSizeRatio.Init(base.mgr)

	p.ClusteringCompactionMaxTrainSizeRatio = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxTrainSizeRatio",
		Version:      "2.4.7",
		DefaultValue: "0.8",
		Doc:          "max data size ratio in Kmeans train, if larger than it, will down sampling to meet this limit",
		Export:       true,
	}
	p.ClusteringCompactionMaxTrainSizeRatio.Init(base.mgr)

	p.ClusteringCompactionMaxCentroidsNum = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxCentroidsNum",
		Version:      "2.4.7",
		DefaultValue: "10240",
		Doc:          "maximum centroids number in Kmeans train",
		Export:       true,
	}
	p.ClusteringCompactionMaxCentroidsNum.Init(base.mgr)

	p.ClusteringCompactionMinCentroidsNum = ParamItem{
		Key:          "dataCoord.compaction.clustering.minCentroidsNum",
		Version:      "2.4.7",
		DefaultValue: "16",
		Doc:          "minimum centroids number in Kmeans train",
		Export:       true,
	}
	p.ClusteringCompactionMinCentroidsNum.Init(base.mgr)

	p.ClusteringCompactionMinClusterSizeRatio = ParamItem{
		Key:          "dataCoord.compaction.clustering.minClusterSizeRatio",
		Version:      "2.4.7",
		DefaultValue: "0.01",
		Doc:          "minimum cluster size / avg size in Kmeans train",
		Export:       true,
	}
	p.ClusteringCompactionMinClusterSizeRatio.Init(base.mgr)

	p.ClusteringCompactionMaxClusterSizeRatio = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxClusterSizeRatio",
		Version:      "2.4.7",
		DefaultValue: "10",
		Doc:          "maximum cluster size / avg size in Kmeans train",
		Export:       true,
	}
	p.ClusteringCompactionMaxClusterSizeRatio.Init(base.mgr)

	p.ClusteringCompactionMaxClusterSize = ParamItem{
		Key:          "dataCoord.compaction.clustering.maxClusterSize",
		Version:      "2.4.7",
		DefaultValue: "5g",
		Doc:          "maximum cluster size in Kmeans train",
		Export:       true,
	}
	p.ClusteringCompactionMaxClusterSize.Init(base.mgr)

	p.EnableGarbageCollection = ParamItem{
		Key:          "dataCoord.enableGarbageCollection",
		Version:      "2.0.0",
		DefaultValue: "true",
		Doc:          "Switch value to control if to enable garbage collection to clear the discarded data in MinIO or S3 service.",
		Export:       true,
	}
	p.EnableGarbageCollection.Init(base.mgr)

	p.GCInterval = ParamItem{
		Key:          "dataCoord.gc.interval",
		Version:      "2.0.0",
		DefaultValue: "3600",
		Doc:          "The interval at which data coord performs garbage collection, unit: second.",
		Export:       true,
	}
	p.GCInterval.Init(base.mgr)

	p.GCScanIntervalInHour = ParamItem{
		Key:          "dataCoord.gc.scanInterval",
		Version:      "2.4.0",
		DefaultValue: "168", // hours, default 7 * 24 hours
		Doc:          "orphan file (file on oss but has not been registered on meta) on object storage garbage collection scanning interval in hours",
		Export:       true,
	}
	p.GCScanIntervalInHour.Init(base.mgr)

	// Do not set this to incredible small value, make sure this to be more than 10 minutes at least
	p.GCMissingTolerance = ParamItem{
		Key:          "dataCoord.gc.missingTolerance",
		Version:      "2.0.0",
		DefaultValue: "86400",
		Doc:          "The retention duration of the unrecorded binary log (binlog) files. Setting a reasonably large value for this parameter avoids erroneously deleting the newly created binlog files that lack metadata. Unit: second.",
		Export:       true,
	}
	p.GCMissingTolerance.Init(base.mgr)

	p.GCDropTolerance = ParamItem{
		Key:          "dataCoord.gc.dropTolerance",
		Version:      "2.0.0",
		DefaultValue: "10800",
		Doc:          "The retention duration of the binlog files of the deleted segments before they are cleared, unit: second.",
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

	p.TaskSlowThreshold = ParamItem{
		Key:          "datacoord.scheduler.taskSlowThreshold",
		Version:      "2.0.0",
		DefaultValue: "300",
	}
	p.TaskSlowThreshold.Init(base.mgr)

	p.BrokerTimeout = ParamItem{
		Key:          "dataCoord.brokerTimeout",
		Version:      "2.3.0",
		DefaultValue: "5000",
		PanicIfEmpty: true,
		Doc:          "5000ms, dataCoord broker rpc timeout",
		Export:       true,
	}
	p.BrokerTimeout.Init(base.mgr)

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

	p.SegmentFlushInterval = ParamItem{
		Key:          "dataCoord.segmentFlushInterval",
		Version:      "2.4.6",
		DefaultValue: "2",
		Doc:          "the minimal interval duration(unit: Seconds) between flusing operation on same segment",
		Export:       true,
	}
	p.SegmentFlushInterval.Init(base.mgr)

	p.FilesPerPreImportTask = ParamItem{
		Key:          "dataCoord.import.filesPerPreImportTask",
		Version:      "2.4.0",
		Doc:          "The maximum number of files allowed per pre-import task.",
		DefaultValue: "2",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.FilesPerPreImportTask.Init(base.mgr)

	p.ImportTaskRetention = ParamItem{
		Key:          "dataCoord.import.taskRetention",
		Version:      "2.4.0",
		Doc:          "The retention period in seconds for tasks in the Completed or Failed state.",
		DefaultValue: "10800",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ImportTaskRetention.Init(base.mgr)

	p.MaxSizeInMBPerImportTask = ParamItem{
		Key:     "dataCoord.import.maxSizeInMBPerImportTask",
		Version: "2.4.0",
		Doc: "To prevent generating of small segments, we will re-group imported files. " +
			"This parameter represents the sum of file sizes in each group (each ImportTask).",
		DefaultValue: "6144",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxSizeInMBPerImportTask.Init(base.mgr)

	p.ImportScheduleInterval = ParamItem{
		Key:          "dataCoord.import.scheduleInterval",
		Version:      "2.4.0",
		Doc:          "The interval for scheduling import, measured in seconds.",
		DefaultValue: "2",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ImportScheduleInterval.Init(base.mgr)

	p.ImportCheckIntervalHigh = ParamItem{
		Key:          "dataCoord.import.checkIntervalHigh",
		Version:      "2.4.0",
		Doc:          "The interval for checking import, measured in seconds, is set to a high frequency for the import checker.",
		DefaultValue: "2",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ImportCheckIntervalHigh.Init(base.mgr)

	p.ImportCheckIntervalLow = ParamItem{
		Key:          "dataCoord.import.checkIntervalLow",
		Version:      "2.4.0",
		Doc:          "The interval for checking import, measured in seconds, is set to a low frequency for the import checker.",
		DefaultValue: "120",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ImportCheckIntervalLow.Init(base.mgr)

	p.MaxFilesPerImportReq = ParamItem{
		Key:          "dataCoord.import.maxImportFileNumPerReq",
		Version:      "2.4.0",
		Doc:          "The maximum number of files allowed per single import request.",
		DefaultValue: "1024",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxFilesPerImportReq.Init(base.mgr)

	p.MaxImportJobNum = ParamItem{
		Key:          "dataCoord.import.maxImportJobNum",
		Version:      "2.4.14",
		Doc:          "Maximum number of import jobs that are executing or pending.",
		DefaultValue: "1024",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxImportJobNum.Init(base.mgr)

	p.WaitForIndex = ParamItem{
		Key:          "dataCoord.import.waitForIndex",
		Version:      "2.4.0",
		Doc:          "Indicates whether the import operation waits for the completion of index building.",
		DefaultValue: "true",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.WaitForIndex.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "dataCoord.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultCoordGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.ClusteringCompactionSlotUsage = ParamItem{
		Key:          "dataCoord.slot.clusteringCompactionUsage",
		Version:      "2.4.6",
		Doc:          "slot usage of clustering compaction job.",
		DefaultValue: "16",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ClusteringCompactionSlotUsage.Init(base.mgr)

	p.MixCompactionSlotUsage = ParamItem{
		Key:          "dataCoord.slot.mixCompactionUsage",
		Version:      "2.4.6",
		Doc:          "slot usage of mix compaction job.",
		DefaultValue: "8",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MixCompactionSlotUsage.Init(base.mgr)

	p.L0DeleteCompactionSlotUsage = ParamItem{
		Key:          "dataCoord.slot.l0DeleteCompactionUsage",
		Version:      "2.4.6",
		Doc:          "slot usage of l0 compaction job.",
		DefaultValue: "8",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.L0DeleteCompactionSlotUsage.Init(base.mgr)

	p.EnableStatsTask = ParamItem{
		Key:          "dataCoord.statsTask.enable",
		Version:      "2.5.0",
		Doc:          "enable stats task",
		DefaultValue: "true",
		PanicIfEmpty: false,
		Export:       false,
	}
	p.EnableStatsTask.Init(base.mgr)

	p.TaskCheckInterval = ParamItem{
		Key:          "dataCoord.taskCheckInterval",
		Version:      "2.5.0",
		Doc:          "task check interval seconds",
		DefaultValue: "60",
		PanicIfEmpty: false,
		Export:       false,
	}
	p.TaskCheckInterval.Init(base.mgr)

	p.StatsTaskTriggerCount = ParamItem{
		Key:          "dataCoord.statsTaskTriggerCount",
		Version:      "2.5.5",
		Doc:          "stats task count per trigger",
		DefaultValue: "100",
		PanicIfEmpty: false,
		Export:       false,
	}
	p.StatsTaskTriggerCount.Init(base.mgr)

	p.JSONStatsTriggerCount = ParamItem{
		Key:          "dataCoord.jsonStatsTriggerCount",
		Version:      "2.5.5",
		Doc:          "jsonkey stats task count per trigger",
		DefaultValue: "10",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.JSONStatsTriggerCount.Init(base.mgr)

	p.JSONStatsTriggerInterval = ParamItem{
		Key:          "dataCoord.jsonStatsTriggerInterval",
		Version:      "2.5.5",
		Doc:          "jsonkey task interval per trigger",
		DefaultValue: "10",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.JSONStatsTriggerInterval.Init(base.mgr)

	p.RequestTimeoutSeconds = ParamItem{
		Key:          "dataCoord.requestTimeoutSeconds",
		Version:      "2.5.5",
		Doc:          "request timeout interval",
		DefaultValue: "600",
		PanicIfEmpty: false,
		Export:       false,
	}
	p.RequestTimeoutSeconds.Init(base.mgr)
	p.JSONKeyStatsMemoryBudgetInTantivy = ParamItem{
		Key:          "dataCoord.jsonKeyStatsMemoryBudgetInTantivy",
		Version:      "2.5.5",
		DefaultValue: "16777216",
		Doc:          "the memory budget for the JSON index In Tantivy, the unit is bytes",
		Export:       true,
	}
	p.JSONKeyStatsMemoryBudgetInTantivy.Init(base.mgr)

	p.EnabledJSONKeyStatsInSort = ParamItem{
		Key:          "dataCoord.enabledJSONKeyStatsInSort",
		Version:      "2.5.5",
		DefaultValue: "false",
		Doc:          "Indicates whether to enable JSON key stats task with sort",
		Export:       true,
	}
	p.EnabledJSONKeyStatsInSort.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- datanode ---
type dataNodeConfig struct {
	FlowGraphMaxQueueLength ParamItem `refreshable:"false"`
	FlowGraphMaxParallelism ParamItem `refreshable:"false"`
	MaxParallelSyncTaskNum  ParamItem `refreshable:"false"`
	MaxParallelSyncMgrTasks ParamItem `refreshable:"true"`

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
	MemoryCheckInterval       ParamItem `refreshable:"true"`
	MemoryForceSyncWatermark  ParamItem `refreshable:"true"`

	// DataNode send timetick interval per collection
	DataNodeTimeTickInterval ParamItem `refreshable:"false"`

	// Skip BF
	SkipBFStatsLoad ParamItem `refreshable:"true"`

	// channel
	ChannelWorkPoolSize ParamItem `refreshable:"true"`

	UpdateChannelCheckpointMaxParallel   ParamItem `refreshable:"true"`
	UpdateChannelCheckpointInterval      ParamItem `refreshable:"true"`
	UpdateChannelCheckpointRPCTimeout    ParamItem `refreshable:"true"`
	MaxChannelCheckpointsPerRPC          ParamItem `refreshable:"true"`
	ChannelCheckpointUpdateTickInSeconds ParamItem `refreshable:"true"`

	// import
	MaxConcurrentImportTaskNum ParamItem `refreshable:"true"`
	MaxImportFileSizeInGB      ParamItem `refreshable:"true"`
	ReadBufferSizeInMB         ParamItem `refreshable:"true"`
	MaxTaskSlotNum             ParamItem `refreshable:"true"`

	// Compaction
	L0BatchMemoryRatio       ParamItem `refreshable:"true"`
	L0CompactionMaxBatchSize ParamItem `refreshable:"true"`
	UseMergeSort             ParamItem `refreshable:"true"`
	MaxSegmentMergeSort      ParamItem `refreshable:"true"`

	GracefulStopTimeout ParamItem `refreshable:"true"`

	// slot
	SlotCap ParamItem `refreshable:"true"`

	// clustering compaction
	ClusteringCompactionMemoryBufferRatio ParamItem `refreshable:"true"`
	ClusteringCompactionWorkerPoolSize    ParamItem `refreshable:"true"`

	BloomFilterApplyParallelFactor ParamItem `refreshable:"true"`

	DeltalogFormat ParamItem `refreshable:"false"`
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
		Key:          "dataNode.dataSync.skipMode.enable",
		Version:      "2.3.4",
		DefaultValue: "true",
		PanicIfEmpty: false,
		Doc:          "Support skip some timetick message to reduce CPU usage",
		Export:       true,
	}
	p.FlowGraphSkipModeEnable.Init(base.mgr)

	p.FlowGraphSkipModeSkipNum = ParamItem{
		Key:          "dataNode.dataSync.skipMode.skipNum",
		Version:      "2.3.4",
		DefaultValue: "4",
		PanicIfEmpty: false,
		Doc:          "Consume one for every n records skipped",
		Export:       true,
	}
	p.FlowGraphSkipModeSkipNum.Init(base.mgr)

	p.FlowGraphSkipModeColdTime = ParamItem{
		Key:          "dataNode.dataSync.skipMode.coldTime",
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
		Doc:          "deprecated, legacy flush manager max conurrency number",
		Export:       false,
	}
	p.MaxParallelSyncTaskNum.Init(base.mgr)

	p.MaxParallelSyncMgrTasks = ParamItem{
		Key:          "dataNode.dataSync.maxParallelSyncMgrTasks",
		Version:      "2.3.4",
		DefaultValue: "256",
		Doc:          "The max concurrent sync task number of datanode sync mgr globally",
		Formatter: func(v string) string {
			concurrency := getAsInt(v)
			if concurrency < 1 {
				log.Warn("positive parallel task number, reset to default 256", zap.String("value", v))
				return "256" // MaxParallelSyncMgrTasks must >= 1
			}
			return strconv.FormatInt(int64(concurrency), 10)
		},
		Export: true,
	}
	p.MaxParallelSyncMgrTasks.Init(base.mgr)

	p.FlushInsertBufferSize = ParamItem{
		Key:          "dataNode.segment.insertBufSize",
		Version:      "2.0.0",
		FallbackKeys: []string{"DATA_NODE_IBUFSIZE"},
		DefaultValue: "16777216",
		PanicIfEmpty: true,
		Doc: `The maximum size of each binlog file in a segment buffered in memory. Binlog files whose size exceeds this value are then flushed to MinIO or S3 service.
Unit: Byte
Setting this parameter too small causes the system to store a small amount of data too frequently. Setting it too large increases the system's demand for memory.`,
		Export: true,
	}
	p.FlushInsertBufferSize.Init(base.mgr)

	p.MemoryForceSyncEnable = ParamItem{
		Key:          "dataNode.memory.forceSyncEnable",
		Version:      "2.2.4",
		DefaultValue: "true",
		Doc:          "Set true to force sync if memory usage is too high",
		Export:       true,
	}
	p.MemoryForceSyncEnable.Init(base.mgr)

	p.MemoryForceSyncSegmentNum = ParamItem{
		Key:          "dataNode.memory.forceSyncSegmentNum",
		Version:      "2.2.4",
		DefaultValue: "1",
		Doc:          "number of segments to sync, segments with top largest buffer will be synced.",
		Export:       true,
	}
	p.MemoryForceSyncSegmentNum.Init(base.mgr)

	p.MemoryCheckInterval = ParamItem{
		Key:          "dataNode.memory.checkInterval",
		Version:      "2.4.0",
		DefaultValue: "3000", // milliseconds
		Doc:          "the interal to check datanode memory usage, in milliseconds",
		Export:       true,
	}
	p.MemoryCheckInterval.Init(base.mgr)

	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		p.MemoryForceSyncWatermark = ParamItem{
			Key:          "dataNode.memory.forceSyncWatermark",
			Version:      "2.4.0",
			DefaultValue: "0.2",
			Doc:          "memory watermark for standalone, upon reaching this watermark, segments will be synced.",
			Export:       true,
		}
	} else {
		log.Info("DeployModeEnv is not set, use default", zap.Float64("default", 0.5))
		p.MemoryForceSyncWatermark = ParamItem{
			Key:          "dataNode.memory.forceSyncWatermark",
			Version:      "2.4.0",
			DefaultValue: "0.5",
			Doc:          "memory watermark for standalone, upon reaching this watermark, segments will be synced.",
			Export:       true,
		}
	}
	p.MemoryForceSyncWatermark.Init(base.mgr)

	p.FlushDeleteBufferBytes = ParamItem{
		Key:          "dataNode.segment.deleteBufBytes",
		Version:      "2.0.0",
		DefaultValue: "16777216",
		Doc:          "Max buffer size in bytes to flush del for a single channel, default as 16MB",
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
		Key:          "dataNode.segment.watchEventTicklerInterval",
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

	p.DataNodeTimeTickInterval = ParamItem{
		Key:          "dataNode.timetick.interval",
		Version:      "2.2.5",
		PanicIfEmpty: false,
		DefaultValue: "500",
		Export:       true,
	}
	p.DataNodeTimeTickInterval.Init(base.mgr)

	p.SkipBFStatsLoad = ParamItem{
		Key:          "dataNode.skip.BFStats.Load",
		Version:      "2.2.5",
		PanicIfEmpty: false,
		DefaultValue: "true",
		Forbidden:    true, // The SkipBFStatsLoad is a static config that not allow dynamic refresh.
	}
	p.SkipBFStatsLoad.Init(base.mgr)

	p.ChannelWorkPoolSize = ParamItem{
		Key:          "dataNode.channel.workPoolSize",
		Version:      "2.3.2",
		PanicIfEmpty: false,
		DefaultValue: "-1",
		Doc: `specify the size of global work pool of all channels
if this parameter <= 0, will set it as the maximum number of CPUs that can be executing
suggest to set it bigger on large collection numbers to avoid blocking`,
		Export: true,
	}
	p.ChannelWorkPoolSize.Init(base.mgr)

	p.UpdateChannelCheckpointMaxParallel = ParamItem{
		Key:          "dataNode.channel.updateChannelCheckpointMaxParallel",
		Version:      "2.3.4",
		PanicIfEmpty: false,
		DefaultValue: "10",
		Doc: `specify the size of global work pool for channel checkpoint updating
if this parameter <= 0, will set it as 10`,
		Export: true,
	}
	p.UpdateChannelCheckpointMaxParallel.Init(base.mgr)

	p.UpdateChannelCheckpointInterval = ParamItem{
		Key:          "dataNode.channel.updateChannelCheckpointInterval",
		Version:      "2.4.0",
		Doc:          "the interval duration(in seconds) for datanode to update channel checkpoint of each channel",
		DefaultValue: "60",
		Export:       true,
	}
	p.UpdateChannelCheckpointInterval.Init(base.mgr)

	p.UpdateChannelCheckpointRPCTimeout = ParamItem{
		Key:          "dataNode.channel.updateChannelCheckpointRPCTimeout",
		Version:      "2.4.0",
		Doc:          "timeout in seconds for UpdateChannelCheckpoint RPC call",
		DefaultValue: "20",
		Export:       true,
	}
	p.UpdateChannelCheckpointRPCTimeout.Init(base.mgr)

	p.MaxChannelCheckpointsPerRPC = ParamItem{
		Key:          "dataNode.channel.maxChannelCheckpointsPerPRC",
		Version:      "2.4.0",
		Doc:          "The maximum number of channel checkpoints per UpdateChannelCheckpoint RPC.",
		DefaultValue: "128",
		Export:       true,
	}
	p.MaxChannelCheckpointsPerRPC.Init(base.mgr)

	p.ChannelCheckpointUpdateTickInSeconds = ParamItem{
		Key:          "dataNode.channel.channelCheckpointUpdateTickInSeconds",
		Version:      "2.4.0",
		Doc:          "The frequency, in seconds, at which the channel checkpoint updater executes updates.",
		DefaultValue: "10",
		Export:       true,
	}
	p.ChannelCheckpointUpdateTickInSeconds.Init(base.mgr)

	p.MaxConcurrentImportTaskNum = ParamItem{
		Key:          "dataNode.import.maxConcurrentTaskNum",
		Version:      "2.4.0",
		Doc:          "The maximum number of import/pre-import tasks allowed to run concurrently on a datanode.",
		DefaultValue: "16",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxConcurrentImportTaskNum.Init(base.mgr)

	p.MaxImportFileSizeInGB = ParamItem{
		Key:          "dataNode.import.maxImportFileSizeInGB",
		Version:      "2.4.0",
		Doc:          "The maximum file size (in GB) for an import file, where an import file refers to either a Row-Based file or a set of Column-Based files.",
		DefaultValue: "16",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxImportFileSizeInGB.Init(base.mgr)

	p.ReadBufferSizeInMB = ParamItem{
		Key:          "dataNode.import.readBufferSizeInMB",
		Version:      "2.4.0",
		Doc:          "The data block size (in MB) read from chunk manager by the datanode during import.",
		DefaultValue: "16",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ReadBufferSizeInMB.Init(base.mgr)

	p.MaxTaskSlotNum = ParamItem{
		Key:          "dataNode.import.maxTaskSlotNum",
		Version:      "2.4.13",
		Doc:          "The maximum number of slots occupied by each import/pre-import task.",
		DefaultValue: "16",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxTaskSlotNum.Init(base.mgr)

	p.L0BatchMemoryRatio = ParamItem{
		Key:          "dataNode.compaction.levelZeroBatchMemoryRatio",
		Version:      "2.4.0",
		Doc:          "The minimal memory ratio of free memory for level zero compaction executing in batch mode",
		DefaultValue: "0.5",
		Export:       true,
	}
	p.L0BatchMemoryRatio.Init(base.mgr)

	p.L0CompactionMaxBatchSize = ParamItem{
		Key:          "dataNode.compaction.levelZeroMaxBatchSize",
		Version:      "2.4.5",
		Doc:          "Max batch size refers to the max number of L1/L2 segments in a batch when executing L0 compaction. Default to -1, any value that is less than 1 means no limit. Valid range: >= 1.",
		DefaultValue: "-1",
		Export:       true,
	}
	p.L0CompactionMaxBatchSize.Init(base.mgr)

	p.UseMergeSort = ParamItem{
		Key:          "dataNode.compaction.useMergeSort",
		Version:      "2.5.0",
		Doc:          "Whether to enable mergeSort mode when performing mixCompaction.",
		DefaultValue: "false",
		Export:       true,
	}
	p.UseMergeSort.Init(base.mgr)

	p.MaxSegmentMergeSort = ParamItem{
		Key:          "dataNode.compaction.maxSegmentMergeSort",
		Version:      "2.5.0",
		Doc:          "The maximum number of segments to be merged in mergeSort mode.",
		DefaultValue: "30",
		Export:       true,
	}
	p.MaxSegmentMergeSort.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "dataNode.gracefulStopTimeout",
		Version:      "2.3.7",
		DefaultValue: strconv.Itoa(DefaultGracefulStopTimeout),
		Doc:          "seconds. force stop node without graceful stop",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)

	p.SlotCap = ParamItem{
		Key:          "dataNode.slot.slotCap",
		Version:      "2.4.2",
		DefaultValue: "16",
		Doc:          "The maximum number of tasks(e.g. compaction, importing) allowed to run concurrently on a datanode",
		Export:       true,
	}
	p.SlotCap.Init(base.mgr)

	p.ClusteringCompactionMemoryBufferRatio = ParamItem{
		Key:          "dataNode.clusteringCompaction.memoryBufferRatio",
		Version:      "2.4.6",
		Doc:          "The ratio of memory buffer of clustering compaction. Data larger than threshold will be flushed to storage.",
		DefaultValue: "0.3",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ClusteringCompactionMemoryBufferRatio.Init(base.mgr)

	p.ClusteringCompactionWorkerPoolSize = ParamItem{
		Key:          "dataNode.clusteringCompaction.workPoolSize",
		Version:      "2.4.6",
		Doc:          "worker pool size for one clustering compaction job.",
		DefaultValue: "8",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.ClusteringCompactionWorkerPoolSize.Init(base.mgr)

	p.BloomFilterApplyParallelFactor = ParamItem{
		Key:          "dataNode.bloomFilterApplyParallelFactor",
		FallbackKeys: []string{"datanode.bloomFilterApplyBatchSize"},
		Version:      "2.4.5",
		DefaultValue: "4",
		Doc:          "parallel factor when to apply pk to bloom filter, default to 4*CPU_CORE_NUM",
		Export:       true,
	}
	p.BloomFilterApplyParallelFactor.Init(base.mgr)

	p.DeltalogFormat = ParamItem{
		Key:          "dataNode.storage.deltalog",
		Version:      "2.5.0",
		DefaultValue: "json",
		Doc:          "deltalog format, options: [json, parquet]",
		Export:       true,
	}
	p.DeltalogFormat.Init(base.mgr)
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
				// use local storage path to check correct device
				localStoragePath := base.Get("localStorage.path")
				if _, err := os.Stat(localStoragePath); os.IsNotExist(err) {
					if err := os.MkdirAll(localStoragePath, os.ModePerm); err != nil {
						log.Fatal("failed to mkdir", zap.String("localStoragePath", localStoragePath), zap.Error(err))
					}
				}
				diskUsage, err := disk.Usage(localStoragePath)
				if err != nil {
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
	}
	p.GracefulStopTimeout.Init(base.mgr)
}

type streamingConfig struct {
	// balancer
	WALBalancerTriggerInterval        ParamItem `refreshable:"true"`
	WALBalancerBackoffInitialInterval ParamItem `refreshable:"true"`
	WALBalancerBackoffMultiplier      ParamItem `refreshable:"true"`

	// broadcaster
	WALBroadcasterConcurrencyRatio ParamItem `refreshable:"false"`

	// txn
	TxnDefaultKeepaliveTimeout ParamItem `refreshable:"true"`
}

func (p *streamingConfig) init(base *BaseTable) {
	// balancer
	p.WALBalancerTriggerInterval = ParamItem{
		Key:     "streaming.walBalancer.triggerInterval",
		Version: "2.5.0",
		Doc: `The interval of balance task trigger at background, 1 min by default. 
It's ok to set it into duration string, such as 30s or 1m30s, see time.ParseDuration`,
		DefaultValue: "1m",
		Export:       true,
	}
	p.WALBalancerTriggerInterval.Init(base.mgr)
	p.WALBalancerBackoffInitialInterval = ParamItem{
		Key:     "streaming.walBalancer.backoffInitialInterval",
		Version: "2.5.0",
		Doc: `The initial interval of balance task trigger backoff, 50 ms by default. 
It's ok to set it into duration string, such as 30s or 1m30s, see time.ParseDuration`,
		DefaultValue: "50ms",
		Export:       true,
	}
	p.WALBalancerBackoffInitialInterval.Init(base.mgr)
	p.WALBalancerBackoffMultiplier = ParamItem{
		Key:          "streaming.walBalancer.backoffMultiplier",
		Version:      "2.5.0",
		Doc:          "The multiplier of balance task trigger backoff, 2 by default",
		DefaultValue: "2",
		Export:       true,
	}
	p.WALBalancerBackoffMultiplier.Init(base.mgr)

	p.WALBroadcasterConcurrencyRatio = ParamItem{
		Key:          "streaming.walBroadcaster.concurrencyRatio",
		Version:      "2.5.4",
		Doc:          `The concurrency ratio based on number of CPU for wal broadcaster, 1 by default.`,
		DefaultValue: "1",
		Export:       true,
	}
	p.WALBroadcasterConcurrencyRatio.Init(base.mgr)

	// txn
	p.TxnDefaultKeepaliveTimeout = ParamItem{
		Key:          "streaming.txn.defaultKeepaliveTimeout",
		Version:      "2.5.0",
		Doc:          "The default keepalive timeout for wal txn, 10s by default",
		DefaultValue: "10s",
		Export:       true,
	}
	p.TxnDefaultKeepaliveTimeout.Init(base.mgr)
}

type runtimeConfig struct {
	CreateTime RuntimeParamItem
	UpdateTime RuntimeParamItem
	Role       RuntimeParamItem
	NodeID     RuntimeParamItem
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
