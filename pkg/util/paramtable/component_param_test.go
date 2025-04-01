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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func shouldPanic(t *testing.T, name string, f func()) {
	defer func() { recover() }()
	f()
	t.Errorf("%s should have panicked", name)
}

func TestComponentParam(t *testing.T) {
	Init()
	params := Get()

	t.Run("test commonConfig", func(t *testing.T) {
		Params := &params.CommonCfg

		assert.NotEqual(t, Params.DefaultPartitionName.GetValue(), "")
		t.Logf("default partition name = %s", Params.DefaultPartitionName.GetValue())

		assert.NotEqual(t, Params.DefaultIndexName.GetValue(), "")
		t.Logf("default index name = %s", Params.DefaultIndexName.GetValue())

		assert.Equal(t, Params.EntityExpirationTTL.GetAsInt64(), int64(-1))
		t.Logf("default entity expiration = %d", Params.EntityExpirationTTL.GetAsInt64())

		// test the case coommo
		params.Save("common.entityExpiration", "50")
		assert.Equal(t, Params.EntityExpirationTTL.GetAsInt(), 50)

		assert.NotEqual(t, Params.SimdType.GetValue(), "")
		t.Logf("knowhere simd type = %s", Params.SimdType.GetValue())

		assert.Equal(t, Params.IndexSliceSize.GetAsInt64(), int64(DefaultIndexSliceSize))
		t.Logf("knowhere index slice size = %d", Params.IndexSliceSize.GetAsInt64())

		assert.Equal(t, Params.GracefulTime.GetAsInt64(), int64(DefaultGracefulTime))
		t.Logf("default grafeful time = %d", Params.GracefulTime.GetAsInt64())

		assert.Equal(t, Params.GracefulStopTimeout.GetAsInt64(), int64(DefaultGracefulStopTimeout))
		assert.Equal(t, params.QueryNodeCfg.GracefulStopTimeout.GetAsInt64(), Params.GracefulStopTimeout.GetAsInt64())
		assert.Equal(t, params.IndexNodeCfg.GracefulStopTimeout.GetAsInt64(), Params.GracefulStopTimeout.GetAsInt64())
		t.Logf("default grafeful stop timeout = %d", Params.GracefulStopTimeout.GetAsInt())
		params.Save(Params.GracefulStopTimeout.Key, "50")
		assert.Equal(t, Params.GracefulStopTimeout.GetAsInt64(), int64(50))

		// -- rootcoord --
		assert.Equal(t, Params.RootCoordTimeTick.GetValue(), "by-dev-rootcoord-timetick")
		t.Logf("rootcoord timetick channel = %s", Params.RootCoordTimeTick.GetValue())

		assert.Equal(t, Params.RootCoordStatistics.GetValue(), "by-dev-rootcoord-statistics")
		t.Logf("rootcoord statistics channel = %s", Params.RootCoordStatistics.GetValue())

		assert.Equal(t, Params.RootCoordDml.GetValue(), "by-dev-rootcoord-dml")
		t.Logf("rootcoord dml channel = %s", Params.RootCoordDml.GetValue())

		// -- querycoord --
		assert.Equal(t, Params.QueryCoordTimeTick.GetValue(), "by-dev-queryTimeTick")
		t.Logf("querycoord timetick channel = %s", Params.QueryCoordTimeTick.GetValue())

		// -- datacoord --
		assert.Equal(t, Params.DataCoordTimeTick.GetValue(), "by-dev-datacoord-timetick-channel")
		t.Logf("datacoord timetick channel = %s", Params.DataCoordTimeTick.GetValue())

		assert.Equal(t, Params.DataCoordSegmentInfo.GetValue(), "by-dev-segment-info-channel")
		t.Logf("datacoord segment info channel = %s", Params.DataCoordSegmentInfo.GetValue())

		assert.Equal(t, Params.DataCoordSubName.GetValue(), "by-dev-dataCoord")
		t.Logf("datacoord subname = %s", Params.DataCoordSubName.GetValue())

		assert.Equal(t, Params.DataNodeSubName.GetValue(), "by-dev-dataNode")
		t.Logf("datanode subname = %s", Params.DataNodeSubName.GetValue())

		assert.Equal(t, Params.SessionTTL.GetAsInt64(), int64(DefaultSessionTTL))
		t.Logf("default session TTL time = %d", Params.SessionTTL.GetAsInt64())
		assert.Equal(t, Params.SessionRetryTimes.GetAsInt64(), int64(DefaultSessionRetryTimes))
		t.Logf("default session retry times = %d", Params.SessionRetryTimes.GetAsInt64())

		params.Save("common.security.superUsers", "super1,super2,super3")
		assert.Equal(t, []string{"super1", "super2", "super3"}, Params.SuperUsers.GetAsStrings())

		assert.Equal(t, "Milvus", Params.DefaultRootPassword.GetValue())
		params.Save("common.security.defaultRootPassword", "defaultMilvus")
		assert.Equal(t, "defaultMilvus", Params.DefaultRootPassword.GetValue())

		params.Save("common.security.superUsers", "")
		assert.Equal(t, []string{}, Params.SuperUsers.GetAsStrings())

		assert.Equal(t, false, Params.PreCreatedTopicEnabled.GetAsBool())

		params.Save("common.preCreatedTopic.names", "topic1,topic2,topic3")
		assert.Equal(t, []string{"topic1", "topic2", "topic3"}, Params.TopicNames.GetAsStrings())

		params.Save("common.preCreatedTopic.timeticker", "timeticker")
		assert.Equal(t, []string{"timeticker"}, Params.TimeTicker.GetAsStrings())

		assert.Equal(t, 1000, params.CommonCfg.BloomFilterApplyBatchSize.GetAsInt())

		params.Save("common.gcenabled", "false")
		assert.False(t, Params.GCEnabled.GetAsBool())
		params.Save("common.gchelper.enabled", "false")
		assert.False(t, Params.GCHelperEnabled.GetAsBool())
		params.Save("common.overloadedMemoryThresholdPercentage", "40")
		assert.Equal(t, 0.4, Params.OverloadedMemoryThresholdPercentage.GetAsFloat())
		params.Save("common.gchelper.maximumGoGC", "100")
		assert.Equal(t, 100, Params.MaximumGOGCConfig.GetAsInt())
		params.Save("common.gchelper.minimumGoGC", "80")
		assert.Equal(t, 80, Params.MinimumGOGCConfig.GetAsInt())

		assert.Equal(t, 0, len(Params.ReadOnlyPrivileges.GetAsStrings()))
		assert.Equal(t, 0, len(Params.ReadWritePrivileges.GetAsStrings()))
		assert.Equal(t, 0, len(Params.AdminPrivileges.GetAsStrings()))

		assert.False(t, params.CommonCfg.LocalRPCEnabled.GetAsBool())
		params.Save("common.localRPCEnabled", "true")
		assert.True(t, params.CommonCfg.LocalRPCEnabled.GetAsBool())

		assert.Equal(t, 60*time.Second, params.CommonCfg.SyncTaskPoolReleaseTimeoutSeconds.GetAsDuration(time.Second))
		params.Save("common.sync.taskPoolReleaseTimeoutSeconds", "100")
		assert.Equal(t, 100*time.Second, params.CommonCfg.SyncTaskPoolReleaseTimeoutSeconds.GetAsDuration(time.Second))
	})

	t.Run("test rootCoordConfig", func(t *testing.T) {
		Params := &params.RootCoordCfg

		assert.NotEqual(t, Params.MaxPartitionNum.GetAsInt64(), 0)
		t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum.GetAsInt64())
		assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex.GetAsInt64(), 0)
		t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex.GetAsInt64())
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("rootCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())

		params.Save("rootCoord.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))

		assert.Equal(t, "{}", Params.DefaultDBProperties.GetValue())
		params.Save("rootCoord.defaultDBProperties", "{\"key\":\"value\"}")
		assert.Equal(t, "{\"key\":\"value\"}", Params.DefaultDBProperties.GetValue())

		SetCreateTime(time.Now())
		SetUpdateTime(time.Now())
	})

	t.Run("test proxyConfig", func(t *testing.T) {
		Params := &params.ProxyCfg

		t.Logf("TimeTickInterval: %v", &Params.TimeTickInterval)

		t.Logf("healthCheckTimeout: %v", &Params.HealthCheckTimeout)

		t.Logf("MsgStreamTimeTickBufSize: %d", Params.MsgStreamTimeTickBufSize.GetAsInt64())

		t.Logf("MaxNameLength: %d", Params.MaxNameLength.GetAsInt64())

		t.Logf("MaxFieldNum: %d", Params.MaxFieldNum.GetAsInt64())

		t.Logf("MaxVectorFieldNum: %d", Params.MaxVectorFieldNum.GetAsInt64())

		t.Logf("MaxShardNum: %d", Params.MaxShardNum.GetAsInt64())

		t.Logf("MaxDimension: %d", Params.MaxDimension.GetAsInt64())

		t.Logf("MaxTaskNum: %d", Params.MaxTaskNum.GetAsInt64())

		t.Logf("AccessLog.Enable: %t", Params.AccessLog.Enable.GetAsBool())

		t.Logf("AccessLog.MaxSize: %d", Params.AccessLog.MaxSize.GetAsInt64())

		t.Logf("AccessLog.MaxBackups: %d", Params.AccessLog.MaxBackups.GetAsInt64())

		t.Logf("AccessLog.MaxDays: %d", Params.AccessLog.RotatedTime.GetAsInt64())

		t.Logf("ShardLeaderCacheInterval: %d", Params.ShardLeaderCacheInterval.GetAsInt64())

		assert.Equal(t, Params.ReplicaSelectionPolicy.GetValue(), "look_aside")
		params.Save(Params.ReplicaSelectionPolicy.Key, "round_robin")
		assert.Equal(t, Params.ReplicaSelectionPolicy.GetValue(), "round_robin")
		params.Save(Params.ReplicaSelectionPolicy.Key, "look_aside")
		assert.Equal(t, Params.ReplicaSelectionPolicy.GetValue(), "look_aside")
		assert.Equal(t, Params.CheckQueryNodeHealthInterval.GetAsInt(), 1000)
		assert.Equal(t, Params.CostMetricsExpireTime.GetAsInt(), 1000)
		assert.Equal(t, Params.RetryTimesOnReplica.GetAsInt(), 2)
		assert.EqualValues(t, Params.HealthCheckTimeout.GetAsInt64(), 3000)

		params.Save("proxy.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))

		assert.False(t, Params.MustUsePartitionKey.GetAsBool())
		params.Save("proxy.mustUsePartitionKey", "true")
		assert.True(t, Params.MustUsePartitionKey.GetAsBool())

		assert.False(t, Params.SkipAutoIDCheck.GetAsBool())
		params.Save("proxy.skipAutoIDCheck", "true")
		assert.True(t, Params.SkipAutoIDCheck.GetAsBool())

		assert.False(t, Params.SkipPartitionKeyCheck.GetAsBool())
		params.Save("proxy.skipPartitionKeyCheck", "true")
		assert.True(t, Params.SkipPartitionKeyCheck.GetAsBool())

		assert.Equal(t, int64(10), Params.CheckWorkloadRequestNum.GetAsInt64())
		assert.Equal(t, float64(0.1), Params.WorkloadToleranceFactor.GetAsFloat())

		assert.Equal(t, int64(16), Params.DDLConcurrency.GetAsInt64())
		assert.Equal(t, int64(16), Params.DCLConcurrency.GetAsInt64())

		assert.Equal(t, 72, Params.MaxPasswordLength.GetAsInt())
		params.Save("proxy.maxPasswordLength", "100")
		assert.Equal(t, 72, Params.MaxPasswordLength.GetAsInt())
		params.Save("proxy.maxPasswordLength", "-10")
		assert.Equal(t, 72, Params.MaxPasswordLength.GetAsInt())
	})

	// t.Run("test proxyConfig panic", func(t *testing.T) {
	// 	Params := params.ProxyCfg
	//
	// 	shouldPanic(t, "proxy.timeTickInterval", func() {
	// 		params.Save("proxy.timeTickInterval", "")
	// 		Params.TimeTickInterval.GetValue()
	// 	})
	//
	// 	shouldPanic(t, "proxy.msgStream.timeTick.bufSize", func() {
	// 		params.Save("proxy.msgStream.timeTick.bufSize", "abc")
	// 		Params.MsgStreamTimeTickBufSize.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxNameLength", func() {
	// 		params.Save("proxy.maxNameLength", "abc")
	// 		Params.MaxNameLength.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxUsernameLength", func() {
	// 		params.Save("proxy.maxUsernameLength", "abc")
	// 		Params.MaxUsernameLength.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.minPasswordLength", func() {
	// 		params.Save("proxy.minPasswordLength", "abc")
	// 		Params.MinPasswordLength.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxPasswordLength", func() {
	// 		params.Save("proxy.maxPasswordLength", "abc")
	// 		Params.MaxPasswordLength.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxFieldNum", func() {
	// 		params.Save("proxy.maxFieldNum", "abc")
	// 		Params.MaxFieldNum.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxShardNum", func() {
	// 		params.Save("proxy.maxShardNum", "abc")
	// 		Params.MaxShardNum.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxDimension", func() {
	// 		params.Save("proxy.maxDimension", "-asdf")
	// 		Params.MaxDimension.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxTaskNum", func() {
	// 		params.Save("proxy.maxTaskNum", "-asdf")
	// 		Params.MaxTaskNum.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxUserNum", func() {
	// 		params.Save("proxy.maxUserNum", "abc")
	// 		Params.MaxUserNum.GetAsInt()
	// 	})
	//
	// 	shouldPanic(t, "proxy.maxRoleNum", func() {
	// 		params.Save("proxy.maxRoleNum", "abc")
	// 		Params.MaxRoleNum.GetAsInt()
	// 	})
	// })

	t.Run("test queryCoordConfig", func(t *testing.T) {
		Params := &params.QueryCoordCfg
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("queryCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())

		params.Save("queryCoord.NextTargetSurviveTime", "100")
		NextTargetSurviveTime := &Params.NextTargetSurviveTime
		assert.Equal(t, int64(100), NextTargetSurviveTime.GetAsInt64())

		params.Save("queryCoord.UpdateNextTargetInterval", "100")
		UpdateNextTargetInterval := &Params.UpdateNextTargetInterval
		assert.Equal(t, int64(100), UpdateNextTargetInterval.GetAsInt64())

		params.Save("queryCoord.checkNodeInReplicaInterval", "100")
		checkNodeInReplicaInterval := &Params.CheckNodeInReplicaInterval
		assert.Equal(t, 100, checkNodeInReplicaInterval.GetAsInt())

		params.Save("queryCoord.checkResourceGroupInterval", "10")
		checkResourceGroupInterval := &Params.CheckResourceGroupInterval
		assert.Equal(t, 10, checkResourceGroupInterval.GetAsInt())

		enableResourceGroupAutoRecover := &Params.EnableRGAutoRecover
		assert.Equal(t, true, enableResourceGroupAutoRecover.GetAsBool())
		params.Save("queryCoord.enableRGAutoRecover", "false")
		enableResourceGroupAutoRecover = &Params.EnableRGAutoRecover
		assert.Equal(t, false, enableResourceGroupAutoRecover.GetAsBool())

		checkHealthInterval := Params.CheckHealthInterval.GetAsInt()
		assert.Equal(t, 3000, checkHealthInterval)
		checkHealthRPCTimeout := Params.CheckHealthRPCTimeout.GetAsInt()
		assert.Equal(t, 2000, checkHealthRPCTimeout)

		updateInterval := Params.UpdateCollectionLoadStatusInterval.GetAsDuration(time.Minute)
		assert.Equal(t, updateInterval, time.Minute*5)

		assert.Equal(t, 0.1, Params.GlobalRowCountFactor.GetAsFloat())
		params.Save("queryCoord.globalRowCountFactor", "0.4")
		assert.Equal(t, 0.4, Params.GlobalRowCountFactor.GetAsFloat())

		assert.Equal(t, 0.05, Params.ScoreUnbalanceTolerationFactor.GetAsFloat())
		params.Save("queryCoord.scoreUnbalanceTolerationFactor", "0.4")
		assert.Equal(t, 0.4, Params.ScoreUnbalanceTolerationFactor.GetAsFloat())

		assert.Equal(t, 1.3, Params.ReverseUnbalanceTolerationFactor.GetAsFloat())
		params.Save("queryCoord.reverseUnBalanceTolerationFactor", "1.5")
		assert.Equal(t, 1.5, Params.ReverseUnbalanceTolerationFactor.GetAsFloat())

		assert.Equal(t, 1000, Params.SegmentCheckInterval.GetAsInt())
		assert.Equal(t, 1000, Params.ChannelCheckInterval.GetAsInt())
		assert.Equal(t, 300, Params.BalanceCheckInterval.GetAsInt())
		params.Save(Params.BalanceCheckInterval.Key, "3000")
		assert.Equal(t, 3000, Params.BalanceCheckInterval.GetAsInt())
		assert.Equal(t, 10000, Params.IndexCheckInterval.GetAsInt())
		assert.Equal(t, 3, Params.CollectionRecoverTimesLimit.GetAsInt())
		assert.Equal(t, true, Params.AutoBalance.GetAsBool())
		assert.Equal(t, true, Params.AutoBalanceChannel.GetAsBool())
		assert.Equal(t, 10, Params.CheckAutoBalanceConfigInterval.GetAsInt())

		params.Save("queryCoord.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))
		assert.Equal(t, true, Params.EnableStoppingBalance.GetAsBool())

		assert.Equal(t, 4, Params.ChannelExclusiveNodeFactor.GetAsInt())

		assert.Equal(t, 200, Params.CollectionObserverInterval.GetAsInt())
		params.Save("queryCoord.collectionObserverInterval", "100")
		assert.Equal(t, 100, Params.CollectionObserverInterval.GetAsInt())
		params.Reset("queryCoord.collectionObserverInterval")

		assert.Equal(t, 100, Params.CheckExecutedFlagInterval.GetAsInt())
		params.Save("queryCoord.checkExecutedFlagInterval", "200")
		assert.Equal(t, 200, Params.CheckExecutedFlagInterval.GetAsInt())
		params.Reset("queryCoord.checkExecutedFlagInterval")

		assert.Equal(t, 0.1, Params.DelegatorMemoryOverloadFactor.GetAsFloat())
		assert.Equal(t, 5, Params.CollectionBalanceSegmentBatchSize.GetAsInt())
		assert.Equal(t, 1, Params.CollectionBalanceChannelBatchSize.GetAsInt())

		assert.Equal(t, 0, Params.ClusterLevelLoadReplicaNumber.GetAsInt())
		assert.Len(t, Params.ClusterLevelLoadResourceGroups.GetAsStrings(), 0)

		assert.Equal(t, 10, Params.CollectionChannelCountFactor.GetAsInt())
		assert.Equal(t, 3000, Params.AutoBalanceInterval.GetAsInt())
	})

	t.Run("test queryNodeConfig", func(t *testing.T) {
		Params := &params.QueryNodeCfg

		interval := Params.StatsPublishInterval.GetAsInt()
		assert.Equal(t, 1000, interval)

		length := Params.FlowGraphMaxQueueLength.GetAsInt32()
		assert.Equal(t, int32(16), length)

		maxParallelism := Params.FlowGraphMaxParallelism.GetAsInt32()
		assert.Equal(t, int32(1024), maxParallelism)

		// test query side config
		chunkRows := Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(128), chunkRows)

		nlist := Params.InterimIndexNlist.GetAsInt64()
		assert.Equal(t, int64(128), nlist)

		nprobe := Params.InterimIndexNProbe.GetAsInt64()
		assert.Equal(t, int64(16), nprobe)

		assert.Equal(t, true, Params.GroupEnabled.GetAsBool())
		assert.Equal(t, int32(10240), Params.MaxReceiveChanSize.GetAsInt32())
		assert.Equal(t, int32(10240), Params.MaxUnsolvedQueueSize.GetAsInt32())
		assert.Equal(t, 10.0, Params.CPURatio.GetAsFloat())
		assert.Equal(t, uint32(hardware.GetCPUNum()), Params.KnowhereThreadPoolSize.GetAsUint32())

		// chunk cache
		assert.Equal(t, "willneed", Params.ReadAheadPolicy.GetValue())
		assert.Equal(t, "disable", Params.ChunkCacheWarmingUp.GetValue())

		// test small indexNlist/NProbe default
		params.Remove("queryNode.segcore.smallIndex.nlist")
		params.Remove("queryNode.segcore.smallIndex.nprobe")
		params.Save("queryNode.segcore.chunkRows", "8192")
		chunkRows = Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(8192), chunkRows)

		enableInterimIndex := Params.EnableInterminSegmentIndex.GetAsBool()
		assert.Equal(t, true, enableInterimIndex)

		params.Save("queryNode.segcore.interimIndex.enableIndex", "true")
		enableInterimIndex = Params.EnableInterminSegmentIndex.GetAsBool()
		assert.Equal(t, true, enableInterimIndex)

		assert.Equal(t, false, Params.KnowhereScoreConsistency.GetAsBool())
		params.Save("queryNode.segcore.knowhereScoreConsistency", "true")
		assert.Equal(t, true, Params.KnowhereScoreConsistency.GetAsBool())
		params.Save("queryNode.segcore.knowhereScoreConsistency", "false")

		nlist = Params.InterimIndexNlist.GetAsInt64()
		assert.Equal(t, int64(128), nlist)

		nprobe = Params.InterimIndexNProbe.GetAsInt64()
		assert.Equal(t, int64(16), nprobe)

		params.Remove("queryNode.segcore.growing.nlist")
		params.Remove("queryNode.segcore.growing.nprobe")
		params.Save("queryNode.segcore.chunkRows", "64")
		chunkRows = Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(128), chunkRows)

		params.Save("queryNode.gracefulStopTimeout", "100")
		gracefulStopTimeout := &Params.GracefulStopTimeout
		assert.Equal(t, int64(100), gracefulStopTimeout.GetAsInt64())

		assert.Equal(t, false, Params.EnableWorkerSQCostMetrics.GetAsBool())

		params.Save("querynode.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))

		assert.Equal(t, 2.5, Params.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat())
		params.Save("queryNode.memoryIndexLoadPredictMemoryUsageFactor", "2.0")
		assert.Equal(t, 2.0, Params.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat())

		assert.NotZero(t, Params.DiskCacheCapacityLimit.GetAsSize())
		params.Save("queryNode.diskCacheCapacityLimit", "70")
		assert.Equal(t, int64(70), Params.DiskCacheCapacityLimit.GetAsSize())
		params.Save("queryNode.diskCacheCapacityLimit", "70m")
		assert.Equal(t, int64(70*1024*1024), Params.DiskCacheCapacityLimit.GetAsSize())

		assert.False(t, Params.LazyLoadEnabled.GetAsBool())
		params.Save("queryNode.lazyload.enabled", "true")
		assert.True(t, Params.LazyLoadEnabled.GetAsBool())

		assert.Equal(t, 30*time.Second, Params.LazyLoadWaitTimeout.GetAsDuration(time.Millisecond))
		params.Save("queryNode.lazyload.waitTimeout", "100")
		assert.Equal(t, 100*time.Millisecond, Params.LazyLoadWaitTimeout.GetAsDuration(time.Millisecond))

		assert.Equal(t, 5*time.Second, Params.LazyLoadRequestResourceTimeout.GetAsDuration(time.Millisecond))
		params.Save("queryNode.lazyload.requestResourceTimeout", "100")
		assert.Equal(t, 100*time.Millisecond, Params.LazyLoadRequestResourceTimeout.GetAsDuration(time.Millisecond))

		assert.Equal(t, 2*time.Second, Params.LazyLoadRequestResourceRetryInterval.GetAsDuration(time.Millisecond))
		params.Save("queryNode.lazyload.requestResourceRetryInterval", "3000")
		assert.Equal(t, 3*time.Second, Params.LazyLoadRequestResourceRetryInterval.GetAsDuration(time.Millisecond))

		assert.Equal(t, 4, Params.BloomFilterApplyParallelFactor.GetAsInt())
		assert.Equal(t, true, Params.SkipGrowingSegmentBF.GetAsBool())

		assert.Equal(t, "/var/lib/milvus/data/mmap", Params.MmapDirPath.GetValue())

		assert.Equal(t, true, Params.MmapChunkCache.GetAsBool())
		assert.Equal(t, 60*time.Second, Params.DiskSizeFetchInterval.GetAsDuration(time.Second))
	})

	t.Run("test dataCoordConfig", func(t *testing.T) {
		Params := &params.DataCoordCfg
		assert.Equal(t, 24*60*60*time.Second, Params.SegmentMaxLifetime.GetAsDuration(time.Second))
		assert.True(t, Params.EnableGarbageCollection.GetAsBool())
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("dataCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())
		assert.Equal(t, int64(4096), Params.GrowingSegmentsMemSizeInMB.GetAsInt64())

		assert.Equal(t, true, Params.AutoBalance.GetAsBool())
		assert.Equal(t, 10, Params.CheckAutoBalanceConfigInterval.GetAsInt())
		assert.Equal(t, false, Params.AutoUpgradeSegmentIndex.GetAsBool())
		assert.Equal(t, 2, Params.FilesPerPreImportTask.GetAsInt())
		assert.Equal(t, 10800*time.Second, Params.ImportTaskRetention.GetAsDuration(time.Second))
		assert.Equal(t, 6144, Params.MaxSizeInMBPerImportTask.GetAsInt())
		assert.Equal(t, 2*time.Second, Params.ImportScheduleInterval.GetAsDuration(time.Second))
		assert.Equal(t, 2*time.Second, Params.ImportCheckIntervalHigh.GetAsDuration(time.Second))
		assert.Equal(t, 120*time.Second, Params.ImportCheckIntervalLow.GetAsDuration(time.Second))
		assert.Equal(t, 1024, Params.MaxFilesPerImportReq.GetAsInt())
		assert.Equal(t, 1024, Params.MaxImportJobNum.GetAsInt())
		assert.Equal(t, true, Params.WaitForIndex.GetAsBool())

		params.Save("datacoord.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))

		params.Save("dataCoord.compaction.gcInterval", "100")
		assert.Equal(t, float64(100), Params.CompactionGCIntervalInSeconds.GetAsDuration(time.Second).Seconds())
		params.Save("dataCoord.compaction.dropTolerance", "100")
		assert.Equal(t, float64(100), Params.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds())
		assert.Equal(t, int64(100), Params.CompactionPreAllocateIDExpansionFactor.GetAsInt64())

		params.Save("dataCoord.compaction.clustering.enable", "true")
		assert.Equal(t, true, Params.ClusteringCompactionEnable.GetAsBool())
		params.Save("dataCoord.compaction.clustering.newDataSizeThreshold", "10")
		assert.Equal(t, int64(10), Params.ClusteringCompactionNewDataSizeThreshold.GetAsSize())
		params.Save("dataCoord.compaction.clustering.newDataSizeThreshold", "10k")
		assert.Equal(t, int64(10*1024), Params.ClusteringCompactionNewDataSizeThreshold.GetAsSize())
		params.Save("dataCoord.compaction.clustering.newDataSizeThreshold", "10m")
		assert.Equal(t, int64(10*1024*1024), Params.ClusteringCompactionNewDataSizeThreshold.GetAsSize())
		params.Save("dataCoord.compaction.clustering.newDataSizeThreshold", "10g")
		assert.Equal(t, int64(10*1024*1024*1024), Params.ClusteringCompactionNewDataSizeThreshold.GetAsSize())
		params.Save("dataCoord.compaction.clustering.maxSegmentSizeRatio", "1.2")
		assert.Equal(t, 1.2, Params.ClusteringCompactionMaxSegmentSizeRatio.GetAsFloat())
		params.Save("dataCoord.compaction.clustering.preferSegmentSizeRatio", "0.5")
		assert.Equal(t, 0.5, Params.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat())
		params.Save("dataCoord.slot.clusteringCompactionUsage", "10")
		assert.Equal(t, 10, Params.ClusteringCompactionSlotUsage.GetAsInt())
		params.Save("dataCoord.slot.mixCompactionUsage", "5")
		assert.Equal(t, 5, Params.MixCompactionSlotUsage.GetAsInt())
		params.Save("dataCoord.slot.l0DeleteCompactionUsage", "4")
		assert.Equal(t, 4, Params.L0DeleteCompactionSlotUsage.GetAsInt())
		params.Save("datacoord.scheduler.taskSlowThreshold", "1000")
		assert.Equal(t, 1000*time.Second, Params.TaskSlowThreshold.GetAsDuration(time.Second))

		params.Save("datacoord.statsTask.enable", "true")
		assert.True(t, Params.EnableStatsTask.GetAsBool())
		params.Save("datacoord.taskCheckInterval", "500")
		assert.Equal(t, 500*time.Second, Params.TaskCheckInterval.GetAsDuration(time.Second))
		params.Save("datacoord.statsTaskTriggerCount", "3")
		assert.Equal(t, 3, Params.StatsTaskTriggerCount.GetAsInt())
	})

	t.Run("test dataNodeConfig", func(t *testing.T) {
		Params := &params.DataNodeCfg

		SetNodeID(2)

		id := GetNodeID()
		t.Logf("NodeID: %d", id)

		length := Params.FlowGraphMaxQueueLength.GetAsInt()
		t.Logf("flowGraphMaxQueueLength: %d", length)

		maxParallelism := Params.FlowGraphMaxParallelism.GetAsInt()
		t.Logf("flowGraphMaxParallelism: %d", maxParallelism)

		flowGraphSkipModeEnable := Params.FlowGraphSkipModeEnable.GetAsBool()
		t.Logf("flowGraphSkipModeEnable: %t", flowGraphSkipModeEnable)

		flowGraphSkipModeSkipNum := Params.FlowGraphSkipModeSkipNum.GetAsInt()
		t.Logf("flowGraphSkipModeSkipNum: %d", flowGraphSkipModeSkipNum)

		flowGraphSkipModeColdTime := Params.FlowGraphSkipModeColdTime.GetAsInt()
		t.Logf("flowGraphSkipModeColdTime: %d", flowGraphSkipModeColdTime)

		maxParallelSyncTaskNum := Params.MaxParallelSyncTaskNum.GetAsInt()
		t.Logf("maxParallelSyncTaskNum: %d", maxParallelSyncTaskNum)

		size := Params.FlushInsertBufferSize.GetAsInt()
		t.Logf("FlushInsertBufferSize: %d", size)

		period := &Params.SyncPeriod
		t.Logf("SyncPeriod: %v", period)
		assert.Equal(t, 10*time.Minute, Params.SyncPeriod.GetAsDuration(time.Second))

		channelWorkPoolSize := Params.ChannelWorkPoolSize.GetAsInt()
		t.Logf("channelWorkPoolSize: %d", channelWorkPoolSize)
		assert.Equal(t, -1, Params.ChannelWorkPoolSize.GetAsInt())

		updateChannelCheckpointMaxParallel := Params.UpdateChannelCheckpointMaxParallel.GetAsInt()
		t.Logf("updateChannelCheckpointMaxParallel: %d", updateChannelCheckpointMaxParallel)
		assert.Equal(t, 10, Params.UpdateChannelCheckpointMaxParallel.GetAsInt())
		assert.Equal(t, 128, Params.MaxChannelCheckpointsPerRPC.GetAsInt())
		assert.Equal(t, 10*time.Second, Params.ChannelCheckpointUpdateTickInSeconds.GetAsDuration(time.Second))

		maxConcurrentImportTaskNum := Params.MaxConcurrentImportTaskNum.GetAsInt()
		t.Logf("maxConcurrentImportTaskNum: %d", maxConcurrentImportTaskNum)
		assert.Equal(t, 16, maxConcurrentImportTaskNum)
		assert.Equal(t, int64(16), Params.MaxImportFileSizeInGB.GetAsInt64())
		assert.Equal(t, 16, Params.ReadBufferSizeInMB.GetAsInt())
		assert.Equal(t, 16, Params.MaxTaskSlotNum.GetAsInt())
		params.Save("datanode.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))
		assert.Equal(t, 16, Params.SlotCap.GetAsInt())

		// clustering compaction
		params.Save("datanode.clusteringCompaction.memoryBufferRatio", "0.1")
		assert.Equal(t, 0.1, Params.ClusteringCompactionMemoryBufferRatio.GetAsFloat())
		params.Save("datanode.clusteringCompaction.workPoolSize", "2")
		assert.Equal(t, int64(2), Params.ClusteringCompactionWorkerPoolSize.GetAsInt64())

		assert.Equal(t, 4, Params.BloomFilterApplyParallelFactor.GetAsInt())
	})

	t.Run("test indexNodeConfig", func(t *testing.T) {
		Params := &params.IndexNodeCfg
		params.Save(Params.GracefulStopTimeout.Key, "50")
		assert.Equal(t, Params.GracefulStopTimeout.GetAsInt64(), int64(50))

		params.Save("indexnode.gracefulStopTimeout", "100")
		assert.Equal(t, 100*time.Second, Params.GracefulStopTimeout.GetAsDuration(time.Second))
	})

	t.Run("test streamingConfig", func(t *testing.T) {
		assert.Equal(t, 1*time.Minute, params.StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse())
		assert.Equal(t, 50*time.Millisecond, params.StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse())
		assert.Equal(t, 2.0, params.StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat())
		assert.Equal(t, 1.0, params.StreamingCfg.WALBroadcasterConcurrencyRatio.GetAsFloat())
		assert.Equal(t, 10*time.Second, params.StreamingCfg.TxnDefaultKeepaliveTimeout.GetAsDurationByParse())
		params.Save(params.StreamingCfg.WALBalancerTriggerInterval.Key, "50s")
		params.Save(params.StreamingCfg.WALBalancerBackoffInitialInterval.Key, "50s")
		params.Save(params.StreamingCfg.WALBalancerBackoffMultiplier.Key, "3.5")
		params.Save(params.StreamingCfg.WALBroadcasterConcurrencyRatio.Key, "1.5")
		params.Save(params.StreamingCfg.TxnDefaultKeepaliveTimeout.Key, "3500ms")
		assert.Equal(t, 50*time.Second, params.StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse())
		assert.Equal(t, 50*time.Second, params.StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse())
		assert.Equal(t, 3.5, params.StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat())
		assert.Equal(t, 1.5, params.StreamingCfg.WALBroadcasterConcurrencyRatio.GetAsFloat())
		assert.Equal(t, 3500*time.Millisecond, params.StreamingCfg.TxnDefaultKeepaliveTimeout.GetAsDurationByParse())
	})

	t.Run("channel config priority", func(t *testing.T) {
		Params := &params.CommonCfg
		params.Save(Params.RootCoordDml.Key, "dml1")
		params.Save(Params.RootCoordDml.FallbackKeys[0], "dml2")

		assert.Equal(t, "by-dev-dml1", Params.RootCoordDml.GetValue())
	})

	t.Run("clustering compaction config", func(t *testing.T) {
		Params := &params.CommonCfg
		params.Save("common.usePartitionKeyAsClusteringKey", "true")
		assert.Equal(t, true, Params.UsePartitionKeyAsClusteringKey.GetAsBool())
		params.Save("common.useVectorAsClusteringKey", "true")
		assert.Equal(t, true, Params.UseVectorAsClusteringKey.GetAsBool())
		params.Save("common.enableVectorClusteringKey", "true")
		assert.Equal(t, true, Params.EnableVectorClusteringKey.GetAsBool())
	})
}

func TestForbiddenItem(t *testing.T) {
	Init()
	params := Get()

	params.baseTable.mgr.OnEvent(&config.Event{
		Key:   params.CommonCfg.ClusterPrefix.Key,
		Value: "new-cluster",
	})
	assert.Equal(t, "by-dev", params.CommonCfg.ClusterPrefix.GetValue())
}

func TestCachedParam(t *testing.T) {
	Init()
	params := Get()

	assert.True(t, params.IndexNodeCfg.EnableDisk.GetAsBool())
	assert.True(t, params.IndexNodeCfg.EnableDisk.GetAsBool())

	assert.Equal(t, 256*1024*1024, params.QueryCoordGrpcServerCfg.ServerMaxRecvSize.GetAsInt())
	assert.Equal(t, 256*1024*1024, params.QueryCoordGrpcServerCfg.ServerMaxRecvSize.GetAsInt())

	assert.Equal(t, int32(16), params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	assert.Equal(t, int32(16), params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())

	assert.Equal(t, uint(100000), params.CommonCfg.BloomFilterSize.GetAsUint())
	assert.Equal(t, uint(100000), params.CommonCfg.BloomFilterSize.GetAsUint())
	assert.Equal(t, "BlockedBloomFilter", params.CommonCfg.BloomFilterType.GetValue())

	assert.Equal(t, uint64(8388608), params.ServiceParam.MQCfg.PursuitBufferSize.GetAsUint64())
	assert.Equal(t, uint64(8388608), params.ServiceParam.MQCfg.PursuitBufferSize.GetAsUint64())

	assert.Equal(t, 60, params.ServiceParam.MQCfg.PursuitBufferTime.GetAsInt())

	assert.Equal(t, int64(1024), params.DataCoordCfg.SegmentMaxSize.GetAsInt64())
	assert.Equal(t, int64(1024), params.DataCoordCfg.SegmentMaxSize.GetAsInt64())

	assert.Equal(t, 0.85, params.QuotaConfig.DataNodeMemoryLowWaterLevel.GetAsFloat())
	assert.Equal(t, 0.85, params.QuotaConfig.DataNodeMemoryLowWaterLevel.GetAsFloat())

	assert.Equal(t, 1*time.Hour, params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	assert.Equal(t, 1*time.Hour, params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))

	params.Save(params.QuotaConfig.DiskQuota.Key, "192")
	assert.Equal(t, float64(192*1024*1024), params.QuotaConfig.DiskQuota.GetAsFloat())
	assert.Equal(t, float64(192*1024*1024), params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat())
	params.Save(params.QuotaConfig.DiskQuota.Key, "256")
	assert.Equal(t, float64(256*1024*1024), params.QuotaConfig.DiskQuota.GetAsFloat())
	assert.Equal(t, float64(256*1024*1024), params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat())
	params.Save(params.QuotaConfig.DiskQuota.Key, "192")
}

func TestFallbackParam(t *testing.T) {
	Init()
	params := Get()
	params.Save("common.chanNamePrefix.cluster", "foo")

	assert.Equal(t, "foo", params.CommonCfg.ClusterPrefix.GetValue())
}
