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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func shouldPanic(t *testing.T, name string, f func()) {
	defer func() { recover() }()
	f()
	t.Errorf("%s should have panicked", name)
}

func TestComponentParam(t *testing.T) {
	var CParams ComponentParam
	CParams.Init()

	t.Run("test kafkaConfig", func(t *testing.T) {

		params := CParams.ServiceParam.KafkaCfg
		producerConfig := params.ProducerExtraConfig
		assert.Equal(t, "dc", producerConfig["client.id"])

		consumerConfig := params.ConsumerExtraConfig
		assert.Equal(t, "dc1", consumerConfig["client.id"])
	})

	t.Run("test commonConfig", func(t *testing.T) {
		Params := CParams.CommonCfg

		assert.NotEqual(t, Params.DefaultPartitionName, "")
		t.Logf("default partition name = %s", Params.DefaultPartitionName)

		assert.NotEqual(t, Params.DefaultIndexName, "")
		t.Logf("default index name = %s", Params.DefaultIndexName)

		assert.Equal(t, Params.RetentionDuration, int64(DefaultRetentionDuration))
		t.Logf("default retention duration = %d", Params.RetentionDuration)

		assert.Equal(t, int64(Params.EntityExpirationTTL), int64(-1))
		t.Logf("default entity expiration = %d", Params.EntityExpirationTTL)

		// test the case coommo
		Params.Base.Save("common.entityExpiration", "50")
		Params.initEntityExpiration()
		assert.Equal(t, int64(Params.EntityExpirationTTL.Seconds()), int64(50))

		assert.NotEqual(t, Params.SimdType, "")
		t.Logf("knowhere simd type = %s", Params.SimdType)

		assert.Equal(t, Params.IndexSliceSize, int64(DefaultIndexSliceSize))
		t.Logf("knowhere index slice size = %d", Params.IndexSliceSize)

		assert.Equal(t, Params.GracefulTime, int64(DefaultGracefulTime))
		t.Logf("default grafeful time = %d", Params.GracefulTime)

		assert.Equal(t, Params.GracefulStopTimeout, int64(DefaultGracefulStopTimeout))
		assert.Equal(t, CParams.QueryNodeCfg.GracefulStopTimeout, Params.GracefulStopTimeout)
		assert.Equal(t, CParams.IndexNodeCfg.GracefulStopTimeout, Params.GracefulStopTimeout)
		t.Logf("default grafeful stop timeout = %d", Params.GracefulStopTimeout)
		Params.Base.Save("common.gracefulStopTimeout", "50")
		Params.Base.Remove("queryNode.gracefulStopTimeout")
		Params.Base.Remove("indexNode.gracefulStopTimeout")
		Params.initGracefulStopTimeout()
		assert.Equal(t, Params.GracefulStopTimeout, int64(50))
		CParams.QueryNodeCfg.initGracefulStopTimeout()
		assert.Equal(t, CParams.QueryNodeCfg.GracefulStopTimeout, int64(50))
		CParams.IndexNodeCfg.initGracefulStopTimeout()
		assert.Equal(t, CParams.IndexNodeCfg.GracefulStopTimeout, int64(50))

		// -- proxy --
		assert.Equal(t, Params.ProxySubName, "by-dev-proxy")
		t.Logf("ProxySubName: %s", Params.ProxySubName)

		// -- rootcoord --
		assert.Equal(t, Params.RootCoordTimeTick, "by-dev-rootcoord-timetick")
		t.Logf("rootcoord timetick channel = %s", Params.RootCoordTimeTick)

		assert.Equal(t, Params.RootCoordStatistics, "by-dev-rootcoord-statistics")
		t.Logf("rootcoord statistics channel = %s", Params.RootCoordStatistics)

		assert.Equal(t, Params.RootCoordDml, "by-dev-rootcoord-dml")
		t.Logf("rootcoord dml channel = %s", Params.RootCoordDml)

		assert.Equal(t, Params.RootCoordDelta, "by-dev-rootcoord-delta")
		t.Logf("rootcoord delta channel = %s", Params.RootCoordDelta)

		assert.Equal(t, Params.RootCoordSubName, "by-dev-rootCoord")
		t.Logf("rootcoord subname = %s", Params.RootCoordSubName)

		// -- querycoord --
		assert.Equal(t, Params.QueryCoordSearch, "by-dev-search")
		t.Logf("querycoord search channel = %s", Params.QueryCoordSearch)

		assert.Equal(t, Params.QueryCoordSearchResult, "by-dev-searchResult")
		t.Logf("querycoord search result channel = %s", Params.QueryCoordSearchResult)

		assert.Equal(t, Params.QueryCoordTimeTick, "by-dev-queryTimeTick")
		t.Logf("querycoord timetick channel = %s", Params.QueryCoordTimeTick)

		// -- querynode --
		assert.Equal(t, Params.QueryNodeSubName, "by-dev-queryNode")
		t.Logf("querynode subname = %s", Params.QueryNodeSubName)

		// -- datacoord --
		assert.Equal(t, Params.DataCoordTimeTick, "by-dev-datacoord-timetick-channel")
		t.Logf("datacoord timetick channel = %s", Params.DataCoordTimeTick)

		assert.Equal(t, Params.DataCoordSegmentInfo, "by-dev-segment-info-channel")
		t.Logf("datacoord segment info channel = %s", Params.DataCoordSegmentInfo)

		assert.Equal(t, Params.DataCoordSubName, "by-dev-dataCoord")
		t.Logf("datacoord subname = %s", Params.DataCoordSubName)

		assert.Equal(t, Params.DataNodeSubName, "by-dev-dataNode")
		t.Logf("datanode subname = %s", Params.DataNodeSubName)

		assert.Equal(t, Params.SessionTTL, int64(DefaultSessionTTL))
		t.Logf("default session TTL time = %d", Params.SessionTTL)
		assert.Equal(t, Params.SessionRetryTimes, int64(DefaultSessionRetryTimes))
		t.Logf("default session retry times = %d", Params.SessionRetryTimes)
		assert.Equal(t, Params.RetryRegisterWhenKeepAliveCancel, DefaultRetryRegisterWhenKeepAliveCancel)
		t.Logf("default session retry register when keep alive cancel enabled = %t", Params.RetryRegisterWhenKeepAliveCancel)

		assert.Equal(t, Params.GrpcRetryTimes, uint(DefaultGrpcRetryTimes))
		Params.Base.Save("grpc.server.retryTimes", "6")
		Params.initGrpcRetryTimes()
		assert.Equal(t, Params.GrpcRetryTimes, uint(6))

		assert.Equal(t, Params.ImportMaxFileSize, int64(DefaultImportMaxFileSize))
		Params.Base.Save("common.ImportMaxFileSize", "600")
		Params.initImportMaxFileSize()
		assert.Equal(t, int64(600), Params.ImportMaxFileSize)

		Params.Base.Save("common.security.superUsers", "super1,super2,super3")
		Params.initSuperUsers()
		assert.Equal(t, []string{"super1", "super2", "super3"}, Params.SuperUsers)
		Params.Base.Save("common.security.superUsers", "super")
		Params.initSuperUsers()
		assert.Equal(t, []string{"super"}, Params.SuperUsers)
		Params.Base.Remove("common.security.superUsers")
		Params.initSuperUsers()
		assert.Equal(t, []string{}, Params.SuperUsers)
		assert.Equal(t, 0, len(Params.SuperUsers))
	})

	t.Run("test rootCoordConfig", func(t *testing.T) {
		Params := CParams.RootCoordCfg

		assert.NotEqual(t, Params.MaxPartitionNum, 0)
		t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum)
		assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex, 0)
		t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex)
		assert.NotEqual(t, Params.ImportTaskExpiration, 0)
		t.Logf("master ImportTaskRetention = %f", Params.ImportTaskRetention)
		assert.Equal(t, Params.EnableActiveStandby, false)
		t.Logf("rootCoord EnableActiveStandby = %t", Params.EnableActiveStandby)

		Params.CreatedTime = time.Now()
		Params.UpdatedTime = time.Now()
		t.Logf("created time: %v", Params.CreatedTime)
		t.Logf("updated time: %v", Params.UpdatedTime)
	})

	t.Run("test proxyConfig", func(t *testing.T) {
		Params := CParams.ProxyCfg

		t.Logf("TimeTickInterval: %v", Params.TimeTickInterval)

		t.Logf("MsgStreamTimeTickBufSize: %d", Params.MsgStreamTimeTickBufSize)

		t.Logf("MaxNameLength: %d", Params.MaxNameLength)

		t.Logf("MaxFieldNum: %d", Params.MaxFieldNum)

		t.Logf("MaxShardNum: %d", Params.MaxShardNum)

		t.Logf("MaxDimension: %d", Params.MaxDimension)

		t.Logf("MaxTaskNum: %d", Params.MaxTaskNum)

		t.Logf("ShardLeaderCacheInterval: %d", Params.ShardLeaderCacheInterval.Load())
	})

	t.Run("test proxyConfig panic", func(t *testing.T) {
		Params := CParams.ProxyCfg

		shouldPanic(t, "proxy.timeTickInterval", func() {
			Params.Base.Save("proxy.timeTickInterval", "")
			Params.initTimeTickInterval()
		})

		shouldPanic(t, "proxy.msgStream.timeTick.bufSize", func() {
			Params.Base.Save("proxy.msgStream.timeTick.bufSize", "abc")
			Params.initMsgStreamTimeTickBufSize()
		})

		shouldPanic(t, "proxy.maxNameLength", func() {
			Params.Base.Save("proxy.maxNameLength", "abc")
			Params.initMaxNameLength()
		})

		shouldPanic(t, "proxy.maxUsernameLength", func() {
			Params.Base.Save("proxy.maxUsernameLength", "abc")
			Params.initMaxUsernameLength()
		})

		shouldPanic(t, "proxy.minPasswordLength", func() {
			Params.Base.Save("proxy.minPasswordLength", "abc")
			Params.initMinPasswordLength()
		})

		shouldPanic(t, "proxy.maxPasswordLength", func() {
			Params.Base.Save("proxy.maxPasswordLength", "abc")
			Params.initMaxPasswordLength()
		})

		shouldPanic(t, "proxy.maxFieldNum", func() {
			Params.Base.Save("proxy.maxFieldNum", "abc")
			Params.initMaxFieldNum()
		})

		shouldPanic(t, "proxy.maxShardNum", func() {
			Params.Base.Save("proxy.maxShardNum", "abc")
			Params.initMaxShardNum()
		})

		shouldPanic(t, "proxy.maxDimension", func() {
			Params.Base.Save("proxy.maxDimension", "-asdf")
			Params.initMaxDimension()
		})

		shouldPanic(t, "proxy.maxTaskNum", func() {
			Params.Base.Save("proxy.maxTaskNum", "-asdf")
			Params.initMaxTaskNum()
		})

		shouldPanic(t, "proxy.maxUserNum", func() {
			Params.Base.Save("proxy.maxUserNum", "abc")
			Params.initMaxUserNum()
		})

		shouldPanic(t, "proxy.maxRoleNum", func() {
			Params.Base.Save("proxy.maxRoleNum", "abc")
			Params.initMaxRoleNum()
		})
	})

	t.Run("test queryCoordConfig", func(t *testing.T) {
		Params := CParams.QueryCoordCfg
		assert.Equal(t, Params.EnableActiveStandby, false)
		t.Logf("queryCoord EnableActiveStandby = %t", Params.EnableActiveStandby)

		assert.Equal(t, float64(300), Params.NextTargetSurviveTime.Seconds())
		assert.Equal(t, float64(30), Params.UpdateNextTargetInterval.Seconds())
		assert.Equal(t, float64(60), Params.CheckNodeInReplicaInterval.Seconds())
		assert.Equal(t, float64(10), Params.CheckResourceGroupInterval.Seconds())
		assert.Equal(t, true, Params.EnableRGAutoRecover)

		//test balance factor legal range
		Params.Base.Save("queryCoord.globalRowCountFactor", "50")
		Params.initGlobalRowCountFactor()
		assert.Equal(t, float64(1), Params.GlobalRowCountFactor)

		Params.Base.Save("queryCoord.globalRowCountFactor", "-1")
		Params.initGlobalRowCountFactor()
		assert.Equal(t, 0.1, Params.GlobalRowCountFactor)

		Params.Base.Save("queryCoord.globalRowCountFactor", "0.4")
		Params.initGlobalRowCountFactor()
		assert.Equal(t, 0.4, Params.GlobalRowCountFactor)

		//test unbalance toleration factor legal range
		Params.Base.Save("queryCoord.scoreUnbalanceTolerationFactor", "-1")
		Params.initScoreUnbalanceTolerationFactor()
		assert.Equal(t, 0.05, Params.ScoreUnbalanceTolerationFactor)

		Params.Base.Save("queryCoord.scoreUnbalanceTolerationFactor", "0.3")
		Params.initScoreUnbalanceTolerationFactor()
		assert.Equal(t, 0.3, Params.ScoreUnbalanceTolerationFactor)

		//test reverse unbalance toleration factor legal range
		Params.Base.Save("queryCoord.reverseUnBalanceTolerationFactor", "-1")
		Params.initReverseUnbalanceTolerationFactor()
		assert.Equal(t, 1.1, Params.ReverseUnbalanceTolerationFactor)

		Params.Base.Save("queryCoord.reverseUnBalanceTolerationFactor", "5.3")
		Params.initReverseUnbalanceTolerationFactor()
		assert.Equal(t, 2.0, Params.ReverseUnbalanceTolerationFactor)

		//test check health interval
		Params.Base.Save("queryCoord.CheckHealthInterval", "10")
		Params.initCheckHealthInterval()
		assert.Equal(t, time.Duration(10)*time.Millisecond, Params.CheckHealthInterval)

		Params.Base.Save("queryCoord.CheckHealthRPCTimeout", "10")
		Params.initCheckHealthRPCTimeout()
		assert.Equal(t, time.Duration(10)*time.Millisecond, Params.CheckHealthRPCTimeout)
	})

	t.Run("test queryNodeConfig", func(t *testing.T) {
		Params := CParams.QueryNodeCfg

		interval := Params.StatsPublishInterval
		assert.Equal(t, 1000, interval)

		length := Params.FlowGraphMaxQueueLength
		assert.Equal(t, int32(1024), length)

		maxParallelism := Params.FlowGraphMaxParallelism
		assert.Equal(t, int32(1024), maxParallelism)

		// test query side config
		chunkRows := Params.ChunkRows
		assert.Equal(t, int64(1024), chunkRows)

		nlist := Params.SmallIndexNlist
		assert.Equal(t, int64(128), nlist)

		nprobe := Params.SmallIndexNProbe
		assert.Equal(t, int64(16), nprobe)

		assert.Equal(t, true, Params.GroupEnabled)
		assert.Equal(t, int32(10240), Params.MaxReceiveChanSize)
		assert.Equal(t, int32(10240), Params.MaxUnsolvedQueueSize)
		assert.Equal(t, int32(runtime.GOMAXPROCS(0)*2), Params.MaxReadConcurrency)
		assert.Equal(t, 10.0, Params.TopKMergeRatio)
		assert.Equal(t, 10.0, Params.CPURatio)

		// test small indexNlist/NProbe default
		Params.Base.Remove("queryNode.segcore.smallIndex.nlist")
		Params.Base.Remove("queryNode.segcore.smallIndex.nprobe")
		Params.Base.Save("queryNode.segcore.chunkRows", "8192")
		Params.initSmallIndexParams()
		chunkRows = Params.ChunkRows
		assert.Equal(t, int64(8192), chunkRows)

		nlist = Params.SmallIndexNlist
		assert.Equal(t, int64(128), nlist)

		nprobe = Params.SmallIndexNProbe
		assert.Equal(t, int64(8), nprobe)

		Params.Base.Remove("queryNode.segcore.smallIndex.nlist")
		Params.Base.Remove("queryNode.segcore.smallIndex.nprobe")
		Params.Base.Save("queryNode.segcore.chunkRows", "64")
		Params.initSmallIndexParams()
		chunkRows = Params.ChunkRows
		assert.Equal(t, int64(1024), chunkRows)

		nlist = Params.SmallIndexNlist
		assert.Equal(t, int64(64), nlist)

		nprobe = Params.SmallIndexNProbe
		assert.Equal(t, int64(4), nprobe)

		Params.Base.Save("queryNode.gracefulStopTimeout", "100")
		Params.initGracefulStopTimeout()
		gracefulStopTimeout := Params.GracefulStopTimeout
		assert.Equal(t, int64(100), gracefulStopTimeout)

		Params.Base.Save("queryNode.segcore.knowhereThreadPoolNumRatio", "16")
		Params.Base.Save("queryNode.enableDisk", "true")
		Params.initEnableDisk()
		Params.initKnowhereThreadPoolSize()
		threadPoolSize := Params.KnowhereThreadPoolSize
		expectedThreadPoolSize := uint32(16 * runtime.GOMAXPROCS(0))
		assert.Equal(t, expectedThreadPoolSize, threadPoolSize)

		Params.Base.Save("queryNode.enableDisk", "false")
		Params.initEnableDisk()
		Params.initKnowhereThreadPoolSize()
		threadPoolSize = Params.KnowhereThreadPoolSize
		expectedThreadPoolSize = uint32(runtime.GOMAXPROCS(0))
		assert.Equal(t, expectedThreadPoolSize, threadPoolSize)
	})

	t.Run("test dataCoordConfig", func(t *testing.T) {
		Params := CParams.DataCoordCfg
		assert.Equal(t, 24*60*60*time.Second, Params.SegmentMaxLifetime)
		assert.True(t, Params.EnableGarbageCollection)
		assert.Equal(t, Params.EnableActiveStandby, false)
		assert.True(t, Params.SegmentCompactableProportion >= Params.SegmentSmallProportion)
		t.Logf("dataCoord EnableActiveStandby = %t", Params.EnableActiveStandby)
	})

	t.Run("test dataNodeConfig", func(t *testing.T) {
		Params := CParams.DataNodeCfg

		Params.SetNodeID(2)

		id := Params.GetNodeID()
		t.Logf("NodeID: %d", id)

		alias := Params.Alias
		t.Logf("Alias: %s", alias)

		length := Params.FlowGraphMaxQueueLength
		t.Logf("flowGraphMaxQueueLength: %d", length)

		maxParallelism := Params.FlowGraphMaxParallelism
		t.Logf("flowGraphMaxParallelism: %d", maxParallelism)

		size := Params.FlushInsertBufferSize
		t.Logf("FlushInsertBufferSize: %d", size)

		period := Params.SyncPeriod
		t.Logf("SyncPeriod: %v", period)
		assert.Equal(t, 10*time.Minute, Params.SyncPeriod)

		Params.CreatedTime = time.Now()
		t.Logf("CreatedTime: %v", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		t.Logf("UpdatedTime: %v", Params.UpdatedTime)
	})

	t.Run("test indexCoordConfig", func(t *testing.T) {
		Params := CParams.IndexCoordCfg

		t.Logf("Address: %v", Params.Address)

		t.Logf("Port: %v", Params.Port)

		Params.CreatedTime = time.Now()
		t.Logf("CreatedTime: %v", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		t.Logf("UpdatedTime: %v", Params.UpdatedTime)

		assert.False(t, Params.BindIndexNodeMode)
		assert.Equal(t, "localhost:22930", Params.IndexNodeAddress)
		assert.False(t, Params.WithCredential)
		assert.Equal(t, int64(0), Params.IndexNodeID)

		assert.Equal(t, Params.EnableActiveStandby, false)
		t.Logf("indexCoord EnableActiveStandby = %t", Params.EnableActiveStandby)
	})

	t.Run("test indexNodeConfig", func(t *testing.T) {
		Params := CParams.IndexNodeCfg

		t.Logf("IP: %v", Params.IP)

		t.Logf("Address: %v", Params.Address)

		t.Logf("Port: %v", Params.Port)

		t.Logf("NodeID: %v", Params.GetNodeID())

		t.Logf("Alias: %v", Params.Alias)

		Params.CreatedTime = time.Now()
		t.Logf("CreatedTime: %v", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		t.Logf("UpdatedTime: %v", Params.UpdatedTime)

		Params.Base.Save("indexNode.gracefulStopTimeout", "100")
		Params.initGracefulStopTimeout()
		assert.Equal(t, Params.GracefulStopTimeout, int64(100))
	})
}
