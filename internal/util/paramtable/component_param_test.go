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

	"github.com/milvus-io/milvus/internal/config"
	"github.com/stretchr/testify/assert"
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
		Params := params.CommonCfg

		assert.NotEqual(t, Params.DefaultPartitionName.GetValue(), "")
		t.Logf("default partition name = %s", Params.DefaultPartitionName.GetValue())

		assert.NotEqual(t, Params.DefaultIndexName, "")
		t.Logf("default index name = %s", Params.DefaultIndexName.GetValue())

		assert.Equal(t, Params.RetentionDuration.GetAsInt64(), int64(DefaultRetentionDuration))
		t.Logf("default retention duration = %d", Params.RetentionDuration.GetAsInt64())

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

		// -- proxy --
		assert.Equal(t, Params.ProxySubName.GetValue(), "by-dev-proxy")
		t.Logf("ProxySubName: %s", Params.ProxySubName.GetValue())

		// -- rootcoord --
		assert.Equal(t, Params.RootCoordTimeTick.GetValue(), "by-dev-rootcoord-timetick")
		t.Logf("rootcoord timetick channel = %s", Params.RootCoordTimeTick.GetValue())

		assert.Equal(t, Params.RootCoordStatistics.GetValue(), "by-dev-rootcoord-statistics")
		t.Logf("rootcoord statistics channel = %s", Params.RootCoordStatistics.GetValue())

		assert.Equal(t, Params.RootCoordDml.GetValue(), "by-dev-rootcoord-dml")
		t.Logf("rootcoord dml channel = %s", Params.RootCoordDml.GetValue())

		assert.Equal(t, Params.RootCoordDelta.GetValue(), "by-dev-rootcoord-delta")
		t.Logf("rootcoord delta channel = %s", Params.RootCoordDelta.GetValue())

		assert.Equal(t, Params.RootCoordSubName.GetValue(), "by-dev-rootCoord")
		t.Logf("rootcoord subname = %s", Params.RootCoordSubName.GetValue())

		// -- querycoord --
		assert.Equal(t, Params.QueryCoordSearch.GetValue(), "by-dev-search")
		t.Logf("querycoord search channel = %s", Params.QueryCoordSearch.GetValue())

		assert.Equal(t, Params.QueryCoordSearchResult.GetValue(), "by-dev-searchResult")
		t.Logf("querycoord search result channel = %s", Params.QueryCoordSearchResult.GetValue())

		assert.Equal(t, Params.QueryCoordTimeTick.GetValue(), "by-dev-queryTimeTick")
		t.Logf("querycoord timetick channel = %s", Params.QueryCoordTimeTick.GetValue())

		// -- querynode --
		assert.Equal(t, Params.QueryNodeSubName.GetValue(), "by-dev-queryNode")
		t.Logf("querynode subname = %s", Params.QueryNodeSubName.GetValue())

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

		params.Save("common.security.superUsers", "")
		assert.Equal(t, []string{""}, Params.SuperUsers.GetAsStrings())
	})

	t.Run("test rootCoordConfig", func(t *testing.T) {
		Params := params.RootCoordCfg

		assert.NotEqual(t, Params.MaxPartitionNum.GetAsInt64(), 0)
		t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum.GetAsInt64())
		assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex.GetAsInt64(), 0)
		t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex.GetAsInt64())
		assert.NotEqual(t, Params.ImportTaskExpiration.GetAsFloat(), 0)
		t.Logf("master ImportTaskRetention = %f", Params.ImportTaskRetention.GetAsFloat())
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("rootCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())

		SetCreateTime(time.Now())
		SetUpdateTime(time.Now())
	})

	t.Run("test proxyConfig", func(t *testing.T) {
		Params := params.ProxyCfg

		t.Logf("TimeTickInterval: %v", Params.TimeTickInterval)

		t.Logf("MsgStreamTimeTickBufSize: %d", Params.MsgStreamTimeTickBufSize.GetAsInt64())

		t.Logf("MaxNameLength: %d", Params.MaxNameLength.GetAsInt64())

		t.Logf("MaxFieldNum: %d", Params.MaxFieldNum.GetAsInt64())

		t.Logf("MaxShardNum: %d", Params.MaxShardNum.GetAsInt64())

		t.Logf("MaxDimension: %d", Params.MaxDimension.GetAsInt64())

		t.Logf("MaxTaskNum: %d", Params.MaxTaskNum.GetAsInt64())

		t.Logf("AccessLog.Enable: %t", Params.AccessLog.Enable.GetAsBool())

		t.Logf("AccessLog.MaxSize: %d", Params.AccessLog.MaxSize.GetAsInt64())

		t.Logf("AccessLog.MaxBackups: %d", Params.AccessLog.MaxBackups.GetAsInt64())

		t.Logf("AccessLog.MaxDays: %d", Params.AccessLog.RotatedTime.GetAsInt64())

		t.Logf("ShardLeaderCacheInterval: %d", Params.ShardLeaderCacheInterval.GetAsInt64())
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
		Params := params.QueryCoordCfg
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("queryCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())

		params.Save("queryCoord.NextTargetSurviveTime", "100")
		NextTargetSurviveTime := Params.NextTargetSurviveTime
		assert.Equal(t, int64(100), NextTargetSurviveTime.GetAsInt64())

		params.Save("queryCoord.UpdateNextTargetInterval", "100")
		UpdateNextTargetInterval := Params.UpdateNextTargetInterval
		assert.Equal(t, int64(100), UpdateNextTargetInterval.GetAsInt64())

		params.Save("queryCoord.checkNodeInReplicaInterval", "100")
		checkNodeInReplicaInterval := Params.CheckNodeInReplicaInterval
		assert.Equal(t, 100, checkNodeInReplicaInterval.GetAsInt())

		params.Save("queryCoord.checkResourceGroupInterval", "10")
		checkResourceGroupInterval := Params.CheckResourceGroupInterval
		assert.Equal(t, 10, checkResourceGroupInterval.GetAsInt())

		enableResourceGroupAutoRecover := Params.EnableRGAutoRecover
		assert.Equal(t, true, enableResourceGroupAutoRecover.GetAsBool())
		params.Save("queryCoord.enableRGAutoRecover", "false")
		enableResourceGroupAutoRecover = Params.EnableRGAutoRecover
		assert.Equal(t, false, enableResourceGroupAutoRecover.GetAsBool())
	})

	t.Run("test queryNodeConfig", func(t *testing.T) {
		Params := params.QueryNodeCfg

		interval := Params.StatsPublishInterval.GetAsInt()
		assert.Equal(t, 1000, interval)

		length := Params.FlowGraphMaxQueueLength.GetAsInt32()
		assert.Equal(t, int32(1024), length)

		maxParallelism := Params.FlowGraphMaxParallelism.GetAsInt32()
		assert.Equal(t, int32(1024), maxParallelism)

		// test query side config
		chunkRows := Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(1024), chunkRows)

		nlist := Params.SmallIndexNlist.GetAsInt64()
		assert.Equal(t, int64(128), nlist)

		nprobe := Params.SmallIndexNProbe.GetAsInt64()
		assert.Equal(t, int64(16), nprobe)

		assert.Equal(t, true, Params.GroupEnabled.GetAsBool())
		assert.Equal(t, int32(10240), Params.MaxReceiveChanSize.GetAsInt32())
		assert.Equal(t, int32(10240), Params.MaxUnsolvedQueueSize.GetAsInt32())
		assert.Equal(t, int32(runtime.GOMAXPROCS(0)*2), Params.MaxReadConcurrency.GetAsInt32())
		assert.Equal(t, int64(1000), Params.MaxGroupNQ.GetAsInt64())
		assert.Equal(t, 10.0, Params.TopKMergeRatio.GetAsFloat())
		assert.Equal(t, 10.0, Params.CPURatio.GetAsFloat())

		// test small indexNlist/NProbe default
		params.Remove("queryNode.segcore.smallIndex.nlist")
		params.Remove("queryNode.segcore.smallIndex.nprobe")
		params.Save("queryNode.segcore.chunkRows", "8192")
		chunkRows = Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(8192), chunkRows)

		nlist = Params.SmallIndexNlist.GetAsInt64()
		assert.Equal(t, int64(128), nlist)

		nprobe = Params.SmallIndexNProbe.GetAsInt64()
		assert.Equal(t, int64(8), nprobe)

		params.Remove("queryNode.segcore.smallIndex.nlist")
		params.Remove("queryNode.segcore.smallIndex.nprobe")
		params.Save("queryNode.segcore.chunkRows", "64")
		chunkRows = Params.ChunkRows.GetAsInt64()
		assert.Equal(t, int64(1024), chunkRows)

		nlist = Params.SmallIndexNlist.GetAsInt64()
		assert.Equal(t, int64(64), nlist)

		nprobe = Params.SmallIndexNProbe.GetAsInt64()
		assert.Equal(t, int64(4), nprobe)

		params.Save("queryNode.gracefulStopTimeout", "100")
		gracefulStopTimeout := Params.GracefulStopTimeout
		assert.Equal(t, int64(100), gracefulStopTimeout.GetAsInt64())
	})

	t.Run("test dataCoordConfig", func(t *testing.T) {
		Params := params.DataCoordCfg
		assert.Equal(t, 24*60*60*time.Second, Params.SegmentMaxLifetime.GetAsDuration(time.Second))
		assert.True(t, Params.EnableGarbageCollection.GetAsBool())
		assert.Equal(t, Params.EnableActiveStandby.GetAsBool(), false)
		t.Logf("dataCoord EnableActiveStandby = %t", Params.EnableActiveStandby.GetAsBool())
	})

	t.Run("test dataNodeConfig", func(t *testing.T) {
		Params := params.DataNodeCfg

		SetNodeID(2)

		id := GetNodeID()
		t.Logf("NodeID: %d", id)

		length := Params.FlowGraphMaxQueueLength.GetAsInt()
		t.Logf("flowGraphMaxQueueLength: %d", length)

		maxParallelism := Params.FlowGraphMaxParallelism.GetAsInt()
		t.Logf("flowGraphMaxParallelism: %d", maxParallelism)

		size := Params.FlushInsertBufferSize.GetAsInt()
		t.Logf("FlushInsertBufferSize: %d", size)

		period := Params.SyncPeriod
		t.Logf("SyncPeriod: %v", period)
		assert.Equal(t, 10*time.Minute, Params.SyncPeriod.GetAsDuration(time.Second))
	})

	t.Run("test indexNodeConfig", func(t *testing.T) {
		Params := params.IndexNodeCfg
		params.Save(Params.GracefulStopTimeout.Key, "50")
		assert.Equal(t, Params.GracefulStopTimeout.GetAsInt64(), int64(50))
	})

	t.Run("channel config priority", func(t *testing.T) {
		Params := params.CommonCfg
		params.Save(Params.RootCoordDml.Key, "dml1")
		params.Save(Params.RootCoordDml.FallbackKeys[0], "dml2")

		assert.Equal(t, "by-dev-dml1", Params.RootCoordDml.GetValue())
	})
}

func TestForbiddenItem(t *testing.T) {
	Init()
	params := Get()

	params.mgr.OnEvent(&config.Event{
		Key:   params.CommonCfg.ClusterPrefix.Key,
		Value: "new-cluster",
	})
	assert.Equal(t, "by-dev", params.CommonCfg.ClusterPrefix.GetValue())
}
