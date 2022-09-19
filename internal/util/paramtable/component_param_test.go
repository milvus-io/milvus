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

	t.Run("test rootCoordConfig", func(t *testing.T) {
		Params := CParams.RootCoordCfg

		assert.NotEqual(t, Params.MaxPartitionNum, 0)
		t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum)
		assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex, 0)
		t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex)
		assert.NotEqual(t, Params.ImportTaskExpiration, 0)
		t.Logf("master ImportTaskRetention = %f", Params.ImportTaskRetention)

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

	t.Run("test queryNodeConfig", func(t *testing.T) {
		Params := CParams.QueryNodeCfg

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
		assert.Equal(t, int64(1000), Params.MaxGroupNQ)
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
	})

	t.Run("test dataCoordConfig", func(t *testing.T) {
		Params := CParams.DataCoordCfg
		assert.Equal(t, 24*60*60*time.Second, Params.SegmentMaxLifetime)

		assert.True(t, Params.EnableGarbageCollection)
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
	})
}
