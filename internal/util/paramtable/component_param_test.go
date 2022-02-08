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
	"log"
	"os"
	"path"
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

	t.Run("test commonConfig", func(t *testing.T) {
		Params := CParams.CommonCfg

		assert.NotEqual(t, Params.DefaultPartitionName, "")
		t.Logf("default partition name = %s", Params.DefaultPartitionName)

		assert.NotEqual(t, Params.DefaultIndexName, "")
		t.Logf("default index name = %s", Params.DefaultIndexName)

		assert.Equal(t, Params.RetentionDuration, int64(DefaultRetentionDuration))
	})

	t.Run("test knowhereConfig", func(t *testing.T) {
		Params := CParams.KnowhereCfg

		assert.NotEqual(t, Params.SimdType, "")
		t.Logf("knowhere simd type = %s", Params.SimdType)
	})

	t.Run("test knowhereConfig", func(t *testing.T) {
		Params := CParams.MsgChannelCfg

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
		assert.Equal(t, Params.QueryNodeStats, "by-dev-query-node-stats")
		t.Logf("querynode stats channel = %s", Params.QueryNodeStats)

		// -- datacoord --
		assert.Equal(t, Params.DataCoordTimeTick, "by-dev-datacoord-timetick-channel")
		t.Logf("datacoord timetick channel = %s", Params.DataCoordTimeTick)

		assert.Equal(t, Params.DataCoordSegmentInfo, "by-dev-segment-info-channel")
		t.Logf("datacoord segment info channel = %s", Params.DataCoordSegmentInfo)

		assert.Equal(t, Params.DataCoordSubName, "by-dev-dataCoord")
		t.Logf("datacoord subname = %s", Params.DataCoordSubName)
	})

	t.Run("test rootCoordConfig", func(t *testing.T) {
		Params := CParams.RootCoordCfg

		assert.NotEqual(t, Params.MaxPartitionNum, 0)
		t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum)

		assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex, 0)
		t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex)

		Params.CreatedTime = time.Now()
		Params.UpdatedTime = time.Now()
		t.Logf("created time: %v", Params.CreatedTime)
		t.Logf("updated time: %v", Params.UpdatedTime)
	})

	t.Run("test proxyConfig", func(t *testing.T) {
		Params := CParams.ProxyCfg

		t.Logf("TimeTickInterval: %v", Params.TimeTickInterval)

		assert.Equal(t, Params.ProxySubName, "by-dev-proxy-0")
		t.Logf("ProxySubName: %s", Params.ProxySubName)

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
	})

	t.Run("test queryCoordConfig", func(t *testing.T) {
		//Params := CParams.QueryCoordCfg
	})

	t.Run("test queryNodeConfig", func(t *testing.T) {
		Params := CParams.QueryNodeCfg

		cacheSize := Params.CacheSize
		assert.Equal(t, int64(32), cacheSize)
		err := os.Setenv("CACHE_SIZE", "2")
		assert.NoError(t, err)
		Params.initCacheSize()
		assert.Equal(t, int64(2), Params.CacheSize)
		err = os.Setenv("CACHE_SIZE", "32")
		assert.NoError(t, err)
		Params.initCacheSize()
		assert.Equal(t, int64(32), Params.CacheSize)

		interval := Params.StatsPublishInterval
		assert.Equal(t, 1000, interval)

		bufSize := Params.SearchReceiveBufSize
		assert.Equal(t, int64(512), bufSize)

		bufSize = Params.SearchResultReceiveBufSize
		assert.Equal(t, int64(64), bufSize)

		bufSize = Params.SearchPulsarBufSize
		assert.Equal(t, int64(512), bufSize)

		length := Params.FlowGraphMaxQueueLength
		assert.Equal(t, int32(1024), length)

		maxParallelism := Params.FlowGraphMaxParallelism
		assert.Equal(t, int32(1024), maxParallelism)

		Params.QueryNodeID = 3
		Params.initQueryNodeSubName()
		name := Params.QueryNodeSubName
		assert.Equal(t, name, "by-dev-queryNode")
	})

	t.Run("test dataCoordConfig", func(t *testing.T) {
		//Params := CParams.DataCoordCfg
	})

	t.Run("test dataNodeConfig", func(t *testing.T) {
		Params := CParams.DataNodeCfg

		Params.NodeID = 2
		Params.Refresh()

		id := Params.NodeID
		log.Println("NodeID:", id)

		alias := Params.Alias
		log.Println("Alias:", alias)

		length := Params.FlowGraphMaxQueueLength
		log.Println("flowGraphMaxQueueLength:", length)

		maxParallelism := Params.FlowGraphMaxParallelism
		log.Println("flowGraphMaxParallelism:", maxParallelism)

		size := Params.FlushInsertBufferSize
		log.Println("FlushInsertBufferSize:", size)

		path1 := Params.InsertBinlogRootPath
		log.Println("InsertBinlogRootPath:", path1)

		name := Params.DataNodeSubName
		assert.Equal(t, name, "by-dev-dataNode-2")
		log.Println("DataNodeSubName:", name)

		Params.CreatedTime = time.Now()
		log.Println("CreatedTime: ", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		log.Println("UpdatedTime: ", Params.UpdatedTime)

		assert.Equal(t, path.Join("files", "insert_log"), Params.InsertBinlogRootPath)

		assert.Equal(t, path.Join("files", "stats_log"), Params.StatsBinlogRootPath)
	})

	t.Run("test indexCoordConfig", func(t *testing.T) {
		Params := CParams.IndexCoordCfg

		t.Logf("Address: %v", Params.Address)

		t.Logf("Port: %v", Params.Port)

		Params.CreatedTime = time.Now()
		t.Logf("CreatedTime: %v", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		t.Logf("UpdatedTime: %v", Params.UpdatedTime)

		t.Logf("IndexStorageRootPath: %v", Params.IndexStorageRootPath)
	})

	t.Run("test indexNodeConfig", func(t *testing.T) {
		Params := CParams.IndexNodeCfg

		t.Logf("IP: %v", Params.IP)

		t.Logf("Address: %v", Params.Address)

		t.Logf("Port: %v", Params.Port)

		t.Logf("NodeID: %v", Params.NodeID)

		t.Logf("Alias: %v", Params.Alias)

		Params.CreatedTime = time.Now()
		t.Logf("CreatedTime: %v", Params.CreatedTime)

		Params.UpdatedTime = time.Now()
		t.Logf("UpdatedTime: %v", Params.UpdatedTime)

		t.Logf("IndexStorageRootPath: %v", Params.IndexStorageRootPath)
	})
}
