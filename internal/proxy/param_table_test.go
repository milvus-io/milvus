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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Normal(t *testing.T) {
	Params.Init()

	t.Run("EtcdEndPoints", func(t *testing.T) {
		t.Logf("EtcdEndPoints: %v", Params.EtcdEndpoints)
	})

	t.Run("MetaRootPath", func(t *testing.T) {
		t.Logf("MetaRootPath: %s", Params.MetaRootPath)
	})

	t.Run("PulsarAddress", func(t *testing.T) {
		t.Logf("PulsarAddress: %s", Params.PulsarAddress)
	})

	t.Run("RocksmqPath", func(t *testing.T) {
		t.Logf("RocksmqPath: %s", Params.RocksmqPath)
	})

	t.Run("TimeTickInterval", func(t *testing.T) {
		t.Logf("TimeTickInterval: %v", Params.TimeTickInterval)
	})

	t.Run("ProxySubName", func(t *testing.T) {
		assert.Equal(t, Params.ProxySubName, "by-dev-proxy-0")
		t.Logf("ProxySubName: %s", Params.ProxySubName)
	})

	t.Run("ProxyTimeTickChannelNames", func(t *testing.T) {
		assert.Equal(t, Params.ProxyTimeTickChannelNames, []string{"by-dev-proxyTimeTick-0"})
		t.Logf("ProxyTimeTickChannelNames: %v", Params.ProxyTimeTickChannelNames)
	})

	t.Run("MsgStreamTimeTickBufSize", func(t *testing.T) {
		t.Logf("MsgStreamTimeTickBufSize: %d", Params.MsgStreamTimeTickBufSize)
	})

	t.Run("MaxNameLength", func(t *testing.T) {
		t.Logf("MaxNameLength: %d", Params.MaxNameLength)
	})

	t.Run("MaxFieldNum", func(t *testing.T) {
		t.Logf("MaxFieldNum: %d", Params.MaxFieldNum)
	})

	t.Run("MaxShardNum", func(t *testing.T) {
		t.Logf("MaxShardNum: %d", Params.MaxShardNum)
	})

	t.Run("MaxDimension", func(t *testing.T) {
		t.Logf("MaxDimension: %d", Params.MaxDimension)
	})

	t.Run("DefaultPartitionName", func(t *testing.T) {
		t.Logf("DefaultPartitionName: %s", Params.DefaultPartitionName)
	})

	t.Run("DefaultIndexName", func(t *testing.T) {
		t.Logf("DefaultIndexName: %s", Params.DefaultIndexName)
	})

	t.Run("PulsarMaxMessageSize", func(t *testing.T) {
		t.Logf("PulsarMaxMessageSize: %d", Params.PulsarMaxMessageSize)
	})

	t.Run("RoleName", func(t *testing.T) {
		t.Logf("RoleName: %s", Params.RoleName)
	})

	t.Run("MaxTaskNum", func(t *testing.T) {
		t.Logf("MaxTaskNum: %d", Params.MaxTaskNum)
	})
}

func shouldPanic(t *testing.T, name string, f func()) {
	defer func() { recover() }()
	f()
	t.Errorf("%s should have panicked", name)
}

func TestParamTable_Panics(t *testing.T) {
	shouldPanic(t, "proxy.timeTickInterval", func() {
		Params.Remove("proxy.timeTickInterval")
		Params.initTimeTickInterval()
	})

	shouldPanic(t, "proxy.timeTickInterval", func() {
		Params.Save("proxy.timeTickInterval", "")
		Params.initTimeTickInterval()
	})

	shouldPanic(t, "proxy.msgStream.timeTick.bufSize", func() {
		Params.Remove("proxy.msgStream.timeTick.bufSize")
		Params.initMsgStreamTimeTickBufSize()
	})

	shouldPanic(t, "proxy.msgStream.timeTick.bufSize", func() {
		Params.Save("proxy.msgStream.timeTick.bufSize", "abc")
		Params.initMsgStreamTimeTickBufSize()
	})

	shouldPanic(t, "proxy.maxNameLength", func() {
		Params.Remove("proxy.maxNameLength")
		Params.initMaxNameLength()
	})

	shouldPanic(t, "proxy.maxNameLength", func() {
		Params.Save("proxy.maxNameLength", "abc")
		Params.initMaxNameLength()
	})

	shouldPanic(t, "proxy.maxFieldNum", func() {
		Params.Remove("proxy.maxFieldNum")
		Params.initMaxFieldNum()
	})

	shouldPanic(t, "proxy.maxFieldNum", func() {
		Params.Save("proxy.maxFieldNum", "abc")
		Params.initMaxFieldNum()
	})

	shouldPanic(t, "proxy.maxShardNum", func() {
		Params.Remove("proxy.maxShardNum")
		Params.initMaxShardNum()
	})

	shouldPanic(t, "proxy.maxShardNum", func() {
		Params.Save("proxy.maxShardNum", "abc")
		Params.initMaxShardNum()
	})

	shouldPanic(t, "proxy.maxDimension", func() {
		Params.Remove("proxy.maxDimension")
		Params.initMaxDimension()
	})

	shouldPanic(t, "proxy.maxDimension", func() {
		Params.Save("proxy.maxDimension", "-asdf")
		Params.initMaxDimension()
	})

	shouldPanic(t, "proxy.maxTaskNum", func() {
		Params.Remove("proxy.maxTaskNum")
		Params.initMaxTaskNum()
	})

	shouldPanic(t, "proxy.maxTaskNum", func() {
		Params.Save("proxy.maxTaskNum", "-asdf")
		Params.initMaxTaskNum()
	})
}
