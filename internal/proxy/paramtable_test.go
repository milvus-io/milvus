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

import "testing"

func TestParamTable(t *testing.T) {
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
		t.Logf("ProxySubName: %s", Params.ProxySubName)
	})

	t.Run("ProxyTimeTickChannelNames", func(t *testing.T) {
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
}
