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

package rootcoord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	assert.NotEqual(t, Params.PulsarAddress, "")
	t.Logf("pulsar address = %s", Params.PulsarAddress)

	assert.NotZero(t, len(Params.EtcdEndpoints))
	t.Logf("etcd endpoints = %s", Params.EtcdEndpoints)

	assert.NotEqual(t, Params.MetaRootPath, "")
	t.Logf("meta root path = %s", Params.MetaRootPath)

	assert.NotEqual(t, Params.KvRootPath, "")
	t.Logf("kv root path = %s", Params.KvRootPath)

	assert.Equal(t, Params.MsgChannelSubName, "by-dev-rootCoord")
	t.Logf("msg channel sub name = %s", Params.MsgChannelSubName)

	assert.Equal(t, Params.TimeTickChannel, "by-dev-rootcoord-timetick")
	t.Logf("master time tick channel = %s", Params.TimeTickChannel)

	assert.Equal(t, Params.StatisticsChannel, "by-dev-rootcoord-statistics")
	t.Logf("master statistics channel = %s", Params.StatisticsChannel)

	assert.Equal(t, Params.DmlChannelName, "by-dev-rootcoord-dml")
	t.Logf("dml channel = %s", Params.DmlChannelName)

	assert.NotEqual(t, Params.MaxPartitionNum, 0)
	t.Logf("master MaxPartitionNum = %d", Params.MaxPartitionNum)

	assert.NotEqual(t, Params.MinSegmentSizeToEnableIndex, 0)
	t.Logf("master MinSegmentSizeToEnableIndex = %d", Params.MinSegmentSizeToEnableIndex)

	assert.NotEqual(t, Params.DefaultPartitionName, "")
	t.Logf("default partition name = %s", Params.DefaultPartitionName)

	assert.NotEqual(t, Params.DefaultIndexName, "")
	t.Logf("default index name = %s", Params.DefaultIndexName)

	assert.NotZero(t, Params.Timeout)
	t.Logf("master timeout = %d", Params.Timeout)

	assert.NotZero(t, Params.TimeTickInterval)
	t.Logf("master timetickerInterval = %d", Params.TimeTickInterval)

	Params.CreatedTime = time.Now()
	Params.UpdatedTime = time.Now()
	t.Logf("created time: %v", Params.CreatedTime)
	t.Logf("updated time: %v", Params.UpdatedTime)
}
