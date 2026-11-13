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

package idempotencyview

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadRetentionConfig(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.Equal(t, "streaming.idempotency.ttlPerPChannel", params.StreamingCfg.IdempotencyTTLPerPChannel.Key)
	require.Equal(t, "streaming.idempotency.maxSizePerPChannel", params.StreamingCfg.IdempotencyMaxSizePerPChannel.Key)
	require.Equal(t, "streaming.idempotency.bestEffortMinCountPerVChannel", params.StreamingCfg.IdempotencyBestEffortMinCountPerVChannel.Key)
	config := LoadRetentionConfig()
	require.Equal(t, time.Hour, config.TTL)
	require.Equal(t, 256*1024*1024, config.MaxSize)
	require.Equal(t, 16, config.MinCount)
	for _, key := range []string{params.StreamingCfg.IdempotencyTTLPerPChannel.Key, params.StreamingCfg.IdempotencyMaxSizePerPChannel.Key, params.StreamingCfg.IdempotencyBestEffortMinCountPerVChannel.Key} {
		t.Cleanup(func() { params.Reset(key) })
	}
	params.Save(params.StreamingCfg.IdempotencyTTLPerPChannel.Key, "20m")
	params.Save(params.StreamingCfg.IdempotencyMaxSizePerPChannel.Key, "128MB")
	params.Save(params.StreamingCfg.IdempotencyBestEffortMinCountPerVChannel.Key, "0")
	config = LoadRetentionConfig()
	require.Equal(t, 20*time.Minute, config.TTL)
	require.Equal(t, 128*1024*1024, config.MaxSize)
	require.Zero(t, config.MinCount)
	params.Save(params.StreamingCfg.IdempotencyTTLPerPChannel.Key, "0s")
	params.Save(params.StreamingCfg.IdempotencyMaxSizePerPChannel.Key, "invalid")
	params.Save(params.StreamingCfg.IdempotencyBestEffortMinCountPerVChannel.Key, "-1")
	config = LoadRetentionConfig()
	require.Equal(t, time.Hour, config.TTL)
	require.Equal(t, 256*1024*1024, config.MaxSize)
	require.Zero(t, config.MinCount)
}
