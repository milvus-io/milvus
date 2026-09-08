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

package initcore

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestJSONStatsBuildExecutorConfig(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	key := pt.DataNodeCfg.JSONStatsBuildMaxWorkers.Key
	require.NoError(t, pt.Reset(key))
	defer pt.Reset(key)

	cpuNum := hardware.GetCPUNum()
	if cpuNum < 1 {
		cpuNum = 1
	}

	assert.Equal(t, min(defaultJSONStatsBuildMaxWorkers, cpuNum),
		ResolveJSONStatsBuildExecutorPoolSize(pt))

	require.NoError(t, pt.Save(key, "2"))
	assert.Equal(t, min(2, cpuNum), ResolveJSONStatsBuildExecutorPoolSize(pt))

	require.NoError(t, pt.Save(key, "0"))
	assert.Equal(t, min(defaultJSONStatsBuildMaxWorkers, cpuNum),
		ResolveJSONStatsBuildExecutorPoolSize(pt))

	RegisterJSONStatsBuildExecutorWatcher(pt, "test")
	var watcherFires atomic.Int32
	sentinel := config.NewHandler("json-stats-build-executor-sentinel", func(*config.Event) {
		watcherFires.Add(1)
	})
	pt.Watch(key, sentinel)
	defer pt.Unwatch(key, sentinel)

	require.NoError(t, pt.Save(key, "1"))
	assert.Positive(t, watcherFires.Load())
}

func TestJSONStatsBuildMemoryBudgetConfig(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	key := pt.DataNodeCfg.JSONStatsBuildMaxInflightBytes.Key
	require.NoError(t, pt.Reset(key))
	defer pt.Reset(key)

	assert.Equal(t, int64(paramtable.DefaultJSONStatsBuildMaxInflightBytes),
		ResolveJSONStatsBuildMaxInflightBytes(pt))

	require.NoError(t, pt.Save(key, "67108864"))
	assert.Equal(t, int64(67108864), ResolveJSONStatsBuildMaxInflightBytes(pt))

	require.NoError(t, pt.Save(key, "0"))
	assert.Zero(t, ResolveJSONStatsBuildMaxInflightBytes(pt))

	require.NoError(t, pt.Save(key, "-1"))
	assert.Equal(t, int64(paramtable.DefaultJSONStatsBuildMaxInflightBytes),
		ResolveJSONStatsBuildMaxInflightBytes(pt))

	UpdateJSONStatsBuildMaxInflightBytes(0)
	UpdateJSONStatsBuildMaxInflightBytes(128 * 1024 * 1024)

	RegisterJSONStatsBuildMemoryBudgetWatcher(pt, "test")
	var watcherFires atomic.Int32
	sentinel := config.NewHandler("json-stats-build-memory-budget-sentinel", func(*config.Event) {
		watcherFires.Add(1)
	})
	pt.Watch(key, sentinel)
	defer pt.Unwatch(key, sentinel)

	require.NoError(t, pt.Save(key, "33554432"))
	assert.Positive(t, watcherFires.Load())
}
