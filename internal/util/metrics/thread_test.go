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

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThreadWatcher(t *testing.T) {
	watcher := NewThreadWatcher()

	watcher.Start()

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		watcher.Stop()
	}()
	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		require.FailNow(t, "watcher failed to close after 5 seconds")
	}
}

func TestClassifyThreadName(t *testing.T) {
	group, ok := classifyThreadName("knowhere_search")
	require.True(t, ok)
	require.Equal(t, "knowhere_search", group)

	group, ok = classifyThreadName("MILVUS_FL_WR_0")
	require.True(t, ok)
	require.Equal(t, "file_write", group)

	group, ok = classifyThreadName("knowhere_fetch_")
	require.True(t, ok)
	require.Equal(t, "knowhere_fetch", group)

	group, ok = classifyThreadName("rocksdb:high")
	require.True(t, ok)
	require.Equal(t, "rocksdb_high", group)

	group, ok = classifyThreadName("rocksdb:low")
	require.True(t, ok)
	require.Equal(t, "rocksdb_low", group)

	group, ok = classifyThreadName("rocksdb:bottom")
	require.True(t, ok)
	require.Equal(t, "rocksdb_bottom", group)

	group, ok = classifyThreadName("grpc_global_tim")
	require.False(t, ok)
	require.Equal(t, unclassifiedThreadPool, group)
}

func TestParseThreadStat(t *testing.T) {
	state, cpu, err := parseThreadStat("123 (knowhere search) R 1 2 3 4 5 6 7 8 9 10 34 56 0 0 0")
	require.NoError(t, err)
	require.Equal(t, byte('R'), state)
	require.Equal(t, uint64(90), cpu)
}

func TestCollectActiveThreadGroups(t *testing.T) {
	current := []threadStat{
		{tid: 1, name: "rocksdb:high", state: 'S', cpu: 11},
		{tid: 2, name: "arrow-worker", state: 'S', cpu: 6},
		{tid: 3, name: "rocksdb:low", state: 'R', cpu: 1},
		{tid: 4, name: "knowhere_search", state: 'S', cpu: 20},
		{tid: 5, name: "rocksdb:bottom", state: 'D', cpu: 30},
	}
	previous := map[int32]threadSample{
		1: {group: "rocksdb_high", cpu: 10},
		2: {group: unclassifiedThreadPool, cpu: 5},
		4: {group: "knowhere_fetch", cpu: 19},
		5: {group: "rocksdb_bottom", cpu: 30},
	}

	activeByGroup, nextSamples := collectActiveThreadGroups(current, previous)

	require.Equal(t, 1, activeByGroup["rocksdb_high"])
	require.Equal(t, 1, activeByGroup[unclassifiedThreadPool])
	require.Equal(t, 0, activeByGroup["rocksdb_low"])
	require.Equal(t, 0, activeByGroup["knowhere_search"])
	require.Equal(t, 1, activeByGroup["rocksdb_bottom"])
	require.Equal(t, threadSample{group: unclassifiedThreadPool, cpu: 6}, nextSamples[2])
	require.Len(t, nextSamples, len(current))
}
