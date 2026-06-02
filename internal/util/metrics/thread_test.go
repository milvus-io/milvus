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

	_, ok = classifyThreadName("grpc_global_tim")
	require.False(t, ok)
}

func TestParseThreadStat(t *testing.T) {
	state, cpu, err := parseThreadStat("123 (knowhere search) R 1 2 3 4 5 6 7 8 9 10 34 56 0 0 0")
	require.NoError(t, err)
	require.Equal(t, byte('R'), state)
	require.Equal(t, uint64(90), cpu)
}
