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

package delegator

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
)

// A failed spawn waits up to thirty seconds before its next attempt. A source
// released or stopped during that wait must give the pending slot up at once,
// not sleep the whole backoff first.
func TestFailedSpawnGivesUpDuringItsBackoff(t *testing.T) {
	cases := []struct {
		name string
		stop func(sd *shardDelegator)
	}{
		{"source releasing", func(sd *shardDelegator) { sd.MarkReleasing() }},
		{"delegator stopped", func(sd *shardDelegator) { sd.lifetime.SetState(lifetime.Stopped) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backoff := mockey.Mock(splitChildSpawnBackoff).Return(time.Hour).Build()
			defer backoff.UnPatch()
			spawner := &fakeChildSpawner{err: errors.New("spawn boom")}
			sd := &shardDelegator{
				vchannelName: "v0",
				children:     make(map[string]ShardDelegator),
				childSpawner: spawner,
				lifetime:     lifetime.NewLifetime(lifetime.Working),
			}

			require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
			require.Eventually(t, func() bool { return spawner.attempts() == 1 }, time.Second, time.Millisecond)
			tc.stop(sd)

			assert.Eventually(t, func() bool {
				sd.childMut.Lock()
				defer sd.childMut.Unlock()
				return len(sd.spawning) == 0
			}, 3*time.Second, 10*time.Millisecond, "the spawn slept through its backoff instead of giving up")
			assert.Equal(t, 1, spawner.attempts())
		})
	}
}

// SetChildSpawner writes the spawner under childMut; a spawn in flight must not
// read it without that lock (run with -race). The spawn uses the spawner that
// was configured when the fence was consumed.
func TestSpawnDoesNotRaceWithSetChildSpawner(t *testing.T) {
	backoff := mockey.Mock(splitChildSpawnBackoff).Return(time.Millisecond).Build()
	defer backoff.UnPatch()
	first := &fakeChildSpawner{failures: 3}
	sd := &shardDelegator{
		vchannelName: "v0",
		children:     make(map[string]ShardDelegator),
		childSpawner: first,
		lifetime:     lifetime.NewLifetime(lifetime.Working),
	}
	require.NoError(t, sd.ProcessSplitShard(context.Background(), newSplitTargets("v1")))
	for i := 0; i < 50; i++ {
		sd.SetChildSpawner(&fakeChildSpawner{})
	}
	require.Eventually(t, func() bool { return len(childVChannels(sd)) == 1 }, 3*time.Second, time.Millisecond)
	assert.Equal(t, 4, first.attempts(), "the spawn switched spawners mid-flight")
}
