// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queryclient

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReplicaObservationFailureAttribution(t *testing.T) {
	for _, scenario := range []string{"planning pending", "streaming pending", "query finished streaming pending", "query pending", "late error", "late success", "caller canceled", "input error", "overload error"} {
		t.Run(scenario, func(t *testing.T) {
			policy := newReplicaPolicy()
			key := replicaPolicyKey{channel: "channel", class: "search"}
			observation := policy.begin(key, 1, adaptiveTestConfig())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			expected := 0
			switch scenario {
			case "streaming pending":
				defer observation.workerStarted(qviews.NewStreamingNodeFromVChannel("by-dev-rootcoord-dml_0_100v0"))()
			case "query finished streaming pending":
				observation.workerStarted(qviews.NewQueryNode(1))()
				defer observation.workerStarted(qviews.NewStreamingNodeFromVChannel("by-dev-rootcoord-dml_0_100v0"))()
			case "query pending", "late error", "late success", "caller canceled":
				defer observation.workerStarted(qviews.NewQueryNode(1))()
				expected = 1
			}
			if scenario == "caller canceled" {
				cancel()
				expected = 0
			}
			observation.overtaken(ctx)
			observation.overtaken(ctx)
			switch scenario {
			case "input error":
				observation.finish(ctx, merr.WrapErrParameterInvalidMsg("invalid request"), true, false)
			case "overload error":
				observation.finish(ctx, &workNodeFailure{node: qviews.NewQueryNode(1), err: merr.WrapErrTooManyRequests(200, "read task queue")}, true, false)
			case "late success":
				observation.finish(ctx, nil, false, true)
				expected = 0
			default:
				observation.finish(ctx, merr.WrapErrServiceNotReadyMsg("worker"), scenario == "late error", false)
			}
			stats := policy.keys[key].replicas[1]
			require.Equal(t, expected, stats.failures)
			require.Zero(t, stats.inflight)
			if scenario == "late success" {
				require.Positive(t, stats.count)
			}
		})
	}
}

func TestReplicaPolicyPrunesRetiredReplicasInActiveChannel(t *testing.T) {
	policy := newReplicaPolicy()
	key := replicaPolicyKey{channel: "channel", class: "query"}
	now := time.Now()
	policy.keys[key] = &replicaPolicyStats{lastUsed: now, replicas: map[int64]*replicaStats{
		1: {lastUsed: now.Add(-11 * time.Minute)},
		2: {lastUsed: now.Add(-11 * time.Minute), inflight: 1},
		3: {lastUsed: now},
	}}
	policy.mu.Lock()
	stats := policy.statsLocked(key, now)
	policy.mu.Unlock()
	require.NotContains(t, stats.replicas, int64(1))
	require.Contains(t, stats.replicas, int64(2))
	require.Contains(t, stats.replicas, int64(3))
}

func TestReplicaRouteAlternativeIsLocalAndDistinct(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}}}}
	choose := func(candidates []qviews.Replica) qviews.Replica { return candidates[0] }
	alternative, ok := route.alternative(1, nil, choose)
	require.True(t, ok)
	require.Equal(t, int64(2), alternative.ReplicaID)
	_, ok = route.alternative(1, map[qviews.Replica]bool{{ReplicaID: 2}: false}, choose)
	require.False(t, ok)
	route.snapshot.Valid = false
	_, ok = route.alternative(1, nil, choose)
	require.False(t, ok)
}

func TestReplicaSelectionLatencyFloorAndInflight(t *testing.T) {
	policy := newReplicaPolicy()
	policy.rng = rand.New(rand.NewSource(1))
	key := replicaPolicyKey{channel: "channel", class: "search"}
	now := time.Now()
	fast, slow := &replicaStats{lastUsed: now}, &replicaStats{lastUsed: now}
	fast.succeed(now, time.Millisecond, time.Hour)
	slow.succeed(now, 100*time.Millisecond, time.Hour)
	policy.keys[key] = &replicaPolicyStats{lastUsed: now, replicas: map[int64]*replicaStats{1: fast, 2: slow}}
	cfg := adaptiveTestConfig()
	cfg.Decay = time.Hour
	cfg.MinShare = .2
	candidates := []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}}
	count := func() int {
		wins := 0
		for i := 0; i < 10000; i++ {
			if policy.selectReplica(key, candidates, cfg).ReplicaID == 1 {
				wins++
			}
		}
		return wins
	}
	require.InDelta(t, 7941, count(), 150, "minimum share preserves exploration of the slower replica")
	fast.inflight = 99
	require.InDelta(t, 5000, count(), 150, "inflight work balances equally effective latencies")
	// An unknown replica inherits peer latency; lack of requests is not a penalty.
	delete(policy.keys[key].replicas, 1)
	require.InDelta(t, 5000, count(), 150)
}
