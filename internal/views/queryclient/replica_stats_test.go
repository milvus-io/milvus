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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDecay = 10 * time.Second

var testEpoch = time.Unix(1_800_000_000, 0)

func TestReplicaStatsMean(t *testing.T) {
	s := &replicaStats{}
	_, ok := s.mean(testEpoch, testDecay)
	assert.False(t, ok)

	for i := 0; i < 100; i++ {
		s.succeed(testEpoch, time.Second, testDecay)
	}
	m, ok := s.mean(testEpoch, testDecay)
	require.True(t, ok)
	assert.InDelta(t, float64(time.Second), float64(m), float64(time.Millisecond))

	// a slow period is forgotten within a few decay times of fast samples
	for i := 1; i <= 300; i++ {
		s.succeed(testEpoch.Add(time.Duration(i)*100*time.Millisecond), 10*time.Millisecond, testDecay)
	}
	m, _ = s.mean(testEpoch.Add(30*time.Second), testDecay)
	assert.Less(t, m, 70*time.Millisecond)

	// an out of order sample does not move the clock back
	s.succeed(testEpoch, 10*time.Millisecond, testDecay)
	assert.Equal(t, testEpoch.Add(30*time.Second), s.sumAt)

	// unknown again after replicaStaleDecays decay times without a sample
	_, ok = s.mean(testEpoch.Add(30*time.Second+replicaStaleDecays*testDecay), testDecay)
	assert.True(t, ok)
	_, ok = s.mean(testEpoch.Add(30*time.Second+replicaStaleDecays*testDecay+time.Second), testDecay)
	assert.False(t, ok)
}

func TestReplicaStatsPenalty(t *testing.T) {
	failInARow := func(s *replicaStats, now time.Time, penalty time.Duration) {
		s.failures = 0
		for i := 0; i < failuresBeforePenalty; i++ {
			s.fail(now, penalty, testDecay)
		}
	}
	s := &replicaStats{}
	failInARow(s, testEpoch, 15*time.Second)
	assert.Equal(t, 15*time.Second, s.currentPenalty(testEpoch, testDecay))
	assert.InDelta(t, float64(15*time.Second)/math.E, float64(s.currentPenalty(testEpoch.Add(testDecay), testDecay)), float64(time.Millisecond))

	// a smaller penalty does not lower the current one
	failInARow(s, testEpoch.Add(time.Second), time.Second)
	assert.Greater(t, s.currentPenalty(testEpoch.Add(time.Second), testDecay), 13*time.Second)

	// the penalty dominates a fast mean, and fades
	s.succeed(testEpoch.Add(time.Second), 10*time.Millisecond, testDecay)
	l, ok := s.latency(testEpoch.Add(time.Second), testDecay)
	require.True(t, ok)
	assert.Greater(t, l, 13*time.Second)
	l, _ = s.latency(testEpoch.Add(100*time.Second), testDecay)
	assert.Less(t, l, 15*time.Millisecond)

	// A penalty does not supply a successful latency measurement.
	s2 := &replicaStats{}
	failInARow(s2, testEpoch, time.Second)
	_, ok = s2.latency(testEpoch, testDecay)
	assert.False(t, ok)
}

// Failures count only after several in a row; a success in between resets.
func TestReplicaStatsFailures(t *testing.T) {
	s := &replicaStats{}
	for round := 0; round < 2; round++ {
		for i := 0; i < failuresBeforePenalty-1; i++ {
			assert.False(t, s.fail(testEpoch, 15*time.Second, testDecay))
		}
		s.succeed(testEpoch, 10*time.Millisecond, testDecay)
	}
	for i := 0; i < failuresBeforePenalty-1; i++ {
		assert.False(t, s.fail(testEpoch, 15*time.Second, testDecay))
	}
	assert.Equal(t, time.Duration(0), s.currentPenalty(testEpoch, testDecay))
	assert.True(t, s.fail(testEpoch, 15*time.Second, testDecay))
	assert.Equal(t, 15*time.Second, s.currentPenalty(testEpoch, testDecay))
	assert.True(t, s.fail(testEpoch, 15*time.Second, testDecay), "and every one after")
}

func TestReplicaStatsInflight(t *testing.T) {
	s := &replicaStats{}
	s.end()
	assert.Equal(t, 0, s.inflight, "ending without in-flight attempts is harmless")
	s.begin()
	s.begin()
	s.end()
	assert.Equal(t, 1, s.inflight)
}
