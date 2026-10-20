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

package task

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadPacer(t *testing.T) {
	cfg := loadPacerConfig{enabled: true, initial: 4, min: 2, max: 64, slowFraction: 0.25}
	timeout := 100 * time.Second

	t.Run("slow load on an idle node does not shrink", func(t *testing.T) {
		p := &loadPacer{}
		require.True(t, p.tryAcquire(cfg))
		p.release(cfg, 30*time.Second, timeout)
		require.Equal(t, 4.0, p.Cap())
	})

	t.Run("sustained slow loads shrink once per window", func(t *testing.T) {
		p := &loadPacer{}
		// Loads keep queueing in QueryCoord: every freed slot is refilled.
		slow := func() {
			for p.tryAcquire(cfg) {
			}
			p.release(cfg, 30*time.Second, timeout)
		}
		for i := 0; i < 3; i++ {
			slow()
		}
		require.Equal(t, 4.0, p.Cap(), "no shrink within the first window")
		slow()
		require.Equal(t, 2.0, p.Cap(), "halved once the window is full")
		for i := 0; i < 8; i++ {
			slow()
		}
		require.Equal(t, 2.0, p.Cap(), "never below the minimum")
	})

	t.Run("the cap settles where loads stay under a quarter of the timeout", func(t *testing.T) {
		// A node whose loads take a quarter of the timeout at 4 in flight and
		// stretch past it beyond that: the cap halves when loads cross the
		// line, grows while they stay under, and keeps hovering around 4.
		p := &loadPacer{}
		for round := 0; round < 200; round++ {
			for p.tryAcquire(cfg) {
			}
			elapsed := time.Duration(float64(p.Cap()) / 4 * 26 * float64(time.Second))
			p.release(cfg, elapsed, timeout)
		}
		require.InDelta(t, 4.0, p.Cap(), 2.0)
	})

	t.Run("fast loads on a saturated node grow the cap", func(t *testing.T) {
		p := &loadPacer{}
		for round := 0; round < 20; round++ {
			for p.tryAcquire(cfg) {
			}
			p.release(cfg, time.Second, timeout)
			p.release(cfg, time.Second, timeout)
			for p.inflight > 0 {
				p.release(cfg, time.Second, timeout)
			}
		}
		require.Greater(t, p.Cap(), 4.0)
	})
}
