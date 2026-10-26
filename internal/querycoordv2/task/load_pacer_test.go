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
	const mib = 1 << 20
	cfg := loadPacerConfig{enabled: true, perCore: 32 * mib, slowFraction: 0.5}
	timeout := 60 * time.Second
	const cpu = 16
	// 16 cores x 32 MiB/s = 512 MiB/s; a quarter of 60 s is 15 s: 7.5 GiB.
	budget := 7.5 * 1024 * mib

	t.Run("an idle node takes one load however large", func(t *testing.T) {
		p := &loadPacer{}
		require.True(t, p.tryAcquire(cfg, cpu, 20*1024*mib, timeout))
		require.False(t, p.tryAcquire(cfg, cpu, mib, timeout), "over budget")
		require.InDelta(t, 512*mib, p.Bandwidth(), 1)
	})

	t.Run("bytes in flight are kept to the budget, loads to the cores", func(t *testing.T) {
		p := &loadPacer{}
		n := 0
		for p.tryAcquire(cfg, cpu, 1024*mib, timeout) {
			n++
		}
		require.Equal(t, 7, n, "7 GiB fits, the 8th does not")
		p = &loadPacer{}
		n = 0
		for p.tryAcquire(cfg, cpu, 30*mib, timeout) {
			n++
		}
		require.Equal(t, cpu, n, "small segments stop at one load per core")
	})

	t.Run("slow load on an idle node does not shrink", func(t *testing.T) {
		p := &loadPacer{}
		require.True(t, p.tryAcquire(cfg, cpu, 1024*mib, timeout))
		p.release(cfg, 1024*mib, 50*time.Second, timeout)
		require.InDelta(t, 512*mib, p.Bandwidth(), 1)
	})

	t.Run("sustained slow loads on a saturated node halve the bandwidth once per window", func(t *testing.T) {
		p := &loadPacer{}
		// Loads keep queueing in QueryCoord: every freed slot is refilled.
		slow := func(bytes float64) {
			for p.tryAcquire(cfg, cpu, bytes, timeout) {
			}
			p.release(cfg, bytes, 40*time.Second, timeout)
		}
		slow(budget / 2)
		require.InDelta(t, 512*mib, p.Bandwidth(), 1, "no shrink within the first window")
		slow(budget / 2)
		require.InDelta(t, 256*mib, p.Bandwidth(), 1, "halved once a budget's worth of bytes is in")
		for i := 0; i < 40; i++ {
			slow(budget / 2)
		}
		require.InDelta(t, 64*mib, p.Bandwidth(), 1, "never below an eighth of the start")
	})

	t.Run("fast loads on a saturated node add one core per window", func(t *testing.T) {
		p := &loadPacer{}
		fast := func(bytes float64) {
			for p.tryAcquire(cfg, cpu, bytes, timeout) {
			}
			p.release(cfg, bytes, 10*time.Second, timeout)
		}
		for i := 0; i < 4; i++ {
			fast(budget / 4)
		}
		require.InDelta(t, (512+32)*mib, p.Bandwidth(), mib, "a window of fast loads adds a core's worth")
		for i := 0; i < 4000; i++ {
			fast(budget / 4)
		}
		require.InDelta(t, 8*512*mib, p.Bandwidth(), 1, "never above eight times the start")
	})

	t.Run("the bandwidth settles where loads stay under half the timeout", func(t *testing.T) {
		// A node that really loads at 256 MiB/s, loads sharing it: the
		// bandwidth halves when what is in flight takes longer than half the
		// timeout to drain and creeps up while it does not, so it settles
		// where the bytes in flight are what the node drains in half the
		// timeout: 30 s x 256 MiB/s = 7.5 GiB, a believed bandwidth of twice
		// the real one at the quarter-timeout budget.
		p := &loadPacer{}
		const real = 256 * mib
		for round := 0; round < 400; round++ {
			for p.tryAcquire(cfg, cpu, 512*mib, timeout) {
			}
			elapsed := time.Duration(p.inflightBytes / real * float64(time.Second))
			p.release(cfg, 512*mib, elapsed, timeout)
		}
		require.InDelta(t, 2*real, p.Bandwidth(), 128*mib)
		require.InDelta(t, 7.5*1024*mib, p.budget(timeout), 1024*mib)
	})

	t.Run("a refused load marks the node to be woken", func(t *testing.T) {
		p := &loadPacer{}
		require.False(t, p.takeHeld())
		require.True(t, p.tryAcquire(cfg, cpu, 1024*mib, timeout))
		require.False(t, p.takeHeld(), "nothing refused yet")
		require.False(t, p.tryAcquire(cfg, cpu, 7*1024*mib, timeout))
		require.True(t, p.takeHeld())
		require.False(t, p.takeHeld(), "taken once")
	})
}
