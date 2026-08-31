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

package resource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const gib = int64(1) << 30

func testGuard(t *testing.T, capacity taskresource.Capacity) *guard {
	t.Helper()
	paramtable.Init()
	g := NewGuard()
	g.setCapacityForTest(capacity)
	return g
}

func TestAcceptRecordsTheCommitment(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Compaction,
		taskresource.Requirement{CPU: 1, Memory: 4 * gib}))

	snap := g.Snapshot()
	assert.Equal(t, 4*gib, snap.Committed.Memory)
	assert.InDelta(t, 1.0, snap.Committed.CPU, 1e-9)
	assert.Equal(t, 48*gib, snap.Capacity.Memory)
	assert.True(t, snap.Admitting)
}

// The defining property: what is reported is the COMMITMENT, made in full the
// instant the task is accepted. A node whose report tracked what the task had
// allocated so far would look empty for as long as its tasks were downloading,
// which is the shape of issue #52180's first incident.
func TestCommitmentIsChargedInFullAtAcceptance(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	for i := int64(1); i <= 8; i++ {
		require.NoError(t, g.Accept(context.Background(), i, taskcommon.Compaction,
			taskresource.Requirement{CPU: 1, Memory: 4 * gib}))
	}

	// Nothing has allocated anything; the ledger still says 32GiB is spoken for.
	assert.Equal(t, 32*gib, g.Snapshot().Committed.Memory)
}

// Nothing is ever refused. Placement was decided by the coordinator from a
// report this node published; re-deciding here is what produced two
// implementations of one contract.
func TestAcceptNeverRefusesEvenBeyondCapacity(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 8, Memory: 4 * gib})

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Index,
		taskresource.Requirement{Memory: 100 * gib}))

	snap := g.Snapshot()
	assert.Equal(t, 100*gib, snap.Committed.Memory)
	// Over-commitment must be VISIBLE rather than clamped: the coordinator
	// needs to tell "exactly full" from "already past full".
	assert.Negative(t, taskresource.Free(snap.Capacity, snap.Committed).Memory)
}

func TestReleaseReturnsTheCommitment(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})
	req := taskresource.Requirement{CPU: 2, Memory: 8 * gib}

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Stats, req))
	g.Release(1)

	assert.Zero(t, g.Snapshot().Committed.Memory)
	assert.Zero(t, g.Snapshot().Committed.CPU)
}

// Release is idempotent, and an unknown id is a no-op. Subtracting twice would
// hand back capacity nobody ever took, which reads to the coordinator as room
// that does not exist.
func TestReleaseIsIdempotent(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Import,
		taskresource.Requirement{Memory: 2 * gib}))
	require.NoError(t, g.Accept(context.Background(), 2, taskcommon.Import,
		taskresource.Requirement{Memory: 2 * gib}))

	g.Release(1)
	g.Release(1)
	g.Release(999)

	assert.Equal(t, 2*gib, g.Snapshot().Committed.Memory)
}

// A re-delivered RPC for a task already running must not charge the node twice
// for one task.
func TestAcceptingTheSameTaskTwiceChargesOnce(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})
	req := taskresource.Requirement{CPU: 1, Memory: 4 * gib}

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Compaction, req))
	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Compaction, req))

	assert.Equal(t, 4*gib, g.Snapshot().Committed.Memory)
}

// CPU requirements are fractional, so subtracting exactly what was added does
// not reliably land on zero. A residue would make an empty node report itself
// as slightly busy forever.
func TestEmptyLedgerReportsExactlyZero(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, g.Accept(context.Background(), i, taskcommon.Import,
			taskresource.Requirement{CPU: 0.1, Memory: 1 << 20}))
	}
	for i := int64(1); i <= 3; i++ {
		g.Release(i)
	}

	assert.Equal(t, 0.0, g.Snapshot().Committed.CPU, "a drained node must report exactly zero, not 2.8e-17")
	assert.Zero(t, g.Snapshot().Committed.Memory)
}

// The safety valve is the ONE thing that can delay a task, and it is not part
// of the arithmetic: it stops everything or nothing.
func TestAcceptWaitsWhileTheSafetyValveIsEngaged(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})
	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		_ = g.Accept(context.Background(), 1, taskcommon.Compaction,
			taskresource.Requirement{Memory: gib})
	}()

	select {
	case <-accepted:
		require.Fail(t, "a frozen node must not take the task")
	case <-time.After(100 * time.Millisecond):
	}
	assert.Zero(t, g.Snapshot().Committed.Memory)
	assert.False(t, g.Snapshot().Admitting)

	// Disengaging must WAKE the waiter rather than leave it until its deadline.
	g.mu.Lock()
	g.frozen = false
	g.thawLocked()
	g.mu.Unlock()

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the task was never taken after the valve disengaged")
	}
	assert.Equal(t, gib, g.Snapshot().Committed.Memory)
}

func TestAcceptHonoursContextWhileFrozen(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 16, Memory: 48 * gib})
	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.Accept(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: gib})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, g.Snapshot().Committed.Memory, "a task that gave up must not be charged")
}

// Concurrent accept/release must leave the ledger exactly balanced. A racy
// counter here shows up as a node that under-reports forever, which the
// coordinator answers by over-placing.
func TestConcurrentAcceptAndReleaseBalance(t *testing.T) {
	g := testGuard(t, taskresource.Capacity{CPU: 64, Memory: 1024 * gib})

	var wg sync.WaitGroup
	for i := int64(0); i < 200; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			require.NoError(t, g.Accept(context.Background(), id, taskcommon.Compaction,
				taskresource.Requirement{CPU: 0.5, Memory: 128 << 20}))
			g.Release(id)
		}(i)
	}
	wg.Wait()

	snap := g.Snapshot()
	assert.Zero(t, snap.Committed.Memory)
	assert.Zero(t, snap.Committed.CPU)
}

func TestSnapshotReportsCapacityFromConfigByDefault(t *testing.T) {
	paramtable.Init()
	g := NewGuard()

	snap := g.Snapshot()
	assert.Equal(t, taskresource.NodeCapacity().Memory, snap.Capacity.Memory)
	assert.True(t, snap.Admitting)
}
