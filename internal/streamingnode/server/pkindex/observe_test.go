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

package pkindex

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

func TestObserveTimeTickOnlyMovesLastObservedForward(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(7)
	assert.EqualValues(t, 7, ch.LastObserved(), "the seed is the global checkpoint")
	ch.ObserveTimeTick(20)
	assert.EqualValues(t, 20, ch.LastObserved())
	ch.ObserveTimeTick(15)
	assert.EqualValues(t, 20, ch.LastObserved(), "never goes back")
}

func TestManagerApplyAddsADirtiedIndexToTheHeap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.ObserveTimeTick(10)
	assert.EqualValues(t, 10, ch.LastAcked(), "a clean Index follows lastObserved")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 11}))
	ch.ObserveTimeTick(12)
	assert.EqualValues(t, 0, ch.lastAckedUnclamped(), "a dirty Index holds LastAcked at its last commit")
	assert.Contains(t, ch.dirtyIndexes(), idx)

	require.NoError(t, ch.Apply(ctx, "no-such-vchannel", index.Write{Puts: puts(1, 1), TimeTick: 11}), "a vchannel without an Index is ignored")
}

func TestCoveredIsAtMostLastObservedAtTheRotate(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 5}))
	ch.ObserveTimeTick(10)
	// a write the observe applied after the TimeTick, not covered by lastObserved yet
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, 2), TimeTick: 15}))

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.EqualValues(t, 10, p.Covered(), "covered is the lastObserved read before the rotate")
	assert.LessOrEqual(t, p.Covered(), ch.LastObserved())
	assert.Equal(t, 1, idx.EngineForTest().Stats().FrozenGenerations)
	assert.Zero(t, idx.EngineForTest().Stats().ActiveBytes, "the write above covered sits in the frozen generation")
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
	require.NoError(t, idx.CommitForTest(ctx, p))

	// a restart replays everything above the checkpoint, so the write above
	// covered is applied again: same key, same value
	idx.SetLastCommittedForTest(10)
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, 2), TimeTick: 15}))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

// TestASlowStageDoesNotBlockTheObserve: StageFrozen writes the whole
// generation to a table under the cycle lock of the engine. A big generation
// makes it take long enough to overlap the observe calls below, which must
// not wait for it: Apply and ObserveTimeTick never take that lock.
func TestASlowStageDoesNotBlockTheObserve(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	pks := make([]int64, 300000)
	for i := range pks {
		pks[i] = int64(i)
	}
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, pks...), TimeTick: 5}))
	ch.ObserveTimeTick(10)

	staged := make(chan struct{})
	go func() {
		defer close(staged)
		_, err := idx.PrepareForTest(ctx) // RotateIncrement + StageFrozen
		assert.NoError(t, err)
	}()
	var worst time.Duration
	overlapped := false
	for i := 0; i < 200; i++ {
		start := time.Now()
		require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, int64(1000000+i)), TimeTick: uint64(20 + i)}))
		ch.ObserveTimeTick(uint64(20 + i))
		if d := time.Since(start); d > worst {
			worst = d
		}
		select {
		case <-staged:
		default:
			overlapped = true
		}
	}
	<-staged
	assert.True(t, overlapped, "the stage must still run while the observe calls happen; grow the generation if not")
	assert.Less(t, worst, 100*time.Millisecond, "an observe call waited for the stage")
}

// TestManagerApplyIgnoresADroppedIndex: the observe looks the Index up and
// then writes it. A DropCollection in between closes the Index, and the
// write is moot: Apply reports no error, so lastObserved keeps moving. A
// closed Index that is still attached reports the closed engine.
func TestManagerApplyIgnoresADroppedIndex(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	dropped := env.add(ch, "v0")
	require.NoError(t, ch.DropCollection(ctx, "v0"))
	require.NoError(t, ch.apply(ctx, dropped, index.Write{Puts: puts(1, 1), TimeTick: 5}))

	closed := env.add(ch, "v1")
	closed.Close(ctx)
	err := ch.Apply(ctx, "v1", index.Write{Puts: puts(1, 1), TimeTick: 5})
	require.True(t, errors.Is(err, engine.ErrClosed), "%v", err)
}
