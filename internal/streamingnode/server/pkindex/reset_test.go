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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

// handoverIndex returns a ready Index of v0 on a PChannelIndexManager without
// the driver, so the tests call the steps of a handover directly.
func handoverIndex(t *testing.T) (*testEnv, *PChannelIndexManager, *index.Index) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	return env, ch, env.add(ch, "v0")
}

func refIDs(refs []index.TableRef) []int64 {
	ids := make([]int64, 0, len(refs))
	for _, r := range refs {
		ids = append(ids, int64(r.ID))
	}
	return ids
}

// presentIn reports whether the Decider of idx holds pk: in the engine or in
// the in-flight set.
func presentIn(t *testing.T, idx *index.Index, pk int64) bool {
	t.Helper()
	present, err := dedup.PresentForTest(context.Background(), idx.Decider(), dedup.Int64PK(pk))
	require.NoError(t, err)
	return present
}

// TestResetForgetsTheKeysBeforeTheTruncate checks that a reset at a truncate
// empties the index, and that the writes after it count again.
func TestResetForgetsTheKeysBeforeTheTruncate(t *testing.T) {
	ctx := context.Background()
	_, ch, idx := handoverIndex(t)
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(7, 1), TimeTick: 3}))
	require.True(t, presentIn(t, idx, 1))

	require.NoError(t, ch.ResetIndex(ctx, "v0", 5))
	assert.False(t, presentIn(t, idx, 1), "the reset forgets the key before the truncate")
	assert.Same(t, idx, ch.IndexOf("v0"), "the reset keeps the Index")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(8, 1), TimeTick: 6}))
	assert.True(t, presentIn(t, idx, 1))
	assert.False(t, presentIn(t, idx, 2))
	assert.Equal(t, uint64(5), scheduleOf(ch, idx).flushTT.Load(), "the reset asks for a handover")
}

// TestResetKeepsTheInFlightWritesAfterTheTruncate checks that a write that
// the append path decided after the truncate stays present: the reset keeps
// the Decider and its in-flight set.
func TestResetKeepsTheInFlightWritesAfterTheTruncate(t *testing.T) {
	ctx := context.Background()
	_, ch, idx := handoverIndex(t)
	intent, err := idx.Decider().Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(9)})
	require.NoError(t, err)
	intent.Appended(6)

	require.NoError(t, ch.ResetIndex(ctx, "v0", 5))
	assert.True(t, presentIn(t, idx, 9), "the in-flight write after the truncate stays")
}

// TestResetAtOrBeforeTheCommittedCheckpointDoesNothing checks the skip rule
// of a replayed truncate whose reset is committed.
func TestResetAtOrBeforeTheCommittedCheckpointDoesNothing(t *testing.T) {
	ctx := context.Background()
	_, ch, idx := handoverIndex(t)
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(7, 1), TimeTick: 30}))
	idx.SetLastCommittedForTest(20)

	dirtied, err := idx.Reset(ctx, 20)
	require.NoError(t, err)
	assert.False(t, dirtied)
	assert.True(t, presentIn(t, idx, 1))
	assert.Nil(t, idx.PendingForTest())
}

// TestHandoverCommitsAResetWithoutNewWrites checks that a reset makes the
// next handover send a flush that replaces the table set, even with no
// table.
func TestHandoverCommitsAResetWithoutNewWrites(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, ch, idx, 10, 1, 2)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	require.True(t, handover(t, idx))
	require.NotEmpty(t, env.client.manifest("v0").Tables)
	before := idx.LastCommittedTimeTick()

	require.NoError(t, ch.ResetIndex(ctx, "v0", 15))
	assert.Equal(t, before, ch.LastAcked(), "the reset holds LastAcked until it commits")
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	assert.Nil(t, p, "the handover waits until the observe passes the truncate")

	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.Equal(t, before, ch.LastAcked(), "the reset holds LastAcked until it commits")
	require.True(t, handover(t, idx))
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, uint64(15), reqs[1].ResetTimeTick)
	assert.Empty(t, reqs[1].AddTables)
	assert.Equal(t, uint64(20), reqs[1].CheckpointTimeTick)
	m := env.client.manifest("v0")
	assert.Empty(t, m.Tables, "the reset empties the manifest")
	assert.Equal(t, uint64(20), idx.LastCommittedTimeTick())
	assert.Zero(t, idx.EngineForTest().Stats().CommittedTables)
	assert.False(t, presentIn(t, idx, 1))
	assert.Equal(t, uint64(20), ch.LastAcked())

	// the next handover is a plain flush again
	insert(t, ch, idx, 11, 3)
	ch.ObserveTimeTick(30, WALTailSnapshot{})
	require.True(t, handover(t, idx))
	reqs = env.client.requestList()
	require.Len(t, reqs, 3)
	assert.Zero(t, reqs[2].ResetTimeTick)
	assert.Equal(t, []int64{refIDs(reqs[2].AddTables)[0]}, sstIDs(env.client.manifest("v0")))
}

// TestResetDuringACommitDropsItsResult checks a handover whose flush is in
// flight when the reset runs: it installs nothing and does not move
// LastCommittedTimeTick, and the next handover commits the reset.
func TestResetDuringACommitDropsItsResult(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, ch, idx, 10, 1, 2)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	env.client.set(func(c *FakeCommitter) { c.commitEntered, c.commitBlock = entered, release })

	done := make(chan bool, 1)
	go func() { done <- handover(t, idx) }()
	select {
	case <-entered:
	case <-time.After(waitTimeout):
		t.Fatal("the flush did not start")
	}
	require.NoError(t, ch.ResetIndex(ctx, "v0", 15))
	close(release)
	<-done
	env.client.set(func(c *FakeCommitter) { c.commitEntered, c.commitBlock = nil, nil })

	assert.Zero(t, idx.LastCommittedTimeTick(), "the result of the flush before the reset is dropped")
	assert.Zero(t, idx.EngineForTest().Stats().CommittedTables)
	assert.False(t, presentIn(t, idx, 1))
	assert.Nil(t, idx.PendingForTest())

	ch.ObserveTimeTick(20, WALTailSnapshot{})
	require.True(t, handover(t, idx))
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, uint64(15), reqs[1].ResetTimeTick)
	assert.Empty(t, env.client.manifest("v0").Tables)
	assert.Equal(t, uint64(20), idx.LastCommittedTimeTick())
}

// TestResetBeforeTheCommitOfAPreparedHandoverSendsNothing checks a prepared
// handover that a reset makes stale before its flush.
func TestResetBeforeTheCommitOfAPreparedHandoverSendsNothing(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, ch, idx, 10, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)

	require.NoError(t, ch.ResetIndex(ctx, "v0", 15))
	require.NoError(t, idx.CommitForTest(ctx, p))
	assert.Empty(t, env.client.requestList())
	assert.Zero(t, idx.LastCommittedTimeTick())
}
