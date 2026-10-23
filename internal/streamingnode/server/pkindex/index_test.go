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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// inHeap reports whether idx is in the LastAcked heap of its PChannelIndexManager.
func inHeap(ch *PChannelIndexManager, idx *index.Index) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.acked.contains(idx)
}

// TestLeftoverInCommittedOnlyIsRemovedAtOpen checks that a file left in the
// committed directory alone, such as a download that did not finish, is
// removed at open, and the Index starts empty.
func TestLeftoverInCommittedOnlyIsRemovedAtOpen(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	committed := filepath.Join(env.root, "v0", "committed")
	require.NoError(t, os.MkdirAll(committed, 0o755))
	// a download that did not finish, and no engine directory
	leftover := filepath.Join(committed, "1001.sst.download-123")
	require.NoError(t, os.WriteFile(leftover, []byte("partial"), 0o600))

	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.NoDirExists(t, committed)
	assert.Empty(t, idx.EngineForTest().FrozenGenerations())
	assert.Zero(t, segmentOf(t, idx, 1))
	assert.Empty(t, env.fatalList())
}

func TestDecideErrorsReachTheCaller(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ctx := context.Background()
	require.NoError(t, idx.EngineForTest().Write(ctx, []engine.Mutation{{Key: codec.EncodeInt64PK(1), Value: []byte{0xff, 0xff}}}))

	// Corrupted data is reported to the caller. The WAL interceptor, not the
	// Index, decides that it makes the WAL unavailable.
	_, err := idx.Authority().Decide(ctx, insertRequest(idx, 1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))
	assert.Empty(t, env.fatalList())

	// The engine of another Index is closed while the Index stays open, so
	// Decide reaches the lookup and it fails with a closed error.
	other := env.add(ch, "v1")
	require.NoError(t, other.EngineForTest().Close())
	_, err = other.Authority().Decide(ctx, insertRequest(other, 2))
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.False(t, errors.Is(err, pkroot.ErrCorrupted))
}

func TestParamsFromConfig(t *testing.T) {
	paramtable.Init()
	p := ParamsFromConfig()
	assert.Equal(t, 12*time.Hour, p.ReuploadAfter, "half of the grace window of the garbage collection")
	assert.EqualValues(t, 4<<20, p.MemTableSize)
}

// TestNotReadyIndexDecidesDegraded checks that an Index whose committed table
// set is not installed treats every key as present: an insert gets a
// companion delete for every key and a delete writes a tombstone for every
// key, so that no record that the install brings back comes back to life.
func TestNotReadyIndexDecidesDegraded(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "v0")
	idx := ch.get("v0")
	ctx := context.Background()
	require.False(t, idx.Ready())

	guard, err := idx.Authority().Decide(ctx, insertRequest(idx, 1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1, 2), guard.Decision().CompanionDeletePKs, "absent keys count as present")
	guard.Apply(ctx, authority.Applied{TimeTick: 5})
	// the observe applies the insert with its companion delete
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Deletes: int64PKs(1, 2), Puts: puts(10, 1, 2), TimeTick: 5}))

	guard, err = idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindDelete, VChannel: idx.VChannel(), PKs: int64PKs(2, 3)})
	require.NoError(t, err)
	assert.Equal(t, int64PKs(2, 3), guard.Decision().PresentPKs, "a tombstone for every key")
	guard.Apply(ctx, authority.Applied{TimeTick: 6})
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Deletes: int64PKs(2, 3), TimeTick: 6}))
	assert.Zero(t, segmentOf(t, idx, 2), "the tombstone removed key 2")

	idx.MarkReady(ctx)
	guard, err = idx.Authority().Decide(ctx, insertRequest(idx, 1, 3))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), guard.Decision().CompanionDeletePKs, "ready: only the present key")
	guard.Discard(ctx, nil)
}
