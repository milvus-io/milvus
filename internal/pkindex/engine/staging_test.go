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

package engine

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// Re-flushing a generation must produce the very same tables under the very
// same IDs. That is what makes an upload retried after a failure, or after a
// restart, idempotent: it writes the same objects again.
func TestStageFrozenIsIdempotent(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{put(pk(1), 10), put(pk(2), 20)}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)

	first, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	assert.Equal(t, first, second, "a second flush must return the same tables")

	// without the completion marker the staging is the debris of an
	// interrupted attempt, so it is redone under fresh IDs
	marker := filepath.Join(stagedDir(e.cfg.Dir, gen), stagedListName)
	require.NoError(t, os.Remove(marker))
	third, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.Len(t, third, len(first))
	assert.NotEqual(t, first[0].Info.ID, third[0].Info.ID,
		"an interrupted staging must not be mistaken for a finished one")
}

// The engine owns the staging: a publisher can read a flushed table until the
// generation retires, and must have taken its own copy by then.
func TestStagedTableReadableUntilRetired(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{put(pk(1), 10)}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	flushed, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, flushed)

	r, err := sst.OpenReader(flushed[0].Path, nil, sst.ExpectSize(flushed[0].Info.Size))
	require.NoError(t, err)
	v, ok, err := r.Get(pk(1))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, v)
	require.NoError(t, r.Close())

	staged := stagedDir(e.cfg.Dir, gen)
	_, err = os.Stat(staged)
	require.NoError(t, err)

	require.NoError(t, e.DropFrozen(ctx, gen))
	_, err = os.Stat(staged)
	assert.True(t, os.IsNotExist(err), "retiring a generation removes its staging")
}

// A generation above the size cap is staged as several tables whose key
// ranges follow each other without overlap.
func TestStageFrozenSplitsLargeGeneration(t *testing.T) {
	old := stagedTableTargetBytes
	stagedTableTargetBytes = 4 << 10
	defer func() { stagedTableTargetBytes = old }()

	e := newTestEngine(t)
	ctx := context.Background()
	const n = 2000
	muts := make([]Mutation, 0, n)
	for i := int64(0); i < n; i++ {
		muts = append(muts, put(pk(i), i))
	}
	require.NoError(t, e.Write(ctx, muts))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	staged, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	require.Greater(t, len(staged), 1)

	var total int64
	for i, s := range staged {
		total += s.Info.NumEntries
		if i > 0 {
			assert.Negative(t, bytes.Compare(staged[i-1].Info.MaxKey, s.Info.MinKey),
				"table %d must start after table %d ends", i, i-1)
		}
	}
	assert.Equal(t, int64(n), total)
}

// The staged tables, once installed in place of the generation, must answer
// every lookup the same way the generation did.
func TestStagedTablesAnswerLikeTheGeneration(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 5; round++ {
		muts := make([]Mutation, 0, 200)
		for i := 0; i < 200; i++ {
			key := pk(rng.Int63n(500))
			if rng.Intn(3) == 0 {
				muts = append(muts, del(key))
			} else {
				muts = append(muts, put(key, rng.Int63n(1<<20)))
			}
		}
		require.NoError(t, e.Write(ctx, muts))
		require.NoError(t, e.active.db.Flush())
	}

	keys := make([][]byte, 0, 600)
	for i := int64(0); i < 600; i++ {
		keys = append(keys, pk(i))
	}
	before, err := e.MultiGet(ctx, keys)
	require.NoError(t, err)

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	staged, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	// links taken first, because retiring the generation removes its staging
	commitTables(t, e, t.TempDir(), staged, nil, gen)
	require.Empty(t, e.FrozenGenerations())

	after, err := e.MultiGet(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// A staging that fails part way writes no STAGED, and the next attempt
// starts over and completes.
func TestStageFrozenFailureLeavesNoStagedList(t *testing.T) {
	old := stagedTableTargetBytes
	stagedTableTargetBytes = 4 << 10
	defer func() { stagedTableTargetBytes = old }()

	var nextID, failAt atomic.Int64
	nextID.Store(1000)
	failAt.Store(1002) // the second table of the first attempt
	e := newTestEngine(t, func(c *Config) {
		c.AllocID = func(context.Context) (int64, error) {
			id := nextID.Add(1)
			if id == failAt.Load() {
				return 0, errors.New("injected id allocation failure")
			}
			return id, nil
		}
	})
	ctx := context.Background()
	muts := make([]Mutation, 0, 2000)
	for i := int64(0); i < 2000; i++ {
		muts = append(muts, put(pk(i), i))
	}
	require.NoError(t, e.Write(ctx, muts))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)

	_, err = e.StageFrozen(ctx, gen)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(stagedDir(e.cfg.Dir, gen), stagedListName))
	require.True(t, os.IsNotExist(err), "a failed staging must not write STAGED")

	failAt.Store(-1)
	staged, err := e.StageFrozen(ctx, gen)
	require.NoError(t, err)
	var total int64
	for _, s := range staged {
		require.Greater(t, int64(s.Info.ID), int64(1002), "the retry must use new IDs")
		total += s.Info.NumEntries
	}
	assert.Equal(t, int64(2000), total)
}
