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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

func TestApplySkipsWritesAtOrBelowTheCommittedCheckpoint(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	idx.SetLastCommittedForTest(10)

	dirtied, err := idx.Apply(ctx, index.Write{Puts: puts(5, 1), TimeTick: 10})
	require.NoError(t, err)
	assert.False(t, dirtied)
	assert.Zero(t, segmentOf(t, idx, 1), "a write at the checkpoint is in the committed tables already")

	dirtied, err = idx.Apply(ctx, index.Write{Puts: puts(5, 1), TimeTick: 11})
	require.NoError(t, err)
	assert.True(t, dirtied, "the first write since the last rotate")
	assert.EqualValues(t, 5, segmentOf(t, idx, 1))

	dirtied, err = idx.Apply(ctx, index.Write{Puts: puts(5, 2), TimeTick: 12})
	require.NoError(t, err)
	assert.False(t, dirtied, "the Index was dirty already")

	dirtied, err = idx.Apply(ctx, index.Write{TimeTick: 13})
	require.NoError(t, err)
	assert.False(t, dirtied, "an empty write changes nothing")
}

func TestApplyWritesDeletesBeforePuts(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	// an insert of pk 1 with its companion delete: the same key in Deletes and Puts
	_, err := idx.Apply(ctx, index.Write{Deletes: int64PKs(1), Puts: puts(7, 1), TimeTick: 5})
	require.NoError(t, err)
	assert.EqualValues(t, 7, segmentOf(t, idx, 1), "the put survives the delete of the same batch")

	// a plain delete writes a tombstone, also for a key the index never saw
	_, err = idx.Apply(ctx, index.Write{Deletes: int64PKs(1, 9), TimeTick: 6})
	require.NoError(t, err)
	assert.Zero(t, segmentOf(t, idx, 1))
	assert.Zero(t, segmentOf(t, idx, 9))
	values, err := idx.EngineForTest().MultiGet(ctx, [][]byte{int64PKs(9)[0].Encode()})
	require.NoError(t, err)
	assert.Nil(t, values[0], "a tombstone reads as absent")
}

func TestApplyOnAClosedIndexReturnsClosed(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	require.NoError(t, idx.EngineForTest().Close())
	_, err := idx.Apply(ctx, index.Write{Puts: puts(1, 1), TimeTick: 1})
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestApplyPutsIntoSeveralSegments(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	// the bodies of one transaction insert into two segments, in one Write
	w := index.Write{Puts: append(append(puts(1, 1), puts(2, 2)...), puts(1, 3)...), TimeTick: 5}
	_, err := idx.Apply(ctx, w)
	require.NoError(t, err)
	assert.EqualValues(t, 1, segmentOf(t, idx, 1))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
	assert.EqualValues(t, 1, segmentOf(t, idx, 3))
}
