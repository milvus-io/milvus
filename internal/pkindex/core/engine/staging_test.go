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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
)

// Re-flushing a generation must produce the very same tables under the very
// same IDs. That is what makes an upload retried after a failure, or after a
// restart, idempotent: it writes the same objects again.
func TestFlushDrainingIsIdempotent(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{put(pk(1), 10), put(pk(2), 20)}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)

	first, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	assert.Equal(t, first, second, "a second flush must return the same tables")

	// without the completion marker the staging is the debris of an
	// interrupted attempt, so it is redone under fresh IDs
	marker := filepath.Join(e.stagedDir(gen), stagedManifestName)
	require.NoError(t, os.Remove(marker))
	third, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.Len(t, third, len(first))
	assert.NotEqual(t, first[0].Info.ID, third[0].Info.ID,
		"an interrupted staging must not be mistaken for a finished one")
}

// The engine owns the staging: a publisher can read a flushed table until the
// generation retires, and must have taken its own copy by then.
func TestFlushedTableReadableUntilRetired(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{put(pk(1), 10)}))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	flushed, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, flushed)

	r, err := sst.OpenReader(flushed[0].Path, nil, sst.ExpectSize(flushed[0].Info.Size))
	require.NoError(t, err)
	v, ok, err := r.Get(pk(1))
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, v)
	require.NoError(t, r.Close())

	stagedDir := e.stagedDir(gen)
	_, err = os.Stat(stagedDir)
	require.NoError(t, err)

	require.NoError(t, e.DropDraining(ctx, gen))
	_, err = os.Stat(stagedDir)
	assert.True(t, os.IsNotExist(err), "retiring a generation removes its staging")
}
