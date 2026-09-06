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

package walsummary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
)

// TestRestoreInheritsPreviousTermChunks covers the term-handoff path: a chunk
// the previous term persisted (its handles released, the WAL checkpoint may
// have passed it) but whose records were never materialized must stay visible
// to the next term. Without the inheritance the new term's empty manifest
// would hide the delete records forever, resurrecting deleted data.
func TestRestoreInheritsPreviousTermChunks(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	// Term 1 flushes one delete into chunk 0 and publishes its manifest.
	store1 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager1 := newTestManager(t, store1, 1, 1<<30)
	require.NoError(t, manager1.Restore(ctx, nil))
	var unused bool
	flushObserved(t, manager1, "v1", 100, &unused)
	assert.Len(t, manager1.manifest.GetChunks(), 1)

	// Term 2 takes over the pchannel (term handoff) and restores. It must see
	// term 1's chunk through its own manifest.
	store2 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 2)
	manager2 := newTestManager(t, store2, 1, 1<<30)
	require.NoError(t, manager2.Restore(ctx, nil))
	require.Len(t, manager2.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(0), manager2.manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, int64(1), manager2.manifest.GetChunks()[0].GetTerm())
	assert.Equal(t, uint64(1), manager2.nextGeneration, "generations continue past the inherited set")

	// The durable backlog through the inherited chunk is readable: recovery of
	// the transform consumer loads the un-materialized delete from it.
	entries, err := manager2.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())

	// Term 2 sealed the inheritance into its own manifest, so the chain never
	// grows beyond one hop: a term 3 restore reads term 2's manifest.
	loaded, found, err := store2.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	assert.Len(t, loaded.GetChunks(), 1)
	assert.Equal(t, int64(1), loaded.GetChunks()[0].GetTerm())
}

// TestRestoreInheritsPreviousTermProbedTail covers the crash window of the
// previous term: a chunk written but not yet recorded in the manifest before
// the handoff. The new term's restore must probe the previous term's objects
// and seal them, exactly as it probes its own.
func TestRestoreInheritsPreviousTermProbedTail(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	// Term 1 writes chunk 0 but the manifest publish "crashes" (we simply do
	// not publish).
	store1 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager1 := newTestManager(t, store1, 1, 1<<30)
	require.NoError(t, manager1.Restore(ctx, nil))
	var unused bool
	flushObserved(t, manager1, "v1", 100, &unused)
	require.Len(t, manager1.manifest.GetChunks(), 1)

	// Term 2 restores: it finds no manifest of its own, then probes term 1.
	store2 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 2)
	manager2 := newTestManager(t, store2, 1, 1<<30)
	require.NoError(t, manager2.Restore(ctx, nil))
	require.Len(t, manager2.manifest.GetChunks(), 1)
	assert.Equal(t, int64(1), manager2.manifest.GetChunks()[0].GetTerm())
}

// TestRestoreInheritsSkipsBurnedIntermediateTerm covers chained handoffs: a
// term that was assigned (TryAssignToServerID burns a term on every
// assignment attempt) but died before ever sealing a manifest leaves an empty
// manifest at term-1. Restore must keep walking back until it finds the most
// recent term that actually holds chunks — otherwise the records of an older
// term vanish from the manifest chain, their deletes silently resurrect, and
// the orphaned chunk objects become unreachable to GC.
func TestRestoreInheritsSkipsBurnedIntermediateTerm(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	// Term 1 flushes one delete into chunk 0 and publishes its manifest.
	store1 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager1 := newTestManager(t, store1, 1, 1<<30)
	require.NoError(t, manager1.Restore(ctx, nil))
	var unused bool
	flushObserved(t, manager1, "v1", 100, &unused)
	require.Len(t, manager1.manifest.GetChunks(), 1)

	// Term 2 was assigned and burned without ever restoring (a failed open):
	// no manifest and no chunk of it exist. A term-3 restore must walk past
	// the empty term 2 down to term 1.

	// Term 3 restores: term 2 left no trace, so the inheritance walk must keep
	// going back to term 1 and adopt its chunk.
	store3 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 3)
	manager3 := newTestManager(t, store3, 1, 1<<30)
	require.NoError(t, manager3.Restore(ctx, nil))
	require.Len(t, manager3.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(0), manager3.manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, int64(1), manager3.manifest.GetChunks()[0].GetTerm())
	assert.Equal(t, uint64(1), manager3.nextGeneration, "generations continue past the inherited set")

	// The delete of term 1 is reachable through term 3's sealed manifest.
	entries, err := manager3.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())
}
