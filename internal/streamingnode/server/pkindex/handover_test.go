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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkerr "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/objpath"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// The tests in this file call prepare and commit of a ready Index directly,
// without the driver, to check the steps of one handover.

// handoverIndex returns a ready Index of v0 on a Channel without the driver.
func handoverIndex(t *testing.T) (*testEnv, *Channel, *Index) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	return env, ch, env.add(ch, "v0")
}

func refIDs(refs []TableRef) []int64 {
	ids := make([]int64, 0, len(refs))
	for _, r := range refs {
		ids = append(ids, int64(r.ID))
	}
	return ids
}

// storeIDs returns the SST IDs of the objects of store under basePath.
func storeIDs(t *testing.T, store *countingStore, basePath string) []int64 {
	var ids []int64
	for _, k := range store.Keys() {
		id, ok := objpath.ParseSSTKey(basePath, k)
		require.True(t, ok, k)
		ids = append(ids, id)
	}
	return ids
}

// TestSSTKeyExtension checks that objpath writes the extension of
// sst.Extension.
func TestSSTKeyExtension(t *testing.T) {
	assert.True(t, strings.HasSuffix(objpath.SSTKey("base", 1), sst.Extension))
}

func TestHandoverNormal(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2, 3)
	ch.Tick(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.request)
	assert.Equal(t, uint64(50), p.covered)
	assert.Empty(t, env.client.requestList())
	assert.Zero(t, idx.lastCommittedTimeTick())
	assert.Equal(t, 0, env.store.Len(), "prepare does not upload")
	assert.True(t, p.uploadedAt.IsZero())

	require.NoError(t, idx.commit(ctx, p))

	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	req := reqs[0]
	assert.Equal(t, uint64(50), req.CheckpointTimeTick)
	assert.Equal(t, testCollectionID, req.CollectionID)
	assert.Equal(t, "v0", req.VChannel)
	assert.Equal(t, testTerm, req.PChannelTerm)
	assert.Equal(t, schemapb.DataType_Int64, req.PKDataType)
	require.NotEmpty(t, req.AddTables)

	for _, ref := range req.AddTables {
		assert.Greater(t, ref.Size, int64(0))
		assert.Equal(t, int64(3), ref.NumEntries)
		assert.NotZero(t, ref.TableFormat)
		assert.Equal(t, codec.EncodeInt64PK(1), ref.MinKey)
		assert.Equal(t, codec.EncodeInt64PK(3), ref.MaxKey)
		path := committedPath(idx.committedDir, ref.ID)
		assert.FileExists(t, path)
		assert.Equal(t, path, p.local[ref.ID])
	}
	addIDs := refIDs(req.AddTables)
	assert.ElementsMatch(t, addIDs, storeIDs(t, env.store, idx.sstBasePath))

	assert.Equal(t, int64(1), idx.currentInstalledVersion(), "the first flush creates version 1")
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, segmentOf(t, idx, pk))
	}
	stats := idx.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Equal(t, len(addIDs), stats.CommittedTables)
	assert.Equal(t, uint64(50), idx.lastCommittedTimeTick())
}

func TestHandoverNothingToPublish(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	idx.lastCommitted.Store(10)
	ch.Tick(20)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Nil(t, p.request)
	assert.Equal(t, uint64(20), p.covered)
	assert.Len(t, p.gens, 1)

	require.NoError(t, idx.commit(ctx, p))

	assert.Empty(t, env.client.requestList())
	assert.Equal(t, 0, env.store.Len())
	assert.Zero(t, idx.currentInstalledVersion())
	assert.Equal(t, uint64(20), idx.lastCommittedTimeTick())
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations)
}

func TestHandoverCommitFailsBeforeApplyThenRetry(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2, 3)
	ch.Tick(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)

	env.client.set(func(c *fakeClient) { c.commitErrs = []error{pkerr.MarkIO(errors.New("disk gone"), "commit")} })
	err = idx.commit(ctx, p)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrIO))
	objects := env.store.Len()
	assert.Equal(t, len(p.request.AddTables), objects)
	assert.Empty(t, env.client.appliedList())
	assert.Equal(t, 1, idx.eng.Stats().FrozenGenerations)
	assert.Zero(t, idx.currentInstalledVersion())
	assert.Zero(t, idx.lastCommittedTimeTick())

	require.NoError(t, idx.commit(ctx, p))

	assert.Equal(t, objects, env.store.Len())
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	assert.Equal(t, []bool{true}, env.client.appliedList())
	assert.Equal(t, uint64(50), env.client.manifest("v0").CheckpointTimeTick)
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations)
	assert.Equal(t, uint64(50), idx.lastCommittedTimeTick())
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, segmentOf(t, idx, pk))
	}
}

// TestHandoverRejectsAManifestThatWentBack checks the safety check after the
// flush: a returned manifest below the installed version, or with a checkpoint
// below the covered time tick, does not have the request in effect. A
// manifest with another SST base path does not list the uploaded tables. It is
// rejected, nothing is installed, and the generations stay frozen.
func TestHandoverRejectsAManifestThatWentBack(t *testing.T) {
	cases := map[string]struct {
		installed int64
		rewrite   func(m ManifestView) ManifestView
	}{
		"version below the installed one": {installed: 100},
		"checkpoint below the covered time tick": {rewrite: func(m ManifestView) ManifestView {
			m.CheckpointTimeTick = 49
			return m
		}},
		"another SST base path": {rewrite: func(m ManifestView) ManifestView {
			m.SSTBasePath = "other/sst"
			return m
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			env, ch, idx := handoverIndex(t)
			env.client.set(func(c *fakeClient) { c.rewrite = tc.rewrite })
			insert(t, idx, 10, 1)
			ch.Tick(50)
			idx.installMu.Lock()
			idx.installedVersion = tc.installed
			idx.installMu.Unlock()

			p, err := idx.prepare(ctx)
			require.NoError(t, err)
			require.NotNil(t, p)
			err = idx.commit(ctx, p)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrRejected))
			assert.Equal(t, 0, idx.eng.Stats().CommittedTables, "nothing is installed")
			assert.Zero(t, idx.lastCommittedTimeTick())
			assert.Equal(t, tc.installed, idx.currentInstalledVersion())
			assert.Equal(t, 1, idx.eng.Stats().FrozenGenerations)
		})
	}
}

// TestHandoverAcceptsTheSameVersion checks that a returned manifest at the
// installed version passes the safety check when its checkpoint covers the
// handover.
func TestHandoverAcceptsTheSameVersion(t *testing.T) {
	ctx := context.Background()
	_, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1)
	ch.Tick(50)
	idx.installMu.Lock()
	idx.installedVersion = 1
	idx.installMu.Unlock()

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, idx.commit(ctx, p))

	assert.Equal(t, int64(1), idx.currentInstalledVersion())
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations, "the generations of the handover retire")
	assert.Equal(t, uint64(50), idx.lastCommittedTimeTick())
}

func TestHandoverWatermarkDidNotMove(t *testing.T) {
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2)
	ch.Tick(30)
	idx.lastCommitted.Store(30)

	p, err := idx.prepare(context.Background())
	require.NoError(t, err)
	assert.Nil(t, p)

	assert.Equal(t, 0, env.store.Len())
	assert.Equal(t, uint64(30), idx.lastCommittedTimeTick())
	stats := idx.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Greater(t, stats.ActiveBytes, uint64(0))
}

func TestLinkCommittedReusesExistingLink(t *testing.T) {
	src := filepath.Join(t.TempDir(), "staged.sst")
	require.NoError(t, os.WriteFile(src, []byte("x"), 0o600))
	dir := filepath.Join(t.TempDir(), "committed")
	s := engine.StagedTable{Info: sst.Info{ID: 42}, Path: src}

	p1, err := linkCommitted(dir, s)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "42.sst"), p1)
	p2, err := linkCommitted(dir, s)
	require.NoError(t, err)
	assert.Equal(t, p1, p2)

	_, err = linkCommitted(dir, engine.StagedTable{Info: sst.Info{ID: 43}, Path: filepath.Join(t.TempDir(), "missing")})
	assert.True(t, errors.Is(err, pkerr.ErrIO))
}

func TestCommitReuploadFailureKeepsThePending(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1)
	ch.Tick(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	old := time.Now().Add(-2 * time.Hour)
	p.uploadedAt = old
	env.store.FailUploads(1)
	err = idx.commit(ctx, p)
	assert.True(t, errors.Is(err, pkerr.ErrIO))
	assert.Empty(t, env.client.requestList(), "no commit after a failed re-upload")
	assert.Equal(t, old, p.uploadedAt)
	assert.Equal(t, 1, idx.eng.Stats().FrozenGenerations)

	require.NoError(t, idx.commit(ctx, p))
	assert.Equal(t, uint64(50), idx.lastCommittedTimeTick())
}
