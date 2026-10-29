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

package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

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
		id, ok := pkroot.ParseSSTKey(basePath, k)
		require.True(t, ok, k)
		ids = append(ids, id)
	}
	return ids
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
	assert.True(t, errors.Is(err, pkroot.ErrIO))
}

func TestHandoverNormal(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2, 3)
	idx.observed.Store(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.request)
	assert.Equal(t, uint64(50), p.covered)
	assert.Empty(t, idx.committer.requestList())
	assert.Zero(t, idx.LastCommittedTimeTick())
	assert.Equal(t, 0, idx.store.Len(), "prepare does not upload")
	assert.True(t, p.uploadedAt.IsZero())

	require.NoError(t, idx.uploadAndCommit(ctx, p))

	reqs := idx.committer.requestList()
	require.Len(t, reqs, 1)
	req := reqs[0]
	assert.Equal(t, uint64(50), req.CheckpointTimeTick)
	assert.Equal(t, testCollectionID, req.CollectionID)
	assert.Equal(t, testVChannel, req.VChannel)
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
	assert.ElementsMatch(t, addIDs, storeIDs(t, idx.store, idx.sstBasePath))

	assert.Equal(t, int64(1), idx.currentInstalledVersion(), "the first flush creates version 1")
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, idx.segmentOf(t, pk))
	}
	stats := idx.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Equal(t, len(addIDs), stats.CommittedTables)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

// TestFirstHandoverCreatesTheManifest checks that a ready Index uploads under
// the SST base path that it computes, and creates the manifest with its first
// handover.
func TestFirstHandoverCreatesTheManifest(t *testing.T) {
	idx := newTestIndex(t)
	assert.Equal(t, "root/pk_index/100/v0/sst", idx.sstBasePath)

	idx.insert(t, 10, 1)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	reqs := idx.committer.requestList()
	require.Len(t, reqs, 1)
	assert.Equal(t, schemapb.DataType_Int64, reqs[0].PKDataType)
	m := idx.committer.manifest()
	assert.EqualValues(t, 1, m.Version)
	assert.Equal(t, idx.sstBasePath, m.SSTBasePath)
	keys := idx.store.Keys()
	for _, ref := range m.Tables {
		assert.Contains(t, keys, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)))
	}
}

func TestTwoHandoversInOneProcess(t *testing.T) {
	idx := newTestIndex(t)

	idx.insert(t, 10, 1, 2, 3)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	first := idx.committer.manifest()
	require.EqualValues(t, 1, first.Version, "the first flush creates the manifest")
	require.EqualValues(t, 10, first.CheckpointTimeTick)
	require.NotEmpty(t, first.Tables)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations())

	idx.insert(t, 20, 4, 5)
	idx.observed.Store(20)
	require.True(t, idx.handover(t))
	second := idx.committer.manifest()
	require.EqualValues(t, 2, second.Version)
	// the second install holds the tables of the first, which come from installedPaths
	assert.Subset(t, sstIDs(second), sstIDs(first))
	assert.Equal(t, len(second.Tables), idx.eng.Stats().CommittedTables)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
	assert.EqualValues(t, 10, idx.segmentOf(t, 1))
	assert.EqualValues(t, 20, idx.segmentOf(t, 4))
	assert.Zero(t, idx.store.downloads.Load())
	assert.False(t, idx.Dirty())
}

func TestHandoverNothingToPublish(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.lastCommitted.Store(10)
	idx.observed.Store(20)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Nil(t, p.request)
	assert.Equal(t, uint64(20), p.covered)
	assert.Len(t, p.gens, 1)

	idx.pending.Store(p)
	committed, err := idx.Index.handover(ctx)
	require.NoError(t, err)
	require.True(t, committed)

	assert.Empty(t, idx.committer.requestList())
	assert.Equal(t, 0, idx.store.Len())
	assert.Zero(t, idx.currentInstalledVersion())
	assert.Equal(t, uint64(20), idx.LastCommittedTimeTick())
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations)
}

func TestHandoverCommitFailsBeforeApplyThenRetry(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2, 3)
	idx.observed.Store(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)

	idx.committer.set(func(c *fakeCommitter) { c.commitErrs = []error{pkroot.MarkIO(errors.New("disk gone"), "commit")} })
	err = idx.uploadAndCommit(ctx, p)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrIO))
	objects := idx.store.Len()
	assert.Equal(t, len(p.request.AddTables), objects)
	assert.Empty(t, idx.committer.appliedList())
	assert.Equal(t, 1, idx.eng.Stats().FrozenGenerations)
	assert.Zero(t, idx.currentInstalledVersion())
	assert.Zero(t, idx.LastCommittedTimeTick())

	require.NoError(t, idx.uploadAndCommit(ctx, p))

	assert.Equal(t, objects, idx.store.Len())
	reqs := idx.committer.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	assert.Equal(t, []bool{true}, idx.committer.appliedList())
	assert.Equal(t, uint64(50), idx.committer.manifest().CheckpointTimeTick)
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, idx.segmentOf(t, pk))
	}
}

func TestLostResponseRetriesTheSameRequest(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	idx.committer.set(func(c *fakeCommitter) { c.loseNext = true })
	committed, err := idx.Handover(ctx)
	require.Error(t, err)
	assert.False(t, committed)
	assert.False(t, IsFatal(err))
	require.NotNil(t, idx.pending.Load())
	assert.Len(t, idx.eng.FrozenGenerations(), 1)

	// new writes and a new tick do not change the retried request
	idx.insert(t, 20, 3)
	idx.observed.Store(20)
	require.True(t, idx.handover(t))
	reqs := idx.committer.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	// the second request took the applied retry branch of the manifest service
	assert.Equal(t, []bool{true, false}, idx.committer.appliedList())
	assert.EqualValues(t, 1, idx.committer.manifest().Version)
	assert.Nil(t, idx.pending.Load())
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations())
	assert.True(t, idx.Dirty(), "the write after the rotate is not committed")

	require.True(t, idx.handover(t))
	reqs = idx.committer.requestList()
	require.Len(t, reqs, 3)
	assert.EqualValues(t, 20, reqs[2].CheckpointTimeTick)
	assert.EqualValues(t, 20, idx.LastCommittedTimeTick())
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
			idx := newTestIndex(t)
			idx.committer.set(func(c *fakeCommitter) { c.rewrite = tc.rewrite })
			idx.insert(t, 10, 1)
			idx.observed.Store(50)
			idx.installedVersion = tc.installed

			p, err := idx.prepare(ctx)
			require.NoError(t, err)
			require.NotNil(t, p)
			err = idx.uploadAndCommit(ctx, p)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrRejected))
			assert.Equal(t, 0, idx.eng.Stats().CommittedTables, "nothing is installed")
			assert.Zero(t, idx.LastCommittedTimeTick())
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
	idx := newTestIndex(t)
	idx.insert(t, 10, 1)
	idx.observed.Store(50)
	idx.installedVersion = 1

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, idx.uploadAndCommit(ctx, p))

	assert.Equal(t, int64(1), idx.currentInstalledVersion())
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations, "the generations of the handover retire")
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

func TestHandoverLastObservedDidNotMove(t *testing.T) {
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(30)
	idx.lastCommitted.Store(30)

	p, err := idx.prepare(context.Background())
	require.NoError(t, err)
	assert.Nil(t, p)

	assert.Equal(t, 0, idx.store.Len())
	assert.Equal(t, uint64(30), idx.LastCommittedTimeTick())
	stats := idx.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Greater(t, stats.ActiveBytes, uint64(0))
}

func TestUploadFailuresKeepThePending(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	idx.store.FailUploads(3)

	_, err := idx.Handover(ctx)
	assert.True(t, errors.Is(err, pkroot.ErrIO))
	p := idx.pending.Load()
	require.NotNil(t, p, "the pending is recorded before the upload")
	gens := idx.eng.FrozenGenerations()
	require.Len(t, gens, 1)
	for i := 0; i < 2; i++ {
		idx.insert(t, 20, int64(3+i))
		idx.observed.Store(uint64(20 + i))
		_, err = idx.Handover(ctx)
		assert.True(t, errors.Is(err, pkroot.ErrIO))
		assert.Same(t, p, idx.pending.Load())
		assert.Equal(t, gens, idx.eng.FrozenGenerations(), "a failed upload does not rotate again")
	}
	assert.Empty(t, idx.committer.requestList())

	require.True(t, idx.handover(t))
	assert.Nil(t, idx.pending.Load())
	reqs := idx.committer.requestList()
	require.Len(t, reqs, 1)
	assert.EqualValues(t, 10, reqs[0].CheckpointTimeTick)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
	assert.Empty(t, idx.eng.FrozenGenerations(), "exactly one rotate happened")
	assert.Equal(t, len(reqs[0].AddTables), idx.store.Len())
	assert.EqualValues(t, 10, idx.segmentOf(t, 1))
}

func TestCommitReuploadFailureKeepsThePending(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1)
	idx.observed.Store(50)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	old := time.Now().Add(-2 * time.Hour)
	p.uploadedAt = old
	idx.store.FailUploads(1)
	err = idx.upload(ctx, p)
	assert.True(t, errors.Is(err, pkroot.ErrIO))
	assert.Empty(t, idx.committer.requestList(), "no commit after a failed re-upload")
	assert.Equal(t, old, p.uploadedAt)
	assert.Equal(t, 1, idx.eng.Stats().FrozenGenerations)

	require.NoError(t, idx.uploadAndCommit(ctx, p))
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

func TestRetryReuploadsOldTables(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	idx.committer.set(func(c *fakeCommitter) {
		c.commitErrs = []error{merr.WrapErrServiceUnavailable("down"), merr.WrapErrServiceUnavailable("down")}
	})
	_, err := idx.Handover(ctx)
	require.Error(t, err)
	p := idx.pending.Load()
	require.NotNil(t, p)
	uploads := idx.store.uploads.Load()
	objects := idx.store.Len()
	keys := idx.store.Keys()
	require.EqualValues(t, len(p.request.AddTables), uploads)

	// a recent upload is not repeated
	_, err = idx.Handover(ctx)
	require.Error(t, err)
	require.Same(t, p, idx.pending.Load())
	assert.Equal(t, uploads, idx.store.uploads.Load())

	idx.params.Store(&Params{ReuploadAfter: 0})
	require.True(t, idx.handover(t))
	assert.Nil(t, idx.pending.Load())
	assert.EqualValues(t, uploads+int64(len(p.request.AddTables)), idx.store.uploads.Load())
	assert.Equal(t, objects, idx.store.Len())
	assert.Equal(t, keys, idx.store.Keys(), "the upload repeats under the same keys")
	reqs := idx.committer.requestList()
	require.Len(t, reqs, 3)
	assert.Equal(t, reqs[0], reqs[2])
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick())
}

// TestHandoverRetiresSeveralEmptyGenerationsInOneCall checks that a handover
// with nothing to publish drops every frozen generation in one engine call:
// the engine retires only the oldest frozen generations as a whole.
func TestHandoverRetiresSeveralEmptyGenerationsInOneCall(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	// a generation left frozen by an earlier failed handover
	_, err := idx.eng.RotateIncrement(ctx)
	require.NoError(t, err)
	idx.lastCommitted.Store(10)
	idx.observed.Store(20)

	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Nil(t, p.request)
	require.Len(t, p.gens, 2)

	idx.pending.Store(p)
	committed, err := idx.Index.handover(ctx)
	require.NoError(t, err)
	require.True(t, committed)
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations)
	assert.Equal(t, uint64(20), idx.LastCommittedTimeTick())
}

// TestRetireCoveredDropsSeveralGenerationsInOneCall checks that the
// generations an older manifest covers are dropped in one engine call.
func TestRetireCoveredDropsSeveralGenerationsInOneCall(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1)
	_, err := idx.eng.RotateIncrement(ctx)
	require.NoError(t, err)
	idx.insert(t, 10, 2)
	_, err = idx.eng.RotateIncrement(ctx)
	require.NoError(t, err)
	gens := idx.eng.FrozenGenerations()
	require.Len(t, gens, 2)

	idx.installedVersion = 5
	require.NoError(t, idx.installIfNewer(ctx, ManifestView{Version: 3}, nil, gens...))
	assert.Equal(t, 0, idx.eng.Stats().FrozenGenerations, "an older manifest retires the generations it covers")
}

// uploadAndCommit runs the upload and the commit of a prepared handover, the
// two steps that handover runs after prepare.
func (idx *testIndex) uploadAndCommit(ctx context.Context, p *pendingHandover) error {
	if err := idx.upload(ctx, p); err != nil {
		return err
	}
	return idx.commit(ctx, p)
}
