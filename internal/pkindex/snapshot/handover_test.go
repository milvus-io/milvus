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

package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/objpath"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot/snapshottest"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	testCollectionID = int64(100)
	testVChannel     = "test-dml_0_100v0"
	testTerm         = int64(3)
	testSSTBasePath  = "root/pkindex/100/test-dml_0_100v0/sst"
)

// fakeTarget is the vchannel side of a handover, over a real engine.
type fakeTarget struct {
	t            *testing.T
	eng          *engine.Engine
	committedDir string

	lastApplied   uint64
	lastCommitted uint64
	installed     int64

	setCommittedCalls []uint64
	installCalls      []installCall
}

type installCall struct {
	manifest ManifestView
	local    map[sst.ID]string
	gens     []engine.Generation
}

func newFakeTarget(t *testing.T) *fakeTarget {
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	eng, err := engine.Open(context.Background(), engine.Config{
		Dir:      t.TempDir(),
		VChannel: testVChannel,
		Shared:   shared,
		AllocID:  func(context.Context) (int64, error) { return nextID.Add(1), nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	return &fakeTarget{t: t, eng: eng, committedDir: filepath.Join(t.TempDir(), "committed")}
}

func (f *fakeTarget) CollectionID() int64           { return testCollectionID }
func (f *fakeTarget) VChannel() string              { return testVChannel }
func (f *fakeTarget) PChannelTerm() int64           { return testTerm }
func (f *fakeTarget) PKDataType() schemapb.DataType { return schemapb.DataType_Int64 }
func (f *fakeTarget) InstalledVersion() int64       { return f.installed }
func (f *fakeTarget) SSTBasePath() string           { return testSSTBasePath }
func (f *fakeTarget) CommittedDir() string          { return f.committedDir }
func (f *fakeTarget) LastAppliedTimeTick() uint64   { return f.lastApplied }
func (f *fakeTarget) LastCommittedTimeTick() uint64 { return f.lastCommitted }
func (f *fakeTarget) Lifecycle() engine.Lifecycle   { return f.eng }

func (f *fakeTarget) SetLastCommittedTimeTick(tt uint64) {
	f.setCommittedCalls = append(f.setCommittedCalls, tt)
	f.lastCommitted = tt
}

func (f *fakeTarget) InstallIfNewer(ctx context.Context, m ManifestView, local map[sst.ID]string, gens ...engine.Generation) error {
	f.installCalls = append(f.installCalls, installCall{manifest: m, local: local, gens: gens})
	if m.Version <= f.installed {
		return nil
	}
	tables := make([]engine.CommittedTable, 0, len(m.Tables))
	for _, ref := range m.Tables {
		path, ok := local[ref.ID]
		require.True(f.t, ok, "no local file for sst %d", ref.ID)
		info, err := sst.ReadInfo(ref.ID, path)
		require.NoError(f.t, err)
		tables = append(tables, engine.CommittedTable{Info: info, Path: path})
	}
	if err := f.eng.InstallCommitted(ctx, tables, gens...); err != nil {
		return err
	}
	f.installed = m.Version
	return nil
}

func (f *fakeTarget) write(t *testing.T, pks ...int64) {
	muts := make([]engine.Mutation, 0, len(pks))
	for _, p := range pks {
		muts = append(muts, engine.Mutation{Key: codec.EncodeInt64PK(p), Value: codec.EncodePKEntry(codec.PKEntry{SegmentID: p * 10})})
	}
	require.NoError(t, f.eng.Write(context.Background(), muts))
}

// fakeCommitter applies flushes to a manifest the way the manifest service does.
type fakeCommitter struct {
	cur *FakeManifest
	// requests are the received requests, in order.
	requests []FlushArgs
	// applied records whether each request changed the manifest.
	applied []bool
	// failNext fails the next call before it applies.
	failNext error
	// loseNext applies the next call and then returns an IO error.
	loseNext bool
	// rewrite changes the returned manifest, if it is not nil.
	rewrite func(m ManifestView) ManifestView
}

func newFakeCommitter(t *testing.T) *fakeCommitter {
	return &fakeCommitter{}
}

func (c *fakeCommitter) ClaimPChannel(ctx context.Context, pchannel string, term int64) error {
	return nil
}

func (c *fakeCommitter) GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error) {
	out := make([]RecoveryManifest, 0, len(refs))
	for range refs {
		out = append(out, FakeRecoveryManifest(c.cur))
	}
	return out, nil
}

func (c *fakeCommitter) CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error) {
	args.AddTables = append([]TableRef(nil), args.AddTables...)
	c.requests = append(c.requests, args)
	if err := c.failNext; err != nil {
		c.failNext = nil
		return ManifestView{}, err
	}
	next, changed, err := FakeFlush(c.cur, testTerm, args, testSSTBasePath)
	if err != nil {
		return ManifestView{}, err
	}
	c.cur = next
	c.applied = append(c.applied, changed)
	if c.loseNext {
		c.loseNext = false
		return ManifestView{}, pkindex.MarkIO(errors.New("connection reset"), "commit flush response")
	}
	if c.rewrite != nil {
		return c.rewrite(next.View), nil
	}
	return next.View, nil
}

func lookup(t *testing.T, eng *engine.Engine, pks ...int64) [][]byte {
	keys := make([][]byte, 0, len(pks))
	for _, p := range pks {
		keys = append(keys, codec.EncodeInt64PK(p))
	}
	vs, err := eng.MultiGet(context.Background(), keys)
	require.NoError(t, err)
	return vs
}

func refIDs(refs []TableRef) []int64 {
	ids := make([]int64, 0, len(refs))
	for _, r := range refs {
		ids = append(ids, int64(r.ID))
	}
	return ids
}

func storeIDs(t *testing.T, store *snapshottest.Store) []int64 {
	var ids []int64
	for _, k := range store.Keys() {
		id, ok := objpath.ParseSSTKey(testSSTBasePath, k)
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
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)

	target.write(t, 1, 2, 3)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.Request)
	assert.Equal(t, uint64(50), p.Covered)
	assert.Empty(t, c.requests)
	assert.Empty(t, target.setCommittedCalls)
	assert.Equal(t, 0, store.Len(), "Prepare does not upload")
	assert.True(t, p.UploadedAt.IsZero())

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))

	require.Len(t, c.requests, 1)
	req := c.requests[0]
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
		path := filepath.Join(target.committedDir, sstFileName(ref.ID))
		assert.FileExists(t, path)
		assert.Equal(t, path, p.Local[ref.ID])
	}
	addIDs := refIDs(req.AddTables)
	assert.ElementsMatch(t, addIDs, storeIDs(t, store))

	require.Len(t, target.installCalls, 1)
	assert.Equal(t, int64(1), target.installed, "the first flush creates version 1")
	for _, v := range lookup(t, target.eng, 1, 2, 3) {
		assert.NotNil(t, v)
	}
	stats := target.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Equal(t, len(addIDs), stats.CommittedTables)
	assert.Equal(t, []uint64{50}, target.setCommittedCalls)
}

func TestHandoverNothingToPublish(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.lastApplied = 20
	target.lastCommitted = 10

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Nil(t, p.Request)
	assert.Equal(t, uint64(20), p.Covered)
	assert.Len(t, p.Gens, 1)

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))

	assert.Empty(t, c.requests)
	assert.Equal(t, 0, store.Len())
	assert.Empty(t, target.installCalls)
	assert.Equal(t, []uint64{20}, target.setCommittedCalls)
	assert.Equal(t, 0, target.eng.Stats().FrozenGenerations)
}

func TestHandoverLostResponseRetriesSameRequest(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.write(t, 1, 2, 3)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)

	c.loseNext = true
	err = Commit(ctx, target, store, c, p, time.Hour)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	assert.Equal(t, []bool{true}, c.applied)
	assert.Equal(t, int64(1), c.cur.View.Version)
	assert.Empty(t, target.installCalls)
	assert.Empty(t, target.setCommittedCalls)

	// new writes arrive while the commit waits for a retry
	target.write(t, 4, 5)
	target.lastApplied = 60
	assert.Equal(t, 1, target.eng.Stats().FrozenGenerations)

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))

	require.Len(t, c.requests, 2)
	assert.Equal(t, c.requests[0], c.requests[1])
	assert.Equal(t, uint64(50), c.requests[1].CheckpointTimeTick)
	// the second request took the applied retry branch
	assert.Equal(t, []bool{true, false}, c.applied)
	assert.Equal(t, int64(1), c.cur.View.Version)
	assert.Equal(t, uint64(50), c.cur.View.CheckpointTimeTick)

	require.Len(t, target.installCalls, 1)
	assert.Equal(t, int64(1), target.installed)
	assert.Equal(t, []uint64{50}, target.setCommittedCalls)
	stats := target.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	// no rotate: the new writes are still in the active generation
	assert.Greater(t, stats.ActiveBytes, uint64(0))
	for _, v := range lookup(t, target.eng, 1, 2, 3, 4, 5) {
		assert.NotNil(t, v)
	}
}

func TestHandoverCommitFailsBeforeApplyThenRetry(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.write(t, 1, 2, 3)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)

	c.failNext = pkindex.MarkIO(errors.New("disk gone"), "commit")
	err = Commit(ctx, target, store, c, p, time.Hour)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	objects := store.Len()
	assert.Equal(t, len(p.Request.AddTables), objects)
	assert.Empty(t, c.applied)
	assert.Equal(t, 1, target.eng.Stats().FrozenGenerations)
	assert.Empty(t, target.installCalls)
	assert.Empty(t, target.setCommittedCalls)

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))

	assert.Equal(t, objects, store.Len())
	require.Len(t, c.requests, 2)
	assert.Equal(t, c.requests[0], c.requests[1])
	assert.Equal(t, []bool{true}, c.applied)
	assert.Equal(t, uint64(50), c.cur.View.CheckpointTimeTick)
	assert.Equal(t, 0, target.eng.Stats().FrozenGenerations)
	assert.Equal(t, []uint64{50}, target.setCommittedCalls)
	for _, v := range lookup(t, target.eng, 1, 2, 3) {
		assert.NotNil(t, v)
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
			target := newFakeTarget(t)
			store := snapshottest.NewStore()
			c := newFakeCommitter(t)
			c.rewrite = tc.rewrite
			target.write(t, 1)
			target.lastApplied = 50
			target.installed = tc.installed

			p, err := Prepare(ctx, target)
			require.NoError(t, err)
			require.NotNil(t, p)
			err = Commit(ctx, target, store, c, p, time.Hour)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrRejected))
			assert.Empty(t, target.installCalls)
			assert.Empty(t, target.setCommittedCalls)
			assert.Equal(t, tc.installed, target.installed)
			assert.Equal(t, 1, target.eng.Stats().FrozenGenerations)
		})
	}
}

// TestHandoverAcceptsTheSameVersion checks that a returned manifest at the
// installed version passes the safety check when its checkpoint covers the
// handover.
func TestHandoverAcceptsTheSameVersion(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.write(t, 1)
	target.lastApplied = 50
	target.installed = 1

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))

	require.Len(t, target.installCalls, 1)
	assert.Equal(t, int64(1), target.installCalls[0].manifest.Version)
	assert.Equal(t, p.Gens, target.installCalls[0].gens)
	assert.Equal(t, []uint64{50}, target.setCommittedCalls)
}

func TestHandoverReturnsCommitErrorUnchanged(t *testing.T) {
	cases := map[string]struct {
		err      error
		sentinel error
	}{
		"fenced":   {errors.Wrap(ErrFenced, "vchannel x"), ErrFenced},
		"rejected": {errors.Wrap(ErrRejected, "vchannel x"), ErrRejected},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			target := newFakeTarget(t)
			c := newFakeCommitter(t)
			target.write(t, 1)
			target.lastApplied = 50

			store := snapshottest.NewStore()
			p, err := Prepare(ctx, target)
			require.NoError(t, err)
			require.NotNil(t, p)

			c.failNext = tc.err
			err = Commit(ctx, target, store, c, p, time.Hour)
			assert.Equal(t, tc.err, err)
			assert.True(t, errors.Is(err, tc.sentinel))
			assert.Empty(t, target.installCalls)
			assert.Empty(t, target.setCommittedCalls)
			assert.Equal(t, uint64(0), target.lastCommitted)
			assert.Equal(t, 1, target.eng.Stats().FrozenGenerations)
		})
	}
}

func TestHandoverWatermarkDidNotMove(t *testing.T) {
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	target.write(t, 1, 2)
	target.lastApplied = 30
	target.lastCommitted = 30

	p, err := Prepare(context.Background(), target)
	require.NoError(t, err)
	assert.Nil(t, p)

	assert.Equal(t, 0, store.Len())
	assert.Empty(t, target.setCommittedCalls)
	stats := target.eng.Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Greater(t, stats.ActiveBytes, uint64(0))
}

func TestHandoverUploadFailsKeepsThePending(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.write(t, 1, 2, 3)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.Request)
	gens := target.eng.FrozenGenerations()
	require.Len(t, gens, 1)
	wantIDs := refIDs(p.Request.AddTables)

	// the uploads fail: Commit sends nothing and the pending stays as it is
	store.FailUploads(2)
	for range 2 {
		err = Commit(ctx, target, store, c, p, time.Hour)
		require.Error(t, err)
		assert.True(t, errors.Is(err, pkindex.ErrIO))
		assert.True(t, p.UploadedAt.IsZero())
	}
	assert.Empty(t, c.requests)
	assert.Equal(t, 0, store.Len())
	assert.Equal(t, gens, target.eng.FrozenGenerations(), "no new rotate")
	assert.Empty(t, target.setCommittedCalls)

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))
	assert.False(t, p.UploadedAt.IsZero())
	require.Len(t, c.requests, 1)
	assert.Equal(t, wantIDs, refIDs(c.requests[0].AddTables))
	assert.ElementsMatch(t, wantIDs, storeIDs(t, store))
	assert.Equal(t, 0, target.eng.Stats().FrozenGenerations)
	assert.Equal(t, []uint64{50}, target.setCommittedCalls)
	for _, v := range lookup(t, target.eng, 1, 2, 3) {
		assert.NotNil(t, v)
	}
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
	assert.True(t, errors.Is(err, pkindex.ErrIO))
}

// countingUploads counts the uploads of a store.
type countingUploads struct {
	*snapshottest.Store
	uploads int
}

func (s *countingUploads) UploadFile(ctx context.Context, key, path string) error {
	s.uploads++
	return s.Store.UploadFile(ctx, key, path)
}

func TestCommitReuploadsOldTables(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := &countingUploads{Store: snapshottest.NewStore()}
	c := newFakeCommitter(t)
	target.write(t, 1, 2)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, p.Request)
	assert.Zero(t, store.uploads)

	// the first Commit uploads every table, then the commit fails
	c.failNext = merr.WrapErrServiceUnavailable("down")
	require.Error(t, Commit(ctx, target, store, c, p, time.Hour))
	assert.False(t, p.UploadedAt.IsZero())
	uploads, objects := store.uploads, store.Len()
	assert.Equal(t, len(p.Request.AddTables), uploads)
	keys := store.Keys()

	// a recent upload is not repeated
	c.failNext = merr.WrapErrServiceUnavailable("down")
	require.Error(t, Commit(ctx, target, store, c, p, time.Hour))
	assert.Equal(t, uploads, store.uploads)

	// an old upload is repeated under the same keys before the retry
	p.UploadedAt = time.Now().Add(-2 * time.Hour)
	before := time.Now()
	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))
	assert.Equal(t, uploads+len(p.Request.AddTables), store.uploads)
	assert.Equal(t, objects, store.Len())
	assert.Equal(t, keys, store.Keys())
	assert.False(t, p.UploadedAt.Before(before))
	require.Len(t, c.requests, 3)
	assert.Equal(t, c.requests[0], c.requests[2])
	assert.Equal(t, uint64(50), target.lastCommitted)
}

func TestCommitReuploadFailureKeepsThePending(t *testing.T) {
	ctx := context.Background()
	target := newFakeTarget(t)
	store := snapshottest.NewStore()
	c := newFakeCommitter(t)
	target.write(t, 1)
	target.lastApplied = 50

	p, err := Prepare(ctx, target)
	require.NoError(t, err)
	old := time.Now().Add(-2 * time.Hour)
	p.UploadedAt = old
	store.FailUploads(1)
	err = Commit(ctx, target, store, c, p, time.Hour)
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	assert.Empty(t, c.requests, "no commit after a failed re-upload")
	assert.Equal(t, old, p.UploadedAt)
	assert.Equal(t, 1, target.eng.Stats().FrozenGenerations)

	require.NoError(t, Commit(ctx, target, store, c, p, time.Hour))
	assert.Equal(t, uint64(50), target.lastCommitted)
}
