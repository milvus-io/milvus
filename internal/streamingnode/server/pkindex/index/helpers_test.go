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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	testVChannel     = "v0"
	testCollectionID = int64(100)
	testTerm         = int64(3)
	testStorageRoot  = "root"
)

// fakeCommitter applies the flushes of one vchannel with the rules of the
// manifest service, see FakeFlush. The pchannel is assigned to testTerm.
type fakeCommitter struct {
	mu sync.Mutex
	// cur is the current manifest, nil before the first flush.
	cur *FakeManifest
	// history keeps every manifest version.
	history []*FakeManifest

	commitErrs []error
	loseNext   bool
	requests   []FlushArgs
	// applied records whether each flush that the fake manifest service
	// accepted changed the manifest.
	applied []bool
	// rewrite changes the returned manifest, if it is not nil.
	rewrite func(m ManifestView) ManifestView
}

func (c *fakeCommitter) CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	args.AddTables = append([]TableRef(nil), args.AddTables...)
	c.requests = append(c.requests, args)
	if len(c.commitErrs) > 0 {
		err := c.commitErrs[0]
		c.commitErrs = c.commitErrs[1:]
		return ManifestView{}, err
	}
	next, changed, err := FakeFlush(c.cur, testTerm, args, pkroot.SSTBasePath(testStorageRoot, args.CollectionID, args.VChannel))
	if err != nil {
		return ManifestView{}, err
	}
	c.cur = next
	if changed {
		c.history = append(c.history, next)
	}
	c.applied = append(c.applied, changed)
	if c.loseNext {
		c.loseNext = false
		return ManifestView{}, errors.Mark(errors.New("response lost"), pkroot.ErrUnavailable)
	}
	if c.rewrite != nil {
		return c.rewrite(next.View), nil
	}
	return next.View, nil
}

func (c *fakeCommitter) set(f func(c *fakeCommitter)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

// manifest returns the current manifest, a zero value before the first flush.
func (c *fakeCommitter) manifest() ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur == nil {
		return ManifestView{}
	}
	return c.cur.View
}

// version returns version v of the manifest, nil when it does not exist.
func (c *fakeCommitter) version(v int64) *ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.history {
		if m.View.Version == v {
			view := m.View
			return &view
		}
	}
	return nil
}

func (c *fakeCommitter) requestList() []FlushArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]FlushArgs(nil), c.requests...)
}

func (c *fakeCommitter) appliedList() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.applied...)
}

// countingStore counts the uploads and downloads of a FakeStore.
type countingStore struct {
	*FakeStore
	uploads   atomic.Int64
	downloads atomic.Int64
}

func (s *countingStore) UploadFile(ctx context.Context, key, path string) error {
	s.uploads.Add(1)
	return s.FakeStore.UploadFile(ctx, key, path)
}

func (s *countingStore) DownloadFile(ctx context.Context, key, path string) error {
	s.downloads.Add(1)
	return s.FakeStore.DownloadFile(ctx, key, path)
}

// testIndex is an Index of testVChannel over a countingStore and a
// fakeCommitter. observed stands for the lastObserved of its pchannel.
type testIndex struct {
	*Index
	root      string
	shared    *engine.SharedResources
	store     *countingStore
	committer *fakeCommitter
	nextID    atomic.Int64
	observed  atomic.Uint64
	params    atomic.Pointer[Params]
}

// newTestIndex opens a ready Index under a new local root.
func newTestIndex(t *testing.T) *testIndex {
	ti := openTestIndex(t)
	ti.MarkReady(context.Background())
	return ti
}

// openTestIndex opens an Index under a new local root. The Index is not
// ready: the recovery would mark it.
func openTestIndex(t *testing.T) *testIndex {
	ti := &testIndex{
		root:      t.TempDir(),
		shared:    engine.NewSharedResources(engine.SharedConfig{CacheBytes: 8 << 20}),
		store:     &countingStore{FakeStore: NewFakeStore()},
		committer: &fakeCommitter{},
	}
	t.Cleanup(ti.shared.Release)
	ti.nextID.Store(1000)
	ti.params.Store(&Params{ReuploadAfter: time.Hour})
	ti.open(t)
	return ti
}

// open opens the Index over the local files under root. The Index is not ready.
func (ti *testIndex) open(t *testing.T) {
	idx, err := Open(context.Background(), Config{
		VChannel:        testVChannel,
		CollectionID:    testCollectionID,
		PKDataType:      schemapb.DataType_Int64,
		Term:            testTerm,
		Ctx:             context.Background(),
		Logger:          mlog.With(mlog.FieldModule("pkindex")),
		Root:            ti.root,
		StorageRootPath: testStorageRoot,
		Shared:          ti.shared,
		AllocID:         func(context.Context) (int64, error) { return ti.nextID.Add(1), nil },
		Store:           ti.store,
		Committer:       ti.committer,
		Params:          func() Params { return *ti.params.Load() },
		LastObserved:    ti.observed.Load,
		OnRefresh:       func(*Index) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { idx.Close(context.Background()) })
	ti.Index = idx
}

// reopen closes the Index and opens it again over the same local files,
// store and committer, as a restart of the vchannel does. The new Index is
// not ready.
func (ti *testIndex) reopen(t *testing.T) {
	ti.Close(context.Background())
	ti.open(t)
}

// insert applies an autocommit insert at the time tick after the observed
// one: a write that the next TimeTick covers.
func (ti *testIndex) insert(t *testing.T, segmentID int64, pks ...int64) {
	_, err := ti.Apply(context.Background(), Write{Puts: puts(segmentID, pks...), TimeTick: ti.observed.Load() + 1})
	require.NoError(t, err)
}

// segmentOf returns the segment of pk in the engine, 0 when pk is absent.
func (ti *testIndex) segmentOf(t *testing.T, pk int64) int64 {
	vs, err := ti.eng.MultiGet(context.Background(), [][]byte{codec.EncodeInt64PK(pk)})
	require.NoError(t, err)
	if vs[0] == nil {
		return 0
	}
	e, err := codec.DecodePKEntry(vs[0])
	require.NoError(t, err)
	return e.SegmentID
}

// puts returns the puts of pks into segmentID.
func puts(segmentID int64, pks ...int64) []Put {
	out := make([]Put, 0, len(pks))
	for _, pk := range pks {
		out = append(out, Put{PK: dedup.Int64PK(pk), SegmentID: segmentID})
	}
	return out
}

func int64PKs(values ...int64) []dedup.PK {
	pks := make([]dedup.PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, dedup.Int64PK(v))
	}
	return pks
}

func insertRequest(pks ...int64) dedup.DecideRequest {
	return dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(pks...)}
}

func sstIDs(m ManifestView) []int64 {
	ids := make([]int64, 0, len(m.Tables))
	for _, s := range m.Tables {
		ids = append(ids, int64(s.ID))
	}
	return ids
}

// handover runs one handover step, as the driver would, and reports whether
// it committed.
func (ti *testIndex) handover(t *testing.T) bool {
	committed, err := ti.Handover(context.Background())
	require.NoError(t, err)
	return committed
}
