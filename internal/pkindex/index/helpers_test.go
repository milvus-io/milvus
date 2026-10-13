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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot/snapshottest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

const (
	testCollectionID = int64(100)
	testTerm         = int64(3)
	waitTimeout      = 10 * time.Second
)

// farFuture is a step time at which every backoff is ready.
var farFuture = time.Now().Add(24 * time.Hour)

func testBasePath(vchannel string) string {
	return fmt.Sprintf("root/pkindex/%d/%s/sst", testCollectionID, vchannel)
}

// fakeClient applies claims and flushes the way the manifest service does.
type fakeClient struct {
	mu        sync.Mutex
	manifests map[string]*snapshot.FakeManifest
	// history keeps every manifest version, by vchannel.
	history map[string][]*snapshot.FakeManifest

	claimErrs  []error
	claimBlock chan struct{}

	commitErrs []error
	loseNext   bool
	// commitEntered gets a value when CommitFlush starts, if it is not nil.
	commitEntered chan struct{}
	// commitBlock blocks CommitFlush until it is closed, if it is not nil.
	commitBlock chan struct{}

	requests []snapshot.FlushArgs
	// applyErrs are the errors of the manifest edits, such as a duplicate table.
	applyErrs []error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		manifests: make(map[string]*snapshot.FakeManifest),
		history:   make(map[string][]*snapshot.FakeManifest),
	}
}

func (c *fakeClient) Claim(ctx context.Context, args snapshot.ClaimArgs) (snapshot.ManifestView, error) {
	c.mu.Lock()
	block := c.claimBlock
	c.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return snapshot.ManifestView{}, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.claimErrs) > 0 {
		err := c.claimErrs[0]
		c.claimErrs = c.claimErrs[1:]
		return snapshot.ManifestView{}, err
	}
	next, changed, err := snapshot.FakeClaim(c.manifests[args.VChannel], args, testBasePath(args.VChannel))
	if err != nil {
		return snapshot.ManifestView{}, err
	}
	c.store(args.VChannel, next, changed)
	return next.View, nil
}

func (c *fakeClient) store(vchannel string, m *snapshot.FakeManifest, changed bool) {
	c.manifests[vchannel] = m
	if changed {
		c.history[vchannel] = append(c.history[vchannel], m)
	}
}

func (c *fakeClient) CommitFlush(ctx context.Context, args snapshot.FlushArgs) (snapshot.ManifestView, error) {
	args.AddTables = append([]snapshot.TableRef(nil), args.AddTables...)
	c.mu.Lock()
	c.requests = append(c.requests, args)
	entered, block := c.commitEntered, c.commitBlock
	c.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if block != nil {
		<-block
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commitErrs) > 0 {
		err := c.commitErrs[0]
		c.commitErrs = c.commitErrs[1:]
		return snapshot.ManifestView{}, err
	}
	next, changed, err := snapshot.FakeFlush(c.manifests[args.VChannel], args)
	if err != nil {
		c.applyErrs = append(c.applyErrs, err)
		return snapshot.ManifestView{}, err
	}
	c.store(args.VChannel, next, changed)
	if c.loseNext {
		c.loseNext = false
		return snapshot.ManifestView{}, errors.Mark(errors.New("response lost"), pkindex.ErrUnavailable)
	}
	return next.View, nil
}

// manifest returns the current manifest of vchannel, a zero value when it
// does not exist.
func (c *fakeClient) manifest(vchannel string) snapshot.ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.manifests[vchannel]; m != nil {
		return m.View
	}
	return snapshot.ManifestView{}
}

// version returns version v of the manifest of vchannel, nil when it does
// not exist.
func (c *fakeClient) version(vchannel string, v int64) *snapshot.ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.history[vchannel] {
		if m.View.Version == v {
			view := m.View
			return &view
		}
	}
	return nil
}

func (c *fakeClient) requestList() []snapshot.FlushArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]snapshot.FlushArgs(nil), c.requests...)
}

func (c *fakeClient) applyErrList() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.applyErrs...)
}

func (c *fakeClient) set(f func(c *fakeClient)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

// countingStore counts uploads and downloads, and can report every object as missing.
type countingStore struct {
	*snapshottest.Store
	downloads atomic.Int64
	uploads   atomic.Int64
	missing   atomic.Bool
}

func (s *countingStore) UploadFile(ctx context.Context, key, path string) error {
	s.uploads.Add(1)
	return s.Store.UploadFile(ctx, key, path)
}

func (s *countingStore) DownloadFile(ctx context.Context, key, path string) error {
	s.downloads.Add(1)
	if s.missing.Load() {
		return errors.Mark(errors.Newf("object %s is missing", key), pkindex.ErrCorrupted)
	}
	return s.Store.DownloadFile(ctx, key, path)
}

// syncScheduler runs a task inside Submit.
type syncScheduler struct{}

func (syncScheduler) Submit(t nodescheduler.Task) nodescheduler.TaskHandle {
	_ = t.Execute(context.Background())
	done := make(chan struct{})
	close(done)
	return chanHandle{done: done}
}

// asyncScheduler runs every task in its own goroutine.
type asyncScheduler struct{}

func (asyncScheduler) Submit(t nodescheduler.Task) nodescheduler.TaskHandle {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = t.Execute(context.Background())
	}()
	return chanHandle{done: done}
}

// droppingScheduler finishes every task without running it, like a closed scheduler.
type droppingScheduler struct{}

func (droppingScheduler) Submit(t nodescheduler.Task) nodescheduler.TaskHandle {
	done := make(chan struct{})
	close(done)
	return chanHandle{done: done}
}

type chanHandle struct{ done chan struct{} }

func (h chanHandle) Cancel() {}

func (h chanHandle) Wait(ctx context.Context) error {
	select {
	case <-h.done:
		return nil
	default:
	}
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// testEnv is a Manager over a real engine, a fake client and an in-memory store.
type testEnv struct {
	t      *testing.T
	root   string
	store  *countingStore
	client *fakeClient
	mgr    *Manager
	params atomic.Pointer[Params]
	// tailBytes and recoveryTimeTick are what the WAL tail reports.
	tailBytes        atomic.Uint64
	recoveryTimeTick atomic.Uint64

	fatalMu sync.Mutex
	fatals  []error
}

func defaultTestParams() Params {
	return Params{
		LockStripes:   16,
		ActiveBytes:   1,
		TailBytes:     math.MaxUint64,
		SoftWatermark: math.MaxUint64,
		BackoffBase:   time.Millisecond,
		MaxBackoff:    10 * time.Millisecond,
		ReuploadAfter: time.Hour,
	}
}

func newTestEnv(t *testing.T, sched nodescheduler.Scheduler) *testEnv {
	env := &testEnv{
		t:      t,
		root:   t.TempDir(),
		store:  &countingStore{Store: snapshottest.NewStore()},
		client: newFakeClient(),
	}
	p := defaultTestParams()
	env.params.Store(&p)
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	env.mgr = NewManager(ManagerConfig{
		Root:      env.root,
		Shared:    shared,
		AllocID:   func(context.Context) (int64, error) { return nextID.Add(1), nil },
		Store:     env.store,
		Committer: env.client,
		Sched:     sched,
		Params:    func() Params { return *env.params.Load() },
	})
	t.Cleanup(env.mgr.Close)
	return env
}

func (env *testEnv) setParams(f func(p *Params)) {
	p := *env.params.Load()
	f(&p)
	env.params.Store(&p)
}

// channel opens a Channel without the driver goroutine.
func (env *testEnv) channel(seed uint64) *Channel {
	info := types.PChannelInfo{Name: "test-dml_0", Term: testTerm}
	return env.mgr.openChannel(info, seed, env.walTail, env.onFatal, false)
}

func (env *testEnv) walTail() WALTail {
	return WALTail{Bytes: env.tailBytes.Load(), RecoveryTimeTick: env.recoveryTimeTick.Load()}
}

func (env *testEnv) onFatal(err error) {
	env.fatalMu.Lock()
	defer env.fatalMu.Unlock()
	env.fatals = append(env.fatals, err)
}

func (env *testEnv) fatalList() []error {
	env.fatalMu.Lock()
	defer env.fatalMu.Unlock()
	return append([]error(nil), env.fatals...)
}

func (env *testEnv) waitFatal() error {
	var err error
	require.Eventually(env.t, func() bool {
		list := env.fatalList()
		if len(list) == 0 {
			return false
		}
		err = list[0]
		return true
	}, waitTimeout, time.Millisecond)
	return err
}

// add opens the Index of vchannel and waits for its claim.
func (env *testEnv) add(ch *Channel, vchannel string) *Index {
	created, err := ch.Add(context.Background(), vchannel, testCollectionID, schemapb.DataType_Int64)
	require.NoError(env.t, err)
	require.True(env.t, created)
	idx := ch.Get(vchannel)
	require.NotNil(env.t, idx)
	waitClaimed(env.t, idx)
	return idx
}

func waitClaimed(t *testing.T, idx *Index) {
	select {
	case <-idx.claimed:
	case <-time.After(waitTimeout):
		t.Fatalf("claim of %s did not finish", idx.vchannel)
	}
}

func int64PKs(values ...int64) []snpkindex.PK {
	pks := make([]snpkindex.PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, snpkindex.Int64PK(v))
	}
	return pks
}

// insert runs an autocommit insert through Decide and Apply.
func insert(t *testing.T, idx *Index, segmentID int64, pks ...int64) {
	ctx := context.Background()
	intent, err := idx.Decide(ctx, snpkindex.Request{Kind: snpkindex.KindInsert, VChannel: idx.vchannel, PKs: int64PKs(pks...)})
	require.NoError(t, err)
	intent.Apply(ctx, snpkindex.Applied{SegmentID: segmentID})
}

// segmentOf returns the segment of pk in the engine, 0 when pk is absent.
func segmentOf(t *testing.T, idx *Index, pk int64) int64 {
	vs, err := idx.eng.MultiGet(context.Background(), [][]byte{codec.EncodeInt64PK(pk)})
	require.NoError(t, err)
	if vs[0] == nil {
		return 0
	}
	e, err := codec.DecodePKEntry(vs[0])
	require.NoError(t, err)
	return e.SegmentID
}

func sstIDs(m snapshot.ManifestView) []int64 {
	ids := make([]int64, 0, len(m.Tables))
	for _, s := range m.Tables {
		ids = append(ids, int64(s.ID))
	}
	return ids
}
