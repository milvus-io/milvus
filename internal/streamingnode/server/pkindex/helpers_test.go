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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
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

// testStorageRoot is the object storage root path of the tests.
const testStorageRoot = "root"

func testBasePath(vchannel string) string {
	return pkroot.SSTBasePath(testStorageRoot, testCollectionID, vchannel)
}

// fakeClient applies claims, lookups and flushes the way the manifest
// service does. The tests use one pchannel.
type fakeClient struct {
	mu sync.Mutex
	// claimedTerm is the claimed term of the pchannel, 0 before a claim.
	claimedTerm int64
	manifests   map[string]*FakeManifest
	// history keeps every manifest version, by vchannel.
	history map[string][]*FakeManifest

	claimErrs  []error
	claimBlock chan struct{}
	claimCalls int

	lookupErrs  []error
	lookupBlock chan struct{}
	// itemErrs are the per-item errors of the lookups, by vchannel.
	itemErrs map[string][]error
	// lookups are the refs of each GetRecoveryManifests call, in order.
	lookups [][]VChannelRef

	commitErrs []error
	loseNext   bool
	// commitEntered gets a value when CommitFlush starts, if it is not nil.
	commitEntered chan struct{}
	// commitBlock blocks CommitFlush until it is closed, if it is not nil.
	commitBlock chan struct{}

	requests []FlushArgs
	// applied records whether each flush that the fake manifest service
	// accepted changed the manifest.
	applied []bool
	// rewrite changes the returned manifest, if it is not nil.
	rewrite func(m ManifestView) ManifestView
	// applyErrs are the errors of the manifest edits, such as a duplicate table.
	applyErrs []error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		manifests: make(map[string]*FakeManifest),
		history:   make(map[string][]*FakeManifest),
	}
}

func (c *fakeClient) ClaimPChannel(ctx context.Context, pchannel string, term int64) error {
	c.mu.Lock()
	c.claimCalls++
	block := c.claimBlock
	c.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.claimErrs) > 0 {
		err := c.claimErrs[0]
		c.claimErrs = c.claimErrs[1:]
		return err
	}
	next, err := FakeClaimPChannel(pchannel, c.claimedTerm, term)
	if err != nil {
		return err
	}
	c.claimedTerm = next
	return nil
}

func (c *fakeClient) GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error) {
	c.mu.Lock()
	c.lookups = append(c.lookups, append([]VChannelRef(nil), refs...))
	block := c.lookupBlock
	c.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.lookupErrs) > 0 {
		err := c.lookupErrs[0]
		c.lookupErrs = c.lookupErrs[1:]
		return nil, err
	}
	out := make([]RecoveryManifest, 0, len(refs))
	for _, ref := range refs {
		if errs := c.itemErrs[ref.VChannel]; len(errs) > 0 {
			c.itemErrs[ref.VChannel] = errs[1:]
			out = append(out, RecoveryManifest{Err: errs[0]})
			continue
		}
		out = append(out, FakeRecoveryManifest(c.manifests[ref.VChannel]))
	}
	return out, nil
}

// seed gives vchannel a manifest with view, as if another owner committed it.
func (c *fakeClient) seed(vchannel string, view ManifestView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	view.SSTBasePath = testBasePath(vchannel)
	c.store(vchannel, &FakeManifest{View: view, LastFlushTerm: 1, PKDataType: schemapb.DataType_Int64}, true)
}

// claimCount returns the number of ClaimPChannel calls.
func (c *fakeClient) claimCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claimCalls
}

// lookupList returns the refs of each GetRecoveryManifests call.
func (c *fakeClient) lookupList() [][]VChannelRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]VChannelRef(nil), c.lookups...)
}

func (c *fakeClient) store(vchannel string, m *FakeManifest, changed bool) {
	c.manifests[vchannel] = m
	if changed {
		c.history[vchannel] = append(c.history[vchannel], m)
	}
}

func (c *fakeClient) CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error) {
	args.AddTables = append([]TableRef(nil), args.AddTables...)
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
		return ManifestView{}, err
	}
	next, changed, err := FakeFlush(c.manifests[args.VChannel], c.claimedTerm, args, testBasePath(args.VChannel))
	if err != nil {
		c.applyErrs = append(c.applyErrs, err)
		return ManifestView{}, err
	}
	c.store(args.VChannel, next, changed)
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

func (c *fakeClient) appliedList() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.applied...)
}

// manifest returns the current manifest of vchannel, a zero value when it
// does not exist.
func (c *fakeClient) manifest(vchannel string) ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.manifests[vchannel]; m != nil {
		return m.View
	}
	return ManifestView{}
}

// version returns version v of the manifest of vchannel, nil when it does
// not exist.
func (c *fakeClient) version(vchannel string, v int64) *ManifestView {
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

func (c *fakeClient) requestList() []FlushArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]FlushArgs(nil), c.requests...)
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
	*FakeStore
	downloads atomic.Int64
	uploads   atomic.Int64
	missing   atomic.Bool
}

func (s *countingStore) UploadFile(ctx context.Context, key, path string) error {
	s.uploads.Add(1)
	return s.FakeStore.UploadFile(ctx, key, path)
}

func (s *countingStore) DownloadFile(ctx context.Context, key, path string) error {
	s.downloads.Add(1)
	if s.missing.Load() {
		return errors.Mark(errors.Newf("object %s is missing", key), pkroot.ErrCorrupted)
	}
	return s.FakeStore.DownloadFile(ctx, key, path)
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
		store:  &countingStore{FakeStore: NewFakeStore()},
		client: newFakeClient(),
	}
	p := defaultTestParams()
	env.params.Store(&p)
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 32 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	env.mgr = NewManager(ManagerConfig{
		Root:            env.root,
		Shared:          shared,
		AllocID:         func(context.Context) (int64, error) { return nextID.Add(1), nil },
		Store:           env.store,
		StorageRootPath: testStorageRoot,
		Committer:       env.client,
		Sched:           sched,
		Params:          func() Params { return *env.params.Load() },
	})
	t.Cleanup(env.mgr.Close)
	return env
}

func (env *testEnv) setParams(f func(p *Params)) {
	p := *env.params.Load()
	f(&p)
	env.params.Store(&p)
}

// channel opens a Channel without the driver goroutine and starts its claim.
// Every Index that a test adds to it is the Index of a new collection.
func (env *testEnv) channel(seed uint64) *Channel {
	ch := env.openChannel(seed)
	ch.StartClaim()
	return ch
}

// recoveredChannel opens a Channel, adds the indexes of vchannels as the
// indexes of existing collections, and starts the claim. It does not wait
// for the claim.
func (env *testEnv) recoveredChannel(seed uint64, vchannels ...string) *Channel {
	ch := env.openChannel(seed)
	for _, v := range vchannels {
		_, created, err := ch.Add(context.Background(), v, testCollectionID, schemapb.DataType_Int64)
		require.NoError(env.t, err)
		require.True(env.t, created)
	}
	ch.StartClaim()
	return ch
}

func (env *testEnv) openChannel(seed uint64) *Channel {
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

// add opens the Index of vchannel and waits until it is ready.
func (env *testEnv) add(ch *Channel, vchannel string) *Index {
	_, created, err := ch.Add(context.Background(), vchannel, testCollectionID, schemapb.DataType_Int64)
	require.NoError(env.t, err)
	require.True(env.t, created)
	idx := ch.Get(vchannel)
	require.NotNil(env.t, idx)
	waitReady(env.t, idx)
	return idx
}

func waitReady(t *testing.T, idx *Index) {
	select {
	case <-idx.ready:
	case <-time.After(waitTimeout):
		t.Fatalf("claim and manifest lookup of %s did not finish", idx.vchannel)
	}
}

// insert runs an autocommit insert through Decide and Apply.
func insert(t *testing.T, idx *Index, segmentID int64, pks ...int64) {
	ctx := context.Background()
	intent, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindInsert, VChannel: idx.vchannel, PKs: int64PKs(pks...)})
	require.NoError(t, err)
	intent.Apply(ctx, authority.Applied{SegmentID: segmentID})
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

func sstIDs(m ManifestView) []int64 {
	ids := make([]int64, 0, len(m.Tables))
	for _, s := range m.Tables {
		ids = append(ids, int64(s.ID))
	}
	return ids
}

func int64PKs(values ...int64) []authority.PK {
	pks := make([]authority.PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, authority.Int64PK(v))
	}
	return pks
}

// slotOf returns the driver bookkeeping of idx.
func slotOf(idx *Index) *handoverSlot {
	return idx.ch.slotOf(idx)
}
