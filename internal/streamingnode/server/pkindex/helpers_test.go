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
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
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
const testStorageRoot = fakeStorageRoot

// countingStore counts uploads and downloads, and can report every object as missing.
type countingStore struct {
	*index.FakeStore
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

// testEnv is a Manager over a real engine, a FakeCommitter and an in-memory store.
type testEnv struct {
	t      *testing.T
	root   string
	store  *countingStore
	client *FakeCommitter
	mgr    *Manager
	params atomic.Pointer[Params]

	fatalMu sync.Mutex
	fatals  []error
}

func defaultTestParams() Params {
	return Params{
		Params:        index.Params{ReuploadAfter: time.Hour},
		ActiveBytes:   1,
		TailBytes:     math.MaxUint64,
		SoftWatermark: math.MaxUint64,
		BackoffBase:   time.Millisecond,
		MaxBackoff:    10 * time.Millisecond,
	}
}

func newTestEnv(t *testing.T, sched nodescheduler.Scheduler) *testEnv {
	env := &testEnv{
		t:      t,
		root:   t.TempDir(),
		store:  &countingStore{FakeStore: index.NewFakeStore()},
		client: NewFakeCommitter(),
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

// channel opens a started PChannelIndexManager without the driver goroutine.
// Every Index that a test adds to it is the Index of a new collection, ready
// at once.
func (env *testEnv) channel(seed uint64) *PChannelIndexManager {
	ch := env.openChannel(seed)
	ch.start(false)
	return ch
}

// recoveredChannel opens a PChannelIndexManager that is not started yet and
// adds the indexes of vchannels as the indexes of existing collections. They
// are not ready: the recovery would mark them.
func (env *testEnv) recoveredChannel(seed uint64, vchannels ...string) *PChannelIndexManager {
	ch := env.openChannel(seed)
	for _, v := range vchannels {
		_, created, err := ch.CreateCollection(context.Background(), v, testCollectionID, schemapb.DataType_Int64)
		require.NoError(env.t, err)
		require.True(env.t, created)
	}
	return ch
}

// openChannel opens the PChannelIndexManager of the test pchannel, whose
// term is the assigned term of the pchannel.
func (env *testEnv) openChannel(seed uint64) *PChannelIndexManager {
	info := types.PChannelInfo{Name: "test-dml_0", Term: testTerm}
	ch := env.mgr.New(PChannelConfig{PChannel: info, Seed: seed, OnFatal: env.onFatal})
	env.client.assign(testTerm)
	return ch
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
func (env *testEnv) add(ch *PChannelIndexManager, vchannel string) *index.Index {
	idx, created, err := ch.CreateCollection(context.Background(), vchannel, testCollectionID, schemapb.DataType_Int64)
	require.NoError(env.t, err)
	require.True(env.t, created)
	require.NotNil(env.t, idx)
	waitReady(env.t, idx)
	return idx
}

func waitReady(t *testing.T, idx *index.Index) {
	select {
	case <-idx.ReadyForTest():
	case <-time.After(waitTimeout):
		t.Fatalf("manifest lookup of %s did not finish", idx.VChannel())
	}
}

// insert applies an autocommit insert the way the observe of the WAL
// recovery does, at the time tick after the last observed one: a write that
// the next TimeTick covers.
func insert(t *testing.T, ch *PChannelIndexManager, idx *index.Index, segmentID int64, pks ...int64) {
	require.NoError(t, ch.Apply(context.Background(), idx.VChannel(), index.Write{Puts: puts(segmentID, pks...), TimeTick: ch.LastObserved() + 1}))
}

// puts returns the puts of pks into segmentID.
func puts(segmentID int64, pks ...int64) []index.Put {
	out := make([]index.Put, 0, len(pks))
	for _, pk := range pks {
		out = append(out, index.Put{PK: dedup.Int64PK(pk), SegmentID: segmentID})
	}
	return out
}

// handover runs one handover step of idx by hand, as the driver would, and
// reports whether it committed.
func handover(t *testing.T, idx *index.Index) bool {
	committed, err := idx.Handover(context.Background())
	require.NoError(t, err)
	return committed
}

// segmentOf returns the segment of pk in the engine, 0 when pk is absent.
func segmentOf(t *testing.T, idx *index.Index, pk int64) int64 {
	vs, err := idx.EngineForTest().MultiGet(context.Background(), [][]byte{codec.EncodeInt64PK(pk)})
	require.NoError(t, err)
	if vs[0] == nil {
		return 0
	}
	e, err := codec.DecodePKEntry(vs[0])
	require.NoError(t, err)
	return e.SegmentID
}

func sstIDs(m index.ManifestView) []int64 {
	ids := make([]int64, 0, len(m.Tables))
	for _, s := range m.Tables {
		ids = append(ids, int64(s.ID))
	}
	return ids
}

func int64PKs(values ...int64) []dedup.PK {
	pks := make([]dedup.PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, dedup.Int64PK(v))
	}
	return pks
}

// picks returns the indexes that the driver would hand over at now.
func picks(ch *PChannelIndexManager, now time.Time) []*index.Index {
	out, _, _ := ch.pickHandovers(now)
	return out
}

// scheduleOf returns the driver bookkeeping of idx.
func scheduleOf(ch *PChannelIndexManager, idx *index.Index) *handoverSchedule {
	return ch.scheduleOf(idx)
}

// inHeap reports whether idx is in the LastAcked heap of its PChannelIndexManager.
func inHeap(ch *PChannelIndexManager, idx *index.Index) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.acked.contains(idx)
}

func insertRequest(idx *index.Index, pks ...int64) dedup.DecideRequest {
	return dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(pks...)}
}
