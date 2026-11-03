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

//go:build test

package indexmanager

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fakeStorageRoot is the object storage root path of NewFakeEnv.
const fakeStorageRoot = "root"

// FakeCommitter applies lookups and flushes the way the manifest service
// does, with the SST base path of a new manifest under fakeStorageRoot. The
// tests of this package set its fields through set, the tests of other
// packages through SetManifest, SetLookupFailures and SetLookupErr. A flush
// is fenced or rejected until assign gives the pchannel meta the term of the
// flush.
type FakeCommitter struct {
	mu sync.Mutex
	// assignedTerm is the term of the pchannel meta of the streaming
	// coordinator, 0 when the pchannel is not assigned. The manifest service
	// fences a flush of another term with it; the tests set it with assign.
	assignedTerm int64
	manifests    map[string]*manifest.FakeManifest
	// history keeps every manifest version, by vchannel.
	history map[string][]*manifest.FakeManifest

	// lookupErr fails every lookup, if it is not nil.
	lookupErr   error
	lookupErrs  []error
	lookupBlock chan struct{}
	// itemErrs are the per-item errors of the lookups, by vchannel.
	itemErrs map[string][]error
	// lookups are the refs of each GetRecoveryManifests call, in order.
	lookups [][]manifest.VChannelRef

	commitErrs []error
	loseNext   bool
	// commitEntered gets a value when CommitFlush starts, if it is not nil.
	commitEntered chan struct{}
	// commitBlock blocks CommitFlush until it is closed, if it is not nil.
	commitBlock chan struct{}

	requests []manifest.FlushArgs
	// applied records whether each flush that the fake manifest service
	// accepted changed the manifest.
	applied []bool
	// rewrite changes the returned manifest, if it is not nil.
	rewrite func(m manifest.View) manifest.View
	// applyErrs are the errors of the manifest edits, such as a duplicate table.
	applyErrs []error
}

// NewFakeCommitter creates a FakeCommitter where no vchannel has a manifest.
func NewFakeCommitter() *FakeCommitter {
	return &FakeCommitter{
		manifests: make(map[string]*manifest.FakeManifest),
		history:   make(map[string][]*manifest.FakeManifest),
	}
}

// assign makes term the term of the pchannel meta, as the streaming
// coordinator does when it assigns the pchannel.
func (c *FakeCommitter) assign(term int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignedTerm = term
}

// SetManifest gives a vchannel a manifest, nil removes it.
func (c *FakeCommitter) SetManifest(vchannel string, m *manifest.FakeManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m == nil {
		delete(c.manifests, vchannel)
		return
	}
	c.store(vchannel, m, true)
}

// errFakeTransient is the transient failure that SetLookupFailures injects.
var errFakeTransient = errors.New("injected transient failure")

// SetLookupFailures makes the next n manifest lookups fail with a transient error.
func (c *FakeCommitter) SetLookupFailures(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookupErrs = nil
	for i := 0; i < n; i++ {
		c.lookupErrs = append(c.lookupErrs, errors.Wrap(errFakeTransient, "look up the manifests"))
	}
}

// SetLookupErr makes every later manifest lookup fail with err.
func (c *FakeCommitter) SetLookupErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookupErr = err
}

// GetRecoveryManifests implements manifest.Committer.
func (c *FakeCommitter) GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error) {
	c.mu.Lock()
	c.lookups = append(c.lookups, append([]manifest.VChannelRef(nil), refs...))
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
	if c.lookupErr != nil {
		return nil, c.lookupErr
	}
	if len(c.lookupErrs) > 0 {
		err := c.lookupErrs[0]
		c.lookupErrs = c.lookupErrs[1:]
		return nil, err
	}
	out := make([]manifest.RecoveryManifest, 0, len(refs))
	for _, ref := range refs {
		if errs := c.itemErrs[ref.VChannel]; len(errs) > 0 {
			c.itemErrs[ref.VChannel] = errs[1:]
			out = append(out, manifest.RecoveryManifest{Err: errs[0]})
			continue
		}
		out = append(out, manifest.FakeRecoveryManifest(ref.VChannel, c.manifests[ref.VChannel]))
	}
	return out, nil
}

// ReadObject implements manifest.ObjectReader for the manifest bodies that
// the lookups point to. The body key names the vchannel and the version.
func (c *FakeCommitter) ReadObject(ctx context.Context, bodyKey string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for vchannel, m := range c.manifests {
		if manifest.FakeRecoveryManifest(vchannel, m).BodyKey == bodyKey {
			return manifest.FakeBody(m.View), nil
		}
	}
	return nil, errors.Wrapf(pkroot.ErrCorrupted, "manifest body %s does not exist", bodyKey)
}

// lookupList returns the refs of each GetRecoveryManifests call.
func (c *FakeCommitter) lookupList() [][]manifest.VChannelRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]manifest.VChannelRef(nil), c.lookups...)
}

func (c *FakeCommitter) store(vchannel string, m *manifest.FakeManifest, changed bool) {
	c.manifests[vchannel] = m
	if changed {
		c.history[vchannel] = append(c.history[vchannel], m)
	}
}

// CommitFlush implements manifest.Committer.
func (c *FakeCommitter) CommitFlush(ctx context.Context, args manifest.FlushArgs) (manifest.View, error) {
	args.AddTables = append([]manifest.TableRef(nil), args.AddTables...)
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
		return manifest.View{}, err
	}
	basePath := manifest.SSTBasePath(fakeStorageRoot, args.CollectionID, args.IndexID, args.VChannel)
	next, changed, err := manifest.FakeFlush(c.manifests[args.VChannel], c.assignedTerm, args, basePath)
	if err != nil {
		c.applyErrs = append(c.applyErrs, err)
		return manifest.View{}, err
	}
	c.store(args.VChannel, next, changed)
	c.applied = append(c.applied, changed)
	if c.loseNext {
		c.loseNext = false
		return manifest.View{}, errors.Mark(errors.New("response lost"), pkroot.ErrUnavailable)
	}
	if c.rewrite != nil {
		return c.rewrite(next.View), nil
	}
	return next.View, nil
}

func (c *FakeCommitter) appliedList() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.applied...)
}

// manifest returns the current manifest of vchannel, a zero value when it
// does not exist.
func (c *FakeCommitter) manifest(vchannel string) manifest.View {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.manifests[vchannel]; m != nil {
		return m.View
	}
	return manifest.View{}
}

// version returns version v of the manifest of vchannel, nil when it does
// not exist.
func (c *FakeCommitter) version(vchannel string, v int64) *manifest.View {
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

func (c *FakeCommitter) requestList() []manifest.FlushArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]manifest.FlushArgs(nil), c.requests...)
}

func (c *FakeCommitter) applyErrList() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.applyErrs...)
}

func (c *FakeCommitter) set(f func(c *FakeCommitter)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

// neverScheduler accepts every task and never runs it.
type neverScheduler struct{}

// Submit returns a handle whose task never runs.
func (neverScheduler) Submit(nodescheduler.Task) nodescheduler.TaskHandle {
	return &neverHandle{done: make(chan struct{})}
}

type neverHandle struct {
	once sync.Once
	done chan struct{}
}

func (h *neverHandle) Cancel() { h.once.Do(func() { close(h.done) }) }

func (h *neverHandle) Wait(ctx context.Context) error {
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FakeEnv is an index Manager over a real engine and the FakeCommitter it uses.
type FakeEnv struct {
	Manager   Manager
	Committer *FakeCommitter
}

// NewFakeEnv creates an index Manager over a real engine with root as the local
// root directory. No size or tail ever starts a handover. The test cleanup
// closes the Manager and releases the engine caches.
func NewFakeEnv(t *testing.T, root string) *FakeEnv {
	env := &FakeEnv{Committer: NewFakeCommitter()}
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 8 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	params := Params{
		Params:        index.Params{ReuploadAfter: time.Hour},
		ActiveBytes:   math.MaxUint64,
		TailBytes:     math.MaxUint64,
		SoftWatermark: math.MaxUint64,
		BackoffBase:   time.Millisecond,
		MaxBackoff:    10 * time.Millisecond,
	}
	env.Manager = NewManager(ManagerConfig{
		Root:            root,
		Engine:          shared,
		AllocID:         func(context.Context) (int64, error) { return nextID.Add(1), nil },
		Store:           index.NewFakeStore(),
		StorageRootPath: fakeStorageRoot,
		Committer:       env.Committer,
		Reader:          env.Committer,
		Sched:           neverScheduler{},
		Params:          func() Params { return params },
	})
	t.Cleanup(env.Manager.Close)
	return env
}

// NewManagerForTest creates the index Manager of NewFakeEnv.
func NewManagerForTest(t *testing.T, root string) Manager {
	return NewFakeEnv(t, root).Manager
}

// IndexForTest returns the Index of a vchannel, nil when it does not exist.
func IndexForTest(ch PChannel, vchannel string) index.Index {
	return ch.(*pchannel).get(vchannel)
}

// RemoveForTest closes the Index of a vchannel and keeps its local files, as
// a restart of the vchannel would.
func RemoveForTest(ctx context.Context, ch PChannel, vchannel string) {
	ch.(*pchannel).remove(ctx, vchannel)
}

// GetForTest returns the open pchannel of a pchannel, nil when
// none is open. Production code gets it from the recovery snapshot.
func GetForTest(mgr Manager, name string) PChannel {
	m := mgr.(*managerImpl)
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch := m.channels[name]; ch != nil {
		return ch
	}
	return nil
}

// StoreForTest returns the table store of the Manager.
func StoreForTest(m Manager) index.TableStore { return m.(*managerImpl).cfg.Store }

// FlushTTForTest returns the time tick of the last flush that the observe
// saw on a vchannel, 0 when it saw none or the vchannel has no Index.
func FlushTTForTest(ch PChannel, vchannel string) uint64 {
	c := ch.(*pchannel)
	idx := c.get(vchannel)
	if idx == nil {
		return 0
	}
	return c.scheduleOf(idx).flushTT.Load()
}

// SetCommitterForTest replaces the Committer of the Manager.
func SetCommitterForTest(m Manager, value manifest.Committer) { m.(*managerImpl).cfg.Committer = value }

// SetSchedForTest replaces the node scheduler of the Manager, which runs the
// handovers and the installs.
func SetSchedForTest(m Manager, value nodescheduler.Scheduler) { m.(*managerImpl).cfg.Sched = value }

// SetReaderForTest replaces the manifest body reader of the Manager.
func SetReaderForTest(m Manager, value manifest.ObjectReader) { m.(*managerImpl).cfg.Reader = value }

// RootForTest returns the local root of the manager.
func RootForTest(m Manager) string { return m.(*managerImpl).cfg.Root }

// DeciderForTest returns the decider of a vchannel, or nil when none is open.
func DeciderForTest(ch PChannel, name string) dedup.Decider {
	idx := ch.(*pchannel).get(name)
	if idx == nil {
		return nil
	}
	return idx.Decider()
}

// SchedulerForTest returns the configured scheduler.
func SchedulerForTest(m Manager) nodescheduler.Scheduler { return m.(*managerImpl).cfg.Sched }

// dropIndexForTest performs both steps for tests of synchronous cleanup.
func (c *pchannel) dropIndexForTest(ctx context.Context, name string, id int64) error {
	if c.ctx.Err() != nil {
		return errClosed("pchannel " + c.info.Name)
	}
	if drop := c.RemoveIndex(name, id); drop != nil {
		return drop(ctx)
	}
	return nil
}

// CommitterForTest returns the configured committer.
func CommitterForTest(m Manager) manifest.Committer { return m.(*managerImpl).cfg.Committer }

// remove closes the Index of a vchannel. It waits for a running handover.
// It keeps the local files, like Close does for every vchannel. Only the
// tests call it, to replay the restart of one vchannel.
func (c *pchannel) remove(ctx context.Context, vchannel string) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closeIndex(ctx, vchannel)
}

// closeIndex detaches and closes the Index of a vchannel, if it is open. The
// caller holds opMu.
func (c *pchannel) closeIndex(ctx context.Context, vchannel string) {
	c.mu.Lock()
	idx, ok := c.indexes[vchannel]
	if ok {
		c.detachLocked(idx)
	}
	c.mu.Unlock()
	if ok {
		idx.Close(ctx)
	}
}
