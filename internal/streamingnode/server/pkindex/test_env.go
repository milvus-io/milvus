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

package pkindex

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
	manifests    map[string]*index.FakeManifest
	// history keeps every manifest version, by vchannel.
	history map[string][]*index.FakeManifest

	// lookupErr fails every lookup, if it is not nil.
	lookupErr   error
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

	requests []index.FlushArgs
	// applied records whether each flush that the fake manifest service
	// accepted changed the manifest.
	applied []bool
	// rewrite changes the returned manifest, if it is not nil.
	rewrite func(m index.ManifestView) index.ManifestView
	// applyErrs are the errors of the manifest edits, such as a duplicate table.
	applyErrs []error
}

// NewFakeCommitter creates a FakeCommitter where no vchannel has a manifest.
func NewFakeCommitter() *FakeCommitter {
	return &FakeCommitter{
		manifests: make(map[string]*index.FakeManifest),
		history:   make(map[string][]*index.FakeManifest),
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
func (c *FakeCommitter) SetManifest(vchannel string, m *index.FakeManifest) {
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

// GetRecoveryManifests implements Committer.
func (c *FakeCommitter) GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error) {
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
	if c.lookupErr != nil {
		return nil, c.lookupErr
	}
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
		out = append(out, FakeRecoveryManifest(ref.VChannel, c.manifests[ref.VChannel]))
	}
	return out, nil
}

// ReadManifest implements Committer. The body key names the vchannel and the version.
func (c *FakeCommitter) ReadManifest(ctx context.Context, bodyKey string) (index.ManifestView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for vchannel, m := range c.manifests {
		if FakeRecoveryManifest(vchannel, m).BodyKey == bodyKey {
			return m.View, nil
		}
	}
	return index.ManifestView{}, errors.Wrapf(pkroot.ErrCorrupted, "manifest body %s does not exist", bodyKey)
}

// lookupList returns the refs of each GetRecoveryManifests call.
func (c *FakeCommitter) lookupList() [][]VChannelRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]VChannelRef(nil), c.lookups...)
}

func (c *FakeCommitter) store(vchannel string, m *index.FakeManifest, changed bool) {
	c.manifests[vchannel] = m
	if changed {
		c.history[vchannel] = append(c.history[vchannel], m)
	}
}

// CommitFlush implements Committer.
func (c *FakeCommitter) CommitFlush(ctx context.Context, args index.FlushArgs) (index.ManifestView, error) {
	args.AddTables = append([]index.TableRef(nil), args.AddTables...)
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
		return index.ManifestView{}, err
	}
	basePath := pkroot.SSTBasePath(fakeStorageRoot, args.CollectionID, args.VChannel)
	next, changed, err := index.FakeFlush(c.manifests[args.VChannel], c.assignedTerm, args, basePath)
	if err != nil {
		c.applyErrs = append(c.applyErrs, err)
		return index.ManifestView{}, err
	}
	c.store(args.VChannel, next, changed)
	c.applied = append(c.applied, changed)
	if c.loseNext {
		c.loseNext = false
		return index.ManifestView{}, errors.Mark(errors.New("response lost"), pkroot.ErrUnavailable)
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
func (c *FakeCommitter) manifest(vchannel string) index.ManifestView {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.manifests[vchannel]; m != nil {
		return m.View
	}
	return index.ManifestView{}
}

// version returns version v of the manifest of vchannel, nil when it does
// not exist.
func (c *FakeCommitter) version(vchannel string, v int64) *index.ManifestView {
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

func (c *FakeCommitter) requestList() []index.FlushArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]index.FlushArgs(nil), c.requests...)
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
func (neverScheduler) Submit(nodescheduler.Task) nodescheduler.TaskHandle { return neverHandle{} }

type neverHandle struct{}

func (neverHandle) Cancel() {}

func (neverHandle) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// FakeEnv is an index Manager over a real engine and the FakeCommitter it uses.
type FakeEnv struct {
	Manager   *Manager
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
		Shared:          shared,
		AllocID:         func(context.Context) (int64, error) { return nextID.Add(1), nil },
		Store:           index.NewFakeStore(),
		StorageRootPath: fakeStorageRoot,
		Committer:       env.Committer,
		Sched:           neverScheduler{},
		Params:          func() Params { return params },
	})
	t.Cleanup(env.Manager.Close)
	return env
}

// NewManagerForTest creates the index Manager of NewFakeEnv.
func NewManagerForTest(t *testing.T, root string) *Manager {
	return NewFakeEnv(t, root).Manager
}

// IndexForTest returns the Index of a vchannel, nil when it does not exist.
func (c *PChannelIndexManager) IndexForTest(vchannel string) *index.Index {
	return c.get(vchannel)
}

// RemoveForTest closes the Index of a vchannel and keeps its local files, as
// a restart of the vchannel would.
func (c *PChannelIndexManager) RemoveForTest(ctx context.Context, vchannel string) {
	c.remove(ctx, vchannel)
}

// GetForTest returns the open PChannelIndexManager of a pchannel, nil when
// none is open. Production code gets it from the recovery snapshot.
func (m *Manager) GetForTest(pchannel string) *PChannelIndexManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.channels[pchannel]
}

// StoreForTest returns the table store of the Manager.
func (m *Manager) StoreForTest() TableStore { return m.cfg.Store }

// FlushTTForTest returns the time tick of the last flush that the observe
// saw on a vchannel, 0 when it saw none or the vchannel has no Index.
func (c *PChannelIndexManager) FlushTTForTest(vchannel string) uint64 {
	idx := c.get(vchannel)
	if idx == nil {
		return 0
	}
	return c.scheduleOf(idx).flushTT.Load()
}

// SetCommitterForTest replaces the Committer of the Manager.
func (m *Manager) SetCommitterForTest(c Committer) { m.cfg.Committer = c }
