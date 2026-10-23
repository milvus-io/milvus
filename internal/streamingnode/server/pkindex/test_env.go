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

// FakeCommitter serves the manifests that SetManifest gives, the way the
// manifest service does. A handover is not tested with it, so CommitFlush
// always fails.
type FakeCommitter struct {
	mu        sync.Mutex
	lookupErr error
	// lookupFailures is the number of next lookups that fail with a transient error.
	lookupFailures int
	manifests      map[string]*index.FakeManifest // by vchannel
}

// NewFakeCommitter creates a FakeCommitter with no manifests.
func NewFakeCommitter() *FakeCommitter {
	return &FakeCommitter{manifests: make(map[string]*index.FakeManifest)}
}

// SetManifest gives a vchannel a manifest, nil removes it.
func (c *FakeCommitter) SetManifest(vchannel string, m *index.FakeManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m == nil {
		delete(c.manifests, vchannel)
		return
	}
	c.manifests[vchannel] = m
}

// SetLookupFailures makes the next n manifest lookups fail with a transient error.
func (c *FakeCommitter) SetLookupFailures(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookupFailures = n
}

// errFakeTransient is the transient failure that SetLookupFailures injects.
var errFakeTransient = errors.New("injected transient failure")

// SetLookupErr makes every later manifest lookup fail with err.
func (c *FakeCommitter) SetLookupErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookupErr = err
}

// GetRecoveryManifests implements Committer.
func (c *FakeCommitter) GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lookupErr != nil {
		return nil, c.lookupErr
	}
	if c.lookupFailures > 0 {
		c.lookupFailures--
		return nil, errors.Wrap(errFakeTransient, "look up the manifests")
	}
	out := make([]RecoveryManifest, len(refs))
	for i, ref := range refs {
		out[i] = FakeRecoveryManifest(ref.VChannel, c.manifests[ref.VChannel])
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

var errFakeNoHandover = errors.New("a handover is not tested here")

// CommitFlush implements Committer. It always fails.
func (c *FakeCommitter) CommitFlush(ctx context.Context, args index.FlushArgs) (index.ManifestView, error) {
	return index.ManifestView{}, errFakeNoHandover
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
		StorageRootPath: "root",
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

// RequestedForTest returns the time tick that an explicit flush asked the
// Index of a vchannel to cover, 0 when none did or the vchannel has no Index.
func (c *PChannelIndexManager) RequestedForTest(vchannel string) uint64 {
	idx := c.get(vchannel)
	if idx == nil {
		return 0
	}
	return c.scheduleOf(idx).requestedTT.Load()
}

// SetCommitterForTest replaces the Committer of the Manager: nil makes the
// node one without a Committer.
func (m *Manager) SetCommitterForTest(c Committer) { m.cfg.Committer = c }
