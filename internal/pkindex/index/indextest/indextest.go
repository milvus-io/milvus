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

// Package indextest builds an index Manager over a real engine for tests.
package indextest

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/index"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot/snapshottest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Committer applies claims the way the manifest service does. No vchannel
// has a manifest. A handover is not tested with it, so CommitFlush always
// fails.
type Committer struct {
	mu       sync.Mutex
	claimed  map[string]int64
	claimErr error
}

// NewCommitter creates a Committer with no claims.
func NewCommitter() *Committer {
	return &Committer{claimed: make(map[string]int64)}
}

// SetClaimErr makes every later claim fail with err.
func (c *Committer) SetClaimErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.claimErr = err
}

// ClaimPChannel implements snapshot.Committer.
func (c *Committer) ClaimPChannel(ctx context.Context, pchannel string, term int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claimErr != nil {
		return c.claimErr
	}
	next, err := snapshot.FakeClaimPChannel(pchannel, c.claimed[pchannel], term)
	if err != nil {
		return err
	}
	c.claimed[pchannel] = next
	return nil
}

// GetRecoveryManifests implements snapshot.Committer. No vchannel has a
// manifest.
func (c *Committer) GetRecoveryManifests(ctx context.Context, refs []snapshot.VChannelRef) ([]snapshot.RecoveryManifest, error) {
	return make([]snapshot.RecoveryManifest, len(refs)), nil
}

var errNoHandover = errors.New("a handover is not tested here")

// CommitFlush implements snapshot.Committer. It always fails.
func (c *Committer) CommitFlush(ctx context.Context, args snapshot.FlushArgs) (snapshot.ManifestView, error) {
	return snapshot.ManifestView{}, errNoHandover
}

// NeverScheduler accepts every task and never runs it.
type NeverScheduler struct{}

// Submit returns a handle whose task never runs.
func (NeverScheduler) Submit(nodescheduler.Task) nodescheduler.TaskHandle { return neverHandle{} }

type neverHandle struct{}

func (neverHandle) Cancel() {}

func (neverHandle) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Env is an index Manager and the Committer it uses.
type Env struct {
	Manager   *index.Manager
	Committer *Committer
}

// NewEnv creates an index Manager over a real engine with root as the local
// root directory. No size or tail ever starts a handover. The test cleanup
// closes the Manager and releases the engine caches.
func NewEnv(t *testing.T, root string) *Env {
	env := &Env{Committer: NewCommitter()}
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 8 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	params := index.Params{
		LockStripes:   16,
		ActiveBytes:   math.MaxUint64,
		TailBytes:     math.MaxUint64,
		SoftWatermark: math.MaxUint64,
		BackoffBase:   time.Millisecond,
		MaxBackoff:    10 * time.Millisecond,
		ReuploadAfter: time.Hour,
	}
	env.Manager = index.NewManager(index.ManagerConfig{
		Root:            root,
		Shared:          shared,
		AllocID:         func(context.Context) (int64, error) { return nextID.Add(1), nil },
		Store:           snapshottest.NewStore(),
		StorageRootPath: "root",
		Committer:       env.Committer,
		Sched:           NeverScheduler{},
		Params:          func() index.Params { return params },
	})
	t.Cleanup(env.Manager.Close)
	return env
}

// NewManager creates the index Manager of NewEnv.
func NewManager(t *testing.T, root string) *index.Manager {
	return NewEnv(t, root).Manager
}
