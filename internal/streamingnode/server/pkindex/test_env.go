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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// FakeCommitter is a manifest service where no vchannel has a manifest. A
// handover is not tested with it, so CommitFlush always fails.
type FakeCommitter struct{}

// NewFakeCommitter creates a FakeCommitter.
func NewFakeCommitter() *FakeCommitter {
	return &FakeCommitter{}
}

// GetRecoveryManifests implements Committer. No vchannel has a
// manifest.
func (c *FakeCommitter) GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error) {
	return make([]RecoveryManifest, len(refs)), nil
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
// root directory. The test cleanup closes the Manager and releases the
// engine caches.
func NewFakeEnv(t *testing.T, root string) *FakeEnv {
	env := &FakeEnv{Committer: NewFakeCommitter()}
	shared := engine.NewSharedResources(engine.SharedConfig{CacheBytes: 8 << 20})
	t.Cleanup(shared.Release)
	var nextID atomic.Int64
	nextID.Store(1000)
	params := Params{Params: index.Params{ReuploadAfter: time.Hour}}
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
