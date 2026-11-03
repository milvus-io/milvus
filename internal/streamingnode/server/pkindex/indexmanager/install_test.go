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

package indexmanager

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// committedTable builds one committed table of vchannel v1 in a first life
// of the pchannel: it writes rows, prepares a handover, uploads the table
// and returns the manifest view that lists it. The local committed copy is
// removed when keepLocal is false, so an install must download the table.
func committedTable(t *testing.T, env *FakeEnv, keepLocal bool) manifest.View {
	ctx := context.Background()
	m := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	idx, _, err := m.OpenIndex(ctx, IndexSpec{VChannel: "v1", CollectionID: 1, IndexID: int64(1001)})
	require.NoError(t, err)
	idx.SetCommitted(nil)
	require.NoError(t, m.Apply(ctx, "v1", index.Write{Puts: puts(1, 1, 2, 3), TimeTick: 50}))
	m.ObserveTimeTick(50, WALTailSnapshot{})
	p, err := index.PrepareForTest(ctx, idx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.Request().AddTables[0]
	store := StoreForTest(env.Manager).(*index.FakeStore)
	require.NoError(t, store.UploadFile(ctx, manifest.SSTKey(index.SSTBasePathForTest(idx), int64(ref.ID)), p.Local()[ref.ID]))
	view := manifest.View{Version: 1, SSTBasePath: index.SSTBasePathForTest(idx), CheckpointTimeTick: 50, Tables: []manifest.TableRef{ref}}
	if !keepLocal {
		require.NoError(t, os.Remove(index.CommittedPathForTest(index.CommittedDirForTest(idx), ref.ID)))
	}
	m.Close()
	require.Nil(t, GetForTest(env.Manager, "p"))
	return view
}

func TestInstallTaskRetriesAndMarksReady(t *testing.T) {
	env := NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)

	env.Committer.SetManifest("v1", &manifest.FakeManifest{View: view})
	store := StoreForTest(env.Manager).(*index.FakeStore)
	store.FailDownloads(2)
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	rec, err := recoverInstallTest(t, env, sched,
		PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}},
	)
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close() })
	idx := IndexForTest(GetForTest(env.Manager, "p"), "v1")
	require.NotNil(t, idx)
	assert.Equal(t, uint64(50), idx.HandoverState().LastCommittedTimeTick)
	assert.False(t, idx.Ready(), "not before Run")

	rec.Start()
	require.Eventually(t, idx.Ready, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), index.InstalledVersionForTest(idx))
}

func TestInstallTaskReportsAMissingObjectAsFatal(t *testing.T) {
	env := NewFakeEnv(t, t.TempDir())
	view := manifest.View{Version: 1, SSTBasePath: "root/1/v1", CheckpointTimeTick: 50, Tables: []manifest.TableRef{{ID: 77, Size: 10}}}
	env.Committer.SetManifest("v1", &manifest.FakeManifest{View: view})
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	counter := metrics.StreamingNodePKIndexFatalTotal.WithLabelValues(paramtable.GetStringNodeID(), ErrorClassCorrupted)
	before := testutil.ToFloat64(counter)

	var mu sync.Mutex
	var fatals []error
	rec, err := recoverInstallTest(t, env, sched,
		PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}, OnFatal: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			fatals = append(fatals, err)
		}},
	)
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close() })
	rec.Start()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fatals) == 1
	}, 10*time.Second, 10*time.Millisecond)
	mu.Lock()
	fatal := fatals[0]
	mu.Unlock()
	assert.True(t, errors.Is(fatal, pkroot.ErrCorrupted), "got %v", fatal)
	assert.False(t, IndexForTest(GetForTest(env.Manager, "p"), "v1").Ready())
	assert.Equal(t, before+1, testutil.ToFloat64(counter), "the install fatal counts in the metric")
}

func TestInstallTaskStopsAtClose(t *testing.T) {
	env := NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)
	env.Committer.SetManifest("v1", &manifest.FakeManifest{View: view})
	store := StoreForTest(env.Manager).(*index.FakeStore)
	store.FailDownloads(1 << 30) // every download fails: the install retries until it is canceled
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	rec, err := recoverInstallTest(t, env, sched,
		PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}},
	)
	require.NoError(t, err)
	rec.Start()
	time.Sleep(50 * time.Millisecond) // let the task start its retries
	done := make(chan struct{})
	go func() {
		rec.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return while an install retries")
	}
	assert.Nil(t, GetForTest(env.Manager, "p"))
}

func TestInstallTaskEndsWhenTheIndexCloses(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)
	store := StoreForTest(env.Manager).(*index.FakeStore)
	store.FailDownloads(1 << 30) // every download fails: the install retries until the Index closes
	m := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}})
	t.Cleanup(m.Close)
	idx, _, err := m.OpenIndex(ctx, IndexSpec{VChannel: "v1", CollectionID: 1, IndexID: int64(1001)})
	require.NoError(t, err)
	idx.SetCommitted(&view)

	var mu sync.Mutex
	var fatals []error
	task := &installTask{
		idx:    idx,
		params: env.Manager.(*managerImpl).cfg.Params,
		onFatal: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			fatals = append(fatals, err)
		},
		logger: mlog.With(),
	}
	done := make(chan error, 1)
	go func() { done <- task.Execute(context.Background()) }()
	time.Sleep(50 * time.Millisecond) // let the task start its retries
	RemoveForTest(ctx, m, "v1")
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Execute did not return after the Index closed")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, fatals)
}

func recoverInstallTest(t *testing.T, env *FakeEnv, sched nodescheduler.Scheduler, cfg PChannelConfig) (PChannel, error) {
	t.Helper()
	SetSchedForTest(env.Manager, sched)
	ch := env.Manager.New(cfg)
	_, err := ch.RecoverIndexes(context.Background(), []IndexSpec{{VChannel: "v1", CollectionID: 1, IndexID: 1001}})
	return ch, err
}
