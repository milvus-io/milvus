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

package recovery

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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// insert writes pks into segment segmentID of idx, as the observe of the
// WAL recovery does.
func insert(t *testing.T, m *pkindex.PChannelIndexManager, idx *index.Index, segmentID int64, pks ...int64) {
	puts := make([]index.Put, 0, len(pks))
	for _, pk := range pks {
		puts = append(puts, index.Put{PK: dedup.Int64PK(pk), SegmentID: segmentID})
	}
	require.NoError(t, m.Apply(context.Background(), idx.VChannel(), index.Write{Puts: puts, TimeTick: 50}))
}

// committedTable builds one committed table of vchannel v1 in a first life
// of the pchannel: it writes rows, prepares a handover, uploads the table
// and returns the manifest view that lists it. The local committed copy is
// removed when keepLocal is false, so an install must download the table.
func committedTable(t *testing.T, env *pkindex.FakeEnv, keepLocal bool) index.ManifestView {
	ctx := context.Background()
	m := env.Manager.New(pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	idx, _, err := m.CreateCollection(ctx, "v1", 1, 0)
	require.NoError(t, err)
	idx.MarkReady(ctx)
	insert(t, m, idx, 1, 1, 2, 3)
	m.ObserveTimeTick(50, pkindex.WALTailSnapshot{})
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.Request().AddTables[0]
	store := env.Manager.StoreForTest().(*index.FakeStore)
	require.NoError(t, store.UploadFile(ctx, pkroot.SSTKey(idx.SSTBasePathForTest(), int64(ref.ID)), p.Local()[ref.ID]))
	view := index.ManifestView{Version: 1, SSTBasePath: idx.SSTBasePathForTest(), CheckpointTimeTick: 50, Tables: []index.TableRef{ref}}
	if !keepLocal {
		require.NoError(t, os.Remove(index.CommittedPathForTest(idx.CommittedDirForTest(), ref.ID)))
	}
	m.Close()
	require.Nil(t, env.Manager.GetForTest("p"))
	return view
}

func TestInstallTaskRetriesAndMarksReady(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)

	env.Committer.SetManifest("v1", &index.FakeManifest{View: view})
	store := env.Manager.StoreForTest().(*index.FakeStore)
	store.FailDownloads(2)
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	require.NotNil(t, idx)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
	assert.False(t, idx.Ready(), "not before Run")

	require.NoError(t, rec.Run(ctx))
	require.Eventually(t, idx.Ready, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), idx.InstalledVersionForTest())
}

func TestInstallTaskReportsAMissingObjectAsFatal(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	view := index.ManifestView{Version: 1, SSTBasePath: "root/1/v1", CheckpointTimeTick: 50, Tables: []index.TableRef{{ID: 77, Size: 10}}}
	env.Committer.SetManifest("v1", &index.FakeManifest{View: view})
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	counter := metrics.StreamingNodePKIndexFatalTotal.WithLabelValues(paramtable.GetStringNodeID(), errorClassCorrupted)
	before := testutil.ToFloat64(counter)

	var mu sync.Mutex
	var fatals []error
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}, OnFatal: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			fatals = append(fatals, err)
		}},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched: sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	require.NoError(t, rec.Run(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fatals) == 1
	}, 10*time.Second, 10*time.Millisecond)
	mu.Lock()
	fatal := fatals[0]
	mu.Unlock()
	assert.True(t, errors.Is(fatal, pkroot.ErrCorrupted), "got %v", fatal)
	assert.False(t, env.Manager.GetForTest("p").IndexForTest("v1").Ready())
	assert.Equal(t, before+1, testutil.ToFloat64(counter), "the install fatal counts in the metric")
}

func TestInstallTaskStopsAtClose(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)
	env.Committer.SetManifest("v1", &index.FakeManifest{View: view})
	store := env.Manager.StoreForTest().(*index.FakeStore)
	store.FailDownloads(1 << 30) // every download fails: the install retries until it is canceled
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          sched,
	})
	require.NoError(t, err)
	require.NoError(t, rec.Run(ctx))
	time.Sleep(50 * time.Millisecond) // let the task start its retries
	done := make(chan struct{})
	go func() {
		rec.Close(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return while an install retries")
	}
	assert.Nil(t, env.Manager.GetForTest("p"))
}

func TestInstallTaskEndsWhenTheIndexCloses(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)
	store := env.Manager.StoreForTest().(*index.FakeStore)
	store.FailDownloads(1 << 30) // every download fails: the install retries until the Index closes
	m := env.Manager.New(pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 2}})
	t.Cleanup(m.Close)
	idx, _, err := m.CreateCollection(ctx, "v1", 1, 0)
	require.NoError(t, err)
	idx.SetCommitted(view)

	var mu sync.Mutex
	var fatals []error
	task := &installTask{
		idx:    idx,
		params: env.Manager.Params,
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
	m.RemoveForTest(ctx, "v1")
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
