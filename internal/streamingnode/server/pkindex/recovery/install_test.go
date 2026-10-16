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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// insert writes pks into segment segmentID through the Authority of idx.
func insert(t *testing.T, idx *index.Index, segmentID int64, pks ...int64) {
	ctx := context.Background()
	keys := make([]authority.PK, 0, len(pks))
	for _, pk := range pks {
		keys = append(keys, authority.Int64PK(pk))
	}
	intent, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindInsert, VChannel: idx.VChannel(), PKs: keys})
	require.NoError(t, err)
	intent.Apply(ctx, authority.Applied{SegmentID: segmentID, TimeTick: 50})
}

// committedTable builds one committed table of vchannel v1 in a first life
// of the pchannel: it writes rows, prepares a handover, uploads the table
// and returns the manifest view that lists it. The local committed copy is
// removed when keepLocal is false, so an install must download the table.
func committedTable(t *testing.T, env *pkindex.FakeEnv, keepLocal bool) index.ManifestView {
	ctx := context.Background()
	m := env.Manager.New(pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}})
	idx, _, err := m.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	idx.MarkReady(ctx, false)
	insert(t, idx, 1, 1, 2, 3)
	m.ObserveTimeTick(50)
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
	require.Nil(t, env.Manager.Get("p"))
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
		Info:  types.PChannelInfo{Name: "p", Term: 2},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched: sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	idx := env.Manager.Get("p").IndexForTest("v1")
	require.NotNil(t, idx)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
	assert.False(t, idx.Ready(), "not before Finish")

	require.NoError(t, rec.Finish(ctx))
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

	var mu sync.Mutex
	var fatals []error
	rec, err := Recover(ctx, env.Manager, Param{
		Info:  types.PChannelInfo{Name: "p", Term: 2},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched: sched,
		OnFatal: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			fatals = append(fatals, err)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	require.NoError(t, rec.Finish(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fatals) == 1
	}, 10*time.Second, 10*time.Millisecond)
	mu.Lock()
	fatal := fatals[0]
	mu.Unlock()
	assert.True(t, errors.Is(fatal, pkroot.ErrCorrupted), "got %v", fatal)
	assert.False(t, env.Manager.Get("p").IndexForTest("v1").Ready())
}

func TestInstallTaskStopsAtClose(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	view := committedTable(t, env, false)
	env.Committer.SetManifest("v1", &index.FakeManifest{View: view})
	store := env.Manager.StoreForTest().(*index.FakeStore)
	store.FailDownloads(1 << 30) // every download fails: the install retries until it is cancelled
	sched := nodescheduler.New(2)
	t.Cleanup(sched.Close)
	rec, err := Recover(ctx, env.Manager, Param{
		Info:  types.PChannelInfo{Name: "p", Term: 2},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched: sched,
	})
	require.NoError(t, err)
	require.NoError(t, rec.Finish(ctx))
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
	assert.Nil(t, env.Manager.Get("p"))
}
