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

package compaction

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// dirStore keeps objects as files under a directory: key -> <root>/<key>.
type dirStore struct{ root string }

func (s dirStore) UploadFile(_ context.Context, key, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dst := filepath.Join(s.root, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0o600)
}

func (s dirStore) DownloadFile(_ context.Context, key, path string) error {
	b, err := os.ReadFile(filepath.Join(s.root, key))
	if os.IsNotExist(err) {
		// The manifest names an object that is gone: permanent, like objstore.
		return errors.Mark(err, pkindex.ErrCorrupted)
	}
	if err != nil {
		return pkindex.MarkIO(err, "read %s", key)
	}
	return os.WriteFile(path, b, 0o600)
}

func testExecutor(t *testing.T, ttl time.Duration) (*Executor, dirStore) {
	t.Helper()
	store := dirStore{root: t.TempDir()}
	cfg := Config{LocalRoot: filepath.Join(t.TempDir(), "pkindex_compaction"), TerminalTaskTTL: ttl, TaskSlot: 4, Parallelism: 2, DiskMargin: 0}
	e, err := New(context.Background(), cfg, store)
	require.NoError(t, err)
	t.Cleanup(e.Close)
	return e, store
}

// uploadInput writes a table into the store under base and returns its ref.
func uploadInput(t *testing.T, store dirStore, base string, id int64, kv ...string) *pkindexpb.SSTRef {
	t.Helper()
	dir := t.TempDir()
	w, err := sst.NewWriter(dir, sst.ID(id))
	require.NoError(t, err)
	for i := 0; i+1 < len(kv); i += 2 {
		require.NoError(t, w.Add([]byte(kv[i]), []byte(kv[i+1])))
	}
	info, err := w.Close()
	require.NoError(t, err)
	require.NoError(t, store.UploadFile(context.Background(), pkindex.SSTKey(base, id), filepath.Join(dir, sst.FileName(sst.ID(id)))))
	return &pkindexpb.SSTRef{Id: id, Size: info.Size, MinKey: info.MinKey, MaxKey: info.MaxKey, NumEntries: info.NumEntries, TableFormat: uint32(info.TableFormat)}
}

func waitState(t *testing.T, e *Executor, taskID int64, want taskcommon.State) *pkindexpb.CompactionResult {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		state, _, result, ok := e.Query(taskID)
		require.True(t, ok)
		if state == want {
			return result
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("task %d did not reach %s", taskID, want)
	return nil
}

func TestExecutorRunsACompaction(t *testing.T) {
	e, store := testExecutor(t, time.Hour)
	base := "sst/1/v"
	newer := uploadInput(t, store, base, 2, "a", "new", "b", "b")
	older := uploadInput(t, store, base, 1, "a", "old", "c", "c")
	req := &pkindexpb.CompactionRequest{TaskId: 7, CollectionId: 1, Vchannel: "v", SstBasePath: base,
		Inputs: []*pkindexpb.SSTRef{newer, older}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20}
	require.NoError(t, e.Create(context.Background(), req))
	require.EqualValues(t, 4, e.Slots())

	result := waitState(t, e, 7, taskcommon.Finished)
	require.Len(t, result.GetOutputs(), 1)
	out := result.GetOutputs()[0]
	require.EqualValues(t, 100, out.GetId())
	require.EqualValues(t, 9, out.GetRunId())
	require.EqualValues(t, 3, out.GetNumEntries())
	require.Equal(t, "a", string(out.GetMinKey()))
	require.Equal(t, "c", string(out.GetMaxKey()))
	require.EqualValues(t, 4, result.GetEntriesProcessed())
	require.False(t, result.GetRetriable())
	require.Zero(t, e.Slots())
	st, err := os.Stat(filepath.Join(store.root, pkindex.SSTKey(base, 100)))
	require.NoError(t, err)
	require.Equal(t, out.GetSize(), st.Size())

	e.Drop(7)
	_, _, _, ok := e.Query(7)
	require.False(t, ok)
	_, err = os.Stat(filepath.Join(e.cfg.LocalRoot, "7-100"))
	require.True(t, os.IsNotExist(err))
}

func TestExecutorFailsOnMissingInput(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	req := &pkindexpb.CompactionRequest{TaskId: 8, SstBasePath: "sst/1/v", RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20,
		Inputs: []*pkindexpb.SSTRef{{Id: 55, Size: 10}}}
	require.NoError(t, e.Create(context.Background(), req))
	result := waitState(t, e, 8, taskcommon.Failed)
	_, reason, _, _ := e.Query(8)
	require.Contains(t, reason, "55")
	// The store reports a missing object as ErrCorrupted: the manifest names
	// an object that is gone, and another node would not find it either.
	require.False(t, result.GetRetriable())
	require.Zero(t, e.Slots())
}

// ioFailStore fails every download with an I/O error.
type ioFailStore struct{ dirStore }

func (ioFailStore) DownloadFile(_ context.Context, key, _ string) error {
	return pkindex.MarkIO(errors.New("connection reset"), "read %s", key)
}

func TestExecutorRetriesOnIOFailure(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	e.store = ioFailStore{}
	req := &pkindexpb.CompactionRequest{TaskId: 14, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 1}}, RunId: 9, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
	require.NoError(t, e.Create(context.Background(), req))
	result := waitState(t, e, 14, taskcommon.Failed)
	require.True(t, result.GetRetriable()) // another node may read it
}

// failingUploadStore fails the nth UploadFile with an I/O error.
type failingUploadStore struct {
	dirStore
	failAt int
	n      atomic.Int32
}

func (s *failingUploadStore) UploadFile(ctx context.Context, key, path string) error {
	if int(s.n.Add(1)) == s.failAt {
		return pkindex.MarkIO(errors.New("put failed"), "write %s", key)
	}
	return s.dirStore.UploadFile(ctx, key, path)
}

func TestExecutorFailsOnUploadFailureMidRun(t *testing.T) {
	e, store := testExecutor(t, time.Hour)
	base := "sst/1/v"
	in := uploadInput(t, store, base, 1, "a", "1", "b", "2", "c", "3", "d", "4")
	e.store = &failingUploadStore{dirStore: store, failAt: 2}
	// A target of one byte cuts a table after every entry.
	req := &pkindexpb.CompactionRequest{TaskId: 15, SstBasePath: base, Inputs: []*pkindexpb.SSTRef{in}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1}
	require.NoError(t, e.Create(context.Background(), req))
	result := waitState(t, e, 15, taskcommon.Failed)
	require.True(t, result.GetRetriable())
	require.Empty(t, result.GetOutputs())
}

// blockingUploadStore downloads from the directory and blocks every upload
// until ctx ends.
type blockingUploadStore struct {
	dirStore
	started chan struct{}
}

func (s blockingUploadStore) UploadFile(ctx context.Context, _, _ string) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestExecutorDropDuringUploadReturnsPromptly(t *testing.T) {
	e, store := testExecutor(t, time.Hour)
	base := "sst/1/v"
	in := uploadInput(t, store, base, 1, "a", "a")
	bs := blockingUploadStore{dirStore: store, started: make(chan struct{}, 1)}
	e.store = bs
	req := &pkindexpb.CompactionRequest{TaskId: 16, SstBasePath: base, Inputs: []*pkindexpb.SSTRef{in}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20}
	require.NoError(t, e.Create(context.Background(), req))
	select {
	case <-bs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the upload did not start")
	}
	done := make(chan struct{})
	go func() { e.Drop(16); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Drop did not cancel the upload")
	}
	require.Zero(t, e.Slots())
}

func TestExecutorCloseMarksARunningTaskRetriable(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	e.store = blockingStore{release: make(chan struct{})}
	req := &pkindexpb.CompactionRequest{TaskId: 17, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 1}}, RunId: 9, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
	require.NoError(t, e.Create(context.Background(), req))
	e.mu.Lock()
	tk := e.tasks[17]
	e.mu.Unlock()
	e.Close()
	state, err, _ := tk.getState()
	require.Equal(t, taskcommon.Failed, state)
	require.Error(t, err)
	require.True(t, retriable(err)) // a cancellation is never the request's fault
}

func TestExecutorCreateAfterCloseFails(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	e.Close()
	req := &pkindexpb.CompactionRequest{TaskId: 18, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 1}}, RunId: 9, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
	err := e.Create(context.Background(), req)
	require.Error(t, err)
	require.True(t, retriable(err))
	_, _, _, ok := e.Query(18)
	require.False(t, ok)
}

func TestExecutorCreateDoesNotBlockWhenWorkersAreBusy(t *testing.T) {
	e, _ := testExecutor(t, time.Hour) // Parallelism: 2
	e.store = blockingStore{release: make(chan struct{})}
	for id := int64(20); id < 23; id++ {
		req := &pkindexpb.CompactionRequest{TaskId: id, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 1}}, RunId: 9, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
		start := time.Now()
		require.NoError(t, e.Create(context.Background(), req))
		require.Less(t, time.Since(start), time.Second)
	}
	state, _, _, ok := e.Query(22)
	require.True(t, ok)
	require.Equal(t, taskcommon.InProgress, state) // waits for a worker
	require.EqualValues(t, 12, e.Slots())          // the waiting task holds a slot
}

func TestExecutorFailsOnWrongSize(t *testing.T) {
	e, store := testExecutor(t, time.Hour)
	base := "sst/1/v"
	in := uploadInput(t, store, base, 1, "a", "a")
	in.Size++ // the manifest says the table is bigger than the object
	req := &pkindexpb.CompactionRequest{TaskId: 11, SstBasePath: base, Inputs: []*pkindexpb.SSTRef{in}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20}
	require.NoError(t, e.Create(context.Background(), req))
	result := waitState(t, e, 11, taskcommon.Failed)
	require.False(t, result.GetRetriable()) // ErrCorrupted: permanent
}

func TestExecutorRejectsBadRequests(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	require.Error(t, e.Create(context.Background(), &pkindexpb.CompactionRequest{TaskId: 1, SstBasePath: "b", OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}))                                       // no inputs
	require.Error(t, e.Create(context.Background(), &pkindexpb.CompactionRequest{TaskId: 1, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1}}, OutputIdBegin: 2, OutputIdEnd: 2, TargetFileSize: 1})) // empty range
	require.Error(t, e.Create(context.Background(), &pkindexpb.CompactionRequest{TaskId: 1, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1}}, OutputIdBegin: 1, OutputIdEnd: 2}))                    // no target size
	require.Error(t, e.Create(context.Background(), &pkindexpb.CompactionRequest{TaskId: 1, Inputs: []*pkindexpb.SSTRef{{Id: 1}}, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}))                   // no base path
	_, _, _, ok := e.Query(1)
	require.False(t, ok)
}

func TestExecutorRejectsWhenDiskIsFull(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	e.freeDisk = func(string) (uint64, error) { return 100, nil }
	req := &pkindexpb.CompactionRequest{TaskId: 12, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 60}}, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
	err := e.Create(context.Background(), req)
	require.Error(t, err)
	require.True(t, retriable(err))
	e.freeDisk = func(string) (uint64, error) { return 1000, nil }
	require.NoError(t, e.Create(context.Background(), req))
	waitState(t, e, 12, taskcommon.Failed) // the input does not exist; only the admission mattered here
}

func TestExecutorReapsTerminalTasks(t *testing.T) {
	e, store := testExecutor(t, 50*time.Millisecond)
	e.Start()
	base := "sst/1/v"
	in := uploadInput(t, store, base, 1, "a", "a")
	req := &pkindexpb.CompactionRequest{TaskId: 9, SstBasePath: base, Inputs: []*pkindexpb.SSTRef{in}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20}
	require.NoError(t, e.Create(context.Background(), req))
	waitState(t, e, 9, taskcommon.Finished)
	// Query refreshes the TTL, so look at the task table without it.
	require.Eventually(t, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		_, ok := e.tasks[9]
		return !ok
	}, 2*time.Second, 10*time.Millisecond)
	_, err := os.Stat(filepath.Join(e.cfg.LocalRoot, "9-100"))
	require.True(t, os.IsNotExist(err))
}

func TestExecutorReplacesARepeatedTask(t *testing.T) {
	e, store := testExecutor(t, time.Hour)
	base := "sst/1/v"
	in := uploadInput(t, store, base, 1, "a", "a")
	req := &pkindexpb.CompactionRequest{TaskId: 10, SstBasePath: base, Inputs: []*pkindexpb.SSTRef{in}, RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20}
	require.NoError(t, e.Create(context.Background(), req))
	waitState(t, e, 10, taskcommon.Finished)
	again := proto.Clone(req).(*pkindexpb.CompactionRequest)
	again.OutputIdBegin, again.OutputIdEnd = 200, 210
	require.NoError(t, e.Create(context.Background(), again))
	result := waitState(t, e, 10, taskcommon.Finished)
	require.EqualValues(t, 200, result.GetOutputs()[0].GetId())
}

func TestExecutorDropCancelsARunningTask(t *testing.T) {
	e, _ := testExecutor(t, time.Hour)
	blocked := make(chan struct{})
	e.store = blockingStore{release: blocked}
	req := &pkindexpb.CompactionRequest{TaskId: 13, SstBasePath: "b", Inputs: []*pkindexpb.SSTRef{{Id: 1, Size: 1}}, RunId: 9, OutputIdBegin: 1, OutputIdEnd: 2, TargetFileSize: 1}
	require.NoError(t, e.Create(context.Background(), req))
	require.EqualValues(t, 4, e.Slots())
	done := make(chan struct{})
	go func() { e.Drop(13); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Drop did not cancel the running task")
	}
	close(blocked)
	require.Zero(t, e.Slots())
}

// blockingStore blocks DownloadFile until ctx ends or release is closed.
type blockingStore struct{ release chan struct{} }

func (s blockingStore) UploadFile(context.Context, string, string) error { return nil }
func (s blockingStore) DownloadFile(ctx context.Context, _, _ string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func TestExecutorWipesLocalRootAtStart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pkindex_compaction")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "42", "in"), 0o755))
	e, err := New(context.Background(), Config{LocalRoot: root, TerminalTaskTTL: time.Hour, TaskSlot: 1, Parallelism: 1}, dirStore{root: t.TempDir()})
	require.NoError(t, err)
	defer e.Close()
	_, err = os.Stat(filepath.Join(root, "42"))
	require.True(t, os.IsNotExist(err))
}
