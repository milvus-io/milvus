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

package packed

import (
	"context"
	"errors"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAsyncManifestRoundTrip(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(2)
	defer io.Close()
	ctx := context.Background()
	base := path.Join(cfg.RootPath, "async-segment")
	index := ManifestIndexInfo{ColumnName: "100", IndexName: "index", IndexType: "FLAT", Path: "artifact", FieldID: 100, IndexID: 1, BuildID: 2}
	first, err := CommitManifestUpdatesAsync(ctx, io, base, 0, cfg, &ManifestUpdates{Indexes: []ManifestIndexInfo{index}})
	require.NoError(t, err)
	entries, err := GetManifestIndexInfosAsync(ctx, io, first, cfg)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	syncEntries, err := GetManifestIndexInfos(first, cfg)
	require.NoError(t, err)
	require.Equal(t, entries, syncEntries)
	lobs, err := GetManifestLobFilesAsync(ctx, io, first, cfg)
	require.NoError(t, err)
	require.Empty(t, lobs)
	require.Equal(t, int64(2), entries[0].BuildID)
	_, version, err := UnmarshalManifestPath(first)
	require.NoError(t, err)
	_, err = CommitManifestUpdatesAsync(ctx, io, base, version, cfg, &ManifestUpdates{DropIndexes: []DropIndexEntry{{IndexID: 1, ExpectedBuildID: 3}}})
	require.Error(t, err)
	second, err := CommitManifestUpdatesAsync(ctx, io, base, version, cfg, &ManifestUpdates{DropIndexes: []DropIndexEntry{{IndexID: 1, ExpectedBuildID: 2}}})
	require.NoError(t, err)
	entries, err = GetManifestIndexInfosAsync(ctx, io, second, cfg)
	require.NoError(t, err)
	require.Empty(t, entries)
	_, version, err = UnmarshalManifestPath(second)
	require.NoError(t, err)
	unchanged, err := CommitManifestUpdatesAsync(ctx, io, base, version, cfg, &ManifestUpdates{DropIndexes: []DropIndexEntry{{IndexID: 1, ExpectedBuildID: 2}}})
	require.NoError(t, err)
	require.Equal(t, second, unchanged)
	// Closing one owner never shuts down another coordinator's context.
	io.Close()
	other := NewManifestIOContext(1)
	defer other.Close()
	_, err = GetManifestIndexInfosAsync(ctx, other, first, cfg)
	require.NoError(t, err)
	_, err = GetManifestIndexInfosAsync(ctx, io, first, cfg)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

// Expose a deadline without signalling Go cancellation, so this test can only
// pass when the caller-owned handle carries the timeout into the native queue.
type manifestDeadlineOnlyContext struct {
	context.Context
	deadline time.Time
}

func (ctx manifestDeadlineOnlyContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func TestAsyncManifestNativeQueueDeadline(t *testing.T) {
	for _, operation := range []string{"open", "commit"} {
		t.Run(operation, func(t *testing.T) {
			cfg := manifestTestStorageConfig(t)
			io := NewManifestIOContext(2)
			defer io.Close()
			base := path.Join(cfg.RootPath, "native-deadline")
			invoke := func(ctx context.Context) error {
				_, err := GetManifestIndexInfosAsync(ctx, io, MarshalManifestPath(base, 0), cfg)
				return err
			}
			if operation == "commit" {
				commit, cleanup, err := testOpenManifestCommit(io, base, cfg)
				require.NoError(t, err)
				defer cleanup()
				invoke = func(ctx context.Context) error { _, err := commit(ctx); return err }
			}
			release := make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			for i := 0; i < 2; i++ {
				entered, err := testQueueManifestBlock(io, release)
				require.NoError(t, err)
				<-entered
			}
			ctx := manifestDeadlineOnlyContext{context.Background(), time.Now().Add(200 * time.Millisecond)}
			done := make(chan error, 1)
			go func() { done <- invoke(ctx) }()
			require.Eventually(t, func() bool { return len(io.tasks) == 1 }, time.Second, time.Millisecond)
			<-time.After(time.Until(ctx.deadline) + 20*time.Millisecond)
			unblock.Do(func() { close(release) })
			err := <-done
			require.ErrorIs(t, err, ErrLoonTransient)
			require.ErrorContains(t, err, "did not start")
			if operation == "commit" {
				var commitErr *ManifestCommitError
				require.ErrorAs(t, err, &commitErr)
				require.Equal(t, ManifestNotCommitted, commitErr.Outcome)
			}
		})
	}
}

func TestAsyncManifestQueuedCancelAndCloseDrain(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(1)
	release := make(chan struct{})
	var unblock sync.Once
	defer io.Close()
	defer unblock.Do(func() { close(release) })
	entered, err := testQueueManifestBlock(io, release)
	require.NoError(t, err)
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := GetManifestIndexInfosAsync(ctx, io, MarshalManifestPath(path.Join(cfg.RootPath, "queued"), 0), cfg)
		done <- err
	}()
	require.Eventually(t, func() bool { return len(io.tasks) == 1 }, time.Second, time.Millisecond)
	cancel()
	closed := make(chan struct{})
	go func() { io.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned before the accepted operation drained")
	case <-time.After(20 * time.Millisecond):
	}
	unblock.Do(func() { close(release) })
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain callbacks")
	}
}

func TestAsyncManifestAdmissionRejection(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(1)
	release := make(chan struct{})
	defer io.Close()
	defer close(release)
	entered, err := testQueueManifestBlock(io, release)
	require.NoError(t, err)
	<-entered
	_, err = testQueueManifestBlock(io, release)
	require.NoError(t, err)
	_, err = testQueueManifestBlock(io, release)
	require.NoError(t, err)
	_, err = GetManifestIndexInfosAsync(context.Background(), io, MarshalManifestPath(path.Join(cfg.RootPath, "rejected"), 0), cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLoonTransient)
	called := false
	err = SubmitManifestIndexInfos(context.Background(), io, MarshalManifestPath(path.Join(cfg.RootPath, "rejected"), 0), cfg,
		func([]ManifestIndexInfo, error) { called = true })
	require.ErrorIs(t, err, ErrLoonTransient)
	require.False(t, called, "rejected submissions must not deliver a callback")
	require.Empty(t, io.slots, "rejected submission must release admission")
}

func TestAsyncManifestQueuedCommitDeadline(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(2)
	defer io.Close()
	commit, cleanup, err := testOpenManifestCommit(io, path.Join(cfg.RootPath, "cancel-commit"), cfg)
	require.NoError(t, err)
	defer cleanup()
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		entered, err := testQueueManifestBlock(io, release)
		require.NoError(t, err)
		<-entered
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := commit(ctx); done <- err }()
	require.Eventually(t, func() bool { return len(io.tasks) == 1 }, time.Second, time.Millisecond)
	<-ctx.Done()
	// The native task checks the expired deadline before executing the commit.
	unblock.Do(func() { close(release) })
	var commitErr *ManifestCommitError
	err = <-done
	require.ErrorAs(t, err, &commitErr)
	require.Equal(t, ManifestNotCommitted, commitErr.Outcome)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAsyncManifestCommitOutcomeSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	version, err := (manifestCommitResult{outcome: ManifestCommitted, version: 42}).finish(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), version)
	for _, outcome := range []ManifestCommitOutcome{ManifestNotCommitted, ManifestCommitUnknown} {
		_, err := (manifestCommitResult{outcome: outcome, version: -1, err: ErrLoonTransient}).finish(ctx)
		var commitErr *ManifestCommitError
		require.True(t, errors.As(merr.Wrap(err, "coordinator commit"), &commitErr))
		require.Equal(t, outcome, commitErr.Outcome)
		if outcome == ManifestNotCommitted {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NotErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, ErrLoonTransient)
		}
	}
}

func TestAsyncManifestAdmissionCancellation(t *testing.T) {
	io := NewManifestIOContext(1)
	defer io.Close()
	io.slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, io.acquire(ctx), context.Canceled)
	<-io.slots
}

// Exercise UNKNOWN from the real C callback, rather than only constructing the
// Go error: opening version zero succeeds but commit cannot create its directory.
func TestAsyncManifestNativeCommitFailureIsUnknown(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(1)
	defer io.Close()
	obstacle := path.Join(cfg.RootPath, "file")
	require.NoError(t, os.WriteFile(obstacle, []byte("not a directory"), 0600))
	got, err := CommitManifestUpdatesAsync(context.Background(), io, path.Join(obstacle, "segment"), 0, cfg,
		&ManifestUpdates{DeltaLogs: []DeltaLogEntry{{Path: "delta", NumEntries: 1}}})
	require.Empty(t, got)
	var commitErr *ManifestCommitError
	require.ErrorAs(t, err, &commitErr)
	require.Equal(t, ManifestCommitUnknown, commitErr.Outcome)
}

func TestAsyncManifestCloseWakesAdmissionWaiters(t *testing.T) {
	io := NewManifestIOContext(1)
	require.NoError(t, io.acquire(context.Background()))
	var release sync.Once
	defer io.Close()
	defer release.Do(io.release)
	waiting := make(chan error, 1)
	go func() { waiting <- io.acquire(context.Background()) }()
	closed := make(chan struct{})
	go func() { io.Close(); close(closed) }()
	select {
	case err := <-waiting:
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	case <-time.After(time.Second):
		t.Fatal("Close did not wake queued admission")
	}
	release.Do(io.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after callers drained")
	}
}

func TestAsyncManifestSingleExecutorSubmissions(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(1)
	defer io.Close()
	base := path.Join(cfg.RootPath, "single-executor")
	manifest, err := CommitManifestUpdates(base, 0, cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{{ColumnName: "100", IndexName: "index", IndexType: "FLAT", Path: "artifact", FieldID: 100, IndexID: 1, BuildID: 2}},
	})
	require.NoError(t, err)
	type result struct {
		entries []ManifestIndexInfo
		err     error
	}
	const count = 16
	completed := make(chan result, count)
	submitted := make(chan error, 1)
	go func() {
		for range count {
			if err := SubmitManifestIndexInfos(context.Background(), io, manifest, cfg, func(entries []ManifestIndexInfo, err error) {
				completed <- result{entries, err}
			}); err != nil {
				submitted <- err
				return
			}
		}
		submitted <- nil
	}()
	// Even without a result consumer, callbacks must release admission. With
	// one executor worker this also catches waiting for a callback in that pool.
	select {
	case err := <-submitted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("single executor stalled while submitting reads")
	}
	io.Close()
	require.Len(t, completed, count, "Close must drain all callback deliveries")
	for range count {
		result := <-completed
		require.NoError(t, result.err)
		require.Len(t, result.entries, 1)
		require.Equal(t, int64(2), result.entries[0].BuildID)
	}
	called := false
	err = SubmitManifestIndexInfos(context.Background(), io, manifest, cfg, func([]ManifestIndexInfo, error) { called = true })
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.False(t, called)
}

func TestAsyncManifestSubmissionCancellationDrains(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(1)
	defer io.Close()
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	entered, err := testQueueManifestBlock(io, release)
	require.NoError(t, err)
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, 1)
	require.NoError(t, SubmitManifestIndexInfos(ctx, io, MarshalManifestPath(path.Join(cfg.RootPath, "cancel"), 0), cfg,
		func(_ []ManifestIndexInfo, err error) { completed <- err }))
	cancel()
	closed := make(chan struct{})
	go func() { io.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned before accepted read's callback")
	case <-time.After(20 * time.Millisecond):
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-completed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled read never completed")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain the cancelled read")
	}
}
