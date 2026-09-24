package queryresource

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReusedRuntimeReadinessWaitsForCommitAndSealApplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	module := &recordingModule{}
	runtime := newQueryRuntime(dispatcher, module)
	defer runtime.Close()
	var owner sync.Mutex
	committed := false
	applied, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var applyOriginal func(*recordingModule, context.Context, walview.VChannelResourceEvent)
	patch := mockey.Mock((*recordingModule).ApplyLiveEvent).To(func(m *recordingModule, ctx context.Context, event walview.VChannelResourceEvent) {
		close(applied)
		<-resume
		applyOriginal(m, ctx, event)
	}).Origin(&applyOriginal).Build()
	defer patch.UnPatch()
	require.NoError(t, runtime.Initialize(ctx, walview.VChannelWALView{
		WithResourceEventLock: func(fn func()) { owner.Lock(); defer owner.Unlock(); fn() },
		PrepareQueryView: func() bool {
			if !committed {
				return false
			}
			return runtime.ObserveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 7}})
		},
	}))
	manager := NewManager(Config{Scheduler: scheduler})
	manager.runtime = runtime
	manager.refs = make(map[qviews.QueryViewKey]queryViewRef)
	meta, key := testManagerQueryViewMetaAndKey(2)
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, nil)
	ready := make(chan struct{})
	err := manager.prepareReady(ctx, key, func() { close(ready) })
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	owner.Lock()
	committed = true
	owner.Unlock()
	done := make(chan error, 1)
	go func() { done <- manager.prepareReady(ctx, key, func() { close(ready) }) }()
	select {
	case <-applied:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-ready:
		t.Fatal("ready before the sealed event was applied")
	default:
	}
	unblock()
	require.NoError(t, <-done)
	require.Equal(t, []int64{7}, module.segmentIDs())
	select {
	case <-ready:
	default:
		t.Fatal("view did not become ready")
	}
}

func TestQueryViewBarrierStopsOnCloseOrCancellation(t *testing.T) {
	for _, closeRuntime := range []bool{false, true} {
		runtime := NewQueryRuntime()
		require.NoError(t, runtime.Initialize(context.Background(), walview.VChannelWALView{}))
		// Leave events queued to model a stalled dispatcher without blocking Close.
		patch := mockey.Mock((*QueryRuntime).submitDrain).Return().Build()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runtime.PrepareQueryView(ctx, qviews.DataVersion{}) }()
		require.Eventually(t, func() bool {
			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			return len(runtime.pending) == 1
		}, time.Second, time.Millisecond)
		if closeRuntime {
			runtime.Close()
		} else {
			cancel()
		}
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("barrier waiter did not stop")
		}
		cancel()
		runtime.Close()
		patch.UnPatch()
	}
}

func TestManagerRejectsOlderCrossReplicaAcquisition(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	manager := NewManager(Config{Scheduler: scheduler})
	manager.runtime = NewQueryRuntime()
	defer manager.Close()
	acquire := func(version int64, replica int64, reject func()) qviews.QueryViewKey {
		meta, key := testManagerQueryViewMetaAndKey(version)
		meta.ReplicaId, key.ShardID.ReplicaID = replica, replica
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnUnrecoverable: reject}, nil)
		return key
	}
	oldest := acquire(1, 1, nil)
	newest := acquire(3, 1, nil)
	// Removing the newest reference must not lower the admission watermark.
	manager.Release(snview.ReleaseResource{Key: newest})
	rejected := make(chan struct{})
	stale := acquire(2, 2, func() { close(rejected) })
	select {
	case <-rejected:
	case <-time.After(time.Second):
		t.Fatal("stale replica view was accepted")
	}
	require.NotContains(t, manager.refs, stale)
	equal := acquire(3, 2, nil)
	require.Contains(t, manager.refs, equal)
	require.Contains(t, manager.refs, oldest, "existing older views retain their resources")
	acquire(1, 1, func() { t.Error("idempotent existing reference rejected") })
	manager.Release(snview.ReleaseResource{Key: oldest})
	manager.Release(snview.ReleaseResource{Key: equal})
	require.Empty(t, manager.refs)
}

func TestConcurrentReplicaReleaseKeepsWatermarksOrdered(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	module := &recordingModule{}
	runtime := NewQueryRuntime(module)
	require.NoError(t, runtime.Initialize(context.Background(), walview.VChannelWALView{}))
	manager := NewManager(Config{Scheduler: scheduler})
	manager.runtime = runtime
	defer manager.Close()
	keys := make([]qviews.QueryViewKey, 3)
	for i := range keys {
		meta, key := testManagerQueryViewMetaAndKey(int64(i + 1))
		meta.ReplicaId, key.ShardID.ReplicaID = int64(i+1), int64(i+1)
		keys[i] = key
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, nil)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var original func(*QueryRuntime, qviews.DataVersion)
	patch := mockey.Mock((*QueryRuntime).Advance).To(func(r *QueryRuntime, v qviews.DataVersion) {
		if v.StreamingVersion == 2 {
			close(entered)
			<-resume
		}
		original(r, v)
	}).Origin(&original).Build()
	defer patch.UnPatch()
	panics := make(chan any, 2)
	var dropped atomic.Int32
	release := func(key qviews.QueryViewKey) {
		defer func() { panics <- recover() }()
		manager.Release(snview.ReleaseResource{Key: key, OnDropped: func() { dropped.Add(1) }})
	}
	go release(keys[0])
	<-entered
	go release(keys[1])
	require.Never(t, func() bool { return len(module.advancedVersions()) != 0 }, 30*time.Millisecond, time.Millisecond)
	unblock()
	require.Nil(t, <-panics)
	require.Nil(t, <-panics)
	require.Equal(t, []qviews.DataVersion{{StreamingVersion: 2}, {StreamingVersion: 3}}, module.advancedVersions())
	require.Eventually(t, func() bool { return dropped.Load() == 2 }, time.Second, time.Millisecond)
}

func TestInitialAdvanceCannotBeOvertakenByReadyAdvance(t *testing.T) {
	module := &recordingModule{}
	runtime := NewQueryRuntime(module)
	defer runtime.Close()
	initial := qviews.DataVersion{StreamingVersion: 2}
	next := qviews.DataVersion{StreamingVersion: 3}
	runtime.Advance(initial)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var original func(*recordingModule, qviews.DataVersion)
	patch := mockey.Mock((*recordingModule).Advance).To(func(m *recordingModule, v qviews.DataVersion) {
		if v.EQ(initial) {
			close(entered)
			<-resume
		}
		original(m, v)
	}).Origin(&original).Build()
	defer patch.UnPatch()
	initialized := make(chan error, 1)
	go func() { initialized <- runtime.Initialize(context.Background(), walview.VChannelWALView{}) }()
	<-entered
	advanced := make(chan struct{})
	go func() { runtime.Advance(next); close(advanced) }()
	require.Never(t, func() bool { return len(module.advancedVersions()) != 0 }, 30*time.Millisecond, time.Millisecond)
	unblock()
	require.NoError(t, <-initialized)
	<-advanced
	require.Equal(t, []qviews.DataVersion{initial, next}, module.advancedVersions())
	runtime.Close()
	require.NotPanics(t, func() { runtime.Advance(initial) })
}
