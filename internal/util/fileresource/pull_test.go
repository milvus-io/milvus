package fileresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// flightWaitContext signals each flight wait, including both Prepare stages.
// FlightGroup evaluates Done after registering the waiter; Err does not signal.
type flightWaitContext struct {
	context.Context
	waiting chan struct{}
}

func (c *flightWaitContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func receiveFlightResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shared work")
		var zero T
		return zero
	}
}

func TestPrepareFileResourcesPublishesFetchedSnapshot(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	var applied *internalpb.SyncFileResourceRequest
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
		require.NoError(t, ctx.Err())
		applied = proto.Clone(&internalpb.SyncFileResourceRequest{Version: version, Resources: resources}).(*internalpb.SyncFileResourceRequest)
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	resource := &internalpb.FileResourceInfo{Id: 3, Name: "model", Path: "models/rank.ubj", StorageName: "store"}
	SetSnapshotFetcher(func(ctx context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		require.EqualValues(t, 4, knownVersion)
		_, bounded := ctx.Deadline()
		require.False(t, bounded, "an individual RPC deadline must not bound shared work")
		return []*internalpb.FileResourceInfo{resource}, 5, nil
	})
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 5, applied.GetVersion())
	require.True(t, proto.Equal(resource, applied.GetResources()[0]))
	for _, version := range []uint64{5, 4} {
		applied = nil
		SetSnapshotFetcher(func(_ context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
			require.EqualValues(t, 5, knownVersion)
			return nil, version, nil
		})
		require.NoError(t, Prepare(t.Context()))
		require.Nil(t, applied, "unchanged and stale responses must not clear resources")
		require.EqualValues(t, 5, manager.GetVersion())
	}
	SetSnapshotFetcher(func(_ context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		require.EqualValues(t, 5, knownVersion)
		return nil, 6, nil
	})
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 6, applied.GetVersion())
	require.Empty(t, applied.GetResources(), "removal publishes the empty newer inventory")
}

func TestPrepareFileResourcesFailureDoesNotPublish(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	apply := mockey.Mock((*SyncManager).Sync).To(func(*SyncManager, context.Context, uint64, []*internalpb.FileResourceInfo) error {
		t.Fatal("failed fetch cannot replace the active resources")
		return nil
	}).Build()
	defer apply.UnPatch()
	SetSnapshotFetcher(func(context.Context, uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		return nil, 0, merr.ErrIoPermissionDenied
	})
	require.ErrorIs(t, Prepare(t.Context()), merr.ErrIoPermissionDenied)
	SetSnapshotFetcher(func(ctx context.Context, _ uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		<-ctx.Done()
		return nil, 0, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, Prepare(ctx), context.Canceled)
	require.EqualValues(t, 4, manager.GetVersion())
	SetSnapshotFetcher(nil)
	require.NoError(t, Prepare(t.Context()), "workers using the existing push path need no fetcher")
}

func TestPrepareFileResourcesRetriesFromAppliedVersion(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	resource := &internalpb.FileResourceInfo{Id: 3, Name: "dictionary", Path: "files/dictionary.txt"}
	SetSnapshotFetcher(func(_ context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		require.EqualValues(t, 4, knownVersion, "a failed application must not acknowledge the fetched version")
		return []*internalpb.FileResourceInfo{resource}, 5, nil
	})
	fail := true
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, _ context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
		require.EqualValues(t, 5, version)
		require.Len(t, resources, 1)
		if fail {
			return merr.ErrIoPermissionDenied
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	require.ErrorIs(t, Prepare(t.Context()), merr.ErrIoPermissionDenied)
	require.EqualValues(t, 4, manager.GetVersion())
	fail = false
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 5, manager.GetVersion())
}

func TestPrepareFileResourcesIgnoresSnapshotOvertakenDuringFetch(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	SetSnapshotFetcher(func(_ context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		require.EqualValues(t, 4, knownVersion)
		// A concurrent pull or push publishes version 6 while this RPC is in flight.
		manager.resourceMap = map[string]int64{"dictionary": 10}
		manager.version.Store(6)
		return nil, 5, nil
	})
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 6, manager.GetVersion())
	require.Equal(t, map[string]int64{"dictionary": 10}, manager.resourceMap)
}

func setupPullTestManager(t *testing.T, version uint64) *SyncManager {
	t.Helper()
	previous := GlobalFileManager
	manager := &SyncManager{version: atomic.NewUint64(version), resourceFileNames: make(map[string]string)}
	GlobalFileManager = manager
	t.Cleanup(func() {
		SetSnapshotFetcher(nil)
		GlobalFileManager = previous
	})
	return manager
}

func TestValidateAnalyzerUsesResourceSnapshot(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	manager.resourceMap = map[string]int64{"dictionary": 10}
	manager.resourceFileNames = map[string]string{"dictionary": "stop.txt"}
	native := mockey.Mock(analyzer.ValidateAnalyzer).To(func(params, extraInfo string) ([]int64, error) {
		// A later sync may replace or mutate the inventory. The native request must
		// still use the version copied under the manager lock, not the global table.
		manager.Lock()
		manager.resourceMap["dictionary"] = 20
		manager.resourceFileNames["dictionary"] = "replacement.txt"
		manager.version.Store(5)
		manager.Unlock()
		require.Equal(t, "{}", params)
		require.JSONEq(t, `{"resource_map":{"dictionary":10},"resource_file_names":{"dictionary":"stop.txt"},"validate_resource_names":true}`, extraInfo)
		var snapshot struct {
			Resources map[string]int64 `json:"resource_map"`
		}
		require.NoError(t, json.Unmarshal([]byte(extraInfo), &snapshot))
		return []int64{snapshot.Resources["dictionary"]}, nil
	}).Build()
	defer native.UnPatch()
	ids, err := ValidateAnalyzer("{}")
	require.NoError(t, err)
	require.Equal(t, []int64{10}, ids)
	require.EqualValues(t, 5, manager.GetVersion())
}

func TestValidateAnalyzerWithoutSyncInventory(t *testing.T) {
	previous := GlobalFileManager
	defer func() { GlobalFileManager = previous }()
	native := mockey.Mock(analyzer.ValidateAnalyzer).To(func(_, extraInfo string) ([]int64, error) {
		require.Empty(t, extraInfo, "non-sync inventories must not classify missing names as input errors")
		return nil, merr.ErrServiceInternal
	}).Build()
	defer native.UnPatch()
	for _, manager := range []Manager{nil, &BaseManager{}, &RefManager{}} {
		GlobalFileManager = manager
		_, err := ValidateAnalyzer("{}")
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func TestSyncPublishesResourceFileNamesAtomically(t *testing.T) {
	mockey.PatchConvey("filenames follow the successfully applied resource snapshot", t, func() {
		manager := setupPullTestManager(t, 0)
		manager.localPath = t.TempDir()
		var downloadErr error
		download := mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, _ context.Context, _ storage.ChunkManager, _ *internalpb.FileResourceInfo, _ string, _ time.Duration) error {
			return downloadErr
		}).Build()
		var activationErr error
		mockey.Mock(analyzer.UpdateGlobalResourceInfo).To(func(map[string]int64) error {
			return activationErr
		}).Build()
		first := []*internalpb.FileResourceInfo{{Id: 10, Name: "dictionary", Path: "dicts/stop.txt"}}
		require.NoError(t, manager.Sync(t.Context(), 1, first))
		require.Equal(t, map[string]string{"dictionary": "stop.txt"}, manager.resourceFileNames)
		require.NoError(t, manager.Sync(t.Context(), 2, first))
		require.Equal(t, 1, download.Times(), "an unchanged resource must reuse its file")
		require.Equal(t, map[string]string{"dictionary": "stop.txt"}, manager.resourceFileNames)
		next := []*internalpb.FileResourceInfo{{Id: 20, Name: "dictionary", Path: "dicts/replacement.txt"}}
		downloadErr = merr.ErrServiceUnavailable
		require.ErrorIs(t, manager.Sync(t.Context(), 3, next), downloadErr)
		downloadErr = nil
		activationErr = merr.ErrServiceInternal
		require.ErrorIs(t, manager.Sync(t.Context(), 3, next), activationErr)
		require.EqualValues(t, 2, manager.GetVersion())
		require.Equal(t, map[string]int64{"dictionary": 10}, manager.resourceMap)
		require.Equal(t, map[string]string{"dictionary": "stop.txt"}, manager.resourceFileNames)
		activationErr = nil
		require.NoError(t, manager.Sync(t.Context(), 3, next))
		require.Equal(t, map[string]int64{"dictionary": 20}, manager.resourceMap)
		require.Equal(t, map[string]string{"dictionary": "replacement.txt"}, manager.resourceFileNames)
		require.NoError(t, manager.Sync(t.Context(), 4, nil))
		require.Empty(t, manager.resourceMap)
		require.Empty(t, manager.resourceFileNames)
	})
}

func TestPrepareSharesConcurrentFetchAndDownload(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	fetchEntered := make(chan struct{}, 2)
	downloadEntered := make(chan context.Context, 2)
	releaseFetch, releaseDownload := make(chan struct{}), make(chan struct{})
	var fetchOnce, downloadOnce sync.Once
	unblockFetch := func() { fetchOnce.Do(func() { close(releaseFetch) }) }
	unblockDownload := func() { downloadOnce.Do(func() { close(releaseDownload) }) }
	defer unblockFetch()
	defer unblockDownload()
	SetSnapshotFetcher(func(ctx context.Context, version uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		fetchEntered <- struct{}{}
		select {
		case <-releaseFetch:
			return []*internalpb.FileResourceInfo{{Id: 7, Name: "dictionary"}}, 1, nil
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	})
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		downloadEntered <- ctx
		select {
		case <-releaseDownload:
			manager.version.Store(version)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Build()
	defer apply.UnPatch()
	callerCtx, cancel := context.WithCancel(t.Context())
	firstCtx := &flightWaitContext{Context: callerCtx, waiting: make(chan struct{}, 2)}
	defer cancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- Prepare(firstCtx) }()
	receiveFlightResult(t, fetchEntered)
	secondCtx := &flightWaitContext{Context: t.Context(), waiting: make(chan struct{}, 2)}
	go func() { secondDone <- Prepare(secondCtx) }()
	receiveFlightResult(t, secondCtx.waiting)
	unblockFetch()
	downloadCtx := receiveFlightResult(t, downloadEntered)
	receiveFlightResult(t, firstCtx.waiting)  // Snapshot fetch.
	receiveFlightResult(t, firstCtx.waiting)  // Snapshot application.
	receiveFlightResult(t, secondCtx.waiting) // Snapshot application.
	cancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, downloadCtx.Err())
	unblockDownload()
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.EqualValues(t, 1, manager.GetVersion())
	require.Empty(t, fetchEntered)
	require.Empty(t, downloadEntered)
}

func TestPrepareFetchesNewVersionDuringDownload(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	currentVersion := atomic.NewUint64(1)
	fetchVersions := make(chan uint64, 2)
	SetSnapshotFetcher(func(context.Context, uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		version := currentVersion.Load()
		fetchVersions <- version
		return nil, version, nil
	})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		manager.Lock()
		defer manager.Unlock()
		if version == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if version > manager.GetVersion() {
			manager.version.Store(version)
		}
		return nil
	}).Build()
	defer apply.UnPatch()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- Prepare(t.Context()) }()
	receiveFlightResult(t, started)
	require.EqualValues(t, 1, receiveFlightResult(t, fetchVersions))
	currentVersion.Store(2)
	go func() { secondDone <- Prepare(t.Context()) }()
	require.EqualValues(t, 2, receiveFlightResult(t, fetchVersions), "a new caller cannot just join the old download")
	unblock()
	require.NoError(t, receiveFlightResult(t, firstDone))
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.EqualValues(t, 2, manager.GetVersion())
}

func TestCanceledSyncDoesNotPublish(t *testing.T) {
	mockey.PatchConvey("cancellation before publication preserves the active snapshot", t, func() {
		manager := setupPullTestManager(t, 1)
		manager.localPath = t.TempDir()
		manager.resourceMap = map[string]int64{"dictionary": 7}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mockey.Mock((*BaseManager).downloadFile).To(func(*BaseManager, context.Context, storage.ChunkManager, *internalpb.FileResourceInfo, string, time.Duration) error {
			cancel()
			return nil
		}).Build()
		publish := mockey.Mock(analyzer.UpdateGlobalResourceInfo).Return(nil).Build()
		err := manager.Sync(ctx, 2, []*internalpb.FileResourceInfo{{Id: 8, Name: "dictionary", Path: "dict.txt"}})
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 1, manager.GetVersion())
		require.Equal(t, map[string]int64{"dictionary": 7}, manager.resourceMap)
		require.Zero(t, publish.Times())
	})
}

func TestNewerSnapshotKeepsSharedDownloadAlive(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	preparer := &resourcePreparer{manager: manager}
	entered := make(chan context.Context, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var downloads atomic.Int32
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		if manager.GetVersion() == 0 {
			downloads.Inc()
			entered <- ctx
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	firstCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- preparer.syncSnapshot(firstCtx, resourceSnapshot{version: 1}) }()
	workCtx := receiveFlightResult(t, entered)
	secondCtx := t.Context()
	secondWaitCtx := &flightWaitContext{Context: secondCtx, waiting: make(chan struct{}, 2)}
	go func() { secondDone <- preparer.syncSnapshot(secondWaitCtx, resourceSnapshot{version: 2}) }()
	receiveFlightResult(t, secondWaitCtx.waiting)
	cancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, workCtx.Err(), "the newer inventory still references the in-flight dictionary")
	unblock()
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.EqualValues(t, 2, manager.GetVersion())
	require.EqualValues(t, 1, downloads.Load())
}

func TestSnapshotSyncCancelsAfterAllVersionsLeave(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	preparer := &resourcePreparer{manager: manager}
	entered, stopped := make(chan context.Context, 1), make(chan struct{})
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, _ uint64, _ []*internalpb.FileResourceInfo) error {
		defer close(stopped)
		entered <- ctx
		<-ctx.Done()
		return ctx.Err()
	}).Build()
	defer apply.UnPatch()
	firstCtx, firstCancel := context.WithCancel(t.Context())
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(t.Context())
	defer secondCancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- preparer.syncSnapshot(firstCtx, resourceSnapshot{version: 1}) }()
	workerCtx := receiveFlightResult(t, entered)
	secondWaitCtx := &flightWaitContext{Context: secondCtx, waiting: make(chan struct{}, 2)}
	go func() { secondDone <- preparer.syncSnapshot(secondWaitCtx, resourceSnapshot{version: 2}) }()
	receiveFlightResult(t, secondWaitCtx.waiting)
	firstCancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, workerCtx.Err())
	secondCancel()
	require.ErrorIs(t, receiveFlightResult(t, secondDone), context.Canceled)
	receiveFlightResult(t, stopped)
	require.ErrorIs(t, workerCtx.Err(), context.Canceled)
	require.Zero(t, manager.GetVersion())
}

func TestPrepareRetriesTransientFailuresWithinCall(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	fetchCalls, syncCalls := 0, 0
	SetSnapshotFetcher(func(_ context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		fetchCalls++
		require.EqualValues(t, 4, knownVersion)
		if fetchCalls == 1 {
			return nil, 0, merr.ErrServiceUnavailable
		}
		return []*internalpb.FileResourceInfo{{Id: 7, Name: "dictionary"}}, 5, nil
	})
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		syncCalls++
		require.NoError(t, ctx.Err())
		require.EqualValues(t, 4, manager.GetVersion())
		if syncCalls == 1 {
			return context.DeadlineExceeded // A per-file timeout, not the shared lifetime.
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, Prepare(ctx), "one request should recover without an upstream retry")
	require.Equal(t, 2, fetchCalls)
	require.Equal(t, 2, syncCalls)
	require.EqualValues(t, 5, manager.GetVersion())
}

func TestPrepareFetchRetryStopsWhenAllWaitersLeave(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	entered := make(chan context.Context, 1)
	var once sync.Once
	SetSnapshotFetcher(func(ctx context.Context, _ uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		once.Do(func() { entered <- ctx })
		return nil, 0, merr.ErrServiceUnavailable
	})
	firstCtx, firstCancel := context.WithCancel(t.Context())
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(t.Context())
	defer secondCancel()
	secondWaitCtx := &flightWaitContext{Context: secondCtx, waiting: make(chan struct{}, 2)}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- Prepare(firstCtx) }()
	workerCtx := receiveFlightResult(t, entered)
	go func() { secondDone <- Prepare(secondWaitCtx) }()
	receiveFlightResult(t, secondWaitCtx.waiting)
	firstCancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, workerCtx.Err())
	secondCancel()
	require.ErrorIs(t, receiveFlightResult(t, secondDone), context.Canceled)
	receiveFlightResult(t, workerCtx.Done())
	require.ErrorIs(t, workerCtx.Err(), context.Canceled)
	require.Zero(t, manager.GetVersion())
}

func TestPreparationRetryReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	go func() {
		done <- retryPreparation(ctx, func() error {
			once.Do(func() { close(entered) })
			return merr.ErrServiceUnavailable
		})
	}()
	receiveFlightResult(t, entered)
	cancel()
	require.ErrorIs(t, receiveFlightResult(t, done), context.Canceled,
		"retry must not leak its previous transient error on cancellation")
}

func TestSnapshotSyncRetryUsesNewestTarget(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	preparer := &resourcePreparer{manager: manager}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var versions []uint64
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		versions = append(versions, version)
		if version == 1 {
			close(entered)
			select {
			case <-release:
				return merr.ErrServiceUnavailable
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- preparer.syncSnapshot(ctx, resourceSnapshot{version: 1}) }()
	receiveFlightResult(t, entered)
	secondCtx := &flightWaitContext{Context: ctx, waiting: make(chan struct{}, 2)}
	go func() { secondDone <- preparer.syncSnapshot(secondCtx, resourceSnapshot{version: 2}) }()
	receiveFlightResult(t, secondCtx.waiting)
	unblock()
	require.NoError(t, receiveFlightResult(t, firstDone))
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.Equal(t, []uint64{1, 2}, versions)
	require.EqualValues(t, 2, manager.GetVersion())
}

func TestSnapshotSyncOldFailureDoesNotFailNewerTarget(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	preparer := &resourcePreparer{manager: manager}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var versions []uint64
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		versions = append(versions, version)
		if version == 1 {
			close(entered)
			select {
			case <-release:
				return merr.ErrIoKeyNotFound
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- preparer.syncSnapshot(ctx, resourceSnapshot{version: 1}) }()
	receiveFlightResult(t, entered)
	secondCtx := &flightWaitContext{Context: ctx, waiting: make(chan struct{}, 2)}
	go func() { secondDone <- preparer.syncSnapshot(secondCtx, resourceSnapshot{version: 2}) }()
	receiveFlightResult(t, secondCtx.waiting)
	unblock()
	// Depending on scheduling, v2 may already satisfy the first caller too.
	if err := receiveFlightResult(t, firstDone); err != nil {
		require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	}
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.Equal(t, []uint64{1, 2}, versions)
	require.EqualValues(t, 2, manager.GetVersion())
}

func TestSnapshotSyncNewerFailurePreservesSatisfiedCaller(t *testing.T) {
	manager := setupPullTestManager(t, 0)
	preparer := &resourcePreparer{manager: manager}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	apply := mockey.Mock((*SyncManager).Sync).To(func(_ *SyncManager, ctx context.Context, version uint64, _ []*internalpb.FileResourceInfo) error {
		require.EqualValues(t, 2, version)
		close(entered)
		select {
		case <-release:
			// Another application has installed the older caller's target.
			manager.version.Store(1)
			return merr.ErrIoPermissionDenied
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Build()
	defer apply.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	newerDone, olderDone := make(chan error, 1), make(chan error, 1)
	go func() { newerDone <- preparer.syncSnapshot(ctx, resourceSnapshot{version: 2}) }()
	receiveFlightResult(t, entered)
	olderCtx := &flightWaitContext{Context: ctx, waiting: make(chan struct{}, 2)}
	go func() { olderDone <- preparer.syncSnapshot(olderCtx, resourceSnapshot{version: 1}) }()
	receiveFlightResult(t, olderCtx.waiting)
	unblock()
	require.NoError(t, receiveFlightResult(t, olderDone))
	require.ErrorIs(t, receiveFlightResult(t, newerDone), merr.ErrIoPermissionDenied)
	require.EqualValues(t, 1, manager.GetVersion())
}

func TestPreparationPermanentErrorsDoNotRetry(t *testing.T) {
	for _, cause := range []error{merr.ErrIoKeyNotFound, merr.ErrIoPermissionDenied, grpcstatus.Error(codes.Unimplemented, "snapshot RPC unsupported")} {
		calls := 0
		err := retryPreparation(t.Context(), func() error {
			calls++
			return cause
		})
		require.ErrorIs(t, err, cause)
		require.Equal(t, 1, calls)
	}
}
