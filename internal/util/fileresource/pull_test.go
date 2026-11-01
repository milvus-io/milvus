package fileresource

import (
	"context"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

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

func setupResourceLoader(t *testing.T,
	fetch func(context.Context, int64) (*internalpb.FileResourceInfo, error),
	lookup func(context.Context, string) (*internalpb.FileResourceInfo, error),
) *AnalyzerResourceManager {
	t.Helper()
	return setupBatchResourceLoader(t, func(ctx context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
		infos := make([]*internalpb.FileResourceInfo, 0, len(ids)+len(names))
		for _, id := range ids {
			info, err := fetch(ctx, id)
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)
		}
		for _, name := range names {
			info, err := lookup(ctx, name)
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)
		}
		return infos, nil
	})
}

func setupBatchResourceLoader(t *testing.T, fetch ResourceFetcher) *AnalyzerResourceManager {
	t.Helper()
	SetResourceFetcher(nil, fetch)
	loader := analyzerResources.manager
	loader.localPath = t.TempDir()
	// Existing lifetime tests exercise the configurable immediate-eviction mode.
	loader.lease = 0
	t.Cleanup(func() { SetResourceFetcher(nil, nil) })
	return loader
}

func dictionaryInfo(id int64) *internalpb.FileResourceInfo {
	return &internalpb.FileResourceInfo{Id: id, Name: "dictionary", Path: "files/stop.txt"}
}

func mockResourceDownload() *mockey.Mocker {
	return mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, _ context.Context, _ storage.ChunkManager, _ *internalpb.FileResourceInfo, destination string, _ time.Duration) error {
		if err := os.MkdirAll(path.Dir(destination), 0o700); err != nil {
			return err
		}
		return os.WriteFile(destination, []byte("a\nthe"), 0o600)
	}).Build()
}

func TestAcquirePinsSchemaIDsAndFiles(t *testing.T) {
	mockey.PatchConvey("schema IDs bypass names and pin files until the last consumer closes", t, func() {
		calls := 0
		loader := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			calls++
			require.EqualValues(t, 7, id)
			return dictionaryInfo(id), nil
		}, func(context.Context, string) (*internalpb.FileResourceInfo, error) {
			t.Error("bound schema must not resolve by current name")
			return dictionaryInfo(8), nil
		})
		download := mockResourceDownload()
		first, err := Acquire(t.Context(), []int64{7, 7}, remoteDictionaryParams)
		require.NoError(t, err)
		second, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		defer second.Close()
		require.Equal(t, 1, calls)
		require.Equal(t, 1, download.Times())
		require.JSONEq(t, `{"storage_name":"analyzer-demand","resource_map":{"dictionary":7}}`, first.ExtraInfo())
		mockey.Mock(analyzer.ValidateAnalyzer).To(func(_ string, extra string) ([]int64, error) {
			require.JSONEq(t, `{"storage_name":"analyzer-demand","resource_map":{"dictionary":7},"resource_file_names":{"dictionary":"stop.txt"},"validate_resource_names":true}`, extra)
			return []int64{7}, nil
		}).Build()
		ids, err := first.ValidateAnalyzer(remoteDictionaryParams)
		require.NoError(t, err)
		require.Equal(t, []int64{7}, ids)
		first.Close()
		first.Close()
		require.FileExists(t, path.Join(loader.resourcePath(7), "stop.txt"))
		second.Close()
		require.NoDirExists(t, loader.resourcePath(7))
		require.Empty(t, loader.entries)
	})
}

func TestAcquireSharesDownloadAcrossMetadataRequests(t *testing.T) {
	for _, cancelAll := range []bool{false, true} {
		mockey.PatchConvey("independent metadata reads share the same resource download", t, func() {
			var calls atomic.Int32
			setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
				calls.Add(1)
				return dictionaryInfo(id), nil
			}, nil)
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan context.Context, 1)
				stopped, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				download := mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, ctx context.Context, _ storage.ChunkManager, _ *internalpb.FileResourceInfo, _ string, _ time.Duration) error {
					defer close(stopped)
					entered <- ctx
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}).Build()
				type result struct {
					resources *Resources
					err       error
				}
				first, second := make(chan result, 1), make(chan result, 1)
				firstCtx, firstCancel := context.WithCancel(t.Context())
				defer firstCancel()
				secondCtx, secondCancel := context.WithCancel(t.Context())
				defer secondCancel()
				go func() { r, err := Acquire(firstCtx, []int64{7}, remoteDictionaryParams); first <- result{r, err} }()
				workerCtx := receiveFlightResult(t, entered)
				go func() { r, err := Acquire(secondCtx, []int64{7}, remoteDictionaryParams); second <- result{r, err} }()
				// Both callers are now waiting on the same unfinished download.
				synctest.Wait()
				firstCancel()
				require.ErrorIs(t, receiveFlightResult(t, first).err, context.Canceled)
				require.NoError(t, workerCtx.Err())
				if cancelAll {
					secondCancel()
					require.ErrorIs(t, receiveFlightResult(t, second).err, context.Canceled)
					receiveFlightResult(t, stopped)
					require.ErrorIs(t, workerCtx.Err(), context.Canceled)
				} else {
					unblock()
					remaining := receiveFlightResult(t, second)
					require.NoError(t, remaining.err)
					remaining.resources.Close()
				}
				require.EqualValues(t, 2, calls.Load(), "each RPC owns its metadata query")
				require.Equal(t, 1, download.Times())
			})
		})
	}
}

func TestAcquireRetriesTransientFailuresLocally(t *testing.T) {
	for _, failure := range []error{merr.ErrServiceUnavailable, grpcstatus.Error(codes.Unavailable, "offline"), context.DeadlineExceeded} {
		mockey.PatchConvey(failure.Error(), t, func() {
			calls, downloads := 0, 0
			setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
				calls++
				if calls == 1 {
					return nil, failure
				}
				return dictionaryInfo(id), nil
			}, nil)
			mockey.Mock((*BaseManager).downloadFile).To(func(*BaseManager, context.Context, storage.ChunkManager, *internalpb.FileResourceInfo, string, time.Duration) error {
				downloads++
				if downloads == 1 {
					return failure
				}
				return nil
			}).Build()
			resources, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
			require.NoError(t, err)
			resources.Close()
			require.Equal(t, 2, calls, "download retries reuse fetched metadata")
			require.Equal(t, 2, downloads)
		})
	}
}

func TestAcquireFailureIsNotCachedAndDoesNotBlockOtherIDs(t *testing.T) {
	mockey.PatchConvey("failure is scoped to one immutable identity", t, func() {
		var failure error = merr.ErrIoPermissionDenied
		loader := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			if id == 7 && failure != nil {
				return nil, failure
			}
			return dictionaryInfo(id), nil
		}, nil)
		mockResourceDownload()
		_, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.ErrorIs(t, err, failure)
		require.Empty(t, loader.entries)
		other, err := Acquire(t.Context(), []int64{8}, remoteDictionaryParams)
		require.NoError(t, err)
		defer other.Close()
		failure = nil
		recovered, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		recovered.Close()
	})
}

func TestAcquireFailedDownloadReleasesEarlierResources(t *testing.T) {
	mockey.PatchConvey("partial acquisition rolls back references and files", t, func() {
		loader := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, _ context.Context, _ storage.ChunkManager, info *internalpb.FileResourceInfo, destination string, _ time.Duration) error {
			if info.GetId() == 8 {
				return merr.WrapErrIoKeyNotFound(info.GetPath())
			}
			require.NoError(t, os.MkdirAll(path.Dir(destination), 0o700))
			return os.WriteFile(destination, []byte("a"), 0o600)
		}).Build()
		_, err := Acquire(t.Context(), []int64{7, 8}, remoteDictionaryParams)
		require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
		require.Eventually(t, func() bool {
			loader.mu.Lock()
			defer loader.mu.Unlock()
			return len(loader.entries) == 0
		}, 5*time.Second, time.Millisecond)
		require.NoDirExists(t, loader.resourcePath(7))
		require.NoDirExists(t, loader.resourcePath(8))
	})
}

func TestAcquireDoesNotShareStaleNameResolution(t *testing.T) {
	mockey.PatchConvey("a delayed negative lookup cannot reject a newly added name", t, func() {
		started, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var resolutions atomic.Int32
		setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, func(context.Context, string) (*internalpb.FileResourceInfo, error) {
			if resolutions.Add(1) == 1 {
				close(started)
				<-release
				return nil, merr.WrapErrParameterInvalidMsg("dictionary not found")
			}
			return dictionaryInfo(7), nil
		})
		mockResourceDownload()
		first := make(chan error, 1)
		go func() { _, err := Acquire(t.Context(), nil, remoteDictionaryParams); first <- err }()
		receiveFlightResult(t, started)
		resources, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.NoError(t, err)
		defer resources.Close()
		unblock()
		require.ErrorIs(t, receiveFlightResult(t, first), merr.ErrParameterInvalid)
		require.EqualValues(t, 2, resolutions.Load())
	})
}

func TestAcquireResolvingRecreatedNameGetsNewID(t *testing.T) {
	mockey.PatchConvey("resolved names are not cached across requests", t, func() {
		id := int64(7)
		setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, func(context.Context, string) (*internalpb.FileResourceInfo, error) { return dictionaryInfo(id), nil })
		mockResourceDownload()
		first, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.NoError(t, err)
		defer first.Close()
		id = 8
		second, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.NoError(t, err)
		defer second.Close()
		require.Contains(t, first.ExtraInfo(), `"dictionary":7`)
		require.Contains(t, second.ExtraInfo(), `"dictionary":8`)
	})
}

func TestAcquireSkipsResourceFreeConfigs(t *testing.T) {
	setupResourceLoader(t, func(context.Context, int64) (*internalpb.FileResourceInfo, error) {
		t.Error("unrelated resource fetched")
		return nil, retry.Unrecoverable(merr.ErrServiceInternal)
	}, func(context.Context, string) (*internalpb.FileResourceInfo, error) {
		t.Error("resource-free config resolved a name")
		return nil, retry.Unrecoverable(merr.ErrServiceInternal)
	})
	for _, params := range []string{"", "{}", `{"tokenizer":"standard"}`, `{"type":"local","path":"local.txt"}`} {
		resources, err := Acquire(t.Context(), []int64{7}, params)
		require.NoError(t, err)
		require.JSONEq(t, `{"resource_map":{}}`, resources.ExtraInfo())
		resources.Close()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Acquire(ctx, []int64{7}, remoteDictionaryParams)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAcquireReplacementSurvivesAbandonedWorkerCleanup(t *testing.T) {
	mockey.PatchConvey("an abandoned worker cannot remove a replacement consumer's files", t, func() {
		loader := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		started, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var calls atomic.Int32
		mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, ctx context.Context, _ storage.ChunkManager, _ *internalpb.FileResourceInfo, destination string, _ time.Duration) error {
			if calls.Add(1) == 1 {
				close(started)
				<-release // Simulate I/O that takes time to observe cancellation.
				return ctx.Err()
			}
			if err := os.MkdirAll(path.Dir(destination), 0o700); err != nil {
				return err
			}
			return os.WriteFile(destination, []byte("the"), 0o600)
		}).Build()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first := make(chan error, 1)
		go func() { _, err := Acquire(ctx, []int64{7}, remoteDictionaryParams); first <- err }()
		receiveFlightResult(t, started)
		cancel()
		require.ErrorIs(t, receiveFlightResult(t, first), context.Canceled)
		second, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		defer second.Close()
		require.EqualValues(t, 2, calls.Load())
		unblock()
		require.Eventually(t, func() bool {
			loader.mu.Lock()
			defer loader.mu.Unlock()
			return loader.entries[7].workers == 0
		}, 5*time.Second, time.Millisecond)
		require.FileExists(t, path.Join(loader.resourcePath(7), "stop.txt"))
		second.Close()
		require.NoDirExists(t, loader.resourcePath(7))
	})
}

func TestAcquireRejectsMismatchedFetchIdentity(t *testing.T) {
	loader := setupResourceLoader(t, func(context.Context, int64) (*internalpb.FileResourceInfo, error) { return dictionaryInfo(8), nil }, nil)
	_, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Empty(t, loader.entries)
}

func TestAcquireByNameDoesNotFetchAgainByID(t *testing.T) {
	mockey.PatchConvey("inline inputs reuse the metadata returned by name Fetch", t, func() {
		lookups := 0
		setupResourceLoader(t, func(context.Context, int64) (*internalpb.FileResourceInfo, error) {
			t.Error("name Fetch already returned full metadata")
			return nil, retry.Unrecoverable(merr.ErrServiceInternal)
		}, func(_ context.Context, name string) (*internalpb.FileResourceInfo, error) {
			lookups++
			require.Equal(t, "dictionary", name)
			return dictionaryInfo(7), nil
		})
		download := mockResourceDownload()
		resources, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.NoError(t, err)
		defer resources.Close()
		require.Equal(t, 1, lookups)
		require.Equal(t, 1, download.Times())
		require.Contains(t, resources.ExtraInfo(), `"dictionary":7`)
	})
}

func TestAcquireBatchesOnlyMissingMetadata(t *testing.T) {
	mockey.PatchConvey("one metadata RPC contains the uncached unique IDs", t, func() {
		var batches [][]int64
		setupBatchResourceLoader(t, func(_ context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
			require.Empty(t, names)
			batches = append(batches, append([]int64(nil), ids...))
			infos := make([]*internalpb.FileResourceInfo, 0, len(ids))
			for _, id := range ids {
				infos = append(infos, dictionaryInfo(id))
			}
			return infos, nil
		})
		download := mockResourceDownload()
		first, err := Acquire(t.Context(), []int64{7, 8, 7}, remoteDictionaryParams)
		require.NoError(t, err)
		defer first.Close()
		second, err := Acquire(t.Context(), []int64{8, 9}, remoteDictionaryParams)
		require.NoError(t, err)
		defer second.Close()
		require.Equal(t, [][]int64{{7, 8}, {9}}, batches)
		require.Equal(t, 3, download.Times())
	})
}

func TestAcquireBatchesNamesWithoutRefetchingIDs(t *testing.T) {
	mockey.PatchConvey("validation fetches all referenced names in one RPC", t, func() {
		calls := 0
		setupBatchResourceLoader(t, func(_ context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
			calls++
			require.Empty(t, ids)
			require.Equal(t, []string{"dictionary", "synonyms"}, names)
			return []*internalpb.FileResourceInfo{dictionaryInfo(7), {Id: 8, Name: "synonyms", Path: "files/synonyms.txt"}}, nil
		})
		download := mockResourceDownload()
		resources, err := Acquire(t.Context(), nil, remoteDictionaryParams, `{"type":"remote","resource_name":"synonyms","file_name":"synonyms.txt"}`, remoteDictionaryParams)
		require.NoError(t, err)
		defer resources.Close()
		require.Equal(t, 1, calls)
		require.Equal(t, 2, download.Times())
		require.JSONEq(t, `{"storage_name":"analyzer-demand","resource_map":{"dictionary":7,"synonyms":8}}`, resources.ExtraInfo())
	})
}

func TestOverlappingResourceBatchesCancelIndependently(t *testing.T) {
	mockey.PatchConvey("canceling [A,B] does not cancel B still needed by [B,C]", t, func() {
		var batchesMu sync.Mutex
		var batches [][]int64
		setupBatchResourceLoader(t, func(_ context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
			batchesMu.Lock()
			batches = append(batches, append([]int64(nil), ids...))
			batchesMu.Unlock()
			infos := make([]*internalpb.FileResourceInfo, 0, len(ids))
			for _, id := range ids {
				infos = append(infos, dictionaryInfo(id))
			}
			return infos, nil
		})
		synctest.Test(t, func(t *testing.T) {
			entered := map[int64]chan context.Context{7: make(chan context.Context, 2), 8: make(chan context.Context, 2), 9: make(chan context.Context, 2)}
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			download := mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, ctx context.Context, _ storage.ChunkManager, info *internalpb.FileResourceInfo, _ string, _ time.Duration) error {
				entered[info.GetId()] <- ctx
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}).Build()
			firstCtx, cancelFirst := context.WithCancel(t.Context())
			defer cancelFirst()
			first := make(chan error, 1)
			go func() { _, err := Acquire(firstCtx, []int64{7, 8}, remoteDictionaryParams); first <- err }()
			aCtx := receiveFlightResult(t, entered[7])
			bCtx := receiveFlightResult(t, entered[8])
			type result struct {
				resources *Resources
				err       error
			}
			second := make(chan result, 1)
			go func() {
				r, err := Acquire(t.Context(), []int64{8, 9}, remoteDictionaryParams)
				second <- result{r, err}
			}()
			cCtx := receiveFlightResult(t, entered[9])
			// Wait until both batches have registered every resource wait.
			synctest.Wait()
			cancelFirst()
			require.ErrorIs(t, receiveFlightResult(t, first), context.Canceled)
			receiveFlightResult(t, aCtx.Done())
			require.NoError(t, bCtx.Err(), "the surviving caller still owns B")
			require.NoError(t, cCtx.Err())
			unblock()
			remaining := receiveFlightResult(t, second)
			require.NoError(t, remaining.err)
			remaining.resources.Close()
			require.Equal(t, 3, download.Times(), "B downloads only once despite overlapping metadata requests")
			batchesMu.Lock()
			require.Equal(t, [][]int64{{7, 8}, {8, 9}}, batches)
			batchesMu.Unlock()
		})
	})
}

func TestAcquireNamedPinsExactIdentity(t *testing.T) {
	mockey.PatchConvey("name reuse selects a new ID while existing consumers retain the old file", t, func() {
		var id int64 = 7
		manager := setupResourceLoader(t, nil, func(_ context.Context, name string) (*internalpb.FileResourceInfo, error) {
			return &internalpb.FileResourceInfo{Id: id, Name: name, Path: "models/rank.ubj"}, nil
		})
		download := mockResourceDownload()
		first, err := AcquireNamed(t.Context(), "rank")
		require.NoError(t, err)
		second, err := AcquireNamed(t.Context(), "rank")
		require.NoError(t, err)
		require.Equal(t, 1, download.Times())
		old := first.Resolved()[0]
		require.EqualValues(t, 7, old.ID)
		require.FileExists(t, old.LocalPath)
		old.LocalPath = "mutated"
		require.NotEqual(t, old.LocalPath, first.Resolved()[0].LocalPath)
		id = 8
		third, err := AcquireNamed(t.Context(), "rank")
		require.NoError(t, err)
		require.EqualValues(t, 8, third.Resolved()[0].ID)
		first.Close()
		require.FileExists(t, second.Resolved()[0].LocalPath)
		second.Close()
		require.NoDirExists(t, manager.resourcePath(7))
		require.FileExists(t, third.Resolved()[0].LocalPath)
		manager.Close()
		require.FileExists(t, third.Resolved()[0].LocalPath)
		third.Close()
		require.NoDirExists(t, manager.resourcePath(8))
	})
}
