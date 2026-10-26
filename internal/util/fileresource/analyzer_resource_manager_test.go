package fileresource

import (
	"context"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Capture expiry callbacks so the tests can run an old callback after a new
// consumer or lease has taken ownership, without relying on scheduler timing.
func captureResourceExpirations() *[]func() {
	callbacks := new([]func())
	mockey.Mock(time.AfterFunc).To(func(_ time.Duration, f func()) *time.Timer {
		*callbacks = append(*callbacks, f)
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		return timer
	}).Build()
	return callbacks
}

func TestAnalyzerResourceManagerRenewsIdleLease(t *testing.T) {
	mockey.PatchConvey("sequential consumers reuse files and stale expiry cannot remove a renewed lease", t, func() {
		fetches := 0
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			fetches++
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = time.Hour
		expirations := captureResourceExpirations()
		download := mockResourceDownload()
		first, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		second, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		first.Close()
		require.True(t, manager.entries[7].lastActive.IsZero(), "another consumer still holds the resource")
		require.Empty(t, *expirations)
		second.Close()
		second.Close()
		require.False(t, manager.entries[7].lastActive.IsZero())
		require.Len(t, *expirations, 1)

		third, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		require.Equal(t, 1, fetches)
		require.Equal(t, 1, download.Times())
		require.True(t, manager.entries[7].lastActive.IsZero())
		(*expirations)[0]() // A callback already dispatched before timer.Stop.
		require.FileExists(t, path.Join(manager.resourcePath(7), "stop.txt"))
		third.Close()
		require.Len(t, *expirations, 2)
		manager.mu.Lock()
		manager.entries[7].lastActive = time.Now().Add(-2 * manager.lease)
		manager.mu.Unlock()
		(*expirations)[0]() // The old callback cannot act on the newer idle lease.
		require.Contains(t, manager.entries, int64(7))
		(*expirations)[1]()
		require.Empty(t, manager.entries)
		require.NoDirExists(t, manager.resourcePath(7))

		fourth, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		defer fourth.Close()
		require.Equal(t, 2, fetches)
		require.Equal(t, 2, download.Times())
	})
}

func TestAnalyzerResourceManagerExpiresWithoutAnotherRequest(t *testing.T) {
	mockey.PatchConvey("idle resources are evicted even when there are no further callers", t, func() {
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = 10 * time.Millisecond
		mockResourceDownload()
		resources, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		resources.Close()
		require.Eventually(t, func() bool {
			manager.mu.Lock()
			defer manager.mu.Unlock()
			return len(manager.entries) == 0
		}, 5*time.Second, time.Millisecond)
		require.NoDirExists(t, manager.resourcePath(7))
	})
}

func TestAnalyzerResourceManagerDoesNotReuseExpiredEntry(t *testing.T) {
	mockey.PatchConvey("acquisition checks the lease even if its timer has not run", t, func() {
		fetches := 0
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			fetches++
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = time.Hour
		expirations := captureResourceExpirations()
		download := mockResourceDownload()
		first, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		first.Close()
		oldEntry := manager.entries[7]
		oldEntry.lastActive = time.Now().Add(-2 * manager.lease)

		second, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		defer second.Close()
		require.NotSame(t, oldEntry, manager.entries[7])
		require.Equal(t, 2, fetches)
		require.Equal(t, 2, download.Times())
		(*expirations)[0]()
		require.FileExists(t, path.Join(manager.resourcePath(7), "stop.txt"))
	})
}

func TestAnalyzerResourceManagerRefreshesNamesButReusesFiles(t *testing.T) {
	mockey.PatchConvey("idle retention must not cache a stale name binding", t, func() {
		id, lookups := int64(7), 0
		var lookupErr error
		manager := setupResourceLoader(t, nil, func(_ context.Context, _ string) (*internalpb.FileResourceInfo, error) {
			lookups++
			if lookupErr != nil {
				return nil, lookupErr
			}
			return dictionaryInfo(id), nil
		})
		manager.lease = time.Hour
		captureResourceExpirations()
		download := mockResourceDownload()
		for range 2 {
			resources, err := Acquire(t.Context(), nil, remoteDictionaryParams)
			require.NoError(t, err)
			resources.Close()
		}
		require.Equal(t, 2, lookups)
		require.Equal(t, 1, download.Times(), "validation and inline calls reuse the same ID's idle file")

		lookupErr = merr.WrapErrParameterInvalidMsg("resource name no longer exists")
		_, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.ErrorIs(t, err, lookupErr)
		require.Equal(t, 1, download.Times(), "cached files must not hide name removal")
		lookupErr = nil
		id = 8
		recreated, err := Acquire(t.Context(), nil, remoteDictionaryParams)
		require.NoError(t, err)
		defer recreated.Close()
		require.JSONEq(t, `{"storage_name":"analyzer-demand","resource_map":{"dictionary":8}}`, recreated.ExtraInfo())
		require.Equal(t, 2, download.Times(), "same-name recreation loads the new immutable ID")
	})
}

func TestAnalyzerResourceManagerDoesNotLeaseFailedDownloads(t *testing.T) {
	mockey.PatchConvey("an incomplete download is immediately removed", t, func() {
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = time.Hour
		expirations := captureResourceExpirations()
		mockey.Mock((*BaseManager).downloadFile).To(func(*BaseManager, context.Context, storage.ChunkManager, *internalpb.FileResourceInfo, string, time.Duration) error {
			return merr.ErrIoPermissionDenied
		}).Build()
		_, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
		require.Empty(t, manager.entries)
		require.Empty(t, *expirations)
	})
}

func TestAnalyzerResourceManagerWaitsForAbandonedWorker(t *testing.T) {
	mockey.PatchConvey("expiry and shutdown cannot delete a directory still used by a canceled worker", t, func() {
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = time.Hour
		started, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		mockey.Mock((*BaseManager).downloadFile).To(func(_ *BaseManager, ctx context.Context, _ storage.ChunkManager, _ *internalpb.FileResourceInfo, destination string, _ time.Duration) error {
			if err := os.MkdirAll(path.Dir(destination), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(destination, []byte("partial"), 0o600); err != nil {
				return err
			}
			close(started)
			<-release
			return ctx.Err()
		}).Build()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := Acquire(ctx, []int64{7}, remoteDictionaryParams)
			result <- err
		}()
		receiveFlightResult(t, started)
		cancel()
		require.ErrorIs(t, receiveFlightResult(t, result), context.Canceled)
		manager.mu.Lock()
		entry := manager.entries[7]
		lastActive := entry.lastActive
		entry.lastActive = time.Now().Add(-2 * manager.lease)
		manager.cleanupLocked(entry)
		manager.mu.Unlock()
		require.False(t, lastActive.IsZero())
		manager.Close()
		require.FileExists(t, path.Join(manager.resourcePath(7), "stop.txt"))
		unblock()
		require.Eventually(t, func() bool {
			manager.mu.Lock()
			defer manager.mu.Unlock()
			return len(manager.entries) == 0
		}, 5*time.Second, time.Millisecond)
		require.NoDirExists(t, manager.resourcePath(7))
	})
}

func TestAnalyzerResourceManagerClosePreservesActiveConsumers(t *testing.T) {
	mockey.PatchConvey("shutdown evicts idle files but waits for consumers before removing active files", t, func() {
		manager := setupResourceLoader(t, func(_ context.Context, id int64) (*internalpb.FileResourceInfo, error) {
			return dictionaryInfo(id), nil
		}, nil)
		manager.lease = time.Hour
		expirations := captureResourceExpirations()
		mockResourceDownload()
		idle, err := Acquire(t.Context(), []int64{7}, remoteDictionaryParams)
		require.NoError(t, err)
		idle.Close()
		active, err := Acquire(t.Context(), []int64{8}, remoteDictionaryParams)
		require.NoError(t, err)

		CloseAnalyzerResourceManager()
		require.NoDirExists(t, manager.resourcePath(7))
		require.FileExists(t, path.Join(manager.resourcePath(8), "stop.txt"))
		(*expirations)[0]()
		_, err = Acquire(t.Context(), []int64{8}, remoteDictionaryParams)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		active.Close()
		require.Empty(t, manager.entries)
		require.NoDirExists(t, manager.resourcePath(8))
	})
}

func TestAnalyzerResourceManagerReadsConfiguredLease(t *testing.T) {
	paramtable.Init()
	config := &paramtable.Get().CommonCfg.FileResourceCacheTTL
	require.NoError(t, paramtable.Get().Save(config.Key, "42s"))
	defer paramtable.Get().Reset(config.Key)
	manager := NewAnalyzerResourceManager(nil, nil)
	defer manager.Close()
	require.Equal(t, 42*time.Second, manager.lease)
}
