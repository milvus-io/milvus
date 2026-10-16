package fileresource

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrepareFileResourcesPublishesFetchedSnapshot(t *testing.T) {
	manager := setupPullTestManager(t, 4)
	var applied *internalpb.SyncFileResourceRequest
	apply := mockey.Mock(Sync).To(func(ctx context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
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
		require.True(t, bounded)
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
	apply := mockey.Mock(Sync).To(func(context.Context, uint64, []*internalpb.FileResourceInfo) error {
		t.Fatal("failed fetch cannot replace the active resources")
		return nil
	}).Build()
	defer apply.UnPatch()
	SetSnapshotFetcher(func(context.Context, uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
		return nil, 0, merr.ErrServiceUnavailable
	})
	require.ErrorIs(t, Prepare(t.Context()), merr.ErrServiceUnavailable)
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
	apply := mockey.Mock(Sync).To(func(_ context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
		require.EqualValues(t, 5, version)
		require.Len(t, resources, 1)
		if fail {
			return merr.ErrServiceUnavailable
		}
		manager.version.Store(version)
		return nil
	}).Build()
	defer apply.UnPatch()
	require.ErrorIs(t, Prepare(t.Context()), merr.ErrServiceUnavailable)
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
