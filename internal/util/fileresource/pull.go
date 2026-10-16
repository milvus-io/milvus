package fileresource

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SnapshotFetcher returns the current inventory version and, only when it
// differs from knownVersion, the full inventory. knownVersion must be the last
// successfully applied version. An unchanged response omits the inventory;
// a newer response with an empty inventory means all resources were removed.
type SnapshotFetcher func(ctx context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error)

var snapshotFetcher struct {
	sync.RWMutex
	fetch SnapshotFetcher
}

// SetSnapshotFetcher must be called after InitManager. It enables preparation
// by pull for workers outside QueryCoord's legacy file-resource push distribution.
func SetSnapshotFetcher(fetch SnapshotFetcher) {
	snapshotFetcher.Lock()
	snapshotFetcher.fetch = fetch
	snapshotFetcher.Unlock()
}

// Prepare refreshes the atomic resource inventory before creating a consumer.
// Sync publishes only after downloads finish and ignores stale versions; it also
// notifies analyzer, XGBoost and PyUDF consumers using the same resource paths.
// Existing consumers do not hold the preparation lock while doing their work.
func Prepare(ctx context.Context) error {
	snapshotFetcher.RLock()
	fetch := snapshotFetcher.fetch
	snapshotFetcher.RUnlock()
	if fetch == nil {
		return nil
	}
	// Background runner initialization also needs a bounded coordinator call.
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	knownVersion := GlobalFileManager.GetVersion()
	resources, version, err := fetch(fetchCtx, knownVersion)
	if err != nil {
		return err
	}
	if version <= knownVersion {
		return nil
	}
	// Never infer "unchanged" from an empty list: a newer empty snapshot clears
	// the inventory. Sync rechecks the version under its lock in case another
	// pull or the legacy push path has advanced it while this RPC was in flight.
	return Sync(ctx, version, resources)
}

// ValidateAnalyzer must follow a successful Prepare. A sync-mode inventory is
// authoritative for name validation; other modes cannot blame a missing name on
// the caller. Explicit extra info keeps all native lookups on the same snapshot.
func ValidateAnalyzer(params string) ([]int64, error) {
	manager, ok := GlobalFileManager.(*SyncManager)
	if !ok {
		return analyzer.ValidateAnalyzer(params, "")
	}
	manager.Lock()
	// Marshal under the lock: Sync cannot mutate the map midway through the copy.
	extraInfo, err := json.Marshal(map[string]any{
		"resource_map":            manager.resourceMap,
		"resource_file_names":     manager.resourceFileNames,
		"validate_resource_names": true,
	})
	manager.Unlock()
	if err != nil {
		return nil, merr.Wrap(err, "marshal analyzer validation resource snapshot")
	}
	// Sync can remove files after the snapshot was captured. Native file I/O errors
	// retain their system classification, and RootCoord still reserves returned IDs.
	return analyzer.ValidateAnalyzer(params, string(extraInfo))
}
