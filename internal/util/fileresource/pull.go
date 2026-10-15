package fileresource

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
