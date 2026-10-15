package fileresource

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type SnapshotFetcher func(context.Context) ([]*internalpb.FileResourceInfo, uint64, error)

var snapshotFetcher struct {
	sync.RWMutex
	fetch SnapshotFetcher
}

// SetSnapshotFetcher enables preparation by pull for workers that are not
// members of QueryCoord's legacy file-resource push distribution.
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
	resources, version, err := fetch(fetchCtx)
	if err != nil {
		return err
	}
	return Sync(ctx, version, resources)
}
