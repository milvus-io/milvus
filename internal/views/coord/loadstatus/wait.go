package loadstatus

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Wait uses the same version-checked predicate as public progress reporting.
func Wait(ctx context.Context, store *loadmgr.LoadConfigStore, registry *coordview.ShardViewRegistry, changes *readiness.Notifications, collectionID int64, vchannels []string, schemaVersion int32, dataVersion qviews.DataVersion) error {
	var schema *int32
	if schemaVersion != 0 {
		schema = &schemaVersion
	}
	return wait(ctx, store, registry, changes, collectionID, vchannels, schema, dataVersion)
}

// WaitAtSchema waits for an exact read schema, including version zero.
func WaitAtSchema(ctx context.Context, store *loadmgr.LoadConfigStore, registry *coordview.ShardViewRegistry, changes *readiness.Notifications, collectionID int64, vchannels []string, schemaVersion int32) error {
	return wait(ctx, store, registry, changes, collectionID, vchannels, &schemaVersion, qviews.DataVersion{})
}

func wait(ctx context.Context, store *loadmgr.LoadConfigStore, registry *coordview.ShardViewRegistry, changes *readiness.Notifications, collectionID int64, vchannels []string, schemaVersion *int32, dataVersion qviews.DataVersion) error {
	// Subscribe before checking so state changes cannot be lost before blocking.
	subscription := changes.Subscribe(collectionID)
	defer subscription.Close()
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		select {
		case <-changes.Done():
			return merr.WrapErrServiceUnavailableMsg("QueryView coordinator stopped while waiting for collection %d", collectionID)
		default:
		}
		changed, released := subscription.Observe()
		if released {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		// The same configuration-version-checked result drives public loading
		// progress and AutoLoad readiness. Observe release again after the read
		// so a coalesced release/reload cannot satisfy an older subscriber.
		progress := getAtVersions(store, registry, collectionID, vchannels, schemaVersion, dataVersion)
		if progress.Config == nil {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		if _, released := subscription.Observe(); released {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		if progress.Ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changes.Done():
			return merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
		case <-changed:
		}
	}
}
