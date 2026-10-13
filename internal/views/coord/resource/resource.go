// Package resource exposes the shared Coordinator QueryView infrastructure.
// DDL owners call it directly; QueryCoord RPC services are not an internal bus.
package resource

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var current atomic.Pointer[Resources]

// DataViewReconciler publishes DataCoord's committed SegmentMeta projection.
// Wiring supplies DataCoord directly, without routing through a Coord RPC.
type DataViewReconciler interface {
	RefreshDataView(context.Context, int64) (*viewpb.DataVersion, error)
}

// Resources borrows the recovered managers owned by Coordinator. Dependencies
// are installed once before DDL callbacks and external requests are enabled.
// The owners retain responsibility for shutting down the underlying managers.
type Resources struct {
	DataViews balancer.DataViewProvider
	Balancer  interface {
		Trigger(...balancer.TriggerScope)
	}
	BalanceCache    *cache.Cache
	LoadConfigs     *loadmgr.LoadConfigStore
	LoadManager     *loadmgr.CollectionLoadManager
	ShardViews      *coordview.ShardViewRegistry
	Readiness       *readiness.Notifications
	Reconciler      DataViewReconciler
	VChannels       func(context.Context, int64) ([]string, error)
	RefreshVersions *sync.Map // collection ID -> greatest requested qviews.DataVersion
}

// Init publishes a fully initialized instance before Coordinator serves DDLs.
func Init(r *Resources) { current.Store(r) }

// Resource returns the current Coordinator's QueryView infrastructure.
// Callers must be inside the initialized Coordinator lifecycle.
func Resource() *Resources { return current.Load() }

// Refresh publishes pending data mutations and waits for their serving views.
func (r *Resources) Refresh(ctx context.Context, collectionID int64) error {
	if err := r.checkActive(ctx); err != nil {
		return err
	}
	version, err := r.Reconciler.RefreshDataView(ctx, collectionID)
	if err != nil {
		return err
	}
	return r.WaitDataVersion(ctx, collectionID, qviews.FromProtoDataVersion(version))
}

// WaitDataVersion waits for all desired replicas/shards to serve the publication.
// Release removes the obligation; a subsequent load uses the committed data.
func (r *Resources) WaitDataVersion(ctx context.Context, collectionID int64, version qviews.DataVersion) error {
	if err := r.checkActive(ctx); err != nil {
		return err
	}
	if !r.LoadConfigs.Contains(collectionID) {
		return nil
	}
	for {
		old, loaded := r.RefreshVersions.LoadOrStore(collectionID, version)
		if !loaded || old.(qviews.DataVersion).GTE(version) || r.RefreshVersions.CompareAndSwap(collectionID, old, version) {
			break
		}
	}
	channels, err := r.VChannels(ctx, collectionID)
	if err != nil {
		return err
	}
	err = loadstatus.Wait(ctx, r.LoadConfigs, r.ShardViews, r.Readiness, collectionID, channels, 0, version)
	if errors.Is(err, merr.ErrCollectionNotLoaded) {
		return nil
	}
	return err
}

// AddPartition runs under CreatePartition's collection broadcast lock. Applying
// its existing WAL operation must not recursively acquire that broadcast lock.
func (r *Resources) AddPartition(ctx context.Context, collectionID, partitionID int64) error {
	if err := r.checkActive(ctx); err != nil {
		return err
	}
	if err := r.LoadManager.AddPartition(ctx, collectionID, partitionID); err != nil {
		return err
	}
	if !r.LoadConfigs.Contains(collectionID) {
		return nil
	}
	channels, err := r.VChannels(ctx, collectionID)
	if err != nil {
		return err
	}
	return loadstatus.Wait(ctx, r.LoadConfigs, r.ShardViews, r.Readiness, collectionID, channels, 0, qviews.DataVersion{})
}

func (r *Resources) checkActive(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.Readiness.Done():
		return merr.WrapErrServiceUnavailableMsg("QueryView coordinator stopped")
	default:
		return nil
	}
}
