package fileresource

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// SnapshotFetcher returns the current inventory version and, only when it
// differs from knownVersion, the full inventory. knownVersion must be the last
// successfully applied version. An unchanged response omits the inventory;
// a newer response with an empty inventory means all resources were removed.
type SnapshotFetcher func(ctx context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error)

var snapshotFetcher struct {
	sync.RWMutex
	preparer *resourcePreparer
}

// SetSnapshotFetcher must be called after InitManager. It enables preparation
// by pull for workers outside QueryCoord's legacy file-resource push distribution.
func SetSnapshotFetcher(fetch SnapshotFetcher) {
	snapshotFetcher.Lock()
	snapshotFetcher.preparer = nil
	if fetch != nil {
		snapshotFetcher.preparer = &resourcePreparer{manager: GlobalFileManager, fetch: fetch}
	}
	snapshotFetcher.Unlock()
}

// IsPullEnabled identifies consumers of the new demand-driven preparation path.
// It does not interpret or change the legacy file-resource mode configuration.
func IsPullEnabled() bool {
	snapshotFetcher.RLock()
	defer snapshotFetcher.RUnlock()
	return snapshotFetcher.preparer != nil
}

// retryPreparation keeps transient preparation failures on the worker. Its
// context belongs to the flight, so retries stop only when all waiters leave.
// An inner RPC/download deadline can expire while this shared context is alive.
func retryPreparation(ctx context.Context, work func() error) error {
	err := retry.Handle(ctx, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		err := work()
		return err != nil && ctx.Err() == nil && retry.IsRecoverable(err) && !merr.IsNonRetryableErr(err) &&
			grpcstatus.Code(err) != codes.Unimplemented, err
	}, retry.AttemptAlways())
	// retry.Handle can return its previous attempt's error after cancellation.
	// The shared lifetime, rather than that stale transient cause, ended the wait.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type resourceSnapshot struct {
	resources []*internalpb.FileResourceInfo
	version   uint64
}

// resourcePreparer keeps the two flight stages and latest target for one manager.
// The target is metadata, not a cached Prepare result: every call still fetches.
type resourcePreparer struct {
	manager Manager
	fetch   SnapshotFetcher

	snapshotFlights syncutil.FlightGroup[uint64, resourceSnapshot]
	syncFlights     syncutil.FlightGroup[struct{}, uint64]
	mu              sync.Mutex
	latest          resourceSnapshot
}

// Prepare refreshes the atomic resource inventory before creating a consumer.
// Fetch and application are separate flights so calls arriving during a download
// can discover newer snapshots. Each caller checks its own required version after
// an application completes. Only the last departing waiter cancels shared work.
func Prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshotFetcher.RLock()
	preparer := snapshotFetcher.preparer
	snapshotFetcher.RUnlock()
	if preparer == nil {
		return nil
	}
	knownVersion := preparer.manager.GetVersion()
	snapshot, err := preparer.snapshotFlights.Do(ctx, knownVersion, func(ctx context.Context) (resourceSnapshot, error) {
		var snapshot resourceSnapshot
		err := retryPreparation(ctx, func() error {
			resources, version, err := preparer.fetch(ctx, knownVersion)
			snapshot = resourceSnapshot{resources: resources, version: version}
			return err
		})
		return snapshot, err
	})
	if err != nil {
		return err
	}
	return preparer.syncSnapshot(ctx, snapshot)
}

func (p *resourcePreparer) syncSnapshot(ctx context.Context, snapshot resourceSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot.version <= p.manager.GetVersion() {
		return nil
	}
	p.mu.Lock()
	if snapshot.version > p.latest.version {
		p.latest = snapshot
	}
	p.mu.Unlock()

	for {
		attempted, err := p.syncFlights.Do(ctx, struct{}{}, p.syncLatest)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A newer application may fail after this caller's version was installed.
		if snapshot.version <= p.manager.GetVersion() {
			return nil
		}
		if attempted >= snapshot.version {
			return err
		}
		// This caller joined an older application. Its result (including a
		// permanent error) cannot decide whether the newer target can be applied.
	}
}

func (p *resourcePreparer) syncLatest(ctx context.Context) (uint64, error) {
	var target resourceSnapshot
	err := retryPreparation(ctx, func() error {
		p.mu.Lock()
		target = p.latest
		p.mu.Unlock()
		// Re-read the target on every retry: a newer snapshot can remove the
		// resource that made the previous attempt fail. Sync publishes atomically.
		if target.version <= p.manager.GetVersion() {
			return nil
		}
		return p.manager.Sync(ctx, target.version, target.resources)
	})
	return target.version, err
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
