package queryresource

import (
	"context"
	"path"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// The vchannel embeds the collection identity. Entries are immutable after load.
type partitionStatsKey struct {
	vchannel    string
	partitionID int64
	version     int64
}

type partitionStatsResource struct {
	key          partitionStatsKey
	collectionID int64
	refs         int // guarded by the owning Manager.mu
	mu           sync.Mutex
	loading      bool
	ctx          context.Context
	cancel       context.CancelFunc
	snapshot     *storage.PartitionStatsSnapshot
}

// acquirePartitionStatsLocked runs before preparation is scheduled. Even a
// preparing View owns its resources, so an older View's release cannot evict them.
func (m *Manager) acquirePartitionStatsLocked(req snviewAcquireStats) []*partitionStatsResource {
	resources := make([]*partitionStatsResource, 0, len(req.stats))
	for _, ref := range req.stats {
		if ref.GetVersion() == 0 {
			continue
		}
		key := partitionStatsKey{req.vchannel, ref.GetPartitionId(), ref.GetVersion()}
		entry := m.partitionStats[key]
		if entry == nil {
			ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Ownership transfers to entry; last View release cancels it.
			entry = &partitionStatsResource{key: key, collectionID: req.collectionID, ctx: ctx, cancel: cancel}
			if m.partitionStats == nil {
				m.partitionStats = make(map[partitionStatsKey]*partitionStatsResource)
			}
			m.partitionStats[key] = entry
		}
		entry.refs++
		resources = append(resources, entry)
	}
	return resources
}

type snviewAcquireStats struct {
	collectionID int64
	vchannel     string
	stats        []*viewpb.ClusteredPartitionStats
}

func (m *Manager) releasePartitionStatsLocked(resources []*partitionStatsResource) {
	for _, entry := range resources {
		entry.refs--
		if entry.refs == 0 {
			entry.cancel()
			delete(m.partitionStats, entry.key)
		}
	}
}

// prepare shares one synchronous scheduler load. Other scheduler workers yield
// instead of blocking behind it. Last-release cancellation prevents publication
// into an evicted entry; a replacement acquisition owns a different entry.
func (r *partitionStatsResource) prepare(ctx context.Context, cm storage.ChunkManager) error {
	r.mu.Lock()
	if r.snapshot != nil {
		r.mu.Unlock()
		return nil
	}
	if r.loading {
		r.mu.Unlock()
		return nodescheduler.ErrDelay
	}
	r.loading = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.loading = false; r.mu.Unlock() }()
	ctx, cancel := contextutil.MergeContext(ctx, r.ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if cm == nil {
		return merr.WrapErrServiceInternalMsg("partition statistics storage is unavailable")
	}
	object := path.Join(cm.RootPath(), common.PartitionStatsPath, strconv.FormatInt(r.collectionID, 10), strconv.FormatInt(r.key.partitionID, 10), r.key.vchannel, strconv.FormatInt(r.key.version, 10))
	data, err := cm.Read(ctx, object)
	if err != nil {
		return merr.Wrapf(err, "read partition statistics %s", object)
	}
	snapshot := &storage.PartitionStatsSnapshot{}
	if err := json.Unmarshal(data, snapshot); err != nil {
		return merr.Wrap(errors.Join(merr.ErrDataIntegrity, err), "invalid partition statistics JSON")
	}
	// The object path is the version identity. Existing DataCoord remapping
	// deserializes only segmentStats and writes Version=0 back to the JSON.
	snapshot.Version = r.key.version
	if snapshot.SegmentStats == nil {
		return merr.WrapErrDataIntegrityMsg("partition statistics has no segment map: %s", object)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r.snapshot = snapshot
	return nil
}

// collectPartitionStatsSnapshots is called only after every resource prepared
// successfully. Snapshots are immutable after publication by prepare; its mutex
// establishes visibility, and neither release nor cancellation clears them.
// The resulting map is owned by one QueryView reference and is never mutated.
func collectPartitionStatsSnapshots(resources []*partitionStatsResource) map[int64]*storage.PartitionStatsSnapshot {
	result := make(map[int64]*storage.PartitionStatsSnapshot, len(resources))
	for _, entry := range resources {
		result[entry.key.partitionID] = entry.snapshot
	}
	return result
}
