// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const (
	manifestIndexBackfillSucceeded = "success"
	manifestIndexBackfillFailed    = "failed"
	manifestIndexBackfillStale     = "stale"
	manifestIndexBackfillSkipped   = "skipped"
)

// manifestIndexBackfillInspector migrates historical finished StorageV3
// SegmentIndex rows to the exclusive manifest placement used by foreground
// publication. Each segment batch moves in one CommitSegmentManifest transaction:
// the new revision and the catalog-row deletion become visible together, while
// the existing in-memory record remains available to readers.
//
// There is intentionally no second prune phase. In the exclusive-placement
// design, a catalog row is both the backlog marker and the record being
// retired. Deleting it in the publication transaction is what makes the
// migration crash-safe and removes the read-then-delete window a separate
// prune task would introduce.
type ManifestIndexBackfillInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mt        *meta.Meta
	indexMeta *IndexMeta

	// cursor is the last selected segment ID. Scans rotate around it so a
	// failing segment cannot starve the rest of a large migration.
	cursor int64

	// lastPending suppresses repeated completion logs. -1 means that no active
	// scan has run yet, so a cluster with no backlog does not announce a false
	// transition on its first tick.
	lastPending int
}

func NewManifestIndexBackfillInspector(ctx context.Context, mt *meta.Meta, im *IndexMeta) *ManifestIndexBackfillInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &ManifestIndexBackfillInspector{
		ctx:         ctx,
		cancel:      cancel,
		mt:          mt,
		indexMeta:   im,
		lastPending: -1,
	}
}

// Start launches no goroutine unless both operator choices are active. The
// backfill switch opts into the background migration; manifest publication
// selects its destination. Both are restart-scoped configuration.
func (i *ManifestIndexBackfillInspector) Start() {
	if !manifestIndexBackfillEnabled() {
		mlog.Info(i.ctx, "manifest index backfill is disabled, not starting",
			mlog.String("switch", Params.DataCoordCfg.ManifestIndexBackfillEnabled.Key))
		return
	}
	if !WriteSegmentIndexToManifest() {
		mlog.Info(i.ctx, "manifest index backfill requires manifest publication, not starting",
			mlog.String("switch", Params.DataCoordCfg.WriteSegmentIndexToManifest.Key))
		return
	}
	if i.mt == nil || i.indexMeta == nil {
		return
	}
	i.wg.Add(1)
	go i.backfillLoop(i.ctx)
}

func (i *ManifestIndexBackfillInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *ManifestIndexBackfillInspector) backfillLoop(ctx context.Context) {
	defer i.wg.Done()
	runManifestIndexMigrationLoop(ctx, i.indexMeta.manifestIndexBackfillNotify, manifestIndexBackfillInterval, i.runOnce)
}

// Both migration directions scan on startup, retry while work remains, and
// wait for record notifications once their completion checks pass.
func runManifestIndexMigrationLoop(ctx context.Context, updates <-chan struct{}, interval func() time.Duration, runOnce func(context.Context) bool) {
	timer := time.NewTimer(interval())
	timer.Stop()
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		// Drain only before scanning. Notifications arriving during or after
		// the scan must survive its transition to idle, even if it finds zero.
		select {
		case <-updates:
		default:
		}
		scanStarted := time.Now()
		// The first scan is immediate, including after restart. Pending is
		// derived from recovered records, never from a separate checkpoint.
		if runOnce(ctx) {
			// Pace retries and additional batches; new records coalesce while
			// work is active rather than bypassing the configured interval.
			timer.Reset(interval())
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			continue
		}
		mlog.Info(ctx, "manifest index migration scan stopped; waiting for record updates")
		select {
		case <-ctx.Done():
			return
		case <-updates:
		}
		// Legacy completions can wake an inspector without adding eligible
		// work. Coalesce these notifications, including ones received during
		// an empty scan, without scanning faster than the configured interval.
		timer.Reset(max(0, interval()-time.Since(scanStarted)))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// runOnce scans the complete backlog and selects whole segments until the
// batchSize record budget is reached. It returns whether periodic scans are
// still needed. A successful final batch is followed by one confirming scan.
func (i *ManifestIndexBackfillInspector) runOnce(ctx context.Context) bool {
	if !manifestIndexBackfillActive() || i.mt == nil || i.indexMeta == nil {
		return false
	}
	record := timerecord.NewTimeRecorder("manifestIndexBackfill")

	work, pending := i.scan(ctx)
	if ctx.Err() != nil {
		return false
	}
	i.reportPending(ctx, pending)
	if len(work) == 0 {
		return false
	}

	succeeded := i.execute(ctx, work)
	mlog.Info(ctx, "manifest index backfill batch finished",
		mlog.Int("recordsAttempted", countManifestIndexBackfillRecords(work)),
		mlog.Int("recordsSucceeded", succeeded),
		mlog.Int("recordsPending", pending),
		mlog.Duration("duration", record.ElapseSpan()))
	return true
}

// segmentManifestIndexBackfill keeps all selected records for one segment
// together. Only the catalog transaction limit can split its publication.
type segmentManifestIndexBackfill struct {
	segment *meta.SegmentInfo
	records []manifestIndexBackfillCandidate
}

// The scan only needs identities. Artifact metadata is reloaded for the selected
// transaction batch, so file-key slices are never duplicated for the backlog.
type manifestIndexBackfillCandidate struct {
	SegmentID int64
	IndexID   int64
	BuildID   int64
}

// scan counts the full eligible backlog before limiting work. Selection rotates
// by SegmentID and finishes each selected segment even if its records exceed
// the remaining scan budget. Deleted definitions and task-only records stay on
// their existing lifecycle paths.
func (i *ManifestIndexBackfillInspector) scan(ctx context.Context) ([]segmentManifestIndexBackfill, int) {
	// Filter records before touching segment metadata. Manifest-resident and
	// task-only records do not cause per-segment lookups or index-map copies.
	groups := make(map[int64]*segmentManifestIndexBackfill)
	pending := 0
	i.indexMeta.SegmentBuildInfo.buildID2SegmentIndex.Range(func(_ int64, record *model.SegmentIndex) bool {
		if ctx.Err() != nil {
			return false
		}
		if !segmentIndexNeedsManifestBackfill(record) ||
			!i.indexMeta.IsIndexExist(record.CollectionID, record.IndexID) {
			return true
		}
		// The build table also retains superseded records for GC. Only the
		// current (segment, index) occupant can supply a manifest entry.
		indexes, ok := i.indexMeta.SegmentIndexes.Get(record.SegmentID)
		if !ok {
			return true
		}
		current, ok := indexes.Get(record.IndexID)
		if !ok || current.BuildID != record.BuildID {
			return true
		}
		group := groups[record.SegmentID]
		if group == nil {
			segment := i.mt.GetSegment(ctx, record.SegmentID)
			if !meta.IsSegmentHealthy(segment) || segment.GetStorageVersion() != storage.StorageV3 ||
				segment.GetLevel() == datapb.SegmentLevel_L0 || segment.GetIsImporting() ||
				segment.GetState() == commonpb.SegmentState_Importing {
				return true
			}
			group = &segmentManifestIndexBackfill{segment: segment}
			groups[record.SegmentID] = group
		}
		group.records = append(group.records, manifestIndexBackfillCandidate{
			SegmentID: record.SegmentID, IndexID: record.IndexID, BuildID: record.BuildID,
		})
		pending++
		return true
	})
	if ctx.Err() != nil {
		return nil, 0
	}
	candidates := make([]segmentManifestIndexBackfill, 0, len(groups))
	for _, group := range groups {
		sort.Slice(group.records, func(left, right int) bool {
			return group.records[left].BuildID < group.records[right].BuildID
		})
		candidates = append(candidates, *group)
	}
	limit := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.GetAsInt()
	if len(candidates) == 0 || limit < 1 {
		return nil, pending
	}
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].segment.GetID() < candidates[right].segment.GetID()
	})
	start := sort.Search(len(candidates), func(idx int) bool {
		return candidates[idx].segment.GetID() > i.cursor
	})
	work := make([]segmentManifestIndexBackfill, 0)
	selected := 0
	for offset := 0; offset < len(candidates) && selected < limit; offset++ {
		item := candidates[(start+offset)%len(candidates)]
		work = append(work, item)
		selected += len(item.records)
		i.cursor = item.segment.GetID()
	}
	return work, pending
}

func segmentIndexNeedsManifestBackfill(segIdx *model.SegmentIndex) bool {
	return segIdx != nil && !segIdx.IsDeleted &&
		segIdx.IndexState == commonpb.IndexState_Finished &&
		len(segIdx.IndexFileKeys) > 0 &&
		!segIdx.ManifestPublished
}

func countManifestIndexBackfillRecords(work []segmentManifestIndexBackfill) int {
	total := 0
	for _, item := range work {
		total += len(item.records)
	}
	return total
}

func (i *ManifestIndexBackfillInspector) execute(ctx context.Context, work []segmentManifestIndexBackfill) int {
	if len(work) == 0 {
		return 0
	}
	var workers errgroup.Group
	workers.SetLimit(max(1, min(len(work), Params.DataCoordCfg.ManifestIndexBackfillConcurrency.GetAsInt())))
	results := make([]int, len(work))
	for idx, item := range work {
		if ctx.Err() != nil {
			break
		}
		workers.Go(func() error {
			results[idx] = i.backfillSegment(ctx, item)
			return nil
		})
	}
	// Each goroutine owns its segment protocol, including locks and catalog
	// publication. Manifest I/O uses the shared async commit executor. Wait
	// for accepted native operations before the inspector can stop.
	_ = workers.Wait()
	succeeded := 0
	for _, n := range results {
		succeeded += n
	}
	return succeeded
}

func (i *ManifestIndexBackfillInspector) backfillSegment(ctx context.Context, item segmentManifestIndexBackfill) int {
	// Healthy segments contribute one pointer PUT; each index adds one DELETE.
	limit := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt() - 1
	if limit < 1 {
		for _, candidate := range item.records {
			i.recordFailure(ctx, candidate, merr.WrapErrServiceInternalMsg("index backfill requires at least two etcd transaction operations"))
		}
		return 0
	}
	succeeded := 0
	for offset := 0; offset < len(item.records); offset += limit {
		if ctx.Err() != nil {
			break
		}
		batch := item.records[offset:min(offset+limit, len(item.records))]
		buildIDs := make([]int64, 0, len(batch))
		for _, candidate := range batch {
			buildIDs = append(buildIDs, candidate.BuildID)
		}
		if err := i.backfillIndexes(ctx, item.segment.GetID(), buildIDs...); err != nil {
			for _, candidate := range batch {
				i.recordFailure(ctx, candidate, err)
			}
			continue
		}
		succeeded += len(batch)
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillSucceeded).Add(float64(len(batch)))
	}
	return succeeded
}

// backfillIndexes publishes one segment batch without a manifest pre-read.
// Every projection is revalidated under its BuildID lock before manifest I/O.
// A failed or stale batch leaves all its catalog rows available for a later scan.
func (i *ManifestIndexBackfillInspector) backfillIndexes(ctx context.Context, segmentID int64, buildIDs ...int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	segment := i.mt.GetSegment(ctx, segmentID)
	if segment == nil || !meta.IsSegmentHealthy(segment) {
		return merr.WrapErrSegmentNotFound(segmentID)
	}
	entries := make([]packed.ManifestIndexInfo, 0, len(buildIDs))
	mutations := make([]meta.SegmentIndexMutation, 0, len(buildIDs))
	for _, buildID := range buildIDs {
		segIdx, ok := i.indexMeta.GetIndexJob(buildID)
		if !ok || !segmentIndexNeedsManifestBackfill(segIdx) ||
			!i.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			return meta.ErrSegmentIndexBackfillSkipped
		}
		destination := model.CloneSegmentIndex(segIdx)
		destination.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
		manifestIndex, err := meta.BuildManifestIndexInfo(i.mt, segment, destination)
		if err != nil {
			return err
		}
		if err := meta.ValidateManifestIndexPublishable(segmentID, manifestIndex); err != nil {
			return err
		}
		entries = append(entries, manifestIndex)
		mutations = append(mutations, meta.SegmentIndexMutation{Type: meta.SegmentIndexBackfill, BuildID: buildID})
	}
	if len(entries) == 0 {
		return nil
	}
	return i.mt.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: meta.CreateStorageConfig(),
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: entries},
		},
		CatalogMutation: meta.SegmentCatalogMutation{SegmentIndexes: mutations},
	})
}

func (i *ManifestIndexBackfillInspector) recordFailure(ctx context.Context, segIdx manifestIndexBackfillCandidate, err error) {
	switch {
	case errors.Is(err, meta.ErrSegmentIndexBackfillSkipped), errors.Is(err, merr.ErrSegmentNotFound):
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillSkipped).Inc()
		mlog.RatedInfo(ctx, rate.Limit(10), "segment index left manifest backfill eligibility before commit",
			mlog.FieldSegmentID(segIdx.SegmentID),
			mlog.FieldBuildID(segIdx.BuildID))
	case errors.Is(err, meta.ErrSegmentManifestStale):
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillStale).Inc()
		mlog.RatedInfo(ctx, rate.Limit(10), "manifest index backfill lost a manifest race; retrying on a later tick",
			mlog.FieldSegmentID(segIdx.SegmentID),
			mlog.FieldBuildID(segIdx.BuildID))
	default:
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillFailed).Inc()
		mlog.RatedWarn(ctx, rate.Limit(10), "failed to backfill segment index into manifest",
			mlog.FieldSegmentID(segIdx.SegmentID),
			mlog.FieldIndexID(segIdx.IndexID),
			mlog.FieldBuildID(segIdx.BuildID),
			mlog.Err(err))
	}
}

func (i *ManifestIndexBackfillInspector) reportPending(ctx context.Context, pending int) {
	metrics.DataCoordManifestIndexBackfillPending.Set(float64(pending))
	if pending == 0 && i.lastPending > 0 {
		mlog.Info(ctx, "manifest index backfill complete: no eligible finished SegmentIndex catalog row remains; no separate prune is required")
	}
	i.lastPending = pending
}

func manifestIndexBackfillEnabled() bool {
	return Params.DataCoordCfg.ManifestIndexBackfillEnabled.GetAsBool()
}

func manifestIndexBackfillActive() bool {
	return manifestIndexBackfillEnabled() && WriteSegmentIndexToManifest()
}

func manifestIndexBackfillInterval() time.Duration {
	interval := Params.DataCoordCfg.ManifestIndexBackfillInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return time.Minute
	}
	return interval
}
