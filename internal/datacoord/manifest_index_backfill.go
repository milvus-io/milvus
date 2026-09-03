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

package datacoord

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
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
// publication. Each record is moved by one CommitSegmentManifest transaction:
// the new revision and the catalog-row deletion become visible together, while
// the existing in-memory record remains available to readers.
//
// There is intentionally no second prune phase. In the exclusive-placement
// design, a catalog row is both the backlog marker and the record being
// retired. Deleting it in the publication transaction is what makes the
// migration crash-safe and removes the read-then-delete window a separate
// prune task would introduce.
type manifestIndexBackfillInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	meta *meta

	// cursor is the last selected build ID. Scans sort and rotate around it so
	// one permanently broken record cannot consume the front of every bounded
	// batch and starve the rest of a large migration.
	cursor int64

	// lastPending suppresses repeated completion logs. -1 means that no active
	// scan has run yet, so a cluster with no backlog does not announce a false
	// transition on its first tick.
	lastPending int
}

func newManifestIndexBackfillInspector(ctx context.Context, meta *meta) *manifestIndexBackfillInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &manifestIndexBackfillInspector{
		ctx:         ctx,
		cancel:      cancel,
		meta:        meta,
		lastPending: -1,
	}
}

// Start launches no goroutine unless both operator choices are active. The
// backfill switch opts into the background migration; manifest publication
// selects its destination. Both are restart-scoped configuration.
func (i *manifestIndexBackfillInspector) Start() {
	if !manifestIndexBackfillEnabled() {
		mlog.Info(i.ctx, "manifest index backfill is disabled, not starting",
			mlog.String("switch", Params.DataCoordCfg.ManifestIndexBackfillEnabled.Key))
		return
	}
	if !writeSegmentIndexToManifest() {
		mlog.Info(i.ctx, "manifest index backfill requires manifest publication, not starting",
			mlog.String("switch", Params.DataCoordCfg.WriteSegmentIndexToManifest.Key))
		return
	}
	i.wg.Add(1)
	go i.backfillLoop(i.ctx)
}

func (i *manifestIndexBackfillInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *manifestIndexBackfillInspector) backfillLoop(ctx context.Context) {
	defer i.wg.Done()
	interval := manifestIndexBackfillInterval()
	mlog.Info(ctx, "start manifest index backfill loop", mlog.Duration("interval", interval))
	// Populate the operator-facing pending gauge promptly instead of leaving
	// its Prometheus default (zero) visible for a full interval, which could be
	// mistaken for migration completion immediately after a restart.
	i.runOnce(ctx)
	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "manifest index backfill loop exited")
			return
		case <-ticker.C:
			i.runOnce(ctx)
			if next := manifestIndexBackfillInterval(); next != interval {
				interval = next
				ticker.Reset(interval)
				mlog.Info(ctx, "manifest index backfill interval updated", mlog.Duration("interval", interval))
			}
		}
	}
}

// runOnce performs one complete in-memory backlog scan and executes at most
// batchSize record migrations. It is split out so tests can drive one tick.
func (i *manifestIndexBackfillInspector) runOnce(ctx context.Context) {
	if !manifestIndexBackfillActive() || i.meta == nil || i.meta.indexMeta == nil {
		return
	}
	record := timerecord.NewTimeRecorder("manifestIndexBackfill")

	work, pending := i.scan(ctx)
	i.reportPending(ctx, pending)
	if len(work) == 0 {
		return
	}

	succeeded := i.execute(ctx, work)
	mlog.Info(ctx, "manifest index backfill batch finished",
		mlog.Int("recordsAttempted", countManifestIndexBackfillRecords(work)),
		mlog.Int("recordsSucceeded", succeeded),
		mlog.Int("recordsPending", pending),
		mlog.Duration("duration", record.ElapseSpan()))
}

type manifestIndexBackfillCandidate struct {
	segment *SegmentInfo
	record  *model.SegmentIndex
}

// segmentManifestIndexBackfill groups selected records by segment. Records in
// one group are committed sequentially so several pool workers never consume
// slots waiting for the same per-segment manifest lock.
type segmentManifestIndexBackfill struct {
	segment *SegmentInfo
	records []*model.SegmentIndex
}

// scan finds finished, artifact-bearing records whose catalog row still
// exists. GetSegmentIndexes deliberately filters definitions already handed to
// GC; fake-finished and non-terminal task rows remain in etcd by design and are
// not migration work.
//
// The full candidate count is returned before batch limiting. Selection rotates
// by BuildID to guarantee that a repeatedly failing record does not starve
// candidates ordered after it.
func (i *manifestIndexBackfillInspector) scan(ctx context.Context) ([]segmentManifestIndexBackfill, int) {
	segments := i.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			segment.GetStorageVersion() == storage.StorageV3 &&
			segment.GetLevel() != datapb.SegmentLevel_L0
	}))

	candidates := make([]manifestIndexBackfillCandidate, 0)
	for _, segment := range segments {
		for _, segIdx := range i.meta.indexMeta.GetSegmentIndexes(segment.GetCollectionID(), segment.GetID()) {
			if !segmentIndexNeedsManifestBackfill(i.meta.indexMeta, segIdx) {
				continue
			}
			candidates = append(candidates, manifestIndexBackfillCandidate{segment: segment, record: segIdx})
		}
	}
	if len(candidates) == 0 {
		return nil, 0
	}

	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].record.BuildID < candidates[right].record.BuildID
	})
	pending := len(candidates)
	limit := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.GetAsInt()
	if limit < 1 {
		// Paramtable clamps this already. Keep the scan safe for focused tests
		// that construct configuration manually instead of going through Init.
		return nil, pending
	}
	if limit > pending {
		limit = pending
	}
	start := sort.Search(len(candidates), func(idx int) bool {
		return candidates[idx].record.BuildID > i.cursor
	})

	selected := make([]manifestIndexBackfillCandidate, 0, limit)
	for offset := 0; offset < limit; offset++ {
		selected = append(selected, candidates[(start+offset)%len(candidates)])
	}
	i.cursor = selected[len(selected)-1].record.BuildID

	work := make([]segmentManifestIndexBackfill, 0, len(selected))
	positions := make(map[int64]int, len(selected))
	for _, candidate := range selected {
		segmentID := candidate.segment.GetID()
		position, ok := positions[segmentID]
		if !ok {
			positions[segmentID] = len(work)
			work = append(work, segmentManifestIndexBackfill{segment: candidate.segment})
			position = len(work) - 1
		}
		work[position].records = append(work[position].records, candidate.record)
	}
	return work, pending
}

func segmentIndexNeedsManifestBackfill(indexMeta *indexMeta, segIdx *model.SegmentIndex) bool {
	return segIdx != nil && !segIdx.IsDeleted &&
		segIdx.IndexState == commonpb.IndexState_Finished &&
		len(segIdx.IndexFileKeys) > 0 &&
		!indexMeta.isSegmentIndexCatalogAbsent(segIdx.BuildID)
}

func countManifestIndexBackfillRecords(work []segmentManifestIndexBackfill) int {
	total := 0
	for _, item := range work {
		total += len(item.records)
	}
	return total
}

func (i *manifestIndexBackfillInspector) execute(ctx context.Context, work []segmentManifestIndexBackfill) int {
	pool := conc.NewPool[int](Params.DataCoordCfg.ManifestIndexBackfillConcurrency.GetAsInt())
	defer pool.Release()

	futures := make([]*conc.Future[int], 0, len(work))
	for idx := range work {
		item := work[idx]
		futures = append(futures, pool.Submit(func() (int, error) {
			return i.backfillSegment(ctx, item), nil
		}))
	}
	// BlockOnAll drains every started segment group before Release closes the
	// pool. Per-record failures are classified below and never abort siblings.
	_ = conc.BlockOnAll(futures...)

	succeeded := 0
	for _, future := range futures {
		succeeded += future.Value()
	}
	return succeeded
}

func (i *manifestIndexBackfillInspector) backfillSegment(ctx context.Context, item segmentManifestIndexBackfill) int {
	succeeded := 0
	for _, candidate := range item.records {
		if ctx.Err() != nil {
			break
		}
		if err := i.backfillIndex(ctx, item.segment.GetID(), candidate.BuildID); err != nil {
			i.recordFailure(ctx, candidate, err)
			continue
		}
		succeeded++
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillSucceeded).Inc()
	}
	return succeeded
}

// backfillIndex performs no manifest pre-read. Re-publishing an entry already
// present under the same index_id is idempotent, while the current SegmentIndex
// record supplies the exact metadata foreground publication uses. The commit
// revalidates that projection under the BuildID lock before any manifest I/O.
func (i *manifestIndexBackfillInspector) backfillIndex(ctx context.Context, segmentID, buildID int64) error {
	segment := i.meta.GetSegment(ctx, segmentID)
	if segment == nil || !isSegmentHealthy(segment) {
		return merr.WrapErrSegmentNotFound(segmentID)
	}
	segIdx, ok := i.meta.indexMeta.GetIndexJob(buildID)
	if !ok || !segmentIndexNeedsManifestBackfill(i.meta.indexMeta, segIdx) ||
		!i.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
		return errSegmentIndexBackfillSkipped
	}

	manifestIndex, err := buildManifestIndexInfo(i.meta, segment, segIdx)
	if err != nil {
		return err
	}
	if err := validateManifestIndexPublishable(segmentID, manifestIndex); err != nil {
		return err
	}

	return i.meta.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: createStorageConfig(),
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{manifestIndex}},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndexes: []SegmentIndexMutation{{
				Type:    SegmentIndexBackfill,
				BuildID: buildID,
			}},
		},
	})
}

func (i *manifestIndexBackfillInspector) recordFailure(ctx context.Context, segIdx *model.SegmentIndex, err error) {
	switch {
	case errors.Is(err, errSegmentIndexBackfillSkipped), errors.Is(err, merr.ErrSegmentNotFound):
		metrics.DataCoordManifestIndexBackfillRecords.WithLabelValues(manifestIndexBackfillSkipped).Inc()
		mlog.RatedInfo(ctx, rate.Limit(10), "segment index left manifest backfill eligibility before commit",
			mlog.FieldSegmentID(segIdx.SegmentID),
			mlog.FieldBuildID(segIdx.BuildID))
	case errors.Is(err, errSegmentManifestStale):
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

func (i *manifestIndexBackfillInspector) reportPending(ctx context.Context, pending int) {
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
	return manifestIndexBackfillEnabled() && writeSegmentIndexToManifest()
}

func manifestIndexBackfillInterval() time.Duration {
	interval := Params.DataCoordCfg.ManifestIndexBackfillInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return time.Minute
	}
	return interval
}
