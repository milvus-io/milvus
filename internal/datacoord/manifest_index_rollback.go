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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func manifestIndexRollbackEnabled() bool {
	return Params.DataCoordCfg.ManifestIndexRollbackEnabled.GetAsBool()
}

func isSegmentIndexRollbackRetained(segment *SegmentInfo) bool {
	return segment != nil && segment.GetStorageVersion() == storage.StorageV3 &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		(isSegmentHealthy(segment) || segment.GetState() == commonpb.SegmentState_Dropped)
}

// rollbackSegmentIndexes restores selected in-memory records to etcd. The
// segment lock protects the source manifest through publication; BuildID locks
// in the commit revalidate the records before changing their durable placement.
func (m *meta) rollbackSegmentIndexes(ctx context.Context, segmentID int64, selectedBuilds ...int64) (int, error) {
	locks := m.getSegmentManifestLocks()
	lockStart := time.Now()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	lockWait := time.Since(lockStart)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sources := make(map[int64]*packed.ManifestIndexInfo, len(selectedBuilds))
	for _, buildID := range selectedBuilds {
		if record, ok := m.indexMeta.GetIndexJob(buildID); ok && record.ManifestPublished && record.SegmentID == segmentID {
			sources[buildID] = nil
		}
	}
	if len(sources) == 0 {
		return 0, nil
	}
	segment := m.GetSegment(ctx, segmentID)
	if segment == nil {
		return 0, nil
	}
	if !isSegmentIndexRollbackRetained(segment) || segment.GetManifestPath() == "" {
		return 0, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has an unsupported manifest index placement", segmentID)
	}
	cfg := createStorageConfig()
	entries, err := m.readManifestIndexes(ctx, segment.GetManifestPath(), cfg)
	if err != nil {
		return 0, merr.Wrap(err, "read manifest indexes for rollback")
	}
	// Resolve only selected records. Superseded builds may no longer have an
	// entry in the current manifest. Entries without selected records belong
	// to their existing lifecycle (including GC and copy installation).
	indexIDs := make(map[int64]struct{}, len(sources))
	for idx := range entries {
		entry := &entries[idx]
		previous, selected := sources[entry.BuildID]
		if !selected {
			continue
		}
		_, duplicateIndex := indexIDs[entry.IndexID]
		if _, valid := manifestIndexFilePathInfoForSegment(m.chunkManager.RootPath(), segment.SegmentInfo, *entry); !valid || duplicateIndex || previous != nil {
			return 0, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has an invalid or duplicate selected manifest index %d build %d", segmentID, entry.IndexID, entry.BuildID)
		}
		indexIDs[entry.IndexID] = struct{}{}
		sources[entry.BuildID] = entry
	}
	limit := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt() - 1
	if limit < 1 {
		return 0, merr.WrapErrServiceInternalMsg("index rollback requires at least two etcd transaction operations")
	}
	selected := make([]int64, 0, len(sources))
	for buildID := range sources {
		selected = append(selected, buildID)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
	selected = selected[:min(len(selected), limit)]
	drops := make([]packed.DropIndexEntry, 0, len(selected))
	mutations := make([]SegmentIndexMutation, 0, len(selected))
	for _, buildID := range selected {
		entry := sources[buildID]
		if entry != nil {
			drops = append(drops, packed.DropIndexEntry{IndexID: entry.IndexID, ExpectedBuildID: buildID})
		}
		mutations = append(mutations, SegmentIndexMutation{Type: SegmentIndexRollback, BuildID: buildID, rollbackEntry: entry})
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	mutation := ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{DropIndexes: drops}}
	if len(drops) == 0 {
		// The source read proved these records are already absent from the
		// manifest. Publish their PUTs against the unchanged, locked pointer.
		mutation = ManifestMutation{Type: ManifestMutationNoop, ManifestPath: segment.GetManifestPath()}
	}
	err = m.commitSegmentManifestLocked(ctx, SegmentManifestCommit{
		SegmentID: segmentID, StorageConfig: cfg,
		Mutation:        mutation,
		CatalogMutation: SegmentCatalogMutation{SegmentIndexes: mutations},
	}, segment.GetManifestPath(), lockWait)
	if err != nil {
		return 0, err
	}
	return len(selected), nil
}

// validateRollbackIndex runs with the segment and BuildID locks held. The
// record must still describe the artifact whose metadata leaves the manifest.
func (m *meta) validateRollbackIndex(segment *SegmentInfo, mutation SegmentIndexMutation, record *model.SegmentIndex) error {
	entry := mutation.rollbackEntry
	if record == nil || !record.ManifestPublished {
		return errSegmentIndexRollbackSkipped
	}
	if entry == nil {
		// Only the locked rollback preparer can supply this proof of absence.
		return nil
	}
	if entry.BuildID != record.BuildID || entry.IndexID != record.IndexID {
		return merr.Wrapf(merr.ErrDataIntegrity, "rollback record disagrees with manifest index identity, buildID=%d", mutation.BuildID)
	}
	if record.IndexState != commonpb.IndexState_Finished || len(record.IndexFileKeys) == 0 {
		return merr.Wrapf(merr.ErrDataIntegrity, "manifest-only rollback record is not a finished artifact, buildID=%d", record.BuildID)
	}
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.Wrap(err, "parse rollback manifest")
	}
	normalized := *entry
	normalized.Path, err = packed.ManifestIndexRelativePath(basePath, entry.Path)
	if err != nil {
		return merr.Wrap(err, "normalize rollback index path")
	}
	return validateManifestIndexTaskProjection(m, segment, normalized, record)
}

type manifestIndexRollbackInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	meta   *meta
	cursor int64
	ready  bool
}

func newManifestIndexRollbackInspector(ctx context.Context, m *meta) *manifestIndexRollbackInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &manifestIndexRollbackInspector{ctx: ctx, cancel: cancel, meta: m}
}

func (i *manifestIndexRollbackInspector) Start() {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !manifestIndexRollbackEnabled() || i.meta == nil || i.meta.indexMeta == nil {
		return
	}
	mlog.Info(i.ctx, "starting manifest index rollback; new index completions use etcd")
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		runManifestIndexMigrationLoop(i.ctx, i.meta.indexMeta.manifestIndexRollbackNotify, manifestIndexRollbackInterval, i.runOnce)
	}()
}

func manifestIndexRollbackInterval() time.Duration {
	interval := Params.DataCoordCfg.ManifestIndexRollbackInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return time.Minute
	}
	return interval
}

func (i *manifestIndexRollbackInspector) Stop() {
	i.cancel()
	i.wg.Wait()
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
}

type segmentManifestIndexRollback struct {
	segment  *SegmentInfo
	buildIDs []int64
}

// scan groups manifest-resident records by segment, including superseded
// builds and dropped definitions whose catalog rows still need restoring.
func (i *manifestIndexRollbackInspector) scan(ctx context.Context) ([]segmentManifestIndexRollback, int) {
	groups := make(map[int64]*segmentManifestIndexRollback)
	pendingRecords := 0
	i.meta.indexMeta.segmentBuildInfo.buildID2SegmentIndex.Range(func(_ int64, record *model.SegmentIndex) bool {
		if ctx.Err() != nil {
			return false
		}
		if !record.ManifestPublished {
			return true
		}
		// Count even records without a segment so inconsistent metadata cannot
		// turn into a false ready signal.
		pendingRecords++
		group := groups[record.SegmentID]
		if group == nil {
			segment := i.meta.GetSegment(ctx, record.SegmentID)
			if segment == nil {
				return true
			}
			group = &segmentManifestIndexRollback{segment: segment}
			groups[record.SegmentID] = group
		}
		group.buildIDs = append(group.buildIDs, record.BuildID)
		return true
	})
	if ctx.Err() != nil {
		return nil, 0
	}
	work := make([]segmentManifestIndexRollback, 0, len(groups))
	for _, group := range groups {
		work = append(work, *group)
	}
	return work, pendingRecords
}

func (i *manifestIndexRollbackInspector) runOnce(ctx context.Context) bool {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !manifestIndexRollbackEnabled() || i.meta == nil || i.meta.indexMeta == nil || ctx.Err() != nil {
		return false
	}
	work, pendingRecords := i.scan(ctx)
	if ctx.Err() != nil {
		return false
	}
	metrics.DataCoordManifestIndexRollbackPending.Set(float64(len(work)))
	metrics.DataCoordManifestIndexRollbackPendingRecords.Set(float64(pendingRecords))
	ready := pendingRecords == 0
	if ready {
		metrics.DataCoordManifestIndexRollbackReady.Set(1)
		if !i.ready {
			mlog.Info(ctx, "manifest index rollback complete: no manifest-resident index records remain")
		}
	}
	i.ready = ready
	if len(work) == 0 {
		return !ready
	}
	sort.Slice(work, func(a, b int) bool { return work[a].segment.GetID() < work[b].segment.GetID() })
	start := sort.Search(len(work), func(idx int) bool { return work[idx].segment.GetID() > i.cursor })
	limit := min(len(work), max(1, Params.DataCoordCfg.ManifestIndexRollbackBatchSize.GetAsInt()))
	pool := conc.NewPool[int](min(limit, max(1, Params.DataCoordCfg.ManifestIndexRollbackConcurrency.GetAsInt())))
	defer pool.Release()
	futures := make([]*conc.Future[int], 0, limit)
	for offset := 0; offset < limit; offset++ {
		item := work[(start+offset)%len(work)]
		segment := item.segment
		i.cursor = segment.GetID()
		futures = append(futures, pool.Submit(func() (int, error) {
			if ctx.Err() != nil {
				return 0, nil
			}
			n, err := i.meta.rollbackSegmentIndexes(ctx, segment.GetID(), item.buildIDs...)
			status := "success"
			if err != nil {
				status = "failed"
				if errors.Is(err, errSegmentManifestStale) || errors.Is(err, errSegmentIndexRollbackSkipped) {
					status = "stale"
				}
				mlog.RatedWarn(ctx, 1, "manifest index rollback will retry", mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
			}
			metrics.DataCoordManifestIndexRollbackAttempts.WithLabelValues(status).Inc()
			return n, nil
		}))
	}
	_ = conc.BlockOnAll(futures...)
	return true
}
