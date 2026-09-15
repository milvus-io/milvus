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

// rollbackSegmentIndexes transfers at most one atomic batch from the current
// manifest to etcd. The lock covers discovery as well as publication, and is
// shared with dropped-segment GC so no prefix is removed during this transfer.
func (m *meta) rollbackSegmentIndexes(ctx context.Context, segmentID int64, catalogAbsentBuilds ...int64) (int, error) {
	locks := m.getSegmentManifestLocks()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	segment := m.GetSegment(ctx, segmentID)
	if segment == nil || (!segment.GetManifestHasIndex() && len(catalogAbsentBuilds) == 0) {
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
	// Validate the entire source before publishing any part of it. A malformed
	// entry must remain visible as backlog, even if it sorts after this batch.
	indexIDs := make(map[int64]struct{}, len(entries))
	buildIDs := make(map[int64]struct{}, len(entries))
	sources := make(map[int64]*packed.ManifestIndexInfo, len(entries)+len(catalogAbsentBuilds))
	for idx := range entries {
		entry := entries[idx]
		_, duplicateIndex := indexIDs[entry.IndexID]
		_, duplicateBuild := buildIDs[entry.BuildID]
		if _, valid := manifestIndexFilePathInfoForSegment(m.chunkManager.RootPath(), segment.SegmentInfo, entry); !valid || duplicateIndex || duplicateBuild {
			return 0, merr.Wrapf(merr.ErrDataIntegrity, "segment %d has an invalid or duplicate manifest index %d build %d", segmentID, entry.IndexID, entry.BuildID)
		}
		indexIDs[entry.IndexID] = struct{}{}
		buildIDs[entry.BuildID] = struct{}{}
		sources[entry.BuildID] = &entries[idx]
	}
	// A newer build can replace an index_id in the manifest while its old
	// in-memory build still protects files (including snapshot references).
	// Restore those records too, after proving their absence in this source.
	for _, buildID := range catalogAbsentBuilds {
		if _, present := sources[buildID]; !present && m.indexMeta.isSegmentIndexCatalogAbsent(buildID) {
			sources[buildID] = nil
		}
	}
	if len(sources) == 0 {
		return 0, m.UpdateSegmentsInfo(ctx, clearEmptyManifestIndexMarker(segmentID, segment.GetManifestPath()))
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
	}, segment.GetManifestPath(), 0)
	if err != nil {
		return 0, err
	}
	return len(selected), nil
}

// validateRollbackIndex runs with the segment and BuildID locks held. Existing
// catalog rows win conflicts, as on startup. A manifest-only record must still
// describe the exact artifact whose metadata is leaving the manifest.
func (m *meta) validateRollbackIndex(segment *SegmentInfo, mutation SegmentIndexMutation, record *model.SegmentIndex) error {
	entry := mutation.rollbackEntry
	if record == nil {
		return merr.Wrapf(merr.ErrDataIntegrity, "rollback has no record for build %d", mutation.BuildID)
	}
	if entry == nil {
		// Only the locked rollback preparer can supply this proof of absence.
		return nil
	}
	if entry.BuildID != record.BuildID || entry.IndexID != record.IndexID {
		return merr.Wrapf(merr.ErrDataIntegrity, "rollback record disagrees with manifest index identity, buildID=%d", mutation.BuildID)
	}
	if !m.indexMeta.isSegmentIndexCatalogAbsent(record.BuildID) {
		return nil
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
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	meta     *meta
	copyMeta CopySegmentMeta
	cursor   int64
	ready    bool
}

func newManifestIndexRollbackInspector(ctx context.Context, m *meta, copyMeta CopySegmentMeta) *manifestIndexRollbackInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &manifestIndexRollbackInspector{ctx: ctx, cancel: cancel, meta: m, copyMeta: copyMeta}
}

func (i *manifestIndexRollbackInspector) Start() {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !manifestIndexRollbackEnabled() {
		return
	}
	mlog.Info(i.ctx, "starting manifest index rollback; new index completions use etcd")
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for i.ctx.Err() == nil {
			i.runOnce(i.ctx)
			interval := Params.DataCoordCfg.ManifestIndexRollbackInterval.GetAsDuration(time.Second)
			if interval <= 0 {
				interval = time.Minute
			}
			timer := time.NewTimer(interval)
			select {
			case <-i.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (i *manifestIndexRollbackInspector) Stop() {
	i.cancel()
	i.wg.Wait()
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
}

// Snapshot copy tasks first: a task may publish its target before becoming
// Completed. Seeing it as active conservatively blocks this scan; seeing it
// completed means its target publication precedes the following segment scan.
func (i *manifestIndexRollbackInspector) activeCopyTargets(ctx context.Context) (map[int64]struct{}, int) {
	targets := make(map[int64]struct{})
	pending := 0
	if i.copyMeta != nil {
		for _, task := range i.copyMeta.GetTaskBy(ctx) {
			state := task.GetState()
			if !task.GetCleanupRequired() && (state == datapb.CopySegmentTaskState_CopySegmentTaskCompleted ||
				state == datapb.CopySegmentTaskState_CopySegmentTaskFailed) {
				continue
			}
			pending++
			for _, mapping := range task.GetIdMappings() {
				targets[mapping.GetTargetSegmentId()] = struct{}{}
			}
		}
	}
	return targets, pending
}

func (i *manifestIndexRollbackInspector) runOnce(ctx context.Context) {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !manifestIndexRollbackEnabled() || i.meta == nil || i.meta.indexMeta == nil || ctx.Err() != nil {
		return
	}
	blocked, pendingCopies := i.activeCopyTargets(ctx)
	segments := i.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool { return segment.GetManifestHasIndex() }))
	seen := make(map[int64]struct{}, len(segments))
	for _, segment := range segments {
		seen[segment.GetID()] = struct{}{}
	}
	absentBySegment := make(map[int64][]int64)
	pendingRecords := 0
	i.meta.indexMeta.segmentIndexCatalogAbsent.Range(func(buildID int64) bool {
		pendingRecords++
		if record, ok := i.meta.indexMeta.GetIndexJob(buildID); ok {
			absentBySegment[record.SegmentID] = append(absentBySegment[record.SegmentID], buildID)
			if _, present := seen[record.SegmentID]; !present {
				if segment := i.meta.GetSegment(ctx, record.SegmentID); segment != nil {
					segments = append(segments, segment)
					seen[record.SegmentID] = struct{}{}
				}
			}
		}
		return ctx.Err() == nil
	})
	if ctx.Err() != nil {
		return
	}
	metrics.DataCoordManifestIndexRollbackPending.Set(float64(len(segments)))
	metrics.DataCoordManifestIndexRollbackPendingCopies.Set(float64(pendingCopies))
	metrics.DataCoordManifestIndexRollbackPendingRecords.Set(float64(pendingRecords))
	ready := len(segments) == 0 && pendingCopies == 0 && pendingRecords == 0
	if ready {
		metrics.DataCoordManifestIndexRollbackReady.Set(1)
		if !i.ready {
			mlog.Info(ctx, "manifest index rollback complete: no manifest-only index records or unfinished copy tasks remain")
		}
	}
	i.ready = ready
	if len(segments) == 0 {
		return
	}
	sort.Slice(segments, func(a, b int) bool { return segments[a].GetID() < segments[b].GetID() })
	start := sort.Search(len(segments), func(idx int) bool { return segments[idx].GetID() > i.cursor })
	limit := min(len(segments), max(1, Params.DataCoordCfg.ManifestIndexRollbackBatchSize.GetAsInt()))
	pool := conc.NewPool[int](min(limit, max(1, Params.DataCoordCfg.ManifestIndexRollbackConcurrency.GetAsInt())))
	defer pool.Release()
	futures := make([]*conc.Future[int], 0, limit)
	for offset := 0; offset < limit; offset++ {
		segment := segments[(start+offset)%len(segments)]
		i.cursor = segment.GetID()
		if _, active := blocked[segment.GetID()]; active {
			continue
		}
		futures = append(futures, pool.Submit(func() (int, error) {
			if ctx.Err() != nil {
				return 0, nil
			}
			n, err := i.meta.rollbackSegmentIndexes(ctx, segment.GetID(), absentBySegment[segment.GetID()]...)
			status := "success"
			if err != nil {
				status = "failed"
				if errors.Is(err, errSegmentManifestStale) {
					status = "stale"
				}
				mlog.RatedWarn(ctx, 1, "manifest index rollback will retry", mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
			}
			metrics.DataCoordManifestIndexRollbackAttempts.WithLabelValues(status).Inc()
			metrics.DataCoordManifestIndexRollbackRecords.Add(float64(n))
			return n, nil
		}))
	}
	_ = conc.BlockOnAll(futures...)
}
