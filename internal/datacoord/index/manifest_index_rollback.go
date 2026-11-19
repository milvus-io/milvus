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

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type ManifestIndexRollbackInspector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mt        *meta.Meta
	indexMeta *IndexMeta
	cursor    int64
	ready     bool
}

func NewManifestIndexRollbackInspector(ctx context.Context, m *meta.Meta, im *IndexMeta) *ManifestIndexRollbackInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &ManifestIndexRollbackInspector{ctx: ctx, cancel: cancel, mt: m, indexMeta: im}
}

func (i *ManifestIndexRollbackInspector) Start() {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !meta.ManifestIndexRollbackEnabled() || i.mt == nil || i.indexMeta == nil {
		return
	}
	mlog.Info(i.ctx, "starting manifest index rollback; new index completions use etcd")
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		runManifestIndexMigrationLoop(i.ctx, i.indexMeta.manifestIndexRollbackNotify, manifestIndexRollbackInterval, i.runOnce)
	}()
}

func manifestIndexRollbackInterval() time.Duration {
	interval := Params.DataCoordCfg.ManifestIndexRollbackInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return time.Minute
	}
	return interval
}

func (i *ManifestIndexRollbackInspector) Stop() {
	i.cancel()
	i.wg.Wait()
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
}

type segmentManifestIndexRollback struct {
	segment  *meta.SegmentInfo
	buildIDs []int64
}

// scan groups manifest-resident records by segment, including superseded
// builds and dropped definitions whose catalog rows still need restoring.
func (i *ManifestIndexRollbackInspector) scan(ctx context.Context) ([]segmentManifestIndexRollback, int) {
	groups := make(map[int64]*segmentManifestIndexRollback)
	pendingRecords := 0
	i.indexMeta.SegmentBuildInfo.buildID2SegmentIndex.Range(func(_ int64, record *model.SegmentIndex) bool {
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
			segment := i.mt.GetSegment(ctx, record.SegmentID)
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

func (i *ManifestIndexRollbackInspector) runOnce(ctx context.Context) bool {
	metrics.DataCoordManifestIndexRollbackReady.Set(0)
	if !meta.ManifestIndexRollbackEnabled() || i.mt == nil || i.indexMeta == nil || ctx.Err() != nil {
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
	concurrency := min(limit, max(1, Params.DataCoordCfg.ManifestIndexRollbackConcurrency.GetAsInt()))
	// One owner per scan, shared by all segment reads and closed only after
	// every accepted read and commit has drained. Commits lease meta's executor.
	io := packed.NewManifestIOContext(concurrency)
	defer io.Close()
	var workers errgroup.Group
	workers.SetLimit(concurrency)
	for offset := 0; offset < limit; offset++ {
		if ctx.Err() != nil {
			break
		}
		item := work[(start+offset)%len(work)]
		segment := item.segment
		i.cursor = segment.GetID()
		workers.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
			_, err := i.mt.RollbackSegmentIndexes(ctx, io, segment.GetID(), item.buildIDs...)
			status := "success"
			if err != nil {
				status = "failed"
				if errors.Is(err, meta.ErrSegmentManifestStale) || errors.Is(err, meta.ErrSegmentIndexRollbackSkipped) {
					status = "stale"
				}
				mlog.RatedWarn(ctx, 1, "manifest index rollback will retry", mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
			}
			metrics.DataCoordManifestIndexRollbackAttempts.WithLabelValues(status).Inc()
			return nil
		})
	}
	_ = workers.Wait()
	return true
}
