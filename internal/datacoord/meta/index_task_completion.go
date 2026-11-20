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

package meta

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FinishTask persists a result not published through the manifest. Summary-mode
// completions without files atomically publish the summary and retire the task.
func (m *Meta) FinishTask(ctx context.Context, result *workerpb.IndexTaskInfo) error {
	if !m.IndexCatalog.UsesSegmentSummaries() || result.GetState() != commonpb.IndexState_Finished {
		return m.IndexCatalog.FinishTask(result)
	}
	record, ok := m.IndexCatalog.GetIndexJob(result.GetBuildID())
	if !ok {
		return nil
	}
	segmentID := record.SegmentID
	locks := m.GetSegmentManifestLocks()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	if len(result.GetIndexFileKeys()) > 0 {
		if segment := m.GetSegment(ctx, segmentID); IsSegmentHealthy(segment) && m.IndexCatalog.IsIndexExist(record.CollectionID, record.IndexID) {
			return merr.Wrapf(merr.ErrDataIntegrity, "summary mode requires manifest publication for build %d", result.GetBuildID())
		}
		return m.IndexCatalog.FinishTask(result)
	}
	finishAsTask := false
	err := func() error {
		ids := []int64{result.GetBuildID()}
		m.IndexCatalog.LockBuildIDs(ids)
		defer m.IndexCatalog.UnlockBuildIDs(ids)
		staged, err := m.IndexCatalog.StageSegmentIndexMutation(SegmentIndexMutation{
			Type: SegmentIndexUpsert, BuildID: result.GetBuildID(), SegmentID: segmentID, FinishedTask: result,
		})
		if errors.Is(err, ErrSegmentIndexRecordGone) {
			return nil
		}
		if err != nil {
			return err
		}
		if staged.Obsolete {
			return m.retireObsoleteIndexTask(ctx, staged)
		}
		var observe func()
		defer func() {
			if observe != nil {
				observe()
			}
		}()
		m.IndexCatalog.RLockFieldIndexes()
		defer m.IndexCatalog.RUnlockFieldIndexes()
		fieldID, live := m.IndexCatalog.GetLiveIndexFieldIDLocked(record.CollectionID, record.IndexID)
		if !live {
			finishAsTask = true
			return nil
		}
		defer m.LockSegments(segmentID)()
		segment := m.Segments.GetSegment(segmentID)
		if !IsSegmentHealthy(segment) {
			finishAsTask = true
			return nil
		}
		for _, summary := range segment.GetIndexSummaries() {
			if summary.GetIndexId() == record.IndexID && summary.GetHasArtifact() {
				return merr.WrapErrServiceInternalMsg("empty completion cannot replace a live index artifact")
			}
		}
		updated := segment.Clone()
		ApplyIndexSummary(updated.SegmentInfo, staged.Summary, fieldID, true)
		if err := m.Catalog.Update(ctx, metastore.UpdateSegment(updated.SegmentInfo), *staged.Action); err != nil {
			return merr.Wrap(err, "publish empty index completion")
		}
		m.Segments.SetSegment(segmentID, updated)
		observe = staged.Install()
		return nil
	}()
	if finishAsTask {
		return m.IndexCatalog.FinishTask(result)
	}
	return err
}

// The caller holds the segment manifest lock and the obsolete build's lock.
func (m *Meta) retireObsoleteIndexTask(ctx context.Context, staged *StagedSegmentIndexMutation) error {
	if err := m.Catalog.Update(ctx, *staged.Action); err != nil {
		return merr.Wrap(err, "retire obsolete index task")
	}
	staged.Install()()
	return nil
}
