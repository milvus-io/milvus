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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
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
	segment := m.GetSegment(ctx, record.SegmentID)
	if !IsSegmentHealthy(segment) || segment.GetStorageVersion() != storage.StorageV3 ||
		segment.GetLevel() == datapb.SegmentLevel_L0 || !m.publishesSegmentSummaries(record.SegmentID) {
		return m.IndexCatalog.FinishTask(result)
	}
	if len(result.GetIndexFileKeys()) > 0 {
		if metautil.IsSegmentRooted(record.IndexStorePathVersion) && m.IndexCatalog.IsIndexExist(record.CollectionID, record.IndexID) {
			return merr.Wrapf(merr.ErrDataIntegrity, "summary mode requires manifest publication for build %d", result.GetBuildID())
		}
		return m.IndexCatalog.FinishTask(result)
	}
	if !ManifestIndexPublicationEnabled() && !metautil.IsSegmentRooted(record.IndexStorePathVersion) {
		return m.IndexCatalog.FinishTask(result)
	}
	return m.publishIndexWithoutArtifact(ctx, record, result)
}

func (m *Meta) publishesSegmentSummaries(segmentID int64) bool {
	if source, ok := m.IndexCatalog.(interface{ PublishesSegmentSummaries(int64) bool }); ok {
		return source.PublishesSegmentSummaries(segmentID)
	}
	return m.IndexCatalog.UsesSegmentSummaries()
}

// MigrateIndexWithoutArtifact retires a legacy finished record without
// changing the manifest. A concurrent build or artifact keeps its ownership.
func (m *Meta) MigrateIndexWithoutArtifact(ctx context.Context, record *model.SegmentIndex) error {
	return m.publishIndexWithoutArtifact(ctx, record, nil)
}

func (m *Meta) publishIndexWithoutArtifact(ctx context.Context, record *model.SegmentIndex, result *workerpb.IndexTaskInfo) error {
	segmentID := record.SegmentID
	locks := m.GetSegmentManifestLocks()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	if err := m.CheckSegmentManifestIndexes(segmentID); err != nil {
		return err
	}
	finishAsTask := false
	err := func() error {
		ids := []int64{record.BuildID}
		m.IndexCatalog.LockBuildIDs(ids)
		defer m.IndexCatalog.UnlockBuildIDs(ids)
		mutation := SegmentIndexMutation{Type: SegmentIndexUpsert, BuildID: record.BuildID, SegmentID: segmentID, FinishedTask: result}
		if result == nil {
			current, ok := m.IndexCatalog.GetIndexJob(record.BuildID)
			if !ok || len(current.IndexFileKeys) != 0 || !m.publishesSegmentSummaries(segmentID) {
				return ErrSegmentIndexBackfillSkipped
			}
			mutation.Type = SegmentIndexBackfill
		}
		staged, err := m.IndexCatalog.StageSegmentIndexMutation(mutation)
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
		if !IsSegmentHealthy(segment) || segment.GetStorageVersion() != storage.StorageV3 ||
			segment.GetLevel() == datapb.SegmentLevel_L0 ||
			result == nil && (segment.GetIsImporting() || segment.GetState() == commonpb.SegmentState_Importing) {
			finishAsTask = true
			return nil
		}
		for _, summary := range segment.GetIndexSummaries() {
			if summary.GetIndexId() == record.IndexID && summary.GetHasArtifact() {
				if result == nil {
					return ErrSegmentIndexBackfillSkipped
				}
				return merr.WrapErrServiceInternalMsg("empty completion cannot replace a live index artifact")
			}
		}
		updated := segment.Clone()
		ApplyIndexSummary(updated.SegmentInfo, staged.Summary, fieldID, true)
		MarkIndexSummaryCoverage(segment.SegmentInfo, updated.SegmentInfo)
		if err := m.Catalog.Update(ctx, metastore.UpdateSegment(updated.SegmentInfo), *staged.Action); err != nil {
			return merr.Wrap(err, "publish empty index completion")
		}
		m.Segments.SetSegment(segmentID, updated)
		observe = staged.Install()
		return nil
	}()
	if finishAsTask {
		if result == nil {
			return ErrSegmentIndexBackfillSkipped
		}
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
