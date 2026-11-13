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

// CompleteEmptyIndex atomically publishes completion without an artifact or a
// new manifest revision. It uses the same lock order as artifact publication.
func (m *Meta) CompleteEmptyIndex(ctx context.Context, segmentID int64, result *workerpb.IndexTaskInfo) (bool, error) {
	if result.GetState() != commonpb.IndexState_Finished || len(result.GetIndexFileKeys()) != 0 {
		return false, merr.WrapErrServiceInternalMsg("empty index completion requires a finished result without files")
	}
	m.segmentManifestLocks.Lock(segmentID)
	defer m.segmentManifestLocks.Unlock(segmentID)
	ids := []int64{result.GetBuildID()}
	m.IndexCatalog.LockBuildIDs(ids)
	defer m.IndexCatalog.UnlockBuildIDs(ids)
	record, ok := m.IndexCatalog.GetIndexJob(result.GetBuildID())
	if !ok {
		return true, nil
	}
	if record.SegmentID != segmentID {
		return false, merr.WrapErrServiceInternalMsg("empty index result belongs to another segment")
	}
	fieldID := m.IndexCatalog.GetFieldIDByIndexID(record.CollectionID, record.IndexID)
	staged, err := m.IndexCatalog.StageSegmentIndexMutation(SegmentIndexMutation{Type: SegmentIndexUpsert, BuildID: result.GetBuildID(), FinishedTask: result})
	if errors.Is(err, ErrSegmentIndexRecordGone) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var observe func()
	err = func() error {
		m.SegMu.Lock()
		defer m.SegMu.Unlock()
		segment := m.Segments.GetSegment(segmentID)
		if !IsSegmentHealthy(segment) {
			return merr.WrapErrSegmentNotFound(segmentID)
		}
		for _, summary := range segment.GetIndexSummaries() {
			if summary.GetIndexId() == record.IndexID && summary.GetBuildId() > record.BuildID {
				return merr.WrapErrServiceInternalMsg("obsolete empty index completion for build %d", record.BuildID)
			}
		}
		updated := segment.Clone()
		ApplyIndexSummary(updated.SegmentInfo, staged.Record, fieldID, true)
		if err := m.Catalog.Update(ctx, metastore.UpdateSegment(updated.SegmentInfo), *staged.Action); err != nil {
			return merr.Wrap(err, "publish empty index completion")
		}
		m.Segments.SetSegment(segmentID, updated)
		observe = staged.Install()
		return nil
	}()
	if err != nil {
		return false, err
	}
	if observe != nil {
		observe()
	}
	return true, nil
}
