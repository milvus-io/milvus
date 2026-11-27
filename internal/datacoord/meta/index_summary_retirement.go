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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RetireIndexSummaryLocked is used after deleting a dropped segment's files or
// an artifact-free completion. The caller holds the segment manifest lock.
func (m *Meta) RetireIndexSummaryLocked(ctx context.Context, segmentID, buildID int64) error {
	ids := []int64{buildID}
	m.IndexCatalog.LockBuildIDs(ids)
	defer m.IndexCatalog.UnlockBuildIDs(ids)
	staged, err := m.IndexCatalog.StageSegmentIndexMutation(SegmentIndexMutation{Type: SegmentIndexRemove, SegmentID: segmentID, BuildID: buildID})
	if err != nil {
		return err
	}
	if staged.Summary == nil {
		return nil
	}
	var observe func()
	err = func() error {
		defer m.LockSegments(segmentID)()
		segment := m.Segments.GetSegment(segmentID)
		if segment != nil && IsSegmentHealthy(segment) && staged.Record == nil && staged.Summary.HasArtifact {
			return merr.WrapErrServiceInternalMsg("live artifact summary must be retired with its manifest entry")
		}
		var updated *SegmentInfo
		var actions []metastore.UpdateAction
		if segment != nil && staged.PublishSummary {
			updated = segment.Clone()
			ApplyIndexSummary(updated.SegmentInfo, staged.Summary, 0, false)
			actions = append(actions, metastore.UpdateSegment(updated.SegmentInfo))
		}
		if staged.Action != nil {
			actions = append(actions, *staged.Action)
		}
		if err := m.Catalog.Update(ctx, actions...); err != nil {
			return merr.Wrap(err, "retire index summary")
		}
		if updated != nil {
			m.Segments.SetSegment(segmentID, updated)
		}
		observe = staged.Install()
		return nil
	}()
	if err == nil && observe != nil {
		observe()
	}
	return err
}

// RetireDroppedSegmentIndexSummariesLocked removes the readiness inventory in one
// catalog update. The caller holds the segment manifest lock; the manifest and
// task rows remain available to retry file cleanup.
func (m *Meta) RetireDroppedSegmentIndexSummariesLocked(ctx context.Context, segmentID int64) error {
	segment := m.Segments.GetSegment(segmentID)
	if segment == nil || len(segment.GetIndexSummaries()) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(segment.GetIndexSummaries()))
	for _, summary := range segment.GetIndexSummaries() {
		ids = append(ids, summary.GetBuildId())
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	m.IndexCatalog.LockBuildIDs(ids)
	defer m.IndexCatalog.UnlockBuildIDs(ids)

	retired := make(map[int64]*StagedSegmentIndexMutation, len(ids))
	for _, id := range ids {
		staged, err := m.IndexCatalog.StageSegmentIndexMutation(SegmentIndexMutation{
			Type: SegmentIndexRemove, SegmentID: segmentID, BuildID: id,
		})
		if err != nil {
			return err
		}
		if staged.Summary != nil && staged.Record == nil {
			retired[id] = staged
		}
	}
	if len(retired) == 0 {
		return nil
	}

	err := func() error {
		defer m.LockSegments(segmentID)()
		current := m.Segments.GetSegment(segmentID)
		if current == nil || current.GetState() != commonpb.SegmentState_Dropped {
			return merr.WrapErrServiceNotReadyMsg("segment %d is no longer dropped during index retirement", segmentID)
		}
		updated := current.Clone()
		updated.IndexSummaries = slices.DeleteFunc(updated.IndexSummaries, func(summary *datapb.SegmentIndexSummary) bool {
			_, remove := retired[summary.GetBuildId()]
			return remove
		})
		if err := m.Catalog.Update(ctx, metastore.UpdateSegment(updated.SegmentInfo)); err != nil {
			return merr.Wrap(err, "retire dropped segment index summaries")
		}
		m.Segments.SetSegment(segmentID, updated)
		for _, staged := range retired {
			staged.Install()
		}
		return nil
	}()
	if err == nil {
		m.IndexCatalog.PublishStoredIndexSizes()
	}
	return err
}
