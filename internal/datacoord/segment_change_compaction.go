// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Compaction plan IDs come from the global allocator, so reusing the plan ID
// as group ID gives retries a durable identity, including empty-output tasks.
// Inspect this identity before consulting the opt-in flag or parent health.
func (m *meta) compactionChangeGroupLocked(task *datapb.CompactionTask) (*model.SegmentChangeGroup, error) {
	if task.GetType() != datapb.CompactionType_MixCompaction && task.GetType() != datapb.CompactionType_SortCompaction && task.GetType() != datapb.CompactionType_ClusteringCompaction && task.GetType() != datapb.CompactionType_BumpSchemaVersionCompaction {
		return nil, nil
	}
	g := m.segmentChangeGroups[task.GetPlanID()]
	if g == nil {
		if task.GetType() == datapb.CompactionType_SortCompaction {
			for _, candidate := range m.segmentChangeGroups {
				if job, ok := candidate.SortJobs[task.GetPlanID()]; ok {
					if candidate.CollectionID != task.GetCollectionID() || !segmentIDSetEqual(task.GetInputSegments(), []int64{job.InputSegmentID}) {
						return nil, merr.WrapErrDataIntegrityMsg("sort plan %d conflicts with its group lineage", task.GetPlanID())
					}
					return candidate, nil
				}
			}
		}
		return nil, nil
	}
	source := model.SegmentChangeSourceMixCompaction
	if task.GetType() == datapb.CompactionType_SortCompaction {
		source = model.SegmentChangeSourceSortCompaction
	}
	if task.GetType() == datapb.CompactionType_BumpSchemaVersionCompaction {
		source = model.SegmentChangeSourceBumpSchema
	}
	parents := task.GetInputSegments()
	if task.GetType() == datapb.CompactionType_ClusteringCompaction {
		source = model.SegmentChangeSourceClustering
		parents = clusteringPublicationGroup(task).SupersededSegmentIDs
	}
	if g.Source != source || g.SourceJobID != task.GetPlanID() || g.CollectionID != task.GetCollectionID() ||
		!segmentIDSetEqual(g.SupersededSegmentIDs, parents) {
		return nil, merr.WrapErrDataIntegrityMsg("compaction plan %d conflicts with an existing segment change group", task.GetPlanID())
	}
	return g, nil
}

func (m *meta) getCompactionChangeGroup(task *datapb.CompactionTask) (*model.SegmentChangeGroup, error) {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	g, err := m.compactionChangeGroupLocked(task)
	if g == nil || err != nil {
		return nil, err
	}
	return g.Clone(), nil
}

func compactionGroupResultIDs(g *model.SegmentChangeGroup, task *datapb.CompactionTask) []int64 {
	if job, ok := g.SortJobs[task.GetPlanID()]; ok {
		return []int64{job.OutputSegmentID}
	}
	return append([]int64(nil), g.NewSegmentIDs...)
}

func (m *meta) stagedCompactionResultLocked(task *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, bool, error) {
	g, err := m.compactionChangeGroupLocked(task)
	if err != nil || g == nil {
		return nil, false, err
	}
	ids := make([]int64, 0, len(result.GetSegments()))
	for _, output := range result.GetSegments() {
		ids = append(ids, output.GetSegmentID())
	}
	expected := g.InitialSegmentIDs
	if expected == nil {
		expected = g.NewSegmentIDs
	}
	if job, ok := g.SortJobs[task.GetPlanID()]; ok {
		expected = []int64{job.OutputSegmentID}
	}
	if !segmentIDSetEqual(ids, expected) {
		return nil, true, merr.WrapErrDataIntegrityMsg("compaction plan %d replay changed its registered outputs", task.GetPlanID())
	}
	segments := make([]*SegmentInfo, 0, len(ids))
	for _, id := range compactionGroupResultIDs(g, task) {
		if seg := m.segments.GetSegment(id); seg != nil {
			segments = append(segments, seg)
		} else if g.State != model.SegmentChangeStateCommitted {
			return nil, true, merr.WrapErrDataIntegrityMsg("compaction plan %d has a missing staged output %d", task.GetPlanID(), id)
		}
	}
	return segments, true, nil
}

// Sorting incomplete outputs remain private members of this group until their
// sort results replace them through completeGroupSortLocked.
func (m *meta) canStageCompactionOutputs(task *datapb.CompactionTask, outputs []*SegmentInfo) bool {
	return Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && task.GetType() == datapb.CompactionType_MixCompaction
}

func (m *meta) stageCompactionOutputsLocked(ctx context.Context, task *datapb.CompactionTask, outputs []*SegmentInfo) error {
	g := &model.SegmentChangeGroup{
		GroupID: task.GetPlanID(), SourceJobID: task.GetPlanID(), Source: model.SegmentChangeSourceMixCompaction,
		CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(),
		State: model.SegmentChangeStateStaged, SupersededSegmentIDs: append([]int64(nil), task.GetInputSegments()...),
		CreateTS: time.Now().Unix(),
	}
	if task.GetType() == datapb.CompactionType_SortCompaction {
		g.Source = model.SegmentChangeSourceSortCompaction
	}
	if task.GetType() == datapb.CompactionType_BumpSchemaVersionCompaction {
		g.Source = model.SegmentChangeSourceBumpSchema
	}
	for _, output := range outputs {
		g.NewSegmentIDs = append(g.NewSegmentIDs, output.GetID())
		output.IsInvisible = true
	}
	g.InitialSegmentIDs = append([]int64(nil), g.NewSegmentIDs...)
	if err := m.updateSegmentsInfoAndChangeGroupsLocked(ctx, nil, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)}, func(pack *updateSegmentPack) bool {
		for _, output := range outputs {
			pack.segments[output.GetID()] = output.Clone()
			pack.increments[output.GetID()] = metastore.BinlogsIncrement{Segment: output.SegmentInfo}
		}
		return true
	}); err != nil {
		return err
	}
	for _, id := range task.GetInputSegments() {
		m.segments.SetIsCompacting(id, true)
	}
	return nil
}

// Only unsorted STAGED members may enter a sort task. Other compactions still
// exclude both unpublished outputs and superseded parents.
func (m *meta) canSortGroupMemberLocked(id int64) bool {
	g := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]
	seg := m.segments.GetSegment(id)
	return g != nil && g.State == model.SegmentChangeStateStaged && seg != nil && isFlushState(seg.GetState()) &&
		seg.GetIsInvisible() && !seg.GetIsImporting() && !seg.GetIsSorted() && !seg.GetIsSortedByNamespace() && !isEmptySegmentChangeOutput(seg)
}

// Read copy admission before taking segMu: copy metadata owns its own mutex.
// Groups are read afterward, so a copy task finishing between the snapshots
// is still covered by the group it registered before completion.
func (c *compactionInspector) segmentChangeL0Exclusions() typeutil.Set[string] {
	channels := typeutil.NewSet[string]()
	m, ok := c.meta.(*meta)
	if !ok {
		return channels
	}
	if Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() {
		if provider, ok := c.copySegmentMeta.(interface{ activeCopyCollectionIDs() []int64 }); ok {
			for _, id := range provider.activeCopyCollectionIDs() {
				if coll := m.GetCollection(id); coll != nil {
					channels.Insert(coll.VChannelNames...)
				}
			}
		}
	}
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	for _, g := range m.segmentChangeGroups {
		if g.IsTerminal() {
			continue
		}
		if g.VChannel != "" {
			channels.Insert(g.VChannel)
		}
		for _, id := range append(append([]int64(nil), g.NewSegmentIDs...), g.SupersededSegmentIDs...) {
			if seg := m.segments.GetSegment(id); seg != nil {
				channels.Insert(seg.GetInsertChannel())
			}
		}
	}
	return channels
}

func (m *meta) groupSortCandidates(collectionID int64) map[int64]bool {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	ids := make(map[int64]bool)
	for id := range m.stagedSegmentToGroup {
		if g := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]; (collectionID <= 0 || g.CollectionID == collectionID) && m.canSortGroupMemberLocked(id) {
			ids[id] = true
		}
	}
	return ids
}

func (m *meta) reserveGroupSort(task *datapb.CompactionTask) bool {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if task.GetType() != datapb.CompactionType_SortCompaction || len(task.GetInputSegments()) != 1 {
		return false
	}
	id := task.GetInputSegments()[0]
	seg := m.segments.GetSegment(id)
	if !m.canSortGroupMemberLocked(id) || seg.isCompacting || seg.GetCollectionID() != task.GetCollectionID() {
		return false
	}
	m.segments.SetIsCompacting(id, true)
	return true
}

func (m *meta) completeGroupSortLocked(task *datapb.CompactionTask, output *SegmentInfo) (bool, error) {
	id := task.GetInputSegments()[0]
	current := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]
	if current == nil {
		return false, nil
	}
	if !m.canSortGroupMemberLocked(id) || task.GetCollectionID() != current.CollectionID || output.GetCollectionID() != current.CollectionID ||
		m.segments.GetSegment(output.GetID()) != nil || m.stagedSegmentToGroup[output.GetID()] != 0 || m.supersededSegmentToGroup[output.GetID()] != 0 {
		return true, merr.WrapErrDataIntegrityMsg("sort plan %d cannot replace group member %d", task.GetPlanID(), id)
	}
	next := current.Clone()
	if next.InitialSegmentIDs == nil {
		next.InitialSegmentIDs = append([]int64(nil), current.NewSegmentIDs...)
	}
	for i, member := range next.NewSegmentIDs {
		if member == id {
			next.NewSegmentIDs[i] = output.GetID()
		}
	}
	if next.SortJobs == nil {
		next.SortJobs = make(map[int64]model.SegmentChangeSortJob)
	}
	next.SortJobs[task.GetPlanID()] = model.SegmentChangeSortJob{InputSegmentID: id, OutputSegmentID: output.GetID()}
	output.IsInvisible = true
	err := m.updateSegmentsInfoAndChangeGroupsMaterializedLocked(m.ctx, nil, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(next)}, next,
		func(pack *updateSegmentPack) bool {
			pack.segments[output.GetID()] = output.Clone()
			pack.increments[output.GetID()] = metastore.BinlogsIncrement{Segment: output.SegmentInfo}
			return true
		}, UpdateStatusOperator(id, commonpb.SegmentState_Dropped), UpdateCompactedOperator(id))
	return true, err
}
