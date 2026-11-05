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

package meta

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

// SegmentChangeL0Channels returns channels protected by nonterminal groups.
func (m *Meta) SegmentChangeL0Channels(copyCollectionIDs []int64) typeutil.Set[string] {
	channels := typeutil.NewSet[string]()
	for _, id := range copyCollectionIDs {
		if coll := m.GetCollection(id); coll != nil {
			channels.Insert(coll.VChannelNames...)
		}
	}
	m.SegMu.RLock()
	defer m.SegMu.RUnlock()
	for _, g := range m.segmentChangeGroups {
		if g.IsTerminal() {
			continue
		}
		if g.VChannel != "" {
			channels.Insert(g.VChannel)
		}
		for _, id := range append(append([]int64(nil), g.NewSegmentIDs...), g.SupersededSegmentIDs...) {
			if seg := m.Segments.GetSegment(id); seg != nil {
				channels.Insert(seg.GetInsertChannel())
			}
		}
	}
	return channels
}

// Compaction plan IDs come from the global allocator, so reusing the plan ID
// as group ID gives retries a durable identity, including empty-output tasks.
// Inspect this identity before consulting the opt-in flag or parent health.
func (m *Meta) compactionChangeGroupLocked(task *datapb.CompactionTask) (*model.SegmentChangeGroup, error) {
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
		parents = ClusteringPublicationGroup(task).SupersededSegmentIDs
	}
	if g.Source != source || g.SourceJobID != task.GetPlanID() || g.CollectionID != task.GetCollectionID() ||
		!segmentIDSetEqual(g.SupersededSegmentIDs, parents) {
		return nil, merr.WrapErrDataIntegrityMsg("compaction plan %d conflicts with an existing segment change group", task.GetPlanID())
	}
	return g, nil
}

func (m *Meta) GetCompactionChangeGroup(task *datapb.CompactionTask) (*model.SegmentChangeGroup, error) {
	m.SegMu.RLock()
	defer m.SegMu.RUnlock()
	g, err := m.compactionChangeGroupLocked(task)
	if g == nil || err != nil {
		return nil, err
	}
	return g.Clone(), nil
}

func CompactionGroupResultIDs(g *model.SegmentChangeGroup, task *datapb.CompactionTask) []int64 {
	if job, ok := g.SortJobs[task.GetPlanID()]; ok {
		return []int64{job.OutputSegmentID}
	}
	return append([]int64(nil), g.NewSegmentIDs...)
}

func (m *Meta) stagedCompactionResultLocked(task *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, bool, error) {
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
	for _, id := range CompactionGroupResultIDs(g, task) {
		if seg := m.Segments.GetSegment(id); seg != nil {
			segments = append(segments, seg)
		} else if g.State != model.SegmentChangeStateCommitted {
			return nil, true, merr.WrapErrDataIntegrityMsg("compaction plan %d has a missing staged output %d", task.GetPlanID(), id)
		}
	}
	return segments, true, nil
}

// Sorting incomplete outputs remain private members of this group until their
// sort results replace them through completeGroupSortLocked.
func (m *Meta) canStageCompactionOutputs(task *datapb.CompactionTask, outputs []*SegmentInfo) bool {
	return Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && task.GetType() == datapb.CompactionType_MixCompaction
}

func (m *Meta) stageCompactionOutputsLocked(ctx context.Context, task *datapb.CompactionTask, outputs []*SegmentInfo) error {
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
	if err := m.updateSegmentsInfoAndChangeGroupsLocked(ctx, nil, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)}, func(pack *UpdateSegmentPack) bool {
		for _, output := range outputs {
			pack.Segments[output.GetID()] = output.Clone()
			pack.Increments[output.GetID()] = metastore.BinlogsIncrement{Segment: output.SegmentInfo}
		}
		return true
	}); err != nil {
		return err
	}
	for _, id := range task.GetInputSegments() {
		m.Segments.SetIsCompacting(id, true)
	}
	return nil
}

// Only unsorted STAGED members may enter a sort task. Other compactions still
// exclude both unpublished outputs and superseded parents.
func (m *Meta) canSortGroupMemberLocked(id int64) bool {
	g := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]
	seg := m.Segments.GetSegment(id)
	return g != nil && g.State == model.SegmentChangeStateStaged && seg != nil && IsFlushState(seg.GetState()) &&
		seg.GetIsInvisible() && !seg.GetIsImporting() && !seg.GetIsSorted() && !seg.GetIsSortedByNamespace() && !isEmptySegmentChangeOutput(seg)
}

func (m *Meta) GroupSortCandidates(collectionID int64) map[int64]bool {
	m.SegMu.RLock()
	defer m.SegMu.RUnlock()
	ids := make(map[int64]bool)
	for id := range m.stagedSegmentToGroup {
		if g := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]; (collectionID <= 0 || g.CollectionID == collectionID) && m.canSortGroupMemberLocked(id) {
			ids[id] = true
		}
	}
	return ids
}

func (m *Meta) ReserveGroupSort(task *datapb.CompactionTask) bool {
	m.SegMu.Lock()
	defer m.SegMu.Unlock()
	if task.GetType() != datapb.CompactionType_SortCompaction || len(task.GetInputSegments()) != 1 {
		return false
	}
	id := task.GetInputSegments()[0]
	seg := m.Segments.GetSegment(id)
	if !m.canSortGroupMemberLocked(id) || seg.IsCompacting || seg.GetCollectionID() != task.GetCollectionID() {
		return false
	}
	m.Segments.SetIsCompacting(id, true)
	return true
}

func (m *Meta) completeGroupSortLocked(task *datapb.CompactionTask, output *SegmentInfo) (bool, error) {
	id := task.GetInputSegments()[0]
	current := m.segmentChangeGroups[m.stagedSegmentToGroup[id]]
	if current == nil {
		return false, nil
	}
	if !m.canSortGroupMemberLocked(id) || task.GetCollectionID() != current.CollectionID || output.GetCollectionID() != current.CollectionID ||
		m.Segments.GetSegment(output.GetID()) != nil || m.stagedSegmentToGroup[output.GetID()] != 0 || m.supersededSegmentToGroup[output.GetID()] != 0 {
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
	err := m.updateSegmentsInfoAndChangeGroupsMaterializedLocked(m.Ctx, nil, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(next)}, next,
		func(pack *UpdateSegmentPack) bool {
			pack.Segments[output.GetID()] = output.Clone()
			pack.Increments[output.GetID()] = metastore.BinlogsIncrement{Segment: output.SegmentInfo}
			return true
		}, UpdateStatusOperator(id, commonpb.SegmentState_Dropped), UpdateCompactedOperator(id))
	return true, err
}

func ClusteringPublicationGroup(task *datapb.CompactionTask) *model.SegmentChangeGroup {
	parents := append([]int64(nil), task.GetInputSegments()...)
	for _, id := range task.GetTmpSegments() {
		if !containsInt64(task.GetResultSegments(), id) && !containsInt64(parents, id) {
			parents = append(parents, id)
		}
	}
	return &model.SegmentChangeGroup{
		GroupID: task.GetPlanID(), SourceJobID: task.GetPlanID(), Source: model.SegmentChangeSourceClustering,
		CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(), VChannel: task.GetChannel(),
		NewSegmentIDs: append([]int64(nil), task.GetResultSegments()...), SupersededSegmentIDs: parents,
		PartitionStats: &datapb.PartitionStatsInfo{
			CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(),
			VChannel: task.GetChannel(), Version: task.GetPlanID(), SegmentIDs: append([]int64(nil), task.GetResultSegments()...), CommitTime: time.Now().Unix(),
		},
	}
}
