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
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *meta) producerChangeGroup(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	return m.producerChangeGroupLocked(source, jobID, channel).Clone()
}

func (m *meta) producerChangeGroupLocked(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	for _, g := range m.segmentChangeGroups {
		if g.Source == source && g.SourceJobID == jobID && g.VChannel == channel {
			return g
		}
	}
	return nil
}

func (m *meta) producerChangeGroups(source model.SegmentChangeSource, jobID int64, channel string) []*model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	var groups []*model.SegmentChangeGroup
	for _, g := range m.segmentChangeGroups {
		if g.Source == source && g.SourceJobID == jobID && (channel == "" || g.VChannel == channel) {
			groups = append(groups, g.Clone())
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].SubgroupOrdinal < groups[j].SubgroupOrdinal })
	return groups
}

// Registration is the producer's durable publication anchor. The flag only
// controls new registrations; retries always recover the existing identity.
func (m *meta) stageExistingChange(ctx context.Context, g *model.SegmentChangeGroup, operators ...UpdateOperator) (*model.SegmentChangeGroup, bool, error) {
	return m.stageAdmittedChange(ctx, g, false, operators...)
}

func (m *meta) stageAdmittedChange(ctx context.Context, g *model.SegmentChangeGroup, admitted bool, operators ...UpdateOperator) (*model.SegmentChangeGroup, bool, error) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if current := m.segmentChangeGroups[g.GroupID]; current != nil {
		if current.CollectionID != g.CollectionID || current.Source != g.Source || current.SourceJobID != g.SourceJobID || current.VChannel != g.VChannel {
			return nil, true, merr.WrapErrDataIntegrityMsg("producer %d conflicts with its registered group", g.SourceJobID)
		}
		return current.Clone(), true, nil
	}
	if !admitted && !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() {
		return nil, false, nil
	}
	g = g.Clone()
	g.State = model.SegmentChangeStateStaged
	g.CreateTS = time.Now().Unix()
	g.InitialSegmentIDs = append([]int64(nil), g.NewSegmentIDs...)
	for _, id := range g.NewSegmentIDs {
		operators = append(operators, SetSegmentIsInvisible(id, true), UpdateIsImporting(id, false))
	}
	if err := m.updateSegmentsInfoAndChangeGroupsLocked(ctx, nil, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)}, operators...); err != nil {
		return nil, true, err
	}
	return g.Clone(), true, nil
}

func (m *meta) requirePublishedChange(ctx context.Context, g *model.SegmentChangeGroup) error {
	current := m.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
	if current == nil {
		return merr.WrapErrDataIntegrityMsg("producer group %d disappeared", g.GroupID)
	}
	switch current.State {
	case model.SegmentChangeStateCommitted:
		return nil
	case model.SegmentChangeStateFailed, model.SegmentChangeStateAborted:
		return merr.WrapErrDataIntegrityMsg("producer group %d ended in %s: %s", current.GroupID, current.State, current.FailReason)
	default:
		return merr.WrapErrServiceUnavailableMsg("producer group %d is waiting for publication", current.GroupID)
	}
}

func (m *meta) abortProducerChange(ctx context.Context, g *model.SegmentChangeGroup, reason string) error {
	if g == nil || g.State == model.SegmentChangeStateCommitted {
		return nil
	}
	if !g.IsTerminal() {
		g = g.Clone()
		g.State = model.SegmentChangeStateAborted
		g.FailReason = reason
		if err := m.UpdateSegmentChangeGroup(ctx, g); err != nil {
			return err
		}
	}
	return m.reclaimSegmentChangeGroup(ctx, g)
}

// The commit fence is assigned by this VChannel's WAL, including on a CDC
// secondary. Physical segment/group IDs are local and never enter the WAL.
func (s *Server) stageImportChange(ctx context.Context, job ImportJob, channel string, ids []int64, commitTS uint64) (bool, error) {
	return s.stageImportChangeFromSource(ctx, job, channel, ids, commitTS, model.SegmentChangeSourceImportJob)
}

// Check C23 before creating an irreversible WAL commit. The caller holds the
// collection broadcast resource key, which also serializes schema DDL. Once
// committed, the callback may wait for stats/indexes without a later schema
// callback invalidating the group while it holds that same resource key.
func (s *Server) prepareImportPublication(ctx context.Context, job ImportJob) error {
	if !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && (s.meta == nil || len(s.meta.producerChangeGroups(model.SegmentChangeSourceImportJob, job.GetJobID(), "")) == 0) {
		return nil
	}
	if s.meta == nil {
		return merr.WrapErrServiceUnavailableMsg("import metadata is not ready")
	}
	coll := s.meta.GetCollection(job.GetCollectionID())
	if coll == nil || coll.Schema == nil {
		return merr.WrapErrServiceUnavailableMsg("import collection schema is not ready")
	}
	for _, channel := range job.GetVchannels() {
		ids := s.getImportSegmentIDsByVchannel(ctx, job.GetJobID(), channel)
		groups, _, err := s.stageImportGroups(ctx, job, channel, ids, 0, model.SegmentChangeSourceImportJob)
		if err != nil {
			return err
		}
		for _, g := range groups {
			ids = append(ids, g.NewSegmentIDs...)
		}
		for _, id := range ids {
			seg := s.meta.GetSegment(ctx, id)
			if seg != nil && isSegmentHealthy(seg) && !isEmptySegmentChangeOutput(seg) && seg.GetSchemaVersion() < coll.Schema.GetVersion() {
				return merr.WrapErrServiceUnavailableMsg("import segment %d schema %d is behind collection schema %d; replan the import before committing", id, seg.GetSchemaVersion(), coll.Schema.GetVersion())
			}
		}
		inspector := newSegmentChangeInspector(ctx, s.meta, nil)
		for _, g := range groups {
			if g.State == model.SegmentChangeStateStaged {
				inspector.evaluateStaged(ctx, g)
			}
			current := s.meta.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
			if current.State != model.SegmentChangeStateReady {
				return merr.WrapErrServiceUnavailableMsg("import group %d is not ready before WAL commit", g.GroupID)
			}
		}
	}
	return nil
}

func (s *Server) stageImportChangeFromSource(ctx context.Context, job ImportJob, channel string, ids []int64, commitTS uint64, source model.SegmentChangeSource) (bool, error) {
	groups, handled, err := s.stageImportGroups(ctx, job, channel, ids, commitTS, source)
	if err != nil || !handled {
		return handled, err
	}
	// The WAL ack already carries this channel's fence, and preparation made
	// the groups READY before the append. Publish subgroups in ordinal order
	// while the callback holds the collection resource key; the inspector still
	// retries any transient failure after the callback returns.
	inspector := newSegmentChangeInspector(ctx, s.meta, s.allocator)
	defer inspector.Stop()
	for _, g := range groups {
		current := s.meta.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
		if current != nil && current.State == model.SegmentChangeStateReady {
			inspector.tryPublish(ctx, current)
		}
		if err := s.meta.requirePublishedChange(ctx, g); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (s *Server) stageImportGroups(ctx context.Context, job ImportJob, channel string, ids []int64, commitTS uint64, source model.SegmentChangeSource) ([]*model.SegmentChangeGroup, bool, error) {
	if s.meta == nil {
		return nil, false, nil
	}
	groups := s.meta.producerChangeGroups(source, job.GetJobID(), channel)
	if len(groups) == 0 && !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && len(s.meta.producerChangeGroups(source, job.GetJobID(), "")) == 0 {
		return nil, false, nil
	}
	claimed := make(map[int64]bool)
	for _, g := range groups {
		if g.CollectionID != job.GetCollectionID() {
			return nil, true, merr.WrapErrDataIntegrityMsg("import group %d belongs to another collection", g.GroupID)
		}
		for _, id := range g.InitialSegmentIDs {
			claimed[id] = true
		}
		for _, id := range g.NewSegmentIDs {
			claimed[id] = true
		}
		if g.CommitTS != 0 {
			commitTS = g.CommitTS
		}
	}
	var members []int64
	for _, id := range ids {
		seg := s.meta.GetSegment(ctx, id)
		// A rolling opt-in may meet a partially completed legacy commit. Those
		// already-visible outputs keep their original publication; only its
		// remaining private outputs enter new groups.
		if seg != nil && !seg.GetIsImporting() && !seg.GetIsInvisible() {
			continue
		}
		if !claimed[id] && seg != nil && (isSegmentHealthy(seg) || isEmptySegmentChangeOutput(seg)) {
			members = append(members, id)
			claimed[id] = true
		}
	}
	limit := max(1, Params.DataCoordCfg.SegmentChangeGroupMaxMembers.GetAsInt())
	for len(members) > 0 {
		n := min(limit, len(members))
		batch := members[:n]
		members = members[n:]
		g := &model.SegmentChangeGroup{
			GroupID: batch[0], Source: source, SourceJobID: job.GetJobID(), CollectionID: job.GetCollectionID(), VChannel: channel,
			NewSegmentIDs: append([]int64(nil), batch...), CommitTS: commitTS, AwaitCommitFence: true, SubgroupOrdinal: int64(len(groups)),
		}
		if len(groups) > 0 {
			g.PreviousGroupID = groups[len(groups)-1].GroupID
		}
		g, _, err := s.meta.stageAdmittedChange(s.meta.ctx, g, true)
		if err != nil {
			return nil, true, err
		}
		groups = append(groups, g)
		notifySegmentIndexBuild(batch...)
	}
	for i, g := range groups {
		if commitTS > 0 && g.CommitTS == 0 {
			g = g.Clone()
			g.CommitTS = commitTS
			if err := s.meta.UpdateSegmentChangeGroup(s.meta.ctx, g); err != nil {
				return nil, true, err
			}
			groups[i] = g
		}
	}
	return groups, true, nil
}

func groupHasCommittedImportFence(g *model.SegmentChangeGroup) bool {
	return g.AwaitCommitFence && g.CommitTS > 0 && (g.Source == model.SegmentChangeSourceImportJob || g.Source == model.SegmentChangeSourceCDCReplicated)
}

func clusteringPublicationGroup(task *datapb.CompactionTask) *model.SegmentChangeGroup {
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
