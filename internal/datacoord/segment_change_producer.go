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

	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The commit fence is assigned by this VChannel's WAL, including on a CDC
// secondary. Physical segment/group IDs are local and never enter the WAL.
func (s *Server) stageImportChange(ctx context.Context, job bulkimport.ImportJob, channel string, ids []int64, commitTS uint64) (bool, error) {
	return s.stageImportChangeFromSource(ctx, job, channel, ids, commitTS, model.SegmentChangeSourceImportJob)
}

// Check C23 before creating an irreversible WAL commit. The caller holds the
// collection broadcast resource key, which also serializes schema DDL. Once
// committed, the callback may wait for stats/indexes without a later schema
// callback invalidating the group while it holds that same resource key.
func (s *Server) prepareImportPublication(ctx context.Context, job bulkimport.ImportJob) error {
	if !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && (s.meta == nil || len(s.meta.ProducerChangeGroups(model.SegmentChangeSourceImportJob, job.GetJobID(), "")) == 0) {
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
		ids := bulkimport.GetImportSegmentIDsByVchannel(ctx, s.importMeta, s.meta, job.GetJobID(), channel)
		groups, _, err := s.stageImportGroups(ctx, job, channel, ids, 0, model.SegmentChangeSourceImportJob)
		if err != nil {
			return err
		}
		for _, g := range groups {
			ids = append(ids, g.NewSegmentIDs...)
		}
		for _, id := range ids {
			seg := s.meta.GetSegment(ctx, id)
			if seg != nil && meta.IsSegmentHealthy(seg) && !meta.IsEmptySegmentChangeOutput(seg) && seg.GetSchemaVersion() < coll.Schema.GetVersion() {
				err := merr.WrapErrImportSysFailedMsg("import segment %d schema %d is behind collection schema %d; retry the import", id, seg.GetSchemaVersion(), coll.Schema.GetVersion())
				// The state and broadcast marker are checked under importMeta's
				// lock. Once a commit may be durable the job must never be failed.
				if updateErr := s.importMeta.UpdateJob(ctx, job.GetJobID(), func(current bulkimport.ImportJob) {
					if current.GetState() == internalpb.ImportJobState_Uncommitted && !current.GetCommitBroadcastStarted() {
						bulkimport.UpdateJobState(internalpb.ImportJobState_Failed)(current)
						bulkimport.UpdateJobReason(err.Error())(current)
					}
				}); updateErr != nil {
					return merr.Wrap(updateErr, "fail import with stale schema")
				}
				return err
			}
		}
		inspector := newSegmentChangeInspector(ctx, s.meta, nil)
		for _, g := range groups {
			if g.State == model.SegmentChangeStateStaged {
				inspector.evaluateStaged(ctx, g)
			}
			current := s.meta.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
			if current == nil {
				return merr.WrapErrServiceUnavailableMsg("import group %d disappeared before WAL commit", g.GroupID)
			}
			if current.State != model.SegmentChangeStateReady {
				return merr.WrapErrServiceUnavailableMsg("import group %d is not ready before WAL commit", g.GroupID)
			}
		}
	}
	return nil
}

func (s *Server) stageImportChangeFromSource(ctx context.Context, job bulkimport.ImportJob, channel string, ids []int64, commitTS uint64, source model.SegmentChangeSource) (bool, error) {
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
		if current != nil && current.State == model.SegmentChangeStateStaged && source == model.SegmentChangeSourceCDCReplicated {
			// A secondary did not run primary-side preparation. Evaluate it
			// now so ready groups do not wait for the periodic inspector tick.
			inspector.evaluateStaged(ctx, current)
			current = s.meta.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
		}
		if current != nil && current.State == model.SegmentChangeStateReady {
			inspector.tryPublish(ctx, current)
		}
		if err := s.meta.RequirePublishedChange(ctx, g); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (s *Server) stageImportGroups(ctx context.Context, job bulkimport.ImportJob, channel string, ids []int64, commitTS uint64, source model.SegmentChangeSource) ([]*model.SegmentChangeGroup, bool, error) {
	if s.meta == nil {
		return nil, false, nil
	}
	groups := s.meta.ProducerChangeGroups(source, job.GetJobID(), channel)
	if len(groups) == 0 && !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() && len(s.meta.ProducerChangeGroups(source, job.GetJobID(), "")) == 0 {
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
		if !claimed[id] && seg != nil && (meta.IsSegmentHealthy(seg) || meta.IsEmptySegmentChangeOutput(seg)) {
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
		g, _, err := s.meta.StageAdmittedChange(s.meta.Ctx, g, true)
		if err != nil {
			return nil, true, err
		}
		groups = append(groups, g)
		meta.NotifySegmentIndexBuild(batch...)
	}
	for i, g := range groups {
		if commitTS > 0 && g.CommitTS == 0 {
			g = g.Clone()
			g.CommitTS = commitTS
			if err := s.meta.UpdateSegmentChangeGroup(s.meta.Ctx, g); err != nil {
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
