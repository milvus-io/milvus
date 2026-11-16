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
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func groupHasCommittedImportFence(g *model.SegmentChangeGroup) bool {
	return g.AwaitCommitFence && g.CommitTS > 0 && (g.Source == model.SegmentChangeSourceImportJob || g.Source == model.SegmentChangeSourceCDCReplicated)
}

func GroupHasCommittedImportFence(g *model.SegmentChangeGroup) bool {
	return groupHasCommittedImportFence(g)
}

func (m *Meta) GetProducerChangeGroup(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	return m.ProducerChangeGroup(source, jobID, channel)
}

func (m *Meta) RecomputeCompactionDataView(ctx context.Context, collectionID int64) {
	m.RecomputeDataView(ctx, collectionID)
}

func (m *Meta) CompactionContext() context.Context {
	if m.Ctx != nil {
		return m.Ctx
	}
	return context.TODO()
}

func (m *Meta) CompactionStorageRoot() string {
	return m.ChunkManager.RootPath()
}

func (m *Meta) ProducerChangeGroup(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	m.groupMu.RLock()
	defer m.groupMu.RUnlock()
	return m.producerChangeGroupLocked(source, jobID, channel).Clone()
}

func (m *Meta) producerChangeGroupLocked(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	for _, g := range m.segmentChangeGroups {
		if g.Source == source && g.SourceJobID == jobID && g.VChannel == channel {
			return g
		}
	}
	return nil
}

func (m *Meta) ProducerChangeGroups(source model.SegmentChangeSource, jobID int64, channel string) []*model.SegmentChangeGroup {
	m.groupMu.RLock()
	defer m.groupMu.RUnlock()
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
func (m *Meta) StageExistingChange(ctx context.Context, g *model.SegmentChangeGroup, operators ...UpdateOperator) (*model.SegmentChangeGroup, bool, error) {
	return m.StageAdmittedChange(ctx, g, false, operators...)
}

func (m *Meta) StageAdmittedChange(ctx context.Context, g *model.SegmentChangeGroup, admitted bool, operators ...UpdateOperator) (*model.SegmentChangeGroup, bool, error) {
	current := m.GetSegmentChangeGroup(ctx, 0, g.GroupID)
	if current != nil {
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
	actions := []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)}
	locks, pack, err := m.runOperators(append([]UpdateOperator{m.segmentChangeReferences(ctx, actions)}, operators...))
	defer locks.release()
	if err != nil {
		return nil, true, err
	}
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	// Another registration may have won while operators waited for a writer
	// lock. Recover its identity without overwriting its already-staged output.
	if current := m.segmentChangeGroups[g.GroupID]; current != nil {
		if current.CollectionID != g.CollectionID || current.Source != g.Source || current.SourceJobID != g.SourceJobID || current.VChannel != g.VChannel {
			return nil, true, merr.WrapErrDataIntegrityMsg("producer %d conflicts with its registered group", g.SourceJobID)
		}
		return current.Clone(), true, nil
	}
	if !admitted && !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() {
		return nil, false, nil
	}
	if err := m.applySegmentChangesLocked(ctx, nil, actions, nil, false, pack); err != nil {
		return nil, true, err
	}
	return g.Clone(), true, nil
}

func (m *Meta) RequirePublishedChange(ctx context.Context, g *model.SegmentChangeGroup) error {
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

func (m *Meta) AbortProducerChange(ctx context.Context, g *model.SegmentChangeGroup, reason string) error {
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
	return m.ReclaimSegmentChangeGroup(ctx, g)
}

// Failed import jobs must release staged groups before their task and job
// metadata can be collected. Both V2 and V3 use this idempotent path.
func (m *Meta) AbortFailedImportGroups(ctx context.Context, jobID int64) error {
	if m == nil {
		return nil
	}
	if m.FailedImportGroupsHaveCommitFence(jobID) {
		return merr.WrapErrServiceUnavailableMsg("cannot reclaim import job %d after its WAL commit fence", jobID)
	}
	for _, source := range []model.SegmentChangeSource{model.SegmentChangeSourceImportJob, model.SegmentChangeSourceCDCReplicated} {
		for _, group := range m.ProducerChangeGroups(source, jobID, "") {
			if err := m.AbortProducerChange(ctx, group, "import job failed before WAL commit"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Meta) FailedImportGroupsHaveCommitFence(jobID int64) bool {
	if m == nil {
		return false
	}
	for _, source := range []model.SegmentChangeSource{model.SegmentChangeSourceImportJob, model.SegmentChangeSourceCDCReplicated} {
		for _, group := range m.ProducerChangeGroups(source, jobID, "") {
			if groupHasCommittedImportFence(group) {
				return true
			}
		}
	}
	return false
}
