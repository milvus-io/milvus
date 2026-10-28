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

func (m *Meta) ProducerChangeGroup(source model.SegmentChangeSource, jobID int64, channel string) *model.SegmentChangeGroup {
	m.SegMu.RLock()
	defer m.SegMu.RUnlock()
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
	m.SegMu.RLock()
	defer m.SegMu.RUnlock()
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
	m.SegMu.Lock()
	defer m.SegMu.Unlock()
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
