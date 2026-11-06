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
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file implements the READY -> COMMITTED publication for 3.0, which has
// no DataView subsystem. Member visibility, parent retirement and the terminal
// group record use the existing SegmentMeta composite write. The inspector
// owns the state machine; publishGroup reports failures without transitioning.

// groupUsesPublishCommitTS reports whether the group's source allocates a
// publish commit timestamp (design §5.4, C5): import / CDC / copy rows carry a
// data timestamp allocated at creation and become "officially present" at
// commit_ts. Compaction outputs (mix/sort/clustering/forcemerge) MUST keep
// CommitTimestamp=0 — their rows keep per-input normalized timestamps, and a
// non-zero commit_ts makes segcore treat every row as inserted at commit_ts,
// dropping any delete with a timestamp <= commit_ts (MVCC/TTL shift).
func groupUsesPublishCommitTS(source model.SegmentChangeSource) bool {
	switch source {
	case model.SegmentChangeSourceImportJob,
		model.SegmentChangeSourceCopySegment,
		model.SegmentChangeSourceCDCReplicated:
		return true
	default:
		return false
	}
}

// publishGroup publishes a READY group through the SegmentMeta/group composite
// write. The catalog uses one transaction when the actions fit its operation
// limit, otherwise its existing ordered fallback. Retrying an interrupted READY
// publication is idempotent, but crash reconciliation before serving legacy
// views remains a prerequisite for producer admission on 3.0.
//
// Validation takes segMu.RLock; the composite write later takes segMu.Lock and
// builds operators from current segment records. There is no cached DataView
// manifest projection to protect with master's segmentManifestLocks framework.
// The caller owns retries and terminal transitions after a failed publish.
func (m *meta) publishGroup(ctx context.Context, g *model.SegmentChangeGroup, alloc allocator.Allocator, extraOperators ...UpdateOperator) error {
	if g == nil {
		return merr.WrapErrServiceInternalMsg("cannot publish a nil segment change group")
	}
	if g.State != model.SegmentChangeStateReady {
		return merr.WrapErrServiceInternalMsg(
			"segment change group %d cannot be published in state %s (requires READY)", g.GroupID, g.State)
	}

	// Phase -1: allocate commit_ts before holding any lock, so the rootcoord TSO
	// RTT never sits on a lock. TSO only increases; a failure may retry with a
	// fresh value.
	commitTs, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return err
	}

	// R5: re-validate readiness at publish time (design §5.4.1 / §5.5). The
	// inspector may have marked the group READY up to a tick ago; in the window
	// a member's index state can regress (rebuild retry / index DDL) or a
	// schema bump can land, and publishing would expose an indexed-out or
	// schema-behind member while its superseded parents are retired — data
	// temporarily vanishing from queries. A failed re-check returns an error
	// and the caller regresses the group to STAGED for re-evaluation.
	coll := m.GetCollection(g.CollectionID)
	readiness := m.prepareReadiness(coll)

	// Validate the members under segMu.RLock. Ownership is enforced through
	// the reverse index because SegmentInfo has no change_group_id field yet.
	m.segMu.RLock()
	for _, id := range g.NewSegmentIDs {
		seg := m.segments.GetSegment(id)
		if seg == nil {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d is missing from meta (externally dropped?)", g.GroupID, id)
		}
		if seg.GetState() == commonpb.SegmentState_Dropped {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d was externally dropped", g.GroupID, id)
		}
		if owner, ok := m.stagedSegmentToGroup[id]; !ok || owner != g.GroupID {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d is not staged by this group", g.GroupID, id)
		}
		if len(seg.GetBinlogs()) == 0 && seg.GetManifestPath() == "" {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d has no data footprint", g.GroupID, id)
		}
		if readiness != nil {
			if !readiness.schemaReady(seg) {
				m.segMu.RUnlock()
				return merr.WrapErrDataIntegrityMsg(
					"segment change group %d publish: member %d schema version behind collection at publish (C23)", g.GroupID, id)
			}
			if !readiness.indexReady(seg) {
				m.segMu.RUnlock()
				return merr.WrapErrDataIntegrityMsg(
					"segment change group %d publish: member %d index state regressed after READY (R5)", g.GroupID, id)
			}
		}
		if seg.GetManifestPath() != "" {
			if _, _, err := packed.UnmarshalManifestPath(seg.GetManifestPath()); err != nil {
				m.segMu.RUnlock()
				return merr.WrapErrStorage(err, "failed to parse Manifest path for Segment %d", seg.GetID())
			}
		}
	}
	m.segMu.RUnlock()

	// The composite write orders members before parents and the terminal
	// group marker, and verifies every member flip and parent retirement.
	operators := make([]UpdateOperator, 0, len(g.NewSegmentIDs)*2+len(g.SupersededSegmentIDs)+len(extraOperators))
	for _, id := range g.NewSegmentIDs {
		operators = append(operators, SetSegmentIsInvisible(id, false))
		if groupUsesPublishCommitTS(g.Source) {
			operators = append(operators, UpdateCommitTimestamp(id, commitTs))
		}
	}
	for _, id := range g.SupersededSegmentIDs {
		operators = append(operators, UpdateStatusOperator(id, commonpb.SegmentState_Dropped))
	}
	operators = append(operators, extraOperators...)

	committed := g.Clone()
	committed.State = model.SegmentChangeStateCommitted
	committed.CommitTS = commitTs
	committed.CommitTime = time.Now().Unix()
	groupActions := []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(committed)}

	if err := m.UpdateSegmentsInfoAndChangeGroups(ctx, groupActions, operators...); err != nil {
		return err
	}
	mlog.Info(ctx, "segment change group published",
		mlog.Int64("groupID", g.GroupID),
		mlog.FieldCollectionID(g.CollectionID),
		mlog.String("source", g.Source.String()),
		mlog.Int64s("members", g.NewSegmentIDs),
		mlog.Uint64("commitTS", commitTs))
	return nil
}
