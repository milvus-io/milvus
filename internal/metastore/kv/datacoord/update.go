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
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Update applies a composite set of UpdateActions as a single write. Each
// action is dispatched on its (Entry, Type) into a kv encoding, accumulated
// into a txn.Builder, and committed via txn.Commit - atomically when the op
// count fits the store's txn op limit, else via the caller-ordered chunked
// fallback.
//
// The segment encoding is chosen by action type: an ActionAdd writes the
// segment record plus its binlog KVs (a new segment); an ActionUpdate writes
// the segment record, record-only by default, or via the legacy AlterSegments
// encoding (record + GC-compat binlog KVs for a dropped pre-prefix segment)
// when SegmentEntry.AlterEncoding is set. Entries this catalog does not own,
// or Type/Entry combinations it does not implement, are a caller programming
// error and are rejected with a ServiceInternal error and no write.
func (kc *Catalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	b, err := kc.EncodeUpdate(ctx, actions...)
	if err != nil {
		return err
	}
	if containsSegmentIndexUpdate(actions) {
		// A segment index removal and the segment manifest pointer that
		// publishes or retracts its artifact must land together. Refuse a
		// chunked fallback that could expose only half of the transition.
		return txn.CommitWithoutFallback(ctx, kc.MetaKv, b)
	}
	return txn.Commit(ctx, kc.MetaKv, b)
}

// EncodeUpdate encodes actions into the ops Update would commit, without
// committing them. DataCoord's optimistic segment store uses it to run the
// non-segment actions of a composite write inside its own versioned
// transaction; keys are relative to the MetaKv root path.
func (kc *Catalog) EncodeUpdate(ctx context.Context, actions ...metastore.UpdateAction) (*txn.Builder, error) {
	b := txn.New()
	for _, action := range actions {
		switch e := action.Entry.(type) {
		case metastore.SegmentEntry:
			if err := kc.applySegmentEntry(ctx, b, action.Type, e, publishesSegmentVersion(e.Segment, actions)); err != nil {
				return nil, err
			}
		case metastore.SegmentIndexEntry:
			if e.SegmentIndex == nil {
				return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil segment index in UpdateAction")
			}
			key := BuildSegmentIndexKey(
				e.SegmentIndex.CollectionID,
				e.SegmentIndex.PartitionID,
				e.SegmentIndex.SegmentID,
				e.SegmentIndex.BuildID,
			)
			switch action.Type {
			case metastore.ActionDelete:
				// Remove, not CommitRemove: an action set containing a segment
				// index entry never takes the ordered fallback path (see
				// containsSegmentIndexUpdate below), so there is no visibility
				// point to mark.
				b.Remove(key)
			default:
				return nil, unsupportedAction(action)
			}
		case metastore.ChannelEntry:
			if action.Type != metastore.ActionUpdate {
				return nil, unsupportedAction(action)
			}
			// CommitSave marks the tombstone as the visibility point, so any
			// earlier ops in this composite write land before it on the
			// ordered fallback path.
			b.CommitSave(buildChannelRemovePath(e.Channel), RemoveFlagTomestone)
		case metastore.RefreshTaskEntry:
			switch action.Type {
			case metastore.ActionAdd:
				// Same encoding as catalog.SaveExternalCollectionRefreshTask.
				if e.Task == nil {
					return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil refresh task in UpdateAction")
				}
				value, err := proto.Marshal(e.Task)
				if err != nil {
					return nil, err
				}
				b.Save(buildExternalCollectionRefreshTaskKey(e.Task.GetTaskId()), string(value))
			case metastore.ActionDelete:
				b.Remove(buildExternalCollectionRefreshTaskKey(e.TaskID))
			default:
				return nil, unsupportedAction(action)
			}
		case metastore.RefreshJobEntry:
			switch action.Type {
			case metastore.ActionUpdate:
				// Same encoding as catalog.SaveExternalCollectionRefreshJob.
				// CommitSave marks the job save as the visibility point: the
				// job is the failover anchor for its tasks, so every
				// RefreshTaskEntry save above must land before it on the
				// ordered fallback path.
				if e.Job == nil {
					return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil refresh job in UpdateAction")
				}
				value, err := proto.Marshal(e.Job)
				if err != nil {
					return nil, err
				}
				b.CommitSave(buildExternalCollectionRefreshJobKey(e.Job.GetJobId()), string(value))
			case metastore.ActionDelete:
				// CommitRemove marks the job removal as the visibility point:
				// the job is the failover anchor for its tasks, so every
				// RefreshTaskEntry removal above must land before it on the
				// ordered fallback path.
				b.CommitRemove(buildExternalCollectionRefreshJobKey(e.JobID))
			default:
				return nil, unsupportedAction(action)
			}
		case metastore.AnalyzeTaskEntry:
			if action.Type != metastore.ActionDelete {
				return nil, unsupportedAction(action)
			}
			b.Remove(buildAnalyzeTaskKey(e.TaskID))
		case metastore.DataViewEntry:
			if action.Type != metastore.ActionAdd {
				return nil, unsupportedAction(action)
			}
			if e.DataView == nil || e.DataView.GetDataVersion() == nil {
				return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil DataView or DataVersion in UpdateAction")
			}
			key := buildDataViewVersionKey(
				e.DataView.GetCollectionId(),
				e.DataView.GetDataVersion().GetStreamingVersion(),
				e.DataView.GetDataVersion().GetCompactVersion(),
			)
			value, err := proto.Marshal(e.DataView)
			if err != nil {
				return nil, err
			}
			// The snapshot key is immutable per version; a new version always
			// writes a fresh key, so the write is idempotent-safe. CommitSave
			// marks the DataView key as the visibility point of the whole
			// composite write: in the over-limit fallback, every SegmentMeta op
			// recorded before it is flushed first and the DataView lands in the
			// final guarded txn, so a visible DataView implies its SegmentMeta
			// is committed.
			b.CommitSave(key, string(value))
		case metastore.PartitionStatsVersionEntry:
			if action.Type != metastore.ActionUpdate {
				return nil, unsupportedAction(action)
			}
			b.Save(buildCurrentPartitionStatsVersionPath(e.CollectionID, e.PartitionID, e.VChannel),
				strconv.FormatInt(e.Version, 10))
		case metastore.PartitionStatsEntry:
			if e.Info == nil {
				return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil partition stats info in UpdateAction")
			}
			switch action.Type {
			case metastore.ActionAdd:
				// Same encoding as catalog.SavePartitionStatsInfo: clone, then
				// buildPartitionStatsInfoKv. Paired with a trailing
				// SavePartitionStatsVersion this is always two ops, so it
				// always fits a single atomic txn - the chunked fallback (and
				// thus the commit-marker ordering) never comes into play; the
				// version pointer is still composed last as the logical marker.
				cloned := proto.Clone(e.Info).(*datapb.PartitionStatsInfo)
				k, v, err := buildPartitionStatsInfoKv(cloned)
				if err != nil {
					return nil, err
				}
				b.Save(k, v)
			case metastore.ActionDelete:
				// CommitRemove marks the partition-stats info removal as the
				// visibility point for a partition-stats-and-analyze-task
				// cleanup: the AnalyzeTaskEntry removal and any
				// PartitionStatsVersionEntry rollback above must land before it
				// on the ordered fallback path.
				b.CommitRemove(buildPartitionStatsInfoPath(e.Info))
			default:
				return nil, unsupportedAction(action)
			}
		case metastore.SegmentChangeGroupEntry:
			switch action.Type {
			case metastore.ActionUpdate:
				// Same encoding as catalog.SaveSegmentChangeGroup.
				if e.Group == nil {
					return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil segment change group in UpdateAction")
				}
				value, err := model.MarshalSegmentChangeGroup(e.Group)
				if err != nil {
					return nil, err
				}
				if e.Group.IsTerminal() {
					// Terminal states (COMMITTED/FAILED/ABORTED) are the
					// visibility marker of the composite write: CommitSave lands
					// the record LAST in the chunked-fallback flush, so a visible
					// terminal record implies every non-commit op (member flips,
					// superseded retirement) already landed.
					b.CommitSave(buildSegmentChangeGroupKey(e.Group.CollectionID, e.Group.GroupID), string(value))
				} else {
					// ALIVE states (STAGED/READY) are NOT a commit marker (C13):
					// for STAGED creation the group record must land BEFORE its
					// staged members in the fallback flush, otherwise a crash
					// between the two leaves invisible members with no group
					// record — unrecoverable without SegmentInfo.change_group_id.
					// Plain in-order Save lets the caller control that ordering.
					b.Save(buildSegmentChangeGroupKey(e.Group.CollectionID, e.Group.GroupID), string(value))
				}
			case metastore.ActionDelete:
				// CommitRemove marks the group removal as the visibility point:
				// the group must land last when its members/superseded
				// retirement are composed before it on the ordered fallback path.
				b.CommitRemove(buildSegmentChangeGroupKey(e.CollectionID, e.GroupID))
			default:
				return nil, unsupportedAction(action)
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply entry %T", action.Entry)
		}
	}
	return b, nil
}

func containsSegmentIndexUpdate(actions []metastore.UpdateAction) bool {
	for _, action := range actions {
		if _, ok := action.Entry.(metastore.SegmentIndexEntry); ok {
			return true
		}
	}
	return false
}

// publishesSegmentVersion identifies a first Flush publication. Later compaction
// views must not pull all previously published segment records into the final txn.
func publishesSegmentVersion(segment *datapb.SegmentInfo, actions []metastore.UpdateAction) bool {
	if segment.GetSealedAtDataVersion() == nil {
		return false
	}
	for _, action := range actions {
		if entry, ok := action.Entry.(metastore.DataViewEntry); ok &&
			entry.DataView.GetCollectionId() == segment.GetCollectionID() &&
			proto.Equal(entry.DataView.GetDataVersion(), segment.GetSealedAtDataVersion()) {
			return true
		}
	}
	return false
}

// applySegmentEntry stages the kv writes for a segment action (see
// SegmentCodec.EncodeAction). The binlog KVs come before the record, and a
// first-publication record is the commit op: a durable binding must never
// precede its DataView, so just the record (not its potentially large binlog
// set) joins the final atomic transaction on the chunked path.
func (kc *Catalog) applySegmentEntry(ctx context.Context, b *txn.Builder, t metastore.ActionType, e metastore.SegmentEntry, publishesVersion bool) error {
	w, err := kc.segmentCodec().EncodeAction(ctx, t, e)
	if err != nil {
		return err
	}
	for _, op := range w.Ops {
		if op.Delete {
			b.Remove(op.Key)
		} else {
			b.Save(op.Key, op.Value)
		}
	}
	key, value, err := buildSegmentKv(w.Record)
	if err != nil {
		return err
	}
	if publishesVersion {
		b.CommitSave(key, value)
	} else {
		b.Save(key, value)
	}
	return nil
}

func unsupportedAction(action metastore.UpdateAction) error {
	return merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply action type %v to entry %T", action.Type, action.Entry)
}
