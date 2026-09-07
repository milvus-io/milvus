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
	b := txn.New()
	for _, action := range actions {
		switch e := action.Entry.(type) {
		case metastore.SegmentEntry:
			if err := kc.applySegmentEntry(ctx, b, action.Type, e); err != nil {
				return err
			}
		case metastore.ChannelEntry:
			if action.Type != metastore.ActionUpdate {
				return unsupportedAction(action)
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
					return merr.WrapErrServiceInternalMsg("datacoord catalog: nil refresh task in UpdateAction")
				}
				value, err := proto.Marshal(e.Task)
				if err != nil {
					return err
				}
				b.Save(buildExternalCollectionRefreshTaskKey(e.Task.GetTaskId()), string(value))
			case metastore.ActionDelete:
				b.Remove(buildExternalCollectionRefreshTaskKey(e.TaskID))
			default:
				return unsupportedAction(action)
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
					return merr.WrapErrServiceInternalMsg("datacoord catalog: nil refresh job in UpdateAction")
				}
				value, err := proto.Marshal(e.Job)
				if err != nil {
					return err
				}
				b.CommitSave(buildExternalCollectionRefreshJobKey(e.Job.GetJobId()), string(value))
			case metastore.ActionDelete:
				// CommitRemove marks the job removal as the visibility point:
				// the job is the failover anchor for its tasks, so every
				// RefreshTaskEntry removal above must land before it on the
				// ordered fallback path.
				b.CommitRemove(buildExternalCollectionRefreshJobKey(e.JobID))
			default:
				return unsupportedAction(action)
			}
		case metastore.AnalyzeTaskEntry:
			if action.Type != metastore.ActionDelete {
				return unsupportedAction(action)
			}
			b.Remove(buildAnalyzeTaskKey(e.TaskID))
		case metastore.PartitionStatsVersionEntry:
			if action.Type != metastore.ActionUpdate {
				return unsupportedAction(action)
			}
			b.Save(buildCurrentPartitionStatsVersionPath(e.CollectionID, e.PartitionID, e.VChannel),
				strconv.FormatInt(e.Version, 10))
		case metastore.PartitionStatsEntry:
			if e.Info == nil {
				return merr.WrapErrServiceInternalMsg("datacoord catalog: nil partition stats info in UpdateAction")
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
					return err
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
				return unsupportedAction(action)
			}
		default:
			return merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply entry %T", action.Entry)
		}
	}
	return txn.Commit(ctx, kc.MetaKv, b)
}

// applySegmentEntry stages the kv writes for a segment action.
//   - ActionAdd    -> segment record + its binlog KVs (a new segment).
//   - ActionUpdate -> segment record rewrite; the caller supplies the
//     already-mutated segment value. AlterEncoding selects the encoding:
//     false -> record-only (SaveDroppedSegmentsInBatch), true -> the legacy
//     AlterSegments encoding, which for a Dropped segment also writes the
//     handleDroppedSegment GC-compat binlog KVs.
//   - anything else (e.g. ActionDelete: physical segment removal) is not
//     wired yet and is rejected.
func (kc *Catalog) applySegmentEntry(ctx context.Context, b *txn.Builder, t metastore.ActionType, e metastore.SegmentEntry) error {
	if e.Segment == nil {
		return merr.WrapErrServiceInternalMsg("datacoord catalog: nil segment in UpdateAction")
	}
	switch t {
	case metastore.ActionAdd:
		kvs, removals, err := kc.buildAlterSegmentsKvs(ctx,
			[]*datapb.SegmentInfo{e.Segment},
			[]metastore.BinlogsIncrement{{Segment: e.Segment}})
		if err != nil {
			return err
		}
		for k, v := range kvs {
			b.Save(k, v)
		}
		for _, k := range removals {
			b.Remove(k)
		}
	case metastore.ActionUpdate:
		if e.AlterEncoding {
			// Legacy AlterSegments encoding. A nil binlog increment persists no
			// new binlog KVs (the segment is being retired, not extended); for a
			// Dropped segment buildAlterSegmentsKvs still emits the
			// handleDroppedSegment GC-compat write when the segment predates
			// binlog-prefix persistence, keeping compaction's compactFrom
			// retirement byte-identical to catalog.AlterSegments.
			kvs, removals, err := kc.buildAlterSegmentsKvs(ctx, []*datapb.SegmentInfo{e.Segment}, nil)
			if err != nil {
				return err
			}
			for k, v := range kvs {
				b.Save(k, v)
			}
			for _, k := range removals {
				b.Remove(k)
			}
			return nil
		}
		kvs, err := buildDroppedSegmentKvs([]*datapb.SegmentInfo{e.Segment})
		if err != nil {
			return err
		}
		for k, v := range kvs {
			b.Save(k, v)
		}
	default:
		return merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply action type %v to a segment", t)
	}
	return nil
}

func unsupportedAction(action metastore.UpdateAction) error {
	return merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply action type %v to entry %T", action.Type, action.Entry)
}
