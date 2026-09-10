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

package metastore

import (
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// ActionType classifies the intent of an UpdateAction.
type ActionType int

const (
	// ActionAdd creates a new entry; the whole object is persisted.
	ActionAdd ActionType = iota + 1
	// ActionUpdate changes an existing entry (as opposed to creating or
	// physically removing one - e.g. marking a segment Dropped flips a field,
	// it does not remove keys).
	//
	// Current semantics are full-value replace/upsert: the implementation
	// re-encodes the whole record from the supplied object, so the caller must
	// pass the complete already-mutated value. This is always safe today
	// because every caller holds the authoritative in-memory object under the
	// single-writer meta lock and supplies it whole.
	//
	// ActionUpdate is a placeholder for a future partial-update (mutator) API:
	// a transactional read-modify-write that applies a func(current) to a clone
	// at the apply site, so a caller could touch only the fields it changes
	// (see the SegmentEntry note on the deferred mutator field). Until such a
	// caller exists, treat ActionUpdate as replace, not patch.
	ActionUpdate
	// ActionDelete physically removes an entry's keys from the store (e.g.
	// collection drop; reserved for future segment GC).
	ActionDelete
)

// Entry is a primitive metadata model targeted by an UpdateAction. It is a
// pure data-model reference - it carries no action verb. Sealed to this
// package via the unexported isEntry marker.
type Entry interface {
	isEntry()
}

// SegmentEntry targets a single segment.
//
// For ActionAdd, Segment is the full new segment: its record and its binlog
// KVs are persisted. For ActionUpdate, Segment is the segment's new value and
// only its record is rewritten; AlterEncoding selects which legacy encoding
// the record rewrite reproduces (see below).
//
// Under the single-writer meta lock the caller holds the authoritative
// in-memory value and supplies the already-mutated segment directly, so no
// mutator callback is needed. If we ever want a transactional
// read-modify-write - a remote catalog that reads the current value and
// applies the change server-side, or an in-process mutation that must observe
// the latest persisted value - SegmentEntry will need a mutator field
// (func(*datapb.SegmentInfo)) applied to a clone at the apply site. It is
// intentionally omitted until such a caller exists.
type SegmentEntry struct {
	Segment *datapb.SegmentInfo
	// AlterEncoding selects the legacy AlterSegments key/value encoding for an
	// ActionUpdate instead of the record-only SaveDroppedSegmentsInBatch
	// encoding. It matters only for a Dropped segment: AlterSegments also
	// writes the per-field binlog KVs that GC needs when the segment predates
	// binlog-prefix persistence (the handleDroppedSegment compat write).
	// Compaction sets it (via AlterSegment) to retire its compactFrom inputs
	// byte-identically to the legacy catalog.AlterSegments path; channel drop
	// leaves it false (via UpdateSegment) so an unbounded batch of dropped
	// segments avoids a per-segment prefix-existence read.
	AlterEncoding bool
}

// ChannelEntry targets a channel's removal tombstone.
type ChannelEntry struct {
	Channel string
}

// CollectionEntry targets a collection and its children.
type CollectionEntry struct {
	Collection *model.Collection
}

// RefreshTaskEntry targets a single external-collection-refresh task. For an
// ActionAdd, Task is the full task and its record is persisted; for an
// ActionDelete, TaskID identifies the task record to remove.
type RefreshTaskEntry struct {
	Task   *datapb.ExternalCollectionRefreshTask
	TaskID int64
}

// RefreshJobEntry targets an external-collection-refresh job. The job is the
// failover anchor for its tasks: a SaveRefreshJob action must be composed
// after every AddRefreshTask action for the job (so a persisted job records
// only persisted tasks), and a DropRefreshJob action after every
// DropRefreshTask action, so the job lands last either way. For an
// ActionUpdate, Job is the full job and its record is persisted; for an
// ActionDelete, JobID identifies the job record to remove.
type RefreshJobEntry struct {
	Job   *datapb.ExternalCollectionRefreshJob
	JobID int64
}

// AnalyzeTaskEntry targets a single analyze task's removal.
type AnalyzeTaskEntry struct {
	TaskID int64
}

// PartitionStatsEntry targets a partition-stats info. For an ActionAdd, Info
// is persisted as a new stats record (compose it before the
// SavePartitionStatsVersion that repoints the current version to it). For an
// ActionDelete, Info's record is removed; that removal is the commit marker
// for a partition-stats-and-analyze-task cleanup and must be composed last,
// after the analyze task removal and any current-version rollback for the
// same drop.
type PartitionStatsEntry struct {
	Info *datapb.PartitionStatsInfo
}

// PartitionStatsVersionEntry targets a channel-partition's current
// partition-stats version pointer.
type PartitionStatsVersionEntry struct {
	CollectionID int64
	PartitionID  int64
	VChannel     string
	Version      int64
}

// ReplicaEntry targets a single replica's upsert.
type ReplicaEntry struct {
	Replica *querypb.Replica
}

// ReplicaKeyEntry targets a single replica's kv record for removal, by the
// same (collectionID, replicaID) key SaveReplica/ReleaseReplica encode.
type ReplicaKeyEntry struct {
	CollectionID int64
	ReplicaID    int64
}

func (SegmentEntry) isEntry()               {}
func (ChannelEntry) isEntry()               {}
func (CollectionEntry) isEntry()            {}
func (RefreshTaskEntry) isEntry()           {}
func (RefreshJobEntry) isEntry()            {}
func (AnalyzeTaskEntry) isEntry()           {}
func (PartitionStatsEntry) isEntry()        {}
func (PartitionStatsVersionEntry) isEntry() {}
func (ReplicaEntry) isEntry()               {}
func (ReplicaKeyEntry) isEntry()            {}

// UpdateAction is a single composable write against a metastore catalog,
// applied via that catalog's composite Update. Type and Entry together
// determine the kv encoding a catalog applies; a catalog that does not
// recognize an Entry kind (or a Type/Entry combination) for its own metadata
// domain rejects the action.
type UpdateAction struct {
	Type  ActionType
	Entry Entry
}

// AddSegment returns an UpdateAction that persists seg as a new segment
// (record plus its binlog KVs).
func AddSegment(seg *datapb.SegmentInfo) UpdateAction {
	return UpdateAction{Type: ActionAdd, Entry: SegmentEntry{Segment: seg}}
}

// UpdateSegment returns an UpdateAction that persists a change to an existing
// segment's record using the record-only SaveDroppedSegmentsInBatch encoding.
// seg is the new value; the caller sets the desired fields (e.g. State =
// Dropped) before calling. Only the segment record is rewritten - binlog KVs
// are untouched, and no prefix-existence read is issued (safe for an unbounded
// batch, e.g. dropping every segment on a channel). See SegmentEntry for why
// there is no mutator callback. Use AlterSegment instead when the legacy
// AlterSegments GC-compat behavior is required.
func UpdateSegment(seg *datapb.SegmentInfo) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: SegmentEntry{Segment: seg}}
}

// AlterSegment returns an UpdateAction that rewrites an existing segment's
// record using the legacy AlterSegments encoding. For a Dropped segment it
// additionally persists the GC-compat binlog KVs a pre-binlog-prefix segment
// needs (the handleDroppedSegment compat write), so it stays byte-identical to
// catalog.AlterSegments. Compaction uses it to retire its compactFrom inputs;
// prefer UpdateSegment for the record-only batch-drop case, which avoids the
// per-segment prefix-existence read this path performs.
func AlterSegment(seg *datapb.SegmentInfo) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: SegmentEntry{Segment: seg, AlterEncoding: true}}
}

// MarkChannelDropped returns an UpdateAction that marks channel as removed.
func MarkChannelDropped(channel string) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: ChannelEntry{Channel: channel}}
}

// CreateCollection returns an UpdateAction that creates coll.
func CreateCollection(coll *model.Collection) UpdateAction {
	return UpdateAction{Type: ActionAdd, Entry: CollectionEntry{Collection: coll}}
}

// DropCollection returns an UpdateAction that drops coll.
func DropCollection(coll *model.Collection) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: CollectionEntry{Collection: coll}}
}

// AddRefreshTask returns an UpdateAction that persists task as a new external-
// collection-refresh task record.
func AddRefreshTask(task *datapb.ExternalCollectionRefreshTask) UpdateAction {
	return UpdateAction{Type: ActionAdd, Entry: RefreshTaskEntry{Task: task}}
}

// SaveRefreshJob returns an UpdateAction that persists job's record (an
// upsert). Compose it after every AddRefreshTask action for the job, so the
// job - the failover anchor - is written last as the commit marker.
func SaveRefreshJob(job *datapb.ExternalCollectionRefreshJob) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: RefreshJobEntry{Job: job}}
}

// DropRefreshTask returns an UpdateAction that removes an external-collection
// -refresh task.
func DropRefreshTask(taskID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: RefreshTaskEntry{TaskID: taskID}}
}

// DropRefreshJob returns an UpdateAction that removes an external-collection
// -refresh job. Compose it after every DropRefreshTask action for the job, so
// the job - the failover anchor - is removed last.
func DropRefreshJob(jobID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: RefreshJobEntry{JobID: jobID}}
}

// DropAnalyzeTask returns an UpdateAction that removes an analyze task.
func DropAnalyzeTask(taskID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: AnalyzeTaskEntry{TaskID: taskID}}
}

// AddPartitionStats returns an UpdateAction that persists info as a new
// partition-stats info record. Compose it before the SavePartitionStatsVersion
// action that repoints the current version to it, so the version pointer - the
// commit marker of a clustering-compaction completion - is written last.
func AddPartitionStats(info *datapb.PartitionStatsInfo) UpdateAction {
	return UpdateAction{Type: ActionAdd, Entry: PartitionStatsEntry{Info: info}}
}

// DropPartitionStats returns an UpdateAction that removes a partition-stats
// info. Compose it last in a partition-stats cleanup, after DropAnalyzeTask
// and any SavePartitionStatsVersion rollback for the same drop, so it serves
// as the commit marker.
func DropPartitionStats(info *datapb.PartitionStatsInfo) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: PartitionStatsEntry{Info: info}}
}

// SavePartitionStatsVersion returns an UpdateAction that repoints a
// channel-partition's current partition-stats version.
func SavePartitionStatsVersion(collectionID, partitionID int64, vchannel string, version int64) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: PartitionStatsVersionEntry{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		VChannel:     vchannel,
		Version:      version,
	}}
}

// SaveReplica returns an UpdateAction that persists r as a replica upsert.
func SaveReplica(r *querypb.Replica) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: ReplicaEntry{Replica: r}}
}

// ReleaseReplica returns an UpdateAction that removes a replica's kv record.
func ReleaseReplica(collectionID, replicaID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: ReplicaKeyEntry{CollectionID: collectionID, ReplicaID: replicaID}}
}
