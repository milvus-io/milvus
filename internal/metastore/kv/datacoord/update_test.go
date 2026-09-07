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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalog_Update_Atomic(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	c := NewCatalog(metakv, "", "")
	seg := &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 1, State: commonpb.SegmentState_Flushed}
	err := c.Update(context.TODO(),
		metastore.AddSegment(seg),
		metastore.MarkChannelDropped("ch-1"))
	assert.NoError(t, err)
}

func TestCatalog_Update_Empty(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")
	err := c.Update(context.TODO())
	assert.NoError(t, err)
}

// TestCatalog_Update_AddSegmentEncodingMatchesLegacy proves AddSegment writes
// the same kvs as the legacy AlterSegments (record + binlog KVs).
func TestCatalog_Update_AddSegmentEncodingMatchesLegacy(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	seg := &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 1, State: commonpb.SegmentState_Flushed}

	var legacySaves map[string]string
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		legacySaves = kvs
		return nil
	}).Once()
	c := NewCatalog(metakv, "", "")
	assert.NoError(t, c.AlterSegments(context.TODO(), []*datapb.SegmentInfo{seg}, metastore.BinlogsIncrement{Segment: seg}))

	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2, "", "")
	assert.NoError(t, c2.Update(context.TODO(), metastore.AddSegment(seg)))

	assert.Equal(t, legacySaves, compositeSaves)
}

// TestCatalog_Update_UpdateSegmentEncodingMatchesLegacy proves UpdateSegment
// (record-only) writes the same kvs as the legacy SaveDroppedSegmentsInBatch.
func TestCatalog_Update_UpdateSegmentEncodingMatchesLegacy(t *testing.T) {
	seg := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 1,
		PartitionID:  1,
		State:        commonpb.SegmentState_Dropped,
		Binlogs:      []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
	}

	var legacySaves map[string]string
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		legacySaves = kvs
		return nil
	}).Once()
	c := NewCatalog(metakv, "", "")
	assert.NoError(t, c.SaveDroppedSegmentsInBatch(context.TODO(), []*datapb.SegmentInfo{seg}))

	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2, "", "")
	assert.NoError(t, c2.Update(context.TODO(), metastore.UpdateSegment(seg)))

	assert.Equal(t, legacySaves, compositeSaves)
	// UpdateSegment writes the record only - no binlog KVs, unlike AddSegment.
	assert.Len(t, compositeSaves, 1)
}

// TestCatalog_Update_AlterSegmentEncodingMatchesLegacy proves AlterSegment
// (the compaction compactFrom path) writes the same kvs as the legacy
// AlterSegments - including the handleDroppedSegment GC-compat binlog write
// that fires for a dropped segment lacking binlog-prefix KVs (the pre-split
// inline-binlog format). This is the baseline the record-only UpdateSegment
// path does NOT match, which is why compaction uses AlterSegment.
func TestCatalog_Update_AlterSegmentEncodingMatchesLegacy(t *testing.T) {
	seg := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 1,
		PartitionID:  1,
		State:        commonpb.SegmentState_Dropped,
		Binlogs:      []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
	}

	// No binlog-prefix KVs exist, so handleDroppedSegment writes the GC-compat
	// binlog KVs from the inline Binlogs on both paths.
	var legacySaves map[string]string
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().HasPrefix(mock.Anything, mock.Anything).Return(false, nil)
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		legacySaves = kvs
		return nil
	}).Once()
	c := NewCatalog(metakv, "", "")
	assert.NoError(t, c.AlterSegments(context.TODO(), []*datapb.SegmentInfo{seg}))

	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().HasPrefix(mock.Anything, mock.Anything).Return(false, nil)
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2, "", "")
	assert.NoError(t, c2.Update(context.TODO(), metastore.AlterSegment(seg)))

	assert.Equal(t, legacySaves, compositeSaves)
	// record KV + at least one GC-compat binlog KV - proves the compat write is
	// preserved, unlike the record-only UpdateSegment path (which writes 1 KV).
	assert.Greater(t, len(compositeSaves), 1)
}

// TestCatalog_Update_SegmentRecordPersistedAsIs proves UpdateSegment persists
// the caller-supplied segment record as-is: the caller sets the desired state
// (e.g. Dropped) before calling, and the catalog performs no mutation of its
// own.
func TestCatalog_Update_SegmentRecordPersistedAsIs(t *testing.T) {
	seg := &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 1, State: commonpb.SegmentState_Dropped}

	var saved map[string]string
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, _ []string, _ ...predicates.Predicate) error {
			saved = saves
			return nil
		}).Once()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(), metastore.UpdateSegment(seg))
	assert.NoError(t, err)

	// The persisted record reflects the caller-supplied state.
	key := buildSegmentPath(seg.CollectionID, seg.PartitionID, seg.ID)
	persisted := &datapb.SegmentInfo{}
	assert.NoError(t, proto.Unmarshal([]byte(saved[key]), persisted))
	assert.Equal(t, commonpb.SegmentState_Dropped, persisted.GetState())
}

// TestCatalog_Update_RejectsForeignEntry proves the datacoord catalog's
// Update rejects an entry it does not own (CollectionEntry belongs to the
// rootcoord catalog) with a merr ServiceInternal error (a programming bug,
// not user input), and issues no KV call.
func TestCatalog_Update_RejectsForeignEntry(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")
	err := c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionAdd,
		Entry: metastore.CollectionEntry{},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_RejectsUnsupportedType proves the datacoord catalog's
// Update rejects a segment entry paired with an action type it does not
// implement (ActionDelete: physical segment removal, not wired) with a merr
// ServiceInternal error and no KV call (metakv has no EXPECT, so any KV call
// would panic).
func TestCatalog_Update_RejectsUnsupportedType(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")
	seg := &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 1, State: commonpb.SegmentState_Flushed}
	err := c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionDelete,
		Entry: metastore.SegmentEntry{Segment: seg},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_DropRefreshJobAndTasks proves DropRefreshTask actions
// remove the task keys and a trailing DropRefreshJob action removes the job
// key, with the job landing last among the removals.
func TestCatalog_Update_DropRefreshJobAndTasks(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var removals []string
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, dels []string, _ ...predicates.Predicate) error {
			assert.Empty(t, saves)
			removals = dels
			return nil
		}).Once()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(),
		metastore.DropRefreshTask(1001),
		metastore.DropRefreshTask(1002),
		metastore.DropRefreshJob(1))
	assert.NoError(t, err)

	assert.Equal(t, []string{
		buildExternalCollectionRefreshTaskKey(1001),
		buildExternalCollectionRefreshTaskKey(1002),
		buildExternalCollectionRefreshJobKey(1),
	}, removals)
}

// TestCatalog_Update_RefreshEntries_RejectsUnsupportedType proves a
// RefreshTaskEntry/RefreshJobEntry paired with an action type it does not
// implement is rejected, with no KV call. A RefreshTaskEntry supports
// ActionAdd (save) and ActionDelete (remove); a RefreshJobEntry supports
// ActionUpdate (save) and ActionDelete (remove).
func TestCatalog_Update_RefreshEntries_RejectsUnsupportedType(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.RefreshTaskEntry{TaskID: 1}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.RefreshJobEntry{JobID: 1}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_AddRefreshTask_RejectsNilTask proves a RefreshTaskEntry
// ActionAdd with a nil Task is rejected with no KV call.
func TestCatalog_Update_AddRefreshTask_RejectsNilTask(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")
	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.RefreshTaskEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_SaveRefreshJob_RejectsNilJob proves a RefreshJobEntry
// ActionUpdate with a nil Job is rejected with no KV call.
func TestCatalog_Update_SaveRefreshJob_RejectsNilJob(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")
	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.RefreshJobEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_AddRefreshTasksAndSaveJobEncodingMatchesLegacy proves the
// composite create-side write (AddRefreshTask x N + SaveRefreshJob) persists
// byte-identical kvs to the legacy per-object catalog methods
// (SaveExternalCollectionRefreshTask + SaveExternalCollectionRefreshJob), and
// that the job save lands last among the saves (the commit marker).
func TestCatalog_Update_AddRefreshTasksAndSaveJobEncodingMatchesLegacy(t *testing.T) {
	task1 := &datapb.ExternalCollectionRefreshTask{TaskId: 1001, JobId: 7, CollectionId: 3}
	task2 := &datapb.ExternalCollectionRefreshTask{TaskId: 1002, JobId: 7, CollectionId: 3}
	job := &datapb.ExternalCollectionRefreshJob{JobId: 7, CollectionId: 3, TaskIds: []int64{1001, 1002}}

	// Legacy: three independent Saves.
	legacySaves := make(map[string]string)
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, k string, v string) error {
		legacySaves[k] = v
		return nil
	}).Times(3)
	c := NewCatalog(metakv, "", "")
	assert.NoError(t, c.SaveExternalCollectionRefreshTask(context.TODO(), task1))
	assert.NoError(t, c.SaveExternalCollectionRefreshTask(context.TODO(), task2))
	assert.NoError(t, c.SaveExternalCollectionRefreshJob(context.TODO(), job))

	// Composite: one MultiSave.
	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2, "", "")
	assert.NoError(t, c2.Update(context.TODO(),
		metastore.AddRefreshTask(task1),
		metastore.AddRefreshTask(task2),
		metastore.SaveRefreshJob(job)))

	assert.Equal(t, legacySaves, compositeSaves)
	assert.Len(t, compositeSaves, 3)
}

// TestCatalog_Update_DropPartitionStatsAndAnalyzeTask proves the composite
// partition-stats-and-analyze-task cleanup issues: a Remove for the analyze
// task, a Save for the current-partition-stats-version rollback (when
// present), and a Remove for the partition-stats info, with the
// partition-stats removal landing last.
func TestCatalog_Update_DropPartitionStatsAndAnalyzeTask(t *testing.T) {
	info := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 100}

	t.Run("with rollback", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
		var saves map[string]string
		var removals []string
		metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
				saves = s
				removals = dels
				return nil
			}).Once()
		c := NewCatalog(metakv, "", "")

		err := c.Update(context.TODO(),
			metastore.DropAnalyzeTask(55),
			metastore.SavePartitionStatsVersion(1, 2, "ch-1", 90),
			metastore.DropPartitionStats(info))
		assert.NoError(t, err)

		assert.Equal(t, map[string]string{
			buildCurrentPartitionStatsVersionPath(1, 2, "ch-1"): "90",
		}, saves)
		assert.Equal(t, []string{
			buildAnalyzeTaskKey(55),
			buildPartitionStatsInfoPath(info),
		}, removals)
	})

	t.Run("without rollback", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
		var saves map[string]string
		var removals []string
		metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
				saves = s
				removals = dels
				return nil
			}).Once()
		c := NewCatalog(metakv, "", "")

		err := c.Update(context.TODO(),
			metastore.DropAnalyzeTask(55),
			metastore.DropPartitionStats(info))
		assert.NoError(t, err)

		assert.Empty(t, saves)
		assert.Equal(t, []string{
			buildAnalyzeTaskKey(55),
			buildPartitionStatsInfoPath(info),
		}, removals)
	})
}

// TestCatalog_Update_DropPartitionStats_RejectsNilInfo proves a
// PartitionStatsEntry with a nil Info is rejected with no KV call.
func TestCatalog_Update_DropPartitionStats_RejectsNilInfo(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionDelete, Entry: metastore.PartitionStatsEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_AddPartitionStats_RejectsNilInfo proves a
// PartitionStatsEntry ActionAdd with a nil Info is rejected with no KV call.
func TestCatalog_Update_AddPartitionStats_RejectsNilInfo(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.PartitionStatsEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_AddPartitionStatsAndVersionEncodingMatchesLegacy proves
// the composite save-side write (AddPartitionStats + SavePartitionStatsVersion)
// persists byte-identical kvs to the legacy pair of catalog methods
// (the removed SavePartitionStatsInfo + SaveCurrentPartitionStatsVersion),
// reproduced here directly from the shared encoders.
func TestCatalog_Update_AddPartitionStatsAndVersionEncodingMatchesLegacy(t *testing.T) {
	info := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 100, SegmentIDs: []int64{5, 6}}

	// Legacy encoding: the partition-stats info kv (buildPartitionStatsInfoKv,
	// on a clone) plus the current-version pointer (formatted int at
	// buildCurrentPartitionStatsVersionPath).
	legacySaves := make(map[string]string)
	k, v, err := buildPartitionStatsInfoKv(proto.Clone(info).(*datapb.PartitionStatsInfo))
	assert.NoError(t, err)
	legacySaves[k] = v
	legacySaves[buildCurrentPartitionStatsVersionPath(1, 2, "ch-1")] = "100"

	// Composite: one MultiSave.
	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2, "", "")
	assert.NoError(t, c2.Update(context.TODO(),
		metastore.AddPartitionStats(info),
		metastore.SavePartitionStatsVersion(1, 2, "ch-1", 100)))

	assert.Equal(t, legacySaves, compositeSaves)
	assert.Equal(t, "100", compositeSaves[buildCurrentPartitionStatsVersionPath(1, 2, "ch-1")])
}
