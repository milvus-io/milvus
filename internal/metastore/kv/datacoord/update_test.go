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
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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

// ---------------------------------------------------------------------------
// Import job + task composite tests
// ---------------------------------------------------------------------------

// memMetaKv adapts the in-memory TxnKV to kv.MetaKv so crash-injection tests
// can replay a retry against real store state (the mocks.MetaKv harness above
// records calls but keeps no state). Only the read/write surface the import
// composite touches is implemented.
type memMetaKv struct {
	*memkv.MemoryKV
}

func (m *memMetaKv) GetPath(key string) string { return key }

func (m *memMetaKv) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	return false, errors.New("not implemented")
}

func (m *memMetaKv) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, vals, err := m.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i := range keys {
		if err := fn([]byte(keys[i]), []byte(vals[i])); err != nil {
			return err
		}
	}
	return nil
}

func dumpMetaKV(t *testing.T, k *memkv.MemoryKV) map[string]string {
	keys, vals, err := k.LoadWithPrefix(context.TODO(), "")
	assert.NoError(t, err)
	got := make(map[string]string, len(keys))
	for i, key := range keys {
		got[key] = vals[i]
	}
	return got
}

// TestCatalog_Update_AddImportTasksAndSaveJobEncodingMatchesLegacy proves the
// composite import-create write (AddPreImportTask/AddImportTask x N +
// SaveImportJob) persists byte-identical kvs to the legacy per-object catalog
// methods (SavePreImportTask + SaveImportTask + SaveImportJob), in a single
// txn.
func TestCatalog_Update_AddImportTasksAndSaveJobEncodingMatchesLegacy(t *testing.T) {
	preTask := &datapb.PreImportTask{JobID: 7, TaskID: 1001, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending}
	task := &datapb.ImportTaskV2{JobID: 7, TaskID: 1002, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending}
	job := &datapb.ImportJob{JobID: 7, CollectionID: 3, State: internalpb.ImportJobState_PreImporting}

	// Legacy: three independent Saves.
	legacySaves := make(map[string]string)
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, k string, v string) error {
		legacySaves[k] = v
		return nil
	}).Times(3)
	c := NewCatalog(metakv, "", "")
	assert.NoError(t, c.SavePreImportTask(context.TODO(), preTask))
	assert.NoError(t, c.SaveImportTask(context.TODO(), task))
	assert.NoError(t, c.SaveImportJob(context.TODO(), job))

	// Composite: one MultiSaveAndRemove.
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
		metastore.AddPreImportTask(preTask),
		metastore.AddImportTask(task),
		metastore.SaveImportJob(job)))

	assert.Equal(t, legacySaves, compositeSaves)
	assert.Len(t, compositeSaves, 3)
}

// TestCatalog_Update_ImportEntries_RejectsUnsupportedTypeAndNil proves an
// ImportTaskEntry/ImportJobEntry paired with an action type it does not
// implement, or carrying a nil payload, is rejected with no KV call. An
// ImportTaskEntry supports ActionAdd only; an ImportJobEntry supports
// ActionUpdate (upsert) only.
func TestCatalog_Update_ImportEntries_RejectsUnsupportedTypeAndNil(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv, "", "")

	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.ImportTaskEntry{Task: &datapb.ImportTaskV2{TaskID: 1}}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.ImportJobEntry{Job: &datapb.ImportJob{JobID: 1}}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.ImportTaskEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.ImportJobEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// importAtomicCrashKV fails the first MultiSaveAndRemove to simulate a crash
// of the atomic commit.
type importAtomicCrashKV struct {
	*memMetaKv
	failures int
}

func (f *importAtomicCrashKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected crash")
	}
	return f.memMetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// TestCatalog_Update_ImportJobAndTasks_AtomicCrashRetry: on the atomic path a
// failed commit must leave neither task records nor a job-state flip behind
// (the two key classes live and die together - no task orphans under a job
// that never left Pending, no PreImporting job without its tasks), and
// retrying the same composite write must converge to the legacy end state.
func TestCatalog_Update_ImportJobAndTasks_AtomicCrashRetry(t *testing.T) {
	ctx := context.TODO()
	pending := &datapb.ImportJob{JobID: 7, CollectionID: 3, State: internalpb.ImportJobState_Pending}
	preImporting := &datapb.ImportJob{JobID: 7, CollectionID: 3, State: internalpb.ImportJobState_PreImporting}
	tasks := []*datapb.PreImportTask{
		{JobID: 7, TaskID: 1001, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending},
		{JobID: 7, TaskID: 1002, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending},
	}

	// Legacy end state: job created, then its tasks and the job transition.
	legacyKV := &memMetaKv{MemoryKV: memkv.NewMemoryKV()}
	legacy := NewCatalog(legacyKV, "", "")
	assert.NoError(t, legacy.SaveImportJob(ctx, pending))
	for _, task := range tasks {
		assert.NoError(t, legacy.SavePreImportTask(ctx, task))
	}
	assert.NoError(t, legacy.SaveImportJob(ctx, preImporting))

	fk := &importAtomicCrashKV{memMetaKv: &memMetaKv{MemoryKV: memkv.NewMemoryKV()}, failures: 1}
	c := NewCatalog(fk, "", "")
	assert.NoError(t, c.SaveImportJob(ctx, pending))
	before := dumpMetaKV(t, fk.MemoryKV)

	save := func() error {
		return c.Update(ctx,
			metastore.AddPreImportTask(tasks[0]),
			metastore.AddPreImportTask(tasks[1]),
			metastore.SaveImportJob(preImporting))
	}
	assert.Error(t, save())
	// atomic: the failed commit applied nothing - no orphan task records, and
	// the job record still carries its pre-transition value.
	assert.Equal(t, before, dumpMetaKV(t, fk.MemoryKV))

	assert.NoError(t, save())
	assert.Equal(t, dumpMetaKV(t, legacyKV.MemoryKV), dumpMetaKV(t, fk.MemoryKV))
}

// importFallbackCommitCrashKV shrinks MaxTxnOps so the composite import write
// takes the chunked fallback, records which keys each MultiSave chunk
// carried, and fails the final guarded commit txn - simulating a crash after
// the task flush but before the job marker lands.
type importFallbackCommitCrashKV struct {
	*memMetaKv
	multiSaveKeys  [][]string
	commitFailures int
}

func (f *importFallbackCommitCrashKV) MaxTxnOps() int { return 2 }

func (f *importFallbackCommitCrashKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	f.multiSaveKeys = append(f.multiSaveKeys, keys)
	return f.memMetaKv.MultiSave(ctx, kvs)
}

func (f *importFallbackCommitCrashKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.commitFailures > 0 {
		f.commitFailures--
		return errors.New("injected crash")
	}
	return f.memMetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// TestCatalog_Update_ImportJobMarkerLandsLast_FallbackCrashRetry: on the
// chunked fallback path the job record is the sole visibility marker of the
// batch. A crash between the task flush and the final commit txn must leave
// the job record un-flipped (still Pending) - the flushed task records sit
// inert under a job that never observed them - and retrying the same
// composite write must converge to the legacy end state.
func TestCatalog_Update_ImportJobMarkerLandsLast_FallbackCrashRetry(t *testing.T) {
	ctx := context.TODO()
	pending := &datapb.ImportJob{JobID: 7, CollectionID: 3, State: internalpb.ImportJobState_Pending}
	preImporting := &datapb.ImportJob{JobID: 7, CollectionID: 3, State: internalpb.ImportJobState_PreImporting}
	tasks := []*datapb.PreImportTask{
		{JobID: 7, TaskID: 1001, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending},
		{JobID: 7, TaskID: 1002, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending},
		{JobID: 7, TaskID: 1003, CollectionID: 3, State: datapb.ImportTaskStateV2_Pending},
	}

	legacyKV := &memMetaKv{MemoryKV: memkv.NewMemoryKV()}
	legacy := NewCatalog(legacyKV, "", "")
	assert.NoError(t, legacy.SaveImportJob(ctx, pending))
	for _, task := range tasks {
		assert.NoError(t, legacy.SavePreImportTask(ctx, task))
	}
	assert.NoError(t, legacy.SaveImportJob(ctx, preImporting))

	fk := &importFallbackCommitCrashKV{memMetaKv: &memMetaKv{MemoryKV: memkv.NewMemoryKV()}, commitFailures: 1}
	c := NewCatalog(fk, "", "")
	assert.NoError(t, c.SaveImportJob(ctx, pending))
	pendingValue, err := fk.Load(ctx, buildImportJobKey(7))
	assert.NoError(t, err)

	save := func() error {
		return c.Update(ctx,
			metastore.AddPreImportTask(tasks[0]),
			metastore.AddPreImportTask(tasks[1]),
			metastore.AddPreImportTask(tasks[2]),
			metastore.SaveImportJob(preImporting))
	}
	// 4 ops against a 2-op txn limit: the task saves flush in chunks, the job
	// save must ride the final guarded commit txn - which is failed here.
	assert.Error(t, save())

	// The job marker never rode in a task chunk...
	assert.NotEmpty(t, fk.multiSaveKeys)
	for _, keys := range fk.multiSaveKeys {
		assert.NotContains(t, keys, buildImportJobKey(7))
	}
	// ...so the crash left the job record un-flipped.
	got, err := fk.Load(ctx, buildImportJobKey(7))
	assert.NoError(t, err)
	assert.Equal(t, pendingValue, got)

	assert.NoError(t, save())
	assert.Equal(t, dumpMetaKV(t, legacyKV.MemoryKV), dumpMetaKV(t, fk.MemoryKV))
}
