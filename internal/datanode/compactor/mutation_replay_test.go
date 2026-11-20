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

package compactor

import (
	"context"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	coordtask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func mutationIDs(keys ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}
}

func mutationValue(n int64) *schemapb.UpdateLiteral {
	return &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: n}}}}
}

func (s *MixCompactionTaskStorageV3Suite) TestTypedMergeSplit() { s.checkTypedRewrite(false) }
func (s *MixCompactionTaskStorageV3Suite) TestTypedMergeSort()  { s.checkTypedRewrite(true) }
func (s *MixCompactionTaskStorageV3Suite) checkTypedRewrite(sorted bool) {
	s.task.compactionParams.UseMergeSort = sorted
	s.task.compactionParams.MaxSegmentMergeSort = 10
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	testTypedRewrite(s.T(), s.task.compactionParams.StorageConfig, s.task.plan, sorted, s.task.Compact)
}

func (s *SortCompactionTaskSuite) TestTypedSort() {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	initcore.CleanArrowFileSystem()
	s.Require().NoError(initcore.InitLocalArrowFileSystem(rootPath))
	s.task.compactionParams = compaction.GenParams()
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	testTypedRewrite(s.T(), s.task.compactionParams.StorageConfig, s.task.plan, false, s.task.Compact)
}

func testTypedRewrite(t *testing.T, cfg *indexpb.StorageConfig, plan *datapb.CompactionPlan, sorted bool, compact func() (*datapb.CompactionPlanResult, error)) {
	ctx := context.Background()
	schema := w6bCompactionSchema()
	source := w6bWriteCompactionSource(t, cfg, schema, 10, []int64{1, 2, 3, 4}, sorted)
	// This column was absent from the source. Its default must survive on
	// untouched rows while an explicit SET NULL must remain null.
	const age int64 = 105
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: age, Name: "age", DataType: schemapb.DataType_Int64, Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}}})
	plan.Schema = schema
	plan.Channel = "test-channel"
	plan.TotalRows = 4
	plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{source}
	birth := tsoutil.ComposeTSByTime(getMilvusBirthday())
	set := func(ts uint64, ord uint32, value *schemapb.UpdateLiteral, ids ...int64) deltacodec.Row {
		return deltacodec.Row{TimeTick: ts, SourceOrdinal: ord, Operation: deltacodec.Update, PrimaryKeys: mutationIDs(ids...), Assignments: []*messagespb.UpdateAssignment{{FieldId: age, Literal: value}}}
	}
	early := set(birth+1, 2, mutationValue(10), 1, 2, 3)
	late := set(birth+1, 4, mutationValue(99), 1)
	rows := [][]deltacodec.Row{{late, set(birth+2, 0, &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}, 2), {TimeTick: birth + 3, Operation: deltacodec.Delete, PrimaryKeys: mutationIDs(3)}}, {early, set(birth+4, 0, mutationValue(55), 3), set(birth, 0, mutationValue(11), 4)}, {early}}
	attach := func(source *datapb.CompactionSegmentBinlogs, rows [][]deltacodec.Row) {
		base, _, err := packed.UnmarshalManifestPath(source.Manifest)
		require.NoError(t, err)
		for i, batch := range rows {
			path := metautil.BuildDeltaLogPathV3(base, int64(10000+i))
			_, err := storage.WriteMutationDelta(ctx, path, storage.MutationDelta{Layout: deltacodec.Layout{CollectionID: source.CollectionID, VChannel: plan.Channel, PartitionID: source.PartitionID, PKType: schemapb.DataType_Int64, Fields: []deltacodec.Field{{ID: age, Type: schemapb.DataType_Int64}}}, Rows: batch}, cfg, nil)
			require.NoError(t, err)
			source.Manifest, err = packed.AddDeltaLogsToManifest(source.Manifest, cfg, []packed.DeltaLogEntry{{Path: path, NumEntries: int64(len(batch))}})
			require.NoError(t, err)
		}
	}
	attach(source, rows)
	result, err := compact()
	require.NoError(t, err)
	readValues := func(result *datapb.CompactionPlanResult) map[int64]any {
		require.Equal(t, datapb.CompactionTaskState_completed, result.GetState())
		got := map[int64]any{}
		for _, seg := range result.GetSegments() {
			r, err := storage.NewManifestRecordReader(ctx, seg.Manifest, schema, storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg))
			require.NoError(t, err)
			for {
				rec, err := r.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				for j := range rec.Len() {
					pk := rec.Column(w6bCompactionPK).(*array.Int64).Value(j)
					require.EqualValues(t, birth, rec.Column(common.TimeStampField).(*array.Int64).Value(j))
					values := rec.Column(age).(*array.Int64)
					if values.IsNull(j) {
						got[pk] = nil
					} else {
						got[pk] = values.Value(j)
					}
				}
			}
			require.NoError(t, r.Close())
		}
		return got
	}
	require.Equal(t, map[int64]any{1: int64(99), 2: nil, 4: int64(7)}, readValues(result))
	// Reopen the compacted base and replay the suffix after birth+1. It
	// includes two previously delivered mutations and a new assignment.
	require.Len(t, result.GetSegments(), 1)
	out := result.GetSegments()[0]
	next := &datapb.CompactionSegmentBinlogs{SegmentID: out.SegmentID, CollectionID: source.CollectionID, PartitionID: source.PartitionID, Manifest: out.Manifest, StorageVersion: storage.StorageV3, IsSorted: sorted}
	attach(next, [][]deltacodec.Row{{set(birth+5, 0, mutationValue(81), 4), rows[1][1]}, {rows[0][1], rows[0][2]}})
	plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{next}
	plan.TotalRows = out.GetNumOfRows()
	plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 30000, End: 31000}
	plan.PreAllocatedLogIDs = &datapb.IDRange{Begin: 31000, End: 50000}
	result, err = compact()
	require.NoError(t, err)
	require.Equal(t, map[int64]any{1: int64(99), 2: nil, 4: int64(81)}, readValues(result))
}

func (s *MixCompactionTaskStorageV3Suite) TestCertifiedL0Dispatch() {
	t := s.T()
	ctx := t.Context()
	cfg := s.task.compactionParams.StorageConfig
	ratio := paramtable.Get().DataNodeCfg.L0BatchMemoryRatio
	require.NoError(t, paramtable.Get().Save(ratio.Key, "1"))
	t.Cleanup(func() { paramtable.Get().Reset(ratio.Key) })
	free := mockey.Mock(hardware.GetFreeMemoryCount).Return(uint64(1 << 20)).Build()
	defer free.UnPatch()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, Name: "pk", DataType: schemapb.DataType_Int64}, {FieldID: 105, Name: "age", DataType: schemapb.DataType_Int64}}}
	plan := &datapb.CompactionPlan{PlanID: 22, Schema: schema, Type: datapb.CompactionType_Level0DeleteCompaction, MaterializedThrough: 200, Channel: "p1_100v0", PreAllocatedLogIDs: &datapb.IDRange{Begin: 1000, End: 1100}}
	task := NewLevelZeroCompactionTask(ctx, binlogio.NewBinlogIO(s.task.cm), s.task.cm, plan, s.task.compactionParams)
	var inputs, targets []*datapb.CompactionSegmentBinlogs
	bfs := map[int64]*pkoracle.BloomFilterSet{}
	for i, part := range []int64{1, 2, common.AllPartitionsID, 4} {
		seg := &datapb.CompactionSegmentBinlogs{CollectionID: 100, PartitionID: part, SegmentID: int64(30 + i), Level: datapb.SegmentLevel_L0}
		ts := []uint64{100, 130, 160, 180}[i]
		path := metautil.BuildDeltaLogPath(cfg.RootPath, 100, part, seg.SegmentID, int64(i)+1)
		keys := mutationIDs(42, 43)
		d := storage.MutationDelta{Layout: deltacodec.Layout{CollectionID: 100, PartitionID: part, VChannel: plan.Channel, PKType: schemapb.DataType_Int64, Fields: []deltacodec.Field{{ID: 105, Type: schemapb.DataType_Int64}}}, Rows: []deltacodec.Row{{TimeTick: ts, SourceOrdinal: 3, Operation: deltacodec.Update, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{{FieldId: 105, Literal: mutationValue(int64(ts))}}}}}
		counts := &datapb.DeltaMutationCounts{UpdateKeys: 2}
		if part == 4 {
			d.Rows[0].Operation = deltacodec.Delete
			d.Rows[0].Assignments = nil
			d.Rows[0].PrimaryKeys = mutationIDs(77)
			counts = &datapb.DeltaMutationCounts{DeleteKeys: 1}
		}
		size, err := storage.WriteMutationDelta(ctx, path, d, cfg, nil)
		require.NoError(t, err)
		seg.Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: path, LogSize: size, MemorySize: 768 << 10, EntriesNum: 1, MutationCounts: counts}}}}
		inputs = append(inputs, seg)
	}
	for i, part := range []int64{1, 2, 3, 3, 4} {
		seg := &datapb.CompactionSegmentBinlogs{CollectionID: 100, PartitionID: part, SegmentID: int64(40 + i), Level: datapb.SegmentLevel_L1}
		targets = append(targets, seg)
		bfs[seg.SegmentID] = pkoracle.NewBloomFilterSet()
		key := int64(42)
		if i == 3 {
			key = 99
		}
		if i == 4 {
			key = 77
		}
		require.NoError(t, bfs[seg.SegmentID].UpdatePKRange(&storage.Int64FieldData{Data: []int64{key}}))
	}
	// A legacy base can have no stats at all, which cannot certify a negative
	// BF hit. Deliver all scope-eligible keys and let the base resolve PKs.
	noStats := &datapb.CompactionSegmentBinlogs{CollectionID: 100, PartitionID: 3, SegmentID: 45, Level: datapb.SegmentLevel_L1}
	loaded, err := task.loadBF(ctx, []*datapb.CompactionSegmentBinlogs{noStats})
	require.NoError(t, err)
	require.Nil(t, loaded[45])
	targets = append(targets, noStats)
	mockBF := mockey.Mock((*LevelZeroCompactionTask).loadBF).Return(bfs, nil).Build()
	defer mockBF.UnPatch()
	plan.SegmentBinlogs = append(inputs, targets...)
	result, err := task.Compact()
	require.NoError(t, err)
	require.EqualValues(t, 200, result.GetMaterializedThrough())
	results := result.GetSegments()
	require.NoError(t, err)
	require.Len(t, results, 6)
	for i, result := range results {
		require.Equal(t, targets[i].SegmentID, result.SegmentID)
		if i == 3 {
			require.Empty(t, result.Deltalogs)
			continue
		}
		files := result.Deltalogs[0].Binlogs
		if i == 4 {
			require.Len(t, files, 1)
			require.Nil(t, files[0].GetMutationCounts())
			legacy, err := compaction.ComposeDeleteFromDeltalogs(ctx, schemapb.DataType_Int64, &datapb.CompactionSegmentBinlogs{Deltalogs: result.Deltalogs}, storage.WithDownloader(s.task.cm.MultiRead), storage.WithStorageConfig(cfg))
			require.NoError(t, err)
			require.Equal(t, map[any]uint64{int64(77): 180}, legacy)
			continue
		}
		want := []uint64{160}
		if i < 2 {
			want = [][]uint64{{100, 160}, {130, 160}}[i]
		}
		keys := []int64{42}
		if i == 5 {
			keys = []int64{42, 43}
		}
		var observed []uint64
		var updateKeys int64
		for _, file := range files {
			updateKeys += file.GetMutationCounts().GetUpdateKeys()
			err := storage.ReadMutationDelta(ctx, s.task.cm, file, storage.StorageV2, schemapb.DataType_Int64, nil, cfg, nil, func(l deltacodec.Layout, rows []deltacodec.Row) error {
				require.Equal(t, targets[i].PartitionID, l.PartitionID)
				for _, row := range rows {
					observed = append(observed, row.TimeTick)
					require.EqualValues(t, 3, row.SourceOrdinal)
					require.Equal(t, keys, row.PrimaryKeys.GetIntId().Data)
				}
				return nil
			})
			require.NoError(t, err)
		}
		require.EqualValues(t, len(want)*len(keys), updateKeys)
		require.ElementsMatch(t, want, observed)
	}

	// A genuinely oversized final file fails the whole attempt, even though
	// earlier files could be routed and written within the budget.
	failedPlan := proto.Clone(plan).(*datapb.CompactionPlan)
	failedPlan.PreAllocatedLogIDs = &datapb.IDRange{Begin: 2000, End: 2100}
	failedPlan.SegmentBinlogs[len(inputs)-1].Deltalogs[0].Binlogs[0].MemorySize = 2 << 20
	failedWorker := NewLevelZeroCompactionTask(ctx, binlogio.NewBinlogIO(s.task.cm), s.task.cm, failedPlan, s.task.compactionParams)
	incomplete, err := failedWorker.Compact()
	require.ErrorIs(t, err, merr.ErrServiceMemoryLimitExceeded)
	require.Nil(t, incomplete)

	// Publish this exact multi-file worker result and retire both private
	// and shared sources only after every target has reached the frontier.
	metadata, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	tasks, err := coordtask.NewCompactionTaskMeta(ctx, metadata.Catalog)
	require.NoError(t, err)
	frozen := &datapb.CompactionTask{PlanID: plan.PlanID, NodeID: 9, TriggerID: 8, Type: plan.Type, Channel: plan.Channel, CollectionID: 100, PartitionID: common.AllPartitionsID, MaterializedThrough: plan.MaterializedThrough, L0Targets: &datapb.L0DispatchTargets{}}
	for _, seg := range plan.SegmentBinlogs {
		info := &datapb.SegmentInfo{ID: seg.SegmentID, CollectionID: seg.CollectionID, PartitionID: seg.PartitionID, InsertChannel: plan.Channel, State: commonpb.SegmentState_Flushed, Level: seg.Level}
		if seg.Level == datapb.SegmentLevel_L0 {
			frozen.InputSegments = append(frozen.InputSegments, seg.SegmentID)
		} else {
			frozen.L0Targets.SegmentIds = append(frozen.L0Targets.SegmentIds, seg.SegmentID)
			info.TransformStartAfterTimetick = 50
		}
		require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(info)))
	}
	publication := coordtask.NewL0CompactionTask(frozen, nil, metadata, tasks)
	worker := session.NewMockCluster(t)
	worker.EXPECT().QueryCompaction(frozen.NodeID, &datapb.CompactionStateRequest{PlanID: plan.PlanID}).Return(result, nil).Once()
	publication.QueryTaskOnWorker(worker)
	require.True(t, publication.GetTaskProto().GetL0Targets().GetPublished())
	for _, out := range results {
		seg := metadata.GetSegment(ctx, out.SegmentID)
		require.EqualValues(t, 200, seg.GetTransformStartAfterTimetick())
		require.Equal(t, out.GetDeltalogs(), seg.GetDeltalogs())
	}
	for _, id := range frozen.InputSegments {
		require.Equal(t, commonpb.SegmentState_Flushed, metadata.GetSegment(ctx, id).GetState())
	}
	require.True(t, publication.Process())
	for _, id := range frozen.InputSegments {
		require.Equal(t, commonpb.SegmentState_Dropped, metadata.GetSegment(ctx, id).GetState())
	}
}
