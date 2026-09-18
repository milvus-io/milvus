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
	"fmt"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	hashSplitE2ESegmentID = int64(3001)
	hashSplitE2EDeltaPath = "deltalog/3001"
)

// hashSplitE2ETargets is a 1 -> 2 split at modulus 2: the shape the combined
// e2e drives, where the source owned the whole value space.
var hashSplitE2ETargets = []*datapb.SplitShardTaskTarget{
	{Vchannel: "by-dev-rootcoord-dml_1_1v0", Buckets: []uint64{0}},
	{Vchannel: "by-dev-rootcoord-dml_2_1v0", Buckets: []uint64{1}},
}

// HashSplitRewriteSuite runs whole rewrites over real binlogs: the input segment
// is serialized by a SegmentWriter, served through a mocked BinlogIO, and the
// outputs are checked per target.
type HashSplitRewriteSuite struct {
	suite.Suite
	mockBinlogIO *mock_util.MockBinlogIO
}

func TestHashSplitRewriteSuite(t *testing.T) {
	suite.Run(t, new(HashSplitRewriteSuite))
}

func (s *HashSplitRewriteSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *HashSplitRewriteSuite) SetupTest() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, s.T().TempDir())
	initcore.InitStorageV2FileSystem(paramtable.Get())
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
}

func (s *HashSplitRewriteSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
}

// hashSplitE2ERow builds one row of genCollectionSchema, plus any extra columns.
func hashSplitE2ERow(pk int64, ts int64, extra map[int64]any) map[int64]any {
	row := map[int64]any{
		common.RowIDField:     pk,
		common.TimeStampField: ts,
		100:                   pk,
		101:                   int32(pk),
		102:                   "varchar",
		103:                   []float32{1, 2, 3, 4},
	}
	for k, v := range extra {
		row[k] = v
	}
	return row
}

// prepareRewrite serializes numRows rows (pk 0..numRows-1) into one input
// segment, deletes deletedPKs after every insert, and returns the plan.
func (s *HashSplitRewriteSuite) prepareRewrite(
	schema *schemapb.CollectionSchema,
	numRows int64,
	extra func(pk int64) map[int64]any,
	deletedPKs []int64,
) *datapb.CompactionPlan {
	rowTs := int64(tsoutil.ComposeTSByTimeWithLogical(getMilvusBirthday(), 0))
	segWriter, err := NewSegmentWriter(schema, numRows, compactionBatchSize, hashSplitE2ESegmentID, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)
	for pk := int64(0); pk < numRows; pk++ {
		var cols map[int64]any
		if extra != nil {
			cols = extra(pk)
		}
		s.Require().NoError(segWriter.Write(&storage.Value{
			PK:        storage.NewInt64PrimaryKey(pk),
			Timestamp: rowTs,
			Value:     hashSplitE2ERow(pk, rowTs, cols),
		}))
	}
	segWriter.FlushAndIsFull()

	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, segWriter)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
		left, right := lo.Difference(keys, lo.Keys(kvs))
		return len(left) == 0 && len(right) == 0
	})).Return(lo.Values(kvs), nil).Once()

	var deltalogs []*datapb.FieldBinlog
	if len(deletedPKs) > 0 {
		deleteTs := tsoutil.ComposeTSByTimeWithLogical(getMilvusBirthday(), 10)
		tss := make([]uint64, len(deletedPKs))
		for i := range tss {
			tss[i] = deleteTs
		}
		blob, err := getInt64DeltaBlobs(hashSplitE2ESegmentID, deletedPKs, tss)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{hashSplitE2EDeltaPath}).
			Return([][]byte{blob.GetValue()}, nil).Once()
		deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: hashSplitE2EDeltaPath}}}}
	}

	params, err := compaction.GenerateJSONParams(schema)
	s.Require().NoError(err)
	return &datapb.CompactionPlan{
		PlanID:  77,
		Type:    datapb.CompactionType_HashSplitCompaction,
		Channel: "by-dev-rootcoord-dml_0_1v0",
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:    hashSplitE2ESegmentID,
			CollectionID: CollectionID,
			PartitionID:  PartitionID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs:    deltalogs,
		}},
		Schema:                 schema,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: 29531},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		TotalRows:              numRows,
		HashSplitTargets:       hashSplitE2ETargets,
		HashSplitModulus:       2,
	}
}

// rowsPerChannel runs the rewrite and sums the output rows of each channel.
func (s *HashSplitRewriteSuite) rowsPerChannel(plan *datapb.CompactionPlan) map[string]int64 {
	task := NewHashSplitCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams())
	result, err := task.Compact()
	s.Require().NoError(err)
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
	s.Equal(datapb.CompactionType_HashSplitCompaction, result.GetType())
	s.Equal(plan.GetChannel(), result.GetChannel())

	got := map[string]int64{}
	for _, seg := range result.GetSegments() {
		got[seg.GetChannel()] += seg.GetNumOfRows()
	}
	return got
}

// writePathTable is the routing table the write path derives for the split
// collection's post-split meta: routing.TableFromMeta, what the proxy routes
// every insert and delete by.
func (s *HashSplitRewriteSuite) writePathTable() *routing.ResidueTable {
	channels := lo.Map(hashSplitE2ETargets, func(t *datapb.SplitShardTaskTarget, _ int) string { return t.GetVchannel() })
	infos := lo.Map(hashSplitE2ETargets, func(t *datapb.SplitShardTaskTarget, _ int) *schemapb.CollectionShardInfo {
		return hashInfo(t.GetVchannel(), t.GetBuckets()...)
	})
	table, err := routing.TableFromMeta(channels, infos, 2)
	s.Require().NoError(err)
	return table
}

// expectedByPK places every surviving pk where the write path would.
func (s *HashSplitRewriteSuite) expectedByPK(numRows int64, deleted []int64) map[string]int64 {
	table := s.writePathTable()
	gone := lo.SliceToMap(deleted, func(pk int64) (int64, struct{}) { return pk, struct{}{} })
	pks := make([]int64, 0, numRows)
	for pk := int64(0); pk < numRows; pk++ {
		if _, ok := gone[pk]; !ok {
			pks = append(pks, pk)
		}
	}
	want := map[string]int64{}
	for _, pk := range pks {
		vchannel, err := table.VChannelOfPK(pk)
		s.Require().NoError(err)
		want[vchannel]++
	}
	return want
}

func (s *HashSplitRewriteSuite) TestRewriteRoutesByPrimaryKeyAndFoldsDeletes() {
	const numRows = 1000
	deleted := []int64{3, 7, 11, 500, 999}
	plan := s.prepareRewrite(genCollectionSchema(), numRows, nil, deleted)

	got := s.rowsPerChannel(plan)
	want := s.expectedByPK(numRows, deleted)
	s.Len(want, 2, "both halves must own rows for the check to mean anything")
	s.Equal(want, got)
}

// datacoord keeps binlogs with the path stripped to a log id, so a rewrite plan
// arrives carrying ids. The compactor must rebuild the paths before reading,
// exactly as the mix and L0 compactors do -- otherwise the input's own
// deltalogs fail to download and the whole rewrite fails.
func (s *HashSplitRewriteSuite) TestRewriteRebuildsCompressedBinlogPaths() {
	const numRows = 200
	const deltaLogID = int64(4242)
	deleted := []int64{5, 6}

	plan := s.prepareRewrite(genCollectionSchema(), numRows, nil, nil)
	rootPath := compaction.GenParams().StorageConfig.GetRootPath()
	// The path convention the rebuild must produce, spelled out rather than
	// derived, so a change to it fails here.
	wantPath := fmt.Sprintf("%s/delta_log/%d/%d/%d/%d", rootPath, CollectionID, PartitionID, hashSplitE2ESegmentID, deltaLogID)

	deleteTs := tsoutil.ComposeTSByTimeWithLogical(getMilvusBirthday(), 20)
	tss := make([]uint64, len(deleted))
	for i := range tss {
		tss[i] = deleteTs
	}
	blob, err := getInt64DeltaBlobs(hashSplitE2ESegmentID, deleted, tss)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{wantPath}).
		Return([][]byte{blob.GetValue()}, nil).Once()
	// As meta holds it: a log id and no path at all.
	plan.SegmentBinlogs[0].Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogID: deltaLogID}}}}

	got := s.rowsPerChannel(plan)
	s.Equal(s.expectedByPK(numRows, deleted), got)
	s.EqualValues(numRows-len(deleted), lo.Sum(lo.Values(got)))
}

// A plan whose binlog paths cannot be rebuilt fails before any IO, rather than
// reading a delete set that is silently short.
func (s *HashSplitRewriteSuite) TestRewriteFailsWhenBinlogPathsCannotBeRebuilt() {
	plan := s.prepareRewritePlanOnly(genCollectionSchema(), 10, nil)
	failing := mockey.Mock(binlog.DecompressCompactionBinlogsWithRootPath).
		Return(merr.WrapErrServiceInternalMsg("bad binlog type")).Build()
	defer failing.UnPatch()

	task := NewHashSplitCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams())
	_, err := task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "bad binlog type")
}

// varCharPKSchema is genCollectionSchema with a VarChar primary key.
func varCharPKSchema() *schemapb.CollectionSchema {
	schema := genCollectionSchema()
	for _, f := range schema.GetFields() {
		if f.GetFieldID() == 100 {
			f.DataType = schemapb.DataType_VarChar
			f.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}
		}
	}
	return schema
}

func (s *HashSplitRewriteSuite) TestRewriteRoutesAVarCharPrimaryKey() {
	const numRows = 400
	schema := varCharPKSchema()
	rowTs := int64(tsoutil.ComposeTSByTimeWithLogical(getMilvusBirthday(), 0))
	segWriter, err := NewSegmentWriter(schema, numRows, compactionBatchSize, hashSplitE2ESegmentID, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)
	keys := make([]string, 0, numRows)
	for i := int64(0); i < numRows; i++ {
		key := fmt.Sprintf("pk-%d", i)
		keys = append(keys, key)
		row := hashSplitE2ERow(i, rowTs, map[int64]any{100: key})
		s.Require().NoError(segWriter.Write(&storage.Value{PK: storage.NewVarCharPrimaryKey(key), Timestamp: rowTs, Value: row}))
	}
	segWriter.FlushAndIsFull()
	kvs, fBinlogs, err := serializeWrite(context.TODO(), allocator.NewLocalAllocator(100, math.MaxInt64), segWriter)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(lo.Values(kvs), nil).Once()

	plan := s.prepareRewritePlanOnly(schema, numRows, lo.Values(fBinlogs))
	got := s.rowsPerChannel(plan)

	table := s.writePathTable()
	want := map[string]int64{}
	for _, key := range keys {
		vchannel, err := table.VChannelOfPK(key)
		s.Require().NoError(err)
		want[vchannel]++
	}
	s.Len(want, 2)
	s.Equal(want, got)
}

// prepareRewritePlanOnly builds the plan for already-registered binlog mocks.
func (s *HashSplitRewriteSuite) prepareRewritePlanOnly(schema *schemapb.CollectionSchema, numRows int64, binlogs []*datapb.FieldBinlog) *datapb.CompactionPlan {
	params, err := compaction.GenerateJSONParams(schema)
	s.Require().NoError(err)
	return &datapb.CompactionPlan{
		PlanID:  78,
		Type:    datapb.CompactionType_HashSplitCompaction,
		Channel: "by-dev-rootcoord-dml_0_1v0",
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:    hashSplitE2ESegmentID,
			CollectionID: CollectionID,
			PartitionID:  PartitionID,
			FieldBinlogs: binlogs,
		}},
		Schema:                 schema,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: 29531},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		TotalRows:              numRows,
		HashSplitTargets:       hashSplitE2ETargets,
		HashSplitModulus:       2,
	}
}

func (s *HashSplitRewriteSuite) TestRewriteWithAnEmptyHalf() {
	// One row: exactly one target owns it and the other receives nothing. An
	// empty half is legal and simply produces no output segment.
	plan := s.prepareRewrite(genCollectionSchema(), 1, nil, nil)
	got := s.rowsPerChannel(plan)
	want := s.expectedByPK(1, nil)
	s.Len(want, 1)
	s.Equal(want, got)
}

func (s *HashSplitRewriteSuite) TestRewriteFailsOnARowNoTargetOwns() {
	// Targets claiming only the even residues of modulus 4: every odd-hashing
	// pk belongs to neither, and the rewrite must fail instead of guessing.
	const numRows = 200
	plan := s.prepareRewrite(genCollectionSchema(), numRows, nil, nil)
	plan.HashSplitModulus = 4
	plan.HashSplitTargets = []*datapb.SplitShardTaskTarget{
		{Vchannel: "even0", Buckets: []uint64{0}},
		{Vchannel: "even2", Buckets: []uint64{2}},
	}
	task := NewHashSplitCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams())
	_, err := task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "matches none of the split targets")
}

func (s *HashSplitRewriteSuite) TestRewriteSurfacesPlanAndIOFailures() {
	schema := genCollectionSchema()
	params, err := compaction.GenerateJSONParams(schema)
	s.Require().NoError(err)
	base := func() *datapb.CompactionPlan {
		return &datapb.CompactionPlan{
			PlanID:  79,
			Type:    datapb.CompactionType_HashSplitCompaction,
			Channel: "src",
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
				SegmentID: hashSplitE2ESegmentID, CollectionID: CollectionID, PartitionID: PartitionID,
				FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "insert/100", EntriesNum: 1}}}},
			}},
			Schema:                 schema,
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: 29531},
			PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
			MaxSize:                64 * 1024 * 1024,
			JsonParams:             params,
			TotalRows:              1,
			HashSplitTargets:       hashSplitE2ETargets,
			HashSplitModulus:       2,
		}
	}
	run := func(plan *datapb.CompactionPlan) error {
		task := NewHashSplitCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams())
		_, err := task.Compact()
		return err
	}

	s.Run("invalid plan", func() {
		plan := base()
		plan.SegmentBinlogs = nil
		s.ErrorIs(run(plan), merr.ErrServiceInternal)
	})
	s.Run("no modulus", func() {
		plan := base()
		plan.HashSplitModulus = 0
		s.ErrorIs(run(plan), merr.ErrServiceInternal)
	})
	s.Run("id range too small", func() {
		plan := base()
		plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 10, End: 11}
		s.ErrorIs(run(plan), merr.ErrServiceInternal)
	})
	s.Run("no primary key", func() {
		plan := base()
		plan.Schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, DataType: schemapb.DataType_Int32}}}
		s.Error(run(plan))
	})
	s.Run("deltalog download fails", func() {
		plan := base()
		plan.SegmentBinlogs[0].Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "delta/x"}}}}
		s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"delta/x"}).Return(nil, merr.WrapErrIoFailedReason("boom")).Once()
		s.ErrorIs(run(plan), merr.ErrIoFailed)
	})
	s.Run("insert binlog download fails", func() {
		plan := base()
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, merr.WrapErrIoFailedReason("boom")).Once()
		s.Error(run(plan))
	})
}

func TestHashSplitCompactorLifecycleAccessors(t *testing.T) {
	params := compaction.GenParams()
	plan := hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 10000, End: 10100})
	plan.SlotUsage = 3
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, params)
	assert.Equal(t, int64(3), task.GetSlotUsage())
	assert.Equal(t, params.StorageConfig, task.GetStorageConfig())
	task.Complete()
	assert.Error(t, task.ctx.Err())
	task2 := NewHashSplitCompactionTask(context.Background(), nil, plan, params)
	task2.Stop()
	assert.Error(t, task2.ctx.Err())
}

func (s *HashSplitRewriteSuite) TestRewriteFailsCleanlyWhenAppendFails() {
	plan := s.prepareRewrite(genCollectionSchema(), 50, nil, nil)
	appendMock := mockey.Mock((*storage.RecordBuilder).Append).Return(merr.WrapErrServiceInternalMsg("injected append failure")).Build()
	defer appendMock.UnPatch()

	task := NewHashSplitCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams())
	result, err := task.Compact()
	s.Nil(result)
	s.ErrorContains(err, "injected append failure")
}
