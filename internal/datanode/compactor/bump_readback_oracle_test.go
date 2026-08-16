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

// Real-path read-back oracle (UT_ENHANCEMENT_PLAN §2). Reads a bumped segment
// back through the PRODUCTION read path and returns fieldID->logical-value rows
// so S0 tests can diff actual materialized content, not counts. Row order is the
// read order — bump routes (additive append / fullRewrite selection / bumpOnly)
// preserve source row order, so the default oracle is order-sensitive.

import (
	"context"
	"io"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readBackRowsFromBinlogs reads seg via newCompactionSegmentRecordReader (prod
// read path) + ValueDeserializerWithSchema, returning rows in read order. Each
// row is a fieldID->logical-value map (nil = physically-null cell).
func (s *BumpSchemaVersionCompactionTaskSuite) readBackRowsFromBinlogs(seg *datapb.CompactionSegmentBinlogs, schema *schemapb.CollectionSchema) []map[int64]interface{} {
	reader, _, err := newCompactionSegmentRecordReader(context.Background(), seg, schema, s.task.compactionParams.StorageConfig,
		storage.WithVersion(seg.GetStorageVersion()),
		storage.WithDownloader(s.task.chunkManager.MultiRead),
		storage.WithStorageConfig(s.task.compactionParams.StorageConfig),
	)
	s.Require().NoError(err)
	defer reader.Close()

	rows := make([]map[int64]interface{}, 0)
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
		vs := make([]*storage.Value, rec.Len())
		s.Require().NoError(storage.ValueDeserializerWithSchema(rec, vs, schema, true))
		for _, v := range vs {
			row, ok := v.Value.(map[typeutil.UniqueID]interface{})
			s.Require().True(ok, "deserialized value is not a field map")
			m := make(map[int64]interface{}, len(row))
			for k, val := range row {
				m[k] = val
			}
			rows = append(rows, m)
		}
	}
	return rows
}

// readBackResultSegment adapts a bump result segment to the reader input; the
// reader stack follows the result segment's StorageVersion (bump is V3-only;
// downstream consumers may be V2 — §9).
func (s *BumpSchemaVersionCompactionTaskSuite) readBackResultSegment(seg *datapb.CompactionSegment, schema *schemapb.CollectionSchema) []map[int64]interface{} {
	return s.readBackRowsFromBinlogs(&datapb.CompactionSegmentBinlogs{
		SegmentID:      seg.GetSegmentID(),
		CollectionID:   1,
		PartitionID:    1,
		FieldBinlogs:   seg.GetInsertLogs(),
		InsertChannel:  "test_channel",
		StorageVersion: seg.GetStorageVersion(),
		Manifest:       seg.GetManifest(),
	}, schema)
}

// assertFieldMaterialized asserts a field is PHYSICALLY present in the result
// segment's insert logs with the expected entry count — the "physical existence"
// pillar. It distinguishes a materialized null column from a field that was never
// written (pre-fix metadata-only bump), which a logical null read-back alone cannot.
func (s *BumpSchemaVersionCompactionTaskSuite) assertFieldMaterialized(seg *datapb.CompactionSegment, fieldID int64, wantEntries int64) {
	for _, fl := range seg.GetInsertLogs() {
		if fl.GetFieldID() == fieldID {
			s.Require().NotEmpty(fl.GetBinlogs(), "field %d present but has no binlog entry", fieldID)
			s.EqualValues(wantEntries, fl.GetBinlogs()[0].GetEntriesNum(), "field %d entry count", fieldID)
			return
		}
	}
	s.Require().Fail("field not physically materialized", "fieldID %d absent from result insert logs", fieldID)
}

// assertSameInsertLogIDs asserts two insert-log sets carry identical fieldID→LogID
// mappings — i.e. zero new writes (bump-only passthrough), the replay invariant.
func (s *BumpSchemaVersionCompactionTaskSuite) assertSameInsertLogIDs(a, b []*datapb.FieldBinlog) {
	idOf := func(fbs []*datapb.FieldBinlog) map[int64][]int64 {
		m := make(map[int64][]int64)
		for _, fb := range fbs {
			for _, bl := range fb.GetBinlogs() {
				m[fb.GetFieldID()] = append(m[fb.GetFieldID()], bl.GetLogID())
			}
		}
		return m
	}
	s.Equal(idOf(a), idOf(b), "insert log IDs must be unchanged (bump-only = zero new writes)")
}

// assertManifestContainsField asserts a field is in the COMMITTED MANIFEST
// (storage truth), not merely reported in insert logs. Closes FS-1: for a nullable
// absent field, the null read-back + insert-log EntriesNum pillars are both blind to
// a "reported but manifest-dropped column" bug (the read path silently filters
// schema-has/manifest-missing fields, so a dropped column reads back as null just
// like a real null). This pillar reads the manifest field set directly.
func (s *BumpSchemaVersionCompactionTaskSuite) assertManifestContainsField(seg *datapb.CompactionSegment, fieldID int64) {
	ids, err := packed.GetManifestFieldIDs(seg.GetManifest(), s.task.compactionParams.StorageConfig)
	s.Require().NoError(err)
	_, ok := ids[fieldID]
	s.True(ok, "field %d must be in the committed manifest (storage truth), not just the insert-log report", fieldID)
}

// assertRowsOrdered diffs got against expected in read order (S0 default). A
// nil expected value means the cell must be physically null/absent.
func (s *BumpSchemaVersionCompactionTaskSuite) assertRowsOrdered(expected []map[int64]interface{}, got []map[int64]interface{}) {
	s.Require().Equal(len(expected), len(got), "row count mismatch")
	for i := range expected {
		for fid, want := range expected[i] {
			if want == nil {
				gv, present := got[i][fid]
				s.True(!present || gv == nil, "row %d field %d expected null, got %v", i, fid, gv)
				continue
			}
			s.EqualValues(want, got[i][fid], "row %d field %d value mismatch", i, fid)
		}
	}
}

// assertRowsByPK diffs by primary key — ONLY for consumers legally allowed to
// reorder (sort/merge_sort/clustering); bump paths must use assertRowsOrdered.
func (s *BumpSchemaVersionCompactionTaskSuite) assertRowsByPK(pkFieldID int64, expected []map[int64]interface{}, got []map[int64]interface{}) {
	s.Require().Equal(len(expected), len(got), "row count mismatch")
	index := make(map[interface{}]map[int64]interface{}, len(got))
	for _, r := range got {
		index[r[pkFieldID]] = r
	}
	for _, want := range expected {
		gr, ok := index[want[pkFieldID]]
		s.Require().True(ok, "pk %v missing in result", want[pkFieldID])
		for fid, wv := range want {
			if wv == nil {
				gv, present := gr[fid]
				s.True(!present || gv == nil, "pk %v field %d expected null, got %v", want[pkFieldID], fid, gv)
				continue
			}
			s.EqualValues(wv, gr[fid], "pk %v field %d value mismatch", want[pkFieldID], fid)
		}
	}
}

// TestReadBackOracleSelfBootstrap bootstraps the oracle on a hand-built segment
// with KNOWN constant values (expected = the constants written, never a
// production-path product) so a harness bug cannot hide behind a self-referential
// expectation.
func (s *BumpSchemaVersionCompactionTaskSuite) TestReadBackOracleSelfBootstrap() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	// initSegBufferForSchemaBumpWithFields writes, per row i in [0,3):
	//   field 100 (pk)   = segID + i
	//   field 101 (text) = "test string " + rune('0'+i)
	schema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	rows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], schema)
	s.Require().Len(rows, 3)
	for i := 0; i < 3; i++ {
		s.EqualValues(segID+int64(i), rows[i][100], "pk row %d", i)
		s.Equal("test string "+string(rune('0'+i)), rows[i][101], "text row %d", i)
		// system columns present and non-null
		s.NotNil(rows[i][common.RowIDField], "row_id row %d", i)
		s.NotNil(rows[i][common.TimeStampField], "ts row %d", i)
	}
}

// TestAdditiveAbsentNullableField_ReadBack — S0. Adding a nullable ordinary
// field to a sealed segment must materialize it as physical NULL on every row,
// while preserved fields and system columns stay byte-identical and row order
// is kept. Pre-fix this routed to metadata-only bump (field never written);
// count-only tests cannot see that the synthesized column is actually null.
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveAbsentNullableField_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	baseSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], baseSchema)
	s.Require().Len(srcRows, 3)

	const absentID = int64(103)
	targetFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: absentID, Name: "added_nullable", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_absent_nullable", Fields: targetFields}
	s.task.plan.Functions = nil
	targetSchema := &schemapb.CollectionSchema{Fields: targetFields}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	// Physical-existence pillar first: the field must be a real materialized
	// column (distinguishes null column from never-written pre-fix bump).
	s.assertFieldMaterialized(result.GetSegments()[0], absentID, 3)
	// FS-1 storage-truth pillar: a nullable null read-back cannot tell a real null
	// from a manifest-dropped column; assert the manifest actually carries the field.
	s.assertManifestContainsField(result.GetSegments()[0], absentID)
	dstRows := s.readBackResultSegment(result.GetSegments()[0], targetSchema)

	s.Require().Len(dstRows, 3, "row count preserved")
	for i := 0; i < 3; i++ {
		s.Nil(dstRows[i][absentID], "row %d: absent nullable field must be physical NULL", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][101], dstRows[i][101], "row %d: text preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved byte-for-byte", i)
		s.EqualValues(srcRows[i][common.TimeStampField], dstRows[i][common.TimeStampField], "row %d: ts conserved byte-for-byte", i)
	}
}

// TestAdditiveAbsentDefaultField_ReadBack — S0. Absent field WITH a default must
// be materialized as the default value on every historical row (not null).
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveAbsentDefaultField_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	baseSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], baseSchema)

	const defID = int64(103)
	targetFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: defID, Name: "added_default", DataType: schemapb.DataType_Int64, Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
	})
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_absent_default", Fields: targetFields}
	s.task.plan.Functions = nil
	targetSchema := &schemapb.CollectionSchema{Fields: targetFields}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	s.assertFieldMaterialized(result.GetSegments()[0], defID, 3)
	dstRows := s.readBackResultSegment(result.GetSegments()[0], targetSchema)

	s.Require().Len(dstRows, 3)
	for i := 0; i < 3; i++ {
		s.EqualValues(42, dstRows[i][defID], "row %d: absent field must carry default value 42", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved", i)
	}
}

// TestAdditiveAbsentStructArrayVectorSubfield_52555 — verifies our fix covers
// issue #52555: adding a nullable struct-array field with a vector sub-field
// (fieldID 106) to a sealed segment. Because GetAllFieldSchemas expands struct
// sub-fields, absentOrdinarySchemaFields includes 106, so the additive path must
// materialize 106's binlog. #52555's root cause is that the (pre-fix) bump-only
// path wrote NO binlog for the added field, so the index pre-check reported
// "vector array field binlog not found; fieldID=106". If additive materializes it,
// the pre-check finds the binlog and create_index no longer fails.
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveAbsentStructArrayVectorSubfield_52555() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	const structFieldID = int64(104)
	const vecSubFieldID = int64(106)
	targetSchema := &schemapb.CollectionSchema{
		Name:   "bump_struct_array",
		Fields: schemaBumpBaseFields(),
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: structFieldID,
			Name:    "profile",
			Fields: []*schemapb.FieldSchema{{
				FieldID:     vecSubFieldID,
				Name:        "p_vec",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				Nullable:    true,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
			}},
		}},
	}
	s.task.plan.Schema = targetSchema
	s.task.plan.Functions = nil
	params, err := compaction.GenerateJSONParams(targetSchema)
	s.Require().NoError(err)
	s.task.plan.JsonParams = params

	result, err := s.task.Compact()
	s.Require().NoError(err, "additive must materialize absent struct-array vector sub-field, not fail (#52555)")
	s.Require().Len(result.GetSegments(), 1)

	// 106 must have a materialized binlog (top-level or as a struct child field) —
	// exactly what #52555's index pre-check looks for.
	seg := result.GetSegments()[0]
	found := false
	for _, fb := range seg.GetInsertLogs() {
		if fb.GetFieldID() == vecSubFieldID {
			found = true
		}
		for _, cf := range fb.GetChildFields() {
			if cf == vecSubFieldID {
				found = true
			}
		}
	}
	s.True(found, "struct-array vector sub-field %d must be materialized into insert logs (#52555)", vecSubFieldID)
	s.assertManifestContainsField(seg, vecSubFieldID)
}

// TestAdditiveStructArrayThreeChildFields_FillEmpty — verifies additive fills empty
// for EVERY child of a struct-array field (not just one vector sub-field). Since
// GetAllFieldSchemas expands all struct sub-fields, absentOrdinarySchemaFields must
// contain all 3, and each must be materialized (its own column group + binlog).
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveStructArrayThreeChildFields_FillEmpty() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	const structFieldID = int64(104)
	childIDs := []int64{106, 107, 108}
	targetSchema := &schemapb.CollectionSchema{
		Name:   "bump_struct_3child",
		Fields: schemaBumpBaseFields(),
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: structFieldID,
			Name:    "profile",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 106, Name: "p_vec", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
				},
				{
					FieldID: 107, Name: "p_tags", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar, Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
				},
				{
					FieldID: 108, Name: "p_nums", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64, Nullable: true,
				},
			},
		}},
	}
	s.task.plan.Schema = targetSchema
	s.task.plan.Functions = nil
	params, err := compaction.GenerateJSONParams(targetSchema)
	s.Require().NoError(err)
	s.task.plan.JsonParams = params

	result, err := s.task.Compact()
	s.Require().NoError(err, "additive must fill empty for EACH struct child, not fail")
	s.Require().Len(result.GetSegments(), 1)
	seg := result.GetSegments()[0]

	for _, cid := range childIDs {
		found := false
		for _, fb := range seg.GetInsertLogs() {
			if fb.GetFieldID() == cid {
				found = true
			}
			for _, cf := range fb.GetChildFields() {
				if cf == cid {
					found = true
				}
			}
		}
		s.True(found, "struct child field %d must be filled empty + materialized into insert logs", cid)
		s.assertManifestContainsField(seg, cid)
	}
}

// TestAdditiveAbsentTextFieldAsBinary_ReadBack — S0/S2. Adding a nullable TEXT
// field routes additive through setupWriter's WithTextRefsAsBinary branch (the
// plain partial writer REJECTS TEXT via validatePackedRecordBatchWriterSchema).
// Without that writer this Compact would fail; the materializer must also fill the
// absent TEXT column as a binary null (a utf8 fill panics array.NewRecord). Prior
// tests only covered the fullRewrite TEXT path + Int64 additive — the additive
// TEXT path (this exact writer branch) was uncovered.
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveAbsentTextFieldAsBinary_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	baseSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], baseSchema)
	s.Require().Len(srcRows, 3)

	const textID = int64(105)
	targetFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: textID, Name: "note", DataType: schemapb.DataType_Text, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
	})
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_absent_text", Fields: targetFields}
	s.task.plan.Functions = nil
	params, err := compaction.GenerateJSONParams(s.task.plan.GetSchema())
	s.Require().NoError(err)
	s.task.plan.JsonParams = params
	targetSchema := &schemapb.CollectionSchema{Fields: targetFields}

	result, err := s.task.Compact()
	s.Require().NoError(err, "additive must route TEXT schema through WithTextRefsAsBinary, not fail")
	s.Require().Len(result.GetSegments(), 1)
	s.assertFieldMaterialized(result.GetSegments()[0], textID, 3)
	s.assertManifestContainsField(result.GetSegments()[0], textID)

	dstRows := s.readBackResultSegment(result.GetSegments()[0], targetSchema)
	s.Require().Len(dstRows, 3)
	for i := 0; i < 3; i++ {
		s.Nil(dstRows[i][textID], "row %d: absent TEXT field must be physical NULL (binary LOB-ref)", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved", i)
	}
}

// TestAdditiveBM25Output_ReadBack — S0. Backfilling a BM25 function output
// (sparse) must physically materialize it on every row, with the input text and
// system columns conserved. Sparse content correctness is pinned at unit level
// (mock-runner byte oracle); this integration test pins physical-existence +
// non-null + no row loss/misalignment through the real loon-FFI write/read path.
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveBM25Output_ReadBack() {
	s.prepareBumpSchemaVersionCompaction() // source: base fields (text 101), no sparse 102

	srcSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], srcSchema)
	s.Require().Len(srcRows, 3)

	const sparseID = int64(102)
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	s.assertFieldMaterialized(result.GetSegments()[0], sparseID, 3)

	dstRows := s.readBackResultSegment(result.GetSegments()[0], s.task.plan.GetSchema())
	s.Require().Len(dstRows, 3)

	// mirror-oracle: run the SAME BM25 runner over the source text; the materialized
	// sparse must equal it byte-for-byte per row. Catches row rotation/misalignment
	// that a NotNil check cannot (v3 B2b). Bound: pins materialize-path vs runner-path
	// consistency, NOT BM25 math itself (v3 §9).
	coll := s.task.plan.GetSchema()
	runner, err := function.NewFunctionRunner(coll, coll.GetFunctions()[0])
	s.Require().NoError(err)
	defer runner.Close()
	texts := make([]string, len(srcRows))
	for i := range srcRows {
		texts[i] = srcRows[i][101].(string)
	}
	out, err := runner.BatchRun(texts)
	s.Require().NoError(err)
	expected, ok := out[0].(*schemapb.SparseFloatArray)
	s.Require().True(ok)
	s.Require().Len(expected.GetContents(), 3)

	for i := 0; i < 3; i++ {
		gotSparse, ok := dstRows[i][sparseID].([]byte)
		s.Require().True(ok, "row %d: sparse must be []byte", i)
		s.Equal(expected.GetContents()[i], gotSparse, "row %d: sparse must match mirror-oracle (byte-for-byte, catches rotation)", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][101], dstRows[i][101], "row %d: text input preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved", i)
	}
}

// TestEnsureAdditiveReadAnchor_Branches — S3. The additive read must always
// carry a physical row-count anchor: RowID preferred, else Timestamp, else a
// deterministic error (never a stuck/zero-row bump).
func (s *BumpSchemaVersionCompactionTaskSuite) TestEnsureAdditiveReadAnchor_Branches() {
	s.task.plan.Schema = &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	empty := func() *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{}}
	}

	_, ids, err := s.task.ensureAdditiveReadAnchor(empty(), nil, map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}})
	s.Require().NoError(err)
	s.Contains(ids, int64(common.RowIDField), "RowID preferred as anchor")

	_, ids, err = s.task.ensureAdditiveReadAnchor(empty(), nil, map[int64]struct{}{common.TimeStampField: {}})
	s.Require().NoError(err)
	s.Contains(ids, int64(common.TimeStampField), "Timestamp fallback anchor")

	_, _, err = s.task.ensureAdditiveReadAnchor(empty(), nil, map[int64]struct{}{100: {}})
	s.Require().Error(err, "neither RowID nor Timestamp physically present -> deterministic error, no stuck bump")

	// already carrying an anchor -> unchanged
	_, ids, err = s.task.ensureAdditiveReadAnchor(empty(), []int64{common.RowIDField}, map[int64]struct{}{})
	s.Require().NoError(err)
	s.Equal([]int64{common.RowIDField}, ids)
}

// TestAdditiveReplayBumpOnly_ReadBack — S3, REAL replay vector. After the first
// additive bump commits (materializes 103), a crash-after-commit re-triggers the
// plan on the ADVANCED manifest (already carrying 103). The second run must route
// bump-only: manifest unchanged, insert logs passed through with identical LogIDs
// (zero new writes), data identical. The prior version re-ran the same original
// plan (tested determinism, not the real advanced-manifest replay).
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReplayBumpOnly_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	const absentID = int64(103)
	targetFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: absentID, Name: "added_nullable", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_replay", Fields: targetFields}
	s.task.plan.Functions = nil
	targetSchema := &schemapb.CollectionSchema{Fields: targetFields}

	// First bump: additive materializes 103.
	res1, err := s.task.Compact()
	s.Require().NoError(err)
	seg1 := res1.GetSegments()[0]
	s.assertManifestContainsField(seg1, absentID) // FS-1: seg1 manifest truly carries the field before replay
	rows1 := s.readBackResultSegment(seg1, targetSchema)
	s.Require().Len(rows1, 3)

	// Real replay: feed the advanced manifest (contains 103) back as plan input.
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{{
		SegmentID:      seg1.GetSegmentID(),
		CollectionID:   1,
		PartitionID:    1,
		FieldBinlogs:   seg1.GetInsertLogs(),
		InsertChannel:  "test_channel",
		StorageVersion: seg1.GetStorageVersion(),
		Manifest:       seg1.GetManifest(),
	}}
	s.task.plan.TotalRows = seg1.GetNumOfRows()

	// Real replay is a FRESH task object on the advanced manifest (crash-after-commit
	// meta re-trigger), not the same in-memory task — build a new one over the same plan.
	task2 := NewBumpSchemaVersionCompactionTask(context.Background(), s.task.chunkManager, s.task.plan, s.task.compactionParams)
	res2, err := task2.Compact()
	s.Require().NoError(err)
	seg2 := res2.GetSegments()[0]

	// 103 already present -> bump-only: zero new writes, data unchanged.
	s.Equal(seg1.GetManifest(), seg2.GetManifest(), "replay must not rewrite manifest (bump-only)")
	s.assertSameInsertLogIDs(seg1.GetInsertLogs(), seg2.GetInsertLogs())
	rows2 := s.readBackResultSegment(seg2, targetSchema)
	s.assertRowsOrdered(rows1, rows2)
}

// TestFullRewriteDropsField_ReadBack — S0/S1. Dropping a field routes to
// fullRewrite; the result must physically lose the dropped column while every
// kept row's other columns stay byte-identical and no row is lost (no delete).
// This drives the fullRewrite data flow through the REAL writer + read-back,
// replacing the mock-writer-only coverage.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteDropsField_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	srcFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: 104, Name: "to_drop", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.initSegBufferForSchemaBumpWithFields(segID, srcFields, func(i int, v map[int64]interface{}) {
		v[104] = int64(1000 + i)
	})
	s.finishBumpSchemaVersionSegment()

	srcSchema := &schemapb.CollectionSchema{Fields: srcFields}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], srcSchema)
	s.Require().Len(srcRows, 3)
	s.EqualValues(1000, srcRows[0][104], "sanity: source carried field 104")

	// target drops 104 -> droppedFieldIDs -> fullRewrite route
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_drop", Fields: schemaBumpBaseFields()}
	s.task.plan.Functions = nil
	targetSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	dstRows := s.readBackResultSegment(result.GetSegments()[0], targetSchema)

	s.Require().Len(dstRows, 3, "no delete -> all rows kept")
	for i := 0; i < 3; i++ {
		_, has104 := dstRows[i][104]
		s.False(has104, "row %d: dropped field 104 must be physically gone", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][101], dstRows[i][101], "row %d: text preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved", i)
		s.EqualValues(srcRows[i][common.TimeStampField], dstRows[i][common.TimeStampField], "row %d: ts conserved", i)
	}
}

// TestDeclaredFunctionOutputFields_Branches — S0. A field flagged
// IsFunctionOutput with no producing function is persisted-schema corruption and
// MUST error, never be silently backfilled as an ordinary (a non-nullable sparse
// vector "default/NULL" fill would corrupt/panic).
func (s *BumpSchemaVersionCompactionTaskSuite) TestDeclaredFunctionOutputFields_Branches() {
	ok := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}},
	}
	declared, err := declaredFunctionOutputFields(ok)
	s.Require().NoError(err)
	s.Contains(declared, int64(102))

	orphan := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
	}
	_, err = declaredFunctionOutputFields(orphan)
	s.Require().Error(err, "orphan function-output field must error, not be backfilled as ordinary")
}

// TestAbsentOrdinarySchemaFields_Classification — S0. Exactly the non-system,
// non-function-output, physically-absent fields are classified as absent
// ordinary (mis-classification either drops a real field or backfills a vector).
func (s *BumpSchemaVersionCompactionTaskSuite) TestAbsentOrdinarySchemaFields_Classification() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
		},
	}
	existing := map[int64]struct{}{common.RowIDField: {}, 100: {}, 101: {}}
	funcOut := map[int64]struct{}{102: {}}
	absent := absentOrdinarySchemaFields(schema, existing, funcOut)
	s.Require().Len(absent, 1, "only 103 is absent ordinary")
	s.EqualValues(103, absent[0].GetFieldID())
}

// TestAdditiveMinHashOutput_ReadBack — S0. Symmetric to BM25: backfilling a
// MinHash binary-vector function output must physically materialize it non-null
// on every row with inputs/system columns conserved.
func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveMinHashOutput_ReadBack() {
	s.setupMinHashTest()
	s.prepareMinHashBumpSchemaVersionCompaction()

	srcSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	srcRows := s.readBackRowsFromBinlogs(s.task.plan.SegmentBinlogs[0], srcSchema)
	s.Require().Len(srcRows, 3)

	const minhashID = int64(102)
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	s.assertFieldMaterialized(result.GetSegments()[0], minhashID, 3)

	dstRows := s.readBackResultSegment(result.GetSegments()[0], s.task.plan.GetSchema())
	s.Require().Len(dstRows, 3)

	// mirror-oracle: run the SAME MinHash runner over source text; materialized
	// binary output must equal it byte-for-byte per row (catches rotation). Bound:
	// pins materialize-path vs runner-path consistency, not MinHash math (v3 §9).
	coll := s.task.plan.GetSchema()
	runner, err := function.NewFunctionRunner(coll, coll.GetFunctions()[0])
	s.Require().NoError(err)
	defer runner.Close()
	texts := make([]string, len(srcRows))
	for i := range srcRows {
		texts[i] = srcRows[i][101].(string)
	}
	out, err := runner.BatchRun(texts)
	s.Require().NoError(err)
	fd, ok := out[0].(*schemapb.FieldData)
	s.Require().True(ok)
	binVec := fd.GetVectors().GetBinaryVector()
	bytesPerRow := int(fd.GetVectors().GetDim()) / 8
	s.Require().Equal(3*bytesPerRow, len(binVec))

	for i := 0; i < 3; i++ {
		gotBin, ok := dstRows[i][minhashID].([]byte)
		s.Require().True(ok, "row %d: minhash must be []byte", i)
		s.Equal(binVec[i*bytesPerRow:(i+1)*bytesPerRow], gotBin, "row %d: minhash must match mirror-oracle (byte-for-byte, catches rotation)", i)
		s.EqualValues(srcRows[i][100], dstRows[i][100], "row %d: pk preserved", i)
		s.EqualValues(srcRows[i][101], dstRows[i][101], "row %d: text input preserved", i)
		s.EqualValues(srcRows[i][common.RowIDField], dstRows[i][common.RowIDField], "row %d: row_id conserved", i)
	}
}

// TestFullRewriteTTLExpiresAllRows — S0/S1. When a fullRewrite (dropped field)
// coincides with a collection TTL that expires every row, the result must be a
// clean empty segment (0 rows), not a malformed manifest or a crash. Exercises
// the selectFullRewriteRecord Filtered branch + runFullSchemaRewrite empty path.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteTTLExpiresAllRows() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	srcFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: 104, Name: "to_drop", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.initSegBufferForSchemaBumpWithFields(segID, srcFields, func(i int, v map[int64]interface{}) {
		v[104] = int64(1000 + i)
	})
	s.finishBumpSchemaVersionSegment()

	// drop 104 -> fullRewrite; rows carry getMilvusBirthday() ts (years old), so
	// a 1s collection TTL expires all of them.
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_ttl_all", Fields: schemaBumpBaseFields()}
	s.task.plan.Functions = nil
	s.task.plan.CollectionTtl = int64(time.Second)

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	// Empty-segment BOUNDARY case: all rows expired -> 0 rows is the correct
	// oracle here (no rows exist to read back); the row-selection correctness is
	// pinned by TestFullRewritePartialTTL_ReadBack below, so this is not count-only.
	seg := result.GetSegments()[0]
	s.EqualValues(0, seg.GetNumOfRows(), "all rows TTL-expired -> empty result segment")
	// Close the former over-claim: actually assert the manifest, don't just say it.
	// An all-expired fullRewrite must not emit a partial/malformed manifest — here
	// the correct oracle is an empty manifest (verified: probe showed "").
	s.Empty(seg.GetManifest(), "empty segment must carry no (malformed/partial) manifest")
}

// TestFullRewritePartialTTL_ReadBack — S0/S1. Replaces the earlier count-only TTL
// check: with rows at MIXED timestamps and a collection TTL that expires only some,
// the result must keep exactly the non-expired rows and drop the field — verified by
// reading the kept row's actual pk/text (a "keep wrong row" bug would slip past a
// count==1 check).
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewritePartialTTL_ReadBack() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Pin task time; rows 0,1 are old (expired under a 1h TTL), row 2 is recent (kept).
	currentTime := getMilvusBirthday().Add(time.Hour)
	s.task.currentTime = currentTime
	insertTs := int64(tsoutil.ComposeTSByTime(currentTime))
	oldTs := int64(tsoutil.ComposeTSByTime(currentTime.Add(-2 * time.Hour)))

	srcFields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID: 104, Name: "to_drop", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.initSegBufferForSchemaBumpWithFields(segID, srcFields, func(i int, v map[int64]interface{}) {
		v[104] = int64(1000 + i)
		if i < 2 {
			v[common.TimeStampField] = oldTs
		} else {
			v[common.TimeStampField] = insertTs
		}
	})
	s.finishBumpSchemaVersionSegment()

	// drop 104 -> fullRewrite; collectionTTL=1h expires rows 0,1 only.
	s.task.plan.Schema = &schemapb.CollectionSchema{Name: "bump_partial_ttl", Fields: schemaBumpBaseFields()}
	s.task.plan.Functions = nil
	s.task.plan.CollectionTtl = int64(time.Hour)
	targetSchema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 1)
	dstRows := s.readBackResultSegment(result.GetSegments()[0], targetSchema)

	s.Require().Len(dstRows, 1, "exactly the one non-expired row is kept")
	s.EqualValues(segID+2, dstRows[0][100], "kept row must be the recent one (pk=102), not an expired one")
	s.Equal("test string 2", dstRows[0][101], "kept row text must match the recent row")
	_, has104 := dstRows[0][104]
	s.False(has104, "dropped field 104 must be physically gone")
}
