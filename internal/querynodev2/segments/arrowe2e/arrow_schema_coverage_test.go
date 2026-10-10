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

// Schema coverage for the Arrow retrieve transport.
//
// TestArrowTransportEndToEndMatchesProtobuf uses a deliberately narrow output
// set -- the types the one-pass materializer writes straight into FieldData --
// because those are what the latency measurements are about. That leaves the
// rest of the type system unverified, including everything that takes the
// gather, and "the fast path is correct" says nothing about those.
//
// These tests close that gap. The bar is the same: byte-identical to the
// protobuf path, because results from both transports can meet in one reduce
// during a rolling config change and the merge indexes columns positionally.
package arrowe2e_test

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func initSegcore(t *testing.T) {
	t.Helper()
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
}

// expectedArrowCols is how many of the fixture's output columns must travel as
// Arrow: every requested field except the ones WorthCarryingAsArrow
// (segment_c.cpp) leaves in the protobuf header -- system fields, ARRAY and
// sparse float vectors.
//
// Deriving it here rather than hand-counting at each call site means a change to
// the exclusion set fails loudly in one place instead of silently weakening
// every differential test.
func expectedArrowCols(t *testing.T, f *fixture) int {
	t.Helper()
	byID := lo.SliceToMap(typeutil.GetAllFieldSchemas(f.schema),
		func(fs *schemapb.FieldSchema) (int64, *schemapb.FieldSchema) {
			return fs.GetFieldID(), fs
		})
	n := 0
	for _, id := range f.outputs {
		if common.IsSystemField(id) {
			continue
		}
		fs, ok := byID[id]
		require.True(t, ok, "output field %d is not in the fixture schema", id)
		switch fs.GetDataType() {
		case schemapb.DataType_Array, schemapb.DataType_SparseFloatVector,
			schemapb.DataType_ArrayOfVector:
			continue
		}
		n++
	}
	return n
}

// compareBothTransports runs the same query with the transport off and on and
// requires the two responses to be byte-identical, column by column.
//
// The Arrow COLUMN COUNT is asserted, not merely that a selection exists. A
// regression that stopped moving columns out of the protobuf header would still
// produce a (zero-column) record and a non-empty selection, and every
// comparison below would pass byte-for-byte because both arms would be the
// protobuf path. Counting columns is what makes this non-vacuous.
func compareBothTransports(t *testing.T, f *fixture, node *planpb.PlanNode, wantArrow bool) {
	t.Helper()

	setZeroCopy(t, false)
	want, arrowColsOff := f.runOnce(t, node)
	require.Zero(t, arrowColsOff, "transport off must not report an Arrow selection")

	setZeroCopy(t, true)
	got, arrowColsOn := f.runOnce(t, node)
	if wantArrow {
		require.Equal(t, expectedArrowCols(t, f), arrowColsOn,
			"wrong number of columns traveled as Arrow; the comparison would be vacuous")
		require.NotZero(t, arrowColsOn,
			"no column traveled as Arrow; the comparison would be vacuous")
	} else {
		require.Zero(t, arrowColsOn,
			"this shape must not route to Arrow")
	}

	require.Equal(t, len(want.GetFieldsData()), len(got.GetFieldsData()), "column count differs")
	for i := range want.GetFieldsData() {
		w, g := want.GetFieldsData()[i], got.GetFieldsData()[i]
		require.Equal(t, w.GetFieldId(), g.GetFieldId(),
			"column %d: field id differs -- positional merge would misattribute data", i)
		// Dead bytes under a false validity bit are the one documented
		// exception to byte-identity, and they are normalized on BOTH sides
		// rather than skipped -- so valid_data itself, the lengths, and every
		// value at a valid row are still compared strictly. See
		// zeroInvalidRows for why the exception is sound.
		zeroInvalidRows(w)
		zeroInvalidRows(g)
		if !proto.Equal(w, g) {
			t.Errorf("column %d (field %d, %s) differs:\n  want %.300s\n   got %.300s",
				i, w.GetFieldId(), w.GetType(), w.String(), g.String())
		}
	}
	assert.True(t, proto.Equal(want.GetIds(), got.GetIds()), "ids differ")
	assert.Equal(t, want.GetAllRetrieveCount(), got.GetAllRetrieveCount())
	assert.Equal(t, want.GetHasMoreResult(), got.GetHasMoreResult())
}

// TestArrowTransportEndToEndAllTypes asks for EVERY field of the standard test
// schema, which puts both materialization paths in one query: the straight-line
// writer for bool/int8/int16/int32/float/double and the four dense vector
// types. ARRAY and sparse stay in fields_data, so this also covers the mixed
// case where some columns ride each transport.
func TestArrowTransportEndToEndAllTypes(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"1seg", 1, 1000, 300},
		{"4seg", 4, 500, 300},
		{"8seg", 8, 300, 240},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("arrow-all", schemapb.DataType_Int64, true)
			outputs := allFieldsOf(schema)
			f := newFixtureWith(t, schema, outputs, c.numSegments, c.rowsPerSeg)
			defer f.release()

			node := requeryPlanNodeFor(
				spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
			compareBothTransports(t, f, node, true)
		})
	}
}

// TestArrowTransportEndToEndNullableVector covers the nullable path, which the
// materializer deliberately declines: a nullable column's protobuf payload is
// COMPACTED to the valid rows only, so its logical and physical indices
// diverge and a straight-line copy would read the wrong rows. It must fall
// back to the gather, and the result must still match protobuf exactly --
// including valid_data, which is reconciled from the export's own record of
// which columns had a bitmap rather than guessed from schema nullability.
func TestArrowTransportEndToEndNullableVector(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"1seg", 1, 1200, 300},
		{"4seg", 4, 600, 300},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchemaWithNullableVec("arrow-nullable", schemapb.DataType_Int64)
			outputs := allFieldsOf(schema)
			f := newFixtureWith(t, schema, outputs, c.numSegments, c.rowsPerSeg)
			defer f.release()

			node := requeryPlanNodeFor(
				spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
			compareBothTransports(t, f, node, true)
		})
	}
}

// TestArrowTransportEndToEndVarCharPK covers VARCHAR, whose protobuf payload is
// a slice of separate strings rather than one flat buffer, so it has no
// straight-line writer and takes the gather. Using it as the PK also drives the
// reduce's PK comparison down its string branch.
func TestArrowTransportEndToEndVarCharPK(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"1seg", 1, 1000, 300},
		{"4seg", 4, 500, 300},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("arrow-varchar", schemapb.DataType_VarChar, true)
			outputs := allFieldsOf(schema)
			f := newFixtureWith(t, schema, outputs, c.numSegments, c.rowsPerSeg)
			defer f.release()

			// A VarChar PK needs a string TermExpr, and GenInsertData numbers
			// VarChar PKs as the decimal form of the row index, so the hit list
			// is the same spread rendered as strings.
			node := varCharRequeryPlanNode(
				spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
			compareBothTransports(t, f, node, true)
		})
	}
}

// TestArrowTransportEndToEndSubsetSelection covers the case the lazy-selection
// design exists for and that every other differential test misses: a selection
// that is a STRICT SUBSET of the rows the segments retrieved.
//
// All the other cases give each segment a disjoint PK range and an Unlimited
// request limit, so the reduce neither truncates nor dedups and selected ==
// retrieved. That is the one shape where "report a selection" and "merge the
// columns" cannot diverge -- so it is the shape that proves the least.
//
// Both ways of becoming a strict subset are covered:
//
//	truncate  a finite request limit, so the reduce keeps fewer rows than it saw
//	dedup     overlapping PKs across segments, so duplicates are discarded
//
// For dedup the per-segment timestamps must differ, or the winner is arbitrary
// and the comparison means nothing (see TestFixtureIsDeterministic).
//
// A strict subset is necessary but NOT sufficient: it also has to span more than
// one record. perPKWinner is what produces that (see the case list).
//
// What it cannot produce -- and what no fixture can -- is a walk that goes
// BACKWARDS within a record. segcore returns each segment's retrieve output in
// PK order (SegmentInterface.cpp:585: "the reduce phase depends on the ids to do
// merge-sort") and the reduce is a k-way merge over those, so rowIdx is
// ascending within every record by construction. Numbering a segment's PKs
// against insert order was tried and changes nothing, because the record is
// PK-sorted either way. So this is an invariant of the path rather than a
// coverage gap, and a materializer that relied on it would be correct.
//
// This is not hypothetical coverage. A review found that SparseFloatArray.Dim
// was computed from the SELECTED rows on the Arrow path but from each
// contributing segment's declared dim on the protobuf path -- identical
// whenever selected == retrieved, and divergent here. Nothing in the suite
// could see it.
func TestArrowTransportEndToEndSubsetSelection(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	// A finite request limit with MORE THAN ONE segment turns ignore_non_pk on
	// (shouldEnableIgnoreNonPk: !hasGroupBy && segmentNum > 1 && Limit !=
	// Unlimited), and ignore_non_pk is excluded from the Arrow routing. So
	// truncation is only reachable on THIS path with a single segment, which
	// is why there is no multi-segment truncation case here --
	// TestIgnoreNonPkRoutingUnderLimit records that routing fact instead.
	//
	// wantArrow is stated per case rather than assumed: getting it backwards is
	// how a differential test goes vacuous.
	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
		opts        fixtureOpts
		wantArrow   bool
	}{
		{"truncate_1seg", 1, 1000, 400, fixtureOpts{limit: 50}, true},
		{"dedup_4seg", 4, 500, 300, fixtureOpts{overlapPKs: true}, true},
		{"dedup_8seg", 8, 300, 240, fixtureOpts{overlapPKs: true}, true},
		{"dedup_1seg_truncate", 1, 1000, 400, fixtureOpts{limit: 40, overlapPKs: true}, true},
		// Measured shapes of the four cases above (records touched / selection
		// walk): 1/prefix, 1/identity, 1/identity, 1/prefix. With ONE timestamp
		// per segment the highest-numbered segment wins every PK, so the "dedup"
		// cases do not in fact span records -- they select all of one record.
		// They exercise the row arithmetic on the easiest possible walk.
		//
		// perPKWinner makes the winner vary by PK, which is what actually spans
		// records: measured 4 records touched with 299 record switches over 300
		// rows, and 8 with 239 over 240 -- a fully interleaved walk where each
		// record contributes a strided strict subset. That is the shape the lazy
		// selection exists for, and the shape under which a bug in
		// selectedSources' per-row record indexing shows up.
		{
			"interleaved_4seg", 4, 500, 300,
			fixtureOpts{overlapPKs: true, perPKWinner: true},
			true,
		},
		{
			"interleaved_8seg", 8, 300, 240,
			fixtureOpts{overlapPKs: true, perPKWinner: true},
			true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("arrow-subset", schemapb.DataType_Int64, true)
			outputs := allFieldsOf(schema)
			// With overlapping PKs every segment holds the same range, so the
			// hit list must stay inside one segment's range to match anything.
			spread := c.numSegments * c.rowsPerSeg
			if c.opts.overlapPKs {
				spread = c.rowsPerSeg
			}
			f := newFixtureOpts(t, schema, outputs, c.numSegments, c.rowsPerSeg, c.opts)
			defer f.release()

			node := requeryPlanNodeFor(spreadPKs(c.hits, spread), outputs)
			compareBothTransports(t, f, node, c.wantArrow)
		})
	}
}

// TestArrowTransportZeroColumnBatchDoesNotCrash drives a request with NO output
// fields, which makes the export build and hand over a RecordBatch of zero
// columns.
//
// What it does NOT test, despite an earlier name and comment here saying so, is
// count(*). The proxy compiles count(*) into an aggregate
// (dql/task_query.go:658), so shouldUseArrowTransport's hasAggregation check
// excludes it before any of this runs -- a real count never reaches the Arrow
// path, and planpb's IsCount is not read anywhere in the core, so
// countPlanNode() below is just an unfiltered retrieve with no outputs.
//
// It is kept because the zero-column batch is still a real shape the C++ side
// exports and arrow-go's cdata import must accept, and a crash there would be a
// QueryNode crash. Both arms return empty FieldsData, so the byte-identity
// comparison is trivially satisfied -- that is the honest scope: it is a
// does-not-crash test, not a correctness differential.
func TestArrowTransportZeroColumnBatchDoesNotCrash(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	schema := mock_segcore.GenTestCollectionSchema("arrow-count", schemapb.DataType_Int64, true)
	f := newFixtureWith(t, schema, []int64{}, 4, 500)
	defer f.release()

	node := countPlanNode()
	// wantArrow is false: with no output fields there is no user column for a
	// selection to carry.
	compareBothTransports(t, f, node, false)
}

// TestIgnoreNonPkRoutingUnderLimit pins the routing fact that keeps the
// subset-selection test above honest: a finite limit across several segments
// goes to ignore_non_pk, not to the Arrow retrieve transport.
//
// It asserts routing only, deliberately. Comparing the two configs byte-for-
// byte here would not test this change at all: ignore_non_pk reads the SAME
// common.interface.zeroCopy flag (ignore_non_pk_ops.go:216) to pick between
// fetchFieldsArrow and fetchFieldsProto, so both arms would be exercising
// #52973's pre-existing code. (Doing so shows those two produce identical data
// but disagree on FieldData.FieldName, which fetchFieldsArrow populates and
// fetchFieldsProto leaves for the proxy to fill. Benign downstream, but it is
// not this change's to assert on.)
func TestIgnoreNonPkRoutingUnderLimit(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	schema := mock_segcore.GenTestCollectionSchema("arrow-inp-route", schemapb.DataType_Int64, true)
	outputs := allFieldsOf(schema)
	f := newFixtureOpts(t, schema, outputs, 4, 500, fixtureOpts{limit: 50})
	defer f.release()

	node := requeryPlanNodeFor(spreadPKs(400, 2000), outputs)
	setZeroCopy(t, true)
	res, arrowCols := f.runOnce(t, node)
	require.Zero(t, arrowCols,
		"a finite limit over 4 segments must route to ignore_non_pk, not the Arrow transport")
	// Without this the test would also pass when the query matched nothing: a
	// zero-row result nils the selection too, so "did not use Arrow" would be
	// true for the wrong reason.
	require.NotEmpty(t, res.GetFieldsData(),
		"query returned no columns; the routing assertion above would pass vacuously")
	require.EqualValues(t, 50, typeutil.GetSizeOfIDs(res.GetIds()),
		"expected the limit to truncate to 50 rows")
}

// nullableScalarSchema marks scalar fields nullable, which the shared helpers
// do not cover: GenTestCollectionSchemaWithNullableVec marks only the float
// vector.
//
// Scalars and vectors use OPPOSITE null conventions in segcore --
// MergeDataArray's scalar branch indexes logically (src_offset per row) while
// its vector branch indexes the compacted payload (getValidDataOffset) -- so
// nullable scalars are a distinct path from the nullable vector covered above,
// and one the Arrow builders treat differently (BuildFixedWidthArray /
// BuildVarLenArray emit AppendNull, discarding whatever segcore stored under
// the false validity bit, where the protobuf merge copies it through).
func nullableScalarSchema(name string) *schemapb.CollectionSchema {
	schema := mock_segcore.GenTestCollectionSchema(name, schemapb.DataType_Int64, true)
	for _, f := range schema.GetFields() {
		switch f.GetDataType() {
		case schemapb.DataType_Int32, schemapb.DataType_Double, schemapb.DataType_JSON:
			f.Nullable = true
		}
	}
	return schema
}

// TestArrowTransportEndToEndNullableScalar is the nullable-scalar counterpart of
// TestArrowTransportEndToEndNullableVector. It exists because the two use
// opposite null conventions, so covering one says nothing about the other.
func TestArrowTransportEndToEndNullableScalar(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"1seg", 1, 1200, 300},
		{"4seg", 4, 600, 300},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := nullableScalarSchema("arrow-nullable-scalar")
			outputs := allFieldsOf(schema)
			f := newFixtureWith(t, schema, outputs, c.numSegments, c.rowsPerSeg)
			defer f.release()

			node := requeryPlanNodeFor(
				spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
			compareBothTransports(t, f, node, true)
		})
	}
}

// zeroInvalidRows clears the payload at rows whose validity bit is false.
//
// This is the ONE place the two transports are not byte-identical, and it is
// confined to bytes nothing reads. segcore stores a value for a null scalar row
// (MergeDataArray's scalar branch walks src_offset per row and copies
// unconditionally), the protobuf path carries it through, and Arrow's builders
// call AppendNull, which writes a zero. Nullable VECTORS do not diverge --
// their payload is compacted to the valid rows on both paths.
//
// Why normalizing is sound rather than hiding a defect: the byte-identical bar
// exists because results from both transports can meet in one delegator reduce
// that merges columns POSITIONALLY, so a difference in length, in column order,
// or in valid_data would misalign data. None of those differ here -- only the
// value beneath a false bit, which AppendFieldData copies along with the false
// bit and no SDK ever surfaces. The design doc records this exception.
func zeroInvalidRows(fd *schemapb.FieldData) {
	valid := typeutil.GetFieldDataValidData(fd)
	if len(valid) == 0 {
		return
	}
	sc := fd.GetScalars()
	if sc == nil {
		// Vectors compact their payload on both paths; there is nothing to
		// normalize, and zeroing by logical index would be wrong.
		return
	}
	for i, ok := range valid {
		if ok {
			continue
		}
		switch d := sc.GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			if i < len(d.BoolData.Data) {
				d.BoolData.Data[i] = false
			}
		case *schemapb.ScalarField_IntData:
			if i < len(d.IntData.Data) {
				d.IntData.Data[i] = 0
			}
		case *schemapb.ScalarField_LongData:
			if i < len(d.LongData.Data) {
				d.LongData.Data[i] = 0
			}
		case *schemapb.ScalarField_FloatData:
			if i < len(d.FloatData.Data) {
				d.FloatData.Data[i] = 0
			}
		case *schemapb.ScalarField_DoubleData:
			if i < len(d.DoubleData.Data) {
				d.DoubleData.Data[i] = 0
			}
		case *schemapb.ScalarField_StringData:
			if i < len(d.StringData.Data) {
				d.StringData.Data[i] = ""
			}
		case *schemapb.ScalarField_JsonData:
			if i < len(d.JsonData.Data) {
				d.JsonData.Data[i] = nil
			}
		}
	}
}

// TestArrowTransportEndToEndSealed is the same byte-identity bar on SEALED
// segments, which is where production data actually lives and which every other
// test in this package misses.
//
// SCOPE, measured rather than assumed: these cases cover sealed segments served
// by bulk_subscript. They do NOT cover ArrowToDataArray, segcore's take()
// output builder -- a take reader is null without a storage-v2 manifest, which
// no Go test helper builds, so segcore logs
// "[TakeAPI] retrieve fallback to bulk_subscript" and the take cases collapse
// onto the same producer. Covering it needs a storage-v2 fixture; see the
// design doc's follow-ups.
//
// It is a different producer, not a different configuration. FillTargetEntry
// tries TryTakeForRetrieve first for a ChunkedSegmentSealedImpl
// (SegmentInterface.cpp), so the DataArrays the Arrow export aliases are built
// by ArrowToDataArray instead of CreateScalarDataArrayFrom /
// CreateVectorDataArrayFrom. Three conventions have to match or the query fails
// outright, since RetrieveArrow has no fallback:
//
//   - valid_data must be present for exactly the nullable fields, or
//     ReconcileValidData strips or invents a bitmap;
//   - a nullable VECTOR payload must be compacted to the valid rows, because
//     BuildDenseVectorArray's nullable branch indexes it physically;
//   - the payload oneof must be set, or WorthCarryingAsArrow carries a column
//     FieldDataToArrow cannot build.
//
// The fixture stamps the rows identically for both segment types and only
// changes how they are written, so a failure here is a producer disagreement
// rather than a data difference.
//
// Needs a reachable object store (sealed loads from binlogs). It is skipped
// where there is none rather than failing, so the rest of the package stays
// runnable; CI has one.
func TestArrowTransportEndToEndSealed(t *testing.T) {
	initSegcore(t)
	defer setZeroCopy(t, false)

	for _, c := range []struct {
		name        string
		schema      func(string) *schemapb.CollectionSchema
		numSegments int
		rowsPerSeg  int
		hits        int
		useTake     bool
	}{
		// Every column type at once, including the ones that stay on the
		// protobuf path, so the mixed-transport assembly is covered on sealed.
		{"all_types_1seg", func(n string) *schemapb.CollectionSchema {
			return mock_segcore.GenTestCollectionSchema(n, schemapb.DataType_Int64, true)
		}, 1, 1000, 300, false},
		{"all_types_4seg", func(n string) *schemapb.CollectionSchema {
			return mock_segcore.GenTestCollectionSchema(n, schemapb.DataType_Int64, true)
		}, 4, 500, 300, false},
		// The compaction convention: a nullable vector's payload holds only the
		// valid rows on both producers, or the physical indexing is wrong.
		{"nullable_vector", func(n string) *schemapb.CollectionSchema {
			return mock_segcore.GenTestCollectionSchemaWithNullableVec(n, schemapb.DataType_Int64)
		}, 2, 600, 300, false},
		// The logical-indexing convention, which is the opposite of the above.
		{"nullable_scalar", nullableScalarSchema, 2, 600, 300, false},

		// UseTakeForOutput on, which is the default for external collections.
		// With storage v1 binlogs the take reader is null, so segcore logs
		// "[TakeAPI] retrieve fallback to bulk_subscript" and answers the
		// ordinary way -- that fallback is itself worth pinning, because it is
		// the configuration an external collection actually runs in.
		//
		// It does NOT reach ArrowToDataArray: that needs a non-null take
		// reader, i.e. a storage-v2 segment with a manifest, which no Go test
		// helper builds today. Only one case here, because with the reader null
		// every take case collapses onto the same producer as the cases above.
		{"take_enabled_falls_back", func(n string) *schemapb.CollectionSchema {
			return mock_segcore.GenTestCollectionSchema(n, schemapb.DataType_Int64, true)
		}, 2, 600, 300, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			schema := c.schema("arrow-sealed-" + c.name)
			outputs := allFieldsOf(schema)
			f := newFixtureOpts(t, schema, outputs, c.numSegments, c.rowsPerSeg,
				fixtureOpts{sealed: true, useTake: c.useTake})
			defer f.release()

			node := requeryPlanNodeFor(
				spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
			compareBothTransports(t, f, node, true)
		})
	}
}
