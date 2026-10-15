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

// End-to-end differential test for the Arrow retrieve transport.
//
// It drives the whole QueryNode-side chain the change touches:
//
//	segments.Retrieve         routing, per-segment CGO, the Arrow record
//	RunQNQueryPipeline        the cross-segment reduce, which on the Arrow path
//	                          reports a row selection instead of merging columns
//	MaterializeArrowSelection the one payload pass, at the RPC boundary
//
// and asserts the result is byte-identical to the same query with the transport
// switched off. Byte-identical is the right bar: results from both paths can meet
// in one delegator reduce during a rolling config change, and buildMergedFieldData
// indexes columns positionally, so a difference in order or in valid_data is
// silent corruption rather than a visible error.
//
// Why a separate package: internal/querynodev2/segments has test files importing
// bytedance/mockey, which does not build on Go 1.27, making that package's test
// binary unbuildable locally. Everything used here is exported.
package arrowe2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	collID    = 920
	partID    = 92
	segBase   = 9200
	pkFieldID = 109
)

// 109 int64 PK, 101 int8, 104 float, 107 128-dim floatVector, 1 Timestamp.
//
// Deliberately a narrow subset: these are the types the one-pass materializer
// writes straight into FieldData, so they are what the latency numbers measure.
// TestArrowTransportEndToEndAllTypes covers the rest, including every type that
// falls back to the gather.
var outputFields = []int64{109, 101, 104, 107, 1}

// allFieldsOf asks for every user field of a schema plus the Timestamp system
// column, which is the set that exercises both materialization paths at once:
// the straight-line writer for the fixed-width and dense-vector columns, and
// the gather, which only a column containing a NULL now reaches.
//
// Derived from the schema rather than hardcoded because the schema variants
// differ -- GenTestCollectionSchemaWithNullableVec has no sparse field, for
// instance -- and naming a field a schema lacks does not fail the query, it
// ABORTS THE PROCESS: the segcore assert throws inside the retrieve future,
// where nothing catches it (see the note in AsyncRetrieveAsArrow).
func allFieldsOf(schema *schemapb.CollectionSchema) []int64 {
	ids := make([]int64, 0, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		if common.IsSystemField(f.GetFieldID()) {
			continue
		}
		ids = append(ids, f.GetFieldID())
	}
	return append(ids, common.TimeStampField)
}

type fixture struct {
	manager *segments.Manager
	schema  *schemapb.CollectionSchema
	outputs []int64
	segs    []segments.Segment
	// limit, when non-zero, overrides the request's Unlimited so the reduce
	// truncates. See request.
	limit int64
	// overlapPKs keeps every segment's PKs on the SAME range instead of
	// shifting them apart, so the reduce dedups. Paired with distinct
	// per-segment timestamps the winner stays deterministic.
	overlapPKs bool
	// sealed records which segment type was built, because the request's data
	// scope has to match it -- GetAndPin resolves Streaming against growing and
	// Historical against sealed, and a mismatch reports the segment as not
	// loaded rather than as a scope error.
	sealed bool
}

func newFixture(t *testing.T, numSegments, rowsPerSegment int) *fixture {
	return newFixtureWith(t,
		mock_segcore.GenTestCollectionSchema("arrow-e2e", schemapb.DataType_Int64, true),
		outputFields, numSegments, rowsPerSegment)
}

// fixtureOpts are the knobs only the subset-selection tests need. The zero
// value is the ordinary requery shape: disjoint PKs, no limit.
type fixtureOpts struct {
	// limit overrides the request's Unlimited so the reduce truncates, making
	// the selection a strict subset of what the segments retrieved.
	limit int64
	// overlapPKs puts every segment's PKs on the same range so the reduce
	// dedups, which is the other way the selection becomes a strict subset.
	overlapPKs bool
	// sealed loads each segment as SEALED from binlogs instead of inserting it
	// as GROWING.
	//
	// This is not a cosmetic variation. A sealed segment builds its DataArray
	// through a DIFFERENT producer: FillTargetEntry tries TryTakeForRetrieve
	// first for ChunkedSegmentSealedImpl (SegmentInterface.cpp), so the columns
	// the Arrow export aliases come from ArrowToDataArray rather than from
	// CreateScalarDataArrayFrom / CreateVectorDataArrayFrom. The two must agree
	// on nullable valid_data, nullable-vector compaction and which payload oneof
	// is set, or WorthCarryingAsArrow retains a column the exporter cannot build
	// and the query fails -- RetrieveArrow has no fallback.
	//
	// The data is stamped identically to the growing path before being written,
	// so segment type is the only variable between the two arms.
	sealed bool
	// useTake, with sealed, turns on the take() output path
	// (SegmentLoadInfo.UseTakeForOutput), which is what actually routes the
	// columns through ArrowToDataArray.
	//
	// Without it a sealed segment still answers via bulk_subscript, so the
	// sealed cases would cover a different producer than the one worth worrying
	// about. External collections default this ON
	// (ExternalCollectionUseTakeForOutput), so it is a reachable production
	// configuration, not a synthetic one.
	useTake bool
	// perPKWinner, with overlapPKs, makes the dedup winner vary BY PK instead of
	// one segment winning everything: PK j is won by segment j%numSegments.
	//
	// This is the shape the lazy selection exists for and the only one here that
	// produces it: sel.Rows interleaves across every record round-robin, and
	// each record contributes a strided strict subset. With one timestamp per
	// segment the highest-numbered segment wins every PK, so the selection
	// collapses to all-of-one-record in identity order.
	perPKWinner bool
}

func newFixtureWith(
	t *testing.T,
	schema *schemapb.CollectionSchema,
	outputs []int64,
	numSegments, rowsPerSegment int,
) *fixture {
	return newFixtureOpts(t, schema, outputs, numSegments, rowsPerSegment, fixtureOpts{})
}

func newFixtureOpts(
	t *testing.T,
	schema *schemapb.CollectionSchema,
	outputs []int64,
	numSegments, rowsPerSegment int,
	opts fixtureOpts,
) *fixture {
	t.Helper()
	ctx := context.Background()

	mgr := segments.NewManager()
	mgr.Collection.PutOrRef(collID, schema,
		mock_segcore.GenTestIndexMeta(collID, schema),
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: collID,
			PartitionIDs: []int64{partID},
		})
	coll := mgr.Collection.Get(collID)
	require.NotNil(t, coll)

	f := &fixture{
		manager: mgr, schema: schema, outputs: outputs,
		limit: opts.limit, overlapPKs: opts.overlapPKs, sealed: opts.sealed,
	}

	// Only sealed segments need the remote chunk manager, and initializing it
	// is not free -- it constructs an S3 client. Keep it off the growing path
	// so those tests stay runnable without an object store.
	var chunkManager storage.ChunkManager
	segType := segments.SegmentTypeGrowing
	if opts.sealed {
		segType = segments.SegmentTypeSealed
		chunkManager = initSealedStorage(t)
	}

	for i := 0; i < numSegments; i++ {
		segID := int64(segBase + i)
		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:     segID,
			CollectionID:  collID,
			PartitionID:   partID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collID),
			Level:         datapb.SegmentLevel_Legacy,
		}
		if opts.sealed {
			loadInfo.NumOfRows = int64(rowsPerSegment)
			loadInfo.UseTakeForOutput = opts.useTake
		}
		seg, err := segments.NewSegment(ctx, coll, mgr.Segment, segType, 0, loadInfo)
		require.NoError(t, err)

		insertMsg, err := mock_segcore.GenInsertMsg(coll.GetCCollection(), partID, segID, rowsPerSegment)
		require.NoError(t, err)

		// GenInsertMsg numbers PKs 0..rows-1 in EVERY segment, which makes the
		// reduce's dedup winner arbitrary when the same PK exists in several
		// segments with equal timestamps -- the protobuf path then differs from
		// itself run to run and the differential comparison below means nothing.
		// TestFixtureIsDeterministic guards this. Shift each segment's PKs into
		// its own range so every PK lives in exactly one segment, which is also
		// what a real collection looks like.
		if f.overlapPKs {
			// Same PK range in every segment, so the reduce has duplicates to
			// resolve. The timestamps must then differ per segment or the
			// winner is arbitrary and a differential comparison is meaningless
			// -- see TestFixtureIsDeterministic.
			shiftPrimaryKeys(t, insertMsg.FieldsData, 0)
			for j := range insertMsg.Timestamps {
				if opts.perPKWinner {
					// Segment pk%numSegments wins pk, every other segment
					// loses it. The 2000 band beats the whole 1000 band, so
					// for each PK exactly one segment holds the maximum and
					// the winner is still deterministic.
					if j%numSegments == i {
						insertMsg.Timestamps[j] = uint64(2000 + i)
					} else {
						insertMsg.Timestamps[j] = uint64(1000 + i)
					}
					continue
				}
				insertMsg.Timestamps[j] = uint64(1000 + i)
			}
		} else {
			shiftPrimaryKeys(t, insertMsg.FieldsData, int64(i*rowsPerSegment))
		}
		stampNullableValidData(t, schema, insertMsg.FieldsData, rowsPerSegment)
		stampArrayColumns(t, schema, insertMsg.FieldsData, int64(i*rowsPerSegment))

		if opts.sealed {
			// Same stamped rows as the growing arm, written as binlogs and
			// loaded back, so the only difference is the producer segcore uses.
			insertData, err := storage.InsertMsgToInsertData(insertMsg, schema)
			require.NoError(t, err)
			binlogs, _, err := mock_segcore.SaveBinLogWithData(ctx,
				collID, partID, segID, insertData, schema, chunkManager)
			require.NoError(t, err)

			local, ok := seg.(*segments.LocalSegment)
			require.True(t, ok, "sealed fixture needs a LocalSegment to load into")
			g, err := local.StartLoadData()
			require.NoError(t, err)
			for _, binlog := range binlogs {
				require.NoError(t, local.LoadFieldData(
					ctx, binlog.FieldID, int64(rowsPerSegment), binlog))
			}
			g.Done(nil)
		} else {
			rec, _, err := storage.TransferInsertMsgToInsertRecord(schema, insertMsg)
			require.NoError(t, err)
			require.NoError(t, seg.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, rec))
		}

		mgr.Segment.Put(ctx, segType, seg)
		f.segs = append(f.segs, seg)
	}
	return f
}

// initSealedStorage points the chunk manager at LOCAL storage and returns it.
//
// Sealed segments load from binlogs, so they need a chunk manager; growing
// segments need none. Using local storage rather than MinIO is deliberate: where
// the binlogs live is orthogonal to what this test checks (which producer builds
// the DataArray that the Arrow export then aliases), and it keeps the sealed
// coverage runnable with no object store on either side of the cgo boundary --
// initcore.InitRemoteChunkManager forwards storage_type, so "local" gives C++ a
// LocalChunkManager and never constructs an S3 client.
func initSealedStorage(t *testing.T) storage.ChunkManager {
	t.Helper()
	ctx := context.Background()
	pt := paramtable.Get()

	root := t.TempDir()
	for key, val := range map[string]string{
		pt.CommonCfg.StorageType.Key: "local",
		pt.LocalStorageCfg.Path.Key:  root,
	} {
		prev := pt.Save(key, val)
		_ = prev
		k := key
		t.Cleanup(func() { _ = pt.Reset(k) })
	}
	require.Equal(t, "local", pt.CommonCfg.StorageType.GetValue(),
		"storage type must be local, or the sealed fixture needs an object store")

	factory := storage.NewChunkManagerFactoryWithParam(pt)
	cm, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	require.NoError(t, initcore.InitRemoteChunkManager(pt))
	return cm
}

// varCharPK renders a VarChar primary key. GenerateStringArray produces random
// sentences that are neither unique nor predictable, so a VarChar-PK fixture
// has to overwrite the column: duplicate PKs make the reduce's dedup winner
// arbitrary, which makes a differential comparison meaningless (see
// TestFixtureIsDeterministic). Zero padding keeps the lexical order of the
// strings equal to the numeric order of their indices, which the reduce's
// k-way merge over sorted inputs relies on.
func varCharPK(i int64) string {
	return fmt.Sprintf("pk-%08d", i)
}

// shiftPrimaryKeys rebases the PK column onto [base, base+rows), for either PK
// type, so every PK lives in exactly one segment -- which is both what a real
// collection looks like and what makes the dedup winner well defined.
// stampArrayColumns makes each ARRAY row's contents depend on the row.
//
// testutils.GenerateArrayOfIntArray fills EVERY row with GenerateInt32Array's
// [0,1,..,n-1], so as generated the column is byte-identical in every row of
// every segment -- a wrong-row bug in it is invisible. The ARRAY column is also
// the one output field that stays on the protobuf path
// (WorthCarryingAsArrow excludes it), which is exactly the half of the
// all-types test the differential comparison is supposed to be watching.
//
// Overwritten here rather than in the shared generator, which three other call
// sites depend on.
func stampArrayColumns(t *testing.T, schema *schemapb.CollectionSchema,
	fields []*schemapb.FieldData, base int64,
) {
	t.Helper()
	arrayFields := make(map[int64]struct{})
	for _, fs := range typeutil.GetAllFieldSchemas(schema) {
		if fs.GetDataType() == schemapb.DataType_Array {
			arrayFields[fs.GetFieldID()] = struct{}{}
		}
	}
	stamped := 0
	for _, fd := range fields {
		if _, ok := arrayFields[fd.GetFieldId()]; !ok {
			continue
		}
		rows := fd.GetScalars().GetArrayData().GetData()
		for r, row := range rows {
			ints := row.GetIntData().GetData()
			require.NotEmpty(t, ints,
				"array row %d of field %d has no int payload; the generator's "+
					"shape changed and this stamping is silently doing nothing",
				r, fd.GetFieldId())
			for k := range ints {
				ints[k] = int32(base) + int32(r*len(ints)+k)
			}
		}
		if len(rows) > 1 {
			require.NotEqual(t,
				rows[0].GetIntData().GetData(), rows[1].GetIntData().GetData(),
				"consecutive array rows must differ, or a wrong-row bug in this "+
					"column is invisible to the differential comparison")
		}
		stamped++
	}
	require.Equal(t, len(arrayFields), stamped,
		"every ARRAY field in the schema must be stamped")
}

func shiftPrimaryKeys(t *testing.T, fields []*schemapb.FieldData, base int64) {
	t.Helper()
	for _, fd := range fields {
		if fd.GetFieldId() != pkFieldID {
			continue
		}
		if data := fd.GetScalars().GetLongData().GetData(); len(data) > 0 {
			for i := range data {
				data[i] = base + int64(i)
			}
			return
		}
		data := fd.GetScalars().GetStringData().GetData()
		require.NotEmpty(t, data, "pk column is neither int64 nor varchar")
		for i := range data {
			data[i] = varCharPK(base + int64(i))
		}
		return
	}
	t.Fatalf("pk field %d not found in insert data", pkFieldID)
}

// stampNullableValidData makes the nullable fields of an insert message
// self-consistent, which GenInsertMsg does not do -- only GenInsertData does.
//
// Two things are required and segcore rejects the insert without either:
//
//	valid_data must be present  ("nullable vector field %s requires valid_data")
//	the payload must be COMPACTED to the valid rows only
//	                            ("has %d valid rows, but compact physical
//	                             payload rows is %d")
//
// That second invariant is the same one the one-pass materializer declines to
// handle: a compacted payload's physical index is not its logical index, so a
// straight-line copy would read the wrong rows. Building the fixture this way
// is what makes this test exercise the gather for real -- a nullable column
// is the only thing that still reaches it.
//
// The null pattern is the shared deterministic one (every third row), because a
// differential comparison needs the null positions to be reproducible.
func stampNullableValidData(
	t *testing.T,
	schema *schemapb.CollectionSchema,
	fields []*schemapb.FieldData,
	rows int,
) {
	t.Helper()
	nullable := make(map[int64]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		if f.GetNullable() {
			nullable[f.GetFieldID()] = f
		}
	}
	if len(nullable) == 0 {
		return
	}
	valid := mock_segcore.NullablePatternValidData(rows)
	stamped := 0
	for _, fd := range fields {
		fs, ok := nullable[fd.GetFieldId()]
		if !ok {
			continue
		}
		compactNullableVector(t, fd, fs, valid)
		typeutil.SetFieldDataValidData(fd, append([]bool(nil), valid...))
		stamped++
	}
	require.Equal(t, len(nullable), stamped,
		"schema declares %d nullable fields but %d were stamped; the insert would be rejected",
		len(nullable), stamped)
}

// compactNullableVector drops the invalid rows from a dense vector payload so
// it holds exactly the valid ones, in order.
func compactNullableVector(
	t *testing.T,
	fd *schemapb.FieldData,
	fs *schemapb.FieldSchema,
	valid []bool,
) {
	t.Helper()
	// Nullable SCALARS are not compacted: MergeDataArray's scalar branch walks
	// src_offset per row, while its vector branch uses getValidDataOffset. So a
	// nullable scalar keeps its full-length payload and needs no compaction.
	if fd.GetVectors() == nil {
		return
	}
	vectors := fd.GetVectors()
	dim := int(vectors.GetDim())

	switch data := vectors.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		src := data.FloatVector.GetData()
		require.Len(t, src, len(valid)*dim)
		data.FloatVector.Data = mock_segcore.CompactFloatVecData(src, valid, dim)
	default:
		t.Fatalf("field %d (%s) is nullable but this helper cannot compact its payload",
			fd.GetFieldId(), fs.GetDataType())
	}
}

// varCharRequeryPlanNode is requeryPlanNodeFor for a VarChar PK: the same spread
// of indices, rendered through varCharPK.
func varCharRequeryPlanNode(pks []int64, outputs []int64) *planpb.PlanNode {
	values := lo.Map(pks, func(id int64, _ int) *planpb.GenericValue {
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{StringVal: varCharPK(id)},
		}
	})
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: &planpb.Expr{Expr: &planpb.Expr_TermExpr{
				TermExpr: &planpb.TermExpr{
					ColumnInfo: &planpb.ColumnInfo{
						FieldId: pkFieldID, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true,
					},
					Values: values,
				},
			}},
			Limit: int64(len(values)),
		}},
		OutputFieldIds: outputs,
	}
}

func (f *fixture) release() {
	for _, s := range f.segs {
		f.manager.Segment.Remove(context.Background(), s.ID(), querypb.DataScope_Streaming)
	}
}

// requeryPlanNode mirrors planparserv2.CreateRequeryPlan: a PK TermExpr with
// IsPrimaryKey set and the PLAN limit equal to len(values).
func requeryPlanNode(pks []int64) *planpb.PlanNode {
	return requeryPlanNodeFor(pks, outputFields)
}

func requeryPlanNodeFor(pks []int64, outputs []int64) *planpb.PlanNode {
	values := lo.Map(pks, func(id int64, _ int) *planpb.GenericValue {
		return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: id}}
	})
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: &planpb.Expr{Expr: &planpb.Expr_TermExpr{
				TermExpr: &planpb.TermExpr{
					ColumnInfo: &planpb.ColumnInfo{
						FieldId: pkFieldID, DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
					},
					Values: values,
				},
			}},
			Limit: int64(len(values)),
		}},
		OutputFieldIds: outputs,
	}
}

// request uses an Unlimited REQUEST limit, which is what a real requery carries
// and what keeps ignore_non_pk off so the payload flows through AsyncRetrieve.
// The plan's own Limit is a different field.
func (f *fixture) request(node *planpb.PlanNode) *querypb.QueryRequest {
	expr, _ := proto.Marshal(node)
	// Unlimited is what a real requery carries, and it is also what keeps
	// ignore_non_pk off so the payload flows through AsyncRetrieve. A finite
	// limit is set only by the truncation test, which needs the reduce to keep
	// FEWER rows than the segments retrieved.
	limit := typeutil.Unlimited
	if f.limit != 0 {
		limit = f.limit
	}
	scope := querypb.DataScope_Streaming
	if f.sealed {
		scope = querypb.DataScope_Historical
	}
	return &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:       collID,
			PartitionIDs:       []int64{partID},
			SerializedExprPlan: expr,
			OutputFieldsId:     f.outputs,
			MvccTimestamp:      typeutil.MaxTimestamp,
			Limit:              limit,
		},
		SegmentIDs: lo.Map(f.segs, func(s segments.Segment, _ int) int64 { return s.ID() }),
		// Must match the segment type the fixture built; see fixture.sealed.
		Scope:           scope,
		FromShardLeader: true,
	}
}

func (f *fixture) newPlan(t *testing.T, node *planpb.PlanNode) *segcore.RetrievePlan {
	t.Helper()
	expr, err := proto.Marshal(node)
	require.NoError(t, err)
	plan, err := segcore.NewRetrievePlan(
		f.manager.Collection.Get(collID).GetCCollection(),
		expr, typeutil.MaxTimestamp, 1<<30, 0, 0, 0)
	require.NoError(t, err)
	return plan
}

// runOnce drives the full node-side chain and returns the response exactly as
// query_task.go assembles it, plus whether the Arrow carrier was used.
//
// Mirroring query_task.go rather than calling it matters: the assembly step is
// where MaterializeArrowSelection converts the selection into FieldsData, so a
// test that stopped at RunQNQueryPipeline would not cover it.
func (f *fixture) runOnce(t *testing.T, node *planpb.PlanNode) (*internalpb.RetrieveResults, int) {
	t.Helper()
	ctx := context.Background()
	plan := f.newPlan(t, node)
	defer plan.Delete()
	req := f.request(node)

	results, pinned, err := segments.Retrieve(ctx, f.manager, plan, req)
	require.NoError(t, err)
	defer f.manager.Segment.Unpin(pinned)
	defer segments.ReleaseRecords(results)

	reduceResults := make([]*segcorepb.RetrieveResults, 0, len(results))
	querySegments := make([]segments.Segment, 0, len(results))
	records := make([]arrow.Record, 0, len(results))
	hasArrow := false
	for _, r := range results {
		reduceResults = append(reduceResults, r.Result)
		querySegments = append(querySegments, r.Segment)
		records = append(records, r.Record)
		if r.Record != nil {
			hasArrow = true
		}
	}
	if !hasArrow {
		records = nil
	}

	reduced, selection, err := segments.RunQNQueryPipeline(
		ctx, req, f.schema, node, reduceResults, records, querySegments, f.manager, plan)
	require.NoError(t, err)
	// The column COUNT, not merely "a selection exists", is what makes the
	// differential assertions non-vacuous. The export builds and exports a
	// record even when it carries zero columns, and MaterializeArrowSelection
	// degenerates to a header reorder in that case (0 columns == 0 user ids
	// passes its cardinality guard), so a regression that retained every column
	// in the protobuf header would still produce a non-empty selection and keep
	// every byte-identity comparison green with the transport effectively OFF.
	// Counting columns is what rules that out.
	arrowCols := 0
	if !selection.Empty() {
		arrowCols = int(selection.Template().NumCols())
	}

	// The materialization is part of the node's own response assembly, which is
	// why it is covered here rather than on the delegator.
	require.NoError(t, queryutil.MaterializeArrowSelection(reduced, selection, f.schema))

	out := &internalpb.RetrieveResults{
		Ids:              reduced.GetIds(),
		FieldsData:       reduced.GetFieldsData(),
		AllRetrieveCount: reduced.GetAllRetrieveCount(),
		HasMoreResult:    reduced.GetHasMoreResult(),
		ElementLevel:     reduced.GetElementLevel(),
	}
	return out, arrowCols
}

func TestArrowTransportEndToEndMatchesProtobuf(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	defer setZeroCopy(t, false)

	cases := []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
	}{
		{"1seg", 1, 2000, 500},
		// PKs are disjoint per segment, so a hit list spread across the whole
		// range makes every segment contribute rows and the cross-segment gather
		// actually run -- with hits confined to one segment it would reduce to
		// the 1seg case.
		{"4seg", 4, 1000, 500},
		{"8seg", 8, 500, 400},
		{"zero_hits", 1, 2000, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, c.numSegments, c.rowsPerSeg)
			defer f.release()

			pks := spreadPKs(c.hits, c.numSegments*c.rowsPerSeg)
			node := requeryPlanNode(pks)

			setZeroCopy(t, false)
			want, arrowColsOff := f.runOnce(t, node)
			require.Zero(t, arrowColsOff, "transport off must not report an Arrow selection")

			setZeroCopy(t, true)
			got, arrowColsOn := f.runOnce(t, node)

			if c.hits > 0 {
				// Non-vacuity, and the COUNT is what makes it so: the export
				// emits a record even when it carries no columns, so asserting
				// only "a selection exists" would stay green with every column
				// left in the protobuf header -- i.e. with both arms running the
				// protobuf path and the comparison below testing nothing.
				require.Equal(t, len(f.outputs)-1, arrowColsOn,
					"wrong number of columns traveled as Arrow (outputs minus the "+
						"timestamp system field); the comparison would be vacuous")
			} else {
				require.Zero(t, arrowColsOn, "a zero-hit query selects no rows")
			}

			require.Equal(t, len(want.GetFieldsData()), len(got.GetFieldsData()),
				"column count differs")
			for i := range want.GetFieldsData() {
				w, g := want.GetFieldsData()[i], got.GetFieldsData()[i]
				require.Equal(t, w.GetFieldId(), g.GetFieldId(),
					"column %d: field id differs -- positional merge would misattribute data", i)
				if !proto.Equal(w, g) {
					t.Errorf("column %d (field %d, %s) differs:\n  want %.400s\n   got %.400s",
						i, w.GetFieldId(), w.GetType(), w.String(), g.String())
				}
			}
			assert.True(t, proto.Equal(want.GetIds(), got.GetIds()), "ids differ")
			assert.Equal(t, want.GetAllRetrieveCount(), got.GetAllRetrieveCount())
			assert.Equal(t, want.GetHasMoreResult(), got.GetHasMoreResult())
		})
	}
}

// spreadPKs picks n primary keys spaced across [0, total) so the hits land in
// every segment rather than clustering in the first one.
func spreadPKs(n, total int) []int64 {
	if n == 0 {
		return nil
	}
	step := total / n
	if step < 1 {
		step = 1
	}
	pks := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		pks = append(pks, int64(i*step))
	}
	return pks
}

func setZeroCopy(t *testing.T, on bool) {
	t.Helper()
	pt := paramtable.Get()
	val := "false"
	if on {
		val = "true"
	}
	require.NoError(t, pt.Save(pt.CommonCfg.InterfaceZeroCopyEnabled.Key, val))
	require.Equal(t, on, pt.CommonCfg.InterfaceZeroCopyEnabled.GetAsBool())
}

// countPlanNode is a count(*) plan: no predicate, IsCount set, no output
// fields. Its single column carries field id 0 and is identified positionally.
func countPlanNode() *planpb.PlanNode {
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			IsCount: true,
			Limit:   0,
		}},
	}
}
