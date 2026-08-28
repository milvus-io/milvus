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

package segments

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// =========================================================================
// Shared fixture: 2 sealed segments, PK/Float/FloatVector output fields.
//
// mock_segcore.GenSimpleRetrievePlan only sets OutputFieldIds to the PK
// field, which is not enough to exercise the Arrow bulk column path (we
// need a scalar and a vector column too). fetchFieldsAsRecord/
// fetchFieldsViaProto are offset-based (segIndices/segOffsets are supplied
// explicitly), so the plan's predicate expr is never evaluated for these
// calls -- only plan.OutputFieldIds (which becomes the C++ side's
// field_ids_, i.e. the Arrow record's column order) matters. We therefore
// build a small retrieve plan here mirroring
// mock_segcore.genSimpleRetrievePlanExpr but with a 3-field OutputFieldIds
// list, and pass the exact same field order as fieldSchemas to
// segcore.ArrowFieldsToProto (matching how query_pipeline.go's
// buildIgnoreNonPkPipeline builds fieldSchemas from OutputFieldsId).
// =========================================================================

// buildArrowTestRetrievePlanExpr mirrors mock_segcore's internal
// genSimpleRetrievePlanExpr, but allows the caller to choose which fields
// are requested as output (OutputFieldIds -> plan.field_ids_ -> the Arrow
// record's column order).
func buildArrowTestRetrievePlanExpr(schema *schemapb.CollectionSchema, outputFieldIDs []int64) ([]byte, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  pkField.FieldID,
							DataType: pkField.DataType,
						},
						Values: []*planpb.GenericValue{
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
						},
					},
				},
			},
		},
		OutputFieldIds: outputFieldIDs,
	}
	return proto.Marshal(planNode)
}

func buildArrowTestRetrievePlan(collection *segcore.CCollection, outputFieldIDs []int64) (*segcore.RetrievePlan, error) {
	exprBytes, err := buildArrowTestRetrievePlanExpr(collection.Schema(), outputFieldIDs)
	if err != nil {
		return nil, err
	}
	return segcore.NewRetrievePlan(collection, exprBytes, typeutil.Timestamp(1000), 100, 0, 0, 0)
}

func mustFindFieldByType(tb testing.TB, schema *schemapb.CollectionSchema, dt schemapb.DataType) *schemapb.FieldSchema {
	tb.Helper()
	for _, f := range schema.GetFields() {
		if f.GetDataType() == dt {
			return f
		}
	}
	tb.Fatalf("schema has no field of type %s", dt)
	return nil
}

type arrowFetchFixture struct {
	ctx          context.Context
	rootPath     string
	chunkManager storage.ChunkManager
	manager      *Manager
	collection   *Collection
	segments     []Segment
	plan         *segcore.RetrievePlan
	fieldSchemas []*schemapb.FieldSchema // in plan.OutputFieldIds order
	merged       *MergedResultWithOffsets
}

// setupArrowFetchFixture loads 2 sealed segments with rowsPerSegment rows
// each (Int64 PK, Float scalar, FloatVector -- dim taken from
// mock_segcore.GenTestCollectionSchema, currently mock_segcore.DefaultDim
// = 128) and builds a MergedResultWithOffsets that selects every row from
// both segments (interleaved by segment), simulating the output of
// Phase 1 (NewMergeByPKWithOffsetsOperator) for the correctness test and
// the benchmarks below.
func setupArrowFetchFixture(tb testing.TB, rowsPerSegment int) *arrowFetchFixture {
	tb.Helper()
	paramtable.Init()

	ctx := context.Background()
	rootPath := tb.Name()

	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), rootPath)
	chunkManager, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	require.NoError(tb, err)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(rootPath)
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	collectionID := int64(90000)
	partitionID := int64(9000)

	manager := NewManager()
	schema := mock_segcore.GenTestCollectionSchema("test-arrow-fetch", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(collectionID, schema)
	err = manager.Collection.PutOrRef(collectionID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: collectionID,
		PartitionIDs: []int64{partitionID},
	})
	require.NoError(tb, err)
	collection := manager.Collection.Get(collectionID)
	loader := NewLoader(ctx, manager, chunkManager)

	segments := make([]Segment, 0, 2)
	for i := range 2 {
		segmentID := int64(90100 + i)
		binlogs, statslogs, err := mock_segcore.SaveBinLog(ctx,
			collectionID, partitionID, segmentID, rowsPerSegment, schema, chunkManager)
		require.NoError(tb, err)

		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			NumOfRows:     int64(rowsPerSegment),
			BinlogPaths:   binlogs,
			Statslogs:     statslogs,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		}

		seg, err := NewSegment(ctx, collection, manager.Segment, SegmentTypeSealed, 0, loadInfo)
		require.NoError(tb, err)

		bfs, err := loader.loadSingleBloomFilterSet(ctx, collectionID, loadInfo, SegmentTypeSealed)
		require.NoError(tb, err)
		seg.SetPKCandidate(bfs)

		localSeg, ok := seg.(*LocalSegment)
		require.True(tb, ok, "sealed segment must be *LocalSegment for the Arrow retrieve path")
		for _, binlog := range binlogs {
			require.NoError(tb, localSeg.LoadFieldData(ctx, binlog.FieldID, int64(rowsPerSegment), binlog))
		}

		manager.Segment.Put(ctx, SegmentTypeSealed, seg)
		segments = append(segments, seg)
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	require.NoError(tb, err)
	floatField := mustFindFieldByType(tb, schema, schemapb.DataType_Float)
	floatVecField := mustFindFieldByType(tb, schema, schemapb.DataType_FloatVector)

	fieldSchemas := []*schemapb.FieldSchema{pkField, floatField, floatVecField}
	outputFieldIDs := []int64{pkField.GetFieldID(), floatField.GetFieldID(), floatVecField.GetFieldID()}

	plan, err := buildArrowTestRetrievePlan(collection.GetCCollection(), outputFieldIDs)
	require.NoError(tb, err)

	selections := make([]OffsetSelection, 0, rowsPerSegment*len(segments))
	ids := make([]int64, 0, rowsPerSegment*len(segments))
	var pk int64
	for segIdx := range segments {
		for off := 0; off < rowsPerSegment; off++ {
			selections = append(selections, OffsetSelection{SegmentIndex: segIdx, Offset: int64(off)})
			ids = append(ids, pk)
			pk++
		}
	}

	merged := &MergedResultWithOffsets{
		IDs:        makeSegcoreIntIDs(ids),
		Selections: selections,
	}

	return &arrowFetchFixture{
		ctx:          ctx,
		rootPath:     rootPath,
		chunkManager: chunkManager,
		manager:      manager,
		collection:   collection,
		segments:     segments,
		plan:         plan,
		fieldSchemas: fieldSchemas,
		merged:       merged,
	}
}

func (f *arrowFetchFixture) teardown() {
	f.plan.Delete()
	for _, seg := range f.segments {
		seg.Release(f.ctx)
	}
	DeleteCollection(f.collection)
	f.chunkManager.RemoveWithPrefix(f.ctx, f.rootPath)
}

// =========================================================================
// Correctness test
// =========================================================================

// TestFetchFieldsData_ArrowMatchesRetrieveByOffsets verifies that
// fetchFieldsAsRecord (+ segcore.ArrowFieldsToProto) produces the same
// data as per-segment RetrieveByOffsets, manually interleaved in
// PK-sorted order (the reference path).
func TestFetchFieldsData_ArrowMatchesRetrieveByOffsets(t *testing.T) {
	fx := setupArrowFetchFixture(t, 50)
	defer fx.teardown()

	// Arrow path.
	rec, err := fetchFieldsAsRecord(fx.ctx, fx.segments, fx.plan, fx.merged)
	require.NoError(t, err)
	defer rec.Release()
	require.Equal(t, len(fx.fieldSchemas), int(rec.NumCols()), "Arrow record column count must match requested output fields")
	require.Equal(t, int64(len(fx.merged.Selections)), rec.NumRows())

	arrowFieldsData, err := segcore.ArrowFieldsToProto(rec, fx.fieldSchemas)
	require.NoError(t, err)
	require.Len(t, arrowFieldsData, len(fx.fieldSchemas))

	// Reference: per-segment RetrieveByOffsets + manual interleave.
	offsetsBySegment := make([][]int64, len(fx.segments))
	for _, sel := range fx.merged.Selections {
		offsetsBySegment[sel.SegmentIndex] = append(offsetsBySegment[sel.SegmentIndex], sel.Offset)
	}
	segResults := make([]*segcorepb.RetrieveResults, len(fx.segments))
	for segIdx, offsets := range offsetsBySegment {
		r, err := fx.segments[segIdx].RetrieveByOffsets(fx.ctx, &segcore.RetrievePlanWithOffsets{
			RetrievePlan: fx.plan,
			Offsets:      offsets,
		})
		require.NoError(t, err)
		segResults[segIdx] = r
	}

	// Interleave per-segment results into PK-sorted order.
	var refFieldsData []*schemapb.FieldData
	for _, r := range segResults {
		if r != nil && len(r.GetFieldsData()) != 0 {
			refFieldsData = typeutil.PrepareResultFieldData(r.GetFieldsData(), int64(len(fx.merged.Selections)))
			break
		}
	}
	require.NotNil(t, refFieldsData, "reference path produced no fields")

	idxComputers := make([]*typeutil.FieldDataIdxComputer, len(segResults))
	for i, r := range segResults {
		if r != nil {
			idxComputers[i] = typeutil.NewFieldDataIdxComputer(r.GetFieldsData())
		}
	}
	segResOffset := make([]int64, len(segResults))
	for _, sel := range fx.merged.Selections {
		r := segResults[sel.SegmentIndex]
		if r == nil {
			continue
		}
		fieldIdxs := idxComputers[sel.SegmentIndex].Compute(segResOffset[sel.SegmentIndex])
		typeutil.AppendFieldData(refFieldsData, r.GetFieldsData(), segResOffset[sel.SegmentIndex], fieldIdxs...)
		segResOffset[sel.SegmentIndex]++
	}

	require.Len(t, refFieldsData, len(arrowFieldsData), "Arrow and reference paths must return the same number of fields")

	for i, arrowFD := range arrowFieldsData {
		refFD := refFieldsData[i]
		require.NotNil(t, arrowFD, "arrow field %d has nil FieldData", i)
		require.NotNil(t, refFD, "reference field %d has nil FieldData", i)

		// The Arrow path populates FieldName/FieldId/Type from the Go-side
		// schema, while RetrieveByOffsets's C++ segcore leaves FieldName
		// empty and may not set FieldId. Normalize metadata before comparing.
		refFD.FieldName = arrowFD.GetFieldName()
		refFD.FieldId = arrowFD.GetFieldId()
		refFD.Type = arrowFD.GetType()

		require.Truef(t, proto.Equal(refFD, arrowFD),
			"field %q (id=%d): Arrow and reference FieldData diverge\narrow=%v\nref=%v",
			arrowFD.GetFieldName(), arrowFD.GetFieldId(), arrowFD, refFD)
	}
}

// =========================================================================
// Benchmarks
//
// Both benchmarks share the same 2-segment / 100-row / dim-128 fixture as
// the correctness test above. Custom metrics approximate the brief's
// requested breakdown by timing the same production functions the
// integrated NewFetchFieldsDataOperator calls, just orchestrated directly
// here (sequential per-segment loop instead of the operator's conc.Pool
// fan-out) so each phase can be isolated:
//
//   - cgo-ns/op:        the single-shot cgo call into segcore
//                        (segcore.FillRetrieveFieldsOrdered for Arrow,
//                        the sum of per-segment Segment.RetrieveByOffsets
//                        calls for proto).
//   - convert-ns/op:    segcore.ArrowFieldsToProto (Arrow only).
//   - interleave-ns/op: the Go-side typeutil.AppendFieldData merge loop
//                        that reorders per-segment RetrieveByOffsets
//                        results into PK-sorted order (proto only).
// =========================================================================

func BenchmarkFetchFieldsData_Arrow(b *testing.B) {
	fx := setupArrowFetchFixture(b, 50)
	defer fx.teardown()

	cSegments := make([]segcore.CSegment, len(fx.segments))
	for i, seg := range fx.segments {
		cSegments[i] = seg.(*LocalSegment).csegment
	}
	segIndices := make([]int32, len(fx.merged.Selections))
	segOffsets := make([]int64, len(fx.merged.Selections))
	for i, sel := range fx.merged.Selections {
		segIndices[i] = int32(sel.SegmentIndex)
		segOffsets[i] = sel.Offset
	}

	var cgoNs, convertNs int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cgoStart := time.Now()
		rec, err := segcore.FillRetrieveFieldsOrdered(fx.ctx, cSegments, fx.plan, segIndices, segOffsets)
		cgoNs += time.Since(cgoStart).Nanoseconds()
		if err != nil {
			b.Fatal(err)
		}

		convertStart := time.Now()
		fieldsData, err := segcore.ArrowFieldsToProto(rec, fx.fieldSchemas)
		convertNs += time.Since(convertStart).Nanoseconds()
		rec.Release()

		if err != nil {
			b.Fatal(err)
		}
		if len(fieldsData) != len(fx.fieldSchemas) {
			b.Fatalf("unexpected field count: got %d, want %d", len(fieldsData), len(fx.fieldSchemas))
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(cgoNs)/float64(b.N), "cgo-ns/op")
	b.ReportMetric(float64(convertNs)/float64(b.N), "convert-ns/op")
}

func BenchmarkFetchFieldsData_Proto(b *testing.B) {
	fx := setupArrowFetchFixture(b, 50)
	defer fx.teardown()

	offsetsBySegment := make([][]int64, len(fx.segments))
	for _, sel := range fx.merged.Selections {
		offsetsBySegment[sel.SegmentIndex] = append(offsetsBySegment[sel.SegmentIndex], sel.Offset)
	}

	var cgoNs, interleaveNs int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cgoStart := time.Now()
		segmentResults := make([]*segcorepb.RetrieveResults, len(fx.segments))
		futures := make([]*conc.Future[any], 0, len(offsetsBySegment))
		for segIdx, offsets := range offsetsBySegment {
			if offsets == nil {
				continue
			}
			idx := segIdx
			offs := offsets
			future := GetSQPool().Submit(func() (any, error) {
				r, err := fx.segments[idx].RetrieveByOffsets(fx.ctx, &segcore.RetrievePlanWithOffsets{
					RetrievePlan: fx.plan,
					Offsets:      offs,
				})
				if err != nil {
					return nil, err
				}
				segmentResults[idx] = r
				return nil, nil
			})
			futures = append(futures, future)
		}
		if err := conc.BlockOnAll(futures...); err != nil {
			b.Fatal(err)
		}
		cgoNs += time.Since(cgoStart).Nanoseconds()

		interleaveStart := time.Now()
		var fieldsData []*schemapb.FieldData
		for _, r := range segmentResults {
			if r != nil && len(r.GetFieldsData()) != 0 {
				fieldsData = typeutil.PrepareResultFieldData(r.GetFieldsData(), int64(len(fx.merged.Selections)))
				break
			}
		}
		if fieldsData != nil {
			idxComputers := make([]*typeutil.FieldDataIdxComputer, len(segmentResults))
			for idx, r := range segmentResults {
				if r != nil {
					idxComputers[idx] = typeutil.NewFieldDataIdxComputer(r.GetFieldsData())
				}
			}
			segmentResOffset := make([]int64, len(segmentResults))
			for _, sel := range fx.merged.Selections {
				r := segmentResults[sel.SegmentIndex]
				if r == nil {
					continue
				}
				fieldIdxs := idxComputers[sel.SegmentIndex].Compute(segmentResOffset[sel.SegmentIndex])
				typeutil.AppendFieldData(fieldsData, r.GetFieldsData(), segmentResOffset[sel.SegmentIndex], fieldIdxs...)
				segmentResOffset[sel.SegmentIndex]++
			}
		}
		interleaveNs += time.Since(interleaveStart).Nanoseconds()

		if len(fieldsData) != len(fx.fieldSchemas) {
			b.Fatalf("unexpected field count: got %d, want %d", len(fieldsData), len(fx.fieldSchemas))
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(cgoNs)/float64(b.N), "cgo-ns/op")
	b.ReportMetric(float64(interleaveNs)/float64(b.N), "interleave-ns/op")
}

// =========================================================================
// E2E overhead ratio benchmark
//
// Measures the retrieve phase (Phase 2 of IgnoreNonPk) as a fraction of
// a full ANN search on the same data. This answers: "if I run a search
// and then fetch output fields, how much of the total time is retrieval?"
//
// Setup: 2 sealed segments × 50 rows each, dim=128 (DefaultDim),
// brute-force flat scan (no index), topK=10, nq=1.
// Output fields: PK + Float + FloatVector.
// =========================================================================

func BenchmarkOverheadRatio_SearchVsRetrieve(b *testing.B) {
	fx := setupArrowFetchFixture(b, 50)
	defer fx.teardown()

	segIDs := make([]int64, len(fx.segments))
	for i, seg := range fx.segments {
		segIDs[i] = seg.ID()
	}

	topK := int64(10)
	outputFieldIDs := make([]int64, len(fx.fieldSchemas))
	for i, fs := range fx.fieldSchemas {
		outputFieldIDs[i] = fs.GetFieldID()
	}
	searchReq, err := mock_segcore.GenSearchPlanAndRequestsWithOutputFields(
		fx.collection.GetCCollection(), segIDs, 1, topK, outputFieldIDs,
	)
	require.NoError(b, err)

	// Precompute Arrow retrieve args.
	cSegments := make([]segcore.CSegment, len(fx.segments))
	for i, seg := range fx.segments {
		cSegments[i] = seg.(*LocalSegment).csegment
	}
	segIndices := make([]int32, len(fx.merged.Selections))
	segOffsets := make([]int64, len(fx.merged.Selections))
	for i, sel := range fx.merged.Selections {
		segIndices[i] = int32(sel.SegmentIndex)
		segOffsets[i] = sel.Offset
	}

	// Precompute proto per-segment offsets outside the timed loop.
	protoOffsetsBySegment := make([][]int64, len(fx.segments))
	for _, sel := range fx.merged.Selections {
		protoOffsetsBySegment[sel.SegmentIndex] = append(protoOffsetsBySegment[sel.SegmentIndex], sel.Offset)
	}

	var searchNs, retrieveArrowNs, retrieveProtoNs int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// ANN search (brute force flat scan, dim=128, nq=1, topK=10).
		searchStart := time.Now()
		for _, seg := range fx.segments {
			res, err := seg.Search(fx.ctx, searchReq)
			if err != nil {
				b.Fatal(err)
			}
			res.Release()
		}
		searchNs += time.Since(searchStart).Nanoseconds()

		// Retrieve via Arrow (100 rows × 3 fields incl. FloatVector dim=128).
		arrowStart := time.Now()
		rec, err := segcore.FillRetrieveFieldsOrdered(fx.ctx, cSegments, fx.plan, segIndices, segOffsets)
		if err != nil {
			b.Fatal(err)
		}
		_, err = segcore.ArrowFieldsToProto(rec, fx.fieldSchemas)
		if err != nil {
			b.Fatal(err)
		}
		rec.Release()
		retrieveArrowNs += time.Since(arrowStart).Nanoseconds()

		// Retrieve via Proto (same 100 rows).
		protoStart := time.Now()
		for segIdx, offsets := range protoOffsetsBySegment {
			r, err := fx.segments[segIdx].RetrieveByOffsets(fx.ctx, &segcore.RetrievePlanWithOffsets{
				RetrievePlan: fx.plan,
				Offsets:      offsets,
			})
			if err != nil {
				b.Fatal(err)
			}
			_ = r
		}
		retrieveProtoNs += time.Since(protoStart).Nanoseconds()
	}
	b.StopTimer()

	searchAvg := float64(searchNs) / float64(b.N)
	arrowAvg := float64(retrieveArrowNs) / float64(b.N)
	protoAvg := float64(retrieveProtoNs) / float64(b.N)

	b.ReportMetric(searchAvg, "search-ns/op")
	b.ReportMetric(arrowAvg, "retrieve-arrow-ns/op")
	b.ReportMetric(protoAvg, "retrieve-proto-ns/op")
	b.ReportMetric(arrowAvg/(searchAvg+arrowAvg)*100, "arrow-overhead-%")
	b.ReportMetric(protoAvg/(searchAvg+protoAvg)*100, "proto-overhead-%")
}
