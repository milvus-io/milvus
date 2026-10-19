// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func scanIdentityRecord(pool memory.Allocator, offsets, pks, timestamps []int64, stringPK bool) arrow.Record {
	ob, tb := array.NewInt64Builder(pool), array.NewInt64Builder(pool)
	defer ob.Release()
	defer tb.Release()
	ob.AppendValues(offsets, nil)
	tb.AppendValues(timestamps, nil)
	var pk arrow.Array
	if stringPK {
		b := array.NewStringBuilder(pool)
		defer b.Release()
		for _, id := range pks {
			b.Append(fmt.Sprintf("pk-%02d", id))
		}
		pk = b.NewArray()
	} else {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(pks, nil)
		pk = b.NewArray()
	}
	cols := []arrow.Array{ob.NewArray(), pk, tb.NewArray()}
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "$seg_offset", Type: arrow.PrimitiveTypes.Int64},
		{Name: "$id", Type: pk.DataType()},
		{Name: "$timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	return array.NewRecord(schema, cols, int64(len(offsets)))
}

// Compare against the pre-existing reducer, including stable ties, duplicate
// replacement, all reduce modes, empty inputs and query limits. The source row
// must follow the chosen identity rather than the sorted row position.
func TestQueryScanArrowMergeMatchesProto(t *testing.T) {
	for _, stringPK := range []bool{false, true} {
		t.Run(fmt.Sprint(stringPK), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			rng := rand.New(rand.NewSource(71))
			for trial := 0; trial < 40; trial++ {
				var scans []*chain.DataFrame
				var reference []*segcorepb.RetrieveResults
				for seg := 0; seg < 4; seg++ {
					n := rng.Intn(15)
					offsets, pks, timestamps := make([]int64, n), make([]int64, n), make([]int64, n)
					ids := &schemapb.IDs{}
					for i := 0; i < n; i++ {
						offsets[i] = int64(i*3 + 7)
						pks[i] = int64(rng.Intn(8))
						timestamps[i] = int64(rng.Intn(5))
						if stringPK {
							typeutil.AppendPKs(ids, fmt.Sprintf("pk-%02d", pks[i]))
						} else {
							typeutil.AppendPKs(ids, pks[i])
						}
					}
					order := make([]int, n)
					for i := range order {
						order[i] = i
					}
					sort.SliceStable(order, func(i, j int) bool { return pks[order[i]] < pks[order[j]] })
					sortedOffsets, sortedPKs, sortedTS := make([]int64, n), make([]int64, n), make([]int64, n)
					for i, row := range order {
						sortedOffsets[i], sortedPKs[i], sortedTS[i] = offsets[row], pks[row], timestamps[row]
					}
					record := scanIdentityRecord(pool, sortedOffsets, sortedPKs, sortedTS, stringPK)
					scan := scanFrameFromRecord(t, record)
					record.Release()
					scans = append(scans, scan)
					ref := &segcorepb.RetrieveResults{Ids: ids, Offset: append([]int64(nil), offsets...), FieldsData: []*schemapb.FieldData{makeSegcoreTsField(timestamps)}}
					sort.Stable(&byPK{r: ref})
					reference = append(reference, ref)
				}
				for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
					for _, limit := range []int64{-1, 0, 1, 3, 100} {
						expected, err := NewMergeByPKWithOffsetsOperator(limit, mode).Run(context.Background(), nil, reference)
						require.NoError(t, err)
						actual, err := mergeQueryScanResults(scans, limit, mode)
						require.NoError(t, err)
						want := expected[0].(*MergedResultWithOffsets)
						exported, err := chain.ToRetrieveResults(actual.Frame)
						require.NoError(t, err)
						if len(want.Selections) > 0 {
							require.True(t, proto.Equal(want.IDs, exported.Ids))
						}
						require.Equal(t, want.Selections, actual.Selections)
						if len(actual.Selections) > 0 {
							offsets, err := queryFrameInt64Column(actual.Frame, "$seg_offset")
							require.NoError(t, err)
							for i, sel := range actual.Selections {
								require.Equal(t, sel.Offset, offsets.Value(i))
							}
						}
						actual.Frame.Release()
					}
				}
				for _, scan := range scans {
					scan.Release()
				}
			}
		})
	}
}

func TestQueryScanArrowIdentityValidation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	record := scanIdentityRecord(pool, []int64{8, 3}, []int64{200, 100}, []int64{1, 2}, false)
	defer record.Release()
	scan := scanFrameFromRecord(t, record)
	defer scan.Release()
	view, err := prepareQueryFrame(scan)
	require.NoError(t, err)
	require.Equal(t, 2, view.rows)
	_, err = queryScanColumn(nil, "$seg_offset")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = queryScanColumn(record, "missing")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	strings := scanIdentityRecord(pool, []int64{8}, []int64{100}, []int64{1}, true)
	defer strings.Release()
	stringFrame := scanFrameFromRecord(t, strings)
	defer stringFrame.Release()
	_, err = queryFrameInt64Column(stringFrame, "$id")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	nulls := array.NewInt64Builder(pool)
	nulls.AppendNull()
	nullColumn := nulls.NewArray()
	nulls.Release()
	nullRecord := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "$seg_offset", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil), []arrow.Array{nullColumn}, 1)
	nullColumn.Release()
	_, err = queryScanColumn(nullRecord, "$seg_offset")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	nullRecord.Release()
	duplicateRecord := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "$seg_offset", Type: arrow.PrimitiveTypes.Int64}, {Name: "$seg_offset", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{record.Column(0), record.Column(0)}, 2)
	_, err = queryScanColumn(duplicateRecord, "$seg_offset")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	duplicateRecord.Release()
	other := scanFrameFromRecord(t, strings)
	defer other.Release()
	_, err = mergeQueryScanResults([]*chain.DataFrame{scan, other}, -1, reduce.IReduceNoOrder)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func scanFrameFromRecord(t *testing.T, record arrow.Record) *chain.DataFrame {
	t.Helper()
	builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{record.NumRows()})
	defer builder.Release()
	for i, field := range record.Schema().Fields() {
		column := record.Column(i)
		column.Retain()
		require.NoError(t, builder.AddColumnFromChunks(field.Name, []arrow.Array{column}))
	}
	return builder.Build()
}

func TestQueryScanFrameOwnsIdentityColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	record := scanIdentityRecord(pool, []int64{3, 8}, []int64{100, 200}, []int64{2, 1}, false)
	frame := scanFrameFromRecord(t, record)
	record.Release()
	defer frame.Release()
	view, err := prepareQueryFrame(frame)
	require.NoError(t, err)
	require.Equal(t, int64(100), view.pk(0))
	require.Equal(t, []int64{3, 8}, view.offsets.Int64Values())
}

func TestQueryScanLateIdentitiesOwnBuffers(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	identities := scanIdentityRecord(pool, []int64{3, 8}, []int64{100, 200}, []int64{2, 1}, false)
	offsets := array.NewRecord(arrow.NewSchema([]arrow.Field{identities.Schema().Field(0)}, nil), []arrow.Array{identities.Column(0)}, 2)
	frame := scanFrameFromRecord(t, offsets)
	offsets.Release()
	joined, err := withQueryIdentities(frame, identities)
	require.NoError(t, err)
	frame.Release()
	identities.Release()
	defer joined.Release()
	view, err := prepareQueryFrame(joined)
	require.NoError(t, err)
	require.Equal(t, int64(100), view.pk(0))
	require.Equal(t, []int64{3, 8}, view.offsets.Int64Values())
	require.Equal(t, []int64{2, 1}, view.timestamps.Int64Values())
}

func TestQueryScanIdentityFrameOwnership(t *testing.T) {
	for _, offsets := range [][]int64{{8, 3}, {3, 8}, {8}} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		func() {
			record := scanIdentityRecord(pool, []int64{8, 3}, []int64{200, 100}, []int64{1, 2}, false)
			defer func() {
				if record != nil {
					record.Release()
				}
			}()
			builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(offsets))})
			defer builder.Release()
			values := array.NewInt64Builder(pool)
			values.AppendValues(offsets, nil)
			require.NoError(t, builder.AddColumnFromChunks("$seg_offset", []arrow.Array{values.NewArray()}))
			values.AppendValues(offsets, nil)
			require.NoError(t, builder.AddColumnFromChunks("result", []arrow.Array{values.NewArray()}))
			values.Release()
			frame := builder.Build()
			joined, err := withQueryIdentities(frame, record)
			if len(offsets) != 2 || offsets[0] != 8 {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Nil(t, joined)
				frame.Release()
				return
			}
			require.NoError(t, err)
			require.Same(t, frame.Column("result"), joined.Column("result"))
			frame.Release()
			// The joined frame owns PK/timestamp references after the original record dies.
			record.Release()
			record = nil
			defer joined.Release()
			view, err := prepareQueryFrame(joined)
			require.NoError(t, err)
			require.Equal(t, int64(200), view.pk(0))
			require.Equal(t, 2, view.rows)
			require.Equal(t, []int64{8, 3}, joined.Column("result").Chunk(0).(*array.Int64).Int64Values())
		}()
		pool.AssertSize(t, 0)
	}
}

func TestQueryScanMergedFrameOwnsComputedRows(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	var frames []*chain.DataFrame
	for i, inputs := range [][]string{
		{`[{"content":"one","starts":[1],"ends":[2]}]`, `[{"content":"old","starts":[3],"ends":[4]}]`},
		{`[{"content":"two","starts":[2],"ends":[3]}]`, `[{"content":"latest","starts":[4,5],"ends":[5,6]}]`},
	} {
		pks, offsets := []int64{1, 3}, []int64{10, 30}
		if i == 1 {
			pks, offsets = []int64{2, 3}, []int64{20, 31}
		}
		record := scanIdentityRecord(pool, offsets, pks, []int64{int64(i + 1), int64(i + 1)}, false)
		base := scanFrameFromRecord(t, record)
		record.Release()
		builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{2})
		names := base.ColumnNames()
		if i == 1 {
			for left, right := 0, len(names)-1; left < right; left, right = left+1, right-1 {
				names[left], names[right] = names[right], names[left]
			}
		}
		for _, name := range names {
			require.NoError(t, builder.AddColumnFrom(base, name))
		}
		base.Release()
		values := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		for _, input := range inputs {
			values.Append([]byte(input))
		}
		require.NoError(t, builder.SetComputedField(arrow.Field{Name: "matches", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata(
			[]string{chain.ArrowDataTypeMetadataKey}, []string{fmt.Sprint(int(schemapb.DataType_JSON))},
		)}))
		require.NoError(t, builder.AddColumnFromChunks("matches", []arrow.Array{values.NewArray()}))
		values.Release()
		frames = append(frames, builder.Build())
		builder.Release()
	}
	columnOrders := make([][]string, len(frames))
	for i, frame := range frames {
		columnOrders[i] = append([]string(nil), frame.ColumnNames()...)
	}
	merged, err := mergeQueryScanResults(frames, -1, reduce.IReduceNoOrder)
	require.NoError(t, err)
	require.Equal(t, columnOrders[0], merged.Frame.ColumnNames())
	for i, frame := range frames {
		require.Equal(t, columnOrders[i], frame.ColumnNames())
		frame.Release()
	}
	defer merged.Frame.Release()
	require.Equal(t, []OffsetSelection{{SegmentIndex: 0, Offset: 10}, {SegmentIndex: 1, Offset: 20}, {SegmentIndex: 1, Offset: 31}}, merged.Selections)
	result, err := chain.ToRetrieveResults(merged.Frame)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, result.Ids.GetIntId().Data)
	require.Len(t, result.FieldsData, 1)
	require.Equal(t, []int64{1, 2, 2}, result.FieldsData[0].GetScalars().GetLongData().Data)
	require.Equal(t, schemapb.DataType_JSON, result.ComputedResults[0].Type)
	require.JSONEq(t, `[{"content":"latest","starts":[4,5],"ends":[5,6]}]`, string(result.ComputedResults[0].GetScalars().GetJsonData().Data[2]))
}

func TestQueryScanEmptyFramePreservesColumns(t *testing.T) {
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			pk := &schemapb.FieldSchema{Name: "pk", FieldID: 100, DataType: pkType, IsPrimaryKey: true}
			vector := &schemapb.FieldSchema{
				Name: "vector", FieldID: 101, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
			}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk, vector}}
			computed := arrow.NewSchema([]arrow.Field{{Name: "matches", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata(
				[]string{chain.ArrowDataTypeMetadataKey}, []string{fmt.Sprint(int(schemapb.DataType_JSON))},
			)}}, nil)
			frame, err := emptyQueryFrame(schema, []*schemapb.FieldSchema{pk, vector}, computed)
			require.NoError(t, err)
			defer frame.Release()
			require.Zero(t, frame.NumRows())
			require.False(t, frame.Schema().Field(frame.Schema().FieldIndices("matches")[0]).Nullable)
			require.ElementsMatch(t, []string{"$id", "$timestamp", "pk", "vector", "matches"}, frame.ColumnNames())
			for _, name := range frame.ColumnNames() {
				require.NotEqual(t, arrow.NULL, frame.Column(name).DataType().ID())
				require.Empty(t, frame.Column(name).Chunks())
			}
			result, err := chain.ToRetrieveResults(frame)
			require.NoError(t, err)
			if pkType == schemapb.DataType_Int64 {
				require.NotNil(t, result.Ids.GetIntId())
			} else {
				require.NotNil(t, result.Ids.GetStrId())
			}
			require.Len(t, result.FieldsData, 3)
			require.Equal(t, int64(8), result.FieldsData[1].GetVectors().Dim)
			require.NotNil(t, result.FieldsData[2].GetScalars().GetLongData())
			require.Len(t, result.ComputedResults, 1)
			require.Equal(t, schemapb.DataType_JSON, result.ComputedResults[0].Type)
			require.NotNil(t, result.ComputedResults[0].GetScalars().GetJsonData())
			require.Empty(t, result.ComputedResults[0].GetScalars().GetJsonData().Data)
		})
	}
}

func TestQueryScanGatherRejectsMismatchedColumns(t *testing.T) {
	makeFrame := func(fields []arrow.Field) *chain.DataFrame {
		b := chain.NewDataFrameBuilder().SetChunkSizes([]int64{0})
		defer b.Release()
		for _, field := range fields {
			require.NoError(t, b.SetComputedField(field))
			require.NoError(t, b.AddColumns([]string{field.Name}, []*arrow.Chunked{arrow.NewChunked(field.Type, nil)}))
		}
		return b.Build()
	}
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}
	otherField := arrow.Field{Name: "other", Type: arrow.PrimitiveTypes.Int64}
	first := makeFrame([]arrow.Field{field, otherField})
	defer first.Release()
	for name, fields := range map[string][]arrow.Field{
		"missing":  {field},
		"extra":    {field, otherField, {Name: "extra", Type: arrow.PrimitiveTypes.Int64}},
		"renamed":  {field, {Name: "renamed", Type: arrow.PrimitiveTypes.Int64}},
		"type":     {{Name: "value", Type: arrow.BinaryTypes.String}, otherField},
		"nullable": {{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, otherField},
	} {
		t.Run(name, func(t *testing.T) {
			other := makeFrame(fields)
			defer other.Release()
			out, err := gatherQueryFrames([]*chain.DataFrame{first, other}, nil, nil)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestQueryScanOutputEstimateBeforeMaterialization(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	old := params.QuotaConfig.MaxOutputSize.GetValue()
	defer params.Save(params.QuotaConfig.MaxOutputSize.Key, old)
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	record := scanIdentityRecord(pool, []int64{0, 1, 2}, []int64{1, 2, 3}, []int64{1, 1, 1}, false)
	base := scanFrameFromRecord(t, record)
	record.Release()
	builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{3})
	for _, name := range base.ColumnNames() {
		require.NoError(t, builder.AddColumnFrom(base, name))
	}
	base.Release()
	values := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	values.Append([]byte("[]"))
	values.Append([]byte(strings.Repeat("x", 1024)))
	values.Append([]byte(strings.Repeat("y", 1024)))
	require.NoError(t, builder.SetComputedField(arrow.Field{Name: "matches", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata(
		[]string{chain.ArrowDataTypeMetadataKey}, []string{strconv.Itoa(int(schemapb.DataType_JSON))},
	)}))
	require.NoError(t, builder.AddColumnFromChunks("matches", []arrow.Array{values.NewArray()}))
	values.Release()
	frame := builder.Build()
	builder.Release()
	defer frame.Release()
	selected, err := mergeQueryScanResults([]*chain.DataFrame{frame}, 1, reduce.IReduceNoOrder)
	require.NoError(t, err)
	defer selected.Frame.Release()
	require.EqualValues(t, 1, selected.Frame.NumRows())
	check := func(t *testing.T, selected *queryScanMergeResult, limit, expectedBudget int64) {
		t.Helper()
		require.NoError(t, params.Save(params.QuotaConfig.MaxOutputSize.Key, strconv.FormatInt(limit, 10)))
		calls := 0
		patch := mockey.Mock(fetchFieldsAsRecord).To(func(_ context.Context, _ []Segment, _ *segcore.RetrievePlan, merged *MergedResultWithOffsets, budget int64) (arrow.Record, error) {
			calls++
			require.Equal(t, selected.Selections, merged.Selections)
			require.Equal(t, expectedBudget, budget)
			return nil, context.Canceled
		}).Build()
		defer patch.UnPatch()
		result, err := materializeQueryFrame(context.Background(), nil, nil, selected, nil)
		require.Nil(t, result)
		if expectedBudget < 0 {
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Equal(t, merr.InputError, merr.GetErrorType(err))
			require.Zero(t, calls)
		} else {
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)
		}
	}
	computedSize := int64(selected.Frame.Column("matches").Chunk(0).Data().SizeInBytes())
	t.Run("remaining budget and merged selections reach native reader", func(t *testing.T) {
		check(t, selected, computedSize+123, 123)
	})
	t.Run("exact computed budget leaves zero for ordinary fields", func(t *testing.T) {
		check(t, selected, computedSize, 0)
	})
	t.Run("computed output alone exceeds budget", func(t *testing.T) {
		check(t, selected, computedSize-1, -1)
	})
	t.Run("numeric computed output", func(t *testing.T) {
		record := scanIdentityRecord(pool, []int64{0, 1}, []int64{1, 2}, []int64{1, 1}, false)
		base := scanFrameFromRecord(t, record)
		record.Release()
		builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{2})
		defer builder.Release()
		for _, name := range base.ColumnNames() {
			require.NoError(t, builder.AddColumnFrom(base, name))
		}
		base.Release()
		require.NoError(t, builder.SetComputedField(arrow.Field{Name: "scores", Type: arrow.PrimitiveTypes.Int64}))
		numbers := array.NewInt64Builder(pool)
		numbers.AppendValues([]int64{10, 20}, nil)
		values := numbers.NewArray()
		numbers.Release()
		computedBytes := int64(values.Data().SizeInBytes())
		require.NoError(t, builder.AddColumnFromChunks("scores", []arrow.Array{values}))
		frame := builder.Build()
		defer frame.Release()
		result := &queryScanMergeResult{Frame: frame, Selections: []OffsetSelection{{Offset: 0}, {Offset: 1}}}
		check(t, result, computedBytes+16, 16)
	})
	t.Run("no computed projection passes full budget", func(t *testing.T) {
		record := scanIdentityRecord(pool, []int64{0, 1}, []int64{1, 2}, []int64{1, 1}, false)
		plain := scanFrameFromRecord(t, record)
		record.Release()
		defer plain.Release()
		result := &queryScanMergeResult{Frame: plain, Selections: []OffsetSelection{{Offset: 0}, {Offset: 1}}}
		check(t, result, 100, 100)
	})
}
