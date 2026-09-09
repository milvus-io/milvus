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

package channelmgr

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGenInsertMsgsByPartitionFallbackSingleIndexPass(t *testing.T) {
	for _, tc := range []struct {
		name         string
		threshold    int
		offsets      []int
		batches      [][]int
		disableViews bool
	}{
		{"one batch", 128, []int{1, 3, 5, 7, 9, 11}, [][]int{{1, 3, 5, 7, 9, 11}}, false},
		{"split batches", 33, []int{1, 3, 5, 7, 9, 11}, [][]int{{1, 3}, {5, 7}, {9, 11}}, false},
		{"single row batches", 17, []int{1, 3, 5, 7, 9, 11}, [][]int{{1}, {3}, {5}, {7}, {9}, {11}}, false},
		{"single row copy fallback", 17, []int{1, 3, 5, 7, 9, 11}, [][]int{{1}, {3}, {5}, {7}, {9}, {11}}, true},
		{"mixed copy and view batches", 25, []int{1, 3, 5, 7, 9, 11}, [][]int{{1, 3}, {5}, {7, 9}, {11}}, false},
		{"unordered singleton views", 17, []int{11, 1, 7, 3}, [][]int{{11}, {1}, {7}, {3}}, false},
		{"unordered and repeated rows", 128, []int{7, 1, 1, 5}, [][]int{{7, 1, 1, 5}}, false},
		{"empty selection", 128, nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
			require.NoError(t, paramtable.Get().Save(key, strconv.Itoa(tc.threshold)))
			t.Cleanup(func() { paramtable.Get().Reset(key) })
			src := newNullableVectorInsertMsgForPackTest(12, 2, 1)
			if tc.disableViews {
				patch := mockey.Mock(typeutil.CreateFieldDataRangeView).Return(nil, false).Build()
				t.Cleanup(func() { patch.UnPatch() })
			}

			// Count real index computations without replacing their behavior.
			// Each copied row must be scanned once, even across batch splits.
			// Singleton views additionally compute their exclusive end index.
			var computedRows []int
			var compute func(*typeutil.FieldDataIdxComputer, int64) []int64
			patch := mockey.Mock((*typeutil.FieldDataIdxComputer).Compute).
				To(func(c *typeutil.FieldDataIdxComputer, row int64) []int64 {
					computedRows = append(computedRows, int(row))
					return compute(c, row)
				}).Origin(&compute).Build()
			t.Cleanup(func() { patch.UnPatch() })

			msgs, err := GenInsertMsgsByPartition(context.Background(), 2, 1, "test_partition",
				tc.offsets, "test_channel", src, message.WALNamePulsar)
			require.NoError(t, err)
			require.Len(t, msgs, len(tc.batches))
			wantComputations := append([]int(nil), tc.offsets...)
			for _, batch := range tc.batches {
				if len(batch) == 1 {
					wantComputations = append(wantComputations, batch[0]+1)
				}
			}
			assert.ElementsMatch(t, wantComputations, computedRows)
			for i, offsets := range tc.batches {
				got := msgs[i].(*msgstream.InsertMsg)
				var longs []int64
				var floats []float32
				var valid []bool
				var hashes []uint32
				var timestamps []uint64
				var rowIDs []int64
				for _, row := range offsets {
					longs = append(longs, int64(row))
					valid = append(valid, row%3 != 0)
					if row%3 != 0 {
						floats = append(floats, float32(row*2), float32(row*2+1))
					}
					hashes = append(hashes, src.HashValues[row])
					timestamps = append(timestamps, src.Timestamps[row])
					rowIDs = append(rowIDs, src.RowIDs[row])
				}
				assert.Equal(t, uint64(len(offsets)), got.NumRows)
				assert.Equal(t, int64(2), got.SegmentID)
				assert.Equal(t, int64(1), got.PartitionID)
				assert.Equal(t, "test_partition", got.PartitionName)
				assert.Equal(t, "test_channel", got.ShardName)
				assert.Equal(t, longs, got.FieldsData[0].GetScalars().GetLongData().GetData())
				assert.Equal(t, floats, got.FieldsData[1].GetVectors().GetFloatVector().GetData())
				assert.Equal(t, valid, typeutil.GetFieldDataValidData(got.FieldsData[1]))
				assert.Equal(t, hashes, got.HashValues)
				assert.Equal(t, timestamps, got.Timestamps)
				assert.Equal(t, rowIDs, got.RowIDs)
				if len(offsets) == 1 && !tc.disableViews {
					row := offsets[0]
					assert.True(t, &src.FieldsData[0].GetScalars().GetLongData().Data[row] == &got.FieldsData[0].GetScalars().GetLongData().Data[0])
					assert.True(t, &src.HashValues[row] == &got.HashValues[0])
					assert.True(t, &src.FieldsData[1].GetVectors().ValidData[row] == &got.FieldsData[1].GetVectors().ValidData[0])
				} else {
					assert.False(t, &src.FieldsData[0].GetScalars().GetLongData().Data[offsets[0]] == &got.FieldsData[0].GetScalars().GetLongData().Data[0])
				}
			}
		})
	}
}

func TestGenInsertMsgsByPartitionMixedSelectionKeepsContiguousBatchViews(t *testing.T) {
	src := newNullableVectorInsertMsgForPackTest(10, 2, 1)
	offsets := []int{1, 2, 4, 5}
	idxComputer := typeutil.NewFieldDataIdxComputer(src.FieldsData)
	rowSizes := make([]int, len(offsets))
	for i, offset := range offsets {
		var err error
		rowSizes[i], err = typeutil.EstimateEntitySize(
			src.FieldsData, offset, idxComputer.Compute(int64(offset))...,
		)
		require.NoError(t, err)
	}
	firstPairSize := rowSizes[0] + rowSizes[1]
	secondPairSize := rowSizes[2] + rowSizes[3]
	threshold := max(firstPairSize, secondPairSize) + 1
	require.GreaterOrEqual(t, firstPairSize+rowSizes[2], threshold)

	key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
	require.NoError(t, paramtable.Get().Save(key, strconv.Itoa(threshold)))
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	msgs, err := GenInsertMsgsByPartition(
		context.Background(), 2, 1, "test_partition", offsets,
		"test_channel", src, message.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	sourceLongs := src.FieldsData[0].GetScalars().GetLongData().GetData()
	for i, rowStart := range []int{1, 4} {
		got := msgs[i].(*msgstream.InsertMsg)
		gotLongs := got.FieldsData[0].GetScalars().GetLongData().GetData()
		require.Equal(t, uint64(2), got.NumRows)
		assert.Equal(t, sourceLongs[rowStart:rowStart+2], gotLongs)
		assert.True(t, &sourceLongs[rowStart] == &gotLongs[0])
		assert.True(t, &src.HashValues[rowStart] == &got.HashValues[0])
	}
}

func TestGenInsertMsgsByPartitionGapWithinBatchCopiesContiguousPrefix(t *testing.T) {
	key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
	require.NoError(t, paramtable.Get().Save(key, "1024"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	src := newNullableVectorInsertMsgForPackTest(10, 2, 1)
	offsets := []int{1, 2, 4, 5}
	msgs, err := GenInsertMsgsByPartition(
		context.Background(), 2, 1, "test_partition", offsets,
		"test_channel", src, message.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	got := msgs[0].(*msgstream.InsertMsg)
	sourceLongs := src.FieldsData[0].GetScalars().GetLongData().GetData()
	gotLongs := got.FieldsData[0].GetScalars().GetLongData().GetData()
	assert.Equal(t, []int64{1, 2, 4, 5}, gotLongs)
	assert.False(t, &sourceLongs[1] == &gotLongs[0])
}

func TestGenInsertMsgsByPartitionFallbackNullableSparseVectorSizes(t *testing.T) {
	key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
	require.NoError(t, paramtable.Get().Save(key, "73"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	src := newNullableVectorInsertMsgForPackTest(8, 2, 1)
	logicalContents := make([][]byte, 8)
	var contents [][]byte
	for row := range logicalContents {
		if row%3 == 0 {
			continue
		}
		indices := make([]uint32, row+1)
		values := make([]float32, row+1)
		for column := range indices {
			indices[column] = uint32(column)
			values[column] = float32(row)
		}
		logicalContents[row] = typeutil.CreateSparseFloatRow(indices, values)
		contents = append(contents, logicalContents[row])
	}
	field := src.FieldsData[1]
	field.Type = schemapb.DataType_SparseFloatVector
	field.GetVectors().Dim = 8
	field.GetVectors().Data = &schemapb.VectorField_SparseFloatVector{
		SparseFloatVector: &schemapb.SparseFloatArray{Dim: 8, Contents: contents},
	}

	// Selected row sizes are 24, 8 (null), 56 and 72 bytes. Both sizing
	// and copying must use compact physical indexes, not logical offsets.
	msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition",
		[]int{1, 3, 5, 7}, "test_channel", src, message.WALNamePulsar)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	for i, tc := range []struct {
		rows     []int64
		valid    []bool
		contents [][]byte
	}{
		{[]int64{1, 3}, []bool{true, false}, [][]byte{logicalContents[1]}},
		{[]int64{5}, []bool{true}, [][]byte{logicalContents[5]}},
		{[]int64{7}, []bool{true}, [][]byte{logicalContents[7]}},
	} {
		got := msgs[i].(*msgstream.InsertMsg)
		assert.Equal(t, uint64(len(tc.rows)), got.NumRows)
		assert.Equal(t, tc.rows, got.FieldsData[0].GetScalars().GetLongData().GetData())
		assert.Equal(t, tc.valid, typeutil.GetFieldDataValidData(got.FieldsData[1]))
		assert.Equal(t, tc.contents, got.FieldsData[1].GetVectors().GetSparseFloatVector().GetContents())
	}
}

func TestGenInsertMsgsByPartitionFallbackErrorsDiscardPartialBatches(t *testing.T) {
	key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
	require.NoError(t, paramtable.Get().Save(key, "64"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	for _, tc := range []struct {
		name    string
		lastRow string
		offsets []int
		wantErr error
	}{
		{"oversized later row", strings.Repeat("x", 64), []int{0, 2, 4}, merr.ErrParameterTooLarge},
		{"invalid later offset", "small", []int{0, 2, 5}, merr.ErrParameterInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newVarCharInsertMsgForPackTest(strings.Repeat("a", 40), "skip", strings.Repeat("b", 40), "skip", tc.lastRow)
			msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition",
				tc.offsets, "test_channel", src, message.WALNamePulsar)
			assert.Nil(t, msgs)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.False(t, merr.Status(err).GetRetriable())
		})
	}
}

func newNullableVectorInsertMsgForPackTest(rows, dim, scalarFields int) *msgstream.InsertMsg {
	src := newVarCharInsertMsgForPackTest(make([]string, rows)...)
	src.FieldsData = make([]*schemapb.FieldData, 0, scalarFields+1)
	for field := 0; field < scalarFields; field++ {
		data := make([]int64, rows)
		for row := range data {
			data[row] = int64(row + field*10000)
		}
		src.FieldsData = append(src.FieldsData, &schemapb.FieldData{
			Type:    schemapb.DataType_Int64,
			FieldId: int64(100 + field),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}},
		})
	}
	valid := make([]bool, rows)
	data := make([]float32, 0, rows*dim)
	for row := range valid {
		valid[row] = row%3 != 0
		if valid[row] {
			for column := 0; column < dim; column++ {
				data = append(data, float32(row*dim+column))
			}
		}
		src.HashValues[row] = uint32(row % 8)
		src.Timestamps[row] = uint64(100 + row)
	}
	src.FieldsData = append(src.FieldsData, &schemapb.FieldData{
		Type:    schemapb.DataType_FloatVector,
		FieldId: int64(100 + scalarFields),
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:       int64(dim),
			ValidData: valid,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: data},
			},
		}},
	})
	return src
}
