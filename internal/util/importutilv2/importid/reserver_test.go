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

package importid

import (
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// allocBlock is one [begin, end) handed out by a single allocN call.
type allocBlock struct{ begin, end int64 }

// recordingAlloc records what each allocN call asked for and leaves a gap between blocks,
// so a range that straddles two calls is observable: a real allocator gives no guarantee
// that consecutive AllocN results are adjacent.
func recordingAlloc(calls *[]int64, blocks *[]allocBlock) func(int64) (int64, int64, error) {
	next := int64(1000)
	return func(n int64) (int64, int64, error) {
		*calls = append(*calls, n)
		begin := next
		next += n + 1_000_000
		if blocks != nil {
			*blocks = append(*blocks, allocBlock{begin, begin + n})
		}
		return begin, begin + n, nil
	}
}

func forbiddenAlloc(t *testing.T) func(int64) (int64, int64, error) {
	return func(int64) (int64, int64, error) {
		t.Fatal("allocN must not be called")
		return 0, 0, nil
	}
}

func rangeWidths(ranges []*commonpb.IDRange) []int64 {
	out := make([]int64, len(ranges))
	for i, r := range ranges {
		out[i] = r.GetEnd() - r.GetBegin()
	}
	return out
}

func TestReserveFileIDRanges_Totals(t *testing.T) {
	var calls []int64
	ranges, err := ReserveFileIDRanges([]int64{10, 20, 30}, recordingAlloc(&calls, nil), 0)
	require.NoError(t, err)
	assert.Equal(t, []int64{60}, calls, "sum of reserved == sum of rows: no expansion, no over-allocation")
	assert.Equal(t, []int64{10, 20, 30}, rangeWidths(ranges))
	// Contiguous in file order within the single batch.
	assert.Equal(t, ranges[0].GetEnd(), ranges[1].GetBegin())
	assert.Equal(t, ranges[1].GetEnd(), ranges[2].GetBegin())
}

func TestReserveFileIDRanges_BatchesNeverStraddle(t *testing.T) {
	var calls []int64
	var blocks []allocBlock
	half := maxIDsPerAllocBatch / 2
	// clusterID 0 so the recorded blocks are directly comparable: a non-zero clusterID
	// ORs its bits into every id, shifting the ranges out of the raw blocks. The cluster
	// bits themselves are pinned by TestReserveFileIDRanges_ClusterIDBits.
	ranges, err := ReserveFileIDRanges([]int64{half, half, half}, recordingAlloc(&calls, &blocks), 0)
	require.NoError(t, err)
	// half+half fills one batch exactly; the third opens a new one.
	assert.Equal(t, []int64{2 * half, half}, calls)
	assert.Equal(t, []int64{half, half, half}, rangeWidths(ranges))
	for _, c := range calls {
		assert.LessOrEqual(t, c, maxIDsPerAllocBatch, "no allocN call exceeds the per-batch ceiling")
	}
	for i, r := range ranges {
		inOneBlock := false
		for _, b := range blocks {
			if r.GetBegin() >= b.begin && r.GetEnd() <= b.end {
				inOneBlock = true
			}
		}
		assert.True(t, inOneBlock, "range %d must sit inside a single batch", i)
	}
}

func TestReserveFileIDRanges_FileAtCeiling(t *testing.T) {
	var calls []int64
	ranges, err := ReserveFileIDRanges(
		[]int64{maxIDsPerAllocBatch, 1}, recordingAlloc(&calls, nil), 0)
	require.NoError(t, err)
	assert.Equal(t, []int64{maxIDsPerAllocBatch, 1}, calls, "a full batch forces the next file into its own")
	assert.Equal(t, []int64{maxIDsPerAllocBatch, 1}, rangeWidths(ranges))
}

func TestReserveFileIDRanges_ZeroRowFileGetsEmptyRange(t *testing.T) {
	var calls []int64
	ranges, err := ReserveFileIDRanges([]int64{5, 0, 7}, recordingAlloc(&calls, nil), 0)
	require.NoError(t, err)
	assert.Equal(t, []int64{12}, calls, "a zero-row file consumes no ids")
	assert.Equal(t, []int64{5, 0, 7}, rangeWidths(ranges))
	assert.Equal(t, ranges[1].GetBegin(), ranges[1].GetEnd())
	// The empty range sits at the packing position, keeping the batch contiguous.
	assert.Equal(t, ranges[0].GetEnd(), ranges[1].GetBegin())
	assert.Equal(t, ranges[1].GetEnd(), ranges[2].GetBegin())
}

func TestReserveFileIDRanges_AllZeroAllocatesNothing(t *testing.T) {
	ranges, err := ReserveFileIDRanges([]int64{0, 0}, forbiddenAlloc(t), 0)
	require.NoError(t, err)
	require.Len(t, ranges, 2)
	for _, r := range ranges {
		assert.Equal(t, r.GetBegin(), r.GetEnd())
	}
}

func TestReserveFileIDRanges_NoFiles(t *testing.T) {
	ranges, err := ReserveFileIDRanges(nil, forbiddenAlloc(t), 0)
	require.NoError(t, err)
	assert.Empty(t, ranges)
}

func TestReserveFileIDRanges_RejectsFileOverOneBatch(t *testing.T) {
	var calls []int64
	_, err := ReserveFileIDRanges([]int64{10, maxIDsPerAllocBatch + 1}, recordingAlloc(&calls, nil), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "split the file")
	assert.Empty(t, calls, "nothing is allocated once the request is rejected")
}

func TestReserveFileIDRanges_RejectsNegativeRows(t *testing.T) {
	_, err := ReserveFileIDRanges([]int64{10, -1}, forbiddenAlloc(t), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
}

func TestReserveFileIDRanges_AllocErrorPropagates(t *testing.T) {
	_, err := ReserveFileIDRanges([]int64{10}, func(int64) (int64, int64, error) {
		return 0, 0, merr.WrapErrServiceUnavailableMsg("rootcoord unavailable")
	}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestReserveFileIDRanges_ClusterIDBits(t *testing.T) {
	const clusterID = uint64(0b1011)
	fileRows := []int64{4096, 100}

	var calls []int64
	ranges, err := ReserveFileIDRanges(fileRows, recordingAlloc(&calls, nil), clusterID)
	require.NoError(t, err)
	require.Equal(t, []int64{4196}, calls)

	// The batch begin must equal what common.AllocAutoIDN produces for the same
	// allocation and clusterID: the cluster bits ride in the high bits of every id.
	wantBegin, _, err := common.AllocAutoIDN(func(int64) (int64, int64, error) { return 1000, 1000 + 4196, nil }, 4196, clusterID)
	require.NoError(t, err)
	assert.Equal(t, wantBegin, ranges[0].GetBegin())
	assert.Equal(t, []int64{4096, 100}, rangeWidths(ranges), "ORing the cluster bits preserves the exact widths")
	assert.Equal(t, ranges[0].GetEnd(), ranges[1].GetBegin())
	assert.NotZero(t, ranges[0].GetBegin(), "clusterID 0b1011 must embed its bits")
}

func TestLogIDRangeSize(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.Key, "10")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.Key)

	fields := make([]*schemapb.FieldSchema, 9)
	for i := range fields {
		fields[i] = &schemapb.FieldSchema{FieldID: int64(100 + i), DataType: schemapb.DataType_Int64}
	}
	schema := &schemapb.CollectionSchema{Fields: fields}

	// The log id budget is deliberately row-based: each row belongs to exactly one
	// (vchannel, partition) bucket, so the number of non-empty sync batches is at most
	// totalRows, and each sync batch writes at most binlogNum logs.
	// (totalRows+1)*binlogNum*expansionFactor is therefore a strict upper bound; when it
	// exceeds math.MaxUint32 we keep MaxUint32 instead of letting common.AllocAutoID's
	// uint32 conversion wrap (issue #52632).
	const (
		totalRows       int64 = 78_201_209
		binlogNum       int64 = 9 + 4 // len(fields)+2(user+rowID)+2(stats+bm25)
		expansionFactor int64 = 10
	)
	files := func(rows int64) []*datapb.ImportFileStats {
		return []*datapb.ImportFileStats{{TotalRows: rows}}
	}

	got := logIDRangeSize(schema, files(totalRows))
	old := (totalRows + 1) * binlogNum * expansionFactor
	assert.Greater(t, old, int64(math.MaxUint32), "sanity: this case must exercise the clamp")
	assert.Equal(t, int64(math.MaxUint32), got, "oversized row-based budget must be clamped, not wrapped")

	small := logIDRangeSize(schema, files(100))
	assert.Equal(t, int64(101)*binlogNum*expansionFactor, small)

	empty := logIDRangeSize(schema, files(0))
	assert.Equal(t, int64(1)*binlogNum*expansionFactor, empty)
}

// checkFileIDRanges: a range smaller than the file's row count is terminal (the cursor
// cannot cover the file), a range at least as large is allowed (the extra ids are never
// consumed), a nil range falls back, and the zero-width range of a zero-row file is
// accepted.
func TestCheckFileIDRanges(t *testing.T) {
	const rangeBegin = int64(5000)
	cases := []struct {
		name     string
		begin    int64
		end      int64
		nilRange bool
		rows     int64
		wantErr  bool
	}{
		{name: "exact range", begin: rangeBegin, end: rangeBegin + 100, rows: 100},
		{name: "over-reserved range is allowed", begin: rangeBegin, end: rangeBegin + 100, rows: 99},
		{name: "under-reserved range is terminal", begin: rangeBegin, end: rangeBegin + 100, rows: 101, wantErr: true},
		{name: "zero-width range on a zero-row file is allowed", begin: rangeBegin, end: rangeBegin, rows: 0},
		{name: "zero-width range on a non-empty file is terminal", begin: rangeBegin, end: rangeBegin, rows: 101, wantErr: true},
		{name: "nil range falls back", nilRange: true, rows: 101},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &internalpb.ImportFile{Id: 1, Paths: []string{"f1"}}
			if !tc.nilRange {
				f.IdRange = &commonpb.IDRange{Begin: tc.begin, End: tc.end}
			}
			files := []*datapb.ImportFileStats{{ImportFile: f, TotalRows: tc.rows}}
			err := checkFileIDRanges(files)
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrIDRangeTooSmall))
				assert.ErrorIs(t, err, merr.ErrImportSysFailed)
				return
			}
			require.NoError(t, err)
		})
	}
}

// The scheduler must be able to separate the one terminal assemble failure from the
// retriable ones. It cannot do that on merr classification: ErrImportSysFailed also carries
// transient cases, so the sentinel has to survive merr.Mark.
func TestErrIDRangeTooSmall_IsDistinguishableAndKeepsItsCode(t *testing.T) {
	terminal := merr.Mark(merr.WrapErrImportSysFailedMsg(
		"reserved ID range too small for file %v: %d rows, %d ids reserved",
		[]string{"a.npy"}, 100, 10), ErrIDRangeTooSmall)

	assert.True(t, errors.Is(terminal, ErrIDRangeTooSmall))
	assert.Equal(t, merr.Code(merr.ErrImportSysFailed), merr.Code(terminal),
		"marking must not replace the merr code the wire projection carries")

	// The transient shape AssembleImportRequest and its callees also return: same merr
	// code, and it must NOT be treated as terminal.
	transient := merr.WrapErrImportSysFailedMsg("job %d not found, waiting for import job creation", 1)
	assert.False(t, errors.Is(transient, ErrIDRangeTooSmall))
}
