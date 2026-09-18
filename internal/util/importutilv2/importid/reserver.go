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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ErrIDRangeTooSmall is returned when the ID range of a file cannot hold the rows of the
// file. Retry cannot fix it, so the import job must fail.
//
// A sentinel rather than a dedicated merr code: the error keeps merr.ErrImportSysFailed (the
// code the client and the wire projection see) and merr.Mark only chains the sentinel in.
var ErrIDRangeTooSmall = errors.New("reserved ID range too small")

// maxIDsPerAllocBatch mirrors the per-call ceiling of rootCoordAllocator.AllocN.
const maxIDsPerAllocBatch = int64(math.MaxUint32)

// ReserveFileIDRanges allocates contiguous per-file ID ranges sized to fileRows, which is
// aligned with the job's file order. It returns one range per file in the same order; a
// zero-row file gets an empty range (Begin == End). A single file needing more than one
// allocation batch (MaxUint32 ids) is rejected: a contiguous range cannot cover it and the
// datanode's one-cursor-per-file consumption cannot walk a straddling range.
//
// Two-phase flow: the ImportMsg broadcast carries no ranges and does no file I/O. After the
// PreImport tasks have read every file and produced the row counts, the cluster acting as
// primary allocates the ranges here and ships them to every cluster via the ImportIDRange WAL
// message, so each derives identical primary keys. The reservation is the sum of fileRows
// with no expansion factor: the counts come from fully reading the files.
func ReserveFileIDRanges(fileRows []int64, allocN func(int64) (int64, int64, error), clusterID uint64) ([]*commonpb.IDRange, error) {
	for i, rows := range fileRows {
		if rows < 0 {
			return nil, merr.WrapErrImportSysFailedMsg("import file %d has a negative row count %d", i, rows)
		}
		if rows > maxIDsPerAllocBatch {
			return nil, merr.WrapErrParameterInvalidMsg(
				"import file %d holds %d rows, more than one allocation batch can reserve (max %d); split the file",
				i, rows, maxIDsPerAllocBatch)
		}
	}

	ranges := make([]*commonpb.IDRange, len(fileRows))
	// Pack files into allocation batches greedily so no allocN call exceeds the ceiling and
	// no file's range straddles two batches: every range stays contiguous while the
	// reservation total may exceed the ceiling, only a single file may not.
	for i := 0; i < len(fileRows); {
		var batch int64
		j := i
		for ; j < len(fileRows) && batch+fileRows[j] <= maxIDsPerAllocBatch; j++ {
			batch += fileRows[j]
		}
		// A group of only zero-row files reserves nothing; allocN is never called with 0 and
		// their empty ranges keep the zero value.
		var cur int64
		if batch > 0 {
			begin, _, err := common.AllocAutoIDN(allocN, batch, clusterID)
			if err != nil {
				return nil, err
			}
			cur = begin
		}
		for k := i; k < j; k++ {
			ranges[k] = &commonpb.IDRange{Begin: cur, End: cur + fileRows[k]}
			cur = ranges[k].GetEnd()
		}
		i = j
	}
	return ranges, nil
}

// ReserveLogIDs reserves the log id range of one import request. It checks the ID range
// of every file against the exact row count from preimport first, and returns
// ErrIDRangeTooSmall if a range cannot hold its rows.
func ReserveLogIDs(schema *schemapb.CollectionSchema, files []*datapb.ImportFileStats,
	allocN func(int64) (int64, int64, error), clusterID uint64,
) (*datapb.IDRange, error) {
	if err := checkFileIDRanges(files); err != nil {
		return nil, err
	}
	begin, end, err := common.AllocAutoID(func(n uint32) (int64, int64, error) {
		return allocN(int64(n))
	}, uint32(logIDRangeSize(schema, files)), clusterID)
	if err != nil {
		return nil, err
	}
	return &datapb.IDRange{Begin: begin, End: end}, nil
}

// checkFileIDRanges checks the ID range of every file against the exact row count from
// preimport. A range smaller than the file's row count fails terminally: the datanode cursor
// cannot cover the file and no retry changes either number. A nil range (backup/L0, a job
// that predates the mechanism) has nothing to check. A range at least as large as the row
// count passes through untouched -- the extra ids are never consumed.
func checkFileIDRanges(files []*datapb.ImportFileStats) error {
	for _, fileStat := range files {
		f := fileStat.GetImportFile()
		r := f.GetIdRange()
		if r == nil {
			continue
		}
		reserved := r.GetEnd() - r.GetBegin()
		if reserved < fileStat.GetTotalRows() {
			return merr.Mark(merr.WrapErrImportSysFailedMsg(
				"file %v row count does not match the exactly reserved ID range: %d rows, %d ids reserved",
				f.GetPaths(), fileStat.GetTotalRows(), reserved), ErrIDRangeTooSmall)
		}
	}
	return nil
}

// logIDRangeSize sizes the log id range of one import request.
//
// A row belongs to exactly one (vchannel, partition) bucket, so the number of non-empty sync
// batches is at most totalRows and each sync batch writes at most binlogNum log files, making
// (totalRows+1)*binlogNum a strict upper bound on log id consumption. The expansion factor is
// kept as headroom. The result is clamped to math.MaxUint32 so common.AllocAutoID's uint32
// parameter never wraps (issue #52632).
//
// Compatibility: the size counts every row, because a datanode older than the ID ranges
// takes the row ids from the log id range. After all such datanodes are gone, size it by the
// sync count instead.
func logIDRangeSize(schema *schemapb.CollectionSchema, files []*datapb.ImportFileStats) int64 {
	var totalRows int64
	for _, fileStat := range files {
		totalRows += fileStat.GetTotalRows()
	}
	if totalRows < 0 {
		totalRows = 0
	}

	fieldsNum := len(schema.GetFields()) + 2 // userFields + tsField + rowIDField
	binlogNum := int64(fieldsNum + 2)        // binlogs + statslog + BM25Statslog
	if binlogNum < 1 {
		binlogNum = 1
	}
	expansionFactor := paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.GetAsInt64()
	if expansionFactor < 1 {
		expansionFactor = 1
	}

	factor := binlogNum * expansionFactor
	if factor <= 0 {
		factor = 1
	}
	if totalRows+1 > int64(math.MaxUint32)/factor {
		return int64(math.MaxUint32)
	}
	return (totalRows + 1) * factor
}
