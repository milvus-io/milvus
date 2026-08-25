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

package datacoord

import (
	"context"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// maxIDsPerAllocBatch mirrors the per-call ceiling of rootCoordAllocator.AllocN.
const maxIDsPerAllocBatch = int64(math.MaxUint32)

// fileSizing carries one import file through the three reservation stages. rows
// and reservedIDs are deliberately separate fields: a row count is not an id
// count, and the expansion factor turns one into the other in the middle stage.
type fileSizing struct {
	file *internalpb.ImportFile
	// rows is the upper bound on the file's row count, exact when the format
	// records it (parquet footer, npy header shape) and a byte-derived
	// over-estimate otherwise.
	rows  int64
	exact bool
	// reservedIDs is how many ids the file gets, filled by sizeReservations.
	reservedIDs int64
}

// assignPKRangesToFiles computes a per-file row upper bound, allocates one
// contiguous primary-namespace id block sized Σ reservedIDs via allocN, then writes each
// file its own contiguous pre_allocated_auto_ids slice in the given files order.
// It is called on the PRIMARY at import broadcast for non-backup autoID imports;
// the ranges then travel on the replicated ImportMsg so both clusters derive
// identical primary keys. The layout is bound to each file object, so it survives
// later file regrouping/reordering.
func assignPKRangesToFiles(ctx context.Context, cm storage.ChunkManager,
	schema *schemapb.CollectionSchema, files []*internalpb.ImportFile,
	allocN func(int64) (int64, int64, error), clusterID uint64,
) error {
	sizings, err := computeFileRowUpperBounds(ctx, cm, schema, files)
	if err != nil {
		return err
	}
	if err := sizeReservations(sizings); err != nil {
		return err
	}
	return reserveRanges(sizings, allocN, clusterID)
}

// computeFileRowUpperBounds sizes every file concurrently. Sizing is object-store
// IO (a HEAD per path, or a footer/header read) on the import broadcast path, and
// the request may carry up to dataCoord.maxFilesPerImportReq files, so doing it
// serially would turn a millisecond RPC into a minutes-long one.
func computeFileRowUpperBounds(ctx context.Context, cm storage.ChunkManager,
	schema *schemapb.CollectionSchema, files []*internalpb.ImportFile,
) ([]fileSizing, error) {
	sizings := make([]fileSizing, len(files))
	// Conceal panics so a decoder crash fails this import instead of the process.
	// conc.Submit's recover already stores the panic in the future before
	// re-throwing; concealing stops ants from re-panicking on a worker goroutine,
	// which no caller here could recover. The format packages guard the decoder
	// panic known today -- this covers the ones parquet or a future npyio has left.
	pool := conc.NewPool[struct{}](hardware.GetCPUNum()*2, conc.WithConcealPanic(true))
	defer pool.Release()

	futures := make([]*conc.Future[struct{}], 0, len(files))
	for i, f := range files {
		i, f := i, f
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			rows, exact, err := importutilv2.RowCountUpperBound(ctx, cm, schema, f)
			if err != nil {
				return struct{}{}, err
			}
			sizings[i] = fileSizing{file: f, rows: rows, exact: exact}
			return struct{}{}, nil
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}
	return sizings, nil
}

// sizeReservations turns per-file row counts into per-file id reservations.
//
// An exact count gets the configured expansion factor as headroom, matching how
// import already over-reserves logIDs; the headroom absorbs a reader producing
// slightly more rows than the footer/header advertised. A byte-derived estimate is
// already a gross over-estimate, so multiplying it would only waste id space.
//
// Every reservation is at least one id. The datanode reads an empty range as "no
// range" and silently falls back to its local allocator, which is the divergence
// this whole mechanism exists to prevent, so a zero-row file still gets a slice.
//
// An exact count is never shrunk to fit the allocation ceiling: it is the file's
// real row count, so handing back fewer ids than that is a silent
// under-reservation. A byte-derived estimate is capped instead of refused. It
// tracks bytes rather than rows -- a single-column CSV has no provable per-row
// floor, so its bound is simply the file size -- and refusing on that number
// rejects a legal import for being large rather than for holding too many rows.
// Capping is safe because the estimate is not the last word: AssembleImportRequest
// compares pre-import's exact row count against the reservation and fails the job,
// with both numbers, before any segment is written.
func sizeReservations(sizings []fileSizing) error {
	factor := paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.GetAsInt64()
	if factor < 1 {
		factor = 1
	}
	for i := range sizings {
		b := sizings[i].rows
		if sizings[i].exact {
			// A file needing more ids than one batch holds can never be reserved
			// contiguously, and clamping it here would hand back fewer ids than the
			// file has rows -- a silent under-reservation in a mechanism whose whole
			// premise is that the bound never under-counts. reserveRanges cannot
			// catch it either: it only ever sees the post-clamp value. Reject on the
			// raw count, while it is still visible.
			if b > maxIDsPerAllocBatch {
				return merr.WrapErrParameterInvalidMsg(
					"import file %d holds %d rows, more than one allocation batch can reserve (max %d); split the file",
					i, b, maxIDsPerAllocBatch)
			}
			// Exact counts are authoritative; the factor only adds headroom for a
			// reader emitting marginally more rows than the footer/header advertised.
			// Cap that headroom at the allocation-batch ceiling so an ordinary large
			// file (e.g. a 500M-row column, ~4GB, well under the size limit) is not
			// rejected merely for crossing rows*factor.
			if b <= maxIDsPerAllocBatch/factor {
				b *= factor
			} else {
				b = maxIDsPerAllocBatch
			}
		} else if b > maxIDsPerAllocBatch {
			// A json/csv bound is the file size divided by a provable per-row byte
			// floor, and that floor collapses to 1 for a schema whose source columns
			// may all be empty (a single VarChar column, say). The bound then counts
			// bytes, not rows: a 4 GiB CSV of 500-byte rows bounds at ~4.3e9 for
			// ~8.6M real rows. Reserve one batch -- the most a contiguous range can
			// hold -- rather than refuse the file. A genuine overrun is caught at
			// assemble time against the exact count, which is where an estimate
			// should be settled.
			b = maxIDsPerAllocBatch
		}
		if b < 1 {
			b = 1
		}
		sizings[i].reservedIDs = b
	}
	return nil
}

// reserveRanges writes each file its own contiguous pre_allocated_auto_ids slice,
// chunking the allocation so that no single allocN call exceeds the allocator's
// per-batch ceiling. Files are packed greedily and a file's range never straddles
// two batches, so every range stays contiguous while the import as a whole is not
// capped at one batch: the reservation total may exceed the ceiling, only a single
// file may not.
func reserveRanges(sizings []fileSizing,
	allocN func(int64) (int64, int64, error), clusterID uint64,
) error {
	for i := 0; i < len(sizings); {
		var batch int64
		j := i
		for ; j < len(sizings); j++ {
			if sizings[j].reservedIDs > maxIDsPerAllocBatch {
				// Unreachable through sizeReservations, which refuses an exact count
				// above the ceiling and caps an estimate at it. Kept because a change
				// there would otherwise produce a range straddling two batches, which
				// the datanode's one cursor per file cannot walk -- an internal
				// invariant, not something the request content can provoke.
				return merr.WrapErrImportSysFailedMsg(
					"import file %d reserved %d primary keys, more than one allocation batch holds (max %d)",
					j, sizings[j].reservedIDs, maxIDsPerAllocBatch)
			}
			if batch+sizings[j].reservedIDs > maxIDsPerAllocBatch {
				break
			}
			batch += sizings[j].reservedIDs
		}
		if batch == 0 {
			// Only reachable once every remaining bound is zero, which
			// sizeReservations rules out; guards against a non-advancing loop.
			return nil
		}
		begin, _, err := common.AllocAutoIDN(allocN, batch, clusterID)
		if err != nil {
			return err
		}
		cur := begin
		for k := i; k < j; k++ {
			sizings[k].file.PreAllocatedAutoIds = &commonpb.IDRange{
				Begin: cur,
				End:   cur + sizings[k].reservedIDs,
			}
			cur = sizings[k].file.GetPreAllocatedAutoIds().GetEnd()
		}
		i = j
	}
	return nil
}
