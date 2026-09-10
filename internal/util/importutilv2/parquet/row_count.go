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

package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	magic          = []byte("PAR1")
	magicEncrypted = []byte("PARE")
)

// footerMaxSize returns the largest footer metadata length import sizing
// will read, from dataCoord.import.parquetFooterMaxSize.
//
// Arrow reads the declared length from the file's last 8 bytes
// (parquet/file/file_reader.go:174) and allocates it verbatim (:182, :202), and
// common.SizingReaderAt.ReadAt buffers a whole ranged GET before copying, so a parse
// costs about twice the declared length.
//
// This is stricter than the DataNode reader, which bounds the footer only by the
// file's own size, so a file above the limit is refused at submit although the
// reader would have read it. Footer size tracks row_groups * columns: 128 MiB row
// groups under Milvus's default 64-field ceiling land in the low single-digit MiB,
// but small row groups, wide schemas or untruncated string statistics do not, which
// is why the limit is configurable rather than fixed.
func footerMaxSize() int64 {
	v := paramtable.Get().DataCoordCfg.ImportParquetFooterMaxSize.GetAsInt64()
	if v <= 0 {
		return defaultFooterMaxSize
	}
	return v
}

// defaultFooterMaxSize mirrors the paramtable default; used when the
// configured value is missing or nonsensical.
const defaultFooterMaxSize = 64 << 20

// maxConcurrentFooterParses bounds how many parquet footers are parsed at
// once across the whole process.
//
// footerMaxSize bounds the bytes read, not the memory the decode takes:
// Arrow's generated reader sizes []*RowGroup from the element count the footer
// declares (parquet/internal/gen-go/parquet/parquet.go:12252) before reading a
// single element, while thrift's guard compares that count against a *byte*
// limit (thrift/configuration.go:305, default 100 MiB). A 6-byte footer may
// therefore declare 104857600 elements and allocate 800 MiB of pointers. The cap
// is process-wide, not per-request, because every import request builds its own
// sizing pool and nothing above them limits how many run at once.
const maxConcurrentFooterParses = 4

var footerParseSem = make(chan struct{}, maxConcurrentFooterParses)

// footerGateSlowWait is how long a sizing pass may wait for the gate
// before the wait is worth a log line.
const footerGateSlowWait = 5 * time.Second

// validateFooter rejects an out-of-range declared footer length before Arrow
// allocates it. common.SizingReaderAt.ReadAt buffers a whole ranged GET before copying, so
// each parse costs about twice the declared length.
//
// This bounds the bytes one file may read, which is not the same as the memory its
// decode takes -- see maxConcurrentFooterParses, which bounds how many parses
// run at once and is what actually caps the coordinator's exposure. A per-pass byte
// budget is still absent; the concurrency cap stands in for it.
func validateFooter(ra io.ReaderAt, size int64, path string) error {
	// Leading magic (4) + metadata length (4) + trailing magic (4).
	const minParquetSize = 12
	if size < minParquetSize {
		return merr.WrapErrImportFailedMsg("parquet file too small, size=%d, path=%s", size, path)
	}
	var tail [8]byte
	if _, err := ra.ReadAt(tail[:], size-int64(len(tail))); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if !bytes.Equal(tail[4:], magic) && !bytes.Equal(tail[4:], magicEncrypted) {
		return merr.WrapErrImportFailedMsg("not a parquet file, path=%s", path)
	}
	footerLen := int64(binary.LittleEndian.Uint32(tail[:4]))
	if maxLen := footerMaxSize(); footerLen <= 0 || footerLen > maxLen {
		return merr.WrapErrImportFailedMsg(
			"parquet footer length %d out of range (max %d, dataCoord.import.parquetFooterMaxSize), path=%s",
			footerLen, maxLen, path)
	}
	return nil
}

// NumRows reads only the parquet footer to get the exact row count via a
// common.SizingReaderAt, which retries the ranged reads the footer relies on. Any error
// from NewParquetReader is therefore a genuine file-format problem, not a transient
// fault, and is returned as a non-retryable import error.
//
// The concurrency gate below is deliberately attached here rather than to NewReader:
// only the pre-broadcast sizing pass parses footers on the coordinator, and putting
// the gate on the DataNode read path would serialize ordinary imports at its width.
func NumRows(ctx context.Context, cm storage.ChunkManager, path string) (int64, error) {
	ra, err := common.NewSizingReaderAt(ctx, cm, path)
	if err != nil {
		return 0, err
	}
	if err := validateFooter(ra, ra.FileSize(), path); err != nil {
		return 0, err
	}

	// Hold the gate across the decode only. The reads above are ordinary object
	// storage traffic with their own retry/backoff, and holding a global slot
	// through them would flatten the caller's pool to this gate's width and let
	// one slow request stall every other import's sizing pass.
	//
	// The gate is process-wide and narrow, so concurrent submissions of
	// footer-heavy files queue behind each other and lengthen the broadcast RPC.
	// That is the intended trade against unbounded decode allocation, but it is
	// invisible from the outside, so a wait worth noticing is logged.
	gateStart := time.Now()
	select {
	case footerParseSem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if waited := time.Since(gateStart); waited > footerGateSlowWait {
		mlog.Warn(ctx, "waited for the parquet footer decode gate",
			mlog.String("path", path),
			mlog.Duration("waited", waited),
			mlog.Int("gateWidth", maxConcurrentFooterParses))
	}
	defer func() { <-footerParseSem }()

	pr, err := file.NewParquetReader(ra)
	if err != nil {
		return 0, common.WrapDecodeErr(err, fmt.Sprintf("read parquet footer failed, path=%s", path))
	}
	defer pr.Close()
	return pr.NumRows(), nil
}
