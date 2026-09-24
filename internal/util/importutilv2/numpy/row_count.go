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

package numpy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/sbinet/npyio"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// maxHeaderLen caps the header length a .npy file may declare. numpy pads the
// header to a 64-byte boundary and it carries a single dtype/shape dict, so real
// headers are a few hundred bytes; a v1 length is a uint16 and can never exceed
// this bound at all.
const maxHeaderLen = 1 << 16

// validateHeaderPrefix checks the preconditions npyio's readHeader assumes but never
// verifies (npy/reader.go:97-101): it allocates the declared header length
// verbatim, then slices on the last '\n' without checking r.err or that a newline
// exists. So a v2 file declaring a 4 GiB header allocates 4 GiB per sizing
// goroutine, and a header carrying no newline yields index -1 and panics. Both run
// on the coordinator here, where a panic is not recoverable by the caller: the
// sizing pool's worker is a separate goroutine.
//
// Only the prefix is read, via ReadAt, so the reader's sequential offset is
// untouched and npyio still decodes from the start.
func validateHeaderPrefix(ra io.ReaderAt, path string) error {
	var prefix [12]byte
	n, err := ra.ReadAt(prefix[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < 10 || [6]byte(prefix[0:6]) != npyio.Magic {
		return merr.WrapErrImportFailedMsg("not a numpy file, path=%s", path)
	}
	var hdrLen, hdrOff int64
	switch major := prefix[6]; major {
	case 1:
		hdrLen, hdrOff = int64(binary.LittleEndian.Uint16(prefix[8:10])), 10
	case 2:
		if n < 12 {
			return merr.WrapErrImportFailedMsg("numpy v2 header truncated, path=%s", path)
		}
		hdrLen, hdrOff = int64(binary.LittleEndian.Uint32(prefix[8:12])), 12
	default:
		return merr.WrapErrImportFailedMsg("unsupported numpy major version %d, path=%s", major, path)
	}
	if hdrLen <= 0 || hdrLen > maxHeaderLen {
		return merr.WrapErrImportFailedMsg("numpy header length %d out of range (max %d), path=%s",
			hdrLen, maxHeaderLen, path)
	}
	hdr := make([]byte, hdrLen)
	if _, err := ra.ReadAt(hdr, hdrOff); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if bytes.IndexByte(hdr, '\n') < 0 {
		return merr.WrapErrImportFailedMsg("numpy header is not newline-terminated, path=%s", path)
	}
	return nil
}

// NumRows reads only the .npy headers to get the exact row count of one
// column-based import file. Every path belongs to the same rows, so their shapes
// agree on a well-formed input; the maximum is taken so a disagreement can only
// over-reserve rather than under-reserve (the reader rejects the mismatch later).
// Paths for omitted nullable/default columns are simply absent and contribute
// nothing.
//
// Only the paths the reader will open are inspected. Sizing runs before the
// broadcast, so validating a path the reader ignores would reject at submit an
// input that imports fine -- a redundant .npy naming no schema field is dropped
// by CreateReaders, and must be dropped here by the same rule. When that
// leaves no path at all, this returns 0 rather than an error: the file carries no
// readable column, and saying so is the reader's job, at the same place it said
// so before this sizing stage existed.
func NumRows(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, paths []string) (int64, error) {
	rows, _, _, err := NumRowsAndBytes(ctx, cm, schema, paths)
	return rows, err
}

// NumRowsAndBytes reads only the .npy headers to get the exact row count, the
// decoded column size (shape x dtype item size, summed over the columns), and the
// physical size of all source paths. The decoded size is the logical size the
// Import V3 reshard BFD packs tasks by; the physical size comes for free from the
// SizingReaderAt so callers need not size the objects again.
func NumRowsAndBytes(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, paths []string) (int64, int64, int64, error) {
	var rows, logicalBytes, fileSize int64
	for _, path := range SourcePaths(schema, paths) {
		ra, err := common.NewSizingReaderAt(ctx, cm, path)
		if err != nil {
			return 0, 0, 0, err
		}
		fileSize += ra.FileSize()
		if err := validateHeaderPrefix(ra, path); err != nil {
			return 0, 0, 0, err
		}
		r, err := npyio.NewReader(ra)
		if err != nil {
			return 0, 0, 0, common.WrapDecodeErr(err, fmt.Sprintf("read numpy header failed, path=%s", path))
		}
		shape := r.Header.Descr.Shape
		if len(shape) == 0 {
			continue
		}
		if n := int64(shape[0]); n > rows {
			rows = n
		}
		if elems := shapeProduct(shape); elems > 0 {
			logicalBytes += elems * npyDataTypeSize(r.Header.Descr.Type)
		}
	}
	return rows, logicalBytes, fileSize, nil
}

// shapeProduct returns the number of elements described by a NumPy shape, or 0
// when any dimension is non-positive.
func shapeProduct(shape []int) int64 {
	p := int64(1)
	for _, d := range shape {
		if d <= 0 {
			return 0
		}
		p *= int64(d)
	}
	return p
}

// npyDataTypeSize parses a NumPy dtype string (e.g. "<f4", "|u1", "<U10",
// "<M8[ns]", "V12") into its element size in bytes. It returns 0 for an
// unparseable type; the caller then contributes no logical bytes for that column.
func npyDataTypeSize(dtype string) int64 {
	s := dtype
	if len(s) > 0 && (s[0] == '<' || s[0] == '>' || s[0] == '|' || s[0] == '=') {
		s = s[1:]
	}
	if len(s) == 0 {
		return 0
	}
	kind := s[0]
	rest := s[1:]
	if kind == 'M' || kind == 'm' {
		// datetime64/timedelta64: "<M8[ns]" -> strip the unit suffix.
		if i := indexByte(rest, '['); i >= 0 {
			rest = rest[:i]
		}
	}
	switch kind {
	case 'O':
		return 8
	case '?':
		return 1
	}
	var n int64
	hasDigits := false
	for i := 0; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			break
		}
		n = n*10 + int64(rest[i]-'0')
		hasDigits = true
	}
	if !hasDigits {
		return 0
	}
	switch kind {
	case 'U':
		return n * 4
	case 'a', 'S', 'b', 'i', 'u', 'f', 'c', 'V':
		return n
	default:
		return 0
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
