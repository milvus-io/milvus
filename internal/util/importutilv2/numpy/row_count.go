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
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

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
	if err := ValidateSchema(schema); err != nil {
		return 0, 0, 0, err
	}
	var rows, logicalBytes, fileSize int64
	for _, path := range SourcePaths(schema, paths) {
		ra, err := common.NewSizingReaderAt(ctx, cm, path)
		if err != nil {
			return 0, 0, 0, err
		}
		fileSize += ra.FileSize()
		dataOff, err := parseHeaderPrefix(ra, path)
		if err != nil {
			return 0, 0, 0, err
		}
		r, err := parseHeader(ra)
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
		elems := shapeProduct(shape)
		itemSize := npyDataTypeSize(r.Header.Descr.Type)
		if elems > 0 {
			// The header alone cannot prove the payload is complete: a truncated
			// file keeps its declared shape and fails only when a reader walks
			// past the last byte. Detect that here, by arithmetic, so the
			// count-only preimport fails the job exactly like V2's full read
			// does -- instead of letting Reshard retry an immutable corrupt
			// object forever.
			// Object dtype ('O') holds a pickled payload, not fixed-size
			// elements: the declared shape says nothing about the byte count, so
			// skip the check (the reader rejects object dtype as unsupported).
			if itemSize > 0 && !npyIsObjectDtype(r.Header.Descr.Type) {
				expected := elems * itemSize
				if available := ra.FileSize() - dataOff; available < expected {
					return 0, 0, 0, merr.WrapErrImportFailedMsg(
						"numpy file is truncated: unexpected EOF, path=%s, header declares %d data bytes, file holds %d",
						path, expected, max(available, 0))
				}
			}
			logicalBytes += elems * itemSize
		}
	}
	return rows, logicalBytes, fileSize, nil
}

// npyIsObjectDtype reports the NumPy object dtype ('O'): its payload is a
// pickle, so the element count cannot be multiplied into a byte size.
func npyIsObjectDtype(dtype string) bool {
	s := dtype
	if len(s) > 0 && (s[0] == '<' || s[0] == '>' || s[0] == '|' || s[0] == '=') {
		s = s[1:]
	}
	return len(s) > 0 && s[0] == 'O'
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
