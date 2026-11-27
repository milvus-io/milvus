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

// This file is the single boundary between Milvus and npyio's header parser.
// npyio trusts its input: it allocates the declared header length verbatim and
// slices on the last '\n' without checking that one exists. Callers that open a
// .npy source validate the prefix first (parseHeaderPrefix) and parse through
// parseHeader, so a malformed file fails its import task instead of panicking
// the worker.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// maxHeaderLen caps the header length a .npy file may declare. numpy pads the
// header to a 64-byte boundary and it carries a single dtype/shape dict, so real
// headers are a few hundred bytes; a v1 length is a uint16 and can never exceed
// this bound at all.
const maxHeaderLen = 1 << 16

// parseHeaderPrefix checks the preconditions npyio's readHeader assumes but never
// verifies (npy/reader.go:97-101): it allocates the declared header length
// verbatim, then slices on the last '\n' without checking r.err or that a newline
// exists. So a v2 file declaring a 4 GiB header allocates 4 GiB per sizing
// goroutine, and a header carrying no newline yields index -1 and panics. Both run
// on the coordinator here, where a panic is not recoverable by the caller: the
// sizing pool's worker is a separate goroutine.
//
// Only the prefix is read, via ReadAt. On backends whose ReadAt moves the
// sequential offset (minio-go's Object.ReadAt), the caller must rewind the
// reader before parsing; CreateReaders does so. NumRowsAndBytes uses an
// independent sizing reader, so it needs no rewind.
//
// It also returns the offset of the first data byte (magic + version + length
// field + header dict), so the caller can check the file actually holds the
// payload its shape declares.
func parseHeaderPrefix(ra io.ReaderAt, path string) (int64, error) {
	var prefix [12]byte
	n, err := ra.ReadAt(prefix[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if n < 10 || [6]byte(prefix[0:6]) != npyio.Magic {
		return 0, merr.WrapErrImportFailedMsg("not a numpy file, path=%s", path)
	}
	var hdrLen, hdrOff int64
	switch major := prefix[6]; major {
	case 1:
		hdrLen, hdrOff = int64(binary.LittleEndian.Uint16(prefix[8:10])), 10
	case 2:
		if n < 12 {
			return 0, merr.WrapErrImportFailedMsg("numpy v2 header truncated, path=%s", path)
		}
		hdrLen, hdrOff = int64(binary.LittleEndian.Uint32(prefix[8:12])), 12
	default:
		return 0, merr.WrapErrImportFailedMsg("unsupported numpy major version %d, path=%s", major, path)
	}
	if hdrLen <= 0 || hdrLen > maxHeaderLen {
		return 0, merr.WrapErrImportFailedMsg("numpy header length %d out of range (max %d), path=%s",
			hdrLen, maxHeaderLen, path)
	}
	hdr := make([]byte, hdrLen)
	if _, err := ra.ReadAt(hdr, hdrOff); err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if bytes.IndexByte(hdr, '\n') < 0 {
		return 0, merr.WrapErrImportFailedMsg("numpy header is not newline-terminated, path=%s", path)
	}
	return hdrOff + hdrLen, nil
}

// parseHeader parses the .npy header through npyio, converting a parser panic
// into an error. npyio trusts its input -- a header without a trailing newline
// slices with index -1 -- and the import pool re-panics by default, so an
// uncontained panic takes the worker (and in standalone the whole process) down
// instead of failing the task. Callers classify the result with
// common.WrapDecodeErr: the input itself is what violates the parser.
func parseHeader(r io.Reader) (hdr *npy.Reader, err error) {
	defer func() {
		if v := recover(); v != nil {
			hdr, err = nil, fmt.Errorf("numpy header parser panic: %v", v)
		}
	}()
	return npyio.NewReader(r)
}
