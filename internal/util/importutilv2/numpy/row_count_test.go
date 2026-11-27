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
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// malformedNpy builds a .npy prefix whose declared header length is hdrLen but
// whose header bytes contain no '\n'. npyio's readHeader does not check r.err
// between reading those bytes and slicing on the last newline, so an unguarded
// decode panics with "slice bounds out of range [:-1]".
func malformedNpy(major byte, hdrLen uint32, body []byte) []byte {
	buf := []byte{'\x93', 'N', 'U', 'M', 'P', 'Y', major, 0}
	switch major {
	case 1:
		buf = binary.LittleEndian.AppendUint16(buf, uint16(hdrLen))
	default:
		buf = binary.LittleEndian.AppendUint32(buf, hdrLen)
	}
	return append(buf, body...)
}

func cmServing(t *testing.T, path string, data []byte) *mocks.ChunkManager {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, path).Return(int64(len(data)), nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, path, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			if off >= int64(len(data)) {
				return nil, io.EOF
			}
			end := off + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return data[off:end], nil
		}).Maybe()
	return cm
}

// numpySchema names one Int64 field per .npy basename under test, so every path
// survives SourcePaths and reaches the header decode.
func numpySchema(names ...string) *schemapb.CollectionSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(names))
	for i, name := range names {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(100 + i), Name: name, DataType: schemapb.DataType_Int64,
		})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

func Test_NumRows_malformedHeader(t *testing.T) {
	cases := map[string][]byte{
		// The review's repro: a 10-byte object declaring a zero-length header.
		"zero length header": malformedNpy(1, 0, nil),
		// Declared length is satisfied but carries no newline to slice on.
		"no newline in header": malformedNpy(1, 8, []byte("{'descr'")),
		// v2 takes the header length from a uint32, so the declared size is bounded
		// only by 4 GiB -- reading it before validating is an allocation DoS on its
		// own, independent of the slice panic.
		"huge declared header": malformedNpy(2, math.MaxUint32, []byte("x")),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			cm := cmServing(t, "bad.npy", data)
			assert.NotPanics(t, func() {
				_, err := NumRows(context.Background(), cm, numpySchema("bad"), []string{"bad.npy"})
				assert.Error(t, err)
			})
		})
	}
}

// A path whose basename names no schema field is redundant: CreateReaders
// skips it, so sizing must skip it too. Sizing runs before the broadcast, so a
// redundant .npy that is broken (or missing from the bucket) used to fail the
// whole request at submit even though the same file set imported fine.
//
// The ChunkManager is given no expectation for the redundant path at all, so the
// test fails if sizing so much as calls Size on it -- proving the path is never
// opened, not merely that its decode error is swallowed.
func Test_NumRows_skipsRedundantPaths(t *testing.T) {
	buf := new(bytes.Buffer)
	require.NoError(t, npyio.Write(buf, []int64{1, 2, 3}))

	cm := cmServing(t, "vec.npy", buf.Bytes())
	rows, err := NumRows(context.Background(), cm, numpySchema("vec"),
		[]string{"vec.npy", "README.npy"})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), rows)
}

// A file set naming no field must fail like CreateReaders does when a required
// field has no source; the count-only preimport would otherwise finish a
// zero-row job as a success. The ChunkManager has no expectation, pinning that
// the rejection opens no object.
func Test_NumRows_allPathsRedundant(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	_, err := NumRows(context.Background(), cm, numpySchema("vec"),
		[]string{"README.npy", "LICENSE.npy"})
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrImportFailed),
		"a file set naming no field must be an input error: %v", err)
	assert.Contains(t, err.Error(), "no file for field: vec")
}

// An all-optional schema accepts the same set: no column to read, zero rows.
func Test_NumRows_allPathsRedundantAllOptional(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{FieldID: 101, Name: "note", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	rows, err := NumRows(context.Background(), cm, schema, []string{"README.npy"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

// Test_NumRowsAndBytesReportsDecodedLogicalSize pins the success-path byte math
// that feeds the reshard BFD: logicalBytes is shapeProduct(shape) x
// npyDataTypeSize(dtype) summed over the columns, and the physical size is the
// summed object size.
func Test_NumRowsAndBytesReportsDecodedLogicalSize(t *testing.T) {
	t.Run("int64 column", func(t *testing.T) {
		const path = "vec.npy"
		buf := new(bytes.Buffer)
		require.NoError(t, npyio.Write(buf, []int64{1, 2, 3}))
		cm := cmServing(t, path, buf.Bytes())

		rows, logical, size, err := NumRowsAndBytes(context.Background(), cm, numpySchema("vec"), []string{path})
		require.NoError(t, err)
		require.Equal(t, int64(3), rows)
		// shapeProduct([3]) x npyDataTypeSize("<i8")
		require.Equal(t, int64(3*8), logical)
		require.Equal(t, int64(len(buf.Bytes())), size)
	})

	t.Run("float32 column", func(t *testing.T) {
		const path = "fvec.npy"
		buf := new(bytes.Buffer)
		require.NoError(t, npyio.Write(buf, []float32{1, 2, 3, 4}))
		cm := cmServing(t, path, buf.Bytes())
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "fvec", DataType: schemapb.DataType_Float},
		}}

		rows, logical, _, err := NumRowsAndBytes(context.Background(), cm, schema, []string{path})
		require.NoError(t, err)
		require.Equal(t, int64(4), rows)
		// shapeProduct([4]) x npyDataTypeSize("<f4")
		require.Equal(t, int64(4*4), logical)
	})
}

// Test_NumRowsAndBytesRejectsTruncatedPayload pins that a .npy whose header
// declares more data than the object holds is rejected at preimport with a
// terminal input error. The count-only preimport must fail the job here, like
// V2's full read does: the payload is immutable, so Reshard retrying it can
// never succeed.
func Test_NumRowsAndBytesRejectsTruncatedPayload(t *testing.T) {
	buf := new(bytes.Buffer)
	require.NoError(t, npyio.Write(buf, []int64{1, 2, 3, 4}))
	payload := buf.Bytes()
	require.Greater(t, len(payload), 4)

	cm := cmServing(t, "pk.npy", payload[:len(payload)-3])
	_, _, _, err := NumRowsAndBytes(context.Background(), cm, numpySchema("pk"), []string{"pk.npy"})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "eof")
	require.True(t, errors.Is(err, merr.ErrImportFailed),
		"a truncated payload must be an input error: %v", err)
}

// TestShapeProductAndDataTypeSize pins the two byte-math helpers the decoded
// size is built from: shapeProduct multiplies the dimensions (0 when any is
// non-positive) and npyDataTypeSize parses the dtype's item size (0 for an
// unparseable type, so the column contributes no logical bytes).
func TestShapeProductAndDataTypeSize(t *testing.T) {
	assert.Equal(t, int64(3), shapeProduct([]int{3}))
	assert.Equal(t, int64(6), shapeProduct([]int{1, 2, 3}))
	assert.Equal(t, int64(16), shapeProduct([]int{2, 8}))
	assert.Equal(t, int64(0), shapeProduct([]int{2, 0}))

	assert.Equal(t, int64(8), npyDataTypeSize("<i8"))
	assert.Equal(t, int64(4), npyDataTypeSize("<f4"))
	assert.Equal(t, int64(1), npyDataTypeSize("|b1"))
	assert.Equal(t, int64(40), npyDataTypeSize("<U10"))
	assert.Equal(t, int64(8), npyDataTypeSize("O"))
	assert.Equal(t, int64(0), npyDataTypeSize("notatype"))
}

// objectDtypeNpy builds a valid v1 .npy whose dtype is '|O' (a pickled
// payload), padded the way numpy pads its header.
func objectDtypeNpy(t *testing.T, rows int, payload []byte) []byte {
	t.Helper()
	dict := fmt.Sprintf("{'descr': '|O', 'fortran_order': False, 'shape': (%d,), }", rows)
	pad := (64 - (10+len(dict)+1)%64) % 64
	header := dict + strings.Repeat(" ", pad) + "\n"

	buf := new(bytes.Buffer)
	buf.Write([]byte{0x93, 'N', 'U', 'M', 'P', 'Y', 1, 0})
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint16(len(header))))
	buf.WriteString(header)
	buf.Write(payload)
	return buf.Bytes()
}

// Test_NumRowsAndBytesRejectsObjectDtype pins that the count stage applies the
// reader's dtype check: an object array holds a pickle, which numpy import does
// not support, so the job fails here instead of importing zero rows.
func Test_NumRowsAndBytesRejectsObjectDtype(t *testing.T) {
	content := objectDtypeNpy(t, 2, []byte{0x80, 0x04})
	cm := cmServing(t, "vec.npy", content)

	_, _, _, err := NumRowsAndBytes(context.Background(), cm, numpySchema("vec"), []string{"vec.npy"})
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrImportFailed),
		"an unsupported dtype must be an input error: %v", err)
	require.Contains(t, err.Error(), "is not supported")
}

// idAndVectorSchema is a required Int64 primary key and a required 8-dim vector.
func idAndVectorSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
		},
	}}
}

// npyFile builds a .npy header with an explicit dtype and shape.
func npyFile(t *testing.T, descr, shape string, payload []byte) []byte {
	t.Helper()
	dict := fmt.Sprintf("{'descr': '%s', 'fortran_order': False, 'shape': %s, }", descr, shape)
	pad := (64 - (10+len(dict)+1)%64) % 64
	header := dict + strings.Repeat(" ", pad) + "\n"

	buf := new(bytes.Buffer)
	buf.Write([]byte{0x93, 'N', 'U', 'M', 'P', 'Y', 1, 0})
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint16(len(header))))
	buf.WriteString(header)
	buf.Write(payload)
	return buf.Bytes()
}

// cmServingFiles serves .npy objects by path; any other access fails the test.
func cmServingFiles(t *testing.T, files map[string][]byte) *mocks.ChunkManager {
	t.Helper()
	cm := mocks.NewChunkManager(t)
	for path, data := range files {
		cm.EXPECT().Size(mock.Anything, path).Return(int64(len(data)), nil).Maybe()
		cm.EXPECT().ReadAt(mock.Anything, path, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
				if off >= int64(len(data)) {
					return nil, io.EOF
				}
				end := off + length
				if end > int64(len(data)) {
					end = int64(len(data))
				}
				return data[off:end], nil
			}).Maybe()
	}
	return cm
}

// Test_NumRowsValidatesSchemaBeforeZeroRowCompletion covers inputs that count to
// zero: invalid ones fail on the header check, the valid empty pair imports zero rows.
func Test_NumRowsValidatesSchemaBeforeZeroRowCompletion(t *testing.T) {
	t.Run("missing required column", func(t *testing.T) {
		cm := cmServingFiles(t, map[string][]byte{"id.npy": npyFile(t, "<i8", "(0,)", nil)})
		_, _, _, err := NumRowsAndBytes(context.Background(), cm, idAndVectorSchema(), []string{"id.npy"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no file for field: vector")
	})

	t.Run("scalar array", func(t *testing.T) {
		cm := cmServingFiles(t, map[string][]byte{
			"id.npy":     npyFile(t, "<i8", "()", nil),
			"vector.npy": npyFile(t, "<f4", "()", nil),
		})
		_, _, _, err := NumRowsAndBytes(context.Background(), cm, idAndVectorSchema(),
			[]string{"id.npy", "vector.npy"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected shape")
	})

	t.Run("wrong dim", func(t *testing.T) {
		cm := cmServingFiles(t, map[string][]byte{
			"id.npy":     npyFile(t, "<i8", "(0,)", nil),
			"vector.npy": npyFile(t, "<f4", "(0, 7)", nil),
		})
		_, _, _, err := NumRowsAndBytes(context.Background(), cm, idAndVectorSchema(),
			[]string{"id.npy", "vector.npy"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected dim")
	})

	t.Run("wrong element type", func(t *testing.T) {
		cm := cmServingFiles(t, map[string][]byte{
			"id.npy":     npyFile(t, "<f4", "(0,)", nil),
			"vector.npy": npyFile(t, "<f4", "(0, 8)", nil),
		})
		_, _, _, err := NumRowsAndBytes(context.Background(), cm, idAndVectorSchema(),
			[]string{"id.npy", "vector.npy"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected element type")
	})

	t.Run("valid empty columns still import zero rows", func(t *testing.T) {
		cm := cmServingFiles(t, map[string][]byte{
			"id.npy":     npyFile(t, "<i8", "(0,)", nil),
			"vector.npy": npyFile(t, "<f4", "(0, 8)", nil),
		})
		rows, _, _, err := NumRowsAndBytes(context.Background(), cm, idAndVectorSchema(),
			[]string{"id.npy", "vector.npy"})
		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})
}
