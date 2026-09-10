package numpy

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
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

// Every path redundant means the file carries no readable column. Sizing reports
// 0 instead of an error: rejecting here would again be stricter than the reader,
// which is where "no file for field" is raised, as it was before sizing existed.
func Test_NumRows_allPathsRedundant(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	rows, err := NumRows(context.Background(), cm, numpySchema("vec"),
		[]string{"README.npy", "LICENSE.npy"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}
