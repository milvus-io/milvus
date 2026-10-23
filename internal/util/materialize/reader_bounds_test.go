package materialize

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/materialize/internal/flatbuf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// compressibleRecord has a nullable int64 column (buffers 0 validity, 1
// values) and a utf8 column (2 validity, 3 offsets, 4 values), all
// compressible, so that zstd keeps every buffer compressed.
func compressibleRecord(t *testing.T, rows int) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < rows; i++ {
		if i%7 == 3 {
			b.Field(0).AppendNull()
			b.Field(1).AppendNull()
			continue
		}
		b.Field(0).(*array.Int64Builder).Append(int64(i % 4))
		b.Field(1).(*array.StringBuilder).Append("abcabcabcabcabcabc")
	}
	return b.NewRecord()
}

// The uncompressed-length prefix of a compressed buffer is bounded by what
// its column can need, before anything is allocated: a prefix past it is a
// ServiceInternal error, and no allocation of the claimed size happens.
func TestReadColumns_DecompressPrefixBound(t *testing.T) {
	const rows = 10000
	rec := compressibleRecord(t, rows)
	defer rec.Release()
	s, ze, err := EncodeBatch(rec, CodecZstd)
	require.NoError(t, err)
	schema, err := readSchema(s)
	require.NoError(t, err)
	md := recordBatch(t, ze)
	metaEnd := int64(8 + int(binary.LittleEndian.Uint32(ze[4:])))
	prefixAt := func(i int) int64 {
		var b flatbuf.Buffer
		require.True(t, md.Buffers(&b, i))
		return metaEnd + b.Offset()
	}
	// Every buffer must be compressed for the cases to mean anything.
	for i := 0; i < 5; i++ {
		require.NotEqual(t, int64(-1), int64(binary.LittleEndian.Uint64(ze[prefixAt(i):])), "buffer %d stored raw", i)
	}
	for _, rawSize := range []int64{0, RawSize(rec)} {
		_, _, err = readColumns(schema, [][]byte{ze}, 0, rawSize)
		require.NoError(t, err, "the untouched entry reads, body_raw_size %d", rawSize)
	}

	withPrefix := func(buf int, v int64) []byte {
		out := append([]byte(nil), ze...)
		binary.LittleEndian.PutUint64(out[prefixAt(buf):], uint64(v))
		return out
	}
	cases := []struct {
		name    string
		buf     int
		prefix  int64
		rawSize int64
	}{
		{"validity past (rows+7)/8", 0, bufferLimit((rows+7)/8) + 1, 0},
		{"validity claims 1 GiB", 0, 1 << 30, 0},
		{"int64 values past rows*8", 1, bufferLimit(rows*8) + 1, 0},
		{"int64 values claim 1.9 GiB", 1, 1900 << 20, 0},
		{"offsets past (rows+1)*4", 3, bufferLimit((rows+1)*4) + 1, 0},
		{"var-len values past body_raw_size", 4, 4097, 4096},
		{"var-len values past 2 GiB", 4, maxDecompressed + 1, 0},
	}
	_, _ = decoder() // built once, outside the measurement
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := withPrefix(tc.buf, tc.prefix)
			for _, parts := range [][][]byte{{entry}, everyN(entry, 5)} {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				_, _, err := readColumns(schema, parts, 0, tc.rawSize)
				runtime.ReadMemStats(&after)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.ErrorContains(t, err, "needs at most")
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "nothing near the claimed size is allocated")
			}
		})
	}
}

// A frame that inflates past its buffer's prefix fails with ServiceInternal
// and never writes past the prefix: with a frame content size (as Arrow's
// writer emits) and without one (a streamed frame, only caught while
// decoding).
func TestDecompress_FrameInflatesPastPrefix(t *testing.T) {
	plain := make([]byte, 64<<20) // zeros: 64 MiB in a few KB of zstd
	withFCS, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	fcsFrame := withFCS.EncodeAll(plain, nil)
	require.NoError(t, withFCS.Close())
	var streamed bytes.Buffer
	sw, err := zstd.NewWriter(&streamed)
	require.NoError(t, err)
	_, err = sw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, sw.Close())
	hdr := zstd.Header{}
	require.NoError(t, hdr.Decode(streamed.Bytes()))
	require.False(t, hdr.HasFCS, "the streamed frame must not state its size")

	_, _ = decoder()
	for name, frame := range map[string][]byte{"with content size": fcsFrame, "streamed": streamed.Bytes()} {
		for _, prefix := range []int64{0, 1024, 1 << 20} {
			t.Run(name, func(t *testing.T) {
				buf := make([]byte, 8, 8+len(frame))
				binary.LittleEndian.PutUint64(buf, uint64(prefix))
				buf = append(buf, frame...)
				r := &entryReader{p: newPieces([][]byte{buf})}
				r.cur = &cursor{p: r.p}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				_, err := r.decompress(0, span{p: r.p, n: len(buf)}, maxDecompressed)
				runtime.ReadMemStats(&after)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(prefix)+(8<<20), "decoding stopped near the prefix")
			})
		}
	}

	// The same frame with its true length decodes.
	buf := binary.LittleEndian.AppendUint64(nil, uint64(len(plain)))
	buf = append(buf, fcsFrame...)
	r := &entryReader{p: newPieces([][]byte{buf})}
	r.cur = &cursor{p: r.p}
	out, err := r.decompress(0, span{p: r.p, n: len(buf)}, maxDecompressed)
	require.NoError(t, err)
	require.Equal(t, len(plain), out.n)
}
