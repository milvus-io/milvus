package materialize

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func testRecord(t *testing.T, rows int, seed int64) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata([]string{"milvus.field_order"}, []string{"100,101"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < rows; i++ {
		v := make([]byte, 32)
		r.Read(v)
		b.Field(0).(*array.FixedSizeBinaryBuilder).Append(v)
		if i%5 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append("the quick brown fox jumps over the lazy dog")
		}
	}
	return b.NewRecord()
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	for _, codec := range []Codec{CodecNone, CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			r1 := testRecord(t, 50, 1)
			defer r1.Release()
			r2 := testRecord(t, 7, 2)
			defer r2.Release()
			s1, e1, err := EncodeBatch(r1, codec)
			require.NoError(t, err)
			s2, e2, err := EncodeBatch(r2, codec)
			require.NoError(t, err)
			require.True(t, bytes.Equal(s1, s2), "same schema must encode identically")

			recs, err := decodeBody(s1, [][]byte{e1, e2})
			require.NoError(t, err)
			require.Len(t, recs, 2)
			defer func() {
				for _, r := range recs {
					r.Release()
				}
			}()
			require.True(t, array.RecordEqual(r1, recs[0]))
			require.True(t, array.RecordEqual(r2, recs[1]))
			require.True(t, recs[0].Schema().Equal(r1.Schema()))
			v, ok := recs[0].Schema().Metadata().GetValue("milvus.field_order")
			require.True(t, ok)
			require.Equal(t, "100,101", v)
		})
	}
}

func TestEncode_ZstdShrinksTextNotVectors(t *testing.T) {
	r := testRecord(t, 2000, 3)
	defer r.Release()
	_, raw, err := EncodeBatch(r, CodecNone)
	require.NoError(t, err)
	_, zst, err := EncodeBatch(r, CodecZstd)
	require.NoError(t, err)
	// Text compresses; random 32-byte vectors (64 000 bytes) do not, and are
	// stored raw, so the compressed entry stays above the vector payload.
	require.Less(t, len(zst), len(raw))
	require.Greater(t, len(zst), 2000*32)
}

func TestRawSize(t *testing.T) {
	r := testRecord(t, 10, 5)
	defer r.Release()
	require.GreaterOrEqual(t, RawSize(r), int64(10*32))
}

func TestResolveCodec(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	cases := []struct {
		codec, compression string
		want               Codec
	}{
		{"none", "true", CodecNone},
		{"none", "false", CodecNone},
		{"zstd", "true", CodecZstd},
		{"zstd", "false", CodecZstd},
		{"", "true", CodecNone},
		{"", "false", CodecNone},
		{"unknown", "true", CodecNone},
	}
	for _, c := range cases {
		t.Run(c.codec+"/"+c.compression, func(t *testing.T) {
			pt.Save(pt.QueryNodeCfg.MaterializeCodec.Key, c.codec)
			pt.Save(pt.QueryNodeGrpcClientCfg.CompressionEnabled.Key, c.compression)
			defer pt.Reset(pt.QueryNodeCfg.MaterializeCodec.Key)
			defer pt.Reset(pt.QueryNodeGrpcClientCfg.CompressionEnabled.Key)
			require.Equal(t, c.want, ResolveCodec())
		})
	}
}

func TestSchemaFingerprint(t *testing.T) {
	encodeSchema := func(rec arrow.Record) []byte {
		s, _, err := EncodeBatch(rec, CodecNone)
		require.NoError(t, err)
		return s
	}
	a1 := testRecord(t, 3, 1)
	defer a1.Release()
	a2 := testRecord(t, 7, 2)
	defer a2.Release()
	md := arrow.NewMetadata([]string{"milvus.field_order"}, []string{"100"})
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, &md))
	b.Field(0).(*array.Int64Builder).Append(1)
	other := b.NewRecord()
	b.Release()
	defer other.Release()

	sa1, sa2, so := encodeSchema(a1), encodeSchema(a2), encodeSchema(other)
	// Same schema, different data: same bytes, same fingerprint.
	require.Equal(t, sa1, sa2)
	require.Equal(t, SchemaFingerprint(sa1), SchemaFingerprint(append([]byte(nil), sa2...)))
	require.NotZero(t, SchemaFingerprint(sa1))
	require.NotEqual(t, SchemaFingerprint(sa1), SchemaFingerprint(so))
}

// streamEncode is the previous EncodeBatch: the stock stream writer into a
// growing buffer, then the stream cut at message boundaries. It is the
// reference the payload writer must match byte for byte.
func streamEncode(t *testing.T, rec arrow.Record, opts ...ipc.Option) ([]byte, []byte) {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, opts...)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	stream := buf.Bytes()
	mr := ipc.NewMessageReader(bytes.NewReader(stream))
	defer mr.Release()
	var bounds []int
	off := 0
	for {
		msg, err := mr.Message()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		meta := int(binary.LittleEndian.Uint32(stream[off+4:]))
		off += 8 + meta + int(msg.BodyLen())
		bounds = append(bounds, off)
	}
	require.Len(t, bounds, 2)
	return stream[:bounds[0]], stream[bounds[0]:bounds[1]]
}

func mixedRecord(t *testing.T, rows int, seed int64) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < rows; i++ {
		b.Field(0).(*array.Int64Builder).Append(r.Int63())
		if i%3 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.Float32Builder).Append(r.Float32())
		}
		b.Field(2).(*array.BooleanBuilder).Append(i%2 == 0)
		b.Field(3).(*array.StringBuilder).Append(strings.Repeat("ab", r.Intn(9)))
	}
	return b.NewRecord()
}

// The payload writer lays out the same bytes as the stock stream writer.
// The stock NewWriter drops WithMinSpaceSavings (only
// NewWriterWithPayloadWriter honors it), so the zstd comparison runs
// without that option on both sides.
func TestEncodeBatch_MatchesStreamWriter(t *testing.T) {
	var recs []arrow.Record
	for i, rows := range []int{0, 1, 7, 50, 2000} {
		recs = append(recs, testRecord(t, rows, int64(i)), mixedRecord(t, rows, int64(i)))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	for _, codec := range []string{"none", "zstd"} {
		for i, rec := range recs {
			opts := []ipc.Option{ipc.WithSchema(rec.Schema()), ipc.WithAllocator(memory.DefaultAllocator)}
			if codec == "zstd" {
				opts = append(opts, ipc.WithZstd())
			}
			wantS, wantE := streamEncode(t, rec, opts...)
			gotS, gotE, err := encodeBatch(rec, opts...)
			require.NoError(t, err)
			require.Equal(t, wantS, gotS, "%s record %d schema", codec, i)
			require.Equal(t, wantE, gotE, "%s record %d batch", codec, i)
		}
	}
}

// decodeBody is the tests' oracle: arrow-go's stock stream reader over the
// Schema message followed by the entries. The caller releases the records.
func decodeBody(schemaMsg []byte, entries [][]byte) ([]arrow.Record, error) {
	readers := []io.Reader{bytes.NewReader(schemaMsg)}
	for _, e := range entries {
		readers = append(readers, bytes.NewReader(e))
	}
	rd, err := ipc.NewReader(io.MultiReader(readers...))
	if err != nil {
		return nil, err
	}
	defer rd.Release()
	var out []arrow.Record
	for rd.Next() {
		rec := rd.Record()
		rec.Retain()
		out = append(out, rec)
	}
	if err := rd.Err(); err != nil {
		for _, r := range out {
			r.Release()
		}
		return nil, err
	}
	return out, nil
}
