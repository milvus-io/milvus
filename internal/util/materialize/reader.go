package materialize

import (
	"encoding/binary"
	"math"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/klauspost/compress/zstd"

	"github.com/milvus-io/milvus/internal/util/materialize/internal/flatbuf"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The reader parses a body entry where it lies: the pieces the release codec
// received it in (design doc 20261019 §3, §7). It copies out the pk and
// system columns and the var-len offsets, which the fill and the gather read
// per row. Validity bitmaps and bool values are views of the entry when they
// lie in one piece, and copied only when they straddle pieces; the metadata
// flatbuffer likewise. Every other value (vectors, strings, binary,
// fixed-width user columns) stays in the entry until the gather copies the
// selected rows into the result (gather.go). So a column, and the Batch that
// holds it, must not be used after the body is released.
//
// A compressed entry (Arrow's per-buffer zstd) is decompressed buffer by
// buffer into contiguous memory, the one copy inherent there, and then read
// the same way. Each buffer's uncompressed-length prefix is bounded before
// anything is allocated, by what the column can need (bufferLimit), and the
// decoder cannot write past that length.
//
// Every offset, length and count is checked against the entry before it is
// used, so a malformed entry is a ServiceInternal error, never a panic, an
// out-of-range read or an allocation beyond those bounds.

// column is one column of an entry, read for gathering.
type column struct {
	typ   arrow.Type
	width int // bytes per value of a fixed-width type (not bool)
	rows  int
	nulls int
	// valid is the validity bitmap, nil when the column has no nulls. A view
	// of the entry (or of the decompressed buffer) unless it straddles
	// pieces: do not use it after the body is released.
	valid []byte
	// values are the fixed-width or var-len values, in the entry.
	values span
	// offsets are a var-len column's rows+1 value offsets (copied), relative
	// to values and checked against it.
	offsets []int32
	// bits are a bool column's values; a view, like valid.
	bits []byte
}

func (c *column) isValid(row int) bool {
	return c.valid == nil || bitutil.BitIsSet(c.valid, row)
}

// readColumns reads one entry, a complete encapsulated RecordBatch message
// held as parts, against schema, whose every field must be in colLayoutOf.
// idx names the entry in errors. rawSize is the reply's body_raw_size, which
// bounds a compressed var-len values buffer; 0 when unknown.
func readColumns(schema *arrow.Schema, parts [][]byte, idx int, rawSize int64) (cols []column, rows int, err error) {
	// The FlatBuffers accessors index the metadata without bounds checks of
	// their own; a corrupt table can make them panic.
	defer func() {
		if p := recover(); p != nil {
			cols, rows, err = nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: corrupt metadata: %v", idx, p)
		}
	}()
	p := newPieces(parts)
	total := p.size()
	cur := cursor{p: p}
	if total < 8 {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: missing continuation marker", idx)
	}
	var prefix [8]byte
	cur.read(prefix[:], 0)
	if binary.LittleEndian.Uint32(prefix[:]) != contToken {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: missing continuation marker", idx)
	}
	metaEnd := 8 + int64(binary.LittleEndian.Uint32(prefix[4:]))
	if metaEnd > int64(total) {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: metadata length %d exceeds %d bytes", idx, metaEnd-8, total)
	}
	meta := cur.bytes(8, int(metaEnd)-8)
	if len(meta) < flatbuffers.SizeUOffsetT {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: metadata is %d bytes", idx, len(meta))
	}
	bodyOff, bodyLen := int(metaEnd), total-int(metaEnd)
	msg := flatbuf.GetRootAsMessage(meta, 0)
	if msg.BodyLength() != int64(bodyLen) {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: body is %d bytes, metadata says %d", idx, bodyLen, msg.BodyLength())
	}
	if msg.HeaderType() != flatbuf.MessageHeaderRecordBatch {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: not a record batch (header type %d)", idx, msg.HeaderType())
	}
	var tab flatbuffers.Table
	if !msg.Header(&tab) {
		return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: record batch has no header", idx)
	}
	var md flatbuf.RecordBatch
	md.Init(tab.Bytes, tab.Pos)
	compressed := false
	if c := md.Compression(nil); c != nil {
		if c.Codec() != flatbuf.CompressionTypeZSTD || c.Method() != flatbuf.BodyCompressionMethodBUFFER {
			return nil, 0, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: unsupported body compression %d/%d", idx, c.Codec(), c.Method())
		}
		compressed = true
	}

	nrows := md.Length()
	nfields := schema.NumFields()
	wantBufs := 0
	for i := 0; i < nfields; i++ {
		kind, _, _ := colLayoutOf(schema.Field(i).Type)
		wantBufs += bodyBuffers(kind)
	}
	if nrows < 0 || nrows > math.MaxInt32 || md.NodesLength() != nfields || md.BuffersLength() != wantBufs {
		return nil, 0, merr.WrapErrServiceInternalMsg(
			"materialize: arrow message %d: %d rows, %d nodes, %d buffers; schema has %d fields needing %d buffers",
			idx, nrows, md.NodesLength(), md.BuffersLength(), nfields, wantBufs)
	}

	varLimit := int64(maxDecompressed)
	if rawSize > 0 && rawSize < varLimit {
		varLimit = rawSize
	}
	r := entryReader{md: &md, p: p, cur: &cur, bodyOff: bodyOff, bodyLen: bodyLen, compressed: compressed, idx: idx, varLimit: varLimit}
	cols = make([]column, nfields)
	for i := 0; i < nfields; i++ {
		if err := r.column(i, schema.Field(i).Type, nrows, &cols[i]); err != nil {
			return nil, 0, err
		}
	}
	return cols, int(nrows), nil
}

type entryReader struct {
	md         *flatbuf.RecordBatch
	p          *pieces
	cur        *cursor
	bodyOff    int
	bodyLen    int
	compressed bool
	idx        int
	nextBuf    int
	// varLimit bounds a decompressed var-len values buffer (bufferLimit).
	varLimit int64
}

func (r *entryReader) fail(field int, format string, args ...any) error {
	return merr.WrapErrServiceInternalMsg("materialize: arrow message %d: field %d: "+format,
		append([]any{r.idx, field}, args...)...)
}

// buffer returns the next IPC buffer, decompressed when the body is. limit
// bounds its uncompressed length (bufferLimit).
func (r *entryReader) buffer(limit int64) (span, error) {
	var b flatbuf.Buffer
	i := r.nextBuf
	r.nextBuf++
	if !r.md.Buffers(&b, i) {
		return span{}, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: buffer %d missing", r.idx, i)
	}
	off, n := b.Offset(), b.Length()
	if off < 0 || n < 0 || off > int64(r.bodyLen) || n > int64(r.bodyLen)-off {
		return span{}, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: buffer %d [%d, +%d) is outside the %d-byte body",
			r.idx, i, off, n, r.bodyLen)
	}
	s := span{p: r.p, off: r.bodyOff + int(off), n: int(n)}
	if !r.compressed || n == 0 {
		return s, nil
	}
	return r.decompress(i, s, limit)
}

// maxDecompressed bounds one decompressed buffer: an Arrow buffer's offsets
// are int32, and nothing BuildBatch writes comes near it. It is also the
// bound of a var-len values buffer when the reply's body_raw_size is unknown.
const maxDecompressed = math.MaxInt32

// bufferLimit is the most uncompressed bytes a buffer of need bytes can
// claim: Arrow's IPC writer pads a bitmap, offsets or fixed-width buffer to a
// 64-byte multiple (ipc.kArrowAlignment), never more. need is
// (rows+7)/8 for a validity bitmap or bool values, (rows+1)*4 for var-len
// offsets, and rows*width for fixed-width values. A var-len values buffer
// has no such bound; it is bounded by the reply's body_raw_size, the raw
// size of every buffer of the reply, or by maxDecompressed when that is
// unknown (entryReader.varLimit).
func bufferLimit(need int64) int64 {
	return min(pad64(need), maxDecompressed)
}

var (
	zstdOnce    sync.Once
	zstdDecoder *zstd.Decoder
	zstdErr     error
)

// decoder returns the shared zstd decoder. DecodeAll output is limited to the
// capacity of dst (WithDecodeAllCapLimit): a frame that inflates past its
// buffer's prefix fails with ErrDecoderSizeExceeded instead of growing dst.
// Every request on the proxy shares it, and the library's default of at most 4
// concurrent DecodeAll calls would queue them, so it gets one per core.
func decoder() (*zstd.Decoder, error) {
	zstdOnce.Do(func() {
		zstdDecoder, zstdErr = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(hardware.GetCPUNum()),
			zstd.WithDecodeAllCapLimit(true),
			zstd.WithDecoderMaxMemory(maxDecompressed))
	})
	return zstdDecoder, zstdErr
}

// decompress reads a compressed IPC buffer: an int64 uncompressed length,
// -1 for a buffer stored raw, then the zstd frame or the raw bytes. The
// length must be within limit, and is all that is allocated: the decoder
// cannot write past it.
func (r *entryReader) decompress(i int, s span, limit int64) (span, error) {
	if s.n < 8 {
		return span{}, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: compressed buffer %d is %d bytes", r.idx, i, s.n)
	}
	var prefix [8]byte
	r.cur.read(prefix[:], s.off)
	size := int64(binary.LittleEndian.Uint64(prefix[:]))
	data := span{p: s.p, off: s.off + 8, n: s.n - 8}
	if size == -1 {
		return data, nil
	}
	if size < 0 || size > limit {
		return span{}, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: buffer %d claims %d uncompressed bytes, the column needs at most %d",
			r.idx, i, size, limit)
	}
	dec, err := decoder()
	if err != nil {
		return span{}, merr.WrapErrServiceInternalErr(err, "materialize: zstd decoder")
	}
	out, err := dec.DecodeAll(r.cur.bytes(data.off, data.n), make([]byte, 0, size))
	if err != nil {
		return span{}, merr.WrapErrServiceInternalErr(err, "materialize: arrow message %d: buffer %d", r.idx, i)
	}
	if int64(len(out)) != size {
		return span{}, merr.WrapErrServiceInternalMsg("materialize: arrow message %d: buffer %d decompressed to %d bytes, its prefix says %d", r.idx, i, len(out), size)
	}
	return span{p: newPieces([][]byte{out}), n: len(out)}, nil
}

// contiguous returns s as one slice: its own bytes when it lies in a single
// part, else a copy.
func contiguous(s span) []byte {
	if b, ok := s.view(); ok {
		return b
	}
	out := make([]byte, s.n)
	(&cursor{p: s.p}).read(out, s.off)
	return out
}

// column mirrors the stock loader (arrayLoaderContext.loadPrimitive,
// loadBinary, loadFixedSizeBinary) buffer for buffer: no validity buffer is
// read when the null count is 0, and an empty primitive column may have no
// values buffer.
func (r *entryReader) column(field int, dt arrow.DataType, rows int64, c *column) error {
	var node flatbuf.FieldNode
	if !r.md.Nodes(&node, field) {
		return r.fail(field, "node missing")
	}
	length, nulls := node.Length(), node.NullCount()
	if length != rows || nulls < 0 || nulls > length {
		return r.fail(field, "node has %d rows and %d nulls, batch has %d rows", length, nulls, rows)
	}
	_, width, _ := colLayoutOf(dt)
	*c = column{typ: dt.ID(), width: width, rows: int(length), nulls: int(nulls)}

	bitmapLimit := bufferLimit(bitutil.BytesForBits(length))
	validity, err := r.buffer(bitmapLimit)
	if err != nil {
		return err
	}
	if nulls > 0 {
		if int64(validity.n) < bitutil.BytesForBits(length) {
			return r.fail(field, "validity bitmap is %d bytes for %d rows", validity.n, length)
		}
		c.valid = contiguous(validity)
	}

	switch dt.ID() {
	case arrow.STRING, arrow.BINARY:
		offsets, err := r.buffer(bufferLimit((length + 1) * 4))
		if err != nil {
			return err
		}
		values, err := r.buffer(r.varLimit)
		if err != nil {
			return err
		}
		c.values = values
		return r.readOffsets(field, offsets, length, c)
	case arrow.BOOL:
		values, err := r.buffer(bitmapLimit)
		if err != nil {
			return err
		}
		if int64(values.n) < bitutil.BytesForBits(length) {
			return r.fail(field, "values are %d bytes, %d rows of %s need %d", values.n, length, dt, bitutil.BytesForBits(length))
		}
		c.bits = contiguous(values)
		return nil
	default:
		width := int64(c.width)
		if width <= 0 || length > math.MaxInt64/width {
			return r.fail(field, "%d rows of %s overflow", length, dt)
		}
		values, err := r.buffer(bufferLimit(length * width))
		if err != nil {
			return err
		}
		if int64(values.n) < length*width {
			return r.fail(field, "values are %d bytes, %d rows of %s need %d", values.n, length, dt, length*width)
		}
		c.values = values
		return nil
	}
}

// readOffsets copies and validates a utf8/binary offsets buffer completely
// -- length, monotonicity and range -- since the gather slices the values
// with it.
func (r *entryReader) readOffsets(field int, offsets span, length int64, c *column) error {
	if length == 0 {
		c.offsets = []int32{0}
		return nil
	}
	if length > int64(offsets.n)/4-1 {
		return r.fail(field, "offsets are %d bytes for %d rows", offsets.n, length)
	}
	c.offsets = make([]int32, length+1)
	(&cursor{p: offsets.p}).read(arrow.Int32Traits.CastToBytes(c.offsets), offsets.off)
	prev := c.offsets[0]
	if prev < 0 {
		return r.fail(field, "first offset %d is negative", prev)
	}
	for i := int64(1); i <= length; i++ {
		cur := c.offsets[i]
		if cur < prev {
			return r.fail(field, "offset %d (%d) is below offset %d (%d)", i, cur, i-1, prev)
		}
		prev = cur
	}
	if int(prev) > c.values.n {
		return r.fail(field, "last offset %d exceeds the %d-byte values buffer", prev, c.values.n)
	}
	return nil
}

// int64s copies an int64 column's values out of the entry.
func int64s(c *column) []int64 {
	out := make([]int64, c.rows)
	if c.rows > 0 {
		(&cursor{p: c.values.p}).read(arrow.Int64Traits.CastToBytes(out), c.values.off)
	}
	return out
}

// strs copies a utf8 column's values out of the entry, into one string that
// the returned values slice.
func strs(c *column) []string {
	out := make([]string, c.rows)
	if c.rows == 0 {
		return out
	}
	first, last := int(c.offsets[0]), int(c.offsets[c.rows])
	var sb strings.Builder
	sb.Grow(last - first)
	(&cursor{p: c.values.p}).writeString(&sb, c.values.off+first, last-first)
	joined := sb.String()
	for i := range out {
		out[i] = joined[int(c.offsets[i])-first : int(c.offsets[i+1])-first]
	}
	return out
}
