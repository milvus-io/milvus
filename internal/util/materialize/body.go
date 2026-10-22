// Package materialize encodes and decodes the Materialize RPC body: one
// encapsulated Arrow IPC RecordBatch message per entry, with the Schema
// message carried separately in the header (design doc 20261019, §2-§3).
// It uses arrow-go's stock IPC record encoder through its public
// payload-writer hook, so each message is written once into its own buffer,
// and reads entries where the codec received them, in pieces (reader.go),
// gathering the selected rows straight into FieldData (gather.go).
package materialize

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cespare/xxhash/v2"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Codec string

const (
	CodecNone Codec = "none"
	CodecZstd Codec = "zstd"

	minSpaceSavings = 0.1
)

// ResolveCodec turns queryNode.materialize.codec into a concrete codec
// (design doc 20261019 §3): none and zstd apply as given; auto (or any other
// value) follows gRPC compression, zstd when the QueryNode client compresses
// its messages (grpc.client.compressionEnabled) -- the setting a Query reply
// is compressed by, since the server answers with the caller's compressor --
// and none otherwise. So Materialize never sends uncompressed what Query
// would send compressed; ReplyCodec still sends a vector or small reply raw.
func ResolveCodec() Codec {
	pt := paramtable.Get()
	switch Codec(pt.QueryNodeCfg.MaterializeCodec.GetValue()) {
	case CodecNone:
		return CodecNone
	case CodecZstd:
		return CodecZstd
	}
	if pt.QueryNodeGrpcClientCfg.CompressionEnabled.GetAsBool() {
		return CodecZstd
	}
	return CodecNone
}

// contToken is the IPC continuation marker that opens every encapsulated
// message: 0xFFFFFFFF, then the int32 metadata length.
const contToken uint32 = 0xFFFFFFFF

// ipcAlignment is the stock writer's alignment of the metadata and of every
// body buffer (kArrowIPCAlignment in arrow/ipc); bitutil.CeilByte rounds up
// to it.
const ipcAlignment = 8

// emptyBody stands in for the body when only the metadata's BodyLength is
// read.
var emptyBody = memory.NewBufferBytes(nil)

// messageWriter is an ipc.PayloadWriter that frames each payload as one
// encapsulated message in a buffer of its exact size, laid out as the stock
// stream writer lays it out (writeIPCPayload): continuation, metadata length
// padded so that prefix plus metadata is a multiple of 8, metadata, padding,
// then the body. The schema payload comes first, the record batch second.
type messageWriter struct {
	msgs [][]byte
}

func (w *messageWriter) Start() error { return nil }
func (w *messageWriter) Close() error { return nil }

func (w *messageWriter) WritePayload(p ipc.Payload) error {
	meta := p.Meta()
	if meta == nil {
		return merr.WrapErrServiceInternalMsg("materialize: arrow payload has no metadata")
	}
	defer meta.Release()
	m := ipc.NewMessage(meta, emptyBody)
	bodyLen := m.BodyLen()
	m.Release()

	prefix := bitutil.CeilByte(8 + meta.Len())
	buf := make([]byte, prefix+int(bodyLen))
	binary.LittleEndian.PutUint32(buf, contToken)
	binary.LittleEndian.PutUint32(buf[4:], uint32(prefix-8))
	copy(buf[8:], meta.Bytes())
	bw := &sliceWriter{buf: buf[prefix:]}
	if err := p.SerializeBody(bw); err != nil {
		return merr.WrapErrServiceInternalErr(err, "materialize: serialize arrow body")
	}
	if bw.n != len(bw.buf) {
		return merr.WrapErrServiceInternalMsg("materialize: arrow body is %d bytes, metadata says %d", bw.n, bodyLen)
	}
	w.msgs = append(w.msgs, buf)
	return nil
}

// sliceWriter fills a preallocated slice; it never grows it.
type sliceWriter struct {
	buf []byte
	n   int
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	if len(p) > len(w.buf)-w.n {
		return 0, io.ErrShortWrite
	}
	w.n += copy(w.buf[w.n:], p)
	return len(p), nil
}

// EncodeBatch encodes rec with the stock IPC record encoder and returns the
// Schema message and the single RecordBatch message, each a complete
// encapsulated message in its own buffer. For a record whose schema came
// from BuildBatch, the Schema message is shared per layout
// (encode_cache.go): callers must not modify it.
func EncodeBatch(rec arrow.Record, codec Codec) ([]byte, []byte, error) {
	opts := []ipc.Option{ipc.WithSchema(rec.Schema()), ipc.WithAllocator(memory.DefaultAllocator)}
	if codec == CodecZstd {
		opts = append(opts, ipc.WithZstd(), ipc.WithMinSpaceSavings(minSpaceSavings))
	}
	if l := batchLayouts.layout(rec.Schema()); l != nil {
		return l.encodePooled(rec, codec, opts)
	}
	return encodeBatch(rec, opts...)
}

func encodeBatch(rec arrow.Record, opts ...ipc.Option) ([]byte, []byte, error) {
	mw := &messageWriter{}
	w := ipc.NewWriterWithPayloadWriter(mw, opts...)
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, nil, wrapWriteErr(err)
	}
	if err := w.Close(); err != nil {
		return nil, nil, merr.WrapErrServiceInternalErr(err, "materialize: close arrow writer")
	}
	if len(mw.msgs) != 2 {
		return nil, nil, errMessageCount(len(mw.msgs))
	}
	return mw.msgs[0], mw.msgs[1], nil
}

func wrapWriteErr(err error) error {
	return merr.WrapErrServiceInternalErr(err, "materialize: write arrow batch")
}

func errMessageCount(n int) error {
	return merr.WrapErrServiceInternalMsg("materialize: expected schema and one batch, got %d messages", n)
}

// SchemaFingerprint identifies an encapsulated Schema message from
// EncodeBatch. A caller that already holds the schema names it by this
// value, and the worker then omits the bytes (design doc 20261019 §2).
func SchemaFingerprint(schemaMsg []byte) uint64 {
	return xxhash.Sum64(schemaMsg)
}

// readSchema parses an encapsulated Schema message with the stock reader. It
// does not byte-swap, so the caller can see a non-native endian schema.
func readSchema(schemaMsg []byte) (schema *arrow.Schema, err error) {
	// A corrupt metadata flatbuffer can make its accessors index out of
	// range.
	defer func() {
		if p := recover(); p != nil {
			schema, err = nil, merr.WrapErrServiceInternalMsg("materialize: corrupt arrow schema: %v", p)
		}
	}()
	rd, err := ipc.NewReader(bytes.NewReader(schemaMsg), ipc.WithEnsureNativeEndian(false))
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "materialize: read arrow schema")
	}
	defer rd.Release()
	return rd.Schema(), nil
}

// RawSize is the record's field data size after decompression: the sum of
// all its buffers, children included.
func RawSize(rec arrow.Record) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += dataSize(col.Data())
	}
	return n
}

func dataSize(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += dataSize(c)
	}
	return n
}
