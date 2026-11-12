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

package resource

import (
	"encoding/binary"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MaterializeResults field numbers (internal.proto).
const (
	materializeResultsHeaderField protowire.Number = 1
	materializeResultsBodyField   protowire.Number = 2
)

// marshalMaterializeResults encodes msg as standard protobuf without copying
// the body: field framing (tags, lengths, header) goes into one small heap
// buffer, and each body piece follows as a view. The concatenated output is
// byte-identical to proto.Marshal of the logical message, whose body is the
// attached MaterializeBody, or msg.Body when none is attached.
//
// Lifetime: marshal consumes an attached body, also when it fails. A piece
// of a received buffer goes out as buf.Slice, a view holding its own
// reference, and the body's references are then released, so gRPC's Free of
// the returned slice, once written, is what returns the buffers to their
// pool. A Go-owned piece goes
// out as a mem.SliceBuffer, whose Free is a no-op: the caller must not
// mutate msg.Body after handing msg to gRPC, which is already the contract of
// any gRPC Send or handler return. Unknown fields are not sent.
func marshalMaterializeResults(msg *internalpb.MaterializeResults) (mem.BufferSlice, error) {
	if msg == nil {
		return mem.BufferSlice{}, nil
	}
	header := msg.GetHeader()

	body := TakeMaterializeBody(msg)
	defer body.Release()

	// Size the framing buffer exactly so appends never reallocate (not needed
	// for correctness -- earlier sub-slices stay valid either way -- but it
	// keeps the framing in one allocation).
	headerSize := 0
	framing := 0
	if header != nil {
		headerSize = proto.Size(header)
		framing += protowire.SizeTag(materializeResultsHeaderField) + protowire.SizeBytes(headerSize)
	}
	bodyTagSize := protowire.SizeTag(materializeResultsBodyField)
	pieces := 0
	for i := range body.Len() {
		framing += bodyTagSize + protowire.SizeVarint(uint64(body.EntrySize(i)))
		pieces += len(body.Entry(i))
	}

	scratch := make([]byte, 0, framing)
	if header != nil {
		scratch = protowire.AppendTag(scratch, materializeResultsHeaderField, protowire.BytesType)
		scratch = protowire.AppendVarint(scratch, uint64(headerSize))
		var err error
		scratch, err = proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(scratch, header)
		if err != nil {
			return nil, err
		}
	}

	out := make(mem.BufferSlice, 0, body.Len()+pieces+1)
	pending := 0 // start of framing bytes not yet emitted
	for i := range body.Len() {
		e := &body.entries[i]
		scratch = protowire.AppendTag(scratch, materializeResultsBodyField, protowire.BytesType)
		scratch = protowire.AppendVarint(scratch, uint64(e.size()))
		for j, p := range e.pieces {
			if len(p) == 0 {
				continue // coalesce the framing around empty pieces
			}
			if pending < len(scratch) {
				out = append(out, mem.SliceBuffer(scratch[pending:]))
				pending = len(scratch)
			}
			if e.srcs != nil && e.srcs[j].buf != nil {
				src := e.srcs[j]
				out = append(out, src.buf.Slice(src.off, src.off+len(p)))
			} else {
				out = append(out, mem.SliceBuffer(p))
			}
		}
	}
	if pending < len(scratch) {
		out = append(out, mem.SliceBuffer(scratch[pending:]))
	}
	return out, nil
}

// unmarshalMaterializeResults decodes standard protobuf into msg without
// copying the body. The protobuf framing is parsed straight over the received
// buffers: the header field is decoded normally (copied first only when it
// spans buffers), and each body entry is recorded as views of the buffers it
// lies in. Each buffer backing a body byte gets one reference, so gRPC's Free
// after this returns does not recycle it, and the body is attached to msg
// (SetMaterializeBody); whoever takes it releases the references.
//
// Any other field is skipped: no hop forwards unknown fields. Malformed input
// (groups included, which no version sends) fails with ErrServiceInternal
// and holds nothing.
func unmarshalMaterializeResults(data mem.BufferSlice, msg *internalpb.MaterializeResults) error {
	if msg == nil {
		return merr.WrapErrServiceInternalMsg("releaseCodec: nil *MaterializeResults")
	}
	ReleaseMaterializeBody(msg)
	msg.Reset()

	r := newSliceReader(data)
	type pieceRef struct{ buf, off, n int }
	var (
		entries [][]pieceRef // per body entry, its pieces
		used    []bool       // per buffer: backs a body byte
	)
	for r.remaining > 0 {
		tag, err := r.varint()
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "releaseCodec: malformed MaterializeResults tag")
		}
		num, typ := protowire.DecodeTag(tag)
		if num < protowire.MinValidNumber || num > protowire.MaxValidNumber {
			return merr.WrapErrServiceInternalMsg("releaseCodec: malformed MaterializeResults tag: field number %d", num)
		}
		if typ != protowire.BytesType || (num != materializeResultsHeaderField && num != materializeResultsBodyField) {
			// Unknown field, or a known number with an unexpected wire type,
			// which protobuf also treats as unknown.
			if err := r.skip(num, typ); err != nil {
				return err
			}
			continue
		}
		n, err := r.varint()
		if err != nil || n > uint64(r.remaining) {
			if err == nil {
				err = protowire.ParseError(-1) // io.ErrUnexpectedEOF
			}
			return merr.WrapErrServiceInternalErr(err, "releaseCodec: malformed MaterializeResults field %d", num)
		}
		if num == materializeResultsHeaderField {
			v := r.bytes(int(n))
			// Repeated occurrences of a message field merge (protobuf semantics).
			if msg.Header == nil {
				msg.Header = &internalpb.MaterializeHeader{}
			}
			if err := (proto.UnmarshalOptions{Merge: true}).Unmarshal(v, msg.Header); err != nil {
				return merr.WrapErrServiceInternalErr(err, "releaseCodec: malformed MaterializeHeader")
			}
			continue
		}
		var refs []pieceRef
		for left := int(n); left > 0; {
			buf, off, take := r.piece(left)
			refs = append(refs, pieceRef{buf, off, take})
			if used == nil {
				used = make([]bool, len(data))
			}
			used[buf] = true
			left -= take
		}
		entries = append(entries, refs)
	}
	if len(entries) == 0 {
		return nil
	}

	body := &MaterializeBody{entries: make([]bodyEntry, len(entries))}
	for i, u := range used {
		if u {
			data[i].Ref()
			body.held = append(body.held, data[i])
		}
	}
	for i, refs := range entries {
		e := bodyEntry{pieces: make([][]byte, len(refs)), srcs: make([]pieceSrc, len(refs))}
		for j, p := range refs {
			e.pieces[j] = r.datas[p.buf][p.off : p.off+p.n : p.off+p.n]
			e.srcs[j] = pieceSrc{buf: data[p.buf], off: p.off}
		}
		body.entries[i] = e
	}
	SetMaterializeBody(msg, body)
	return nil
}

// sliceReader reads protobuf framing across the buffers of a BufferSlice.
type sliceReader struct {
	datas     [][]byte // each buffer's ReadOnlyData
	cur, off  int      // the next byte is datas[cur][off]
	remaining int
	scratch   [binary.MaxVarintLen64]byte
}

func newSliceReader(data mem.BufferSlice) *sliceReader {
	r := &sliceReader{datas: make([][]byte, len(data))}
	for i, b := range data {
		r.datas[i] = b.ReadOnlyData()
		r.remaining += len(r.datas[i])
	}
	r.skipEmpty()
	return r
}

func (r *sliceReader) skipEmpty() {
	for r.cur < len(r.datas) && r.off == len(r.datas[r.cur]) {
		r.cur++
		r.off = 0
	}
}

func (r *sliceReader) advance(n int) {
	r.remaining -= n
	for n > 0 {
		step := min(n, len(r.datas[r.cur])-r.off)
		r.off += step
		n -= step
		r.skipEmpty()
	}
	r.skipEmpty()
}

// varint reads one varint, which may span buffers.
func (r *sliceReader) varint() (uint64, error) {
	if r.remaining == 0 {
		return 0, protowire.ParseError(-1) // io.ErrUnexpectedEOF
	}
	b := r.datas[r.cur][r.off:]
	if len(b) < binary.MaxVarintLen64 && len(b) < r.remaining {
		// Spans buffers, possibly: gather what a varint can take.
		n := 0
		for i, off := r.cur, r.off; i < len(r.datas) && n < len(r.scratch); i, off = i+1, 0 {
			n += copy(r.scratch[n:], r.datas[i][off:])
		}
		b = r.scratch[:n]
	}
	v, n := protowire.ConsumeVarint(b)
	if n < 0 {
		return 0, protowire.ParseError(n)
	}
	r.advance(n)
	return v, nil
}

// bytes returns the next n bytes, a view when they lie in one buffer and a
// copy otherwise; the caller checked n against remaining.
func (r *sliceReader) bytes(n int) []byte {
	if n == 0 {
		return nil
	}
	if b := r.datas[r.cur][r.off:]; len(b) >= n {
		r.advance(n)
		return b[:n]
	}
	out := make([]byte, 0, n)
	for len(out) < n {
		b := r.datas[r.cur][r.off:]
		take := min(len(b), n-len(out))
		out = append(out, b[:take]...)
		r.advance(take)
	}
	return out
}

// skip consumes the value of field num, of wire type typ. Groups, which no
// version sends, are rejected.
func (r *sliceReader) skip(num protowire.Number, typ protowire.Type) error {
	var n uint64
	switch typ {
	case protowire.VarintType, protowire.BytesType:
		v, err := r.varint()
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "releaseCodec: malformed MaterializeResults field %d", num)
		}
		if typ == protowire.BytesType {
			n = v
		}
	case protowire.Fixed32Type:
		n = 4
	case protowire.Fixed64Type:
		n = 8
	default:
		return merr.WrapErrServiceInternalMsg("releaseCodec: MaterializeResults field %d has wire type %d", num, typ)
	}
	if n > uint64(r.remaining) {
		return merr.WrapErrServiceInternalMsg("releaseCodec: MaterializeResults field %d is truncated", num)
	}
	r.advance(int(n))
	return nil
}

// piece consumes up to n bytes from the current buffer and says where they
// lie; n > 0 and n <= remaining.
func (r *sliceReader) piece(n int) (buf, off, take int) {
	buf, off = r.cur, r.off
	take = min(n, len(r.datas[r.cur])-r.off)
	r.advance(take)
	return buf, off, take
}
