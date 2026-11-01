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
	"bytes"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testMaterializeHeader() *internalpb.MaterializeHeader {
	return &internalpb.MaterializeHeader{
		Status:             &commonpb.Status{Code: 0, Reason: "ok"},
		BodySchema:         []byte("arrow-schema"),
		SchemaFingerprint:  0xdeadbeefcafe,
		BodyRawSize:        123456,
		SourceId:           7,
		AllRetrieveCount:   42,
		ScannedRemoteBytes: 1 << 20,
		ScannedTotalBytes:  1 << 21,
	}
}

func patternBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

func materializeCodecCases() []struct {
	name string
	msg  *internalpb.MaterializeResults
} {
	return []struct {
		name string
		msg  *internalpb.MaterializeResults
	}{
		{"empty", &internalpb.MaterializeResults{}},
		{"header_only", &internalpb.MaterializeResults{Header: testMaterializeHeader()}},
		{"empty_header", &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{}}},
		{"one_entry", &internalpb.MaterializeResults{Header: testMaterializeHeader(), Body: [][]byte{patternBytes(100, 1)}}},
		{"three_entries_with_empty", &internalpb.MaterializeResults{
			Header: testMaterializeHeader(),
			Body:   [][]byte{{}, patternBytes(300, 2), {}},
		}},
		{"three_entries_1mb", &internalpb.MaterializeResults{
			Header: testMaterializeHeader(),
			Body:   [][]byte{patternBytes(1<<20, 3), {}, patternBytes(200, 4)},
		}},
		{"body_no_header", &internalpb.MaterializeResults{Body: [][]byte{patternBytes(10, 5), patternBytes(70000, 6), patternBytes(1, 7)}}},
	}
}

// splitBufferSlice cuts b into chunk-sized buffers, like gRPC's receive path.
func splitBufferSlice(b []byte, chunk int) mem.BufferSlice {
	var out mem.BufferSlice
	for len(b) > 0 {
		n := min(chunk, len(b))
		c := make([]byte, n)
		copy(c, b[:n])
		out = append(out, mem.SliceBuffer(c))
		b = b[n:]
	}
	return out
}

// logical returns msg with its body in Body, as proto.Marshal sees the
// message the codec encodes; it takes and releases msg's attached body.
func logical(msg *internalpb.MaterializeResults) *internalpb.MaterializeResults {
	out := proto.Clone(msg).(*internalpb.MaterializeResults)
	b := TakeMaterializeBody(msg)
	defer b.Release()
	out.Body = nil
	for i := range b.Len() {
		out.Body = append(out.Body, bytes.Join(b.Entry(i), nil))
	}
	return out
}

// poisonPool is a mem.BufferPool for receive buffers that counts Get and
// Put and poisons every buffer it gets back (0xAA), so a read after Free
// shows up as corrupted bytes. Its buffers have a capacity above gRPC's
// pooling threshold even when short, so mem.NewBuffer pools them.
type poisonPool struct {
	gets, puts atomic.Int64
}

func (p *poisonPool) Get(n int) *[]byte {
	p.gets.Add(1)
	b := make([]byte, n, max(n, 4096))
	return &b
}

func (p *poisonPool) Put(b *[]byte) {
	p.puts.Add(1)
	full := (*b)[:cap(*b)]
	for i := range full {
		full[i] = 0xAA
	}
}

func (p *poisonPool) outstanding() int64 { return p.gets.Load() - p.puts.Load() }

// frames copies wire into pooled buffers cut at cuts (strictly increasing
// offsets inside wire), as gRPC's transport hands a message to the codec.
func (p *poisonPool) frames(wire []byte, cuts ...int) mem.BufferSlice {
	var out mem.BufferSlice
	prev := 0
	for _, c := range append(cuts, len(wire)) {
		if c <= prev {
			continue
		}
		pb := p.Get(c - prev)
		copy(*pb, wire[prev:c])
		out = append(out, mem.NewBuffer(pb, p))
		prev = c
	}
	return out
}

// everyN cuts wire into n-byte frames.
func everyN(wire []byte, n int) []int {
	var cuts []int
	for c := n; c < len(wire); c += n {
		cuts = append(cuts, c)
	}
	return cuts
}

func TestMaterializeCodecRoundTrip(t *testing.T) {
	codec := releaseCodec{}
	for _, tc := range materializeCodecCases() {
		t.Run(tc.name, func(t *testing.T) {
			want, err := proto.Marshal(tc.msg)
			require.NoError(t, err)

			out, err := codec.Marshal(tc.msg)
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, out.Materialize()), "marshal output differs from proto.Marshal")
			out.Free()

			for _, chunk := range []int{0, 16 << 10, 7, 1} {
				if chunk > 0 && len(want)/chunk > 1<<14 {
					continue // too many frames; the boundary test covers small ones
				}
				t.Run(fmt.Sprintf("chunk_%d", chunk), func(t *testing.T) {
					pool := &poisonPool{}
					var cuts []int
					if chunk > 0 {
						cuts = everyN(want, chunk)
					}
					data := pool.frames(want, cuts...)
					got := &internalpb.MaterializeResults{}
					require.NoError(t, codec.Unmarshal(data, got))
					data.Free() // gRPC frees the receive buffers after Unmarshal
					require.Empty(t, got.Body, "the body is attached, not in Body")

					// Relayed: the delegator's send is byte-identical.
					relayed, err := codec.Marshal(got)
					require.NoError(t, err)
					require.True(t, bytes.Equal(want, relayed.Materialize()), "relayed marshal differs from proto.Marshal")
					relayed.Free() // gRPC frees the sent buffers once written
					require.Zero(t, pool.outstanding(), "every receive buffer is back in the pool")

					// Read instead of relayed (the proxy).
					data = pool.frames(want, cuts...)
					got = &internalpb.MaterializeResults{}
					require.NoError(t, codec.Unmarshal(data, got))
					data.Free()
					require.True(t, proto.Equal(tc.msg, logical(got)), "decoded message differs")
					require.Zero(t, pool.outstanding())
				})
			}
		})
	}
}

// TestMaterializeCodecEveryBoundary cuts a message into two and three frames
// at every offset -- inside a tag or a length prefix, inside the header, on
// and inside each entry -- and into 1-byte frames, receives it from poisoned
// pooled buffers, then relays or reads it after gRPC's Free. A body byte read
// after its buffer was freed would read 0xAA; a double Free panics.
func TestMaterializeCodecEveryBoundary(t *testing.T) {
	msg := &internalpb.MaterializeResults{
		Header: testMaterializeHeader(),
		Body:   [][]byte{patternBytes(150, 1), {}, patternBytes(3, 2), patternBytes(200, 3)},
	}
	want, err := proto.Marshal(msg)
	require.NoError(t, err)
	codec := releaseCodec{}
	check := func(t *testing.T, cuts []int) {
		pool := &poisonPool{}
		// Relay.
		data := pool.frames(want, cuts...)
		got := &internalpb.MaterializeResults{}
		require.NoError(t, codec.Unmarshal(data, got))
		data.Free()
		out, err := codec.Marshal(got)
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, out.Materialize()), "cuts %v", cuts)
		out.Free()
		require.Zero(t, pool.outstanding(), "cuts %v", cuts)
		// Read.
		data = pool.frames(want, cuts...)
		got = &internalpb.MaterializeResults{}
		require.NoError(t, codec.Unmarshal(data, got))
		data.Free()
		body := TakeMaterializeBody(got)
		require.Equal(t, len(msg.Body), body.Len())
		for i, e := range msg.Body {
			require.Equal(t, len(e), body.EntrySize(i))
			require.True(t, bytes.Equal(e, bytes.Join(body.Entry(i), nil)), "cuts %v entry %d", cuts, i)
		}
		require.True(t, proto.Equal(msg.GetHeader(), got.GetHeader()))
		body.Release()
		body.Release() // idempotent
		require.Zero(t, pool.outstanding(), "cuts %v", cuts)
	}
	for c := 1; c < len(want); c++ {
		check(t, []int{c})
	}
	for c := 1; c < len(want); c += 7 {
		for d := c + 1; d < len(want); d += 11 {
			check(t, []int{c, d})
		}
	}
	check(t, everyN(want, 1))
}

func TestMaterializeCodecRegistered(t *testing.T) {
	// The pipeline and gRPC look the codec up by name; make sure it routes here.
	msg := &internalpb.MaterializeResults{Body: [][]byte{patternBytes(1<<16, 9)}}
	out, err := encoding.GetCodecV2("proto").Marshal(msg)
	require.NoError(t, err)
	require.Greater(t, len(out), 1)
	require.Same(t, unsafe.SliceData(msg.Body[0]), unsafe.SliceData(out[1].ReadOnlyData()))
}

func TestMaterializeCodecMarshalNoCopy(t *testing.T) {
	msg := &internalpb.MaterializeResults{
		Header: testMaterializeHeader(),
		Body:   [][]byte{patternBytes(1<<20, 1), {}, patternBytes(4096, 2), patternBytes(1<<20, 3)},
	}
	out, err := releaseCodec{}.Marshal(msg)
	require.NoError(t, err)
	for i, entry := range msg.Body {
		if len(entry) == 0 {
			continue
		}
		found := false
		for _, buf := range out {
			d := buf.ReadOnlyData()
			if len(d) == len(entry) && unsafe.SliceData(d) == unsafe.SliceData(entry) {
				found = true
				break
			}
		}
		require.True(t, found, "body entry %d was copied", i)
	}
}

// The received body stays in the receive buffers: every piece is a view of
// one of them, and the relay sends views of the same bytes.
func TestMaterializeCodecUnmarshalViews(t *testing.T) {
	msg := &internalpb.MaterializeResults{
		Header: testMaterializeHeader(),
		Body:   [][]byte{patternBytes(1<<20, 1), {}, patternBytes(5000, 2), patternBytes(70000, 3)},
	}
	wire, err := proto.Marshal(msg)
	require.NoError(t, err)
	pool := &poisonPool{}
	data := pool.frames(wire, everyN(wire, 16<<10)...)
	inData := func(p []byte) bool {
		for _, b := range data {
			d := b.ReadOnlyData()
			start := uintptr(unsafe.Pointer(unsafe.SliceData(d)))
			ps := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
			if ps >= start && ps+uintptr(len(p)) <= start+uintptr(len(d)) {
				return true
			}
		}
		return false
	}
	got := &internalpb.MaterializeResults{}
	require.NoError(t, releaseCodec{}.Unmarshal(data, got))
	body := TakeMaterializeBody(got)
	for i := range body.Len() {
		for _, p := range body.Entry(i) {
			require.True(t, inData(p), "entry %d piece is not a view of the receive buffers", i)
			require.Equal(t, len(p), cap(p), "piece capacity must be capped")
		}
	}
	SetMaterializeBody(got, body)
	out, err := releaseCodec{}.Marshal(got)
	require.NoError(t, err)
	views := 0
	for _, b := range out {
		if inData(b.ReadOnlyData()) {
			views += b.Len()
		}
	}
	require.Equal(t, 1<<20+5000+70000, views, "the relay sends the body as views")
	data.Free()
	require.NotZero(t, pool.outstanding(), "the views hold the buffers")
	out.Free()
	require.Zero(t, pool.outstanding())
}

// A message dropped with its body attached loses the side-table entry once
// collected; its buffers are never freed (garbage collected instead).
func TestMaterializeBodyDroppedMessage(t *testing.T) {
	msg := &internalpb.MaterializeResults{Body: [][]byte{patternBytes(5000, 1)}}
	wire, err := proto.Marshal(msg)
	require.NoError(t, err)
	before := attachedBodies.n.Load()
	pool := &poisonPool{}
	func() {
		data := pool.frames(wire, 100)
		got := &internalpb.MaterializeResults{}
		require.NoError(t, releaseCodec{}.Unmarshal(data, got))
		data.Free()
		require.Equal(t, before+1, attachedBodies.n.Load())
	}()
	require.Eventually(t, func() bool {
		runtime.GC()
		return attachedBodies.n.Load() == before
	}, 5*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 2, pool.outstanding(), "dropped, not freed")
}

func TestMaterializeBodySetTake(t *testing.T) {
	msg := &internalpb.MaterializeResults{Body: [][]byte{[]byte("old")}}
	b := TakeMaterializeBody(msg)
	require.Equal(t, 1, b.Len())
	require.Equal(t, [][]byte{[]byte("old")}, msg.Body, "a plain body is left in place")

	pool := &poisonPool{}
	data := pool.frames([]byte("0123456789"), 4)
	held := &MaterializeBody{held: []mem.Buffer{data[0], data[1]}, entries: []bodyEntry{{pieces: [][]byte{[]byte("x")}}}}
	data[0].Ref()
	data[1].Ref()
	data.Free()
	SetMaterializeBody(msg, held)
	require.Nil(t, msg.Body)
	// Replacing the attached body releases it.
	SetMaterializeBody(msg, newMaterializeBody([][]byte{[]byte("new")}))
	require.Zero(t, pool.outstanding(), "the replaced body was released")
	got := TakeMaterializeBody(msg)
	require.Equal(t, []byte("new"), got.Entry(0)[0])
	require.Zero(t, TakeMaterializeBody(msg).Len(), "taken once")
	require.Zero(t, TakeMaterializeBody(nil).Len())

	// An empty body is not attached.
	SetMaterializeBody(msg, &MaterializeBody{})
	require.Zero(t, TakeMaterializeBody(msg).Len())

	// Append moves entries and references.
	a := newMaterializeBody([][]byte{[]byte("a")})
	a.Append(newMaterializeBody([][]byte{[]byte("b"), []byte("cc")}))
	require.Equal(t, 3, a.Len())
	a.Release()
	require.Zero(t, a.Len())
}

func TestMaterializeCodecUnknownFields(t *testing.T) {
	msg := &internalpb.MaterializeResults{Header: testMaterializeHeader(), Body: [][]byte{patternBytes(64, 1)}}
	wire, err := proto.Marshal(msg)
	require.NoError(t, err)

	// Append unknown fields of every wire type, plus a body-numbered field
	// with the wrong wire type (protobuf treats it as unknown).
	var extra []byte
	extra = protowire.AppendTag(extra, 99, protowire.VarintType)
	extra = protowire.AppendVarint(extra, 12345)
	extra = protowire.AppendTag(extra, 100, protowire.BytesType)
	extra = protowire.AppendBytes(extra, []byte("future"))
	extra = protowire.AppendTag(extra, 101, protowire.Fixed64Type)
	extra = protowire.AppendFixed64(extra, 1)
	extra = protowire.AppendTag(extra, 102, protowire.Fixed32Type)
	extra = protowire.AppendFixed32(extra, 1)
	extra = protowire.AppendTag(extra, 2, protowire.VarintType)
	extra = protowire.AppendVarint(extra, 1)
	wire = append(wire, extra...)

	want := &internalpb.MaterializeResults{}
	require.NoError(t, proto.Unmarshal(wire, want))

	// The codec skips them: no hop forwards unknown fields.
	want.ProtoReflect().SetUnknown(nil)

	for _, chunk := range []int{len(wire), 5, 1} {
		got := &internalpb.MaterializeResults{}
		require.NoError(t, releaseCodec{}.Unmarshal(splitBufferSlice(wire, chunk), got), "chunk %d", chunk)
		require.Empty(t, got.ProtoReflect().GetUnknown())
		require.True(t, proto.Equal(want, logical(got)), "chunk %d", chunk)
	}

	// Marshal does not send unknown fields either.
	withUnknown := &internalpb.MaterializeResults{Header: testMaterializeHeader()}
	withUnknown.ProtoReflect().SetUnknown(extra)
	out, err := releaseCodec{}.Marshal(withUnknown)
	require.NoError(t, err)
	stdOut, err := proto.Marshal(&internalpb.MaterializeResults{Header: testMaterializeHeader()})
	require.NoError(t, err)
	require.True(t, bytes.Equal(stdOut, out.Materialize()))
}

func TestMaterializeCodecRepeatedHeaderMerges(t *testing.T) {
	h1 := &internalpb.MaterializeHeader{SourceId: 1, BodyRawSize: 10}
	h2 := &internalpb.MaterializeHeader{SourceId: 2, AllRetrieveCount: 5}
	var wire []byte
	for _, h := range []*internalpb.MaterializeHeader{h1, h2} {
		b, err := proto.Marshal(h)
		require.NoError(t, err)
		wire = protowire.AppendTag(wire, 1, protowire.BytesType)
		wire = protowire.AppendBytes(wire, b)
	}
	want := &internalpb.MaterializeResults{}
	require.NoError(t, proto.Unmarshal(wire, want))
	got := &internalpb.MaterializeResults{}
	require.NoError(t, releaseCodec{}.Unmarshal(mem.BufferSlice{mem.SliceBuffer(wire)}, got))
	require.True(t, proto.Equal(want, got))
	require.EqualValues(t, 2, got.GetHeader().GetSourceId())
	require.EqualValues(t, 10, got.GetHeader().GetBodyRawSize())
	require.EqualValues(t, 5, got.GetHeader().GetAllRetrieveCount())
}

func TestMaterializeCodecMalformed(t *testing.T) {
	valid, err := proto.Marshal(&internalpb.MaterializeResults{Header: testMaterializeHeader(), Body: [][]byte{patternBytes(100, 1)}})
	require.NoError(t, err)

	badHeader := protowire.AppendTag(nil, 1, protowire.BytesType)
	badHeader = protowire.AppendBytes(badHeader, []byte{0x0a, 0x05}) // status claims 5 bytes, has 0

	cases := []struct {
		name string
		wire []byte
	}{
		{"truncated_body_length", valid[:len(valid)-1]},
		{"truncated_tag", []byte{0x80}},
		{"length_overflows", append(protowire.AppendTag(nil, 2, protowire.BytesType), 0xff, 0xff, 0x03)},
		{"truncated_unknown_field", append(protowire.AppendTag(nil, 50, protowire.Fixed64Type), 1, 2)},
		{"invalid_wire_type", []byte{0x0f}},
		{"truncated_unknown_bytes", append(protowire.AppendTag(nil, 50, protowire.BytesType), 5, 1)},
		{"truncated_unknown_varint", append(protowire.AppendTag(nil, 50, protowire.VarintType), 0x80)},
		{"bad_header", badHeader},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, proto.Unmarshal(tc.wire, &internalpb.MaterializeResults{}), "standard proto must reject too")
			for _, chunk := range []int{len(tc.wire), 1} {
				pool := &poisonPool{}
				data := pool.frames(tc.wire, everyN(tc.wire, chunk)...)
				err := releaseCodec{}.Unmarshal(data, &internalpb.MaterializeResults{})
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				data.Free()
				require.Zero(t, pool.outstanding(), "a failed unmarshal holds nothing")
			}
		})
	}
}

func TestMaterializeCodecNil(t *testing.T) {
	out, err := releaseCodec{}.Marshal((*internalpb.MaterializeResults)(nil))
	require.NoError(t, err)
	require.Zero(t, out.Len())
	require.Error(t, releaseCodec{}.Unmarshal(mem.BufferSlice{}, (*internalpb.MaterializeResults)(nil)))
}

// The codec in materialize_codec.go encodes MaterializeResults by hand: it
// writes only the header (field 1) and the body (field 2), and parses only
// those two. A field added to the message would be silently dropped on send
// and skipped on receive. This test pins the message's
// fields, so a proto change fails here until the codec is updated.
func TestMaterializeCodecPinsDescriptor(t *testing.T) {
	type field struct {
		name     string
		number   int32
		kind     string
		repeated bool
		message  string
	}
	want := []field{
		{"header", int32(materializeResultsHeaderField), "message", false, "milvus.proto.internal.MaterializeHeader"},
		{"body", int32(materializeResultsBodyField), "bytes", true, ""},
	}
	md := (&internalpb.MaterializeResults{}).ProtoReflect().Descriptor()
	fields := md.Fields()
	var got []field
	for i := range fields.Len() {
		fd := fields.Get(i)
		f := field{string(fd.Name()), int32(fd.Number()), fd.Kind().String(), fd.IsList(), ""}
		if fd.Message() != nil {
			f.message = string(fd.Message().FullName())
		}
		got = append(got, f)
	}
	require.Equal(t, want, got,
		"internalpb.MaterializeResults changed: update the hand-written codec in "+
			"pkg/util/resource (materialize_codec.go: marshalMaterializeResults, "+
			"unmarshalMaterializeResults) to encode "+
			"and decode the new fields, then update this test")
	require.Zero(t, md.Oneofs().Len(), "MaterializeResults gained a oneof: update the codec in pkg/util/resource")
	require.Zero(t, md.ExtensionRanges().Len(), "MaterializeResults gained extensions: update the codec in pkg/util/resource")
}

// Taking or releasing a body removes it from the side table; a message
// dropped with its body attached leaves its entry until the message is
// collected, when the cleanup registered at attach drops it.
func TestMaterializeBodyTakeStopsCleanup(t *testing.T) {
	attach := func() *internalpb.MaterializeResults {
		msg := &internalpb.MaterializeResults{}
		SetMaterializeBody(msg, newMaterializeBody([][]byte{[]byte("x")}))
		return msg
	}
	// Flush cleanups of earlier tests' garbage.
	for range 3 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	before := attachedBodies.n.Load()
	func() {
		TakeMaterializeBody(attach()).Release()
		ReleaseMaterializeBody(attach())
		attach() // dropped with its body attached
	}()
	require.Equal(t, before+1, attachedBodies.n.Load(), "only the body still attached is in the side table")
	require.Eventually(t, func() bool {
		runtime.GC()
		return attachedBodies.n.Load() == before
	}, 5*time.Second, 10*time.Millisecond, "the cleanup drops the collected message's entry")
}
