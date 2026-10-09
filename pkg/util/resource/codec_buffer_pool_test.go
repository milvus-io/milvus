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
	"sync"
	"testing"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func fragments(b []byte, fragmentSize int) mem.BufferSlice {
	var out mem.BufferSlice
	for len(b) > 0 {
		n := min(len(b), fragmentSize)
		out = append(out, mem.SliceBuffer(b[:n:n]))
		b = b[n:]
	}
	return out
}

func TestCoalesceDirtyBuffersFullyOverwritten(t *testing.T) {
	p := new(codecBufferPool)
	for _, size := range []int{40000, 33000, 65536, 4100, 16384, 32769, 0, 1, 40000} {
		dirty := p.Get(max(size, 1))
		for i := range *dirty {
			(*dirty)[i] = 0xff
		}
		p.Put(dirty)
		want := bytes.Repeat([]byte{0x37}, size)
		input := fragments(want, max(1, size/3))
		got := input.MaterializeToBuffer(p)
		if !bytes.Equal(got.ReadOnlyData(), want) {
			t.Fatalf("stale bytes at size %d", size)
		}
		got.Free()
	}
}

type countingPool struct {
	gets, puts, requested int
	last                  *[]byte
}

func (p *countingPool) Get(n int) *[]byte {
	p.gets++
	p.requested = n
	b := bytes.Repeat([]byte{0xdd}, n+64)
	b = b[:n]
	p.last = &b
	return &b
}
func (p *countingPool) Put(*[]byte) { p.puts++ }

func TestCoalesceSingleBufferKeepsReferences(t *testing.T) {
	origin := &countingPool{}
	b := origin.Get(40000)
	input := mem.BufferSlice{mem.NewBuffer(b, origin)}
	p := &countingPool{}
	got := input.MaterializeToBuffer(p)
	if got != input[0] || p.gets != 0 {
		t.Fatal("single buffer was copied")
	}
	got.Free()
	if origin.puts != 0 {
		t.Fatal("input reference released too early")
	}
	if input[0].Len() != 40000 {
		t.Fatal("input no longer valid")
	}
	input.Free()
	if origin.puts != 1 {
		t.Fatalf("release count = %d", origin.puts)
	}
}

func TestCodecBufferPoolMatchesDefaultClasses(t *testing.T) {
	for _, n := range []int{0, 1, 256, 257, 4096, 4097, 16384, 16385, 32768, 32769, 1 << 20, (1 << 20) + 1, (4 << 20) + 7} {
		defaultPool, _ := mem.NewBinaryTieredBufferPool(8, 12, 14, 15, 20)
		dirtyPool := new(codecBufferPool)
		clean, dirty := defaultPool.Get(n), dirtyPool.Get(n)
		if len(*clean) != len(*dirty) || cap(*clean) != cap(*dirty) {
			t.Fatalf("class differs at %d: default cap%d dirty cap%d", n, cap(*clean), cap(*dirty))
		}
		defaultPool.Put(clean)
		dirtyPool.Put(dirty)
	}
}

func TestCoalesceConcurrentVaryingLengths(t *testing.T) {
	p := new(codecBufferPool)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				want := bytes.Repeat([]byte{byte(w + i)}, 33000+(i*997)%50000)
				got := fragments(want, 16384).MaterializeToBuffer(p)
				if !bytes.Equal(got.ReadOnlyData(), want) {
					t.Error("concurrent stale data")
				}
				got.Free()
			}
		}(worker)
	}
	wg.Wait()
}

func TestReleaseCodecCoalescingErrorsAndIndependentOutputs(t *testing.T) {
	codec := releaseCodec{}
	var previous, snapshots []*internalpb.RetrieveResults
	for iteration, size := range []int{40000, 33000, 100000, 1, 70000} {
		want := &internalpb.RetrieveResults{ChannelIDsRetrieved: []string{string(bytes.Repeat([]byte{byte('a' + iteration)}, size))}}
		wire, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		got := &internalpb.RetrieveResults{}
		if err := codec.Unmarshal(fragments(wire, 16384), got); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(want, got) {
			t.Fatal("codec output mismatch")
		}
		previous = append(previous, got)
		snapshots = append(snapshots, proto.Clone(want).(*internalpb.RetrieveResults))
		// A multi-fragment message with a truncated length-delimited field must
		// release its coalesced buffer even when the decoder fails.
		bad := append(append([]byte(nil), wire...), 0x2a, 0x80)
		if err := codec.Unmarshal(fragments(bad, 16384), &internalpb.RetrieveResults{}); err == nil {
			t.Fatal("malformed message accepted")
		}
	}
	// Explicitly poison a recycled buffer from every exercised tier, then check
	// complete snapshots (distinct contents), not just valid repeated strings.
	for _, size := range []int{40000, 33000, 100000, 1, 70000} {
		buffer := releaseCodecBufferPool.Get(size)
		for i := range *buffer {
			(*buffer)[i] = 0xdd
		}
		releaseCodecBufferPool.Put(buffer)
	}
	for i, got := range previous {
		if !proto.Equal(got, snapshots[i]) {
			t.Fatalf("decoded result %d aliases recycled buffer", i)
		}
	}
}

func TestReleaseCodecErrorReturnsCoalescedBuffer(t *testing.T) {
	pool := &countingPool{}
	codec := releaseCodec{bufferPool: pool}
	// Long unknown field with a missing terminating byte exercises both the
	// official RetrieveResults and SearchResults decoding paths.
	bad := append([]byte{0xaa, 0x06, 0x80, 0x80, 0x80}, bytes.Repeat([]byte{0x80}, 40000)...)
	for _, message := range []proto.Message{&internalpb.RetrieveResults{}, &internalpb.SearchResults{}} {
		oldGets, oldPuts := pool.gets, pool.puts
		if err := codec.Unmarshal(fragments(bad, 16384), message); err == nil {
			t.Fatal("malformed input accepted")
		}
		if pool.gets != oldGets+1 || pool.puts != oldPuts+1 {
			t.Fatalf("error cleanup Get=%d Put=%d", pool.gets-oldGets, pool.puts-oldPuts)
		}
	}
}

func TestReleaseCodecMarshalDirtySuccessAndError(t *testing.T) {
	pool := &countingPool{}
	codec := releaseCodec{bufferPool: pool}
	for _, metric := range []string{"valid", "\xff"} {
		message := &internalpb.SearchResults{MetricType: metric, SlicedBlob: bytes.Repeat([]byte{0x42}, 40000)}
		oldGets, oldPuts := pool.gets, pool.puts
		cleanups := 0
		MsgPins.Pin(message, func() { cleanups++ })
		out, err := codec.Marshal(message)
		if cleanups != 1 {
			t.Fatal("pinned memory not released on marshal success/error")
		}
		if metric == "\xff" {
			if err == nil || out != nil {
				t.Fatal("invalid UTF8 marshal accepted")
			}
			if pool.puts != oldPuts+1 {
				t.Fatal("marshal error did not return buffer")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			want, e := proto.Marshal(message)
			if e != nil {
				t.Fatal(e)
			}
			if out.Len() != len(want) || pool.requested != len(want) {
				out.Free()
				t.Fatal("wrong published/requested length")
			}
			if !bytes.Equal((*pool.last)[len(want):cap(*pool.last)], bytes.Repeat([]byte{0xdd}, cap(*pool.last)-len(want))) {
				out.Free()
				t.Fatal("wrote beyond output length")
			}
			if !bytes.Equal(out.Materialize(), want) {
				out.Free()
				t.Fatal("marshal includes stale tail")
			}
			if pool.puts != oldPuts {
				t.Fatal("published output freed early")
			}
			out.Free()
			if pool.puts != oldPuts+1 {
				t.Fatal("success output did not return buffer")
			}
		}
		if pool.gets != oldGets+1 {
			t.Fatal("did not use pooled marshal")
		}
	}
}

func TestCodecBufferPoolFallbackVaryingSizes(t *testing.T) {
	pool := new(codecBufferPool)
	for _, n := range []int{(1 << 20) + 1, (4 << 20) + 7, (2 << 20) + 3, (8 << 20) + 5, (1 << 20) + 13} {
		buffer := pool.Get(n)
		if len(*buffer) != n || cap(*buffer) < n {
			t.Fatal("invalid fallback length/capacity")
		}
		for i := range *buffer {
			(*buffer)[i] = 0xdd
		}
		pool.Put(buffer)
		want := bytes.Repeat([]byte{0x33}, n)
		out := fragments(want, 16384).MaterializeToBuffer(pool)
		if out.Len() != n || !bytes.Equal(out.ReadOnlyData(), want) {
			out.Free()
			t.Fatal("fallback exposes stale bytes")
		}
		out.Free()
	}
}

func TestReleaseCodecHeldMarshalOutputNotRecycled(t *testing.T) {
	codec := releaseCodec{bufferPool: new(codecBufferPool)}
	first := &internalpb.SearchResults{SlicedBlob: bytes.Repeat([]byte{0x11}, 40000)}
	want, err := proto.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	held, err := codec.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Free()
	for _, n := range []int{40000, 33000, 100000, 40000} {
		out, err := codec.Marshal(&internalpb.SearchResults{SlicedBlob: bytes.Repeat([]byte{0x22}, n)})
		if err != nil {
			t.Fatal(err)
		}
		out.Free()
		if !bytes.Equal(held.Materialize(), want) {
			t.Fatal("live published buffer recycled")
		}
	}
}

func TestReleaseCodecConcurrentMessages(t *testing.T) {
	codec := releaseCodec{bufferPool: new(codecBufferPool)}
	messages := []proto.Message{
		&internalpb.RetrieveResults{ChannelIDsRetrieved: []string{string(bytes.Repeat([]byte{'v'}, 70000))}},
		&internalpb.SearchResults{SlicedBlob: bytes.Repeat([]byte{0x21}, 40000)},
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				message := messages[(i+index)%len(messages)]
				expected, err := proto.Marshal(message)
				if err != nil {
					t.Error(err)
					return
				}
				out, err := codec.Marshal(message)
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(out.Materialize(), expected) {
					out.Free()
					t.Error("concurrent marshal stale data")
					return
				}
				out.Free()
				target := message.ProtoReflect().New().Interface()
				if err := codec.Unmarshal(fragments(expected, 16384), target); err != nil {
					t.Error(err)
					return
				}
				if !proto.Equal(message, target) {
					t.Error("concurrent decode mismatch")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
