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

package common

import (
	"encoding/binary"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func canonTestPairs(pairs ...string) []*commonpb.KeyValuePair {
	out := make([]*commonpb.KeyValuePair, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonpb.KeyValuePair{Key: pairs[i], Value: pairs[i+1]})
	}
	return out
}

// uniqueCanonValue keeps test values new under -count=N.
func uniqueCanonValue(t *testing.T) string {
	return t.Name() + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func TestCanonicalKeyValuePairs(t *testing.T) {
	v := uniqueCanonValue(t)
	tests := []struct {
		name string
		a, b []*commonpb.KeyValuePair
		same bool
	}{
		{"same content", canonTestPairs("a", v, "b", "2"), canonTestPairs("a", v, "b", "2"), true},
		{"different order", canonTestPairs("a", v, "b", "2"), canonTestPairs("b", "2", "a", v), false},
		{"different value", canonTestPairs("a", v), canonTestPairs("a", v+"x"), false},
		{"separator ambiguity", canonTestPairs("a\x00b", v), canonTestPairs("a", "\x00b"+v), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ra := CanonicalKeyValuePairs(tt.a)
			rb := CanonicalKeyValuePairs(tt.b)
			assert.True(t, slices.EqualFunc(tt.a, ra, kvProtoEqual))
			assert.True(t, slices.EqualFunc(tt.b, rb, kvProtoEqual))
			assert.Equal(t, tt.same, unsafe.SliceData(ra) == unsafe.SliceData(rb))
			assert.Equal(t, len(ra), cap(ra))
			assert.Equal(t, len(rb), cap(rb))
			runtime.KeepAlive(ra)
			runtime.KeepAlive(rb)
		})
	}
}

func TestCanonicalKeyValuePairs_NilAndEmpty(t *testing.T) {
	assert.Nil(t, CanonicalKeyValuePairs(nil))
	tests := []struct {
		name string
		in   []*commonpb.KeyValuePair
	}{
		{name: "empty", in: []*commonpb.KeyValuePair{}},
		{name: "empty with spare capacity", in: make([]*commonpb.KeyValuePair, 0, 4)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := CanonicalKeyValuePairs(test.in)
			assert.NotNil(t, got)
			assert.Empty(t, got)
			assert.Zero(t, cap(got))
			// Two holders appending never write the same backing array.
			a := append(got, &commonpb.KeyValuePair{Key: "a"})
			b := append(got, &commonpb.KeyValuePair{Key: "b"})
			assert.Equal(t, "a", a[0].GetKey())
			assert.Equal(t, "b", b[0].GetKey())
		})
	}
}

func TestCanonicalKeyValuePairs_UnknownFields(t *testing.T) {
	v := uniqueCanonValue(t)
	withUnknown := func() []*commonpb.KeyValuePair {
		kv := &commonpb.KeyValuePair{Key: "k", Value: v}
		// Field 15, varint 1: a field a newer writer could add.
		kv.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 15, protowire.VarintType), 1))
		return []*commonpb.KeyValuePair{kv}
	}
	in := withUnknown()
	got := CanonicalKeyValuePairs(in)
	require.Len(t, got, 1)
	assert.NotSame(t, in[0], got[0])
	assert.True(t, proto.Equal(in[0], got[0]))
	// Not shared with an equal list, nor with the list without the field.
	assert.NotSame(t, unsafe.SliceData(got), unsafe.SliceData(CanonicalKeyValuePairs(withUnknown())))
	plain := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.NotSame(t, unsafe.SliceData(got), unsafe.SliceData(plain))
	assert.Empty(t, plain[0].ProtoReflect().GetUnknown())
}

func TestCanonicalKeyValuePairs_DeepCopy(t *testing.T) {
	v := uniqueCanonValue(t)
	in := canonTestPairs("k", v)
	got := CanonicalKeyValuePairs(in)
	assert.NotSame(t, unsafe.SliceData(in), unsafe.SliceData(got))
	assert.NotSame(t, in[0], got[0])

	in[0].Value = "changed"
	in[0] = nil
	assert.Equal(t, v, got[0].GetValue())
	again := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.Same(t, unsafe.SliceData(got), unsafe.SliceData(again))
}

func TestCanonicalKeyValuePairs_AppendDoesNotLeak(t *testing.T) {
	v := uniqueCanonValue(t)
	got := CanonicalKeyValuePairs(canonTestPairs("k", v))
	extended := append(got, &commonpb.KeyValuePair{Key: "extra", Value: "1"})
	assert.Len(t, extended, 2)
	again := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.Len(t, again, 1)
	assert.Same(t, unsafe.SliceData(got), unsafe.SliceData(again))
	assert.NotSame(t, unsafe.SliceData(got), unsafe.SliceData(extended))
}

func TestCanonicalKeyValuePairs_Concurrent(t *testing.T) {
	const values = 16
	prefix := uniqueCanonValue(t)
	results := make([][]*commonpb.KeyValuePair, 8*values)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				v := i % values
				got := CanonicalKeyValuePairs(canonTestPairs("k", prefix+strconv.Itoa(v)))
				if i < values {
					results[g*values+v] = got
				}
			}
		}()
	}
	wg.Wait()
	for v := 0; v < values; v++ {
		for g := 1; g < 8; g++ {
			assert.Same(t, unsafe.SliceData(results[v]), unsafe.SliceData(results[g*values+v]))
		}
	}
}

func TestCanonicalKeyValuePairs_SurvivesGCWhileHeld(t *testing.T) {
	v := uniqueCanonValue(t)
	held := CanonicalKeyValuePairs(canonTestPairs("k", v))
	runtime.GC()
	runtime.GC()
	again := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.Same(t, unsafe.SliceData(held), unsafe.SliceData(again))
	runtime.KeepAlive(held)
}

func TestCanonicalKeyValuePairs_EntryRemovedAfterRelease(t *testing.T) {
	v := uniqueCanonValue(t)
	key := kvCanonKey(canonTestPairs("k", v))
	func() {
		got := CanonicalKeyValuePairs(canonTestPairs("k", v))
		require.Len(t, got, 1)
		_, ok := kvCanon.Load(key)
		require.True(t, ok)
	}()
	assert.Eventually(t, func() bool {
		runtime.GC()
		_, ok := kvCanon.Load(key)
		return !ok
	}, 5*time.Second, 10*time.Millisecond)

	// A later equal list gets a new, working canonical copy.
	got := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.Equal(t, v, got[0].GetValue())
	again := CanonicalKeyValuePairs(canonTestPairs("k", v))
	assert.Same(t, unsafe.SliceData(got), unsafe.SliceData(again))
}

func TestCanonicalKeyValuePairs_KeyEncoding(t *testing.T) {
	long := strings.Repeat("x", 300)
	tests := []struct {
		name string
		kvs  []*commonpb.KeyValuePair
	}{
		{"empty list", nil},
		{"empty key and value", canonTestPairs("", "")},
		{"short", canonTestPairs("k", "v", "index_type", "HNSW")},
		{"two-byte length prefix", canonTestPairs(long, "v", "k", long)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var want []byte
			for _, kv := range tt.kvs {
				want = binary.AppendUvarint(want, uint64(len(kv.GetKey())))
				want = append(want, kv.GetKey()...)
				want = binary.AppendUvarint(want, uint64(len(kv.GetValue())))
				want = append(want, kv.GetValue()...)
			}
			assert.Equal(t, string(want), kvCanonKey(tt.kvs))
			wantAllocs := 1.0
			if len(want) == 0 {
				wantAllocs = 0
			}
			assert.Equal(t, wantAllocs, testing.AllocsPerRun(10, func() { _ = kvCanonKey(tt.kvs) }))
		})
	}
}

func TestUvarintLen(t *testing.T) {
	for _, x := range []uint64{0, 1, 127, 128, 16383, 16384, 1<<63 - 1, 1<<64 - 1} {
		assert.Equal(t, len(binary.AppendUvarint(nil, x)), uvarintLen(x), "x=%d", x)
	}
}

func kvProtoEqual(a, b *commonpb.KeyValuePair) bool {
	return proto.Equal(a, b)
}
