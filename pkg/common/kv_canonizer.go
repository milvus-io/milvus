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
	"math/bits"
	"runtime"
	"strings"
	"sync"
	"unsafe"
	"weak"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// kvCanonEntry locates a canonical list: a weak pointer to the first element
// of its backing array, and its length. The weak pointer targets the shared
// data itself, so the entry stays valid exactly as long as some holder keeps
// the list (or any slice of it) reachable.
type kvCanonEntry struct {
	first weak.Pointer[*commonpb.KeyValuePair]
	n     int
}

type kvCanonCleanup struct {
	key   string
	entry kvCanonEntry
}

// kvCanon maps the length-prefixed encoding of a list to its kvCanonEntry.
var kvCanon sync.Map

// CanonicalKeyValuePairs returns a canonical list equal to kvs
// (order-sensitive), shared by every caller that passes an equal list while
// the canonical one is in use. The canonical list is a deep copy, never the
// caller's slice, and its capacity equals its length, so an append by a
// holder always reallocates. The returned list and its elements are shared
// and must be treated as read-only.
//
// The sharing is weak, like the unique package: the table does not keep a
// list alive. Once no holder references a canonical list, the GC frees it and
// its entry is removed, and a later equal list gets a new canonical copy.
// nil stays nil, and an empty list is returned with zero capacity.
//
// A list with a pair that carries unknown protobuf fields (from a newer
// writer) is not shared: the key and the copy cover only Key and Value, so it
// gets its own deep copy that keeps those fields.
func CanonicalKeyValuePairs(kvs []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	if len(kvs) == 0 {
		return kvs[:0:0]
	}
	if hasUnknownFields(kvs) {
		out := make([]*commonpb.KeyValuePair, len(kvs))
		for i, kv := range kvs {
			out[i] = proto.Clone(kv).(*commonpb.KeyValuePair)
		}
		return out
	}
	key := kvCanonKey(kvs)
	var fresh []*commonpb.KeyValuePair
	var entry kvCanonEntry
	for {
		v, ok := kvCanon.Load(key)
		if ok {
			old := v.(kvCanonEntry)
			if p := old.first.Value(); p != nil {
				return unsafe.Slice(p, old.n)
			}
		}
		if fresh == nil {
			fresh = CloneKeyValuePairs(kvs)
			entry = kvCanonEntry{first: weak.Make(&fresh[0]), n: len(fresh)}
		}
		var stored bool
		if ok {
			// The canonical list was freed; its cleanup may not have run yet.
			stored = kvCanon.CompareAndSwap(key, v, entry)
		} else {
			_, loaded := kvCanon.LoadOrStore(key, entry)
			stored = !loaded
		}
		if stored {
			runtime.AddCleanup(&fresh[0], func(c kvCanonCleanup) {
				kvCanon.CompareAndDelete(c.key, c.entry)
			}, kvCanonCleanup{key: key, entry: entry})
			return fresh
		}
		// Another caller stored an entry first; use it if it is live.
	}
}

// hasUnknownFields reports whether any pair carries unknown protobuf fields.
func hasUnknownFields(kvs []*commonpb.KeyValuePair) bool {
	for _, kv := range kvs {
		if kv != nil && len(kv.ProtoReflect().GetUnknown()) > 0 {
			return true
		}
	}
	return false
}

// kvCanonKey encodes the pairs in order with length prefixes, so distinct
// lists never collide regardless of the bytes inside keys and values. It
// allocates once, for the exact size, and not at all for an empty list.
func kvCanonKey(kvs []*commonpb.KeyValuePair) string {
	n := 0
	for _, kv := range kvs {
		k, v := len(kv.GetKey()), len(kv.GetValue())
		n += uvarintLen(uint64(k)) + k + uvarintLen(uint64(v)) + v
	}
	if n == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	var prefix [binary.MaxVarintLen64]byte
	for _, kv := range kvs {
		b.Write(binary.AppendUvarint(prefix[:0], uint64(len(kv.GetKey()))))
		b.WriteString(kv.GetKey())
		b.Write(binary.AppendUvarint(prefix[:0], uint64(len(kv.GetValue()))))
		b.WriteString(kv.GetValue())
	}
	return b.String()
}

// uvarintLen returns the length of x in binary.AppendUvarint encoding.
func uvarintLen(x uint64) int {
	return (bits.Len64(x|1) + 6) / 7
}
