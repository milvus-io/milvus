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

package routing

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestEncodeNamespace(t *testing.T) {
	const ns = "tenant-a"
	key := EncodeNamespace(ns)

	// the key is a 4-byte big-endian hash prefix followed by the raw namespace.
	assert.Len(t, key, 4+len(ns))
	assert.Equal(t, typeutil.HashString2Uint32(ns), binary.BigEndian.Uint32(key[:4]))
	assert.Equal(t, ns, string(key[4:]))

	// deterministic.
	assert.Equal(t, key, EncodeNamespace(ns))

	// distinct namespaces never collide even if their hash prefixes were to
	// match, because the raw namespace is appended.
	assert.NotEqual(t, EncodeNamespace("a"), EncodeNamespace("b"))

	// empty namespace is still encodable: just the 4-byte prefix.
	assert.Len(t, EncodeNamespace(""), 4)
}

func TestEncodeNamespaceOrderMatchesHash(t *testing.T) {
	// big-endian encoding makes byte order equal the hash's numeric order.
	a, b := "alpha", "beta"
	ha, hb := typeutil.HashString2Uint32(a), typeutil.HashString2Uint32(b)
	cmp := bytes.Compare(EncodeNamespace(a), EncodeNamespace(b))
	switch {
	case ha < hb:
		assert.Negative(t, cmp)
	case ha > hb:
		assert.Positive(t, cmp)
	default:
		// equal hash prefixes fall back to the raw namespace order.
		assert.Equal(t, bytes.Compare([]byte(a), []byte(b)), cmp)
	}
}

func TestNamespaceEncoder(t *testing.T) {
	var enc NamespaceEncoder
	assert.Equal(t, EncodeNamespace("ns"), enc.EncodeNamespace("ns"))
}

// threeShards returns a table that splits the key space at 0x40 and 0x80 into
// v0 = [-inf, 0x40), v1 = [0x40, 0x80), v2 = [0x80, +inf).
func threeShards(t *testing.T) *RangeRoutingTable {
	t.Helper()
	table, err := DeriveRange(7, []RangeShard{
		{Lower: nil, Upper: []byte{0x40}, Vchannel: "v0"},
		{Lower: []byte{0x40}, Upper: []byte{0x80}, Vchannel: "v1"},
		{Lower: []byte{0x80}, Upper: nil, Vchannel: "v2"},
	})
	assert.NoError(t, err)
	return table
}

func TestDeriveRangeAndLookup(t *testing.T) {
	table := threeShards(t)
	assert.Equal(t, int64(7), table.Version)
	assert.Equal(t, 3, table.NumShards())

	cases := []struct {
		key  []byte
		want string
	}{
		{[]byte{0x00}, "v0"},
		{[]byte{0x3f}, "v0"},
		{[]byte{0x40}, "v1"}, // lower bound is inclusive
		{[]byte{0x7f}, "v1"},
		{[]byte{0x80}, "v2"}, // upper bound is exclusive
		{[]byte{0xff}, "v2"},
		{[]byte{}, "v0"},                 // empty key is the smallest, -inf side
		{[]byte{0x40, 0x00}, "v1"},       // longer key just past a boundary
		{[]byte{0x3f, 0xff, 0xff}, "v0"}, // longer key just before a boundary
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, table.Lookup(c.key), "key=%x", c.key)
	}
}

func TestDeriveRangeSortsInput(t *testing.T) {
	// shards given out of order are sorted by lower bound before use.
	table, err := DeriveRange(1, []RangeShard{
		{Lower: []byte{0x80}, Upper: nil, Vchannel: "v2"},
		{Lower: nil, Upper: []byte{0x40}, Vchannel: "v0"},
		{Lower: []byte{0x40}, Upper: []byte{0x80}, Vchannel: "v1"},
	})
	assert.NoError(t, err)
	assert.Equal(t, "v0", table.Lookup([]byte{0x10}))
	assert.Equal(t, "v1", table.Lookup([]byte{0x60}))
	assert.Equal(t, "v2", table.Lookup([]byte{0x90}))
}

func TestDeriveRangeSingleShard(t *testing.T) {
	// a single shard owns the whole space.
	table, err := DeriveRange(1, []RangeShard{{Lower: nil, Upper: nil, Vchannel: "v0"}})
	assert.NoError(t, err)
	assert.Equal(t, "v0", table.Lookup([]byte{0x00}))
	assert.Equal(t, "v0", table.Lookup([]byte{0xff, 0xff}))
}

func TestDeriveRangeErrors(t *testing.T) {
	cases := []struct {
		name   string
		shards []RangeShard
	}{
		{"empty", nil},
		{"no -inf start", []RangeShard{
			{Lower: []byte{0x10}, Upper: nil, Vchannel: "v0"},
		}},
		{"no +inf end", []RangeShard{
			{Lower: nil, Upper: []byte{0x80}, Vchannel: "v0"},
		}},
		{"gap", []RangeShard{
			{Lower: nil, Upper: []byte{0x40}, Vchannel: "v0"},
			{Lower: []byte{0x50}, Upper: nil, Vchannel: "v1"}, // gap [0x40,0x50)
		}},
		{"overlap", []RangeShard{
			{Lower: nil, Upper: []byte{0x60}, Vchannel: "v0"},
			{Lower: []byte{0x40}, Upper: nil, Vchannel: "v1"}, // overlaps [0x40,0x60)
		}},
		{"empty vchannel", []RangeShard{
			{Lower: nil, Upper: []byte{0x40}, Vchannel: ""},
			{Lower: []byte{0x40}, Upper: nil, Vchannel: "v1"},
		}},
		{"unbounded interior upper", []RangeShard{
			{Lower: nil, Upper: nil, Vchannel: "v0"}, // interior shard cannot be +inf
			{Lower: []byte{0x40}, Upper: nil, Vchannel: "v1"},
		}},
		{"inverted range", []RangeShard{
			{Lower: nil, Upper: []byte{0x40}, Vchannel: "v0"},
			{Lower: []byte{0x40}, Upper: []byte{0x40}, Vchannel: "v1"}, // empty [0x40,0x40)
			{Lower: []byte{0x40}, Upper: nil, Vchannel: "v2"},
		}},
	}
	for _, c := range cases {
		_, err := DeriveRange(1, c.shards)
		assert.Errorf(t, err, "case %s should fail", c.name)
	}
}

func TestLookupNamespace(t *testing.T) {
	// split the namespace key space at the encoded key of "m": namespaces whose
	// encoded key sorts below it go left (v0), the rest go right (v1).
	splitKey := EncodeNamespace("m")
	table, err := DeriveRange(2, []RangeShard{
		{Lower: nil, Upper: splitKey, Vchannel: "v0"},
		{Lower: splitKey, Upper: nil, Vchannel: "v1"},
	})
	assert.NoError(t, err)

	for _, ns := range []string{"a", "m", "tenant-x", "z", "", "namespace-1"} {
		want := "v1"
		if bytes.Compare(EncodeNamespace(ns), splitKey) < 0 {
			want = "v0"
		}
		assert.Equalf(t, want, table.LookupNamespace(ns), "namespace=%q", ns)
	}
}
