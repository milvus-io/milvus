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

// Package routing maps a write's routing key to the vchannel that owns it.
//
// Every collection routes by the hash of its routing key modulo the collection's
// routing modulus; a collection that has never been split carries no routing meta
// and falls back to the legacy hash % shardNum, bit for bit. Table is the entry
// point; RoutingTable below is the legacy-only form the fallback path uses.
package routing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RoutingTable maps primary keys to vchannel names by rawHash % len(channels).
// Use DeriveCompat to create a table that mirrors the legacy behaviour.
type RoutingTable struct {
	// channels is the ordered list of vchannel names. The index into this slice
	// is the bucket number produced by LookupBucket.
	channels []string
}

// DeriveCompat returns a RoutingTable that is bit-for-bit equivalent to calling
// typeutil.HashPK2Channels(pks, channelNames). It preserves the existing
// hash-modulo-numShards behaviour so that P0 routing introduces no correctness
// change.
func DeriveCompat(channelNames []string) *RoutingTable {
	ch := make([]string, len(channelNames))
	copy(ch, channelNames)
	return &RoutingTable{channels: ch}
}

// NumShards returns the number of vchannels (shards) in this routing table.
func (t *RoutingTable) NumShards() int {
	return len(t.channels)
}

// LookupBucket maps a raw hash value to a vchannel name by computing
// rawHash % numShards. Returns "" when the table has no channels.
func (t *RoutingTable) LookupBucket(rawHash uint32) string {
	if len(t.channels) == 0 {
		return ""
	}
	return t.channels[rawHash%uint32(len(t.channels))]
}

// RouteInsert returns the map channelName->rowOffsets and the per-row hash values
// for the given primary keys, bit-for-bit equal to internal/proxy.assignChannelsByPK.
func (t *RoutingTable) RouteInsert(pks *schemapb.IDs) (map[string][]int, []uint32) {
	numChannels := len(t.channels)
	if numChannels == 0 {
		return nil, nil
	}
	hashValues := t.HashPKs(pks)
	avgCapacity := (len(hashValues) / numChannels) + 1
	out := make(map[string][]int, numChannels)
	for offset, channelID := range hashValues {
		idx := int(channelID)
		// idx is always in [0, numChannels) because HashPKs reduces modulo
		// numChannels. The guard is kept to mirror proxy.assignChannelsByPK
		// verbatim, so the two stay structurally identical.
		if idx >= numChannels {
			continue
		}
		name := t.channels[idx]
		if _, ok := out[name]; !ok {
			out[name] = make([]int, 0, avgCapacity)
		}
		out[name] = append(out[name], offset)
	}
	return out, hashValues
}

// HashPKs maps each primary key in pks to a shard index (0-based position in
// the channel list). The result is bit-for-bit identical to
// typeutil.HashPK2Channels(pks, channelNames) for both int64 and varchar PKs;
// unsupported ID types return nil.
func (t *RoutingTable) HashPKs(pks *schemapb.IDs) []uint32 {
	n := uint32(len(t.channels))
	if n == 0 {
		// No shard to reduce against. Returning nil matches RouteInsert and
		// keeps the caller's "no channels" check the one that reports it; the
		// modulo below would divide by zero.
		return nil
	}
	var out []uint32
	switch pks.IdField.(type) {
	case *schemapb.IDs_IntId:
		// Append from nil (not make+index) so a zero-length input yields nil,
		// matching typeutil.HashPK2Channels exactly.
		for _, pk := range pks.GetIntId().Data {
			v, _ := typeutil.Hash32Int64(pk)
			out = append(out, v%n)
		}
	case *schemapb.IDs_StrId:
		for _, pk := range pks.GetStrId().Data {
			out = append(out, typeutil.HashString2Uint32(pk)%n)
		}
	default:
		// Unsupported type: mirrors typeutil.HashPK2Channels behaviour.
	}
	return out
}
