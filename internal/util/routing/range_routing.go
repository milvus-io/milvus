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
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// EncodeNamespace returns the byte-comparable routing key of a namespace:
//
//	routing_key = big_endian(hash32(namespace)) || namespace_utf8
//
// The 4-byte big-endian hash prefix spreads namespaces uniformly across the key
// space to avoid hotspots; appending the raw namespace makes the key unique and
// deterministic, so two distinct namespaces never collide regardless of the
// hash; big-endian encoding keeps byte order equal to the hash's numeric order.
// This is the single encoding shared by the datacoord split planner, the proxy
// router and the streamingnode, so all three agree on shard boundaries
// (design §3.1).
//
// The hash function (decision D1) is intentionally the only thing isolated here:
// because the appended namespace already guarantees uniqueness, changing the
// hash only reshuffles distribution, never routing correctness. It defaults to
// typeutil.HashString2Uint32 — the same hash the legacy pk routing uses — so a
// namespace and a non-namespace collection hash strings identically.
func EncodeNamespace(namespace string) []byte {
	key := make([]byte, 4, 4+len(namespace))
	binary.BigEndian.PutUint32(key, typeutil.HashString2Uint32(namespace))
	return append(key, namespace...)
}

// NamespaceEncoder is the production routing-key encoder. It satisfies the
// datacoord split planner's routingKeyEncoder seam by delegating to
// EncodeNamespace, so the planner and the router share one encoding.
type NamespaceEncoder struct{}

// EncodeNamespace implements the routingKeyEncoder seam.
func (NamespaceEncoder) EncodeNamespace(namespace string) []byte {
	return EncodeNamespace(namespace)
}

// RangeShard describes one shard's ownership of the routing-key space: the
// vchannel and its half-open range [Lower, Upper). A nil bound is unbounded
// (Lower nil = -inf, Upper nil = +inf).
type RangeShard struct {
	Lower    []byte
	Upper    []byte
	Vchannel string
}

// RangeRoutingTable routes a routing key (see EncodeNamespace) to a vchannel by
// binary search over the shards' contiguous [lower, upper) ranges. It backs the
// ModeRange routing of namespace collections after a shard split. Lookup is
// O(log #shards).
type RangeRoutingTable struct {
	// Version identifies the routing epoch; it advances on every routing change
	// (e.g. a shard split) so streaming nodes can detect a stale route.
	Version int64

	// uppers[i] is the exclusive upper bound of shard i. The lower bound of shard
	// i is uppers[i-1] (and nil for i == 0). uppers is strictly increasing and
	// its last element is nil (+inf), so the shards partition the whole key space
	// with no gap and no overlap. channels[i] owns shard i.
	uppers   [][]byte
	channels []string
}

// DeriveRange builds a RangeRoutingTable from the shards of a collection. The
// shards must partition the whole key space: sorted by lower bound, contiguous
// (each shard's upper equals the next shard's lower), with the first lower and
// the last upper unbounded (nil). It returns an error if the shards leave a gap,
// overlap, or do not cover the space, so a malformed routing meta is rejected
// rather than silently mis-routing.
func DeriveRange(version int64, shards []RangeShard) (*RangeRoutingTable, error) {
	if len(shards) == 0 {
		return nil, errors.New("range routing table needs at least one shard")
	}
	sorted := make([]RangeShard, len(shards))
	copy(sorted, shards)
	sort.Slice(sorted, func(i, j int) bool {
		// nil lower (-inf) sorts first; otherwise compare bytewise.
		if sorted[i].Lower == nil {
			return sorted[j].Lower != nil
		}
		if sorted[j].Lower == nil {
			return false
		}
		return bytes.Compare(sorted[i].Lower, sorted[j].Lower) < 0
	})

	if sorted[0].Lower != nil {
		return nil, errors.Errorf("range routing table does not start at -inf, first lower is %x", sorted[0].Lower)
	}
	if sorted[len(sorted)-1].Upper != nil {
		return nil, errors.Errorf("range routing table does not end at +inf, last upper is %x", sorted[len(sorted)-1].Upper)
	}

	uppers := make([][]byte, len(sorted))
	channels := make([]string, len(sorted))
	for i, shard := range sorted {
		if shard.Vchannel == "" {
			return nil, errors.New("range routing shard has an empty vchannel")
		}
		// every interior boundary must be contiguous: this shard's upper is the
		// next shard's lower, with no gap and no overlap.
		if i+1 < len(sorted) {
			if shard.Upper == nil {
				return nil, errors.Errorf("non-last range routing shard %s has an unbounded upper", shard.Vchannel)
			}
			if !bytes.Equal(shard.Upper, sorted[i+1].Lower) {
				return nil, errors.Errorf("range routing shards are not contiguous: shard %s upper %x != next lower %x",
					shard.Vchannel, shard.Upper, sorted[i+1].Lower)
			}
			if bytes.Compare(shard.Upper, sorted[i].Lower) <= 0 {
				return nil, errors.Errorf("range routing shard %s has empty or inverted range [%x, %x)",
					shard.Vchannel, sorted[i].Lower, shard.Upper)
			}
		}
		uppers[i] = shard.Upper
		channels[i] = shard.Vchannel
	}
	return &RangeRoutingTable{Version: version, uppers: uppers, channels: channels}, nil
}

// NumShards returns the number of shards in the table.
func (t *RangeRoutingTable) NumShards() int {
	return len(t.channels)
}

// Lookup returns the vchannel owning the routing key by binary search over the
// shard ranges: the owning shard is the first whose exclusive upper bound is
// greater than the key (a nil upper is +inf). Because the shards cover the whole
// space, every key resolves to exactly one shard.
func (t *RangeRoutingTable) Lookup(key []byte) string {
	i := sort.Search(len(t.uppers), func(i int) bool {
		// nil upper (+inf) is greater than every key; otherwise key < upper.
		return t.uppers[i] == nil || bytes.Compare(key, t.uppers[i]) < 0
	})
	if i >= len(t.channels) {
		// unreachable: the last upper is nil so the predicate holds at i = last.
		return ""
	}
	return t.channels[i]
}

// LookupNamespace returns the vchannel owning a namespace, encoding it first.
func (t *RangeRoutingTable) LookupNamespace(namespace string) string {
	return t.Lookup(EncodeNamespace(namespace))
}
