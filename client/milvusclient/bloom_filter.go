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

package milvusclient

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v3/sbbf"
)

// BloomFilterBlob is a client pre-built Split-Block Bloom Filter (MBF1
// envelope) blob for a bloom_match(field, {blob}) filter. Building the filter on
// the client and shipping the compact blob (≈32 MB for ~24M members at the
// default fpr) lets large membership sets pass the proxy gRPC receive limit,
// which a raw value list (≈90 MB for 10M int64) would exceed. When passed as an
// expression template parameter it travels as a native protobuf bytes value
// (schemapb.TemplateValue.bytes_val, no base64) and the proxy embeds it verbatim
// after validating the envelope — it never rebuilds the filter.
//
// The blob is opaque to the element type: build it from the SAME value domain
// as the target field (integer fields hash int64, VARCHAR fields hash UTF-8),
// or the server-side probe will not match. JSON paths
// (bloom_match(meta["user_id"], {blob})) are strictly typed per row: JSON
// strings behave like VARCHAR and JSON integers like int64, but a JSON double
// NEVER matches — a stored 5.0 does not match an int64 member 5, unlike exact
// `in` (which unifies 5.0 == 5). If your writers may float-encode integers,
// normalize on write or use a typed scalar field.
type BloomFilterBlob []byte

// NewBloomFilterBlob builds a BloomFilterBlob from an integer or string
// membership set. members must be a []int64 (for integer fields, or JSON paths
// holding numbers) or []string (for VARCHAR fields, or JSON paths holding
// strings). fpr is the false-positive rate in [sbbf.MinFPR,
// sbbf.MaxFPR]; pass sbbf.DefaultFPR when you have no specific target. The
// resulting blob is reproducible in other languages from the same spec
// (docs/design-docs/design_docs/20260707-bloom-filter-expression.md).
//
// The blob must fit the proxy's gRPC receive limit
// (proxy.grpc.serverMaxRecvSize, 64 MiB by default) alongside the rest of the
// request; use sbbf.EstimateMarshalSize(n, fpr) to check the exact blob size
// for a planned member count before building the filter.
func NewBloomFilterBlob(members any, fpr float64) (BloomFilterBlob, error) {
	// One switch per member type: it both sizes the builder and inserts, so a
	// future member type cannot be added to one dispatch and forgotten in the
	// other (which would silently build an empty, never-matching filter).
	switch vals := members.(type) {
	case []int64:
		builder, err := sbbf.NewBuilder(uint64(len(vals)), fpr)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			builder.AddInt64(v)
		}
		return BloomFilterBlob(builder.Marshal()), nil
	case []string:
		builder, err := sbbf.NewBuilder(uint64(len(vals)), fpr)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			builder.AddString(v)
		}
		return BloomFilterBlob(builder.Marshal()), nil
	default:
		return nil, errors.Errorf("bloom filter members must be []int64 or []string, got %T", members)
	}
}
