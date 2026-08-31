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

// Package roaringfilter builds the exact MRB1 membership-filter envelope for
// membership_match(field, {bitmap}, type=roaring), as specified by the
// roaring-membership design doc
// (docs/design-docs/design_docs/20260714-roaring-exact-membership-expression.md).
//
// This package only builds. Blobs are validated by the proxy
// (pkg/v3/util/roaringfilter) and decoded by segcore
// (internal/core/src/common/RoaringMembership.cpp); a builder does not need to
// re-validate its own output, and shipping a third copy of the validator here
// would be one more implementation to keep bit-compatible.
//
// Its body is the portable 64-bit Roaring format implemented by Go Roaring v2
// and CRoaring C++, so a blob built here is readable by the C++ prober without
// a format of our own. Java interoperability is not claimed until independent
// Java fixtures pass the cross-language suite. Signed values preserve their
// int64 two's-complement bits when mapped into the uint64 key space: INT8(-1)
// becomes 0xffffffffffffffff, not 0xff. Zero-extending a narrow signed value
// instead would silently place it in a different Roaring high container than
// the one segcore probes.
//
// MRB1 envelope layout (all integers little-endian):
//
//	offset  size  field
//	0       4     magic "MRB1"
//	4       2     version      (= 1)
//	6       2     format       (1 = portable_roaring64)
//	8       8     cardinality  (checked against the decoded bitmap)
//	16      8     body_len     (must equal len(blob) - 32)
//	24      8     reserved     (must be 0)
//	32      ...   body: portable Roaring64
//
// Unlike the MBF1 bloom envelope, cardinality here is verified rather than
// informational: the proxy validator scans the body and rejects a blob whose
// actual cardinality disagrees, so a caller cannot understate the cost of a
// filter it asks the cluster to fan out. Build therefore writes the count the
// encoder produced, never a caller-supplied one.
//
// Like client/v3/membership/sbbf, the server validates independently rather than importing
// this package: the plan parser is compiled into a c-shared library that must
// not depend on the standalone client module.
package roaringfilter

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/cockroachdb/errors"
)

const (
	// Magic is the 4-byte MRB1 envelope magic.
	Magic = "MRB1"
	// Version is the MRB1 envelope version implemented by this package.
	Version uint16 = 1
	// FormatPortableRoaring64 identifies the RoaringFormatSpec portable
	// extension for 64-bit integers.
	FormatPortableRoaring64 uint16 = 1
	// HeaderSize is the size in bytes of the MRB1 envelope header.
	HeaderSize = 32
	// MaxBodyBytes bounds an untrusted portable body.
	MaxBodyBytes = 128 * 1024 * 1024
	// MaxHighContainerCount bounds the number of separately allocated Roaring32
	// children created by the C++ execution engine.
	MaxHighContainerCount = 1 << 18
	// MaxEstimatedDecodedBytes bounds one bitmap. The estimate includes the
	// portable body plus conservative fixed overhead for every high-32 child and
	// every Roaring32 container.
	MaxEstimatedDecodedBytes = 64 * 1024 * 1024
	// EstimatedHighContainerOverheadBytes covers the inline C++ Roaring object,
	// its vector entry, and allocator metadata for its top-level arrays.
	EstimatedHighContainerOverheadBytes = 128
	// EstimatedLowContainerOverheadBytes covers decoded container pointers,
	// keys/typecodes, the container header, and allocator metadata. The portable
	// payload itself is already charged through body bytes.
	EstimatedLowContainerOverheadBytes = 64
)

// Build deduplicates members into a portable Roaring64 bitmap and wraps it in
// an MRB1 envelope.
func Build(members []int64) ([]byte, error) {
	var sortedKeys []uint64
	ascending, highContainerCount, lowContainerCount := memberContainerCounts(members)
	if !ascending {
		sortedKeys = make([]uint64, len(members))
		for i, member := range members {
			sortedKeys[i] = uint64(member)
		}
		slices.Sort(sortedKeys)
		highContainerCount, lowContainerCount = sortedKeyContainerCounts(sortedKeys)
	}
	if _, err := estimateAndCheckDecodedBytes(0, highContainerCount, lowContainerCount); err != nil {
		return nil, err
	}

	bitmap := roaring64.New()
	if len(members) > 0 {
		// Insert in ascending key order. A Roaring bitmap is a set, so input
		// order cannot change the result, but roaring64 keeps its high-32
		// containers in a sorted slice and binary-searches it per inserted
		// value: an unsorted set spread over many 2^32 buckets makes each new
		// key an insert into the middle of that slice, which is O(n^2) memmove
		// overall (measured: 51s for 1M uniformly-random int64, vs 0.2s sorted).
		//
		// Ordering is on the two's-complement key, not the signed value: an
		// int64 slice that is ascending as signed, like {-1, 5}, maps to
		// {0xffffffffffffffff, 5} and is descending as keys.
		if ascending {
			// Already in key order — the common shape for auto-increment ids,
			// contiguous ranges and sorted query results. Insert in place: each
			// Add extends the last container or appends a new one, so this is
			// linear and, unlike the sort path, allocates nothing per member.
			for _, member := range members {
				bitmap.Add(uint64(member))
			}
		} else {
			bitmap.AddMany(sortedKeys)
		}
	}
	bitmap.RunOptimize()

	// GetSerializedSizeInBytes is exact for the portable format, so the envelope
	// can be sized up front and the body written straight into its tail. Going
	// through ToBytes would allocate a second, equally large buffer and copy it
	// in, doubling peak footprint for a blob that is already the largest thing
	// in the request.
	bodyLen := bitmap.GetSerializedSizeInBytes()
	if bodyLen > MaxBodyBytes {
		return nil, errors.Errorf(
			"roaring bitmap body too large: %d bytes, exceeds max %d", bodyLen, MaxBodyBytes)
	}
	if _, err := estimateAndCheckDecodedBytes(bodyLen, highContainerCount, lowContainerCount); err != nil {
		return nil, err
	}

	blob := make([]byte, HeaderSize+int(bodyLen))
	copy(blob[0:4], Magic)
	binary.LittleEndian.PutUint16(blob[4:6], Version)
	binary.LittleEndian.PutUint16(blob[6:8], FormatPortableRoaring64)
	binary.LittleEndian.PutUint64(blob[8:16], bitmap.GetCardinality())
	binary.LittleEndian.PutUint64(blob[16:24], bodyLen)
	binary.LittleEndian.PutUint64(blob[24:32], 0)

	// A zero-length slice over the envelope's tail: WriteTo appends into the
	// existing capacity rather than growing a new buffer.
	body := bytes.NewBuffer(blob[HeaderSize:HeaderSize])
	written, err := bitmap.WriteTo(body)
	if err != nil {
		return nil, errors.Errorf("failed to serialize roaring bitmap: %v", err)
	}
	if uint64(written) != bodyLen || body.Len() != int(bodyLen) {
		// Would mean GetSerializedSizeInBytes disagreed with WriteTo, so the
		// declared body_len no longer describes the bytes. Fail rather than emit
		// a blob every validator downstream would reject.
		return nil, errors.Errorf(
			"roaring bitmap serialized %d bytes into a %d-byte body", written, bodyLen)
	}
	return blob, nil
}

// memberContainerCounts reports whether members is already non-decreasing in
// the unsigned key space and, when it is, counts distinct high-32 children and
// Roaring32 low containers without allocating. Equal keys are duplicates.
func memberContainerCounts(members []int64) (bool, uint64, uint64) {
	if len(members) == 0 {
		return true, 0, 0
	}
	previous := uint64(members[0])
	highContainerCount := uint64(1)
	lowContainerCount := uint64(1)
	for i := 1; i < len(members); i++ {
		key := uint64(members[i])
		if key < previous {
			return false, 0, 0
		}
		if key>>32 != previous>>32 {
			highContainerCount++
		}
		if key>>16 != previous>>16 {
			lowContainerCount++
		}
		previous = key
	}
	return true, highContainerCount, lowContainerCount
}

func sortedKeyContainerCounts(keys []uint64) (uint64, uint64) {
	if len(keys) == 0 {
		return 0, 0
	}
	highContainerCount := uint64(1)
	lowContainerCount := uint64(1)
	previous := keys[0]
	for _, key := range keys[1:] {
		if key>>32 != previous>>32 {
			highContainerCount++
		}
		if key>>16 != previous>>16 {
			lowContainerCount++
		}
		previous = key
	}
	return highContainerCount, lowContainerCount
}

func estimateAndCheckDecodedBytes(bodyBytes, highContainerCount, lowContainerCount uint64) (uint64, error) {
	if highContainerCount > MaxHighContainerCount {
		return 0, errors.Errorf(
			"roaring bitmap high-container count %d exceeds maximum %d",
			highContainerCount, MaxHighContainerCount)
	}
	estimated := bodyBytes + highContainerCount*EstimatedHighContainerOverheadBytes +
		lowContainerCount*EstimatedLowContainerOverheadBytes
	if estimated > MaxEstimatedDecodedBytes {
		return 0, errors.Errorf(
			"roaring bitmap estimated decoded size %d bytes exceeds maximum %d",
			estimated, MaxEstimatedDecodedBytes)
	}
	return estimated, nil
}
