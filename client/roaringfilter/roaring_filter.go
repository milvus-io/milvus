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

// Package roaringfilter implements the exact MRB1 membership-filter envelope,
// as specified by the roaring-membership design doc
// (docs/design-docs/design_docs/20260714-roaring-exact-membership-expression.md).
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
// informational: Validate scans the body and rejects a blob whose actual
// cardinality disagrees, so a caller cannot understate the cost of a filter it
// asks the cluster to fan out.
//
// This package deliberately mirrors client/v3/sbbf's shape and, like it, is
// validated independently on the server (internal/parser/planparserv2) rather
// than imported there — the plan parser is compiled into a c-shared library
// that must not depend on the standalone client module.
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

	portableRoaring64PrefixBytes   = 8
	portableRoaring64MinEntryBytes = 12
)

// ValidationSummary describes a structurally valid MRB1 blob without
// materializing a roaring64.Bitmap.
type ValidationSummary struct {
	Cardinality           uint64
	BodyBytes             uint64
	HighContainerCount    uint64
	LowContainerCount     uint64
	EstimatedDecodedBytes uint64
}

// Filter is an immutable exact-membership bitmap after construction.
type Filter struct {
	bitmap *roaring64.Bitmap
}

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

func prevalidatePortableRoaring64Body(body []byte) error {
	if len(body) < portableRoaring64PrefixBytes {
		return errors.Errorf(
			"invalid portable roaring64 body: too short for high-container count: %d bytes",
			len(body))
	}

	highContainerCount := binary.LittleEndian.Uint64(body[:portableRoaring64PrefixBytes])
	// Every high container needs a 4-byte high key plus at least an 8-byte
	// portable Roaring32 child: the no-run cookie followed by a container
	// count of zero. CRoaring 3.0.0 emits exactly that for an empty child, and
	// both CRoaring and roaring/v2 consume it, so the bound must admit 12 bytes
	// per entry rather than assuming every child carries a value. This
	// necessary bound prevents ReadFrom from allocating its three top-level
	// slices from an attacker-controlled count that the body cannot possibly
	// contain.
	maxCountFromBody := uint64(len(body)-portableRoaring64PrefixBytes) / portableRoaring64MinEntryBytes
	if highContainerCount > maxCountFromBody {
		return errors.Errorf(
			"invalid portable roaring64 body: high-container count %d exceeds body-derived maximum %d",
			highContainerCount, maxCountFromBody)
	}
	if highContainerCount > MaxHighContainerCount {
		return errors.Errorf(
			"roaring bitmap high-container count %d exceeds maximum %d",
			highContainerCount, MaxHighContainerCount)
	}
	return nil
}

func decodePortableRoaring64(body []byte) (bitmap *roaring64.Bitmap, consumed int64, err error) {
	bitmap = roaring64.New()
	defer func() {
		if recovered := recover(); recovered != nil {
			bitmap = nil
			consumed = 0
			err = errors.Errorf(
				"invalid portable roaring64 body: decoder panic: %v", recovered)
		}
	}()

	consumed, err = bitmap.ReadFrom(bytes.NewReader(body))
	if err != nil {
		return nil, consumed, errors.Errorf(
			"invalid portable roaring64 body: %v", err)
	}
	return bitmap, consumed, nil
}

func validateEnvelope(blob []byte) (uint64, []byte, error) {
	if len(blob) < HeaderSize {
		return 0, nil, errors.Errorf(
			"roaring bitmap blob too short: %d bytes, need at least %d", len(blob), HeaderSize)
	}
	if len(blob) > HeaderSize+MaxBodyBytes {
		return 0, nil, errors.Errorf(
			"roaring bitmap blob too large: %d bytes, exceeds max %d",
			len(blob), HeaderSize+MaxBodyBytes)
	}
	if !bytes.Equal(blob[0:4], []byte(Magic)) {
		return 0, nil, errors.Errorf(
			"roaring bitmap blob has invalid magic, expected %q", Magic)
	}
	if version := binary.LittleEndian.Uint16(blob[4:6]); version != Version {
		return 0, nil, errors.Errorf(
			"unsupported roaring bitmap version %d, expected %d", version, Version)
	}
	if format := binary.LittleEndian.Uint16(blob[6:8]); format != FormatPortableRoaring64 {
		return 0, nil, errors.Errorf(
			"unsupported roaring bitmap format %d, expected %d", format, FormatPortableRoaring64)
	}
	if reserved := binary.LittleEndian.Uint64(blob[24:32]); reserved != 0 {
		return 0, nil, errors.Errorf(
			"roaring bitmap reserved field must be 0, got %d", reserved)
	}

	bodyLen := binary.LittleEndian.Uint64(blob[16:24])
	if bodyLen > MaxBodyBytes {
		return 0, nil, errors.Errorf(
			"roaring bitmap body too large: %d bytes, exceeds max %d", bodyLen, MaxBodyBytes)
	}
	actualBodyLen := uint64(len(blob) - HeaderSize)
	if bodyLen != actualBodyLen {
		return 0, nil, errors.Errorf(
			"roaring bitmap body length %d does not match header value %d", actualBodyLen, bodyLen)
	}

	return binary.LittleEndian.Uint64(blob[8:16]), blob[HeaderSize:], nil
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

// Validate checks an MRB1 blob and reports its resource shape without
// materializing a roaring64.Bitmap. Its success path is allocation-free.
func Validate(blob []byte) (ValidationSummary, error) {
	declaredCardinality, body, err := validateEnvelope(blob)
	if err != nil {
		return ValidationSummary{}, err
	}
	wire, err := scanPortableRoaring64Body(body)
	if err != nil {
		return ValidationSummary{}, err
	}
	if wire.cardinality != declaredCardinality {
		return ValidationSummary{}, errors.Errorf(
			"roaring bitmap cardinality %d does not match declared value %d",
			wire.cardinality, declaredCardinality)
	}
	estimatedDecodedBytes, err := estimateAndCheckDecodedBytes(
		uint64(len(body)), wire.highContainerCount, wire.lowContainerCount)
	if err != nil {
		return ValidationSummary{}, err
	}
	return ValidationSummary{
		Cardinality:           wire.cardinality,
		BodyBytes:             uint64(len(body)),
		HighContainerCount:    wire.highContainerCount,
		LowContainerCount:     wire.lowContainerCount,
		EstimatedDecodedBytes: estimatedDecodedBytes,
	}, nil
}

// Parse validates an MRB1 blob and deserializes its portable Roaring64 body.
func Parse(blob []byte) (*Filter, error) {
	summary, err := Validate(blob)
	if err != nil {
		return nil, err
	}
	body := blob[HeaderSize:]
	bitmap, consumed, err := decodePortableRoaring64(body)
	if err != nil {
		return nil, err
	}
	if uint64(consumed) != summary.BodyBytes {
		return nil, errors.Errorf(
			"portable roaring64 body consumed %d bytes, expected %d", consumed, summary.BodyBytes)
	}
	if cardinality := bitmap.GetCardinality(); cardinality != summary.Cardinality {
		return nil, errors.Errorf(
			"roaring bitmap cardinality %d does not match declared value %d",
			cardinality, summary.Cardinality)
	}
	return &Filter{bitmap: bitmap}, nil
}

// ContainsInt64 reports exact membership for v.
func (f *Filter) ContainsInt64(v int64) bool {
	return f.bitmap.Contains(uint64(v))
}

// Cardinality returns the number of distinct values in the bitmap.
func (f *Filter) Cardinality() uint64 {
	return f.bitmap.GetCardinality()
}
