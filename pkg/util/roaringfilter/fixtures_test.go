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

// buildFixture, parseFixture and fixtureFilter are fixtures for the validator tests, not
// package API. The proxy only ever calls Validate; blobs are built by the SDK
// (client/v3/roaringfilter) and decoded by segcore, so exporting a second
// builder and a second decoder from this package would be dead weight that has
// to stay bit-compatible with two other implementations.
//
// They live here rather than in the SDK because the pkg module must not depend
// on the client module, and because a validator is better tested against bytes
// its own package did not produce.

package roaringfilter

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/RoaringBitmap/roaring/v2/roaring64"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fixtureFilter is a decoded exact-membership bitmap.
type fixtureFilter struct {
	bitmap *roaring64.Bitmap
}

// buildFixture encodes members as a portable Roaring64 bitmap in an MRB1 envelope.
//
// It is not the SDK builder and does not try to be: the SDK
// (client/v3/roaringfilter) additionally pre-rejects member sets the proxy
// would reject, and no buildFixture call site in this package asserts a rejection -- they
// all require.NoError. Reproducing that admission logic here
// would be a second copy of it that nothing checks.
//
// The bytes are the same, which is what fixtures need: a Roaring bitmap is a
// set, so insertion order cannot change the encoding, and RunOptimize is
// applied on both sides. The contract that the SDK's output is something
// Validate accepts is tested against the real SDK builder in
// TestClientBuiltBlobsPassProxyValidation (internal/parser/planparserv2).
func buildFixture(members []int64) ([]byte, error) {
	keys := make([]uint64, len(members))
	for i, member := range members {
		keys[i] = uint64(member)
	}
	// Sorted insertion only for speed: roaring64 binary-searches its high-32
	// container slice per inserted value, so an unsorted set spread over many
	// 2^32 buckets is O(n^2) memmove. It does not affect the encoding.
	slices.Sort(keys)

	bitmap := roaring64.New()
	bitmap.AddMany(keys)
	bitmap.RunOptimize()

	bodyLen := bitmap.GetSerializedSizeInBytes()
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
		return nil, merr.WrapErrParameterInvalidMsg("failed to serialize roaring bitmap: %v", err)
	}
	if uint64(written) != bodyLen {
		return nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap serialized %d bytes into a %d-byte body", written, bodyLen)
	}
	return blob, nil
}

func decodePortableRoaring64(body []byte) (bitmap *roaring64.Bitmap, consumed int64, err error) {
	bitmap = roaring64.New()
	defer func() {
		if recovered := recover(); recovered != nil {
			bitmap = nil
			consumed = 0
			err = merr.WrapErrParameterInvalidMsg(
				"invalid portable roaring64 body: decoder panic: %v", recovered)
		}
	}()

	consumed, err = bitmap.ReadFrom(bytes.NewReader(body))
	if err != nil {
		return nil, consumed, merr.WrapErrParameterInvalidMsg(
			"invalid portable roaring64 body: %v", err)
	}
	return bitmap, consumed, nil
}

// parseFixture validates an MRB1 blob and deserializes its portable Roaring64 body.
// The extra decode-and-cross-check is what segcore does in production
// (RoaringMembership::Parse); keeping an equivalent here lets the Go tests
// assert that a blob Validate accepted really does decode to the declared
// cardinality.
func parseFixture(blob []byte) (*fixtureFilter, error) {
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
		return nil, merr.WrapErrParameterInvalidMsg(
			"portable roaring64 body consumed %d bytes, expected %d", consumed, summary.BodyBytes)
	}
	if cardinality := bitmap.GetCardinality(); cardinality != summary.Cardinality {
		return nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap cardinality %d does not match declared value %d",
			cardinality, summary.Cardinality)
	}
	return &fixtureFilter{bitmap: bitmap}, nil
}

// ContainsInt64 reports exact membership for v.
func (f *fixtureFilter) ContainsInt64(v int64) bool {
	return f.bitmap.Contains(uint64(v))
}

// Cardinality returns the number of distinct values in the bitmap.
func (f *fixtureFilter) Cardinality() uint64 {
	return f.bitmap.GetCardinality()
}
