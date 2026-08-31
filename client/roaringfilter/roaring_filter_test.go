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

package roaringfilter

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

// The structural rejection suite for this format lives with the validator, in
// pkg/util/roaringfilter, and the cross-check that a blob built here is one the
// proxy accepts lives in internal/parser/planparserv2 (it needs both modules).
// What is left here is Build's own contract: envelope layout, the signed
// two's-complement mapping, container-shape admission, and the decoded bytes.

// decodeBody deserializes the portable Roaring64 body of a built blob. Tests
// assert against the decoded bitmap rather than a validator in this package, so
// they cannot pass by agreeing with a local copy of the format.
func decodeBody(t *testing.T, blob []byte) *roaring64.Bitmap {
	t.Helper()
	bitmap := roaring64.New()
	consumed, err := bitmap.ReadFrom(bytes.NewReader(blob[HeaderSize:]))
	require.NoError(t, err)
	require.Equal(t, int64(len(blob)-HeaderSize), consumed,
		"body must be consumed exactly")
	return bitmap
}

func contains(bitmap *roaring64.Bitmap, v int64) bool {
	return bitmap.Contains(uint64(v))
}

func TestBuildRoundTrip(t *testing.T) {
	members := []int64{math.MinInt64, -1, 0, 1, 42, math.MaxInt64, 42}

	blob, err := Build(members)
	require.NoError(t, err)
	require.Equal(t, Magic, string(blob[:4]))
	require.Equal(t, Version, binary.LittleEndian.Uint16(blob[4:6]))
	require.Equal(t, FormatPortableRoaring64, binary.LittleEndian.Uint16(blob[6:8]))
	require.Equal(t, uint64(6), binary.LittleEndian.Uint64(blob[8:16]))
	require.Equal(t, uint64(len(blob)-HeaderSize), binary.LittleEndian.Uint64(blob[16:24]))
	require.Zero(t, binary.LittleEndian.Uint64(blob[24:32]))

	bitmap := decodeBody(t, blob)
	require.Equal(t, uint64(6), bitmap.GetCardinality())
	for _, member := range members {
		require.True(t, contains(bitmap, member), "member %d must be present", member)
	}
	require.False(t, contains(bitmap, 2))
}

// TestBuildPreservesSignedBitPattern pins the normative mapping: sign-extend to
// int64, then keep the two's-complement bits as the uint64 key. Zero-extending a
// narrow negative value instead would place it in a different high container
// than the one segcore probes.
func TestBuildPreservesSignedBitPattern(t *testing.T) {
	cases := []struct {
		member int64
		key    uint64
	}{
		{-1, 0xffffffffffffffff},
		{-128, 0xffffffffffffff80},
		{-32768, 0xffffffffffff8000},
		{math.MinInt32, 0xffffffff80000000},
		{math.MinInt64, 0x8000000000000000},
		{42, 0x000000000000002a},
		{math.MaxInt64, 0x7fffffffffffffff},
	}
	members := make([]int64, 0, len(cases))
	for _, c := range cases {
		members = append(members, c.member)
	}

	bitmap := decodeBody(t, mustBuild(t, members))
	require.Equal(t, uint64(len(cases)), bitmap.GetCardinality())
	for _, c := range cases {
		require.Truef(t, bitmap.Contains(c.key),
			"member %d must map to key %#016x", c.member, c.key)
	}
}

func TestBuildEmptySet(t *testing.T) {
	blob, err := Build(nil)
	require.NoError(t, err)
	require.Len(t, blob, HeaderSize+int(binary.LittleEndian.Uint64(blob[16:24])))
	require.Zero(t, binary.LittleEndian.Uint64(blob[8:16]))

	bitmap := decodeBody(t, blob)
	require.Zero(t, bitmap.GetCardinality())
	require.False(t, contains(bitmap, 0))
}

func TestBuildRejectsSparseHighContainersBeforeBitmapConstruction(t *testing.T) {
	members := make([]int64, MaxHighContainerCount+1)
	for i := range members {
		members[i] = int64(uint64(i) << 32)
	}
	_, err := Build(members)
	require.ErrorContains(t, err, "high-container count")
}

// TestDecodedEstimateFormula pins the arithmetic, not just the limit. The same
// formula exists in pkg/util/roaringfilter and in segcore, and the three cannot
// share code across the module and language boundaries; the constants are pinned
// to each other by TestClientBuiltBlobsPassProxyValidation and
// TestRoaringSegcoreConstantsMatch, so pinning the expression here against those same
// constants is what keeps this copy from drifting.
//
// A drift costs a round trip rather than correctness -- the proxy still enforces
// -- but it means the SDK refusing a blob the cluster would have taken, which is
// invisible to every other test: the rejection tests use inputs far enough over
// the ceiling that a wrong coefficient still rejects.
func TestDecodedEstimateFormula(t *testing.T) {
	for _, c := range []struct{ body, high, low uint64 }{
		{0, 0, 0},
		{1024, 1, 3},
		{1 << 20, 300, 5000},
	} {
		got, err := estimateAndCheckDecodedBytes(c.body, c.high, c.low)
		require.NoError(t, err)
		require.Equal(t,
			c.body+c.high*EstimatedHighContainerOverheadBytes+c.low*EstimatedLowContainerOverheadBytes,
			got, "body=%d high=%d low=%d", c.body, c.high, c.low)
	}
}

// TestBuildRejectsDecodedEstimateBeforeBitmapConstruction covers the sibling of
// the guard above: a set can sit well inside MaxHighContainerCount and still be
// too expensive to decode, because the estimate charges per *low* container too.
// Spreading one value per 2^16 block does that -- 1.1M low containers cost
// ~70 MiB of estimate against a 64 MiB ceiling while occupying only 17 high
// containers -- and Build must reject it from the counts alone, before it builds
// a bitmap. The proxy enforces the same ceiling; failing locally saves the round
// trip.
func TestBuildRejectsDecodedEstimateBeforeBitmapConstruction(t *testing.T) {
	const lowContainers = 1_100_000
	members := make([]int64, lowContainers)
	for i := range members {
		members[i] = int64(i) << 16
	}
	require.Less(t, uint64(lowContainers>>16)+1, uint64(MaxHighContainerCount),
		"the set must stay inside the high-container limit, so this exercises the "+
			"decoded-estimate branch and not its sibling")

	_, err := Build(members)
	require.ErrorContains(t, err, "estimated decoded size")
}

func TestBuildPortableContainerEncodings(t *testing.T) {
	members := make([]int64, 0, 5200)
	// Consecutive values become a run container after RunOptimize.
	for value := int64(0); value < 1000; value++ {
		members = append(members, value)
	}
	// More than 4096 non-consecutive values in another Roaring32 key become
	// a bitmap container. A second high-32 key exercises Roaring64 ordering.
	for value := int64(0); value < 4097; value++ {
		members = append(members, (1<<16)+value*2)
	}
	// Raise the Roaring32 container count above the offset threshold while
	// retaining a run container, so the run-cookie offset table is emitted.
	for key := int64(2); key <= 4; key++ {
		members = append(members, (key<<16)+1)
	}
	members = append(members, (1<<32)+7)
	// The size is part of the fixture. One value fewer in the dense block --
	// 4096 rather than 4097 -- encodes as an array container rather than the
	// bitmap this set exists to reach, and pkg/util/roaringfilter, which owns
	// the classifier that can see the difference, keeps its own copy of the set.
	// This catches an edit to this copy before the two drift.
	require.Equal(t, 5101, len(members))

	blob := mustBuild(t, members)
	bitmap := decodeBody(t, blob)
	require.Equal(t, uint64(len(members)), bitmap.GetCardinality())
	for _, value := range []int64{0, 999, 1 << 16, (1 << 16) + 8192, (4 << 16) + 1, (1 << 32) + 7} {
		require.True(t, contains(bitmap, value), "member %d must be present", value)
	}
}

func mustBuild(t *testing.T, members []int64) []byte {
	t.Helper()
	blob, err := Build(members)
	require.NoError(t, err)
	return blob
}
