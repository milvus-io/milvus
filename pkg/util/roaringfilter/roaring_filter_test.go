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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type portableHighContainer struct {
	high  uint32
	child []byte
}

func portableChild(t *testing.T, lows ...uint32) []byte {
	t.Helper()
	bitmap := roaring.New()
	bitmap.AddMany(lows)
	body, err := bitmap.ToBytes()
	require.NoError(t, err)
	return body
}

func portableBody(containers ...portableHighContainer) []byte {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, uint64(len(containers)))
	for _, container := range containers {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, container.high)
		body = append(body, key...)
		body = append(body, container.child...)
	}
	return body
}

func portableRunChild(runCount uint16) []byte {
	// Portable Roaring32 with one run container:
	// cookie(size=1), run bitmap, key/cardinality descriptor, then the runs.
	child := make([]byte, 4+1+4+2+int(runCount)*4)
	binary.LittleEndian.PutUint16(child[0:2], 12347)
	binary.LittleEndian.PutUint16(child[2:4], 0) // one container minus one
	child[4] = 1                                 // container 0 is a run container
	binary.LittleEndian.PutUint16(child[5:7], 0)
	binary.LittleEndian.PutUint16(child[7:9], runCount-1)
	binary.LittleEndian.PutUint16(child[9:11], runCount)
	for i := uint16(0); i < runCount; i++ {
		offset := 11 + int(i)*4
		binary.LittleEndian.PutUint16(child[offset:offset+2], i*2)
		binary.LittleEndian.PutUint16(child[offset+2:offset+4], 0)
	}
	return child
}

func portableSingleRunChild(start, length uint16) []byte {
	child := make([]byte, 15)
	binary.LittleEndian.PutUint16(child[0:2], 12347)
	binary.LittleEndian.PutUint16(child[2:4], 0)
	child[4] = 1
	binary.LittleEndian.PutUint16(child[5:7], 0)
	binary.LittleEndian.PutUint16(child[7:9], length) // cardinality - 1
	binary.LittleEndian.PutUint16(child[9:11], 1)
	binary.LittleEndian.PutUint16(child[11:13], start)
	binary.LittleEndian.PutUint16(child[13:15], length)
	return child
}

// portableTwoRunChild encodes two runs in one container without coalescing
// them, which is what a conforming writer that does not merge adjacent runs
// produces.
func portableTwoRunChild(runs [2][2]uint16) []byte {
	child := make([]byte, 4+1+4+2+2*4)
	binary.LittleEndian.PutUint16(child[0:2], 12347) // run cookie
	binary.LittleEndian.PutUint16(child[2:4], 0)     // one container
	child[4] = 1                                     // container 0 is a run container
	binary.LittleEndian.PutUint16(child[5:7], 0)     // key
	var cardinality uint16
	for _, run := range runs {
		cardinality += run[1] + 1
	}
	binary.LittleEndian.PutUint16(child[7:9], cardinality-1)
	binary.LittleEndian.PutUint16(child[9:11], 2) // run count
	for i, run := range runs {
		offset := 11 + i*4
		binary.LittleEndian.PutUint16(child[offset:offset+2], run[0])
		binary.LittleEndian.PutUint16(child[offset+2:offset+4], run[1])
	}
	return child
}

func mrb1Blob(body []byte, cardinality uint64) []byte {
	blob := make([]byte, HeaderSize+len(body))
	copy(blob[:4], Magic)
	binary.LittleEndian.PutUint16(blob[4:6], Version)
	binary.LittleEndian.PutUint16(blob[6:8], FormatPortableRoaring64)
	binary.LittleEndian.PutUint64(blob[8:16], cardinality)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(body)))
	copy(blob[HeaderSize:], body)
	return blob
}

func compactHighContainerBody(count uint64) []byte {
	child := make([]byte, 11)
	binary.LittleEndian.PutUint16(child[0:2], portableCookieRun)
	child[4] = 0
	body := make([]byte, 8, 8+int(count)*15)
	binary.LittleEndian.PutUint64(body, count)
	var key [4]byte
	for i := uint64(0); i < count; i++ {
		binary.LittleEndian.PutUint32(key[:], uint32(i))
		body = append(body, key[:]...)
		body = append(body, child...)
	}
	return body
}

func manySingletonLowContainersChild(count uint32) []byte {
	child := make([]byte, 8+int(count)*10)
	binary.LittleEndian.PutUint32(child[0:4], portableCookieNoRun)
	binary.LittleEndian.PutUint32(child[4:8], count)
	descriptorStart := 8
	offsetStart := descriptorStart + int(count)*4
	payloadStart := offsetStart + int(count)*4
	for i := uint32(0); i < count; i++ {
		descriptor := descriptorStart + int(i)*4
		binary.LittleEndian.PutUint16(child[descriptor:descriptor+2], uint16(i))
		offset := payloadStart + int(i)*2
		binary.LittleEndian.PutUint32(child[offsetStart+int(i)*4:], uint32(offset))
		binary.LittleEndian.PutUint16(child[offset:offset+2], 0)
	}
	return child
}

func TestValidateRoundTrip(t *testing.T) {
	members := []int64{math.MinInt64, -1, 0, 1, 42, math.MaxInt64, 42}

	blob, err := buildFixture(members)
	require.NoError(t, err)
	require.Equal(t, Magic, string(blob[:4]))
	require.Equal(t, Version, binary.LittleEndian.Uint16(blob[4:6]))
	require.Equal(t, FormatPortableRoaring64, binary.LittleEndian.Uint16(blob[6:8]))
	require.Equal(t, uint64(6), binary.LittleEndian.Uint64(blob[8:16]))
	require.Equal(t, uint64(len(blob)-HeaderSize), binary.LittleEndian.Uint64(blob[16:24]))
	require.Zero(t, binary.LittleEndian.Uint64(blob[24:32]))

	filter, err := parseFixture(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(6), filter.Cardinality())
	for _, member := range members {
		require.True(t, filter.ContainsInt64(member), "member %d must be present", member)
	}
	require.False(t, filter.ContainsInt64(2))
}

func TestValidateEmptySet(t *testing.T) {
	blob, err := buildFixture(nil)
	require.NoError(t, err)

	filter, err := parseFixture(blob)
	require.NoError(t, err)
	require.Zero(t, filter.Cardinality())
	require.False(t, filter.ContainsInt64(0))
}

func TestValidateIsAllocationFreeAndReportsResourceShape(t *testing.T) {
	body := compactHighContainerBody(20_000)
	blob := mrb1Blob(body, 20_000)

	var (
		summary ValidationSummary
		err     error
	)
	allocs := testing.AllocsPerRun(10, func() {
		summary, err = Validate(blob)
	})
	require.NoError(t, err)
	require.Zero(t, allocs)
	require.Equal(t, uint64(20_000), summary.Cardinality)
	require.Equal(t, uint64(20_000), summary.HighContainerCount)
	require.Equal(t, uint64(20_000), summary.LowContainerCount)
	require.Equal(t, uint64(len(body)), summary.BodyBytes)
	require.Equal(t,
		uint64(len(body))+20_000*EstimatedHighContainerOverheadBytes+
			20_000*EstimatedLowContainerOverheadBytes,
		summary.EstimatedDecodedBytes)
}

func TestValidateRejectsDecodedResourceAmplification(t *testing.T) {
	t.Run("high containers", func(t *testing.T) {
		body := compactHighContainerBody(MaxHighContainerCount + 1)
		_, err := Validate(mrb1Blob(body, MaxHighContainerCount+1))
		require.ErrorContains(t, err, "high-container count")
	})

	t.Run("low container decoded estimate", func(t *testing.T) {
		child := manySingletonLowContainersChild(1 << 16)
		containers := make([]portableHighContainer, 16)
		for i := range containers {
			containers[i] = portableHighContainer{high: uint32(i), child: child}
		}
		body := portableBody(containers...)
		_, err := Validate(mrb1Blob(body, 16*(1<<16)))
		require.ErrorContains(t, err, "estimated decoded size")
	})
}

// portableContainerMix classifies the containers a portable Roaring64 body
// encodes, so a test can assert which validator branches a member set reaches
// instead of assuming. It walks the body with the same cookie rules the
// validator uses.
type portableContainerMix struct {
	highContainers int
	arrays         int
	bitmaps        int
	runs           int
	// offsetTables counts every child carrying an offset table;
	// runCookieOffsetTables counts only the run-cookie children that do. The
	// second is the interesting one: the no-run format always carries offsets,
	// so the conditional branch in validatePortableRoaring32 is reached only
	// when a run-cookie child has at least portableNoOffsetThreshold containers.
	offsetTables          int
	runCookieOffsetTables int
}

func classifyPortableBody(t *testing.T, body []byte) portableContainerMix {
	t.Helper()
	var mix portableContainerMix
	high := binary.LittleEndian.Uint64(body[:portableRoaring64PrefixBytes])
	mix.highContainers = int(high)
	cursor := portableRoaring64PrefixBytes
	for i := uint64(0); i < high; i++ {
		cursor += 4 // high key
		child := body[cursor:]
		cookie := binary.LittleEndian.Uint32(child[:4])

		var containerCount, descriptors int
		if uint16(cookie) == portableCookieRun {
			containerCount = int(cookie>>16) + 1
			descriptors = 4 + (containerCount+7)/8
			if containerCount >= portableNoOffsetThreshold {
				mix.offsetTables++
				mix.runCookieOffsetTables++
			}
		} else {
			require.Equal(t, portableCookieNoRun, cookie, "unknown Roaring32 cookie")
			containerCount = int(binary.LittleEndian.Uint32(child[4:8]))
			descriptors = 8
			mix.offsetTables++ // the no-run format always carries offsets
		}

		isRun := func(idx int) bool {
			return uint16(cookie) == portableCookieRun && child[4+idx/8]&(1<<(idx%8)) != 0
		}
		for c := 0; c < containerCount; c++ {
			cardinality := int(binary.LittleEndian.Uint16(child[descriptors+c*4+2:])) + 1
			switch {
			case isRun(c):
				mix.runs++
			case cardinality > portableArrayMaxCardinality:
				mix.bitmaps++
			default:
				mix.arrays++
			}
		}

		// The validator reports how many bytes the child occupies; reusing that
		// is not an independent check of the walk, only of the classification
		// above it, which is what this helper is for.
		consumed, _, validatedContainers, err := validatePortableRoaring32(child)
		require.NoError(t, err)
		// Both sides read the container count the same way, so this is a typo
		// guard rather than an independent check; the classification above it is
		// the part that is genuinely independent.
		require.Equalf(t, uint64(containerCount), validatedContainers,
			"the classifier and the validator disagree on child %d's container count", i)
		cursor += consumed
	}
	// The cursor advances by the validator's own `consumed`, so this cannot catch
	// a classifier error -- it catches a body the validator would not have
	// accepted whole.
	require.Equal(t, len(body), cursor, "the body must be walked exactly")
	return mix
}

// TestPortableContainerEncodingsReachEveryBranch pins what the shared
// container-encodings member set encodes to. Two validator branches -- the
// bitmap container and the run-cookie offset table -- are reachable only with a
// set shaped like this one, and TestClientBuiltBlobsPassProxyValidation
// (internal/parser/planparserv2) carries the same set so those branches also see
// bytes the real SDK builder produced. Nothing pinned that, so a change to a
// roaring threshold or to the member set could quietly stop exercising them.
func TestPortableContainerEncodingsReachEveryBranch(t *testing.T) {
	members := containerEncodingMembers()
	// The size is part of the fixture. One value fewer in the dense block --
	// 4096 rather than 4097 -- still gives two high containers and six low ones
	// and a body of the same length, but encodes as an array container rather
	// than the bitmap this set exists to reach. The mix assertions below are
	// what catch that; this is the cheaper signal that the set was edited at
	// all. 1000 + 4097 + 3 + 1, all distinct.
	require.Equal(t, 5101, len(members))

	blob, err := buildFixture(members)
	require.NoError(t, err)

	mix := classifyPortableBody(t, blob[HeaderSize:])
	require.Equal(t, 2, mix.highContainers)
	require.GreaterOrEqual(t, mix.bitmaps, 1,
		"the 4097 even values in one 2^16 block must encode as a bitmap container")
	require.GreaterOrEqual(t, mix.runs, 1,
		"the 1000 consecutive values must encode as a run container after RunOptimize")
	require.GreaterOrEqual(t, mix.arrays, 1,
		"the singleton keys must encode as array containers")
	require.Equal(t, mix.highContainers, mix.offsetTables,
		"every child here carries an offset table: the no-run one unconditionally, "+
			"the run-cookie one because it is over the threshold")

	require.GreaterOrEqual(t, mix.runCookieOffsetTables, 1,
		"a run-cookie child must carry an offset table, which needs at least "+
			"portableNoOffsetThreshold containers -- asserting on offsetTables "+
			"instead would pass on the no-run child, which always carries them")
}

func TestValidatePortableContainerEncodings(t *testing.T) {
	members := containerEncodingMembers()

	blob, err := buildFixture(members)
	require.NoError(t, err)
	filter, err := parseFixture(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(len(members)), filter.Cardinality())
	for _, value := range []int64{0, 999, 1 << 16, (1 << 16) + 8192, (4 << 16) + 1, (1 << 32) + 7} {
		require.True(t, filter.ContainsInt64(value), "member %d must be present", value)
	}
}

// containerEncodingMembers is the one member set that exercises every portable
// container encoding: a run container, a bitmap container, and enough Roaring32
// containers to force an offset table behind a run cookie.
func containerEncodingMembers() []int64 {
	members := make([]int64, 0, 5200)
	// Consecutive values become a run container after RunOptimize.
	for value := int64(0); value < 1000; value++ {
		members = append(members, value)
	}
	// More than 4096 non-consecutive values in one Roaring32 key become a
	// bitmap container. A second high-32 key exercises Roaring64 ordering.
	for value := int64(0); value < 4097; value++ {
		members = append(members, (1<<16)+value*2)
	}
	// Raise the Roaring32 container count above the offset threshold while
	// retaining a run container, so the run-cookie offset table is emitted.
	for key := int64(2); key <= 4; key++ {
		members = append(members, (key<<16)+1)
	}
	return append(members, (1<<32)+7)
}

func TestValidateAcceptsRunCookieWithSingleValueArrayContainer(t *testing.T) {
	// Portable Roaring32 with one non-run array container under SERIAL_COOKIE:
	// 4-byte cookie, 1-byte all-zero run bitmap, 4-byte descriptor, 2-byte value.
	child := make([]byte, 11)
	binary.LittleEndian.PutUint16(child[0:2], 12347)
	binary.LittleEndian.PutUint16(child[2:4], 0) // one container minus one
	child[4] = 0                                 // container 0 is not a run container
	binary.LittleEndian.PutUint16(child[5:7], 0)
	binary.LittleEndian.PutUint16(child[7:9], 0) // cardinality minus one
	binary.LittleEndian.PutUint16(child[9:11], 42)

	body := portableBody(portableHighContainer{high: 0, child: child})
	require.Len(t, body, 23)
	filter, err := parseFixture(mrb1Blob(body, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), filter.Cardinality())
	require.True(t, filter.ContainsInt64(42))
}

func TestValidateRejectsMalformedEnvelope(t *testing.T) {
	valid, err := buildFixture([]int64{-1, 0, 1})
	require.NoError(t, err)

	tests := map[string]func([]byte) []byte{
		"too short": func([]byte) []byte { return []byte("MRB1") },
		"wrong magic": func(blob []byte) []byte {
			blob[0] = 'X'
			return blob
		},
		"wrong version": func(blob []byte) []byte {
			binary.LittleEndian.PutUint16(blob[4:6], Version+1)
			return blob
		},
		"wrong format": func(blob []byte) []byte {
			binary.LittleEndian.PutUint16(blob[6:8], FormatPortableRoaring64+1)
			return blob
		},
		"wrong body length": func(blob []byte) []byte {
			binary.LittleEndian.PutUint64(blob[16:24], uint64(len(blob)))
			return blob
		},
		"reserved nonzero": func(blob []byte) []byte {
			binary.LittleEndian.PutUint64(blob[24:32], 1)
			return blob
		},
		"truncated body": func(blob []byte) []byte { return blob[:len(blob)-1] },
		"trailing bytes": func(blob []byte) []byte { return append(blob, 0) },
		"portable body trailing bytes": func(blob []byte) []byte {
			blob = append(blob, 0)
			binary.LittleEndian.PutUint64(blob[16:24], uint64(len(blob)-HeaderSize))
			return blob
		},
		"cardinality mismatch": func(blob []byte) []byte {
			binary.LittleEndian.PutUint64(blob[8:16], 4)
			return blob
		},
		"body exceeds maximum": func(blob []byte) []byte {
			binary.LittleEndian.PutUint64(blob[16:24], MaxBodyBytes+1)
			return blob
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := append([]byte(nil), valid...)
			_, err := parseFixture(mutate(candidate))
			require.Error(t, err)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestValidateRejectsImpossibleHighContainerCountWithoutPanic(t *testing.T) {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, ^uint64(0))
	blob := mrb1Blob(body, 0)

	var err error
	require.NotPanics(t, func() {
		_, err = parseFixture(blob)
	})
	require.ErrorContains(t, err, "high-container count")
}

func TestValidateRejectsStructurallyInvalidPortableBody(t *testing.T) {
	first := portableChild(t, 11)
	second := portableChild(t, 22)

	invalidBitmapChild := roaring.New()
	for value := uint32(0); value <= 4096; value++ {
		invalidBitmapChild.Add(value)
	}
	invalidChild, err := invalidBitmapChild.ToBytes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(invalidChild), 24)
	// A one-container, non-run portable bitmap starts its bitmap payload at
	// byte 16. Clearing a set bit preserves the declared cardinality while
	// making the decoded child structurally inconsistent.
	invalidChild = append([]byte(nil), invalidChild...)
	invalidChild[16] &^= 1

	tests := map[string]struct {
		body        []byte
		cardinality uint64
	}{
		"duplicate high keys": {
			body: portableBody(
				portableHighContainer{high: 1, child: first},
				portableHighContainer{high: 1, child: second},
			),
			cardinality: 2,
		},
		"out-of-order high keys": {
			body: portableBody(
				portableHighContainer{high: 2, child: first},
				portableHighContainer{high: 1, child: second},
			),
			cardinality: 2,
		},
		"invalid child container": {
			body: portableBody(
				portableHighContainer{high: 1, child: invalidChild},
			),
			cardinality: 4097,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := parseFixture(mrb1Blob(test.body, test.cardinality))
			require.ErrorContains(t, err, "invalid portable roaring64 body")
		})
	}
}

func TestPortableValidationErrorsRedactMemberKeyPrefixes(t *testing.T) {
	first := portableChild(t, 11)
	second := portableChild(t, 22)
	const (
		firstHigh   = uint32(4_000_000_001)
		secondHigh  = uint32(3_000_000_007)
		firstChild  = uint16(65_000)
		secondChild = uint16(64_000)
	)

	_, highErr := scanPortableRoaring64Body(portableBody(
		portableHighContainer{high: firstHigh, child: first},
		portableHighContainer{high: secondHigh, child: second},
	))
	require.Error(t, highErr)
	require.NotContains(t, highErr.Error(), fmt.Sprint(firstHigh))
	require.NotContains(t, highErr.Error(), fmt.Sprint(secondHigh))
	require.Contains(t, highErr.Error(), "high container 1")

	child := make([]byte, 16)
	binary.LittleEndian.PutUint32(child[0:4], portableCookieNoRun)
	binary.LittleEndian.PutUint32(child[4:8], 2)
	binary.LittleEndian.PutUint16(child[8:10], firstChild)
	binary.LittleEndian.PutUint16(child[10:12], 0)
	binary.LittleEndian.PutUint16(child[12:14], secondChild)
	binary.LittleEndian.PutUint16(child[14:16], 0)
	_, childErr := scanPortableRoaring64Body(portableBody(
		portableHighContainer{high: 0, child: child},
	))
	require.Error(t, childErr)
	require.NotContains(t, childErr.Error(), fmt.Sprint(firstChild))
	require.NotContains(t, childErr.Error(), fmt.Sprint(secondChild))
	require.Contains(t, childErr.Error(), "Roaring32 container 1")
}

func TestValidateHandlesHighRunCountLinearly(t *testing.T) {
	body := portableBody(portableHighContainer{
		high:  0,
		child: portableRunChild(32768),
	})
	_, err := scanPortableRoaring64Body(body)
	require.NoError(t, err)
	filter, err := parseFixture(mrb1Blob(body, 32768))
	require.NoError(t, err)
	require.Equal(t, uint64(32768), filter.Cardinality())
	require.True(t, filter.ContainsInt64(0))
	require.True(t, filter.ContainsInt64(65534))
	require.False(t, filter.ContainsInt64(1))
}

func TestValidateAcceptsPortableRunContainersProducedByOtherImplementations(t *testing.T) {
	// Java can retain two consecutive values as a run after mutating a larger
	// RunContainer; Java and CRoaring also retain the three-value size tie.
	for _, cardinality := range []uint64{2, 3} {
		t.Run(fmt.Sprintf("cardinality_%d", cardinality), func(t *testing.T) {
			body := portableBody(portableHighContainer{
				high:  0,
				child: portableSingleRunChild(0, uint16(cardinality-1)),
			})
			filter, err := parseFixture(mrb1Blob(body, cardinality))
			require.NoError(t, err)
			for value := int64(0); value < int64(cardinality); value++ {
				require.True(t, filter.ContainsInt64(value))
			}
		})
	}
}

func TestValidateAcceptsAdjacentRunIntervals(t *testing.T) {
	// The portable format requires runs to be sorted and non-overlapping, NOT
	// merged. (start 0, len 0) followed by (start 1, len 0) is a legal encoding
	// of {0, 1}; roaring/v2 and CRoaring both decode it that way. Rejecting it
	// would refuse blobs from any conforming writer that does not coalesce.
	body := portableBody(portableHighContainer{
		high:  0,
		child: portableTwoRunChild([2][2]uint16{{0, 0}, {1, 0}}),
	})

	summary, err := Validate(mrb1Blob(body, 2))
	require.NoError(t, err)
	require.Equal(t, uint64(2), summary.Cardinality)

	filter, err := parseFixture(mrb1Blob(body, 2))
	require.NoError(t, err)
	require.True(t, filter.ContainsInt64(0))
	require.True(t, filter.ContainsInt64(1))
	require.False(t, filter.ContainsInt64(2))
}

func TestValidateStillRejectsOverlappingAndDescendingRuns(t *testing.T) {
	// Relaxing the adjacency check must not relax these.
	//
	// The declared envelope cardinality must match what the run descriptor
	// sums to, or validation would fail on the cardinality mismatch instead
	// and the case would stay green even if the overlap check regressed. The
	// error message is asserted for the same reason.
	for _, tc := range []struct {
		name        string
		runs        [2][2]uint16
		cardinality uint64
	}{
		// 0..2 then 2..2: the descriptor sums to 4, double-counting value 2.
		{"overlapping", [2][2]uint16{{0, 2}, {2, 0}}, 4},
		{"descending", [2][2]uint16{{10, 0}, {1, 0}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := portableBody(portableHighContainer{
				high:  0,
				child: portableTwoRunChild(tc.runs),
			})
			_, err := Validate(mrb1Blob(body, tc.cardinality))
			require.Error(t, err)
			require.Contains(t, err.Error(), "run intervals overlap or are out of order")
		})
	}
}

// croaringFixtures are MRB1 blobs written by CRoaring's own portable writer
// (roaring::Roaring64Map::write) in internal/core, not by this package. They
// close the other half of the cross-language contract: RoaringMembershipTest in
// internal/core parses Go-written blobs for the same shapes, and these prove the
// reverse direction over every container shape the portable format can produce.
// Keep the two sets in step -- adding a shape here means adding it there.
//
// Regenerate by building a Roaring64Map from the listed values in C++,
// runOptimize, write portable, and wrap in the MRB1 envelope.
var croaringFixtures = []struct {
	name        string
	description string
	cardinality uint64
	hex         string
}{
	{
		name:        "empty",
		description: "empty membership",
		cardinality: 0,
		hex: "4d52423101000100000000000000000008000000000000000000000000000000" +
			"0000000000000000",
	},
	{
		name:        "signed_boundaries",
		description: "INT64_MIN, -1, 0, 1, 42, INT64_MAX",
		cardinality: 6,
		hex: "4d52423101000100060000000000000064000000000000000000000000000000" +
			"0400000000000000000000003a30000001000000000002001000000000000100" +
			"2a00ffffff7f3a30000001000000ffff000010000000ffff000000803a300000" +
			"0100000000000000100000000000ffffffff3a30000001000000ffff00001000" +
			"0000ffff",
	},
	{
		name:        "array_container",
		description: "an array container",
		cardinality: 4,
		hex: "4d52423101000100040000000000000024000000000000000000000000000000" +
			"0100000000000000000000003a30000001000000000003001000000001000500" +
			"09009210",
	},
	{
		name:        "bitmap_container",
		description: "a bitmap container (5000 even values)",
		cardinality: 5000,
		hex: "4d5242310100010088130000000000001c200000000000000000000000000000" +
			"0100000000000000000000003a30000001000000000087131000000055555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555555555" +
			"5555555555555555555555555555555555555555555555555555555555550000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"00000000000000000000000000000000000000000000000000000000",
	},
	{
		name:        "run_container",
		description: "a run container (300 contiguous values after runOptimize)",
		cardinality: 300,
		hex: "4d524231010001002c010000000000001b000000000000000000000000000000" +
			"0100000000000000000000003b3000000100002b01010000002b01",
	},
	{
		name:        "multi_high_container",
		description: "four high-32 containers",
		cardinality: 5,
		hex: "4d52423101000100050000000000000062000000000000000000000000000000" +
			"0400000000000000000000003a30000001000000000001001000000000000100" +
			"010000003a3000000100000000000000100000000700020000003a3000000100" +
			"000000000000100000000900030000003a300000010000000000000010000000" +
			"0b00",
	},
}

func TestValidateAcceptsCRoaringGeneratedFixtures(t *testing.T) {
	// Membership checks, not just cardinality: two implementations can agree on
	// how many values a body holds while disagreeing on which ones, and the
	// point of a cross-language fixture is that they agree on the values.
	present := map[string][]int64{
		"empty":                {},
		"signed_boundaries":    {math.MinInt64, -1, 0, 1, 42, math.MaxInt64},
		"array_container":      {1, 5, 9, 4242},
		"bitmap_container":     {0, 2, 4998, 9998},
		"run_container":        {0, 1, 150, 299},
		"multi_high_container": {0, 1, int64(1)<<32 + 7, int64(2)<<32 + 9, int64(3)<<32 + 11},
	}
	absent := map[string][]int64{
		"empty":                {0, -1, 1},
		"signed_boundaries":    {2, -2, 43},
		"array_container":      {0, 2, 4241, 4243},
		"bitmap_container":     {1, 3, 9999},
		"run_container":        {-1, 300},
		"multi_high_container": {2, int64(1)<<32 + 8, int64(4) << 32},
	}

	for _, fixture := range croaringFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			blob, err := hex.DecodeString(fixture.hex)
			require.NoError(t, err)

			summary, err := Validate(blob)
			require.NoError(t, err, fixture.description)
			require.Equal(t, fixture.cardinality, summary.Cardinality)

			filter, err := parseFixture(blob)
			require.NoError(t, err, fixture.description)
			require.Equal(t, fixture.cardinality, filter.Cardinality())

			members, ok := present[fixture.name]
			require.True(t, ok, "fixture %q has no membership expectations", fixture.name)
			for _, value := range members {
				require.True(t, filter.ContainsInt64(value), value)
			}
			for _, value := range absent[fixture.name] {
				require.False(t, filter.ContainsInt64(value), value)
			}
		})
	}
}

func TestCRoaringSignedBoundaryFixtureRoundTripsValues(t *testing.T) {
	// The signed mapping is the one place where a divergence between the two
	// implementations would be silent: a zero-extended narrow value or a
	// ZigZag encoding still produces a structurally valid body.
	var fixture string
	for _, candidate := range croaringFixtures {
		if candidate.name == "signed_boundaries" {
			fixture = candidate.hex
		}
	}
	require.NotEmpty(t, fixture)

	blob, err := hex.DecodeString(fixture)
	require.NoError(t, err)
	filter, err := parseFixture(blob)
	require.NoError(t, err)

	for _, value := range []int64{math.MinInt64, -1, 0, 1, 42, math.MaxInt64} {
		require.True(t, filter.ContainsInt64(value), value)
	}
	require.False(t, filter.ContainsInt64(2))
	require.False(t, filter.ContainsInt64(-2))
}

func TestValidateAcceptsEmptyRoaring32Child(t *testing.T) {
	// CRoaring 3.0.0 can write a high entry whose Roaring32 child holds zero
	// containers: a 4-byte high key followed by the no-run cookie and a
	// container count of zero. Both CRoaring and roaring/v2 consume the
	// resulting 20-byte body as an empty set, so rejecting it would refuse a
	// blob produced by the reference writer.
	body := make([]byte, 0, 20)
	body = binary.LittleEndian.AppendUint64(body, 1) // one high container
	body = binary.LittleEndian.AppendUint32(body, 0) // high key 0
	body = binary.LittleEndian.AppendUint32(body, portableCookieNoRun)
	body = binary.LittleEndian.AppendUint32(body, 0) // zero containers
	require.Len(t, body, 20)

	summary, err := Validate(mrb1Blob(body, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(0), summary.Cardinality)
	require.Equal(t, uint64(1), summary.HighContainerCount)

	filter, err := parseFixture(mrb1Blob(body, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(0), filter.Cardinality())
	require.False(t, filter.ContainsInt64(0))
}

func TestValidateAcceptsUnspecifiedRunBitmapPaddingBits(t *testing.T) {
	// The run bitmap is (containerCount + 7) / 8 bytes and only its first
	// containerCount bits are defined; the rest is unspecified padding.
	// 0x81 sets bit 0 (the single container is a run container) and bit 7,
	// which is padding. roaring/v2 and CRoaring both ignore it, so rejecting
	// the body would refuse output those writers can produce.
	//
	// Portable Roaring32: 4-byte run cookie, 1-byte run bitmap, 4-byte
	// descriptor, 2-byte run count, then one (start, length) pair.
	child := make([]byte, 0, 13)
	child = binary.LittleEndian.AppendUint16(child, portableCookieRun)
	child = binary.LittleEndian.AppendUint16(child, 0) // one container minus one
	child = append(child, 0x81)                        // run bit + padding bit
	child = binary.LittleEndian.AppendUint16(child, 0) // key
	child = binary.LittleEndian.AppendUint16(child, 0) // cardinality minus one
	child = binary.LittleEndian.AppendUint16(child, 1) // one run
	child = binary.LittleEndian.AppendUint16(child, 7) // start 7
	child = binary.LittleEndian.AppendUint16(child, 0) // length 0 -> {7}

	body := portableBody(portableHighContainer{high: 0, child: child})

	summary, err := Validate(mrb1Blob(body, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), summary.Cardinality)

	filter, err := parseFixture(mrb1Blob(body, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), filter.Cardinality())
	require.True(t, filter.ContainsInt64(7))
	require.False(t, filter.ContainsInt64(6))
	require.False(t, filter.ContainsInt64(8))
}

func TestValidateIgnoresOffsetTableContents(t *testing.T) {
	t.Run("wrong offset table", func(t *testing.T) {
		// Four containers force the offset table to be present, and every
		// declared offset is zero. roaring/v2 skips the table entirely.
		child := make([]byte, 0)
		child = binary.LittleEndian.AppendUint32(child, portableCookieNoRun)
		child = binary.LittleEndian.AppendUint32(child, 4)
		for key := uint16(0); key < 4; key++ {
			child = binary.LittleEndian.AppendUint16(child, key)
			child = binary.LittleEndian.AppendUint16(child, 0) // cardinality - 1
		}
		for i := 0; i < 4; i++ {
			child = binary.LittleEndian.AppendUint32(child, 0) // bogus offset
		}
		for value := uint16(0); value < 4; value++ {
			child = binary.LittleEndian.AppendUint16(child, value+10)
		}

		body := portableBody(portableHighContainer{high: 0, child: child})
		filter, err := parseFixture(mrb1Blob(body, 4))
		require.NoError(t, err)
		require.Equal(t, uint64(4), filter.Cardinality())
		// Container key k holds value v as the 64-bit value (k<<16)|v.
		for key := int64(0); key < 4; key++ {
			require.True(t, filter.ContainsInt64(key<<16|(10+key)), key)
		}
	})
}
