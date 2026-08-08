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
	"fmt"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"
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

func TestBuildParseRoundTrip(t *testing.T) {
	members := []int64{math.MinInt64, -1, 0, 1, 42, math.MaxInt64, 42}

	blob, err := Build(members)
	require.NoError(t, err)
	require.Equal(t, Magic, string(blob[:4]))
	require.Equal(t, Version, binary.LittleEndian.Uint16(blob[4:6]))
	require.Equal(t, FormatPortableRoaring64, binary.LittleEndian.Uint16(blob[6:8]))
	require.Equal(t, uint64(6), binary.LittleEndian.Uint64(blob[8:16]))
	require.Equal(t, uint64(len(blob)-HeaderSize), binary.LittleEndian.Uint64(blob[16:24]))
	require.Zero(t, binary.LittleEndian.Uint64(blob[24:32]))

	filter, err := Parse(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(6), filter.Cardinality())
	for _, member := range members {
		require.True(t, filter.ContainsInt64(member), "member %d must be present", member)
	}
	require.False(t, filter.ContainsInt64(2))
}

func TestBuildEmptySet(t *testing.T) {
	blob, err := Build(nil)
	require.NoError(t, err)

	filter, err := Parse(blob)
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

func TestBuildRejectsSparseHighContainersBeforeBitmapConstruction(t *testing.T) {
	members := make([]int64, MaxHighContainerCount+1)
	for i := range members {
		members[i] = int64(uint64(i) << 32)
	}
	_, err := Build(members)
	require.ErrorContains(t, err, "high-container count")
}

func TestBuildParsePortableContainerEncodings(t *testing.T) {
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
	// retaining a run container, so the run-cookie offset table is validated.
	for key := int64(2); key <= 4; key++ {
		members = append(members, (key<<16)+1)
	}
	members = append(members, (1<<32)+7)

	blob, err := Build(members)
	require.NoError(t, err)
	filter, err := Parse(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(len(members)), filter.Cardinality())
	for _, value := range []int64{0, 999, 1 << 16, (1 << 16) + 8192, (4 << 16) + 1, (1 << 32) + 7} {
		require.True(t, filter.ContainsInt64(value), "member %d must be present", value)
	}
}

func TestParseAcceptsRunCookieWithSingleValueArrayContainer(t *testing.T) {
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
	filter, err := Parse(mrb1Blob(body, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), filter.Cardinality())
	require.True(t, filter.ContainsInt64(42))
}

func TestParseRejectsMalformedEnvelope(t *testing.T) {
	valid, err := Build([]int64{-1, 0, 1})
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
			_, err := Parse(mutate(candidate))
			require.Error(t, err)
		})
	}
}

func TestParseRejectsImpossibleHighContainerCountWithoutPanic(t *testing.T) {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, ^uint64(0))
	blob := mrb1Blob(body, 0)

	var err error
	require.NotPanics(t, func() {
		_, err = Parse(blob)
	})
	require.ErrorContains(t, err, "high-container count")
}

func TestParseRejectsStructurallyInvalidPortableBody(t *testing.T) {
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
			_, err := Parse(mrb1Blob(test.body, test.cardinality))
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

	highErr := validatePortableRoaring64Body(portableBody(
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
	childErr := validatePortableRoaring64Body(portableBody(
		portableHighContainer{high: 0, child: child},
	))
	require.Error(t, childErr)
	require.NotContains(t, childErr.Error(), fmt.Sprint(firstChild))
	require.NotContains(t, childErr.Error(), fmt.Sprint(secondChild))
	require.Contains(t, childErr.Error(), "Roaring32 container 1")
}

func TestParseHandlesHighRunCountLinearly(t *testing.T) {
	body := portableBody(portableHighContainer{
		high:  0,
		child: portableRunChild(32768),
	})
	require.NoError(t, validatePortableRoaring64Body(body))
	filter, err := Parse(mrb1Blob(body, 32768))
	require.NoError(t, err)
	require.Equal(t, uint64(32768), filter.Cardinality())
	require.True(t, filter.ContainsInt64(0))
	require.True(t, filter.ContainsInt64(65534))
	require.False(t, filter.ContainsInt64(1))
}

func TestParseAcceptsPortableRunContainersProducedByOtherImplementations(t *testing.T) {
	// Java can retain two consecutive values as a run after mutating a larger
	// RunContainer; Java and CRoaring also retain the three-value size tie.
	for _, cardinality := range []uint64{2, 3} {
		t.Run(fmt.Sprintf("cardinality_%d", cardinality), func(t *testing.T) {
			body := portableBody(portableHighContainer{
				high:  0,
				child: portableSingleRunChild(0, uint16(cardinality-1)),
			})
			filter, err := Parse(mrb1Blob(body, cardinality))
			require.NoError(t, err)
			for value := int64(0); value < int64(cardinality); value++ {
				require.True(t, filter.ContainsInt64(value))
			}
		})
	}
}
