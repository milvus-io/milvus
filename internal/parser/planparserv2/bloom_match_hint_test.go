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

package planparserv2

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hintBlob builds a header-shaped byte slice declaring n members. Only the
// n_declared field matters to oversizedBlobHint; the rest is never read.
func hintBlob(n uint64, size int) []byte {
	if size < mbf1HeaderSize {
		size = mbf1HeaderSize
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint64(b[8:16], n)
	return b
}

// sbbfBodyBytes reimplements sbbf.optimalNumOfBytes so the suggested fpr can be
// checked against what the client builder would actually produce.
func sbbfBodyBytes(n uint64, fpp float64) int {
	const minBits, maxBits = 32 * 8, mbf1MaxFilterBytes * 8
	m := -8.0 * float64(n) / math.Log(1.0-math.Pow(fpp, 1.0/8.0))
	var numBits int
	if m < 0 || m > float64(maxBits) {
		numBits = maxBits
	} else {
		numBits = int(m)
	}
	if numBits < minBits {
		numBits = minBits
	}
	if numBits&(numBits-1) != 0 {
		p := 1
		for p < numBits {
			p <<= 1
		}
		numBits = p
	}
	if numBits > maxBits {
		numBits = maxBits
	}
	return numBits >> 3
}

// TestOversizedBlobHintSuggestsWorkableFPR is the property that makes the hint
// worth printing: rebuilding at the suggested fpr must actually fit the cap.
// A hint that still overflows would send the caller round the loop twice.
func TestOversizedBlobHintSuggestsWorkableFPR(t *testing.T) {
	const maxSize = 64 * 1024 * 1024 // the proxy.maxMembershipFilterSize default
	usable := 64 * 1024 * 1024       // largest power-of-two body under the cap

	for _, n := range []uint64{
		30_000_000, 48_000_000, 48_700_000, 50_000_000, 55_000_000, 55_400_000,
	} {
		hint := oversizedBlobHint(hintBlob(n, 128*1024*1024), maxSize)
		require.NotEmptyf(t, hint, "n=%d should produce a hint", n)
		require.Containsf(t, hint, "fpr >=", "n=%d: %s", n, hint)

		fpr := hintFPR(t, hint)
		require.LessOrEqualf(t, fpr, mbf1MaxFPR, "n=%d suggested fpr above the accepted range", n)
		require.GreaterOrEqualf(t, fpr, mbf1MinFPR, "n=%d suggested fpr below the accepted range", n)

		got := sbbfBodyBytes(n, fpr)
		assert.LessOrEqualf(t, got, usable,
			"n=%d: rebuilding at the suggested fpr=%g yields a %d-byte body, still over the %d usable budget",
			n, fpr, got, usable)
	}
}

// TestOversizedBlobHintAtDefaultFPRBoundary pins the case this change exists
// for: 50M members at the default fpr=0.005 need 128 MiB and are rejected, and
// the hint must point at an fpr that brings them under 64 MiB.
func TestOversizedBlobHintAtDefaultFPRBoundary(t *testing.T) {
	const n = 50_000_000
	require.Equal(t, 128*1024*1024, sbbfBodyBytes(n, 0.005), "50M at the default fpr should need 128 MiB")
	require.Equal(t, 64*1024*1024, sbbfBodyBytes(n, 0.01), "50M at fpr=0.01 should fit 64 MiB")

	hint := oversizedBlobHint(hintBlob(n, 128*1024*1024+mbf1HeaderSize), 64*1024*1024)
	require.Contains(t, hint, "50000000")
	fpr := hintFPR(t, hint)
	assert.LessOrEqual(t, sbbfBodyBytes(n, fpr), 64*1024*1024)

	// Pin the exact bound, not just "some fpr that fits". The analytic threshold
	// is (1-e^(-n/B))^8 = 0.005797 for n=50M, B=64 MiB, and the hint rounds up
	// to four decimals, so 0.0058. Documentation that quotes a looser value
	// (0.01 fits, but wastes ~10% of the filter's accuracy budget) has drifted
	// from what the code actually computes; this assertion is what catches that.
	assert.InDelta(t, 0.0058, fpr, 1e-9,
		"the suggested fpr must be the tight bound, not a rounder nearby value")

	// One step below the suggestion must genuinely not fit, or the bound is not
	// tight.
	assert.Greater(t, sbbfBodyBytes(n, 0.0057), 64*1024*1024,
		"fpr just under the suggested bound must still overflow")
}

// TestOversizedBlobHintDegradesSafely covers the inputs where no advice is
// possible. The hint runs before envelope validation, so it must never panic
// or invent a suggestion from a malformed blob.
func TestOversizedBlobHintDegradesSafely(t *testing.T) {
	assert.Empty(t, oversizedBlobHint(nil, 64*1024*1024), "nil blob")
	assert.Empty(t, oversizedBlobHint(make([]byte, 8), 64*1024*1024), "shorter than the header")
	assert.Empty(t, oversizedBlobHint(hintBlob(1000, 64), 0), "nonsensical cap")
	assert.Empty(t, oversizedBlobHint(hintBlob(0, 64), 64*1024*1024), "no declared members")
	assert.Empty(t, oversizedBlobHint(hintBlob(math.MaxUint64, 64), 64*1024*1024), "absurd declared members")

	// Far past what the cap can hold at any accepted fpr: say so instead of
	// suggesting an fpr that would not help.
	hint := oversizedBlobHint(hintBlob(5_000_000_000, 64), 64*1024*1024)
	assert.Contains(t, hint, "even at the maximum fpr")
}

// hintFPR extracts the float following "fpr >= " in the hint.
func hintFPR(t *testing.T, hint string) float64 {
	t.Helper()
	idx := strings.Index(hint, "fpr >= ")
	require.GreaterOrEqualf(t, idx, 0, "hint carries no fpr: %q", hint)
	rest := hint[idx+len("fpr >= "):]
	if end := strings.IndexByte(rest, ' '); end >= 0 {
		rest = rest[:end]
	}
	v, err := strconv.ParseFloat(rest, 64)
	require.NoErrorf(t, err, "unparsable fpr %q in hint %q", rest, hint)
	return v
}
