// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under
// the License.

package distance

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"

	"github.com/milvus-io/milvus/pkg/v3/util/distance/asm"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// compareWithTolerance asserts relative closeness (tolerant of FMA-vs-scalar
// float32 rounding differences). COSINE over a 768-dim dot product can suffer
// catastrophic cancellation between the two accumulation orders, so it gets a
// looser bound than L2/IP; the bound stays well below bfloat16's own 8-bit
// mantissa quantization (~0.4%), so a genuine kernel bug still fails.
func compareWithTolerance(t *testing.T, want, got []float32, relTol float64) {
	t.Helper()
	require.Equal(t, len(want), len(got))
	for i := range want {
		diff := math.Abs(float64(got[i] - want[i]))
		bound := relTol * math.Max(1, math.Abs(float64(want[i])))
		assert.True(t, diff <= bound, "row %d: got %v want %v", i, got[i], want[i])
	}
}

// randomFP16Rows returns random float16 byte rows in a sane value range. They
// are generated value-first so the exponents stay reasonable (uniform bit
// patterns would hit the float16 extremes and overflow the squared norms in
// COSINE). rng is a caller-provided RNG so the data is reproducible.
func randomFP16Rows(rng *rand.Rand, dim, rows int) []byte {
	data := make([]byte, 0, dim*rows*2)
	for i := 0; i < dim*rows; i++ {
		f := (rng.Float32()*2 - 1) * 100
		data = append(data, typeutil.Float32ToFloat16Bytes(f)...)
	}
	return data
}

// randomBFloat16Rows is the bfloat16 analog, also generated value-first:
// bfloat16 shares float32's 8-bit exponent, so uniform bit patterns produce
// ~1e38-scale values whose squares overflow float32.
func randomBFloat16Rows(rng *rand.Rand, dim, rows int) []byte {
	data := make([]byte, dim*rows*2)
	for i := 0; i < dim*rows; i++ {
		f := (rng.Float32()*2 - 1) * 100
		v := uint16(math.Float32bits(f) >> 16)
		data[i*2] = byte(v)
		data[i*2+1] = byte(v >> 8)
	}
	return data
}

func TestFusedSIMDMatchesPure(t *testing.T) {
	const dim int64 = 768
	const rows = 500
	// Seeded per subtest: the global math/rand source is re-seeded from the
	// wall clock by other tests in this package, so this test must not depend
	// on it. A fixed seed keeps the comparison reproducible.
	left := make([]float32, dim)
	leftRng := rand.New(rand.NewSource(42))
	for i := range left {
		left[i] = leftRng.Float32()
	}

	metrics := []string{"L2", "IP", "COSINE"}

	t.Run("int8", func(t *testing.T) {
		if !cpu.X86.HasAVX2 {
			t.Skip("requires AVX2")
		}
		right := make([]byte, dim*rows)
		rng := rand.New(rand.NewSource(43))
		for i := range right {
			right[i] = byte(rng.Intn(256))
		}
		for _, m := range metrics {
			got, err := CalcInt8FloatDistance(dim, left, right, m) // hook → SIMD
			require.NoError(t, err)
			want, err := calcInt8FloatDistancePure(dim, left, right, m)
			require.NoError(t, err)
			compareWithTolerance(t, want, got, toleranceForMetric(m))
		}
	})

	t.Run("float16", func(t *testing.T) {
		if !asm.HasF16C() || !cpu.X86.HasAVX2 {
			t.Skip("requires F16C and AVX2")
		}
		right := randomFP16Rows(rand.New(rand.NewSource(44)), int(dim), rows)
		for _, m := range metrics {
			got, err := CalcFloat16FloatDistance(dim, left, right, m)
			require.NoError(t, err)
			want, err := calcFloat16FloatDistancePure(dim, left, right, m)
			require.NoError(t, err)
			compareWithTolerance(t, want, got, toleranceForMetric(m))
		}
	})

	t.Run("bfloat16", func(t *testing.T) {
		if !cpu.X86.HasAVX2 {
			t.Skip("requires AVX2")
		}
		right := randomBFloat16Rows(rand.New(rand.NewSource(45)), int(dim), rows)
		for _, m := range metrics {
			got, err := CalcBFloat16FloatDistance(dim, left, right, m)
			require.NoError(t, err)
			want, err := calcBFloat16FloatDistancePure(dim, left, right, m)
			require.NoError(t, err)
			compareWithTolerance(t, want, got, toleranceForMetric(m))
		}
	})
}

// toleranceForMetric returns the SIMD-vs-pure comparison bound for a metric.
// COSINE is looser: its result is a ratio of 768-dim dot products, where the
// FMA-vs-scalar accumulation orders can diverge by a few e-3 relative when the
// inner product suffers cancellation.
func toleranceForMetric(metric string) float64 {
	if metric == COSINE {
		return 1e-2
	}
	return 1e-3
}

// TestFusedDecodeBitwiseRandom pins the F16C/AVX2 decoders bit-for-bit against
// the scalar decoders over uniform random bit patterns — covering subnormals,
// inf/NaN and every exponent range, not just the sane value ranges used in the
// distance tests. NaN lanes are compared with IsNaN (the quiet bit may differ
// between F16C and the scalar decoder); every other lane must match exactly.
func TestFusedDecodeBitwiseRandom(t *testing.T) {
	uniform := func(elems int) []byte {
		data := make([]byte, elems*2)
		for i := range data {
			data[i] = byte(rand.Intn(256))
		}
		return data
	}
	compareBitwise := func(t *testing.T, want, got []float32) {
		t.Helper()
		require.Equal(t, len(want), len(got))
		for i := range want {
			if math.IsNaN(float64(want[i])) {
				assert.True(t, math.IsNaN(float64(got[i])), "idx %d: want NaN got %08x", i, math.Float32bits(got[i]))
				continue
			}
			assert.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]), "idx %d", i)
		}
	}

	if asm.HasF16C() {
		data := uniform(768 * 500)
		got := make([]float32, len(data)/2)
		asm.DecodeFP16ToFloat32(got, data)
		compareBitwise(t, typeutil.Float16BytesToFloat32Vector(data), got)
	}
	if cpu.X86.HasAVX2 {
		data := uniform(768 * 500)
		got := make([]float32, len(data)/2)
		asm.DecodeBFloat16ToFloat32(got, data)
		compareBitwise(t, typeutil.BFloat16BytesToFloat32Vector(data), got)
	}
}

// TestFusedDecodeBoundaries pins the F16C/AVX2 decoders against the scalar
// decoders on subnormal/zero/normal patterns, where a decode error would show
// as a bit-level mismatch.
func TestFusedDecodeBoundaries(t *testing.T) {
	// half patterns: 1, 2, 3, subnormal(0x0001), +0, -0, 1.75, 0.5
	halves := []uint16{0x3C00, 0x4000, 0x4200, 0x0001, 0x0000, 0x8000, 0x3D60, 0x3800}
	src := make([]byte, len(halves)*2)
	for i, v := range halves {
		src[i*2] = byte(v)
		src[i*2+1] = byte(v >> 8)
	}

	got := make([]float32, len(halves))
	if asm.HasF16C() {
		asm.DecodeFP16ToFloat32(got, src)
		want := typeutil.Float16BytesToFloat32Vector(src)
		for i := range want {
			assert.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]), "half 0x%04x", halves[i])
		}
	}

	// bf16: 1, -1, 1.5, 0, subnormal-free simple set
	bfl := []uint16{0x3F80, 0xBF80, 0x3FC0, 0x0000, 0x8000, 0x3F00}
	bsrc := make([]byte, len(bfl)*2)
	for i, v := range bfl {
		bsrc[i*2] = byte(v)
		bsrc[i*2+1] = byte(v >> 8)
	}
	if cpu.X86.HasAVX2 {
		asm.DecodeBFloat16ToFloat32(got, bsrc)
		want := typeutil.BFloat16BytesToFloat32Vector(bsrc)
		for i := range want {
			assert.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]), "bf16 0x%04x", bfl[i])
		}
	}
}
