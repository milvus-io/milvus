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

// Golden vector file: testdata/golden_vectors.json
//
// The file is shared with the C++ prober conformance tests (segcore
// BloomFilterExpr); keep the schema stable. Schema:
//
//	{
//	  "description": string,          // human-readable note
//	  "cases": [
//	    {
//	      "name":          string,    // unique case name
//	      "n":             uint64,    // n passed to NewBuilder
//	      "fpr":           float64,   // fpr passed to NewBuilder
//	      "int_values":    [string],  // int64 values inserted (decimal strings,
//	                                  // to survive double-precision JSON parsers),
//	                                  // inserted before string_values
//	      "string_values": [string],  // string values inserted, in order
//	      "blob_hex":      string,    // full MBF1 envelope, lowercase hex
//	      "probes": [
//	        {
//	          "kind":   "int64" | "string",
//	          "int64":  string,       // decimal string, present iff kind == "int64"
//	          "string": string,       // present iff kind == "string"
//	          "member": bool,         // whether the value was inserted
//	          "expect": bool          // exact probe result against blob_hex;
//	                                  // always true for members (no false
//	                                  // negatives); for non-members this pins
//	                                  // the concrete outcome of THIS filter —
//	                                  // false positives are possible and, when
//	                                  // present, are recorded as expect=true
//	        }
//	      ]
//	    }
//	  ]
//	}
//
// Insertion order does not affect the final blob (bit OR is commutative); it
// is recorded only for reproducibility. To regenerate after an intentional
// format change:
//
//	SBBF_REGEN_GOLDEN=1 go test -tags dynamic,test -run TestGoldenVectors ./util/sbbf/...
package sbbf

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBuilderValidation(t *testing.T) {
	for _, fpr := range []float64{0.0001, 0.001, 0.01, 0.05} {
		b, err := NewBuilder(100, fpr)
		require.NoError(t, err, "fpr=%v", fpr)
		require.NotNil(t, b)
	}
	for _, fpr := range []float64{0, 0.00009, 0.051, 1, -0.001, math.NaN(), math.Inf(1)} {
		_, err := NewBuilder(100, fpr)
		require.Error(t, err, "fpr=%v", fpr)
	}
}

func TestSizingMatchesArrowFormula(t *testing.T) {
	// Mirror of Arrow BlockSplitBloomFilter::OptimalNumOfBytes expectations.
	cases := []struct {
		ndv       uint64
		fpp       float64
		wantBytes uint32
	}{
		{0, 0.01, 32},                    // clamped to kMinimumBloomFilterBytes
		{1, 0.05, 32},                    // tiny set still gets the 32-byte minimum
		{100000, 0.001, 256 * 1024},      // m=1461157 bits -> next pow2 = 2^21 bits
		{1 << 40, 0.001, MaxFilterBytes}, // overflow clamps to maximum
	}
	for _, c := range cases {
		got := optimalNumOfBytes(c.ndv, c.fpp)
		require.Equal(t, c.wantBytes, got, "ndv=%d fpp=%v", c.ndv, c.fpp)
		require.Zero(t, got&(got-1), "size must be a power of two")
		require.Zero(t, got%BytesPerBlock)
	}
}

func TestRoundTripInt64NoFalseNegatives(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	values := make([]int64, 0, 10000)
	seen := make(map[int64]struct{}, 10000)
	for len(values) < 10000 {
		v := int64(rng.Uint64())
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}
	// Include boundary values.
	for _, v := range []int64{0, 1, -1, math.MinInt64, math.MaxInt64} {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			values = append(values, v)
		}
	}

	b, err := NewBuilder(uint64(len(values)), 0.001)
	require.NoError(t, err)
	for _, v := range values {
		b.AddInt64(v)
	}
	f, err := Parse(b.Marshal())
	require.NoError(t, err)
	for _, v := range values {
		require.True(t, f.TestInt64(v), "false negative for %d", v)
	}
}

func TestRoundTripStringNoFalseNegatives(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	values := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		values = append(values, fmt.Sprintf("val-%d-%x", i, rng.Uint64()))
	}
	values = append(values, "", "a", "日本語テキスト", "🚀")

	b, err := NewBuilder(uint64(len(values)), 0.001)
	require.NoError(t, err)
	for _, s := range values {
		b.AddString(s)
	}
	f, err := Parse(b.Marshal())
	require.NoError(t, err)
	for _, s := range values {
		require.True(t, f.TestString(s), "false negative for %q", s)
	}
}

func TestEmpiricalFPR(t *testing.T) {
	const (
		nMembers = 100000
		nProbes  = 1000000
		fpr      = 0.001
		maxFPR   = 0.003
	)
	// Members are even, probes odd: disjoint by construction, no RNG
	// collision bookkeeping needed.
	b, err := NewBuilder(nMembers, fpr)
	require.NoError(t, err)
	for i := int64(0); i < nMembers; i++ {
		b.AddInt64(i * 2)
	}
	f, err := Parse(b.Marshal())
	require.NoError(t, err)

	falsePositives := 0
	for i := int64(0); i < nProbes; i++ {
		if f.TestInt64(i*2 + 1) {
			falsePositives++
		}
	}
	measured := float64(falsePositives) / float64(nProbes)
	t.Logf("measured FPR = %v (%d/%d), declared %v", measured, falsePositives, nProbes, fpr)
	require.Less(t, measured, maxFPR)
}

func TestParseNegativeCases(t *testing.T) {
	b, err := NewBuilder(10, 0.001)
	require.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		b.AddInt64(i)
	}
	valid := b.Marshal()
	_, err = Parse(valid)
	require.NoError(t, err)

	mutate := func(blob []byte, f func(b []byte)) []byte {
		out := make([]byte, len(blob))
		copy(out, blob)
		f(out)
		return out
	}

	cases := []struct {
		name string
		blob []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"truncated header", valid[:HeaderSize-1]},
		{"header only, missing body", valid[:HeaderSize]},
		{"truncated body", valid[:len(valid)-1]},
		{"trailing garbage", append(append([]byte{}, valid...), 0x00)},
		{"bad magic", mutate(valid, func(b []byte) { copy(b[0:4], "XBF1") })},
		{"wrong version", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint16(b[4:6], 2) })},
		{"wrong algo", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint16(b[6:8], 0) })},
		{"reserved nonzero", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint32(b[28:32], 1) })},
		{"num_blocks zero", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint32(b[24:28], 0) })},
		{"num_blocks not power of two", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint32(b[24:28], 3) })},
		{"num_blocks over maximum", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint32(b[24:28], 1<<23) })},
		// Hostile size: header claims 2^22 blocks (128 MB) with a tiny body.
		// Must be rejected by length check without allocating 128 MB.
		{"body length mismatch (hostile num_blocks)", mutate(valid, func(b []byte) { binary.LittleEndian.PutUint32(b[24:28], 1<<22) })},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := Parse(c.blob)
			require.Error(t, err)
			require.Nil(t, f)
		})
	}
}

func TestFilterAccessors(t *testing.T) {
	b, err := NewBuilder(1234, 0.01)
	require.NoError(t, err)
	f, err := Parse(b.Marshal())
	require.NoError(t, err)
	require.Equal(t, uint64(1234), f.NDeclared())
	require.Equal(t, 0.01, f.FPRDeclared())
	require.Equal(t, b.NumBlocks(), f.NumBlocks())
	// Empty filter matches nothing.
	require.False(t, f.TestInt64(42))
	require.False(t, f.TestString("42"))
}

// TestEstimateMarshalSize checks the pre-build size estimate equals the actual
// Marshal() length exactly, so callers can reject oversized filters before building.
func TestEstimateMarshalSize(t *testing.T) {
	for _, tc := range []struct {
		n   uint64
		fpr float64
	}{
		{1, 0.05}, {100, 0.001}, {100_000, 0.01}, {1_000_000, 0.001}, {10_000_000, 0.001},
	} {
		est, err := EstimateMarshalSize(tc.n, tc.fpr)
		require.NoError(t, err)
		b, err := NewBuilder(tc.n, tc.fpr)
		require.NoError(t, err)
		b.AddInt64(1) // adding values must not change the size
		require.Equalf(t, len(b.Marshal()), est,
			"estimate must equal actual Marshal size for n=%d fpr=%v", tc.n, tc.fpr)
	}
	// invalid fpr is rejected without building.
	_, err := EstimateMarshalSize(100, 0.5)
	require.Error(t, err)
}

// ---- golden vectors ----

type goldenProbe struct {
	Kind   string  `json:"kind"`
	Int64  string  `json:"int64,omitempty"`
	String *string `json:"string,omitempty"`
	Member bool    `json:"member"`
	Expect bool    `json:"expect"`
}

type goldenCase struct {
	Name         string        `json:"name"`
	N            uint64        `json:"n"`
	FPR          float64       `json:"fpr"`
	IntValues    []string      `json:"int_values"`
	StringValues []string      `json:"string_values"`
	BlobHex      string        `json:"blob_hex"`
	Probes       []goldenProbe `json:"probes"`
}

type goldenFile struct {
	Description string       `json:"description"`
	Cases       []goldenCase `json:"cases"`
}

// goldenInputs defines the fixed inputs of the golden cases. Blob bytes and
// non-member probe outcomes are derived (and pinned) in the vector file.
type goldenInputs struct {
	name       string
	fpr        float64
	ints       []int64
	strings    []string
	nonMembInt []int64
	nonMembStr []string
}

func goldenInputCases() []goldenInputs {
	// Case 3: deterministic "mixed" set, large enough for multiple blocks.
	mixedInts := make([]int64, 0, 100)
	for i := int64(0); i < 100; i++ {
		mixedInts = append(mixedInts, i*i*2654435761-i) // deterministic, spread out
	}
	mixedStrs := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		mixedStrs = append(mixedStrs, fmt.Sprintf("key-%03d", i))
	}
	return []goldenInputs{
		{
			name: "small_int64_set",
			fpr:  0.001,
			ints: []int64{math.MinInt64, -1, 0, 1, 2, 42, 1000000007, math.MaxInt64},
			nonMembInt: []int64{
				3, 7, -2, 123456789, 9999, math.MinInt64 + 1, math.MaxInt64 - 1,
			},
		},
		{
			name:    "small_string_set",
			fpr:     0.001,
			strings: []string{"", "a", "milvus", "bloom", "日本語", "🚀🚀", "hello world"},
			nonMembStr: []string{
				"b", "milvusx", "Bloom", "hell", "世界", " ", "hello  world",
			},
		},
		{
			name:       "mixed_int_string_fpr01",
			fpr:        0.01,
			ints:       mixedInts,
			strings:    mixedStrs,
			nonMembInt: []int64{-12345, 17, 999999999999},
			nonMembStr: []string{"key-100", "key-999", "KEY-000", "absent"},
		},
	}
}

func buildGoldenCase(t *testing.T, in goldenInputs) goldenCase {
	n := uint64(len(in.ints) + len(in.strings))
	b, err := NewBuilder(n, in.fpr)
	require.NoError(t, err)
	for _, v := range in.ints {
		b.AddInt64(v)
	}
	for _, s := range in.strings {
		b.AddString(s)
	}
	blob := b.Marshal()
	f, err := Parse(blob)
	require.NoError(t, err)

	gc := goldenCase{
		Name:         in.name,
		N:            n,
		FPR:          in.fpr,
		IntValues:    make([]string, 0, len(in.ints)),
		StringValues: in.strings,
		BlobHex:      hex.EncodeToString(blob),
	}
	if gc.StringValues == nil {
		gc.StringValues = []string{}
	}
	for _, v := range in.ints {
		gc.IntValues = append(gc.IntValues, strconv.FormatInt(v, 10))
	}
	for _, v := range in.ints {
		require.True(t, f.TestInt64(v), "member %d must probe true", v)
		gc.Probes = append(gc.Probes, goldenProbe{Kind: "int64", Int64: strconv.FormatInt(v, 10), Member: true, Expect: true})
	}
	for _, s := range in.strings {
		require.True(t, f.TestString(s), "member %q must probe true", s)
		s := s
		gc.Probes = append(gc.Probes, goldenProbe{Kind: "string", String: &s, Member: true, Expect: true})
	}
	for _, v := range in.nonMembInt {
		gc.Probes = append(gc.Probes, goldenProbe{Kind: "int64", Int64: strconv.FormatInt(v, 10), Member: false, Expect: f.TestInt64(v)})
	}
	for _, s := range in.nonMembStr {
		s := s
		gc.Probes = append(gc.Probes, goldenProbe{Kind: "string", String: &s, Member: false, Expect: f.TestString(s)})
	}
	return gc
}

func goldenPath(t *testing.T) string {
	return filepath.Join("testdata", "golden_vectors.json")
}

// cppGoldenPath is the C++ unittest's copy of the golden vectors. The C++
// unittest environment does not check out the standalone client/ module, so it
// keeps its own copy under internal/core/unittest; the two must stay
// byte-identical. Empty if the server tree is not present (standalone client).
func cppGoldenPath() string {
	p := filepath.Join("..", "..", "internal", "core", "unittest", "testdata", "bloom", "golden_vectors.json")
	if _, err := os.Stat(filepath.Dir(p)); err != nil {
		return ""
	}
	return p
}

func TestGoldenVectors(t *testing.T) {
	if os.Getenv("SBBF_REGEN_GOLDEN") != "" {
		gf := goldenFile{
			Description: "Milvus MBF1 / parquet SBBF (XXH64 seed=0) golden vectors. " +
				"int64 values are decimal strings hashed as 8-byte little-endian; " +
				"string values are hashed as raw UTF-8 bytes. blob_hex is the full " +
				"MBF1 envelope. See sbbf_test.go for the schema.",
		}
		for _, in := range goldenInputCases() {
			gf.Cases = append(gf.Cases, buildGoldenCase(t, in))
		}
		data, err := json.MarshalIndent(&gf, "", "  ")
		require.NoError(t, err)
		out := append(data, '\n')
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(goldenPath(t), out, 0o600))
		t.Logf("regenerated %s", goldenPath(t))
		// Keep the C++ unittest copy in sync in the same regen run.
		if cpp := cppGoldenPath(); cpp != "" {
			require.NoError(t, os.WriteFile(cpp, out, 0o600))
			t.Logf("regenerated %s", cpp)
		}
	}

	data, err := os.ReadFile(goldenPath(t))
	require.NoError(t, err, "golden vector file missing; regenerate with SBBF_REGEN_GOLDEN=1")

	// The C++ unittest reads its own copy; pin the two byte-identical so a
	// regen can never leave the C++ conformance test on stale vectors.
	if cpp := cppGoldenPath(); cpp != "" {
		cppData, cppErr := os.ReadFile(cpp)
		require.NoError(t, cppErr)
		require.Equal(t, string(data), string(cppData),
			"client/sbbf/testdata and internal/core/unittest/testdata/bloom golden vectors diverged; regenerate with SBBF_REGEN_GOLDEN=1")
	}
	var gf goldenFile
	require.NoError(t, json.Unmarshal(data, &gf))
	require.Len(t, gf.Cases, len(goldenInputCases()))

	inputsByName := make(map[string]goldenInputs)
	for _, in := range goldenInputCases() {
		inputsByName[in.name] = in
	}

	for _, gc := range gf.Cases {
		t.Run(gc.Name, func(t *testing.T) {
			in, ok := inputsByName[gc.Name]
			require.True(t, ok, "unknown golden case %q", gc.Name)

			// Rebuild from the recorded inputs and assert byte-identity.
			rebuilt := buildGoldenCase(t, in)
			require.Equal(t, gc.BlobHex, rebuilt.BlobHex, "builder output diverged from golden blob")
			require.Equal(t, gc.N, rebuilt.N)
			require.Equal(t, gc.IntValues, rebuilt.IntValues)
			require.Equal(t, gc.StringValues, rebuilt.StringValues)

			// Re-verify every probe against the recorded blob.
			blob, err := hex.DecodeString(gc.BlobHex)
			require.NoError(t, err)
			f, err := Parse(blob)
			require.NoError(t, err)
			for _, p := range gc.Probes {
				var got bool
				switch p.Kind {
				case "int64":
					v, err := strconv.ParseInt(p.Int64, 10, 64)
					require.NoError(t, err)
					got = f.TestInt64(v)
				case "string":
					require.NotNil(t, p.String, "string probe missing value")
					got = f.TestString(*p.String)
				default:
					t.Fatalf("unknown probe kind %q", p.Kind)
				}
				require.Equal(t, p.Expect, got, "probe %+v", p)
				if p.Member {
					require.True(t, got, "false negative on member probe %+v", p)
				}
			}
		})
	}
}
