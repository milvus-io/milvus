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

package codec

import (
	"bytes"
	"encoding/hex"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

var int64Boundaries = []int64{
	math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64,
}

func sign(c int) int {
	if c > 0 {
		return 1
	}
	if c < 0 {
		return -1
	}
	return 0
}

func TestInt64PKOrderPreserving(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	pool := append([]int64{}, int64Boundaries...)
	for i := 0; i < 1000; i++ {
		pool = append(pool, r.Int63(), -r.Int63(), r.Int63n(1000)-500)
	}
	for i := 0; i < 100000; i++ {
		a := pool[r.Intn(len(pool))]
		b := pool[r.Intn(len(pool))]
		wantCmp := 0
		if a < b {
			wantCmp = -1
		} else if a > b {
			wantCmp = 1
		}
		got := sign(bytes.Compare(EncodeInt64PK(a), EncodeInt64PK(b)))
		assert.Equalf(t, wantCmp, got, "order mismatch for %d vs %d", a, b)
	}
}

func TestInt64PKRoundtrip(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	vals := append([]int64{}, int64Boundaries...)
	for i := 0; i < 10000; i++ {
		vals = append(vals, r.Int63()-r.Int63())
	}
	for _, v := range vals {
		enc := EncodeInt64PK(v)
		assert.Len(t, enc, Int64PKSize)
		dec, err := DecodeInt64PK(enc)
		assert.NoError(t, err)
		assert.Equal(t, v, dec)
	}
}

func TestDecodeInt64PKInvalidLength(t *testing.T) {
	for _, b := range [][]byte{nil, {}, make([]byte, 7), make([]byte, 9)} {
		_, err := DecodeInt64PK(b)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	}
}

func TestVarcharPKOrderPreserving(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	pool := []string{"", "a", "aa", "ab", "b", "\x00", "\xff", strings.Repeat("z", 300)}
	alphabet := "ab\x00\xffxyz"
	for i := 0; i < 1000; i++ {
		n := r.Intn(20)
		var sb strings.Builder
		for j := 0; j < n; j++ {
			sb.WriteByte(alphabet[r.Intn(len(alphabet))])
		}
		pool = append(pool, sb.String())
	}
	for i := 0; i < 100000; i++ {
		a := pool[r.Intn(len(pool))]
		b := pool[r.Intn(len(pool))]
		got := sign(bytes.Compare(EncodeVarcharPK(a), EncodeVarcharPK(b)))
		assert.Equalf(t, sign(strings.Compare(a, b)), got, "order mismatch for %q vs %q", a, b)
	}
}

func TestVarcharPKRoundtrip(t *testing.T) {
	for _, v := range []string{"", "pk", "\x00\xff", strings.Repeat("x", 1024)} {
		assert.Equal(t, v, DecodeVarcharPK(EncodeVarcharPK(v)))
	}
}

// The exact bytes are the on-disk format: every SST already written and every
// other producer depends on them, so they are pinned rather than only checked
// by round trip.
func TestInt64PKGoldenBytes(t *testing.T) {
	for _, tc := range []struct {
		pk   int64
		want string
	}{
		{math.MinInt64, "0000000000000000"},
		{-1, "7fffffffffffffff"},
		{0, "8000000000000000"},
		{1, "8000000000000001"},
		{math.MaxInt64, "ffffffffffffffff"},
	} {
		assert.Equal(t, tc.want, hex.EncodeToString(EncodeInt64PK(tc.pk)), "pk %d", tc.pk)
	}
}

func TestVarcharPKGoldenBytes(t *testing.T) {
	assert.Equal(t, "", hex.EncodeToString(EncodeVarcharPK("")))
	assert.Equal(t, "6162", hex.EncodeToString(EncodeVarcharPK("ab")))
}
