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

package authority

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt64PKEncodingPreservesOrder(t *testing.T) {
	values := []int64{math.MinInt64, -1 << 40, -2, -1, 0, 1, 2, 1 << 40, math.MaxInt64}
	encoded := make([][]byte, 0, len(values))
	for _, v := range values {
		key := Int64PK(v).Encode()
		require.Len(t, key, 8)
		encoded = append(encoded, key)
	}
	require.True(t, sort.SliceIsSorted(encoded, func(i, j int) bool {
		return bytes.Compare(encoded[i], encoded[j]) < 0
	}))
	for i := 1; i < len(encoded); i++ {
		require.NotEqual(t, encoded[i-1], encoded[i])
	}
}

func TestVarCharPKEncodingIsRawBytes(t *testing.T) {
	require.Equal(t, []byte("pk-1"), VarCharPK("pk-1").Encode())
	require.Empty(t, VarCharPK("").Encode())
}

func TestPKAccessors(t *testing.T) {
	i := Int64PK(42)
	require.False(t, i.IsVarChar())
	require.Equal(t, int64(42), i.Int64())

	s := VarCharPK("a")
	require.True(t, s.IsVarChar())
	require.Equal(t, "a", s.VarChar())

	require.Equal(t, Int64PK(42).Hash(), Int64PK(42).Hash())
	require.Equal(t, VarCharPK("a").Hash(), VarCharPK("a").Hash())
	require.NotEqual(t, Int64PK(1).Hash(), Int64PK(2).Hash())
}

// A plain multiplicative hash keeps the low-bit structure of the key, so keys
// that are all multiples of the stripe count would land on the same stripe.
// The splitmix64 finalizer must spread each key pattern over all 16 stripes.
func TestInt64PKHashSpreadsMultiplesOverStripes(t *testing.T) {
	const stripeCount = 16
	for _, multiplier := range []int64{1, 2, 10, 100, 1024} {
		t.Run("", func(t *testing.T) {
			stripes := make(map[uint64]struct{}, stripeCount)
			for i := int64(0); i < 100000; i++ {
				k := multiplier * i
				stripes[Int64PK(k).Hash()%stripeCount] = struct{}{}
			}
			require.Len(t, stripes, stripeCount)
		})
	}
}
