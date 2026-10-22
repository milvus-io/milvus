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
	"hash/fnv"
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

	require.Equal(t, uint64(42), Int64PK(42).lockKey())
	require.Equal(t, VarCharPK("a").lockKey(), VarCharPK("a").lockKey())
	require.NotEqual(t, VarCharPK("a").lockKey(), VarCharPK("b").lockKey())
}

// The varchar lock key is FNV-1a 64, written out to avoid a []byte copy.
func TestVarCharLockKeyIsFNV1a(t *testing.T) {
	for _, s := range []string{"", "a", "primary key 42"} {
		h := fnv.New64a()
		_, _ = h.Write([]byte(s))
		require.Equal(t, h.Sum64(), VarCharPK(s).lockKey())
	}
}
