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

package routing

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func channels(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = "by-dev-rootcoord-dml_" + string(rune('a'+i))
	}
	return out
}

func intIDs(data []int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}}}
}

func strIDs(data []string) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data}}}
}

func TestHashPKs_EquivalentToHashPK2Channels(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 8, 16} {
		ch := channels(n)
		tbl := DeriveCompat(ch)
		ints := intIDs([]int64{0, 1, 2, 7, 99, -1, 123456789, 1 << 40})
		assert.Equal(t, typeutil.HashPK2Channels(ints, ch), tbl.HashPKs(ints), "int64 n=%d", n)
		strs := strIDs([]string{"", "a", "abc", "namespace-42", "long-key-aaaaaaaaaaaaaaaa", strings.Repeat("x", 101)})
		assert.Equal(t, typeutil.HashPK2Channels(strs, ch), tbl.HashPKs(strs), "varchar n=%d", n)

		// Empty inputs must yield nil (not an empty slice), matching legacy exactly.
		emptyInts := intIDs([]int64{})
		assert.Equal(t, typeutil.HashPK2Channels(emptyInts, ch), tbl.HashPKs(emptyInts), "empty int64 n=%d", n)
		emptyStrs := strIDs([]string{})
		assert.Equal(t, typeutil.HashPK2Channels(emptyStrs, ch), tbl.HashPKs(emptyStrs), "empty varchar n=%d", n)
	}
}

func TestLookupBucket(t *testing.T) {
	tbl := DeriveCompat(channels(4))
	assert.Equal(t, "by-dev-rootcoord-dml_a", tbl.LookupBucket(0))
	assert.Equal(t, "by-dev-rootcoord-dml_b", tbl.LookupBucket(5)) // 5 % 4 == 1
	assert.Equal(t, 4, tbl.NumShards())
	assert.Equal(t, int64(CompatVersion), tbl.Version)
}

func TestLookupBucket_Empty(t *testing.T) {
	tbl := DeriveCompat([]string{})
	assert.Equal(t, "", tbl.LookupBucket(0))
	assert.Equal(t, 0, tbl.NumShards())
}

func TestHashPKs_UnsupportedType(t *testing.T) {
	tbl := DeriveCompat(channels(2))
	// IDs with nil IdField → should return nil just like HashPK2Channels
	ids := &schemapb.IDs{}
	assert.Nil(t, tbl.HashPKs(ids))
}

func TestDeriveCompat_Fields(t *testing.T) {
	ch := channels(3)
	tbl := DeriveCompat(ch)
	assert.Equal(t, int64(CompatVersion), tbl.Version)
	assert.Equal(t, ModeHash, tbl.Mode)
	assert.Equal(t, 3, tbl.NumShards())
}

// legacyAssign mirrors internal/proxy.assignChannelsByPK's map building.
// Note: the proxy computes the hash before the numChannels==0 guard, so its
// empty-channel return for the hash slice differs from RouteInsert's (nil,nil);
// callers below always use numChannels>=1, where the two are identical.
func legacyAssign(pks *schemapb.IDs, channelNames []string) (map[string][]int, []uint32) {
	hashValues := typeutil.HashPK2Channels(pks, channelNames)
	numChannels := len(channelNames)
	if numChannels == 0 {
		return nil, hashValues
	}
	avgCapacity := (len(hashValues) / numChannels) + 1
	out := make(map[string][]int, numChannels)
	for offset, channelID := range hashValues {
		idx := int(channelID)
		if idx >= numChannels {
			continue
		}
		name := channelNames[idx]
		if _, ok := out[name]; !ok {
			out[name] = make([]int, 0, avgCapacity)
		}
		out[name] = append(out[name], offset)
	}
	return out, hashValues
}

func TestRouteInsert_EquivalentToLegacy_Randomized(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for iter := 0; iter < 500; iter++ {
		n := 1 + r.Intn(16)
		ch := channels(n)
		tbl := DeriveCompat(ch)
		rows := 1 + r.Intn(50)
		var pks *schemapb.IDs
		if iter%2 == 0 {
			data := make([]int64, rows)
			for i := range data {
				data[i] = r.Int63() - r.Int63()
			}
			pks = intIDs(data)
		} else {
			data := make([]string, rows)
			for i := range data {
				data[i] = strings.Repeat("k", r.Intn(8)) + string(rune('0'+r.Intn(10)))
			}
			pks = strIDs(data)
		}
		wantMap, wantHash := legacyAssign(pks, ch)
		gotMap, gotHash := tbl.RouteInsert(pks)
		assert.Equal(t, wantHash, gotHash, "iter=%d hash", iter)
		assert.Equal(t, wantMap, gotMap, "iter=%d map", iter)
	}
}

func TestRouteInsert_EmptyChannels(t *testing.T) {
	tbl := DeriveCompat(nil)
	m, h := tbl.RouteInsert(intIDs([]int64{1, 2}))
	assert.Nil(t, m)
	assert.Empty(t, h)
}
