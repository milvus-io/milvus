// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func randomPKs(rng *rand.Rand, n int) ([]int64, []string) {
	ints := make([]int64, n)
	strs := make([]string, n)
	for i := 0; i < n; i++ {
		ints[i] = rng.Int63() - rng.Int63()
		// Lengths past 100 bytes exercise HashString2Uint32's prefix cut.
		b := make([]byte, rng.Intn(160))
		for j := range b {
			b[j] = byte('a' + rng.Intn(26))
		}
		strs[i] = string(b)
	}
	return ints, strs
}

func channelNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("v%d", i)
	}
	return names
}

// A never-split N-shard collection must place every pk exactly where
// typeutil.HashPK2Channels has always placed it.
func TestPKResidueParityWithHashPK2Channels(t *testing.T) {
	rng := rand.New(rand.NewSource(20260918))
	ints, strs := randomPKs(rng, 10000)
	for _, n := range []int{1, 2, 3, 16} {
		channels := channelNames(n)
		table, err := TableFromMeta(channels, nil, 0)
		require.NoError(t, err)
		require.EqualValues(t, n, table.Modulus())

		for _, ids := range []*schemapb.IDs{
			{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ints}}},
			{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: strs}}},
		} {
			want, err := typeutil.HashPK2Channels(ids, channels)
			require.NoError(t, err)
			residues, err := PKResidues(ids, uint64(n))
			require.NoError(t, err)
			require.Len(t, residues, len(want))
			for i := range want {
				require.EqualValues(t, want[i], residues[i], "n=%d i=%d", n, i)
			}
		}

		for i := range ints {
			r, err := PKResidue(ints[i], uint64(n))
			require.NoError(t, err)
			ch, err := table.VChannelOfPK(ints[i])
			require.NoError(t, err)
			require.Equal(t, channels[r], ch)

			r, err = PKResidue(strs[i], uint64(n))
			require.NoError(t, err)
			ch, err = table.VChannelOfPK(strs[i])
			require.NoError(t, err)
			require.Equal(t, channels[r], ch)
		}
	}
}

func TestPKResidueRefusesBadInput(t *testing.T) {
	_, err := PKResidueInt64(1, 0)
	assert.Error(t, err)
	_, err = PKResidueVarChar("a", 0)
	assert.Error(t, err)
	_, err = PKResidue(int32(1), 2)
	assert.Error(t, err)
	_, err = PKResidues(&schemapb.IDs{}, 0)
	assert.Error(t, err)

	residues, err := PKResidues(&schemapb.IDs{}, 2)
	assert.NoError(t, err)
	assert.Empty(t, residues)
	residues, err = PKResidues(nil, 2)
	assert.NoError(t, err)
	assert.Empty(t, residues)
}

// A one-shard collection split into t1 and t2: the modulus doubles to 2, the
// fenced source owns nothing, residue 0 goes to t1 and residue 1 to t2.
func TestTableFromMetaAfterSplit(t *testing.T) {
	vchannels := []string{"s", "t1", "t2"}
	infos := []*schemapb.CollectionShardInfo{
		{VchannelName: "s", State: schemapb.ShardState_ShardSplitting},
		{VchannelName: "t1", State: schemapb.ShardState_ShardCreating, Routing: hashRouting(0)},
		{VchannelName: "t2", State: schemapb.ShardState_ShardCreating, Routing: hashRouting(1)},
	}
	table, err := TableFromMeta(vchannels, infos, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, table.Modulus())

	ch, ok := table.Lookup(0)
	assert.True(t, ok)
	assert.Equal(t, "t1", ch)
	ch, ok = table.Lookup(1)
	assert.True(t, ok)
	assert.Equal(t, "t2", ch)
	_, ok = table.Lookup(2)
	assert.False(t, ok)

	rng := rand.New(rand.NewSource(1))
	ints, strs := randomPKs(rng, 1000)
	for i := range ints {
		for _, pk := range []any{ints[i], strs[i]} {
			r, err := PKResidue(pk, 2)
			require.NoError(t, err)
			ch, err := table.VChannelOfPK(pk)
			require.NoError(t, err)
			require.Equal(t, vchannels[1+r], ch)
		}
	}

	_, err = table.VChannelOfPK(1.5)
	assert.Error(t, err)
}

func TestTableFromMetaLegacy(t *testing.T) {
	vchannels := []string{"v0", "v1", "v2"}
	for _, infos := range [][]*schemapb.CollectionShardInfo{
		nil,
		{
			{VchannelName: "v0", State: schemapb.ShardState_ShardNormal},
			{VchannelName: "v1", State: schemapb.ShardState_ShardNormal},
			{State: schemapb.ShardState_ShardNormal},
		},
	} {
		table, err := TableFromMeta(vchannels, infos, 0)
		require.NoError(t, err)
		assert.EqualValues(t, 3, table.Modulus())
		for r, want := range vchannels {
			ch, ok := table.Lookup(uint64(r))
			assert.True(t, ok)
			assert.Equal(t, want, ch)
		}
	}
}

func TestTableFromMetaRefusesMalformedMeta(t *testing.T) {
	// A modulus with no residues is not quietly read as the legacy table.
	_, err := TableFromMeta([]string{"v0", "v1"}, nil, 4)
	assert.Error(t, err)
	// Infos not parallel to the vchannel list.
	_, err = TableFromMeta([]string{"v0", "v1"}, []*schemapb.CollectionShardInfo{{}}, 0)
	assert.Error(t, err)
}

func TestNilResidueTable(t *testing.T) {
	var table *ResidueTable
	assert.Zero(t, table.Modulus())
	_, ok := table.Lookup(0)
	assert.False(t, ok)
	_, err := table.VChannelOfPK(int64(1))
	assert.Error(t, err)
}

func TestPKResidueInt64PropagatesHashError(t *testing.T) {
	mockHash := mockey.Mock(typeutil.Hash32Int64).Return(uint32(0), merr.WrapErrServiceInternal("hash")).Build()
	defer mockHash.UnPatch()

	_, err := PKResidueInt64(1, 2)
	assert.Error(t, err)
	_, err = PKResidues(&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, 2)
	assert.Error(t, err)
}
