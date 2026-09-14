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

package proxy

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	pkgtypeutil "github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAssignChannelsByPKPreservesModuloRouting(t *testing.T) {
	channelNames := []string{"channel-0", "channel-1", "channel-2"}
	pks := []int64{0, 1, 10, 100, 1000, -1}
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: pks},
		},
	}
	insertMsg := &msgstream.InsertMsg{}

	got, err := assignChannelsByPK(ids, channelNames, insertMsg)

	expectedHashes := expectedInt64ModuloHashes(t, pks, len(channelNames))
	assert.NoError(t, err)
	assert.Equal(t, expectedHashes, insertMsg.HashValues)
	assert.Equal(t, expectedRowOffsetsByChannel(channelNames, expectedHashes), got)
}

func TestAssignChannelsByPKReturnsRoutingErrorWithoutChannels(t *testing.T) {
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}
	insertMsg := &msgstream.InsertMsg{}

	got, err := assignChannelsByPK(ids, nil, insertMsg)

	assert.ErrorIs(t, err, common.ErrRoutingTableNoValues)
	assert.Nil(t, got)
	assert.Empty(t, insertMsg.HashValues)
}

func TestRepackDeleteMsgByHashPreservesModuloRouting(t *testing.T) {
	paramtable.Init()
	vChannels := []string{"vchan-0", "vchan-1", "vchan-2"}
	pks := []int64{0, 1, 10, 100, 1000, -1}
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: pks},
		},
	}

	got, rows, err := repackDeleteMsgByHash(
		context.Background(),
		primaryKeys,
		vChannels,
		allocator.NewLocalAllocator(100, 200),
		1000,
		1,
		"collection",
		2,
		"partition",
		"default",
		nil,
		nil,
	)

	assert.NoError(t, err)
	assert.Equal(t, int64(len(pks)), rows)

	expectedCounts := make(map[uint32]int)
	for _, hash := range expectedInt64ModuloHashes(t, pks, len(vChannels)) {
		expectedCounts[hash]++
	}
	actualCounts := make(map[uint32]int)
	for hash, msgs := range got {
		for _, msg := range msgs {
			assert.Equal(t, vChannels[hash], msg.ShardName)
			for _, msgHash := range msg.HashValues {
				assert.Equal(t, hash, msgHash)
				actualCounts[msgHash]++
			}
		}
	}
	assert.Equal(t, expectedCounts, actualCounts)
}

func TestRepackDeleteMsgByHashReturnsRoutingErrorWithoutChannels(t *testing.T) {
	paramtable.Init()
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}

	got, rows, err := repackDeleteMsgByHash(
		context.Background(),
		primaryKeys,
		nil,
		allocator.NewLocalAllocator(100, 200),
		1000,
		1,
		"collection",
		2,
		"partition",
		"default",
		nil,
		nil,
	)

	assert.ErrorIs(t, err, common.ErrRoutingTableNoValues)
	assert.Nil(t, got)
	assert.Zero(t, rows)
}

func TestRepackDeleteMsgByHashHonorsMaxDeleteSize(t *testing.T) {
	paramtable.Init()
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}
	repack := func() (map[uint32][]*msgstream.DeleteMsg, int64, error) {
		return repackDeleteMsgByHash(
			context.Background(),
			primaryKeys,
			[]string{"vchan-0"},
			allocator.NewLocalAllocator(100, 200),
			1000,
			1,
			"collection",
			2,
			"partition",
			"default",
			nil,
			nil,
		)
	}

	require.NoError(t, Params.Save(Params.QuotaConfig.MaxDeleteSize.Key, "-1"))
	t.Cleanup(func() {
		Params.Reset(Params.QuotaConfig.MaxDeleteSize.Key)
	})
	result, rows, err := repack()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)
	require.Len(t, result, 1)
	var materializedSize int
	for _, msgs := range result {
		require.Len(t, msgs, 1)
		materializedSize = msgs[0].Size()
	}
	require.Positive(t, materializedSize)

	require.NoError(t, Params.Save(Params.QuotaConfig.MaxDeleteSize.Key, strconv.Itoa(materializedSize)))
	_, _, err = repack()
	require.NoError(t, err, "the exact maxDeleteSize boundary must be accepted")

	require.NoError(t, Params.Save(Params.QuotaConfig.MaxDeleteSize.Key, strconv.Itoa(materializedSize-1)))
	result, rows, err = repack()
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Nil(t, result)
	assert.Zero(t, rows)
}

func TestRepackDeleteMsgByHashSwitchesChunkOwner(t *testing.T) {
	paramtable.Init()
	oldMaxMessageSize := Params.PulsarCfg.MaxMessageSize.SwapTempValue("512")
	oldMaxDeleteSize := Params.QuotaConfig.MaxDeleteSize.SwapTempValue("-1")
	oldSplitChunkProxy := Params.ProxyCfg.SplitChunkProxy.SwapTempValue("true")
	t.Cleanup(func() {
		Params.PulsarCfg.MaxMessageSize.SwapTempValue(oldMaxMessageSize)
		Params.QuotaConfig.MaxDeleteSize.SwapTempValue(oldMaxDeleteSize)
		Params.ProxyCfg.SplitChunkProxy.SwapTempValue(oldSplitChunkProxy)
	})

	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{Data: []string{strings.Repeat("a", 300), strings.Repeat("b", 300)}},
		},
	}
	repack := func() map[uint32][]*msgstream.DeleteMsg {
		result, rows, err := repackDeleteMsgByHash(
			context.Background(), primaryKeys, []string{"vchan-0"}, allocator.NewLocalAllocator(100, 200),
			1000, 1, "collection", 2, "partition", "default", nil, nil,
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), rows)
		return result
	}

	result := repack()
	require.Len(t, result[0], 2)

	Params.ProxyCfg.SplitChunkProxy.SwapTempValue("false")
	result = repack()
	require.Len(t, result[0], 1)
}

func expectedInt64ModuloHashes(t *testing.T, keys []int64, targetCount int) []uint32 {
	t.Helper()
	hashes := make([]uint32, 0, len(keys))
	for _, key := range keys {
		hash, err := pkgtypeutil.Hash32Int64(key)
		assert.NoError(t, err)
		hashes = append(hashes, hash%uint32(targetCount))
	}
	return hashes
}

func expectedRowOffsetsByChannel(channelNames []string, hashes []uint32) map[string][]int {
	offsets := make(map[string][]int)
	for offset, hash := range hashes {
		channelName := channelNames[hash]
		offsets[channelName] = append(offsets[channelName], offset)
	}
	return offsets
}
