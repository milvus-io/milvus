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

package dml

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	pkgtypeutil "github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAutoIDCandidatesPreservePKRouting(t *testing.T) {
	channels := []string{"channel-0", "channel-1", "channel-2"}
	keys := []int64{0, 1, 10, 100, 1000, -1}
	got := make([][]int64, len(channels))
	err := appendAutoIDCandidatesByChannels(got, keys, schemapb.DataType_Int64, channels)
	require.NoError(t, err)
	want := make([][]int64, len(channels))
	for row, channel := range expectedInt64ModuloHashes(t, keys, len(channels)) {
		want[channel] = append(want[channel], keys[row])
	}
	assert.Equal(t, want, got)
}

func TestAutoIDCandidatesReturnRoutingErrorWithoutChannels(t *testing.T) {
	err := appendAutoIDCandidatesByChannels(nil, []int64{1}, schemapb.DataType_Int64, nil)
	assert.ErrorIs(t, err, common.ErrRoutingTableNoValues)
}

func TestRepackDeleteMessagesPreservesModuloRouting(t *testing.T) {
	paramtable.Init()
	vChannels := []string{"vchan-0", "vchan-1", "vchan-2"}
	pks := []int64{0, 1, 10, 100, 1000, -1}
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: pks},
		},
	}

	got, rows, err := repackDeleteMessages(
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
	for _, msg := range got {
		hash := msg.HashValues[0]
		assert.Equal(t, vChannels[hash], msg.ShardName)
		var expectedKeys []int64
		for row, expectedHash := range expectedInt64ModuloHashes(t, pks, len(vChannels)) {
			if expectedHash == hash {
				expectedKeys = append(expectedKeys, pks[row])
			}
		}
		assert.Equal(t, expectedKeys, msg.PrimaryKeys.GetIntId().GetData())
		assert.Equal(t, int64(len(expectedKeys)), msg.NumRows)
		assert.Len(t, msg.Timestamps, len(expectedKeys))
		for _, timestamp := range msg.Timestamps {
			assert.Equal(t, uint64(1000), timestamp)
		}
		for _, msgHash := range msg.HashValues {
			assert.Equal(t, hash, msgHash)
			actualCounts[msgHash]++
		}
	}
	assert.Equal(t, expectedCounts, actualCounts)
}

func TestRepackDeleteMessagesReturnsRoutingErrorWithoutChannels(t *testing.T) {
	paramtable.Init()
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}

	got, rows, err := repackDeleteMessages(
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

func TestRepackDeleteMessagesHonorsMaxDeleteSize(t *testing.T) {
	paramtable.Init()
	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}
	repack := func() ([]*msgstream.DeleteMsg, int64, error) {
		return repackDeleteMessages(
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

	require.NoError(t, paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxDeleteSize.Key, "-1"))
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.MaxDeleteSize.Key)
	})
	result, rows, err := repack()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)
	require.Len(t, result, 1)
	var materializedSize int
	for _, msg := range result {
		materializedSize = msg.Size()
	}
	require.Positive(t, materializedSize)

	require.NoError(t, paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxDeleteSize.Key, strconv.Itoa(materializedSize)))
	_, _, err = repack()
	require.NoError(t, err, "the exact maxDeleteSize boundary must be accepted")

	require.NoError(t, paramtable.Get().Save(paramtable.Get().QuotaConfig.MaxDeleteSize.Key, strconv.Itoa(materializedSize-1)))
	result, rows, err = repack()
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Nil(t, result)
	assert.Zero(t, rows)
}

func TestRepackDeleteMessagesLeavesPhysicalChunkingToWAL(t *testing.T) {
	paramtable.Init()
	oldMaxMessageSize := paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue("512")
	oldMaxDeleteSize := paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue("-1")
	t.Cleanup(func() {
		paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(oldMaxMessageSize)
		paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(oldMaxDeleteSize)
	})

	primaryKeys := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{Data: []string{strings.Repeat("a", 300), strings.Repeat("b", 300)}},
		},
	}
	repack := func() []*msgstream.DeleteMsg {
		result, rows, err := repackDeleteMessages(
			context.Background(), primaryKeys, []string{"vchan-0"}, allocator.NewLocalAllocator(100, 200),
			1000, 1, "collection", 2, "partition", "default", nil, nil,
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), rows)
		return result
	}

	result := repack()
	require.Len(t, result, 1)
	require.True(t, proto.Equal(primaryKeys, result[0].PrimaryKeys))
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
