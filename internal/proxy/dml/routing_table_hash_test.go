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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
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

func TestRepackDeleteMessagesSplitsAtMaxDeleteSize(t *testing.T) {
	paramtable.Init()
	oldLimit := paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue("-1")
	t.Cleanup(func() { paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(oldLimit) })
	for _, stringPK := range []bool{false, true} {
		name := "Int64"
		if stringPK {
			name = "VarChar"
		}
		t.Run(name, func(t *testing.T) {
			keys := &schemapb.IDs{}
			intKeys := []int64{0, -1, 127, 128, 16383, 16384, 1 << 58, -1 << 63, 1<<63 - 1}
			for row := range 400 {
				if stringPK {
					pkgtypeutil.AppendID(keys, strings.Repeat("界", row%43)+strconv.Itoa(row%17))
				} else {
					pkgtypeutil.AppendID(keys, intKeys[row%len(intKeys)])
				}
			}
			before := proto.Clone(keys)
			repack := func() ([]*msgstream.DeleteMsg, int64, error) {
				return repackDeleteMessages(t.Context(), keys, []string{"vchan-0"}, allocator.NewLocalAllocator(127, 1000),
					1<<58, 1, "collection", 2, "partition", "default", nil, nil)
			}
			paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue("-1")
			whole, _, err := repack()
			require.NoError(t, err)
			prefix := proto.Clone(whole[0].DeleteRequest).(*msgpb.DeleteRequest)
			prefix.NumRows, prefix.Timestamps = 127, prefix.Timestamps[:127]
			if stringPK {
				prefix.PrimaryKeys.GetStrId().Data = prefix.PrimaryKeys.GetStrId().Data[:127]
			} else {
				prefix.PrimaryKeys.GetIntId().Data = prefix.PrimaryKeys.GetIntId().Data[:127]
			}
			limit := proto.Size(prefix)
			paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(strconv.Itoa(limit))
			messages, count, err := repack()
			require.NoError(t, err)
			require.Equal(t, int64(400), count)
			require.Greater(t, len(messages), 1)
			require.Equal(t, int64(127), messages[0].NumRows)
			joined := &schemapb.IDs{}
			for index, msg := range messages {
				require.LessOrEqual(t, proto.Size(msg.DeleteRequest), limit)
				require.Equal(t, int64(127+index), msg.Base.MsgID)
				require.Equal(t, "vchan-0", msg.ShardName)
				require.Equal(t, int64(2), msg.PartitionID)
				require.Len(t, msg.Timestamps, int(msg.NumRows))
				require.Len(t, msg.HashValues, int(msg.NumRows))
				for row := range int(msg.NumRows) {
					require.Equal(t, uint64(1<<58), msg.Timestamps[row])
					pkgtypeutil.AppendIDs(joined, msg.PrimaryKeys, row)
				}
				if index+1 < len(messages) {
					candidate := proto.Clone(msg.DeleteRequest).(*msgpb.DeleteRequest)
					pkgtypeutil.AppendIDs(candidate.PrimaryKeys, messages[index+1].PrimaryKeys, 0)
					candidate.Timestamps = append(candidate.Timestamps, 1<<58)
					candidate.NumRows++
					require.Greater(t, proto.Size(candidate), limit, "use the largest fitting prefix")
				}
			}
			require.True(t, proto.Equal(keys, joined), "preserve key order and duplicate occurrences")
			require.True(t, proto.Equal(before, keys), "source keys stay unchanged")
		})
	}
}

func TestRepackDeleteMessagesLargeAutoIDBatch(t *testing.T) {
	paramtable.Init()
	const limit = 16 * 1024 * 1024
	old := paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(strconv.Itoa(limit))
	t.Cleanup(func() { paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(old) })
	keys := make([]int64, 1_000_000)
	for row := range keys {
		keys[row] = 1<<58 + int64(row)
	}
	result, rows, err := repackDeleteMessages(t.Context(), &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}},
		[]string{"vchan-0"}, allocator.NewLocalAllocator(100, 200), 1<<58, 1, "collection", -1, "", "default", nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(keys)), rows)
	require.Len(t, result, 2, "the full tombstone exceeds the default 16 MiB logical limit")
	offset := 0
	for _, msg := range result {
		require.LessOrEqual(t, proto.Size(msg.DeleteRequest), limit)
		require.Equal(t, keys[offset:offset+int(msg.NumRows)], msg.PrimaryKeys.GetIntId().Data)
		offset += int(msg.NumRows)
	}
	require.Equal(t, len(keys), offset)
}

func TestRepackDeleteMessagesRejectsWithoutPartialBatch(t *testing.T) {
	paramtable.Init()
	old := paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue("128")
	t.Cleanup(func() { paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(old) })
	t.Run("oversized later key", func(t *testing.T) {
		keys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"ok", strings.Repeat("x", 128)}}}}
		messages, rows, err := repackDeleteMessages(t.Context(), keys, []string{"vchan-0"}, allocator.NewLocalAllocator(100, 200),
			1000, 1, "collection", 2, "partition", "default", nil, nil)
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		require.Equal(t, merr.InputError, merr.GetErrorType(err))
		require.Nil(t, messages)
		require.Zero(t, rows)
	})
	t.Run("later allocation fails", func(t *testing.T) {
		keys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{strings.Repeat("a", 40), strings.Repeat("b", 40)}}}}
		messages, rows, err := repackDeleteMessages(t.Context(), keys, []string{"vchan-0"}, allocator.NewLocalAllocator(100, 101),
			1000, 1, "collection", 2, "partition", "default", nil, nil)
		require.True(t, allocator.IsIDExhausted(err))
		require.Nil(t, messages)
		require.Zero(t, rows)
	})
}

func TestRepackDeleteMessagesSplittingPreservesChannelRouting(t *testing.T) {
	paramtable.Init()
	old := paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue("128")
	t.Cleanup(func() { paramtable.Get().QuotaConfig.MaxDeleteSize.SwapTempValue(old) })
	channels := []string{"vchan-0", "vchan-1", "vchan-2"}
	keys := make([]int64, 64)
	for row := range keys {
		keys[row] = 1<<58 + int64(row)
	}
	primary := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}
	for _, useNamespace := range []bool{false, true} {
		t.Run(strconv.FormatBool(useNamespace), func(t *testing.T) {
			var namespace *string
			var schema *schemapb.CollectionSchema
			hashes := expectedInt64ModuloHashes(t, keys, len(channels))
			if useNamespace {
				namespace = proto.String("tenant-a")
				schema = &schemapb.CollectionSchema{EnableNamespace: true, Properties: []*commonpb.KeyValuePair{
					{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
					{Key: common.NamespaceShardingEnabledKey, Value: "true"},
				}}
				for row := range hashes {
					hashes[row] = pkgtypeutil.HashNamespace2Channels(*namespace, channels)
				}
			}
			result, count, err := repackDeleteMessages(t.Context(), primary, channels, allocator.NewLocalAllocator(127, 1000),
				0, 1, "collection", -1, "", "default", namespace, schema)
			require.NoError(t, err)
			require.Equal(t, int64(len(keys)), count)
			want, got := make([][]int64, len(channels)), make([][]int64, len(channels))
			for row, hash := range hashes {
				want[hash] = append(want[hash], keys[row])
			}
			for _, msg := range result {
				hash := msg.HashValues[0]
				require.Equal(t, channels[hash], msg.ShardName)
				require.LessOrEqual(t, proto.Size(msg.DeleteRequest), 128)
				got[hash] = append(got[hash], msg.PrimaryKeys.GetIntId().Data...)
			}
			require.Equal(t, want, got)
		})
	}
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
