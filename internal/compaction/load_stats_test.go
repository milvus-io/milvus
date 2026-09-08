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

package compaction

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func requireInt64PKInBloomFilter(t *testing.T, stats *storage.PkStatistics, primaryKeys ...int64) {
	t.Helper()
	buf := make([]byte, 8)
	for _, primaryKey := range primaryKeys {
		common.Endian.PutUint64(buf, uint64(primaryKey))
		require.True(t, stats.PkFilter.Test(buf), "primary key %d is missing from the bloom filter", primaryKey)
	}
}

func TestLoadStatsFromPaths(t *testing.T) {
	paramtable.Init()

	t.Run("empty paths", func(t *testing.T) {
		stats, err := LoadStatsFromPaths(context.Background(), nil, 10, nil)
		require.NoError(t, err)
		require.Nil(t, stats)
	})

	t.Run("default stats", func(t *testing.T) {
		writer := &storage.StatsWriter{}
		err := writer.GenerateByData(100, schemapb.DataType_Int64, &storage.Int64FieldData{
			Data: []int64{10, 20},
		})
		require.NoError(t, err)

		paths := []string{"stats/100/0"}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, paths).Return([][]byte{writer.GetBuffer()}, nil).Once()

		stats, err := LoadStatsFromPaths(context.Background(), chunkManager, 10, paths)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		require.True(t, stats[0].MinPK.EQ(storage.NewInt64PrimaryKey(10)))
		require.True(t, stats[0].MaxPK.EQ(storage.NewInt64PrimaryKey(20)))
		requireInt64PKInBloomFilter(t, stats[0], 10, 20)
	})

	t.Run("compound stats take precedence", func(t *testing.T) {
		stat, err := storage.NewPrimaryKeyStats(100, int64(schemapb.DataType_Int64), 2)
		require.NoError(t, err)
		stat.Update(storage.NewInt64PrimaryKey(30))
		stat.Update(storage.NewInt64PrimaryKey(40))
		writer := &storage.StatsWriter{}
		require.NoError(t, writer.GenerateList([]*storage.PrimaryKeyStats{stat}))

		compoundPath := "stats/100/" + storage.CompoundStatsType.LogIdx()
		paths := []string{"stats/100/0", compoundPath}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, []string{compoundPath}).
			Return([][]byte{writer.GetBuffer()}, nil).
			Once()

		stats, err := LoadStatsFromPaths(context.Background(), chunkManager, 10, paths)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		require.True(t, stats[0].MinPK.EQ(storage.NewInt64PrimaryKey(30)))
		require.True(t, stats[0].MaxPK.EQ(storage.NewInt64PrimaryKey(40)))
		requireInt64PKInBloomFilter(t, stats[0], 30, 40)
	})

	t.Run("read error", func(t *testing.T) {
		readErr := errors.New("read stats")
		paths := []string{"stats/100/0"}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, paths).Return(nil, readErr).Once()

		stats, err := LoadStatsFromPaths(context.Background(), chunkManager, 10, paths)
		require.ErrorIs(t, err, readErr)
		require.Nil(t, stats)
	})

	t.Run("corrupt stats", func(t *testing.T) {
		paths := []string{"stats/100/0"}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, paths).Return([][]byte{[]byte("corrupt")}, nil).Once()

		stats, err := LoadStatsFromPaths(context.Background(), chunkManager, 10, paths)
		require.Error(t, err)
		require.Nil(t, stats)
	})
}

func TestLoadBM25StatsFromPaths(t *testing.T) {
	paramtable.Init()

	t.Run("empty paths", func(t *testing.T) {
		stats, err := LoadBM25StatsFromPaths(context.Background(), nil, 10, nil)
		require.NoError(t, err)
		require.Nil(t, stats)
	})

	t.Run("multiple files and fields", func(t *testing.T) {
		pathsByField := map[int64][]string{
			101: {"bm25/101/0", "bm25/101/1"},
			102: {"bm25/102/0", "bm25/102/1"},
		}
		payloadByPath := make(map[string][]byte)
		expectedByField := make(map[int64]*storage.BM25Stats)
		for fieldID, paths := range pathsByField {
			expected := storage.NewBM25Stats()
			for offset, filePath := range paths {
				row := map[uint32]float32{uint32(fieldID*10 + int64(offset)): float32(offset + 1)}
				fileStats := storage.NewBM25Stats()
				fileStats.Append(row)
				payload, err := fileStats.Serialize()
				require.NoError(t, err)
				payloadByPath[filePath] = payload
				expected.Append(row)
			}
			expectedByField[fieldID] = expected
		}

		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, paths []string) ([][]byte, error) {
				require.Len(t, paths, 4)
				payloads := make([][]byte, 0, len(paths))
				for _, filePath := range paths {
					payload, ok := payloadByPath[filePath]
					require.True(t, ok, "unexpected BM25 stats path %s", filePath)
					payloads = append(payloads, payload)
				}
				return payloads, nil
			}).
			Once()

		stats, err := LoadBM25StatsFromPaths(context.Background(), chunkManager, 10, pathsByField)
		require.NoError(t, err)
		require.Len(t, stats, 2)
		require.Equal(t, expectedByField[101], stats[101])
		require.Equal(t, expectedByField[102], stats[102])
	})

	t.Run("read error", func(t *testing.T) {
		readErr := errors.New("read bm25 stats")
		pathsByField := map[int64][]string{101: {"bm25/101/0"}}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, mock.Anything).Return(nil, readErr).Once()

		stats, err := LoadBM25StatsFromPaths(context.Background(), chunkManager, 10, pathsByField)
		require.ErrorIs(t, err, readErr)
		require.Nil(t, stats)
	})

	t.Run("corrupt stats", func(t *testing.T) {
		pathsByField := map[int64][]string{101: {"bm25/101/0"}}
		chunkManager := mock_storage.NewMockChunkManager(t)
		chunkManager.EXPECT().MultiRead(mock.Anything, mock.Anything).
			Return([][]byte{[]byte("corrupt")}, nil).
			Once()

		stats, err := LoadBM25StatsFromPaths(context.Background(), chunkManager, 10, pathsByField)
		require.Error(t, err)
		require.Nil(t, stats)
	})
}
