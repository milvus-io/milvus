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

package datanode

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var binlogTestDir = "/tmp/milvus_test/test_binlog_io"

var validGeneratorFn = func(count int, done <-chan struct{}) <-chan UniqueID {
	ret := make(chan UniqueID, count)
	for i := 0; i < count; i++ {
		ret <- int64(100 + i)
	}
	return ret
}

func TestBinlogIOInterfaceMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(binlogTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	t.Run("Test download", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		b := &binlogIO{cm, alloc}
		tests := []struct {
			isvalid bool
			ks      []string // for preparation

			inctx context.Context

			description string
		}{
			{true, []string{"a", "b", "c"}, context.TODO(), "valid input"},
			{false, nil, context.Background(), "cancel by context"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					inkeys := []string{}
					for _, k := range test.ks {
						blob, key, err := prepareBlob(cm, k)
						require.NoError(t, err)
						assert.NotEmpty(t, blob)
						inkeys = append(inkeys, key)

						loaded, err := b.download(test.inctx, []string{key})
						assert.NoError(t, err)
						assert.ElementsMatch(t, blob, loaded[0].GetValue())
					}

					loaded, err := b.download(test.inctx, inkeys)
					assert.NoError(t, err)
					assert.Equal(t, len(test.ks), len(loaded))
				} else {
					ctx, cancel := context.WithCancel(test.inctx)
					cancel()

					_, err := b.download(ctx, nil)
					assert.EqualError(t, err, errDownloadFromBlobStorage.Error())
				}
			})
		}
	})

	t.Run("Test download twice", func(t *testing.T) {
		mkc := &mockCm{errMultiLoad: true}
		alloc := allocator.NewMockAllocator(t)
		b := &binlogIO{mkc, alloc}

		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*20)
		blobs, err := b.download(ctx, []string{"a"})
		assert.Error(t, err)
		assert.Empty(t, blobs)
		cancel()
	})

	t.Run("Test upload stats log err", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs", schemapb.DataType_Int64)

		t.Run("gen insert blob failed", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(nil, fmt.Errorf("mock err"))
			b := binlogIO{cm, alloc}
			_, _, err := b.uploadStatsLog(context.Background(), 1, 10, genInsertData(), genTestStat(meta), 10, meta)
			assert.Error(t, err)
		})
	})

	t.Run("Test upload insert log err", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs", schemapb.DataType_Int64)

		t.Run("empty insert", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			b := binlogIO{cm, alloc}

			paths, err := b.uploadInsertLog(context.Background(), 1, 10, genEmptyInsertData(), meta)
			assert.NoError(t, err)
			assert.Nil(t, paths)
		})

		t.Run("gen insert blob failed", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			b := binlogIO{cm, alloc}

			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(nil, fmt.Errorf("mock err"))

			_, err := b.uploadInsertLog(context.Background(), 1, 10, genInsertData(), meta)
			assert.Error(t, err)
		})

		t.Run("upload failed", func(t *testing.T) {
			mkc := &mockCm{errMultiLoad: true, errMultiSave: true}
			alloc := allocator.NewMockAllocator(t)
			b := binlogIO{mkc, alloc}

			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := b.uploadInsertLog(ctx, 1, 10, genInsertData(), meta)
			assert.Error(t, err)
		})
	})
}

func prepareBlob(cm storage.ChunkManager, key string) ([]byte, string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k := path.Join(cm.RootPath(), "test_prepare_blob", key)
	blob := []byte{1, 2, 3, 255, 188}

	err := cm.Write(ctx, k, blob[:])
	if err != nil {
		return nil, "", err
	}
	return blob, k, nil
}

func TestBinlogIOInnerMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(binlogTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	t.Run("Test genDeltaBlobs", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

		b := &binlogIO{cm, alloc}
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10002), "test_gen_blobs", schemapb.DataType_Int64)

		tests := []struct {
			isvalid  bool
			deletepk primaryKey
			ts       uint64

			description string
		}{
			{true, newInt64PrimaryKey(1), 1111111, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {

					k, v, err := b.genDeltaBlobs(&DeleteData{
						Pks: []primaryKey{test.deletepk},
						Tss: []uint64{test.ts},
					}, meta.GetID(), 10, 1)

					assert.NoError(t, err)
					assert.NotEmpty(t, k)
					assert.NotEmpty(t, v)

					log.Debug("genDeltaBlobs returns", zap.String("key", k))
				}
			})
		}
	})

	t.Run("Test genDeltaBlobs error", func(t *testing.T) {
		pk := newInt64PrimaryKey(1)

		t.Run("Test serialize error", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			b := &binlogIO{cm, alloc}
			k, v, err := b.genDeltaBlobs(&DeleteData{Pks: []primaryKey{pk}, Tss: []uint64{}}, 1, 1, 1)
			assert.Error(t, err)
			assert.Empty(t, k)
			assert.Empty(t, v)
		})

		t.Run("Test AllocOne error", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocOne().Call.Return(int64(0), fmt.Errorf("mock AllocOne error"))
			bin := binlogIO{cm, alloc}
			k, v, err := bin.genDeltaBlobs(&DeleteData{Pks: []primaryKey{pk}, Tss: []uint64{1}}, 1, 1, 1)
			assert.Error(t, err)
			assert.Empty(t, k)
			assert.Empty(t, v)

		})
	})

	t.Run("Test genInsertBlobs", func(t *testing.T) {
		f := &MetaFactory{}
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		b := binlogIO{cm, alloc}

		tests := []struct {
			pkType      schemapb.DataType
			description string
			expectError bool
		}{
			{schemapb.DataType_Int64, "int64PrimaryField", false},
			{schemapb.DataType_VarChar, "varCharPrimaryField", false},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs", test.pkType)
				iCodec := storage.NewInsertCodecWithSchema(meta)

				kvs := make(map[string][]byte)
				pin, err := b.genInsertBlobs(genInsertData(), 10, 1, iCodec, kvs)

				assert.NoError(t, err)
				assert.Equal(t, 12, len(pin))
				assert.Equal(t, 12, len(kvs))

				log.Debug("test paths",
					zap.Any("kvs no.", len(kvs)),
					zap.String("insert paths field0", pin[common.TimeStampField].GetBinlogs()[0].GetLogPath()))
			})
		}
	})

	t.Run("Test genInsertBlobs error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cm := storage.NewLocalChunkManager(storage.RootPath(binlogTestDir))
		defer cm.RemoveWithPrefix(ctx, cm.RootPath())

		t.Run("serialize error", func(t *testing.T) {
			iCodec := storage.NewInsertCodecWithSchema(nil)

			bin := &binlogIO{cm, allocator.NewMockAllocator(t)}
			kvs := make(map[string][]byte)
			pin, err := bin.genInsertBlobs(genEmptyInsertData(), 10, 1, iCodec, kvs)

			assert.Error(t, err)
			assert.Empty(t, kvs)
			assert.Empty(t, pin)
		})

		t.Run("GetGenerator error", func(t *testing.T) {
			f := &MetaFactory{}
			meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs", schemapb.DataType_Int64)
			iCodec := storage.NewInsertCodecWithSchema(meta)

			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock GetGenerator error"))
			bin := &binlogIO{cm, alloc}
			kvs := make(map[string][]byte)

			pin, err := bin.genInsertBlobs(genInsertData(), 10, 1, iCodec, kvs)

			assert.Error(t, err)
			assert.Empty(t, kvs)
			assert.Empty(t, pin)
		})
	})

	t.Run("Test genStatsBlob", func(t *testing.T) {
		f := &MetaFactory{}
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocOne().Return(0, nil)

		b := binlogIO{cm, alloc}

		tests := []struct {
			pkType      schemapb.DataType
			description string
			expectError bool
		}{
			{schemapb.DataType_Int64, "int64PrimaryField", false},
			{schemapb.DataType_VarChar, "varCharPrimaryField", false},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_stat_blobs", test.pkType)
				iCodec := storage.NewInsertCodecWithSchema(meta)

				kvs := make(map[string][]byte)
				stat, err := b.genStatBlobs(genTestStat(meta), 10, 1, iCodec, kvs, 0)

				assert.NoError(t, err)
				assert.Equal(t, 1, len(stat))
				assert.Equal(t, 1, len(kvs))
			})
		}
	})

	t.Run("Test genStatsBlob error", func(t *testing.T) {
		f := &MetaFactory{}
		alloc := allocator.NewMockAllocator(t)
		b := binlogIO{cm, alloc}

		t.Run("serialize error", func(t *testing.T) {
			meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_stat_blobs_error", schemapb.DataType_Int64)
			iCodec := storage.NewInsertCodecWithSchema(meta)

			kvs := make(map[string][]byte)
			_, err := b.genStatBlobs(nil, 10, 1, iCodec, kvs, 0)
			assert.Error(t, err)
		})
	})
}

type mockCm struct {
	storage.ChunkManager
	errMultiLoad    bool
	errMultiSave    bool
	MultiReadReturn [][]byte
	ReadReturn      []byte
}

var _ storage.ChunkManager = (*mockCm)(nil)

func (mk *mockCm) RootPath() string {
	return "mock_test"
}

func (mk *mockCm) Write(ctx context.Context, filePath string, content []byte) error {
	return nil
}

func (mk *mockCm) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	if mk.errMultiSave {
		return errors.New("mockKv multisave error")
	}
	return nil
}

func (mk *mockCm) Read(ctx context.Context, filePath string) ([]byte, error) {
	return mk.ReadReturn, nil
}

func (mk *mockCm) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	if mk.errMultiLoad {
		return nil, errors.New("mockKv multiload error")
	}

	if mk.MultiReadReturn != nil {
		return mk.MultiReadReturn, nil
	}
	return [][]byte{[]byte("a")}, nil
}

func (mk *mockCm) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	return nil, nil, nil
}

func (mk *mockCm) Remove(ctx context.Context, key string) error           { return nil }
func (mk *mockCm) MultiRemove(ctx context.Context, keys []string) error   { return nil }
func (mk *mockCm) RemoveWithPrefix(ctx context.Context, key string) error { return nil }
func (mk *mockCm) Close()                                                 {}
