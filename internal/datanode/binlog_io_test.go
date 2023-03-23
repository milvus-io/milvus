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

	"github.com/milvus-io/milvus-proto/go-api/schemapb"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/datanode/allocator"

	"github.com/cockroachdb/errors"
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

	t.Run("Test upload", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "uploads", schemapb.DataType_Int64)

		//pkFieldID := int64(106)
		iData := genInsertData()
		pk := newInt64PrimaryKey(888)
		dData := &DeleteData{
			RowCount: 1,
			Pks:      []primaryKey{pk},
			Tss:      []uint64{666666},
		}
		t.Run("Test upload one iData", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

			b := &binlogIO{cm, alloc}
			p, err := b.upload(context.TODO(), 1, 10, []*InsertData{iData}, dData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(p.inPaths))
			assert.Equal(t, 1, len(p.statsPaths))
			assert.Equal(t, 1, len(p.inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(p.statsPaths[0].GetBinlogs()))
			assert.NotNil(t, p.deltaInfo)
		})

		t.Run("Test upload two iData", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

			b := &binlogIO{cm, alloc}
			p, err := b.upload(context.TODO(), 1, 10, []*InsertData{iData, iData}, dData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(p.inPaths))
			assert.Equal(t, 1, len(p.statsPaths))
			assert.Equal(t, 2, len(p.inPaths[0].GetBinlogs()))
			assert.Equal(t, 2, len(p.statsPaths[0].GetBinlogs()))
			assert.NotNil(t, p.deltaInfo)

		})

		t.Run("Test uploadInsertLog", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)

			b := &binlogIO{cm, alloc}

			in, stats, err := b.uploadInsertLog(ctx, 1, 10, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(in))
			assert.Equal(t, 1, len(in[0].GetBinlogs()))
			assert.Equal(t, 1, len(stats))
		})
		t.Run("Test uploadDeltaLog", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

			b := &binlogIO{cm, alloc}
			deltas, err := b.uploadDeltaLog(ctx, 1, 10, dData, meta)
			assert.NoError(t, err)
			assert.NotNil(t, deltas)
			assert.Equal(t, 1, len(deltas[0].GetBinlogs()))
		})

		t.Run("Test context Done", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

			b := &binlogIO{cm, alloc}

			p, err := b.upload(ctx, 1, 10, []*InsertData{iData}, dData, meta)
			assert.EqualError(t, err, errUploadToBlobStorage.Error())
			assert.Nil(t, p)

			in, _, err := b.uploadInsertLog(ctx, 1, 10, iData, meta)
			assert.EqualError(t, err, errUploadToBlobStorage.Error())
			assert.Nil(t, in)

			deltas, err := b.uploadDeltaLog(ctx, 1, 10, dData, meta)
			assert.EqualError(t, err, errUploadToBlobStorage.Error())
			assert.Nil(t, deltas)
		})
	})

	t.Run("Test upload error", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "uploads", schemapb.DataType_Int64)
		dData := &DeleteData{
			Pks: []primaryKey{},
			Tss: []uint64{},
		}

		t.Run("Test upload empty insertData", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			b := &binlogIO{cm, alloc}

			iData := genEmptyInsertData()
			p, err := b.upload(context.TODO(), 1, 10, []*InsertData{iData}, dData, meta)
			assert.NoError(t, err)
			assert.Empty(t, p.inPaths)
			assert.Empty(t, p.statsPaths)
			assert.Empty(t, p.deltaInfo)

			iData = &InsertData{Data: make(map[int64]storage.FieldData)}
			p, err = b.upload(context.TODO(), 1, 10, []*InsertData{iData}, dData, meta)
			assert.NoError(t, err)
			assert.Empty(t, p.inPaths)
			assert.Empty(t, p.statsPaths)
			assert.Empty(t, p.deltaInfo)
		})

		t.Run("Test deleta data not match", func(t *testing.T) {
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
			b := &binlogIO{cm, alloc}
			iData := genInsertData()
			dData := &DeleteData{
				Pks:      []primaryKey{},
				Tss:      []uint64{1},
				RowCount: 1,
			}
			p, err := b.upload(context.TODO(), 1, 10, []*InsertData{iData}, dData, meta)
			assert.Error(t, err)
			assert.Empty(t, p)
		})

		t.Run("Test multisave error", func(t *testing.T) {
			mkc := &mockCm{errMultiSave: true}
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)
			var (
				b     = &binlogIO{mkc, alloc}
				iData = genInsertData()
				pk    = newInt64PrimaryKey(1)
				dData = &DeleteData{
					Pks:      []primaryKey{pk},
					Tss:      []uint64{1},
					RowCount: 1,
				}
			)
			ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Millisecond)
			p, err := b.upload(ctx, 1, 10, []*InsertData{iData}, dData, meta)
			assert.Error(t, err)
			assert.Empty(t, p)

			in, _, err := b.uploadInsertLog(ctx, 1, 10, iData, meta)
			assert.Error(t, err)
			assert.Empty(t, in)

			deltas, err := b.uploadDeltaLog(ctx, 1, 10, dData, meta)
			assert.Error(t, err)
			assert.Empty(t, deltas)
			cancel()
		})
	})

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
				helper, err := typeutil.CreateSchemaHelper(meta.Schema)
				assert.NoError(t, err)
				primaryKeyFieldSchema, err := helper.GetPrimaryKeyField()
				assert.NoError(t, err)
				primaryKeyFieldID := primaryKeyFieldSchema.GetFieldID()

				kvs, pin, pstats, err := b.genInsertBlobs(genInsertData(), 10, 1, meta)

				assert.NoError(t, err)
				assert.Equal(t, 1, len(pstats))
				assert.Equal(t, 12, len(pin))
				assert.Equal(t, 13, len(kvs))

				log.Debug("test paths",
					zap.Any("kvs no.", len(kvs)),
					zap.String("insert paths field0", pin[common.TimeStampField].GetBinlogs()[0].GetLogPath()),
					zap.String("stats paths field0", pstats[primaryKeyFieldID].GetBinlogs()[0].GetLogPath()))
			})
		}
	})

	t.Run("Test genInsertBlobs error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cm := storage.NewLocalChunkManager(storage.RootPath(binlogTestDir))
		defer cm.RemoveWithPrefix(ctx, cm.RootPath())

		t.Run("serialize error", func(t *testing.T) {
			bin := &binlogIO{cm, allocator.NewMockAllocator(t)}
			kvs, pin, pstats, err := bin.genInsertBlobs(genEmptyInsertData(), 10, 1, nil)

			assert.Error(t, err)
			assert.Empty(t, kvs)
			assert.Empty(t, pin)
			assert.Empty(t, pstats)
		})

		t.Run("GetGenerator error", func(t *testing.T) {
			f := &MetaFactory{}
			meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs", schemapb.DataType_Int64)

			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock GetGenerator error"))
			bin := &binlogIO{cm, alloc}
			kvs, pin, pstats, err := bin.genInsertBlobs(genInsertData(), 10, 1, meta)

			assert.Error(t, err)
			assert.Empty(t, kvs)
			assert.Empty(t, pin)
			assert.Empty(t, pstats)
		})
	})
}

type mockCm struct {
	storage.ChunkManager
	errMultiLoad bool
	errMultiSave bool
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
	return nil, nil
}

func (mk *mockCm) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	if mk.errMultiLoad {
		return nil, errors.New("mockKv multiload error")
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
