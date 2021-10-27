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
	"path"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBinlogIOInterfaceMethods(t *testing.T) {
	alloc := NewAllocatorFactory()
	kv := memkv.NewMemoryKV()

	b := &binlogIO{kv, alloc}
	t.Run("Test upload", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "uploads")

		iData := genInsertData()
		dData := &DeleteData{
			Data: map[int64]int64{888: 666666},
		}

		p, err := b.upload(context.TODO(), 1, 10, iData, dData, meta)
		assert.NoError(t, err)
		assert.Equal(t, 11, len(p.inPaths))
		assert.Equal(t, 3, len(p.statsPaths))
		assert.NotNil(t, p.deltaInfo.GetDeltaLogPath())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		p, err = b.upload(ctx, 1, 10, iData, dData, meta)
		assert.EqualError(t, err, errUploadToBlobStorage.Error())
		assert.Nil(t, p)
	})

	t.Run("Test download", func(t *testing.T) {
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
						blob, key, err := prepareBlob(kv, k)
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
}

func prepareBlob(kv kv.BaseKV, key string) ([]byte, string, error) {
	k := path.Join("test_prepare_blob", key)
	blob := []byte{1, 2, 3, 255, 188}

	err := kv.Save(k, string(blob[:]))
	if err != nil {
		return nil, "", err
	}
	return blob, k, nil
}

func TestBinlogIOInnerMethods(t *testing.T) {
	alloc := NewAllocatorFactory()
	b := &binlogIO{
		memkv.NewMemoryKV(),
		alloc,
	}

	t.Run("Test genDeltaBlobs", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10002), "test_gen_blobs")

		tests := []struct {
			isvalid  bool
			deletepk int64
			ts       int64

			description string
		}{
			{true, 1, 1111111, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {

					k, v, err := b.genDeltaBlobs(&DeleteData{
						Data: map[int64]int64{test.deletepk: test.ts},
					}, meta.GetID(), 10, 1)

					assert.NoError(t, err)
					assert.NotEmpty(t, k)
					assert.NotEmpty(t, v)

					log.Debug("genDeltaBlobs returns", zap.String("key", k))
				}
			})
		}

	})

	t.Run("Test genInsertBlobs", func(t *testing.T) {
		f := &MetaFactory{}
		meta := f.GetCollectionMeta(UniqueID(10001), "test_gen_blobs")

		kvs, pin, pstats, err := b.genInsertBlobs(genInsertData(), 10, 1, meta)

		assert.NoError(t, err)
		assert.Equal(t, 3, len(pstats))
		assert.Equal(t, 11, len(pin))
		assert.Equal(t, 14, len(kvs))

		log.Debug("test paths",
			zap.Any("kvs no.", len(kvs)),
			zap.String("insert paths field0", pin[0].GetBinlogs()[0]),
			zap.String("stats paths field0", pstats[0].GetBinlogs()[0]))
	})

	t.Run("Test idxGenerator", func(t *testing.T) {
		tests := []struct {
			isvalid  bool
			innumber int

			expectedNo  int
			description string
		}{
			{false, 0, 0, "Invalid input count n"},
			{true, 1, 1, "valid input n 1"},
			{true, 3, 3, "valid input n 3 with cancel"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				done := make(chan struct{})
				if test.isvalid {
					gen, err := b.idxGenerator(test.innumber, done)
					assert.NoError(t, err)

					r := make([]UniqueID, 0)
					for i := range gen {
						r = append(r, i)
					}

					assert.Equal(t, test.expectedNo, len(r))

					if test.innumber > 1 {
						donedone := make(chan struct{})
						gen, err := b.idxGenerator(test.innumber, donedone)
						assert.NoError(t, err)

						_, ok := <-gen
						assert.True(t, ok)

						donedone <- struct{}{}

						_, ok = <-gen
						assert.False(t, ok)
					}
				} else {
					gen, err := b.idxGenerator(test.innumber, done)
					assert.Error(t, err)
					assert.Nil(t, gen)
				}
			})
		}

		b.close()
	})

}
