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

package io

import (
	"context"
	"path"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type BinlogIO interface {
	Download(ctx context.Context, paths []string) ([][]byte, error)
	Upload(ctx context.Context, kvs map[string][]byte) error
	// JoinFullPath returns the full path by join the paths with the chunkmanager's rootpath
	JoinFullPath(paths ...string) string
}

type BinlogIoImpl struct {
	storage.ChunkManager
	pool *conc.Pool[any]
}

func NewBinlogIO(cm storage.ChunkManager, ioPool *conc.Pool[any]) BinlogIO {
	return &BinlogIoImpl{cm, ioPool}
}

func (b *BinlogIoImpl) Download(ctx context.Context, paths []string) ([][]byte, error) {
	future := b.pool.Submit(func() (any, error) {
		var vs [][]byte
		var err error

		err = retry.Do(ctx, func() error {
			vs, err = b.MultiRead(ctx, paths)
			return err
		})

		return vs, err
	})

	vs, err := future.Await()
	if err != nil {
		return nil, err
	}

	return vs.([][]byte), nil
}

func (b *BinlogIoImpl) Upload(ctx context.Context, kvs map[string][]byte) error {
	future := b.pool.Submit(func() (any, error) {
		err := retry.Do(ctx, func() error {
			return b.MultiWrite(ctx, kvs)
		})

		return nil, err
	})

	_, err := future.Await()
	return err
}

func (b *BinlogIoImpl) JoinFullPath(paths ...string) string {
	return path.Join(b.ChunkManager.RootPath(), path.Join(paths...))
}
