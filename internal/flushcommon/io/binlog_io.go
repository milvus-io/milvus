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
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type BinlogIO interface {
	Download(ctx context.Context, paths []string) ([][]byte, error)
	AsyncDownload(ctx context.Context, paths []string) []*conc.Future[any]
	Upload(ctx context.Context, kvs map[string][]byte) error
	AsyncUpload(ctx context.Context, kvs map[string][]byte) []*conc.Future[any]
}

type BinlogIoImpl struct {
	storage.ChunkManager
	pool *conc.Pool[any]
}

func NewBinlogIO(cm storage.ChunkManager) BinlogIO {
	return &BinlogIoImpl{cm, GetOrCreateIOPool()}
}

func (b *BinlogIoImpl) Download(ctx context.Context, paths []string) ([][]byte, error) {
	futures := b.AsyncDownload(ctx, paths)
	err := conc.AwaitAll(futures...)
	if err != nil {
		return nil, err
	}

	return lo.Map(futures, func(future *conc.Future[any], _ int) []byte {
		return future.Value().([]byte)
	}), nil
}

func (b *BinlogIoImpl) AsyncDownload(ctx context.Context, paths []string) []*conc.Future[any] {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "Download")
	defer span.End()

	futures := make([]*conc.Future[any], 0, len(paths))
	for _, path := range paths {
		path := path
		future := b.pool.Submit(func() (any, error) {
			var val []byte
			var err error

			start := time.Now()
			log.Ctx(ctx).Debug("BinlogIO download", zap.String("path", path))
			err = retry.Do(ctx, func() error {
				val, err = b.Read(ctx, path)
				if err != nil {
					log.Warn("BinlogIO fail to download", zap.String("path", path), zap.Error(err))
				}
				return err
			})

			log.Ctx(ctx).Debug("BinlogIO download success", zap.String("path", path), zap.Int64("cost", time.Since(start).Milliseconds()),
				zap.Error(err))

			return val, err
		})
		futures = append(futures, future)
	}

	return futures
}

func (b *BinlogIoImpl) Upload(ctx context.Context, kvs map[string][]byte) error {
	futures := b.AsyncUpload(ctx, kvs)
	return conc.AwaitAll(futures...)
}

func (b *BinlogIoImpl) AsyncUpload(ctx context.Context, kvs map[string][]byte) []*conc.Future[any] {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "Upload")
	defer span.End()

	futures := make([]*conc.Future[any], 0, len(kvs))
	for k, v := range kvs {
		innerK, innerV := k, v
		future := b.pool.Submit(func() (any, error) {
			var err error
			start := time.Now()
			log.Ctx(ctx).Debug("BinlogIO upload", zap.String("paths", innerK))
			err = retry.Do(ctx, func() error {
				err = b.Write(ctx, innerK, innerV)
				if err != nil {
					log.Warn("BinlogIO fail to upload", zap.String("paths", innerK), zap.Error(err))
				}
				return err
			})
			log.Ctx(ctx).Debug("BinlogIO upload success", zap.String("paths", innerK), zap.Int64("cost", time.Since(start).Milliseconds()), zap.Error(err))
			return struct{}{}, err
		})
		futures = append(futures, future)
	}

	return futures
}
