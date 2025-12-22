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

package common

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

// RetryableReader is a wrapper around a FileReader that retries reads on errors.
type RetryableReader interface {
	storage.FileReader
}

// retryableReader is the implementation of RetryableReader.
type retryableReader struct {
	storage.FileReader
	ctx           context.Context
	path          string
	retryAttempts uint
}

// NewRetryableReader creates a new RetryableReader.
func NewRetryableReader(ctx context.Context, path string, reader storage.FileReader) RetryableReader {
	return &retryableReader{
		FileReader:    reader,
		ctx:           ctx,
		path:          path,
		retryAttempts: paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint(),
	}
}

// Read reads from the underlying FileReader and retries on errors.
func (r *retryableReader) Read(p []byte) (int, error) {
	var n int
	var err error
	err = retry.Handle(r.ctx, func() (bool, error) {
		n, err = r.FileReader.Read(p)
		if err == nil {
			return false, nil
		}
		if errors.Is(err, io.EOF) {
			return false, err
		}
		log.Ctx(r.ctx).Warn("retryable reader read failed",
			zap.String("path", r.path),
			zap.Error(err),
		)
		err = storage.ToMilvusIoError(r.path, err)
		if merr.IsRetryableErr(err) {
			return true, err
		}
		return false, err
	}, retry.Attempts(r.retryAttempts))
	return n, err
}
