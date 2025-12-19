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
	"math"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestRetryableReader_ReadSuccess(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	expectedData := "hello world"

	mockReader := NewMockReader(expectedData)

	reader := NewRetryableReader(ctx, path, mockReader)

	buf := make([]byte, len(expectedData))
	n, err := reader.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(expectedData), n)
	assert.Equal(t, expectedData, string(buf))

	buf = make([]byte, len(expectedData))
	n, err = reader.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}

func TestRetryableReader_ReadWithRetryableError(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	expectedData := "data after retry"

	mockReader := newErrorMockReader(expectedData,
		minio.ErrorResponse{
			Code: "TooManyRequestsException",
		},
		3,
	)

	reader := NewRetryableReader(ctx, path, mockReader)
	buf := make([]byte, len(expectedData))
	n, err := reader.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, len(expectedData), n)
	assert.Equal(t, expectedData, string(buf))
}

func TestRetryableReader_ReadWithNonRetryableError(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"

	mockReader := newErrorMockReader("",
		merr.WrapErrIoFailed(path, io.ErrNoProgress),
		math.MaxInt,
	)

	reader := NewRetryableReader(ctx, path, mockReader)
	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
	assert.Equal(t, 0, n)
}
