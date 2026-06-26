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

package numpy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// End-to-end wiring test: drive the real numpy reader.Size() (which goes through the
// exported, param-driven importcommon.GetFilesSizeWithRetry) and inject the customer's
// exact transient object-store error on the first two StatObject/HEAD calls. Before the
// fix this returned the error on the first attempt and failed the whole pre-import.
func TestReaderSize_RetriesTransientHeadTimeout(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	calls := 0
	cm.EXPECT().Size(mock.Anything, "f.npy").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			calls++
			if calls < 3 {
				return 0, errors.New("net/http: timeout awaiting response headers")
			}
			return 123, nil
		})

	r := &reader{ctx: ctx, cm: cm, paths: []string{"f.npy"}, fileSize: atomic.NewInt64(0)}

	size, err := r.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(123), size)
	assert.Equal(t, 3, calls, "reader.Size must retry the transient HEAD timeout, not fail on the first attempt")

	// Second call is served from the cache, no further StatObject.
	size2, err := r.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(123), size2)
	assert.Equal(t, 3, calls, "cached size must not re-stat")
}

// A permanent error (permission denied) must still fail fast through the real reader.
func TestReaderSize_PermanentErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	calls := 0
	cm.EXPECT().Size(mock.Anything, "f.npy").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			calls++
			return 0, merr.WrapErrIoPermissionDenied("f.npy", errors.New("access denied"))
		}).Once()

	r := &reader{ctx: ctx, cm: cm, paths: []string{"f.npy"}, fileSize: atomic.NewInt64(0)}

	_, err := r.Size()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, calls, "permanent error must not be retried")
}
