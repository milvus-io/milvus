// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type recordingShardLeaderCacheInvalidator struct {
	collectionIDs [][]int64
}

func (i *recordingShardLeaderCacheInvalidator) InvalidateShardLeaderCache(collectionIDs []int64) {
	i.collectionIDs = append(i.collectionIDs, collectionIDs)
}

func TestInvalidateShardLeaderCacheOnQueryNodeError(t *testing.T) {
	t.Setenv("localstoragepath", t.TempDir())

	params := paramtable.Get()
	params.Reset(params.ProxyCfg.SkipInvalidateShardLeaderCacheOnTimeout.Key)
	t.Cleanup(func() {
		params.Reset(params.ProxyCfg.SkipInvalidateShardLeaderCacheOnTimeout.Key)
	})

	tests := []struct {
		name              string
		err               error
		skipTimeoutConfig bool
		invalidate        bool
	}{
		{
			name:       "context canceled with default config",
			err:        context.Canceled,
			invalidate: true,
		},
		{
			name:       "context deadline exceeded with default config",
			err:        context.DeadlineExceeded,
			invalidate: true,
		},
		{
			name:       "wrapped context deadline exceeded with default config",
			err:        errors.Wrap(context.DeadlineExceeded, "query node search failed"),
			invalidate: true,
		},
		{
			name:       "grpc canceled with default config",
			err:        status.Error(codes.Canceled, "request canceled"),
			invalidate: true,
		},
		{
			name:       "grpc deadline exceeded with default config",
			err:        status.Error(codes.DeadlineExceeded, "request deadline exceeded"),
			invalidate: true,
		},
		{
			name:       "wrapped grpc deadline exceeded with default config",
			err:        errors.Wrap(status.Error(codes.DeadlineExceeded, "request deadline exceeded"), "query node search failed"),
			invalidate: true,
		},
		{
			name:              "context canceled with skip config",
			err:               context.Canceled,
			skipTimeoutConfig: true,
		},
		{
			name:              "context deadline exceeded with skip config",
			err:               context.DeadlineExceeded,
			skipTimeoutConfig: true,
		},
		{
			name:              "wrapped context deadline exceeded with skip config",
			err:               errors.Wrap(context.DeadlineExceeded, "query node search failed"),
			skipTimeoutConfig: true,
		},
		{
			name:              "grpc canceled with skip config",
			err:               status.Error(codes.Canceled, "request canceled"),
			skipTimeoutConfig: true,
		},
		{
			name:              "grpc deadline exceeded with skip config",
			err:               status.Error(codes.DeadlineExceeded, "request deadline exceeded"),
			skipTimeoutConfig: true,
		},
		{
			name:              "wrapped grpc deadline exceeded with skip config",
			err:               errors.Wrap(status.Error(codes.DeadlineExceeded, "request deadline exceeded"), "query node search failed"),
			skipTimeoutConfig: true,
		},
		{
			name:       "other query node error",
			err:        errors.New("query node unavailable"),
			invalidate: true,
		},
		{
			name:              "other query node error with skip config",
			err:               errors.New("query node unavailable"),
			skipTimeoutConfig: true,
			invalidate:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params.Reset(params.ProxyCfg.SkipInvalidateShardLeaderCacheOnTimeout.Key)
			if test.skipTimeoutConfig {
				params.Save(params.ProxyCfg.SkipInvalidateShardLeaderCacheOnTimeout.Key, "true")
			}

			cache := &recordingShardLeaderCacheInvalidator{}
			invalidateShardLeaderCacheOnQueryNodeError(cache, 100, test.err)

			expectedCalls := 0
			if test.invalidate {
				expectedCalls = 1
			}
			if len(cache.collectionIDs) != expectedCalls {
				t.Fatalf("expected %d invalidations, got %d", expectedCalls, len(cache.collectionIDs))
			}
			if test.invalidate && cache.collectionIDs[0][0] != 100 {
				t.Fatalf("expected collection 100 to be invalidated, got %v", cache.collectionIDs[0])
			}
		})
	}
}
