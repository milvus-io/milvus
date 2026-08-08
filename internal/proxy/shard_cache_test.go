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
)

type recordingShardLeaderCacheInvalidator struct {
	collectionIDs [][]int64
}

func (i *recordingShardLeaderCacheInvalidator) InvalidateShardLeaderCache(collectionIDs []int64) {
	i.collectionIDs = append(i.collectionIDs, collectionIDs)
}

func TestInvalidateShardLeaderCacheOnQueryNodeError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		invalidate bool
	}{
		{
			name: "context canceled",
			err:  context.Canceled,
		},
		{
			name: "context deadline exceeded",
			err:  context.DeadlineExceeded,
		},
		{
			name: "wrapped context deadline exceeded",
			err:  errors.Wrap(context.DeadlineExceeded, "query node search failed"),
		},
		{
			name: "grpc canceled",
			err:  status.Error(codes.Canceled, "request canceled"),
		},
		{
			name: "grpc deadline exceeded",
			err:  status.Error(codes.DeadlineExceeded, "request deadline exceeded"),
		},
		{
			name: "wrapped grpc deadline exceeded",
			err:  errors.Wrap(status.Error(codes.DeadlineExceeded, "request deadline exceeded"), "query node search failed"),
		},
		{
			name:       "other query node error",
			err:        errors.New("query node unavailable"),
			invalidate: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
