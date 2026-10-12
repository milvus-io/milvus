// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskmodel

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type searchRateLimitContextKey struct{}

// searchRateLimit retains the identity and successful token reservation made
// by the gRPC interceptor. A PRE chain can increase the search workload, but
// must not resolve a different collection/partition snapshot or charge the
// original tokens again. The watermark is shared across tasks in one RPC.
type searchRateLimit struct {
	mu         sync.Mutex
	limiter    types.Limiter
	dbID       int64
	partitions map[int64][]int64
	charged    int
}

// WithSearchRateLimit is called only after the interceptor's initial search
// check succeeds. Clone IDs so subsequent metadata changes cannot redirect the
// supplemental reservation. The same limiter retains its enabled/quota policy.
func WithSearchRateLimit(ctx context.Context, limiter types.Limiter, dbID int64, partitions map[int64][]int64, charged int) context.Context {
	owned := make(map[int64][]int64, len(partitions))
	for collectionID, ids := range partitions {
		owned[collectionID] = append([]int64(nil), ids...)
	}
	return context.WithValue(ctx, searchRateLimitContextKey{}, &searchRateLimit{
		limiter: limiter, dbID: dbID, partitions: owned, charged: charged,
	})
}

// CheckPreparedSearchRateLimit reserves only the extra prepared search vectors.
// Shrinking does not refund the original request reservation. Failed checks do
// not advance the watermark; a retry after success does not double charge.
// Calls without an interceptor snapshot keep their existing internal behavior.
func CheckPreparedSearchRateLimit(ctx context.Context, prepared int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared < 0 {
		return merr.WrapErrServiceInternalMsg("prepared search rate-limit token count must not be negative")
	}
	reservation, _ := ctx.Value(searchRateLimitContextKey{}).(*searchRateLimit)
	if reservation == nil {
		return nil
	}
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared <= reservation.charged {
		return nil
	}
	if reservation.limiter == nil || reservation.charged < 0 {
		return merr.WrapErrServiceInternalMsg("search interceptor rate-limit reservation is malformed")
	}
	if err := reservation.limiter.Check(reservation.dbID, reservation.partitions, internalpb.RateType_DQLSearch, prepared-reservation.charged); err != nil {
		return err
	}
	reservation.charged = prepared
	return nil
}
