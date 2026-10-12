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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type preparedSearchLimiter struct {
	checks     []int
	dbID       int64
	partitions map[int64][]int64
	rateType   internalpb.RateType
	err        error
}

func (l *preparedSearchLimiter) Check(dbID int64, partitions map[int64][]int64, rateType internalpb.RateType, n int) error {
	l.checks = append(l.checks, n)
	l.dbID, l.partitions, l.rateType = dbID, partitions, rateType
	return l.err
}

func (l *preparedSearchLimiter) Alloc(_ context.Context, dbID int64, partitions map[int64][]int64, rateType internalpb.RateType, n int) error {
	return l.Check(dbID, partitions, rateType, n)
}

func TestPreparedSearchRateLimitSnapshotAndWatermark(t *testing.T) {
	limiter := &preparedSearchLimiter{}
	partitions := map[int64][]int64{101: {201, 202}}
	ctx := WithSearchRateLimit(context.Background(), limiter, 11, partitions, 2)
	partitions[101][0], partitions[102] = 999, []int64{998}
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 4))
	require.Equal(t, int64(11), limiter.dbID)
	require.Equal(t, map[int64][]int64{101: {201, 202}}, limiter.partitions)
	require.Equal(t, internalpb.RateType_DQLSearch, limiter.rateType)
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 4))
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 1), "shrinking does not refund or charge")
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 5))
	require.Equal(t, []int{2, 1}, limiter.checks)
}

func TestPreparedSearchRateLimitFailureRetainsCodeAndCanRetry(t *testing.T) {
	limiter := &preparedSearchLimiter{err: merr.ErrServiceQuotaExceeded}
	ctx := WithSearchRateLimit(context.Background(), limiter, 11, nil, 2)
	err := CheckPreparedSearchRateLimit(ctx, 5)
	require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
	require.Equal(t, merr.Code(merr.ErrServiceQuotaExceeded), merr.Status(err).GetCode())
	limiter.err = nil
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 5))
	require.NoError(t, CheckPreparedSearchRateLimit(ctx, 5))
	require.Equal(t, []int{3, 3}, limiter.checks, "only a successful check advances the watermark")
}

func TestPreparedSearchRateLimitConcurrentRetryChargesOnce(t *testing.T) {
	limiter := &preparedSearchLimiter{}
	ctx := WithSearchRateLimit(context.Background(), limiter, 11, nil, 2)
	var group sync.WaitGroup
	errors := make(chan error, 16)
	for index := 0; index < cap(errors); index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errors <- CheckPreparedSearchRateLimit(ctx, 5)
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, []int{3}, limiter.checks)
}

func TestPreparedSearchRateLimitInternalAndCanceledCalls(t *testing.T) {
	require.NoError(t, CheckPreparedSearchRateLimit(context.Background(), 5))
	limiter := &preparedSearchLimiter{}
	ctx, cancel := context.WithCancel(WithSearchRateLimit(context.Background(), limiter, 11, nil, 2))
	cancel()
	require.ErrorIs(t, CheckPreparedSearchRateLimit(ctx, 5), context.Canceled)
	require.Empty(t, limiter.checks)
	require.ErrorIs(t, CheckPreparedSearchRateLimit(context.Background(), -1), merr.ErrServiceInternal)
}
