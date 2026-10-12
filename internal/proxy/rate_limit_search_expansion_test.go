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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

type expansionInterceptorLimiter struct {
	delegate *SimpleLimiter
	tokens   []int
	parts    []map[int64][]int64
	dbIDs    []int64
}

func (l *expansionInterceptorLimiter) Check(dbID int64, parts map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	l.tokens, l.parts, l.dbIDs = append(l.tokens, tokens), append(l.parts, parts), append(l.dbIDs, dbID)
	return l.delegate.Check(dbID, parts, rateType, tokens)
}

func (l *expansionInterceptorLimiter) Alloc(_ context.Context, dbID int64, parts map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	return l.Check(dbID, parts, rateType, tokens)
}

func TestRateLimitInterceptorPreparedSearchUsesSameLimiterAndIdentity(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	previous := params.QuotaConfig.QuotaAndLimitsEnabled.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, previous)) })
	for _, request := range []proto.Message{
		&milvuspb.SearchRequest{DbName: "db1", CollectionName: "collection", PartitionNames: []string{"partition"}, Nq: 1},
		&milvuspb.HybridSearchRequest{DbName: "db1", CollectionName: "collection", PartitionNames: []string{"partition"}, Requests: []*milvuspb.SearchRequest{{Nq: 1}, {Nq: 1}}},
	} {
		for _, mode := range []string{"enabled", "disabled", "initial rejection", "supplemental rejection"} {
			t.Run(string(proto.MessageName(request))+"/"+mode, func(t *testing.T) {
				require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
				if mode == "disabled" {
					require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "false"))
				}
				cache := NewMockCache(t)
				cache.EXPECT().GetDatabaseInfo(mock.Anything, "db1").Return(&databaseInfo{DBID: 100}, nil).Once()
				cache.EXPECT().GetCollectionID(mock.Anything, "db1", "collection").Return(int64(101), nil).Once()
				cache.EXPECT().GetPartitionInfo(mock.Anything, "db1", "collection", "partition").Return(&partitionInfo{PartitionID: 201}, nil).Once()
				limiter := &expansionInterceptorLimiter{delegate: NewSimpleLimiter(0, 0)}
				deny := func() {
					limiter.delegate.rateLimiter.GetRootLimiters().GetLimiters().Insert(internalpb.RateType_DQLSearch, ratelimitutil.NewLimiter(0, 0))
				}
				if mode == "initial rejection" || mode == "disabled" {
					deny()
				}
				handlerCalled := false
				interceptor := RateLimitInterceptorWithMetaCache(func() Cache { return cache }, limiter)
				response, err := interceptor(context.Background(), request, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ interface{}) (interface{}, error) {
					handlerCalled = true
					if mode == "supplemental rejection" {
						deny()
					}
					err := taskmodel.CheckPreparedSearchRateLimit(ctx, 5)
					if err == nil {
						require.NoError(t, taskmodel.CheckPreparedSearchRateLimit(ctx, 5), "successful reservation survives task reentry")
					}
					return &milvuspb.SearchResults{Status: merr.Status(err)}, nil
				})
				_, hybrid := request.(*milvuspb.HybridSearchRequest)
				if mode == "initial rejection" && hybrid {
					// Preserve the existing interceptor projection: HybridSearch
					// initial rejection returns an error instead of a response.
					require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
					require.Nil(t, response)
				} else {
					require.NoError(t, err)
					status := response.(*milvuspb.SearchResults).GetStatus()
					if mode == "initial rejection" || mode == "supplemental rejection" {
						require.Equal(t, merr.Code(merr.ErrServiceQuotaExceeded), status.GetCode())
					} else {
						require.NoError(t, merr.Error(status))
					}
				}
				require.Equal(t, mode != "initial rejection", handlerCalled)
				base := 1
				if hybrid {
					base = 2
				}
				if mode == "initial rejection" {
					require.Equal(t, []int{base}, limiter.tokens)
				} else {
					require.Equal(t, []int{base, 5 - base}, limiter.tokens)
				}
				for index := range limiter.tokens {
					require.Equal(t, int64(100), limiter.dbIDs[index])
					require.Equal(t, map[int64][]int64{101: {201}}, limiter.parts[index])
				}
			})
		}
	}
}
