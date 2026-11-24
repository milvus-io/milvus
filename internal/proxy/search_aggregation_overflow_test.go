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
	"math"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchAggregateOverflowStopsRetry(t *testing.T) {
	// Use the real aggregate finalizer to produce the error consumed by the
	// public Search handler, while isolating the retry policy from shard IO.
	sum := &agg.SumAggregate{}
	state := sum.NewState()
	require.NoError(t, sum.UpdateState(state, agg.NewFieldValue(int64(math.MaxInt64))))
	require.NoError(t, sum.UpdateState(state, agg.NewFieldValue(int64(1))))
	value, overflowErr := sum.Terminate(state)
	require.Nil(t, value)
	require.ErrorIs(t, overflowErr, merr.ErrAggregateOverflow)
	overflowErr = merr.Wrap(overflowErr, `metric "sum_v"`)

	autoLoadKey := Params.ProxyCfg.EnableAutoLoad.Key
	previousAutoLoad := Params.ProxyCfg.EnableAutoLoad.GetValue()
	limitCheckKey := Params.AutoIndexConfig.EnableResultLimitCheck.Key
	previousLimitCheck := Params.AutoIndexConfig.EnableResultLimitCheck.GetValue()
	t.Cleanup(func() {
		require.NoError(t, Params.Save(autoLoadKey, previousAutoLoad))
		require.NoError(t, Params.Save(limitCheckKey, previousLimitCheck))
	})
	require.NoError(t, Params.Save(limitCheckKey, "true"))

	for _, autoLoad := range []string{"false", "true"} {
		for _, delivery := range []string{"go_error", "status_only"} {
			t.Run("auto_load_"+autoLoad+"/"+delivery, func(t *testing.T) {
				require.NoError(t, Params.Save(autoLoadKey, autoLoad))
				node := &Proxy{metaCache: &metacache.MetaCache{}}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				readinessCalls, searchCalls, refreshes := 0, 0, 0
				ready := mockey.Mock((*Proxy).ensureCollectionReady).To(func(*Proxy, context.Context, string, string) error {
					readinessCalls++
					return nil
				}).Build()
				defer ready.UnPatch()
				refresh := mockey.Mock((*metacache.MetaCache).RemoveCollection).To(func(*metacache.MetaCache, context.Context, string, string) {
					refreshes++
				}).Build()
				defer refresh.UnPatch()
				search := mockey.Mock((*Proxy).search).To(func(*Proxy, context.Context, *milvuspb.SearchRequest, bool, bool, bool) (*milvuspb.SearchResults, bool, bool, bool, error) {
					searchCalls++
					var err error
					if delivery == "go_error" {
						err = overflowErr
					}
					// Even if fallback/recall flags are set, the failed status must
					// prevent any additional Search execution.
					return &milvuspb.SearchResults{Status: merr.Status(overflowErr)}, true, true, true, err
				}).Build()
				defer search.UnPatch()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				response, err := node.Search(ctx, &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection"})
				require.NoError(t, err)
				require.NotNil(t, response)
				require.Equal(t, 1, searchCalls)
				require.Equal(t, 1, readinessCalls)
				require.Zero(t, refreshes)
				require.Nil(t, response.GetResults())
				status := response.GetStatus()
				require.EqualValues(t, 2203, status.GetCode())
				require.False(t, status.GetRetriable())
				require.Contains(t, status.GetReason(), `metric "sum_v"`)
				restored := merr.Error(status)
				require.ErrorIs(t, restored, merr.ErrAggregateOverflow)
				require.Equal(t, merr.SystemError, merr.GetErrorType(restored))
			})
		}
	}
}
