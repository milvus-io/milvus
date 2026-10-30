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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDQLViewRefresh(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	for _, autoLoad := range []string{"false", "true"} {
		require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, autoLoad))
		for _, tc := range []struct {
			name             string
			params           []*commonpb.KeyValuePair
			failures         []error
			calls, refreshes int
		}{
			{name: "retired schema and expired requery", failures: []error{
				viewerror.NewViewNotFound("old schema retired"),
				merr.Wrap(viewerror.ConvertViewError("QueryOnView", viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("requery expired")).Err()), "requery"),
			}, calls: 3, refreshes: 2},
			{name: "bounded", failures: []error{viewerror.NewViewNotFound("missing"), viewerror.NewViewNotFound("missing"), viewerror.NewViewNotFound("missing"), viewerror.NewViewNotFound("missing")}, calls: 4, refreshes: 3},
			{name: "iterator keeps binding", params: []*commonpb.KeyValuePair{{Key: IteratorField, Value: "True"}}, failures: []error{viewerror.NewViewInvalidated("expired")}, calls: 1},

			{name: "removed field fails after refresh", failures: []error{viewerror.NewViewNotFound("old schema retired"), merr.WrapErrParameterInvalidMsg("field was removed")}, calls: 2, refreshes: 1},
			{name: "canceled", failures: []error{context.Canceled}, calls: 1},
			{name: "input error", failures: []error{merr.WrapErrAsInputError(viewerror.NewViewNotFound("invalid input"))}, calls: 1},
		} {
			t.Run(autoLoad+"/"+tc.name, func(t *testing.T) {
				node := &Proxy{metaCache: &metacache.MetaCache{}}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				calls, refreshes := 0, 0
				patch := mockey.Mock((*metacache.MetaCache).RemoveCollection).To(func(_ *metacache.MetaCache, ctx context.Context, db, collection string) {
					require.NoError(t, ctx.Err())
					require.Equal(t, "db", db)
					require.Equal(t, "collection", collection)
					refreshes++
				}).Build()
				defer patch.UnPatch()
				err := node.retryDQL(context.Background(), "db", "collection", tc.params, func(context.Context) (bool, error) {
					calls++
					if calls <= len(tc.failures) {
						return false, tc.failures[calls-1]
					}
					return false, nil
				})
				require.Equal(t, tc.calls, calls)
				require.Equal(t, tc.refreshes, refreshes)
				if tc.calls > len(tc.failures) {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.failures[tc.calls-1])
				}
			})
		}
	}
}
