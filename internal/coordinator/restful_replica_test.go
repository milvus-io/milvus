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

package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestHandleReplicaLoadConfigCompliance(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wal, query map[string]string
		ready      bool
		reason     string
	}{
		{name: "empty", ready: true},
		{name: "all ready", query: map[string]string{"B": "", "A": ""}, ready: true},
		{name: "query failure", query: map[string]string{"A": "", "B": "missing replica"}, reason: "missing replica"},
		{name: "WAL does not hide query failures", wal: map[string]string{"A": "WAL migrating", "B": "WAL migrating"}, query: map[string]string{"C": "not serviceable"}, reason: "WAL migrating"},
		{name: "WAL reason wins for same RG", wal: map[string]string{"B": "WAL migrating"}, query: map[string]string{"B": "not serviceable"}, reason: "WAL migrating"},
		{name: "global unknown owner", query: map[string]string{"": "unknown owner", "A": ""}, reason: "unknown owner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer mockey.Mock(balance.GetWithContext).Return(nil, nil).Build().UnPatch()
			defer mockey.Mock(balancer.CheckWALPlacement).To(func(context.Context, balancer.Balancer, string) (map[string]string, error) {
				result := make(map[string]string)
				for rg, reason := range tc.wal {
					result[rg] = reason
				}
				return result, nil
			}).Build().UnPatch()
			calls := 0
			defer mockey.Mock((*querycoordv2.Server).GetLoadConfigCompliance).To(func(*querycoordv2.Server, context.Context) (map[string]string, error) {
				calls++
				return tc.query, nil
			}).Build().UnPatch()
			coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
			var summary LoadConfigComplianceResponse
			for _, output := range []string{"", "summary", "per_resource_group"} {
				req := httptest.NewRequest(http.MethodGet, "/management/replica/loadconfig/compliance?output="+output, nil)
				w := httptest.NewRecorder()
				coord.HandleReplicaLoadConfigCompliance(w, req)
				require.Equal(t, http.StatusOK, w.Code)
				var resp LoadConfigComplianceResponse
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				require.Equal(t, tc.ready, resp.State == LoadConfigComplianceStateReady)
				require.Equal(t, tc.reason, resp.Reason)
				if output != "per_resource_group" {
					require.Nil(t, resp.ResourceGroups)
					summary = resp
					continue
				}
				require.Equal(t, summary.State, resp.State)
				require.NotNil(t, resp.ResourceGroups)
				expected := make(map[string]string)
				for rg, reason := range tc.wal {
					expected[rg] = reason
				}
				for rg, reason := range tc.query {
					if expected[rg] == "" {
						expected[rg] = reason
					}
				}
				delete(expected, "")
				require.Len(t, *resp.ResourceGroups, len(expected))
				previous := ""
				for _, rg := range *resp.ResourceGroups {
					require.Greater(t, rg.ResourceGroup, previous)
					previous = rg.ResourceGroup
					require.Equal(t, expected[rg.ResourceGroup], rg.Reason)
					require.Equal(t, rg.Reason == "", rg.State == LoadConfigComplianceStateReady)
				}
			}
			require.Equal(t, 3, calls)
		})
	}
}

func TestComplianceRequestErrors(t *testing.T) {
	for _, tc := range []struct {
		method, output string
		code           int
	}{
		{http.MethodPost, "", http.StatusMethodNotAllowed},
		{http.MethodGet, "typo", http.StatusBadRequest},
	} {
		coord := &mixCoordImpl{}
		w := httptest.NewRecorder()
		coord.HandleReplicaLoadConfigCompliance(w, httptest.NewRequest(tc.method, "/?output="+tc.output, nil))
		require.Equal(t, tc.code, w.Code)
	}
	for _, stage := range []string{"balancer", "WAL", "query"} {
		t.Run(stage, func(t *testing.T) {
			failure := merr.WrapErrServiceUnavailableMsg("metadata unavailable")
			var balancerErr, walErr, queryErr error
			switch stage {
			case "balancer":
				balancerErr = failure
			case "WAL":
				walErr = failure
			case "query":
				queryErr = failure
			}
			defer mockey.Mock(balance.GetWithContext).Return(nil, balancerErr).Build().UnPatch()
			defer mockey.Mock(balancer.CheckWALPlacement).Return(map[string]string{}, walErr).Build().UnPatch()
			defer mockey.Mock((*querycoordv2.Server).GetLoadConfigCompliance).Return(map[string]string{}, queryErr).Build().UnPatch()
			coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
			w := httptest.NewRecorder()
			coord.HandleReplicaLoadConfigCompliance(w, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, http.StatusInternalServerError, w.Code)
		})
	}
}
