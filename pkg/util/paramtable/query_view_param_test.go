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

package paramtable

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryViewConfiguration(t *testing.T) {
	bt := NewBaseTable(SkipRemote(true), SkipEnv(true), Files([]string{}))
	t.Cleanup(bt.mgr.Close)
	params := &ComponentParam{}
	params.Init(bt)
	p := &params.QueryViewCfg
	require.Equal(t, time.Minute, p.BalancerReconcileInterval.GetAsDurationByParse())
	defaults := map[string]string{
		"autoBalance":                     "true",
		"reconcileInterval":               "1m",
		"targetRowsPerShardNode":          "100000",
		"scoring.globalWeight":            "1",
		"scoring.shardWeight":             "1",
		"scoring.collectionWeight":        "1",
		"scoring.fanoutPenaltyWeight":     "1",
		"scoring.movePrice":               "0.02",
		"scoring.loadPrice":               "0.08",
		"scoring.relativeTolerance":       "0.1",
		"scoring.localTolerance":          "0.1",
		"scoring.fanoutHysteresis":        "0.1",
		"scoring.absoluteToleranceRows":   "100000",
		"scoring.minGainRows":             "1",
		"scoring.maxCandidateEvaluations": "100000",
	}
	// Projections contain configured source/overlay values, not unset defaults.
	for key, value := range defaults {
		require.NoError(t, params.Save("queryView.balancer."+key, value))
	}
	for _, component := range []string{"querycoord", "querynode", "streamingnode", "proxy"} {
		projected := params.GetComponentConfigurations(component, "queryview")
		for key, want := range defaults {
			require.Equal(t, want, projected[strings.ToLower(strings.ReplaceAll("queryviewbalancer"+key, ".", ""))], component+": "+key)
		}
	}
	require.Empty(t, params.GetComponentConfigurations("querycoord", "queryCoord.queryView"))
	require.NoError(t, params.Save(p.BalancerReconcileInterval.Key, "250ms"))
	require.Equal(t, 250*time.Millisecond, p.BalancerReconcileInterval.GetAsDurationByParse())
	require.Equal(t, "250ms", params.GetComponentConfigurations("querycoord", "queryviewbalancerreconcileinterval")["queryviewbalancerreconcileinterval"])
}
