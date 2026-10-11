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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusteringParamDefaults(t *testing.T) {
	base := NewBaseTable(Files([]string{}), SkipRemote(true), SkipEnv(true))
	defer base.mgr.Close()
	var params ComponentParam
	params.Init(base)
	cfg := &params.DataCoordCfg
	require.Equal(t, int64(8*1024*1024), cfg.ClusteringCompactionChunkSizePerCentroid.GetAsSize())
	// Preserve the existing memory-based training budget and centroid cap.
	require.Equal(t, 0.8, cfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat())
	require.Equal(t, 10240, cfg.ClusteringCompactionMaxCentroidsNum.GetAsInt())
	require.Equal(t, "KMEANS", params.KnowhereConfig.ClusterType.GetValue())
}

func TestClusteringParamNamespaces(t *testing.T) {
	base := NewBaseTable(Files([]string{}), SkipRemote(true), SkipEnv(true))
	defer base.mgr.Close()
	cfg := &knowhereConfig{}
	cfg.init(base)
	for key, value := range map[string]string{
		"knowhere.cluster.analyze.num_threads":           "8",
		"knowhere.cluster.analyze.metric_type":           "L2",
		"knowhere.cluster.compactionPlan.planner":        "ivf",
		"knowhere.cluster.compactionPlan.efConstruction": "64",
		"knowhere.IVF_FLAT.build.nlist":                  "128",
	} {
		require.NoError(t, base.Save(key, value))
	}
	analyze := cfg.GetAnalyzeParams()
	plan := cfg.GetCompactionPlanParams()
	require.Equal(t, map[string]string{"num_threads": "8", "metric_type": "L2"}, analyze)
	require.Equal(t, map[string]string{"planner": "ivf", "efConstruction": "64"}, plan)
	require.Equal(t, map[string]string{"nlist": "128"}, cfg.getIndexParam("IVF_FLAT", BuildStage))

	// A task can freeze these copies without later configuration changes
	// mutating the parameters used by a retried build.
	require.NoError(t, base.Save("knowhere.cluster.analyze.num_threads", "16"))
	require.Equal(t, "8", analyze["num_threads"])
	require.Equal(t, "16", cfg.GetAnalyzeParams()["num_threads"])
	plan["planner"] = "changed"
	require.Equal(t, "ivf", cfg.GetCompactionPlanParams()["planner"])
	require.NoError(t, base.Remove("knowhere.cluster.compactionPlan.efConstruction"))
	require.NotContains(t, cfg.GetCompactionPlanParams(), "efConstruction")
}
