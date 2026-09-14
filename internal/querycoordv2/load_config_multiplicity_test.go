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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLoadConfigWatcherResourceGroupMultiplicity(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryCoordCfg
	for key, value := range map[string]string{
		cfg.ClusterLevelLoadReplicaNumber.Key:                "3",
		cfg.ClusterLevelLoadResourceGroups.Key:               "A,B,B",
		cfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key: "false",
	} {
		require.NoError(t, paramtable.Get().Save(key, value))
		t.Cleanup(func() { paramtable.Get().Reset(key) })
	}
	w := &LoadConfigWatcher{
		s:                  &Server{meta: meta.NewMeta(nil, nil, nil)},
		notifier:           syncutil.NewAsyncTaskNotifier[struct{}](),
		previousReplicaNum: 3,
		previousRGs:        []string{"A", "A", "B"},
	}
	calls := 0
	defer mockey.Mock((*Server).updateLoadConfig).To(func(_ *Server, _ context.Context, _ []int64, count int32, groups []string, _ ...bool) error {
		calls++
		require.Equal(t, int32(3), count)
		require.Equal(t, []string{"A", "B", "B"}, groups)
		return nil
	}).Build().UnPatch()
	require.NoError(t, w.applyLoadConfigChanges())
	require.Equal(t, 1, calls)
	require.NoError(t, paramtable.Get().Save(cfg.ClusterLevelLoadResourceGroups.Key, "B,A,B"))
	require.NoError(t, w.applyLoadConfigChanges())
	require.Equal(t, 1, calls, "reordering alone must not schedule another update")
}

func TestUpdateLoadConfigResourceGroupMultiplicity(t *testing.T) {
	for _, tc := range []struct {
		name              string
		actual, requested []string
		count             int32
		jobs              int
	}{
		{name: "redistribute duplicate groups", actual: []string{"A", "A", "B"}, requested: []string{"A", "B", "B"}, count: 3, jobs: 1},
		{name: "single group means all replicas", actual: []string{"A", "A"}, requested: []string{"A"}, count: 2},
		{name: "same multiset", actual: []string{"A", "A", "B"}, requested: []string{"B", "A", "A"}, count: 3},
		{name: "zero count preserves current total", actual: []string{"A", "A", "B"}, requested: []string{"A", "B", "B"}, jobs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{meta: meta.NewMeta(nil, nil, nil), jobScheduler: &job.Scheduler{}}
			c := &meta.Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 100, ReplicaNumber: int32(len(tc.actual))}}
			defer mockey.Mock((*meta.CollectionManager).GetCollection).Return(c).Build().UnPatch()
			replicas := make([]*meta.Replica, 0, len(tc.actual))
			for i, rg := range tc.actual {
				replicas = append(replicas, meta.NewReplica(&querypb.Replica{ID: int64(i + 1), CollectionID: 100, ResourceGroup: rg}, typeutil.NewUniqueSet()))
			}
			defer mockey.Mock((*meta.ReplicaManager).GetByCollection).Return(replicas).Build().UnPatch()
			added := 0
			defer mockey.Mock((*job.Scheduler).Add).To(func(_ *job.Scheduler, _ job.Job) { added++ }).Build().UnPatch()
			defer mockey.Mock((*job.BaseJob).Wait).Return(nil).Build().UnPatch()
			require.NoError(t, s.updateLoadConfig(context.Background(), []int64{100}, tc.count, tc.requested))
			require.Equal(t, tc.jobs, added)
		})
	}
}
