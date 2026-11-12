// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"path"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func manifestHistogram(t *testing.T, operation, phase, result string) *dto.Histogram {
	t.Helper()
	observer, err := metrics.QueryStageDuration.GetMetricWithLabelValues("storage", operation, phase, result)
	require.NoError(t, err)
	var metric dto.Metric
	require.NoError(t, observer.(prometheus.Metric).Write(&metric))
	return metric.GetHistogram()
}

func TestManifestReadMetricsRecordEveryPhaseOnError(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	for _, manifestPath := range []string{"invalid path", MarshalManifestPath("missing/segment", 1)} {
		before := make(map[string]*dto.Histogram)
		for _, phase := range manifestReadPhaseNames {
			before[phase] = manifestHistogram(t, "manifest_create", phase, "error")
		}
		_, err := getManifestStats(manifestPath, cfg, ManifestReadCreate)
		require.Error(t, err)
		for _, phase := range manifestReadPhaseNames {
			after := manifestHistogram(t, "manifest_create", phase, "error")
			require.Equal(t, before[phase].GetSampleCount()+1, after.GetSampleCount(), phase)
			if phase == "get_manifest" || phase == "extract_stats" || manifestPath == "invalid path" && phase != "total" {
				require.Equal(t, before[phase].GetSampleSum(), after.GetSampleSum(), phase)
			}
		}
	}
}

func TestManifestResolverOriginsAndLocalCache(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	manifestPath, err := CommitManifestUpdates(MarshalManifestPath(path.Join(cfg.RootPath, "metrics/segment"), ManifestEarliest), cfg, &ManifestUpdates{
		Stats: []StatEntry{{Key: "bloom_filter.100", Files: []string{path.Join(cfg.RootPath, "metrics/bf")}}},
	})
	require.NoError(t, err)
	for _, tc := range []struct {
		origin    ManifestReadOrigin
		operation string
	}{
		{ManifestReadOther, "manifest_other"},
		{ManifestReadCreate, "manifest_create"},
		{ManifestReadPreload, "manifest_preload"},
		{ManifestReadPostSync, "manifest_postsync"},
		{ManifestReadReopen, "manifest_reopen"},
	} {
		before := make(map[string]*dto.Histogram)
		for _, phase := range manifestReadPhaseNames {
			before[phase] = manifestHistogram(t, tc.operation, phase, "success")
		}
		resolver := NewStatsResolver(manifestPath, cfg).WithManifestReadOrigin(tc.origin)
		require.NoError(t, resolver.loadManifest())
		require.NoError(t, resolver.loadManifest())
		require.Contains(t, resolver.manifestStats, "bloom_filter.100")
		for _, phase := range manifestReadPhaseNames {
			after := manifestHistogram(t, tc.operation, phase, "success")
			require.Equal(t, before[phase].GetSampleCount()+1, after.GetSampleCount(), "resolver's second lookup must not issue another FFI read: "+phase)
		}
	}
}
