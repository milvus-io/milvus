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

package metrics

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func resetFilesystemMetrics() {
	filesystemMetricsStateMu.Lock()
	defer filesystemMetricsStateMu.Unlock()

	FilesystemReadCount.Reset()
	FilesystemWriteCount.Reset()
	FilesystemReadBytes.Reset()
	FilesystemWriteBytes.Reset()
	FilesystemGetFileInfoCount.Reset()
	FilesystemFailedCount.Reset()
	FilesystemMultiPartUploadCreated.Reset()
	FilesystemMultiPartUploadFinished.Reset()
	clear(filesystemMetricsCacheKeys)
	clear(filesystemMetricsLegacyKeys)
}

func prepareFilesystemMetricsTest(t *testing.T) {
	t.Helper()
	resetFilesystemMetrics()
	SetFilesystemMetricsCollectFn(nil)
	t.Cleanup(func() {
		resetFilesystemMetrics()
		SetFilesystemMetricsCollectFn(nil)
	})
}

func TestPublishFilesystemMetricsCompatibility(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	const filesystemKey = "legacy-filesystem"
	PublishFilesystemMetrics(filesystemKey, 1, 2, 3, 4, 5, 6, 7, 8)

	registry := prometheus.NewRegistry()
	RegisterStorageMetrics(registry)

	expected := map[string]float64{
		"milvus_storage_filesystem_read_count":                 1,
		"milvus_storage_filesystem_write_count":                2,
		"milvus_storage_filesystem_read_bytes":                 3,
		"milvus_storage_filesystem_write_bytes":                4,
		"milvus_storage_filesystem_get_file_info_count":        5,
		"milvus_storage_filesystem_failed_count":               6,
		"milvus_storage_filesystem_multi_part_upload_created":  7,
		"milvus_storage_filesystem_multi_part_upload_finished": 8,
	}

	gathered, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, gathered, len(expected))
	for _, family := range gathered {
		require.Contains(t, expected, family.GetName())
		require.Len(t, family.Metric, 1)
		require.Len(t, family.Metric[0].Label, 2)
		require.Equal(t, filesystemKey, family.Metric[0].Label[0].GetValue())
		require.Equal(t, "source", family.Metric[0].Label[1].GetName())
		require.Equal(t, "origin", family.Metric[0].Label[1].GetValue())
		require.Equal(t, expected[family.GetName()], family.Metric[0].GetGauge().GetValue())
	}
}

func TestFilesystemMetricsCollector(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	const (
		localKey  = "file:///tmp/a#fs:a"
		remoteKey = "s3.example.com/bucket#fs:b"
	)
	metricsList := []FilesystemMetrics{
		{
			DisplayKey: localKey, Source: "origin", ReadCount: 1, WriteCount: 2,
			ReadBytes: 3, WriteBytes: 4, GetFileInfoCount: 5,
			FailedCount: 6, MultiPartUploadCreated: 7, MultiPartUploadFinished: 8,
		},
		{
			DisplayKey: remoteKey, Source: "origin", ReadCount: 11, WriteCount: 12,
			ReadBytes: 13, WriteBytes: 14, GetFileInfoCount: 15,
			FailedCount: 0, MultiPartUploadCreated: 17, MultiPartUploadFinished: 18,
		},
		{
			DisplayKey: remoteKey, Source: "talon", ReadCount: 2,
			ReadBytes: 32, FailedCount: 1,
		},
	}
	SetFilesystemMetricsCollectFn(func() []FilesystemMetrics { return metricsList })

	registry := prometheus.NewRegistry()
	registry.MustRegister(&filesystemMetricsCollector{})

	local := filesystemMetricsKey{fs: localKey, source: "origin"}
	origin := filesystemMetricsKey{fs: remoteKey, source: "origin"}
	talon := filesystemMetricsKey{fs: remoteKey, source: "talon"}
	expected := map[string]map[filesystemMetricsKey]float64{
		"milvus_storage_filesystem_read_count":                 {local: 1, origin: 11, talon: 2},
		"milvus_storage_filesystem_write_count":                {local: 2, origin: 12, talon: 0},
		"milvus_storage_filesystem_read_bytes":                 {local: 3, origin: 13, talon: 32},
		"milvus_storage_filesystem_write_bytes":                {local: 4, origin: 14, talon: 0},
		"milvus_storage_filesystem_get_file_info_count":        {local: 5, origin: 15, talon: 0},
		"milvus_storage_filesystem_failed_count":               {local: 6, origin: 0, talon: 1},
		"milvus_storage_filesystem_multi_part_upload_created":  {local: 7, origin: 17, talon: 0},
		"milvus_storage_filesystem_multi_part_upload_finished": {local: 8, origin: 18, talon: 0},
	}

	gathered, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, gathered, len(expected))
	for _, family := range gathered {
		values, ok := expected[family.GetName()]
		require.True(t, ok, family.GetName())
		require.Len(t, family.Metric, len(values))
		for _, metric := range family.Metric {
			var key filesystemMetricsKey
			require.Len(t, metric.Label, 2)
			for _, label := range metric.Label {
				switch label.GetName() {
				case filesystemKeyLabelName:
					key.fs = label.GetValue()
				case filesystemSourceLabelName:
					key.source = label.GetValue()
				}
			}
			require.Contains(t, values, key)
			require.Equal(t, values[key], metric.GetGauge().GetValue())
		}
	}

	// Removing one source must not remove another source for the same filesystem.
	metricsList = metricsList[:2]
	gathered, err = registry.Gather()
	require.NoError(t, err)
	for _, family := range gathered {
		require.Len(t, family.Metric, 2)
		for _, metric := range family.Metric {
			require.Equal(t, "origin", metric.Label[1].GetValue())
		}
	}

	metricsList = metricsList[:1]
	gathered, err = registry.Gather()
	require.NoError(t, err)
	for _, family := range gathered {
		require.Len(t, family.Metric, 1)
		require.Equal(t, localKey, family.Metric[0].Label[0].GetValue())
	}
}

func TestFilesystemMetricsCollectorWithoutMetrics(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	tests := []struct {
		name      string
		collectFn func() []FilesystemMetrics
	}{
		{name: "nil callback"},
		{name: "empty metrics", collectFn: func() []FilesystemMetrics { return nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			SetFilesystemMetricsCollectFn(test.collectFn)

			registry := prometheus.NewRegistry()
			registry.MustRegister(&filesystemMetricsCollector{})

			gathered, err := registry.Gather()
			require.NoError(t, err)
			require.Empty(t, gathered)
		})
	}
}

func TestFilesystemMetricsCollectorsShareCacheOwnership(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	metricsList := []FilesystemMetrics{{DisplayKey: "cache-a", Source: "origin", ReadCount: 1}}
	SetFilesystemMetricsCollectFn(func() []FilesystemMetrics { return metricsList })

	registryA := prometheus.NewRegistry()
	registryA.MustRegister(&filesystemMetricsCollector{})
	registryB := prometheus.NewRegistry()
	registryB.MustRegister(&filesystemMetricsCollector{})

	_, err := registryA.Gather()
	require.NoError(t, err)

	metricsList = []FilesystemMetrics{{DisplayKey: "cache-b", Source: "origin", ReadCount: 2}}
	gathered, err := registryB.Gather()
	require.NoError(t, err)
	for _, family := range gathered {
		require.Len(t, family.Metric, 1)
		require.Equal(t, "cache-b", family.Metric[0].Label[0].GetValue())
	}
}

func TestPublishFilesystemMetricsTakesOwnershipFromCache(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	metricsList := []FilesystemMetrics{
		{DisplayKey: "shared-key", Source: "origin", ReadCount: 1},
		{DisplayKey: "shared-key", Source: "talon", ReadCount: 2},
	}
	SetFilesystemMetricsCollectFn(func() []FilesystemMetrics { return metricsList })

	registry := prometheus.NewRegistry()
	registry.MustRegister(&filesystemMetricsCollector{})
	_, err := registry.Gather()
	require.NoError(t, err)

	PublishFilesystemMetrics("shared-key", 99, 0, 0, 0, 0, 0, 0, 0)
	metricsList = metricsList[1:]
	gathered, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range gathered {
		require.Len(t, family.Metric, 2)
		if family.GetName() == "milvus_storage_filesystem_read_count" {
			expected := map[string]float64{"origin": 99, "talon": 2}
			for _, metric := range family.Metric {
				require.Equal(t, expected[metric.Label[1].GetValue()], metric.GetGauge().GetValue())
			}
		}
	}
	metricsList = nil

	gathered, err = registry.Gather()
	require.NoError(t, err)
	var readCount float64
	for _, family := range gathered {
		require.Len(t, family.Metric, 1)
		require.Equal(t, "shared-key", family.Metric[0].Label[0].GetValue())
		if family.GetName() == "milvus_storage_filesystem_read_count" {
			readCount = family.Metric[0].GetGauge().GetValue()
		}
	}
	require.Equal(t, float64(99), readCount)
}

func TestFilesystemMetricsCollectorConcurrentRegistrationAndGather(t *testing.T) {
	prepareFilesystemMetricsTest(t)

	callbacks := []func() []FilesystemMetrics{
		func() []FilesystemMetrics {
			return []FilesystemMetrics{{DisplayKey: "fs-a", Source: "origin", ReadCount: 1}}
		},
		func() []FilesystemMetrics {
			return []FilesystemMetrics{{DisplayKey: "fs-b", Source: "talon", ReadCount: 2}}
		},
	}
	SetFilesystemMetricsCollectFn(callbacks[0])

	registries := []*prometheus.Registry{
		prometheus.NewRegistry(),
		prometheus.NewRegistry(),
	}
	for _, registry := range registries {
		registry.MustRegister(&filesystemMetricsCollector{})
	}

	const iterations = 100
	start := make(chan struct{})
	results := make(chan struct {
		err                error
		familyCount        int
		oneMetricPerFamily bool
	}, iterations*len(registries))
	var wg sync.WaitGroup
	wg.Add(1 + len(registries))
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			SetFilesystemMetricsCollectFn(callbacks[i%len(callbacks)])
		}
	}()
	for _, registry := range registries {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				gathered, err := registry.Gather()
				oneMetricPerFamily := true
				for _, family := range gathered {
					oneMetricPerFamily = oneMetricPerFamily && len(family.Metric) == 1
				}
				results <- struct {
					err                error
					familyCount        int
					oneMetricPerFamily bool
				}{err: err, familyCount: len(gathered), oneMetricPerFamily: oneMetricPerFamily}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, 8, result.familyCount)
		require.True(t, result.oneMetricPerFamily)
	}
}
