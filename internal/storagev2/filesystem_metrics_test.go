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

package storagev2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestGetFilesystemMetricsWithConfig(t *testing.T) {
	dir := t.TempDir()
	localConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    dir,
	}
	metrics, err := GetFilesystemMetricsWithConfig(localConfig)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.GreaterOrEqual(t, metrics.ReadCount, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteCount, int64(0))
	assert.GreaterOrEqual(t, metrics.ReadBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.GetFileInfoCount, int64(0))
	assert.GreaterOrEqual(t, metrics.FailedCount, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadCreated, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadFinished, int64(0))

	// makePropertiesFromConfig omits fs.max_connections when the producer left
	// it at 0, so milvus-storage keeps its registered default instead of being
	// handed an explicit "0" that lowers the connection cap. Drive both sides
	// of that branch through the public entry point.
	for _, maxConns := range []uint32{0, 64} {
		cfg := &indexpb.StorageConfig{
			StorageType:    "local",
			RootPath:       dir,
			MaxConnections: maxConns,
		}
		m, err := GetFilesystemMetricsWithConfig(cfg)
		require.NoError(t, err, "MaxConnections=%d must build valid properties", maxConns)
		require.NotNil(t, m)
	}
}

func TestListFilesystemMetrics(t *testing.T) {
	metricsBefore, err := ListFilesystemMetrics()
	require.NoError(t, err)
	existingDisplayKeys := make(map[string]struct{}, len(metricsBefore))
	for _, fsMetrics := range metricsBefore {
		existingDisplayKeys[fsMetrics.DisplayKey] = struct{}{}
	}

	dirA := t.TempDir()
	dirB := t.TempDir()
	expectedDisplayPrefixes := map[string]struct{}{
		"file://" + dirA + "#fs:": {},
		"file://" + dirB + "#fs:": {},
	}
	for _, dir := range []string{dirA, dirB} {
		_, err := GetFilesystemMetricsWithConfig(&indexpb.StorageConfig{
			StorageType: "local",
			RootPath:    dir,
		})
		require.NoError(t, err)
	}

	metricsList, err := ListFilesystemMetrics()
	require.NoError(t, err)

	newFilesystemCount := 0
	for _, fsMetrics := range metricsList {
		if _, ok := existingDisplayKeys[fsMetrics.DisplayKey]; ok {
			continue
		}
		newFilesystemCount++
		matched := false
		for prefix := range expectedDisplayPrefixes {
			if strings.HasPrefix(fsMetrics.DisplayKey, prefix) {
				delete(expectedDisplayPrefixes, prefix)
				matched = true
				break
			}
		}
		assert.True(t, matched, fsMetrics.DisplayKey)
		assert.GreaterOrEqual(t, fsMetrics.ReadCount, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.WriteCount, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.ReadBytes, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.WriteBytes, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.GetFileInfoCount, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.FailedCount, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.MultiPartUploadCreated, int64(0))
		assert.GreaterOrEqual(t, fsMetrics.MultiPartUploadFinished, int64(0))
	}
	require.Equal(t, 2, newFilesystemCount)
	require.Empty(t, expectedDisplayPrefixes)
}

func TestNilConfig(t *testing.T) {
	_, err := GetFilesystemMetricsWithConfig(nil)
	assert.Error(t, err)
}
