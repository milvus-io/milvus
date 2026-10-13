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

package segments

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDiskUsageFetcherFirstSampleGate(t *testing.T) {
	paramtable.Init()
	fetcher := NewDiskUsageFetcher(context.Background())
	fetcher.path = t.TempDir()
	loader := newReportTestLoader()
	loader.duf = fetcher
	// Existing callers still see the legacy getter's initial zero value.
	usage, err := fetcher.GetDiskUsage()
	require.NoError(t, err)
	require.Zero(t, usage)
	_, err = fetcher.GetDiskUsageIfSampled()
	require.Error(t, err)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var original func(string) (int64, error)
	patch := mockey.Mock(segcore.GetLocalUsedSize).To(func(path string) (int64, error) {
		if path != fetcher.path {
			return original(path)
		}
		close(started)
		<-release
		return 0, nil // An empty directory is a valid measured zero.
	}).Origin(&original).Build()
	defer patch.UnPatch()
	go func() {
		defer close(done)
		fetcher.fetch()
	}()
	<-started
	report, err := loader.GetLoadResourceReport(context.Background())
	close(release)
	<-done
	require.Error(t, err)
	require.Nil(t, report, "an unfinished first sample must not report a measured zero")
	usage, err = fetcher.GetDiskUsageIfSampled()
	require.NoError(t, err)
	require.Zero(t, usage)
	memoryPatch := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(100)).Build()
	defer memoryPatch.UnPatch()
	report, err = loader.GetLoadResourceReport(context.Background())
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Zero(t, report.GetDiskUsageBytes())
}

func TestDiskUsageFetcherSamplingReadsLocalDirectory(t *testing.T) {
	paramtable.Init()
	path := t.TempDir()
	require.NoError(t, initcore.InitLocalChunkManager(path))
	fetcher := NewDiskUsageFetcher(context.Background())
	fetcher.path = path
	fetcher.fetch()
	usage, err := fetcher.GetDiskUsageIfSampled()
	require.NoError(t, err)
	require.Zero(t, usage)
	data := []byte("measured disk usage")
	require.NoError(t, os.WriteFile(filepath.Join(path, "data.bin"), data, 0o600))
	fetcher.fetch()
	usage, err = fetcher.GetDiskUsageIfSampled()
	require.NoError(t, err)
	require.EqualValues(t, len(data), usage)
	// Failure after a successful sample must not make the old value look fresh.
	var original func(string) (int64, error)
	patch := mockey.Mock(segcore.GetLocalUsedSize).To(func(candidate string) (int64, error) {
		if candidate != path {
			return original(candidate)
		}
		return 0, merr.WrapErrServiceUnavailableMsg("disk scan failed")
	}).Origin(&original).Build()
	defer patch.UnPatch()
	fetcher.fetch()
	_, err = fetcher.GetDiskUsageIfSampled()
	require.Error(t, err)
}
