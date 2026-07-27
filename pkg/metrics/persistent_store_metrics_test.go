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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestFilesystemCollectorExportsCounters(t *testing.T) {
	t.Cleanup(func() { SetFilesystemStatsFn(nil) })

	SetFilesystemStatsFn(func() []FilesystemStats {
		return []FilesystemStats{{
			Key:                     "localhost:9000/bucket-a",
			ReadCount:               100,
			WriteCount:              50,
			ReadBytes:               1000,
			WriteBytes:              500,
			GetFileInfoCount:        10,
			FailedCount:             1,
			MultiPartUploadCreated:  5,
			MultiPartUploadFinished: 3,
		}}
	})

	expected := `
# HELP milvus_storage_filesystem_read_bytes total bytes read from the storage layer
# TYPE milvus_storage_filesystem_read_bytes counter
milvus_storage_filesystem_read_bytes{fs="localhost:9000/bucket-a"} 1000
`
	err := testutil.CollectAndCompare(filesystemCollector, strings.NewReader(expected),
		"milvus_storage_filesystem_read_bytes")
	assert.NoError(t, err)

	// The values are cumulative process counters, so they must be exported as
	// counters -- exporting them as gauges is what made rate() unusable before.
	assert.Equal(t, 8, testutil.CollectAndCount(filesystemCollector))
}

func TestFilesystemCollectorMultipleFilesystems(t *testing.T) {
	t.Cleanup(func() { SetFilesystemStatsFn(nil) })

	SetFilesystemStatsFn(func() []FilesystemStats {
		return []FilesystemStats{
			{Key: "fs-1", ReadBytes: 1000},
			{Key: "fs-2", ReadBytes: 2000},
		}
	})

	assert.Equal(t, 2, testutil.CollectAndCount(filesystemCollector,
		"milvus_storage_filesystem_read_bytes"))
}

// Without a provider installed there is nothing to report; the collector must
// stay silent rather than emit zeros, which would read as "no traffic" instead
// of "not measured".
func TestFilesystemCollectorWithoutProvider(t *testing.T) {
	SetFilesystemStatsFn(nil)
	assert.Equal(t, 0, testutil.CollectAndCount(filesystemCollector))
}

// The provider crosses cgo into the storage layer. If it blows up, the scrape
// must degrade to "no series" instead of taking down /metrics for everything
// else on the endpoint.
func TestFilesystemCollectorSurvivesProviderPanic(t *testing.T) {
	t.Cleanup(func() { SetFilesystemStatsFn(nil) })

	SetFilesystemStatsFn(func() []FilesystemStats {
		panic("storage layer exploded")
	})

	assert.NotPanics(t, func() {
		assert.Equal(t, 0, testutil.CollectAndCount(filesystemCollector))
	})
}
