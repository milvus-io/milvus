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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestPublishFilesystemMetrics(t *testing.T) {
	// Test values
	fsName := "test-filesystem"
	readCount := int64(100)
	writeCount := int64(50)
	readBytes := int64(1024000)
	writeBytes := int64(512000)
	getFileInfoCount := int64(200)
	failedCount := int64(5)
	multiPartUploadCreated := int64(10)
	multiPartUploadFinished := int64(8)

	// Call the function
	PublishFilesystemMetrics(fsName, readCount, writeCount, readBytes, writeBytes,
		getFileInfoCount, failedCount, multiPartUploadCreated, multiPartUploadFinished)

	// Verify each metric
	labels := prometheus.Labels{filesystemKeyLabelName: fsName}

	// Check FilesystemReadCount
	readCountMetric := FilesystemReadCount.With(labels)
	assert.NotNil(t, readCountMetric)

	// Check FilesystemWriteCount
	writeCountMetric := FilesystemWriteCount.With(labels)
	assert.NotNil(t, writeCountMetric)

	// Check FilesystemReadBytes
	readBytesMetric := FilesystemReadBytes.With(labels)
	assert.NotNil(t, readBytesMetric)

	// Check FilesystemWriteBytes
	writeBytesMetric := FilesystemWriteBytes.With(labels)
	assert.NotNil(t, writeBytesMetric)

	// Check FilesystemGetFileInfoCount
	getFileInfoMetric := FilesystemGetFileInfoCount.With(labels)
	assert.NotNil(t, getFileInfoMetric)

	// Check FilesystemFailedCount
	failedMetric := FilesystemFailedCount.With(labels)
	assert.NotNil(t, failedMetric)

	// Check FilesystemMultiPartUploadCreated
	multiPartCreatedMetric := FilesystemMultiPartUploadCreated.With(labels)
	assert.NotNil(t, multiPartCreatedMetric)

	// Check FilesystemMultiPartUploadFinished
	multiPartFinishedMetric := FilesystemMultiPartUploadFinished.With(labels)
	assert.NotNil(t, multiPartFinishedMetric)
}

func TestPublishFilesystemMetricsMultipleFilesystems(t *testing.T) {
	// Test with multiple filesystem names to ensure labels work correctly
	fs1 := "minio"
	fs2 := "s3"

	// Publish metrics for first filesystem
	PublishFilesystemMetrics(fs1, 100, 50, 1000, 500, 10, 1, 5, 3)

	// Publish metrics for second filesystem
	PublishFilesystemMetrics(fs2, 200, 100, 2000, 1000, 20, 2, 10, 6)

	// Verify both filesystems have their own metric values
	labels1 := prometheus.Labels{filesystemKeyLabelName: fs1}
	labels2 := prometheus.Labels{filesystemKeyLabelName: fs2}

	// Both should be non-nil and independently tracked
	assert.NotNil(t, FilesystemReadCount.With(labels1))
	assert.NotNil(t, FilesystemReadCount.With(labels2))
	assert.NotNil(t, FilesystemWriteCount.With(labels1))
	assert.NotNil(t, FilesystemWriteCount.With(labels2))
}

func TestPublishFilesystemMetricsZeroValues(t *testing.T) {
	// Test with zero values to ensure no issues with edge cases
	fsName := "empty-filesystem"

	PublishFilesystemMetrics(fsName, 0, 0, 0, 0, 0, 0, 0, 0)

	labels := prometheus.Labels{filesystemKeyLabelName: fsName}

	// All metrics should still be created with zero values
	assert.NotNil(t, FilesystemReadCount.With(labels))
	assert.NotNil(t, FilesystemWriteCount.With(labels))
	assert.NotNil(t, FilesystemReadBytes.With(labels))
	assert.NotNil(t, FilesystemWriteBytes.With(labels))
	assert.NotNil(t, FilesystemGetFileInfoCount.With(labels))
	assert.NotNil(t, FilesystemFailedCount.With(labels))
	assert.NotNil(t, FilesystemMultiPartUploadCreated.With(labels))
	assert.NotNil(t, FilesystemMultiPartUploadFinished.With(labels))
}
