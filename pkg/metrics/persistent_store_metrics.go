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

	"github.com/prometheus/client_golang/prometheus"
)

const (
	DataGetLabel    = "get"
	DataPutLabel    = "put"
	DataRemoveLabel = "remove"
	DataWalkLabel   = "walk"
	DataStatLabel   = "stat"

	persistentDataOpType = "persistent_data_op_type"
)

var (
	PersistentDataKvSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "kv_size",
			Help:      "kv size stats",
			Buckets:   buckets,
		}, []string{persistentDataOpType})

	PersistentDataRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "request_latency",
			Help:      "request latency on the client side ",
			Buckets:   buckets,
		}, []string{persistentDataOpType})

	PersistentDataOpCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "op_count",
			Help:      "count of persistent data operation",
		}, []string{persistentDataOpType, statusLabelName})
)

// RegisterStorageMetrics registers storage metrics
func RegisterStorageMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PersistentDataKvSize)
	registry.MustRegister(PersistentDataRequestLatency)
	registry.MustRegister(PersistentDataOpCounter)

	registry.MustRegister(filesystemCollector)
}

// FilesystemStats is one filesystem's cumulative counters as reported by the
// storage layer. Every field is monotonic for the lifetime of the process, so
// they are exported as counters. Counts are of real object-storage requests
// (S3 GetObject/PutObject and their local-filesystem equivalents), not of
// higher-level chunk or column reads -- one logical read may coalesce into
// fewer requests, which is exactly what this is meant to make visible.
type FilesystemStats struct {
	Key                     string
	ReadCount               int64
	WriteCount              int64
	ReadBytes               int64
	WriteBytes              int64
	GetFileInfoCount        int64
	FailedCount             int64
	MultiPartUploadCreated  int64
	MultiPartUploadFinished int64
}

var (
	filesystemLabels = []string{filesystemKeyLabelName}

	filesystemReadCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_read_count"),
		"number of object-storage read requests issued by the storage layer",
		filesystemLabels, nil)
	filesystemWriteCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_write_count"),
		"number of object-storage write requests issued by the storage layer",
		filesystemLabels, nil)
	filesystemReadBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_read_bytes"),
		"total bytes read from the storage layer",
		filesystemLabels, nil)
	filesystemWriteBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_write_bytes"),
		"total bytes written to the storage layer",
		filesystemLabels, nil)
	filesystemGetFileInfoCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_get_file_info_count"),
		"number of get file info operations",
		filesystemLabels, nil)
	filesystemFailedCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_failed_count"),
		"number of failed storage layer operations",
		filesystemLabels, nil)
	filesystemMultiPartUploadCreatedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_multi_part_upload_created"),
		"number of multi-part uploads created",
		filesystemLabels, nil)
	filesystemMultiPartUploadFinishedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(milvusNamespace, "storage", "filesystem_multi_part_upload_finished"),
		"number of multi-part uploads finished",
		filesystemLabels, nil)

	filesystemCollector = &filesystemMetricsCollector{}

	filesystemStatsMu sync.RWMutex
	filesystemStatsFn func() []FilesystemStats
)

// SetFilesystemStatsFn installs the callback the collector uses to read the
// storage layer's counters. It lives behind a callback because pkg/ is its own
// module and cannot import the cgo storage package. Safe to call more than
// once; passing nil disables collection.
func SetFilesystemStatsFn(fn func() []FilesystemStats) {
	filesystemStatsMu.Lock()
	defer filesystemStatsMu.Unlock()
	filesystemStatsFn = fn
}

// filesystemMetricsCollector exports the storage layer's counters using the
// pull model: the values are read at scrape time rather than pushed on some
// unrelated event. The push version only ran after a LoadSegments RPC, which
// froze every series at whatever the totals were when the last load finished
// and left roles that never load segments with no series at all.
type filesystemMetricsCollector struct{}

func (c *filesystemMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- filesystemReadCountDesc
	ch <- filesystemWriteCountDesc
	ch <- filesystemReadBytesDesc
	ch <- filesystemWriteBytesDesc
	ch <- filesystemGetFileInfoCountDesc
	ch <- filesystemFailedCountDesc
	ch <- filesystemMultiPartUploadCreatedDesc
	ch <- filesystemMultiPartUploadFinishedDesc
}

func (c *filesystemMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range collectFilesystemStats() {
		ch <- prometheus.MustNewConstMetric(filesystemReadCountDesc, prometheus.CounterValue, float64(s.ReadCount), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemWriteCountDesc, prometheus.CounterValue, float64(s.WriteCount), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemReadBytesDesc, prometheus.CounterValue, float64(s.ReadBytes), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemWriteBytesDesc, prometheus.CounterValue, float64(s.WriteBytes), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemGetFileInfoCountDesc, prometheus.CounterValue, float64(s.GetFileInfoCount), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemFailedCountDesc, prometheus.CounterValue, float64(s.FailedCount), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemMultiPartUploadCreatedDesc, prometheus.CounterValue, float64(s.MultiPartUploadCreated), s.Key)
		ch <- prometheus.MustNewConstMetric(filesystemMultiPartUploadFinishedDesc, prometheus.CounterValue, float64(s.MultiPartUploadFinished), s.Key)
	}
}

// collectFilesystemStats calls the installed callback, which crosses cgo into
// the storage layer. A failure or panic there must degrade to "no series this
// scrape" rather than take down the whole /metrics endpoint.
func collectFilesystemStats() (stats []FilesystemStats) {
	filesystemStatsMu.RLock()
	fn := filesystemStatsFn
	filesystemStatsMu.RUnlock()
	if fn == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			stats = nil
		}
	}()
	return fn()
}
