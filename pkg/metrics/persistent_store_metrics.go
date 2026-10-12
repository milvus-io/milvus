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

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemReadCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_read_count",
			Help:      "number of filesystem read operations",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemWriteCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_write_count",
			Help:      "number of filesystem write operations",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemReadBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_read_bytes",
			Help:      "total bytes read from filesystem",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemWriteBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_write_bytes",
			Help:      "total bytes written to filesystem",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemGetFileInfoCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_get_file_info_count",
			Help:      "number of get file info operations",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemFailedCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_failed_count",
			Help:      "number of failed filesystem operations",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemMultiPartUploadCreated = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_multi_part_upload_created",
			Help:      "number of multi-part uploads created",
		}, []string{filesystemKeyLabelName})

	// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
	FilesystemMultiPartUploadFinished = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_multi_part_upload_finished",
			Help:      "number of multi-part uploads finished",
		}, []string{filesystemKeyLabelName})

	filesystemMetricVecs = []*prometheus.GaugeVec{
		FilesystemReadCount,
		FilesystemWriteCount,
		FilesystemReadBytes,
		FilesystemWriteBytes,
		FilesystemGetFileInfoCount,
		FilesystemFailedCount,
		FilesystemMultiPartUploadCreated,
		FilesystemMultiPartUploadFinished,
	}
)

// FilesystemMetrics holds the metrics for one cached filesystem.
type FilesystemMetrics struct {
	DisplayKey              string
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
	filesystemMetricsCollectFn func() []FilesystemMetrics
	filesystemMetricsMu        sync.Mutex

	filesystemMetricsScrapeMu   sync.Mutex
	filesystemMetricsStateMu    sync.Mutex
	filesystemMetricsCacheKeys  = make(map[string]struct{})
	filesystemMetricsLegacyKeys = make(map[string]struct{})
)

// SetFilesystemMetricsCollectFn sets the callback used to list current filesystem metrics.
func SetFilesystemMetricsCollectFn(fn func() []FilesystemMetrics) {
	filesystemMetricsMu.Lock()
	defer filesystemMetricsMu.Unlock()
	filesystemMetricsCollectFn = fn
}

type filesystemMetricsCollector struct{}

func (c *filesystemMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range filesystemMetricVecs {
		metric.Describe(ch)
	}
}

func (c *filesystemMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	filesystemMetricsScrapeMu.Lock()
	defer filesystemMetricsScrapeMu.Unlock()

	filesystemMetricsMu.Lock()
	fn := filesystemMetricsCollectFn
	filesystemMetricsMu.Unlock()

	var cacheMetrics []FilesystemMetrics
	if fn != nil {
		cacheMetrics = fn()
	}

	filesystemMetricsStateMu.Lock()
	defer filesystemMetricsStateMu.Unlock()

	for key := range filesystemMetricsCacheKeys {
		if _, legacy := filesystemMetricsLegacyKeys[key]; legacy {
			continue
		}
		for _, metric := range filesystemMetricVecs {
			metric.DeleteLabelValues(key)
		}
	}
	clear(filesystemMetricsCacheKeys)

	for _, metric := range cacheMetrics {
		if _, legacy := filesystemMetricsLegacyKeys[metric.DisplayKey]; legacy {
			continue
		}
		setFilesystemMetrics(metric.DisplayKey, metric.ReadCount, metric.WriteCount, metric.ReadBytes, metric.WriteBytes,
			metric.GetFileInfoCount, metric.FailedCount, metric.MultiPartUploadCreated, metric.MultiPartUploadFinished)
		filesystemMetricsCacheKeys[metric.DisplayKey] = struct{}{}
	}

	for _, metric := range filesystemMetricVecs {
		metric.Collect(ch)
	}
}

func setFilesystemMetrics(fs string, readCount, writeCount, readBytes, writeBytes, getFileInfoCount, failedCount, multiPartUploadCreated, multiPartUploadFinished int64) {
	FilesystemReadCount.WithLabelValues(fs).Set(float64(readCount))
	FilesystemWriteCount.WithLabelValues(fs).Set(float64(writeCount))
	FilesystemReadBytes.WithLabelValues(fs).Set(float64(readBytes))
	FilesystemWriteBytes.WithLabelValues(fs).Set(float64(writeBytes))
	FilesystemGetFileInfoCount.WithLabelValues(fs).Set(float64(getFileInfoCount))
	FilesystemFailedCount.WithLabelValues(fs).Set(float64(failedCount))
	FilesystemMultiPartUploadCreated.WithLabelValues(fs).Set(float64(multiPartUploadCreated))
	FilesystemMultiPartUploadFinished.WithLabelValues(fs).Set(float64(multiPartUploadFinished))
}

// PublishFilesystemMetrics publishes filesystem metrics.
//
// Deprecated: filesystem metrics are collected from the filesystem cache at scrape time. Remove in v4.
func PublishFilesystemMetrics(fs string, readCount, writeCount, readBytes, writeBytes, getFileInfoCount, failedCount, multiPartUploadCreated, multiPartUploadFinished int64) {
	filesystemMetricsStateMu.Lock()
	defer filesystemMetricsStateMu.Unlock()

	setFilesystemMetrics(fs, readCount, writeCount, readBytes, writeBytes, getFileInfoCount, failedCount, multiPartUploadCreated, multiPartUploadFinished)
	filesystemMetricsLegacyKeys[fs] = struct{}{}
}

// RegisterStorageMetrics registers storage metrics
func RegisterStorageMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PersistentDataKvSize)
	registry.MustRegister(PersistentDataRequestLatency)
	registry.MustRegister(PersistentDataOpCounter)

	registry.MustRegister(&filesystemMetricsCollector{})
}
