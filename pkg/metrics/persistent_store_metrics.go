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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DataGetLabel    = "get"
	DataPutLabel    = "put"
	DataRemoveLabel = "remove"
	DataWalkLabel   = "walk"
	DataStatLabel   = "stat"

	persistentDataOpType = "persistent_data_op_type"

	StorageComponentLabel      = "component"
	StorageWorkloadClassLabel  = "workload_class"
	StorageWorkloadKindLabel   = "workload_kind"
	StorageOperationLabel      = "operation"
	StorageOutcomeLabel        = "outcome"
	StorageRoleLabel           = "storage_role"
	StorageBackendKindLabel    = "backend_kind"
	StorageDirectionLabel      = "direction"
	StorageRetryReasonLabel    = "retry_reason"
	StorageErrorCategoryLabel  = "error_category"
	StorageCacheTierLabel      = "cache_tier"
	StorageCacheResultLabel    = "cache_result"
	StorageCacheActionLabel    = "cache_action"
	StorageProfileSourceLabel  = "source"
	StorageRequestedLevelLabel = "requested_level"
	StorageEffectiveLevelLabel = "effective_level"
	StorageDecisionReasonLabel = "reason"
	StorageScopeTypeLabel      = "scope_type"
)

var storageLatencyBuckets = []float64{
	0.00025, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100,
	0.250, 0.500, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

var storageSizeBuckets = []float64{
	1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10,
	1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20,
	1 << 30, 4 << 30,
}

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

	StorageOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "operations_total",
			Help:      "Total Milvus-observed storage operations; counts are representative and are not provider billing requests.",
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageOperationLabel, StorageOutcomeLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "operation_duration_seconds",
			Help:      "End-to-end duration in seconds of Milvus-observed storage operations.",
			Buckets:   storageLatencyBuckets,
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageOperationLabel, StorageOutcomeLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageOperationSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "operation_size_bytes",
			Help:      "Completed logical bytes of Milvus-observed storage operations when known.",
			Buckets:   storageSizeBuckets,
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageOperationLabel, StorageOutcomeLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "bytes_total",
			Help:      "Completed logical bytes at Milvus-observed storage boundaries.",
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageDirectionLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "retries_total",
			Help:      "Milvus-visible storage retries; provider SDK internal retries are not included.",
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageOperationLabel, StorageRetryReasonLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageOperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "operation_errors_total",
			Help:      "Failed, canceled, and timed-out Milvus-observed storage operations by bounded error category.",
		}, []string{StorageComponentLabel, StorageWorkloadClassLabel, StorageWorkloadKindLabel,
			StorageOperationLabel, StorageErrorCategoryLabel, StorageRoleLabel, StorageBackendKindLabel})

	StorageOperationsInflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "operations_inflight",
			Help:      "Current Milvus-observed storage operations in flight.",
		}, []string{StorageComponentLabel, StorageWorkloadKindLabel, StorageOperationLabel})

	StorageCacheLookups = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "cache_lookups_total", Help: "Relevant Milvus-visible storage cache lookups."},
		[]string{StorageComponentLabel, StorageWorkloadKindLabel, StorageCacheTierLabel, StorageCacheResultLabel})
	StorageCacheBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "cache_bytes_total", Help: "Relevant Milvus-visible storage cache bytes by bounded action."},
		[]string{StorageComponentLabel, StorageWorkloadKindLabel, StorageCacheTierLabel, StorageCacheActionLabel})
	StorageCacheWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "cache_wait_duration_seconds", Help: "Time blocked waiting for relevant storage cache data.", Buckets: storageLatencyBuckets},
		[]string{StorageComponentLabel, StorageWorkloadKindLabel, StorageCacheTierLabel, StorageOutcomeLabel})
	StorageCacheLoadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "cache_load_duration_seconds", Help: "Duration of relevant Milvus-visible storage cache loads.", Buckets: storageLatencyBuckets},
		[]string{StorageComponentLabel, StorageWorkloadKindLabel, StorageCacheTierLabel, StorageOutcomeLabel})

	StorageProfileDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "profile_decisions_total", Help: "Storage profile policy decisions."},
		[]string{StorageProfileSourceLabel, StorageRequestedLevelLabel, StorageEffectiveLevelLabel, StorageDecisionReasonLabel})
	StorageProfileActiveScopes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "profile_active_scopes", Help: "Current active storage profile scopes."},
		[]string{StorageScopeTypeLabel})
	StorageProfileDroppedSummaries = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "profile_dropped_summaries_total", Help: "Storage profile summaries dropped by bounded safety controls."},
		[]string{StorageDecisionReasonLabel})
	StorageProfileSnapshotDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: milvusNamespace, Subsystem: "storage", Name: "profile_snapshot_duration_seconds", Help: "Time to snapshot a storage profile summary.", Buckets: prometheus.ExponentialBuckets(0.000001, 2, 16)},
		[]string{StorageComponentLabel})

	// Filesystem metrics (default filesystem only) - common across all nodes
	FilesystemReadCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_read_count",
			Help:      "number of filesystem read operations",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemWriteCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_write_count",
			Help:      "number of filesystem write operations",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemReadBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_read_bytes",
			Help:      "total bytes read from filesystem",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemWriteBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_write_bytes",
			Help:      "total bytes written to filesystem",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemGetFileInfoCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_get_file_info_count",
			Help:      "number of get file info operations",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemFailedCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_failed_count",
			Help:      "number of failed filesystem operations",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemMultiPartUploadCreated = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_multi_part_upload_created",
			Help:      "number of multi-part uploads created",
		}, []string{
			filesystemKeyLabelName,
		})

	FilesystemMultiPartUploadFinished = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "storage",
			Name:      "filesystem_multi_part_upload_finished",
			Help:      "number of multi-part uploads finished",
		}, []string{
			filesystemKeyLabelName,
		})
)

// RegisterStorageMetrics registers storage metrics
func RegisterStorageMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PersistentDataKvSize)
	registry.MustRegister(PersistentDataRequestLatency)
	registry.MustRegister(PersistentDataOpCounter)
	registry.MustRegister(StorageOperations)
	registry.MustRegister(StorageOperationDuration)
	registry.MustRegister(StorageOperationSize)
	registry.MustRegister(StorageBytes)
	registry.MustRegister(StorageRetries)
	registry.MustRegister(StorageOperationErrors)
	registry.MustRegister(StorageOperationsInflight)
	registry.MustRegister(StorageCacheLookups)
	registry.MustRegister(StorageCacheBytes)
	registry.MustRegister(StorageCacheWaitDuration)
	registry.MustRegister(StorageCacheLoadDuration)
	registry.MustRegister(StorageProfileDecisions)
	registry.MustRegister(StorageProfileActiveScopes)
	registry.MustRegister(StorageProfileDroppedSummaries)
	registry.MustRegister(StorageProfileSnapshotDuration)

	// filesystem metrics
	registry.MustRegister(FilesystemReadCount)
	registry.MustRegister(FilesystemWriteCount)
	registry.MustRegister(FilesystemReadBytes)
	registry.MustRegister(FilesystemWriteBytes)
	registry.MustRegister(FilesystemGetFileInfoCount)
	registry.MustRegister(FilesystemFailedCount)
	registry.MustRegister(FilesystemMultiPartUploadCreated)
	registry.MustRegister(FilesystemMultiPartUploadFinished)
}

// PublishFilesystemMetrics publishes filesystem metrics (common across all nodes)
func PublishFilesystemMetrics(fs string, readCount, writeCount, readBytes, writeBytes, getFileInfoCount, failedCount, multiPartUploadCreated, multiPartUploadFinished int64) {
	labels := prometheus.Labels{
		filesystemKeyLabelName: fs,
	}

	FilesystemReadCount.With(labels).Set(float64(readCount))
	FilesystemWriteCount.With(labels).Set(float64(writeCount))
	FilesystemReadBytes.With(labels).Set(float64(readBytes))
	FilesystemWriteBytes.With(labels).Set(float64(writeBytes))
	FilesystemGetFileInfoCount.With(labels).Set(float64(getFileInfoCount))
	FilesystemFailedCount.With(labels).Set(float64(failedCount))
	FilesystemMultiPartUploadCreated.With(labels).Set(float64(multiPartUploadCreated))
	FilesystemMultiPartUploadFinished.With(labels).Set(float64(multiPartUploadFinished))
}
