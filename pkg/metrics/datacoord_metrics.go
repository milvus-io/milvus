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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	InsertFileLabel          = "insert_file"
	DeleteFileLabel          = "delete_file"
	StatFileLabel            = "stat_file"
	IndexFileLabel           = "index_file"
	segmentFileTypeLabelName = "segment_file_type"
)

var (
	// DataCoordNumDataNodes records the num of data nodes managed by DataCoord.
	DataCoordNumDataNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "datanode_num",
			Help:      "number of data nodes",
		}, []string{})

	DataCoordNumSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "segment_num",
			Help:      "number of segments",
		}, []string{
			segmentStateLabelName,
			segmentLevelLabelName,
			segmentIsSortedLabelName,
		})

	// DataCoordCollectionNum records the num of collections managed by DataCoord.
	DataCoordNumCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "collection_num",
			Help:      "number of collections",
		}, []string{})

	DataCoordSizeStoredL0Segment = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "store_level0_segment_size",
			Help:      "stored l0 segment size",
			Buckets:   buckets,
		}, []string{
			collectionIDLabelName,
		})

	DataCoordL0DeleteEntriesNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "l0_delete_entries_num",
			Help:      "Delete entries number of Level zero segment",
		}, []string{
			databaseLabelName,
			collectionIDLabelName,
		})

	// DataCoordNumStoredRows all metrics will be cleaned up after removing matched collectionID and
	// segment state labels in CleanupDataCoordNumStoredRows method.
	DataCoordNumStoredRows = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "stored_rows_num",
			Help:      "number of stored rows of healthy segment",
		}, []string{
			databaseLabelName,
			collectionIDLabelName,
			collectionName,
			segmentStateLabelName,
		})

	DataCoordBulkVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "bulk_insert_vectors_count",
			Help:      "counter of vectors successfully bulk inserted",
		}, []string{
			databaseLabelName,
			collectionIDLabelName,
		})

	DataCoordConsumeDataNodeTimeTickLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "consume_datanode_tt_lag_ms",
			Help:      "now time minus tt per physical channel",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	DataCoordCheckpointUnixSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "channel_checkpoint_unix_seconds",
			Help:      "channel checkpoint timestamp in unix seconds",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	DataCoordStoredBinlogSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "stored_binlog_size",
			Help:      "binlog size of healthy segments",
		}, []string{
			databaseLabelName,
			collectionIDLabelName,
			segmentStateLabelName,
		})
	DataCoordSegmentBinLogFileCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "segment_binlog_file_count",
			Help:      "number of binlog files for each segment",
		}, []string{
			collectionIDLabelName,
		})

	DataCoordStoredIndexFilesSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "stored_index_files_size",
			Help:      "index files size of the segments",
		}, []string{
			databaseLabelName,
			collectionName,
			collectionIDLabelName,
		})

	DataCoordDmlChannelNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "watched_dml_chanel_num",
			Help:      "the num of dml channel watched by datanode",
		}, []string{
			nodeIDLabelName,
		})

	DataCoordCompactedSegmentSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "compacted_segment_size",
			Help:      "the segment size of compacted segment",
			Buckets:   sizeBuckets,
		}, []string{})

	DataCoordCompactionTaskNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "compaction_task_num",
			Help:      "Number of compaction tasks currently",
		}, []string{
			nodeIDLabelName,
			compactionTypeLabelName,
			statusLabelName,
		})

	DataCoordCompactionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "compaction_latency",
			Help:      "latency of compaction operation",
			Buckets:   longTaskBuckets,
		}, []string{
			isVectorFieldLabelName,
			channelNameLabelName,
			compactionTypeLabelName,
			stageLabelName,
		})

	ImportJobLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "import_job_latency",
			Help:      "latency of import job",
			Buckets:   longTaskBuckets,
		}, []string{
			importStageLabelName,
		})

	ImportTaskLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "import_task_latency",
			Help:      "latency of import task",
			Buckets:   longTaskBuckets,
		}, []string{
			importStageLabelName,
		})

	FlushedSegmentFileNum = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Name:      "flushed_segment_file_num",
			Help:      "the num of files for flushed segment",
			Buckets:   buckets,
		}, []string{segmentFileTypeLabelName})

	/* garbage collector related metrics */

	// GarbageCollectorFileScanDuration metrics for gc scan storage files.
	GarbageCollectorFileScanDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "gc_file_scan_duration",
			Help:      "duration of scan file in storage while garbage collecting (in milliseconds)",
			Buckets:   longTaskBuckets,
		}, []string{nodeIDLabelName, segmentFileTypeLabelName})

	GarbageCollectorRunCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "gc_run_count",
			Help:      "garbage collection running count",
		}, []string{nodeIDLabelName})

	/* hard to implement, commented now
	DataCoordSegmentSizeRatio = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "segment_size_ratio",
			Help:      "size ratio compared to the configuration size",
			Buckets:   prometheus.LinearBuckets(0.0, 0.1, 15),
		}, []string{})

	DataCoordSegmentFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "segment_flush_duration",
			Help:      "time spent on each segment flush",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 20, 50, 100, 250, 500, 1000, 3600, 5000, 10000}, // unit seconds
		}, []string{})

	DataCoordCompactDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "segment_compact_duration",
			Help:      "time spent on each segment flush",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 20, 50, 100, 250, 500, 1000, 3600, 5000, 10000}, // unit seconds
		}, []string{})

	DataCoordCompactLoad = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "compaction_load",
			Help:      "Information on the input and output of compaction",
		}, []string{})

	*/

	// IndexRequestCounter records the number of the index requests.
	IndexRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "index_req_count",
			Help:      "number of building index requests ",
		}, []string{statusLabelName})

	// IndexTaskNum records the number of index tasks of each type.
	// Deprecated: please ues TaskNum after v2.5.5.
	IndexTaskNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "index_task_count",
			Help:      "number of index tasks of each type",
		}, []string{collectionIDLabelName, indexTaskStatusLabelName})

	// IndexNodeNum records the number of IndexNodes managed by IndexCoord.
	IndexNodeNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "index_node_num",
			Help:      "number of IndexNodes managed by IndexCoord",
		}, []string{})

	ImportJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "import_jobs",
			Help:      "the import jobs grouping by state",
		}, []string{"import_state"})

	ImportTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "import_tasks",
			Help:      "the import tasks grouping by type and state",
		}, []string{"task_type", "import_state"})

	DataCoordTaskExecuteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "task_execute_max_latency",
			Help:      "latency of task execute operation",
			Buckets:   longTaskBuckets,
		}, []string{
			TaskTypeLabel,
			statusLabelName,
		})

	// TaskNum records the number of tasks of each type.
	TaskNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataCoordRole,
			Name:      "task_count",
			Help:      "number of index tasks of each type",
		}, []string{TaskTypeLabel, TaskStateLabel})
)

// RegisterDataCoord registers DataCoord metrics
func RegisterDataCoord(registry *prometheus.Registry) {
	registry.MustRegister(DataCoordNumDataNodes)
	registry.MustRegister(DataCoordNumSegments)
	registry.MustRegister(DataCoordNumCollections)
	registry.MustRegister(DataCoordNumStoredRows)
	registry.MustRegister(DataCoordBulkVectors)
	registry.MustRegister(DataCoordConsumeDataNodeTimeTickLag)
	registry.MustRegister(DataCoordCheckpointUnixSeconds)
	registry.MustRegister(DataCoordStoredBinlogSize)
	registry.MustRegister(DataCoordStoredIndexFilesSize)
	registry.MustRegister(DataCoordSegmentBinLogFileCount)
	registry.MustRegister(DataCoordDmlChannelNum)
	registry.MustRegister(DataCoordCompactedSegmentSize)
	registry.MustRegister(DataCoordCompactionTaskNum)
	registry.MustRegister(DataCoordCompactionLatency)
	registry.MustRegister(ImportJobLatency)
	registry.MustRegister(ImportTaskLatency)
	registry.MustRegister(DataCoordSizeStoredL0Segment)
	registry.MustRegister(DataCoordL0DeleteEntriesNum)
	registry.MustRegister(FlushedSegmentFileNum)
	registry.MustRegister(IndexRequestCounter)
	registry.MustRegister(IndexTaskNum)
	registry.MustRegister(IndexNodeNum)
	registry.MustRegister(ImportTasks)
	registry.MustRegister(GarbageCollectorFileScanDuration)
	registry.MustRegister(GarbageCollectorRunCount)
	registry.MustRegister(DataCoordTaskExecuteLatency)
	registry.MustRegister(TaskNum)

	registerStreamingCoord(registry)
}

func CleanupDataCoordWithCollectionID(collectionID int64) {
	IndexTaskNum.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordNumStoredRows.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordBulkVectors.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordSegmentBinLogFileCount.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordStoredBinlogSize.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordStoredIndexFilesSize.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordSizeStoredL0Segment.Delete(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
	DataCoordL0DeleteEntriesNum.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
}
