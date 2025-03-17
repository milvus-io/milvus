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
	// #nosec
	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	milvusNamespace = "milvus"

	AbandonLabel = "abandon"
	SuccessLabel = "success"
	FailLabel    = "fail"
	CancelLabel  = "cancel"
	TotalLabel   = "total"

	HybridSearchLabel = "hybrid_search"

	InsertLabel    = "insert"
	DeleteLabel    = "delete"
	UpsertLabel    = "upsert"
	SearchLabel    = "search"
	QueryLabel     = "query"
	CacheHitLabel  = "hit"
	CacheMissLabel = "miss"
	TimetickLabel  = "timetick"
	AllLabel       = "all"

	UnissuedIndexTaskLabel   = "unissued"
	InProgressIndexTaskLabel = "in-progress"
	FinishedIndexTaskLabel   = "finished"
	FailedIndexTaskLabel     = "failed"
	RecycledIndexTaskLabel   = "recycled"

	// Note: below must matchcommonpb.SegmentState_name fields.
	SealedSegmentLabel   = "Sealed"
	GrowingSegmentLabel  = "Growing"
	FlushedSegmentLabel  = "Flushed"
	FlushingSegmentLabel = "Flushing"
	DroppedSegmentLabel  = "Dropped"

	StreamingDataSourceLabel  = "streaming"
	BulkinsertDataSourceLabel = "bulkinsert"
	CompactionDataSourceLabel = "compaction"

	Leader     = "OnLeader"
	FromLeader = "FromLeader"

	HookBefore = "before"
	HookAfter  = "after"
	HookMock   = "mock"

	ReduceSegments = "segments"
	ReduceShards   = "shards"

	BatchReduce  = "batch_reduce"
	StreamReduce = "stream_reduce"

	Pending   = "pending"
	Executing = "executing"
	Done      = "done"

	ImportStagePending    = "pending"
	ImportStagePreImport  = "preimport"
	ImportStageImport     = "import"
	ImportStageStats      = "stats"
	ImportStageBuildIndex = "build_index"

	compactionTypeLabelName  = "compaction_type"
	isVectorFieldLabelName   = "is_vector_field"
	segmentPruneLabelName    = "segment_prune_label"
	stageLabelName           = "compaction_stage"
	nodeIDLabelName          = "node_id"
	nodeHostLabelName        = "node_host"
	statusLabelName          = "status"
	indexTaskStatusLabelName = "index_task_status"
	msgTypeLabelName         = "msg_type"
	collectionIDLabelName    = "collection_id"
	channelNameLabelName     = "channel_name"
	functionLabelName        = "function_name"
	queryTypeLabelName       = "query_type"
	collectionName           = "collection_name"
	databaseLabelName        = "db_name"
	ResourceGroupLabelName   = "rg"
	indexName                = "index_name"
	isVectorIndex            = "is_vector_index"
	segmentStateLabelName    = "segment_state"
	segmentLevelLabelName    = "segment_level"
	segmentIsSortedLabelName = "segment_is_sorted"
	usernameLabelName        = "username"
	roleNameLabelName        = "role_name"
	cacheNameLabelName       = "cache_name"
	cacheStateLabelName      = "cache_state"
	dataSourceLabelName      = "data_source"
	dataTypeLabelName        = "data_type"
	importStageLabelName     = "import_stage"
	requestScope             = "scope"
	fullMethodLabelName      = "full_method"
	reduceLevelName          = "reduce_level"
	reduceType               = "reduce_type"
	lockName                 = "lock_name"
	lockSource               = "lock_source"
	lockType                 = "lock_type"
	lockOp                   = "lock_op"
	loadTypeName             = "load_type"
	pathLabelName            = "path"
	cgoNameLabelName         = `cgo_name`
	cgoTypeLabelName         = `cgo_type`
	queueTypeLabelName       = `queue_type`

	// entities label
	LoadedLabel         = "loaded"
	NumEntitiesAllLabel = "all"

	TaskTypeLabel  = "task_type"
	TaskStateLabel = "task_state"
)

var (
	// buckets involves durations in milliseconds,
	// [1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 1.31072e+05]
	buckets = prometheus.ExponentialBuckets(1, 2, 18)

	// longTaskBuckets provides long task duration in milliseconds
	longTaskBuckets = []float64{1, 100, 500, 1000, 5000, 10000, 20000, 50000, 100000, 250000, 500000, 1000000, 3600000, 5000000, 10000000} // unit milliseconds

	// size provides size in byte
	sizeBuckets = []float64{10000, 100000, 1000000, 100000000, 500000000, 1024000000, 2048000000, 4096000000, 10000000000, 50000000000} // unit byte

	NumNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Name:      "num_node",
			Help:      "number of nodes and coordinates",
		}, []string{nodeIDLabelName, roleNameLabelName})

	LockCosts = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Name:      "lock_time_cost",
			Help:      "time cost for various kinds of locks",
		}, []string{
			lockName,
			lockSource,
			lockType,
			lockOp,
		})

	metricRegisterer prometheus.Registerer
)

// GetRegisterer returns the global prometheus registerer
// metricsRegistry must be call after Register is called or no Register is called.
func GetRegisterer() prometheus.Registerer {
	if metricRegisterer == nil {
		return prometheus.DefaultRegisterer
	}
	return metricRegisterer
}

// Register serves prometheus http service
// Should be called by init function.
func Register(r prometheus.Registerer) {
	r.MustRegister(NumNodes)
	r.MustRegister(LockCosts)
	r.MustRegister(BuildInfo)
	r.MustRegister(RuntimeInfo)
	r.MustRegister(ThreadNum)
	metricRegisterer = r
}
