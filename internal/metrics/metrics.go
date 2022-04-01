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
	"net/http"
	// nolint:gosec
	_ "net/http/pprof"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	milvusNamespace = "milvus"

	AbandonLabel = "abandon"
	SuccessLabel = "success"
	FailLabel    = "fail"
	TotalLabel   = "total"

	InsertLabel = "insert"
	DeleteLabel = "delete"
	SearchLabel = "search"
	QueryLabel  = "query"

	CacheHitLabel  = "hit"
	CacheMissLabel = "miss"

	UnissuedIndexTaskLabel   = "unissued"
	InProgressIndexTaskLabel = "in-progress"
	FinishedIndexTaskLabel   = "finished"
	FailedIndexTaskLabel     = "failed"
	RecycledIndexTaskLabel   = "recycled"

	SealedSegmentLabel   = "Sealed"
	GrowingSegmentLabel  = "Growing"
	FlushedSegmentLabel  = "Flushed"
	FlushingSegmentLabel = "Flushing"
	DropedSegmentLabel   = "Dropped"

	nodeIDLabelName          = "node_id"
	statusLabelName          = "status"
	indexTaskStatusLabelName = "index_task_status"
	msgTypeLabelName         = "msg_type"
	collectionIDLabelName    = "collection_id"
	channelNameLabelName     = "channel_name"
	functionLabelName        = "function_name"
	queryTypeLabelName       = "query_type"
	segmentTypeLabelName     = "segment_type"
	usernameLabelName        = "username"
)

var (
	// buckets involves durations in milliseconds,
	// [1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 1.31072e+05]
	buckets = prometheus.ExponentialBuckets(1, 2, 18)
)

//ServeHTTP serves prometheus http service
func ServeHTTP(r *prometheus.Registry) {
	http.Handle("/metrics", promhttp.HandlerFor(r, promhttp.HandlerOpts{}))
	http.Handle("/metrics_default", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9091", nil); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
		}
	}()
}
