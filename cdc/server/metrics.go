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

package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	milvusNamespace = "milvus"
	systemName      = "cdc"

	unknownTypeLabel = "unknown"

	// request status label
	totalStatusLabel          = "total"
	successStatusLabel        = "success"
	failStatusLabel           = "fail"
	finishStatusLabel         = "finish"
	invalidMethodStatusLabel  = "invalid_method"
	readErrorStatusLabel      = "read_error"
	unmarshalErrorStatusLabel = "unmarshal_error"

	// write fail func label
	writeFailOnUpdatePosition = "on_update_position"
	writeFailOnFail           = "on_fail"
	writeFailOnDrop           = "on_drop"

	// read fail func label
	readFailUnknown           = "unknown"
	readFailGetCollectionInfo = "get_collection_info"
	readFailReadStream        = "read_stream"

	taskStateLabelName                 = "task_state"
	requestTypeLabelName               = "request_type"
	requestStatusLabelName             = "request_status"
	taskIDLabelName                    = "task_id"
	writeFailFuncLabelName             = "write_fail_func"
	collectionIDLabelName              = "collection_id"
	vchannelLabelName                  = "vchannel_name"
	readFailFuncLabelName              = "read_fail_func"
	streamingCollectionStatusLabelName = "streaming_collection_status"
)

var (
	registry *prometheus.Registry
	buckets  = prometheus.ExponentialBuckets(1, 2, 18)

	taskNumVec = &TaskNumMetric{
		metricDesc: prometheus.NewDesc(prometheus.BuildFQName(milvusNamespace, systemName, "task_num"),
			"cdc task number", []string{taskStateLabelName}, nil),
	}
	taskRequestLatencyVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_latency",
			Help:      "cdc request latency on the client side ",
			Buckets:   buckets,
		}, []string{requestTypeLabelName})

	taskRequestCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_total",
			Help:      "cdc request count",
		}, []string{requestTypeLabelName, requestStatusLabelName})

	streamingCollectionCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "streaming_collection_total",
			Help:      "the number of collections that are synchronizing data",
		}, []string{taskIDLabelName, streamingCollectionStatusLabelName})

	readerFailCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "reader_fail_total",
		Help:      "the fail count when the reader reads milvus data",
	}, []string{taskIDLabelName, readFailFuncLabelName})

	writerFailCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "writer_callback_fail_total",
		Help:      "the fail count when the writer executes the callback function",
	}, []string{taskIDLabelName, writeFailFuncLabelName})

	// TODO fubang
	writerTimeDifferenceVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "writer_tt_lag_ms",
			Help:      "the time difference between the current time and the current message timestamp",
		}, []string{taskIDLabelName, collectionIDLabelName, vchannelLabelName})
	// TODO fubang diff offset
)

func init() {
	registry = prometheus.NewRegistry()
	registry.MustRegister(taskNumVec)
	registry.MustRegister(taskRequestLatencyVec)
	registry.MustRegister(taskRequestCountVec)
	registry.MustRegister(streamingCollectionCountVec)
	registry.MustRegister(readerFailCountVec)
	registry.MustRegister(writerFailCountVec)
	registry.MustRegister(writerTimeDifferenceVec)
}

func registerMetric() {
	http.Handle("/cdc/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.Handle("/cdc/metrics_default", promhttp.Handler())
}
