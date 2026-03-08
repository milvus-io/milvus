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

import "github.com/prometheus/client_golang/prometheus"

const (
	SendMsgLabel    = "produce"
	ReceiveMsgLabel = "consume" // not used

	CreateProducerLabel = "create_producer"
	CreateConsumerLabel = "create_consumer"

	msgStreamOpType = "message_op_type"
)

var (
	NumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "msg_queue",
			Name:      "consumer_num",
			Help:      "number of consumers",
		}, []string{
			roleNameLabelName,
			nodeIDLabelName,
		})

	MsgStreamRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "msgstream",
			Name:      "request_latency",
			Help:      "request latency on the client side ",
			Buckets:   buckets,
		}, []string{msgStreamOpType})

	MsgStreamOpCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "msgstream",
			Name:      "op_count",
			Help:      "count of stream message operation",
		}, []string{msgStreamOpType, statusLabelName})
)

// RegisterMsgStreamMetrics registers msg stream metrics
func RegisterMsgStreamMetrics(registry *prometheus.Registry) {
	registry.MustRegister(NumConsumers)
	registry.MustRegister(MsgStreamRequestLatency)
	registry.MustRegister(MsgStreamOpCounter)
}
