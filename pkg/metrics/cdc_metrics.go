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
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// CDC metric names
	CDCMetricReplicatedMessagesTotal    = "replicated_messages_total"
	CDCMetricReplicatedBytesTotal       = "replicated_bytes_total"
	CDCMetricReplicateEndToEndLatency   = "replicate_end_to_end_latency"
	CDCMetricReplicateLag               = "replicate_lag"
	CDCMetricStreamRPCActiveConnections = "stream_rpc_active_connections"
	CDCMetricStreamRPCReconnectsTotal   = "stream_rpc_reconnects_total"

	// CDC metric labels
	CDCLabelTargetCluster = "target_cluster"
	CDCLabelChannelName   = WALChannelLabelName
	CDCLabelMsgType       = msgTypeLabelName
	CDCLabelType          = "type"
)

var CDCReplicatedMessagesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicatedMessagesTotal,
		Help:      "Total number of messages successfully forwarded by CDC",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelChannelName,
		CDCLabelMsgType,
	},
)

var CDCReplicatedBytesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicatedBytesTotal,
		Help:      "Total number of bytes of messages forwarded by CDC",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelChannelName,
		CDCLabelMsgType,
	},
)

var CDCReplicateEndToEndLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicateEndToEndLatency,
		Help:      "End-to-end latency from a single message being read from Source WAL to being written to Target WAL and receiving an ack",
		Buckets:   buckets,
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelChannelName,
	},
)

var CDCReplicateLag = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicateLag,
		Help:      "Lag between the latest message in Source and the latest message in Target",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelChannelName,
	},
)

var CDCStreamRPCActiveConnections = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricStreamRPCActiveConnections,
		Help:      "Number of active Stream RPC connections between CDC and the target cluster",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelType, // actual, expected
	},
)

var CDCStreamRPCReconnectsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricStreamRPCReconnectsTotal,
		Help:      "Number of Stream RPC reconnections due to failures/timeouts by CDC",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelChannelName,
	},
)

func RegisterCDC(registry *prometheus.Registry) {
	registry.MustRegister(CDCReplicatedMessagesTotal)
	registry.MustRegister(CDCReplicatedBytesTotal)
	registry.MustRegister(CDCReplicateEndToEndLatency)
	registry.MustRegister(CDCReplicateLag)
	registry.MustRegister(CDCStreamRPCActiveConnections)
	registry.MustRegister(CDCStreamRPCReconnectsTotal)
}
