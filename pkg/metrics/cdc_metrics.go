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

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// CDC metric names
	CDCMetricReplicatedMessagesTotal  = "replicated_messages_total"
	CDCMetricReplicatedBytesTotal     = "replicated_bytes_total"
	CDCMetricReplicateEndToEndLatency = "replicate_end_to_end_latency"
	CDCMetricReplicateLag             = "replicate_lag"
	CDCMetricStreamRPCConnections     = "stream_rpc_connections"
	CDCMetricStreamRPCReconnectTimes  = "stream_rpc_reconnect_times"

	// CDC metric labels
	CDCLabelTargetCluster     = "target_cluster"
	CDCLabelSourceChannelName = "source_channel_name"
	CDCLabelTargetChannelName = "target_channel_name"
	CDCLabelMsgType           = msgTypeLabelName
	CDCLabelConnectionStatus  = "connection_status"

	// CDC metric values
	CDCStatusConnected    = "connected"
	CDCStatusDisconnected = "disconnected"
)

var CDCReplicatedMessagesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicatedMessagesTotal,
		Help:      "Total number of messages successfully forwarded by CDC",
	}, []string{
		CDCLabelSourceChannelName,
		CDCLabelTargetChannelName,
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
		CDCLabelSourceChannelName,
		CDCLabelTargetChannelName,
		CDCLabelMsgType,
	},
)

var CDCReplicateEndToEndLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicateEndToEndLatency,
		Help:      "End-to-end latency in milliseconds from a single message being read from Source WAL to being written to Target WAL and receiving an ack",
		Buckets:   buckets,
	}, []string{
		CDCLabelSourceChannelName,
		CDCLabelTargetChannelName,
	},
)

var CDCReplicateLag = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricReplicateLag,
		Help:      "Lag in milliseconds between the latest synced Source message and the current time",
	}, []string{
		CDCLabelSourceChannelName,
		CDCLabelTargetChannelName,
	},
)

var CDCStreamRPCConnections = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricStreamRPCConnections,
		Help:      "Stream RPC connections status between CDC and the target cluster",
	}, []string{
		CDCLabelTargetCluster,
		CDCLabelConnectionStatus,
	},
)

var CDCStreamRPCReconnectTimes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: typeutil.CDCRole,
		Name:      CDCMetricStreamRPCReconnectTimes,
		Help:      "Stream RPC reconnections times between CDC and the target cluster",
	}, []string{
		CDCLabelTargetCluster,
	},
)

func RegisterCDC(registry *prometheus.Registry) {
	registry.MustRegister(CDCReplicatedMessagesTotal)
	registry.MustRegister(CDCReplicatedBytesTotal)
	registry.MustRegister(CDCReplicateEndToEndLatency)
	registry.MustRegister(CDCReplicateLag)
	registry.MustRegister(CDCStreamRPCConnections)
	registry.MustRegister(CDCStreamRPCReconnectTimes)
}
