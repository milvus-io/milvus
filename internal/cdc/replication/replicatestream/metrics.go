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

package replicatestream

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	streamingpb "github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	message "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ReplicateMetrics interface {
	StartReplicate(msg message.ImmutableMessage)
	OnSent(msg message.ImmutableMessage)
	OnConfirmed(msg message.ImmutableMessage)
	OnConnect()
	OnDisconnect()
	OnReconnect()
}

type msgMetrics struct {
	tr *timerecord.TimeRecorder
}

type replicateMetrics struct {
	replicateInfo *streamingpb.ReplicatePChannelMeta
	msgsMetrics   *typeutil.ConcurrentMap[string, msgMetrics] // message id -> msgMetrics
}

func NewReplicateMetrics(replicateInfo *streamingpb.ReplicatePChannelMeta) ReplicateMetrics {
	return &replicateMetrics{
		replicateInfo: replicateInfo,
		msgsMetrics:   typeutil.NewConcurrentMap[string, msgMetrics](),
	}
}

func (m *replicateMetrics) StartReplicate(msg message.ImmutableMessage) {
	msgID := msg.MessageID().String()
	m.msgsMetrics.Insert(msgID, msgMetrics{
		tr: timerecord.NewTimeRecorder("replicate_msg"),
	})
}

func (m *replicateMetrics) OnSent(msg message.ImmutableMessage) {
	sourceChannel := m.replicateInfo.GetSourceChannelName()
	targetChannel := m.replicateInfo.GetTargetChannelName()
	msgType := msg.MessageType().String()
	metrics.CDCReplicatedMessagesTotal.WithLabelValues(
		sourceChannel,
		targetChannel,
		msgType,
	).Inc()
	metrics.CDCReplicatedBytesTotal.WithLabelValues(
		sourceChannel,
		targetChannel,
		msgType,
	).Add(float64(msg.EstimateSize()))
}

func (m *replicateMetrics) OnConfirmed(msg message.ImmutableMessage) {
	msgMetrics, ok := m.msgsMetrics.GetAndRemove(msg.MessageID().String())
	if !ok {
		return
	}

	replicateDuration := msgMetrics.tr.RecordSpan()
	metrics.CDCReplicateEndToEndLatency.WithLabelValues(
		m.replicateInfo.GetSourceChannelName(),
		m.replicateInfo.GetTargetChannelName(),
	).Observe(float64(replicateDuration.Milliseconds()))

	now := time.Now()
	confirmedTime := tsoutil.PhysicalTime(msg.TimeTick())
	lag := now.Sub(confirmedTime)
	metrics.CDCReplicateLag.WithLabelValues(
		m.replicateInfo.GetSourceChannelName(),
		m.replicateInfo.GetTargetChannelName(),
	).Set(float64(lag.Milliseconds()))
}

func (m *replicateMetrics) OnConnect() {
	metrics.CDCStreamRPCConnections.WithLabelValues(
		m.replicateInfo.GetTargetCluster().GetClusterId(),
		metrics.CDCStatusConnected,
	).Inc()
}

func (m *replicateMetrics) OnDisconnect() {
	targetClusterID := m.replicateInfo.GetTargetCluster().GetClusterId()
	metrics.CDCStreamRPCConnections.WithLabelValues(
		targetClusterID,
		metrics.CDCStatusConnected,
	).Dec()
	metrics.CDCStreamRPCConnections.WithLabelValues(
		targetClusterID,
		metrics.CDCStatusDisconnected,
	).Inc()
}

func (m *replicateMetrics) OnReconnect() {
	targetClusterID := m.replicateInfo.GetTargetCluster().GetClusterId()
	metrics.CDCStreamRPCConnections.WithLabelValues(
		targetClusterID,
		metrics.CDCStatusDisconnected,
	).Dec()
	metrics.CDCStreamRPCConnections.WithLabelValues(
		targetClusterID,
		metrics.CDCStatusConnected,
	).Inc()

	metrics.CDCStreamRPCReconnectTimes.WithLabelValues(
		targetClusterID,
	).Inc()
}
