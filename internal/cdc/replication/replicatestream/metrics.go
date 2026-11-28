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
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ReplicateMetrics interface {
	UpdateLastReplicatedTimeTick(ts uint64)
	StartReplicate(msg message.ImmutableMessage)
	OnSent(msg message.ImmutableMessage)
	OnConfirmed(msg message.ImmutableMessage)
	OnInitiate()
	OnConnect()
	OnDisconnect()
	OnClose()
}

type msgMetrics struct {
	tr *timerecord.TimeRecorder
}

// Connection metrics are shared by all streams targeting the same cluster.
// Serialize owner changes and gauge updates so final cleanup cannot race a new stream.
var connectionMetrics = struct {
	sync.Mutex
	owners map[string]int
}{owners: make(map[string]int)}

type replicateMetrics struct {
	replicateInfo    *streamingpb.ReplicatePChannelMeta
	msgsMetrics      *typeutil.ConcurrentMap[string, msgMetrics] // message id -> msgMetrics
	connectionStatus string                                      // Empty until initiated and after close.
}

func NewReplicateMetrics(replicateInfo *streamingpb.ReplicatePChannelMeta) ReplicateMetrics {
	return &replicateMetrics{
		replicateInfo: replicateInfo,
		msgsMetrics:   typeutil.NewConcurrentMap[string, msgMetrics](),
	}
}

func setLastReplicatedTimeTick(source, target string, ts uint64) {
	metrics.CDCLastReplicatedTimeTick.WithLabelValues(
		source,
		target,
	).Set(tsoutil.PhysicalTimeSeconds(ts))
}

func (m *replicateMetrics) UpdateLastReplicatedTimeTick(ts uint64) {
	setLastReplicatedTimeTick(
		m.replicateInfo.GetSourceChannelName(),
		m.replicateInfo.GetTargetChannelName(),
		ts,
	)
}

// InitLastReplicatedTimeTick initializes the last replicated time tick gauge
// from a known checkpoint. Callers may seed it conservatively and later
// overwrite it with the target-confirmed position.
func InitLastReplicatedTimeTick(info *streamingpb.ReplicatePChannelMeta, ts uint64) {
	if info == nil || ts == 0 {
		return
	}
	setLastReplicatedTimeTick(info.GetSourceChannelName(), info.GetTargetChannelName(), ts)
}

// DeleteLastReplicatedTimeTick deletes the lag series for a replication task
// that has been genuinely removed.
func DeleteLastReplicatedTimeTick(info *streamingpb.ReplicatePChannelMeta) {
	if info == nil {
		return
	}
	metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(
		info.GetSourceChannelName(),
		info.GetTargetChannelName(),
	)
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

	m.UpdateLastReplicatedTimeTick(msg.TimeTick())
}

func (m *replicateMetrics) OnInitiate() {
	m.setConnectionStatus(metrics.CDCStatusDisconnected)
}

func (m *replicateMetrics) OnDisconnect() {
	m.setConnectionStatus(metrics.CDCStatusDisconnected)
}

func (m *replicateMetrics) OnConnect() {
	m.setConnectionStatus(metrics.CDCStatusConnected)
	metrics.CDCStreamRPCReconnectTimes.WithLabelValues(
		m.replicateInfo.GetTargetCluster().GetClusterId(),
	).Inc()
}

func (m *replicateMetrics) OnClose() {
	m.setConnectionStatus("")
}

func (m *replicateMetrics) setConnectionStatus(status string) {
	connectionMetrics.Lock()
	defer connectionMetrics.Unlock()

	if m.connectionStatus == status {
		return
	}
	targetClusterID := m.replicateInfo.GetTargetCluster().GetClusterId()
	if m.connectionStatus == "" {
		connectionMetrics.owners[targetClusterID]++
	} else {
		metrics.CDCStreamRPCConnections.WithLabelValues(targetClusterID, m.connectionStatus).Dec()
	}
	m.connectionStatus = status
	if status != "" {
		metrics.CDCStreamRPCConnections.WithLabelValues(targetClusterID, status).Inc()
		return
	}

	connectionMetrics.owners[targetClusterID]--
	if connectionMetrics.owners[targetClusterID] == 0 {
		delete(connectionMetrics.owners, targetClusterID)
		metrics.CDCStreamRPCConnections.DeleteLabelValues(targetClusterID, metrics.CDCStatusConnected)
		metrics.CDCStreamRPCConnections.DeleteLabelValues(targetClusterID, metrics.CDCStatusDisconnected)
	}
}
