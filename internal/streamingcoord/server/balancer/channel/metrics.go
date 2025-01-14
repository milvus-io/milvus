package channel

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func newPChannelMetrics() *channelMetrics {
	constLabel := prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
	return &channelMetrics{
		pchannelInfo:      metrics.StreamingCoordPChannelInfo.MustCurryWith(constLabel),
		assignmentVersion: metrics.StreamingCoordAssignmentVersion.With(constLabel),
	}
}

type channelMetrics struct {
	pchannelInfo      *prometheus.GaugeVec
	assignmentVersion prometheus.Gauge
}

// RemovePChannelStatus removes the pchannel status metric
func (m *channelMetrics) RemovePChannelStatus(assigned types.PChannelInfoAssigned) {
	m.pchannelInfo.Delete(prometheus.Labels{
		metrics.WALChannelLabelName:     assigned.Channel.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(assigned.Channel.Term, 10),
		metrics.StreamingNodeLabelName:  strconv.FormatInt(assigned.Node.ServerID, 10),
	})
}

// AssignPChannelStatus assigns the pchannel status metric
func (m *channelMetrics) AssignPChannelStatus(meta *streamingpb.PChannelMeta) {
	m.pchannelInfo.With(prometheus.Labels{
		metrics.WALChannelLabelName:     meta.GetChannel().GetName(),
		metrics.WALChannelTermLabelName: strconv.FormatInt(meta.GetChannel().GetTerm(), 10),
		metrics.StreamingNodeLabelName:  strconv.FormatInt(meta.GetNode().GetServerId(), 10),
	}).Set(float64(meta.GetState()))
}

// UpdateAssignmentVersion updates the assignment version metric
func (m *channelMetrics) UpdateAssignmentVersion(version int64) {
	m.assignmentVersion.Set(float64(version))
}
