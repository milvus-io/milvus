package channel

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

// AssignPChannelStatus assigns the pchannel status metric
func (m *channelMetrics) AssignPChannelStatus(meta *streamingpb.PChannelMeta) {
	metrics.StreamingCoordPChannelInfo.DeletePartialMatch(prometheus.Labels{
		metrics.WALChannelLabelName: meta.GetChannel().GetName(),
	})
	m.pchannelInfo.With(prometheus.Labels{
		metrics.WALChannelLabelName:     meta.GetChannel().GetName(),
		metrics.WALChannelTermLabelName: strconv.FormatInt(meta.GetChannel().GetTerm(), 10),
		metrics.StreamingNodeLabelName:  strconv.FormatInt(meta.GetNode().GetServerId(), 10),
		metrics.WALStateLabelName:       meta.GetState().String(),
	}).Set(1)
}

// UpdateAssignmentVersion updates the assignment version metric
func (m *channelMetrics) UpdateAssignmentVersion(version int64) {
	m.assignmentVersion.Set(float64(version))
}
