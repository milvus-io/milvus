package channel

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func newPChannelMetrics() *channelMetrics {
	constLabel := prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
	return &channelMetrics{
		pchannelInfo:      metrics.StreamingCoordPChannelInfo.MustCurryWith(constLabel),
		vchannelTotal:     metrics.StreamingCoordVChannelTotal.MustCurryWith(constLabel),
		assignmentVersion: metrics.StreamingCoordAssignmentVersion.With(constLabel),
	}
}

type channelMetrics struct {
	pchannelInfo      *prometheus.GaugeVec
	vchannelTotal     *prometheus.GaugeVec
	assignmentVersion prometheus.Gauge
}

// UpdateVChannelTotal updates the vchannel total metric
func (m *channelMetrics) UpdateVChannelTotal(meta *PChannelMeta) {
	if !StaticPChannelStatsManager.Ready() {
		return
	}
	metrics.StreamingCoordVChannelTotal.DeletePartialMatch(prometheus.Labels{
		metrics.WALChannelLabelName: meta.Name(),
	})
	stats := StaticPChannelStatsManager.Get().GetPChannelStats(meta.ChannelID())
	m.vchannelTotal.With(prometheus.Labels{
		metrics.WALChannelLabelName:    meta.Name(),
		metrics.StreamingNodeLabelName: strconv.FormatInt(meta.CurrentServerID(), 10),
	}).Set(float64(stats.VChannelCount()))
}

// AssignPChannelStatus assigns the pchannel status metric
func (m *channelMetrics) AssignPChannelStatus(meta *PChannelMeta) {
	metrics.StreamingCoordPChannelInfo.DeletePartialMatch(prometheus.Labels{
		metrics.WALChannelLabelName: meta.Name(),
	})
	m.pchannelInfo.With(prometheus.Labels{
		metrics.WALChannelLabelName:     meta.Name(),
		metrics.WALChannelTermLabelName: strconv.FormatInt(meta.ChannelInfo().Term, 10),
		metrics.StreamingNodeLabelName:  strconv.FormatInt(meta.CurrentServerID(), 10),
		metrics.WALStateLabelName:       meta.State().String(),
	}).Set(1)
	m.UpdateVChannelTotal(meta)
}

// UpdateAssignmentVersion updates the assignment version metric
func (m *channelMetrics) UpdateAssignmentVersion(version int64) {
	m.assignmentVersion.Set(float64(version))
}
