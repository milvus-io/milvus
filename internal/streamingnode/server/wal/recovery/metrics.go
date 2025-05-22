package recovery

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func newRecoveryStorageMetrics(channelInfo types.PChannelInfo) *recoveryMetrics {
	constLabels := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     channelInfo.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(channelInfo.Term, 10),
	}
	return &recoveryMetrics{
		constLabels:            constLabels,
		info:                   metrics.WALRecoveryInfo.MustCurryWith(constLabels),
		inconsistentEventTotal: metrics.WALRecoveryInconsistentEventTotal.With(constLabels),
		isOnPersisting:         metrics.WALRecoveryIsOnPersisting.With(constLabels),
		inMemTimeTick:          metrics.WALRecoveryInMemTimeTick.With(constLabels),
		persistedTimeTick:      metrics.WALRecoveryPersistedTimeTick.With(constLabels),
		truncateTimeTick:       metrics.WALTruncateTimeTick.With(constLabels),
	}
}

type recoveryMetrics struct {
	constLabels            prometheus.Labels
	info                   *prometheus.GaugeVec
	inconsistentEventTotal prometheus.Counter
	isOnPersisting         prometheus.Gauge
	inMemTimeTick          prometheus.Gauge
	persistedTimeTick      prometheus.Gauge
	truncateTimeTick       prometheus.Gauge
}

// ObserveStateChange sets the state of the recovery storage metrics.
func (m *recoveryMetrics) ObserveStateChange(state string) {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	m.info.WithLabelValues(state).Set(1)
}

func (m *recoveryMetrics) ObServeInMemMetrics(tickTime uint64) {
	m.inMemTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObServePersistedMetrics(tickTime uint64) {
	m.persistedTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObServeTruncateMetrics(tickTime uint64) {
	m.truncateTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObserveInconsitentEvent() {
	m.inconsistentEventTotal.Inc()
}

func (m *recoveryMetrics) ObserveIsOnPersisting(onPersisting bool) {
	if onPersisting {
		m.isOnPersisting.Set(1)
	} else {
		m.isOnPersisting.Set(0)
	}
}

func (m *recoveryMetrics) Close() {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInconsistentEventTotal.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryIsOnPersisting.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInMemTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryPersistedTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALTruncateTimeTick.DeletePartialMatch(m.constLabels)
}
