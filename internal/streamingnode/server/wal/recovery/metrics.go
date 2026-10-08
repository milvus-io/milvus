package recovery

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newRecoveryStorageMetrics(channelInfo types.PChannelInfo) *recoveryMetrics {
	constLabels := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     channelInfo.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(channelInfo.Term, 10),
	}
	return &recoveryMetrics{
		constLabels:       constLabels,
		info:              metrics.WALRecoveryInfo.MustCurryWith(constLabels),
		isOnPersisting:    metrics.WALRecoveryIsOnPersisting.With(constLabels),
		observedTimeTick:  metrics.WALRecoveryObservedTimeTick.With(constLabels),
		inMemTimeTick:     metrics.WALRecoveryInMemTimeTick.With(constLabels),
		persistedTimeTick: metrics.WALRecoveryPersistedTimeTick.With(constLabels),
		tailBytes:         metrics.WALRecoveryTailBytes.With(constLabels),
		blockingBytes:     metrics.WALRecoveryBlockingBytes.With(constLabels),
		publishLagBytes:   metrics.WALRecoveryPublishLagBytes.With(constLabels),
	}
}

type recoveryMetrics struct {
	constLabels       prometheus.Labels
	info              *prometheus.GaugeVec
	isOnPersisting    prometheus.Gauge
	observedTimeTick  prometheus.Gauge
	inMemTimeTick     prometheus.Gauge
	persistedTimeTick prometheus.Gauge
	tailBytes         prometheus.Gauge
	blockingBytes     prometheus.Gauge
	publishLagBytes   prometheus.Gauge
}

// ObserveStateChange sets the state of the recovery storage metrics.
func (m *recoveryMetrics) ObserveStateChange(state string) {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	m.info.WithLabelValues(state).Set(1)
}

func (m *recoveryMetrics) ObServeInMemMetrics(tickTime uint64) {
	m.inMemTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObserveObservedTimeTick(tickTime uint64) {
	m.observedTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObServePersistedMetrics(tickTime uint64) {
	m.persistedTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObserveTailBytes(tail, blocking, publishLag uint64) {
	m.tailBytes.Set(float64(tail))
	m.blockingBytes.Set(float64(blocking))
	m.publishLagBytes.Set(float64(publishLag))
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
	metrics.WALRecoveryIsOnPersisting.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryObservedTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInMemTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryPersistedTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryTailBytes.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryBlockingBytes.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryPublishLagBytes.DeletePartialMatch(m.constLabels)
}
