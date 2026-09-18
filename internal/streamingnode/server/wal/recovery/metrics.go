package recovery

import (
	"strconv"
	"time"

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
		constLabels:            constLabels,
		info:                   metrics.WALRecoveryInfo.MustCurryWith(constLabels),
		inconsistentEventTotal: metrics.WALRecoveryInconsistentEventTotal.With(constLabels),
		isOnPersisting:         metrics.WALRecoveryIsOnPersisting.With(constLabels),
		inMemTimeTick:          metrics.WALRecoveryInMemTimeTick.With(constLabels),
		persistedTimeTick:      metrics.WALRecoveryPersistedTimeTick.With(constLabels),
		oldestSplittedAge:      metrics.WALRecoveryOldestSplittedVChannelAgeSeconds.With(constLabels),
	}
}

type recoveryMetrics struct {
	constLabels            prometheus.Labels
	info                   *prometheus.GaugeVec
	inconsistentEventTotal prometheus.Counter
	isOnPersisting         prometheus.Gauge
	inMemTimeTick          prometheus.Gauge
	persistedTimeTick      prometheus.Gauge
	oldestSplittedAge      prometheus.Gauge
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

// ObserveOldestSplittedVChannel sets the age, at now, of the oldest SPLITTED
// vchannel still held by the recovery storage, from its split time tick
// (T_switch). splitTimeTick is 0 when there is none, which sets the age to 0.
func (m *recoveryMetrics) ObserveOldestSplittedVChannel(splitTimeTick uint64, now time.Time) {
	if splitTimeTick == 0 {
		m.oldestSplittedAge.Set(0)
		return
	}
	m.oldestSplittedAge.Set(ageSeconds(splitTimeTick, now))
}

// ObserveTruncationLag sets the lag, at now, of the flusher checkpoint that
// bounds WAL truncation. A nil checkpoint (some vchannel has no flusher
// checkpoint yet, or there is no vchannel) means truncation has no flusher
// bound to lag behind, so the series is removed rather than reported as 0.
func (m *recoveryMetrics) ObserveTruncationLag(flusherCheckpoint *WALCheckpoint, now time.Time) {
	if flusherCheckpoint == nil {
		metrics.WALRecoveryTruncationLagSeconds.Delete(m.constLabels)
		return
	}
	metrics.WALRecoveryTruncationLagSeconds.With(m.constLabels).Set(ageSeconds(flusherCheckpoint.TimeTick, now))
}

// ageSeconds returns how long before now the physical part of tick is, never
// negative (a tick allocated by a clock slightly ahead of this node).
func ageSeconds(tick uint64, now time.Time) float64 {
	age := now.Sub(tsoutil.PhysicalTime(tick)).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func (m *recoveryMetrics) Close() {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInconsistentEventTotal.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryIsOnPersisting.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInMemTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryPersistedTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryOldestSplittedVChannelAgeSeconds.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryTruncationLagSeconds.DeletePartialMatch(m.constLabels)
}
