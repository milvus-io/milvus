package recovery

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// hasSeries reports whether vec currently holds a series carrying every label
// in labels, without creating one the way With/WithLabelValues would.
func hasSeries(t *testing.T, vec *prometheus.GaugeVec, labels prometheus.Labels) bool {
	t.Helper()
	ch := make(chan prometheus.Metric, 1024)
	vec.Collect(ch)
	close(ch)
	for m := range ch {
		pb := &dto.Metric{}
		require.NoError(t, m.Write(pb))
		matched := 0
		for _, pair := range pb.GetLabel() {
			if v, ok := labels[pair.GetName()]; ok && v == pair.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return true
		}
	}
	return false
}

func tickAgo(now time.Time, d time.Duration) uint64 {
	return tsoutil.ComposeTSByTime(now.Add(-d))
}

func newTruncationPinTestVChannel(name string, state streamingpb.VChannelState, splitTimeTick uint64, cpID int64, cpTimeTick uint64) *vchannelRecoveryInfo {
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:       name,
			State:          state,
			SplitTimeTick:  splitTimeTick,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
		flusherCheckpoint: &WALCheckpoint{MessageID: rmq.NewRmqID(cpID), TimeTick: cpTimeTick},
	}
}

// TestRecoveryStorageObservesTruncationPinMetrics: the oldest SPLITTED
// vchannel age and the truncation lag are set from the live recovery state,
// the age falls back to 0 once no SPLITTED vchannel is left, the lag series is
// removed while the pchannel has no flusher bound, and both series are
// removed when the recovery storage closes.
func TestRecoveryStorageObservesTruncationPinMetrics(t *testing.T) {
	channel := types.PChannelInfo{Name: "truncation-pin-metrics-channel", Term: 3}
	rs := newRecoveryStorage(channel, &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1})
	labels := rs.metrics.constLabels
	now := time.Now()

	rs.vchannels = map[string]*vchannelRecoveryInfo{
		// The older split source: its frozen flusher checkpoint is the
		// pchannel minimum by message id, so it is the truncation bound.
		"v0": newTruncationPinTestVChannel("v0", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, tickAgo(now, time.Hour), 5, tickAgo(now, 30*time.Minute)),
		"v1": newTruncationPinTestVChannel("v1", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, tickAgo(now, 10*time.Minute), 10, tickAgo(now, 5*time.Minute)),
		"v2": newTruncationPinTestVChannel("v2", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0, 20, tickAgo(now, time.Minute)),
		// A DROPPED vchannel with a stale split tick is not a pending split.
		"v3": newTruncationPinTestVChannel("v3", streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, tickAgo(now, 5*time.Hour), 30, tickAgo(now, time.Minute)),
	}
	rs.vchannels["v1"].meta.Retired = true

	rs.observeTruncationPinMetrics()
	age := testutil.ToFloat64(metrics.WALRecoveryOldestSplittedVChannelAgeSeconds.With(labels))
	assert.GreaterOrEqual(t, age, time.Hour.Seconds())
	assert.Less(t, age, (time.Hour + time.Minute).Seconds())
	lag := testutil.ToFloat64(metrics.WALRecoveryTruncationLagSeconds.With(labels))
	assert.GreaterOrEqual(t, lag, (30 * time.Minute).Seconds())
	assert.Less(t, lag, (31 * time.Minute).Seconds())

	// The older source is collected: the retired one is now the oldest.
	delete(rs.vchannels, "v0")
	rs.observeTruncationPinMetrics()
	age = testutil.ToFloat64(metrics.WALRecoveryOldestSplittedVChannelAgeSeconds.With(labels))
	assert.GreaterOrEqual(t, age, (10 * time.Minute).Seconds())
	assert.Less(t, age, (11 * time.Minute).Seconds())

	// No SPLITTED vchannel left: the age is 0. A vchannel without a flusher
	// checkpoint leaves truncation without a flusher bound: no lag series.
	delete(rs.vchannels, "v1")
	rs.vchannels["v2"].flusherCheckpoint = nil
	rs.observeTruncationPinMetrics()
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.WALRecoveryOldestSplittedVChannelAgeSeconds.With(labels)))
	assert.False(t, hasSeries(t, metrics.WALRecoveryTruncationLagSeconds, labels))

	// The background loop observes both on start, and Close removes them.
	rs.vchannels["v2"].flusherCheckpoint = &WALCheckpoint{MessageID: rmq.NewRmqID(20), TimeTick: tickAgo(now, time.Minute)}
	go rs.backgroundTask()
	assert.Eventually(t, func() bool {
		return hasSeries(t, metrics.WALRecoveryTruncationLagSeconds, labels)
	}, 10*time.Second, 10*time.Millisecond)
	rs.Close()
	assert.False(t, hasSeries(t, metrics.WALRecoveryOldestSplittedVChannelAgeSeconds, labels))
	assert.False(t, hasSeries(t, metrics.WALRecoveryTruncationLagSeconds, labels))
}

// TestRecoveryMetricsAgeNeverNegative: a tick from a clock slightly ahead of
// this node reports age 0, not a negative age.
func TestRecoveryMetricsAgeNeverNegative(t *testing.T) {
	now := time.Now()
	assert.Equal(t, float64(0), ageSeconds(tsoutil.ComposeTSByTime(now.Add(time.Minute)), now))
}

// TestVChannelRecoveryInfoSplitTimeTickIfSplitted: only a SPLITTED vchannel,
// retired or not, reports its split time tick.
func TestVChannelRecoveryInfoSplitTimeTickIfSplitted(t *testing.T) {
	for _, tc := range []struct {
		state   streamingpb.VChannelState
		retired bool
		want    bool
	}{
		{streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, false, true},
		{streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, true, true},
		{streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, false, false},
		{streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, true, false},
	} {
		info := &vchannelRecoveryInfo{meta: &streamingpb.VChannelMeta{State: tc.state, Retired: tc.retired, SplitTimeTick: 42}}
		tick, ok := info.SplitTimeTickIfSplitted()
		assert.Equal(t, tc.want, ok, tc.state.String())
		if tc.want {
			assert.Equal(t, uint64(42), tick)
		}
	}
}
