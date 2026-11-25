package adaptor

import (
	"context"
	"strconv"
	"testing"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAlterWALUpdatesWriterMetrics(t *testing.T) {
	for _, failSwitch := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "marker_fenced"}[failSwitch], func(t *testing.T) {
			oldID := pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))
			targetID := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
			var assertInfo func(...message.WALName)
			old := &recoveryPhysicalWAL{name: message.WALNamePulsar, WALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
				if msg.MessageType() == message.MessageTypeAlterWAL {
					// Opening the target and writing its anchor must not publish it yet.
					assertInfo(message.WALNamePulsar)
					if failSwitch {
						return nil, walimpls.ErrFenced
					}
				}
				finishFenceTestAppend(ctx, 1)
				return oldID, nil
			})}
			target := &recoveryPhysicalWAL{name: message.WALNameWoodpecker, WALImpls: newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				finishFenceTestAppend(ctx, 2)
				return targetID, nil
			})}
			auto := autowalimpls.New(old, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
				return target, nil
			})
			w := newFenceTestWAL(t, auto)
			registry := prometheus.NewRegistry()
			registry.MustRegister(metrics.WALInfo)
			assertInfo = func(names ...message.WALName) {
				families, err := registry.Gather()
				require.NoError(t, err)
				actual := make(map[string]float64)
				for _, family := range families {
					for _, sample := range family.Metric {
						labels := make(map[string]string)
						for _, label := range sample.Label {
							labels[label.GetName()] = label.GetValue()
						}
						if labels[metrics.WALChannelLabelName] == w.Channel().Name &&
							labels[metrics.NodeIDLabelName] == paramtable.GetStringNodeID() &&
							labels[metrics.WALChannelTermLabelName] == strconv.FormatInt(w.Channel().Term, 10) {
							actual[labels[metrics.WALNameLabelName]] = sample.Gauge.GetValue()
						}
					}
				}
				expected := make(map[string]float64)
				for _, name := range names {
					expected[name.String()] = 1
				}
				require.Equal(t, expected, actual)
			}
			assertInfo(message.WALNamePulsar)
			insert := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(99).WithLastConfirmed(oldID)
			_, err := w.Append(context.Background(), insert)
			require.NoError(t, err)
			_, err = w.Append(context.Background(), newFenceTestAlterWALMessage().WithLastConfirmed(oldID))
			if failSwitch {
				require.Error(t, err)
				assertInfo(message.WALNamePulsar)
				// Later requests rejected by fencing or shutdown must not publish the target either.
				_, err = w.Append(context.Background(), newFenceTestAlterWALMessage().WithLastConfirmed(oldID))
				require.Error(t, err)
				assertInfo(message.WALNamePulsar)
				w.lifetime.SetState(typeutil.LifetimeStateStopped)
				_, err = w.Append(context.Background(), newFenceTestAlterWALMessage().WithLastConfirmed(oldID))
				require.Error(t, err)
				assertInfo(message.WALNamePulsar)
			} else {
				require.NoError(t, err)
				assertInfo(message.WALNameWoodpecker)
				// A redundant request still leaves one gauge for the active backend.
				_, err = w.Append(context.Background(), newFenceTestAlterWALMessage().WithTimeTick(101).WithLastConfirmed(targetID))
				require.NoError(t, err)
				assertInfo(message.WALNameWoodpecker)
				_, err = w.Append(context.Background(), message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(102).WithLastConfirmed(targetID))
				require.NoError(t, err)
				require.Equal(t, float64(2), testutil.ToFloat64(metrics.WALAppendMessageTotal.WithLabelValues(
					paramtable.GetStringNodeID(), w.Channel().Name, message.MessageTypeInsert.String(), metrics.WALStatusOK)))
				histogram := &dto.Metric{}
				require.NoError(t, metrics.WALAppendMessageDurationSeconds.WithLabelValues(
					paramtable.GetStringNodeID(), w.Channel().Name, metrics.WALStatusOK).(prometheus.Metric).Write(histogram))
				require.EqualValues(t, 4, histogram.Histogram.GetSampleCount(), "switches preserve earlier append observations")
			}
			w.writeMetrics.Close()
			assertInfo()
		})
	}
}
