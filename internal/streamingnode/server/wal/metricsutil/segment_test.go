package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSegmentAssignDMLCounters(t *testing.T) {
	paramtable.Init()
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.WALInsertRowsTotal, metrics.WALInsertBytesTotal, metrics.WALDeleteRowsTotal)
	m := NewSegmentAssignMetrics("dml-counter-test")
	t.Cleanup(m.Close)
	m.ObserveInsert(2, 20)
	m.ObserveInsert(3, 40)
	m.ObserveDelete(4)
	want := map[string]float64{"milvus_wal_insert_rows_total": 5, "milvus_wal_insert_bytes_total": 60, "milvus_wal_delete_rows_total": 4}
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, len(want))
	for _, f := range families {
		require.Equal(t, dto.MetricType_COUNTER, f.GetType())
		require.Contains(t, want, f.GetName())
		require.Len(t, f.Metric, 1)
		require.Equal(t, want[f.GetName()], f.Metric[0].GetCounter().GetValue())
	}
	m.Close()
	families, err = reg.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}
