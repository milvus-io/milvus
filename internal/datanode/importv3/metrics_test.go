// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func histogramSampleCount(t *testing.T, o prometheus.Observer) uint64 {
	m, ok := o.(prometheus.Metric)
	require.True(t, ok)
	pb := &dto.Metric{}
	require.NoError(t, m.Write(pb))
	return pb.GetHistogram().GetSampleCount()
}

func TestObserveReshardRunReportsBytesAndStages(t *testing.T) {
	const nodeID = int64(987654321)
	node := strconv.FormatInt(nodeID, 10)
	m := NewMetrics(nodeID)

	logical := metrics.DataNodeImportV3ReshardBytes.WithLabelValues(node, reshardBytesLogical)
	written := metrics.DataNodeImportV3ReshardBytes.WithLabelValues(node, reshardBytesWritten)
	spill := metrics.DataNodeImportV3ReshardBytes.WithLabelValues(node, reshardBytesSpill)
	rows := metrics.DataNodeImportV3ReshardRows.WithLabelValues(node)
	route := metrics.DataNodeImportV3ReshardStageLatency.WithLabelValues(node, reshardStageRoute)

	logicalBefore := testutil.ToFloat64(logical)
	writtenBefore := testutil.ToFloat64(written)
	spillBefore := testutil.ToFloat64(spill)
	rowsBefore := testutil.ToFloat64(rows)
	routeBefore := histogramSampleCount(t, route)

	m.ObserveReshardRun(ReshardRunObservation{
		Rows:         42,
		LogicalBytes: 100, WrittenBytes: 60, SpillBytes: 40,
		ReadWait: 2 * time.Millisecond, RouteCost: 3 * time.Millisecond,
		FlushBlock: 4 * time.Millisecond, FlushWall: 5 * time.Millisecond,
		PrepareRead: time.Millisecond, PrepareNormalize: time.Millisecond,
		PrepareFunctions: time.Millisecond, PrepareSend: time.Millisecond,
		SortRead: time.Millisecond, Sort: time.Millisecond, SortWrite: time.Millisecond,
	})

	require.Equal(t, logicalBefore+100, testutil.ToFloat64(logical))
	require.Equal(t, writtenBefore+60, testutil.ToFloat64(written))
	require.Equal(t, spillBefore+40, testutil.ToFloat64(spill))
	require.Equal(t, rowsBefore+42, testutil.ToFloat64(rows))
	require.Equal(t, routeBefore+1, histogramSampleCount(t, route))
}

func TestSetTaskStatesResetsStaleSeries(t *testing.T) {
	const nodeID = int64(987654322)
	node := strconv.FormatInt(nodeID, 10)
	m := NewMetrics(nodeID)

	m.SetTaskStates(map[string]map[datapb.ImportTaskStateV2]int{
		"import": {datapb.ImportTaskStateV2_InProgress: 1},
	})
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.DataNodeImportV3Tasks.WithLabelValues(node, "import", "InProgress")))

	m.SetTaskStates(map[string]map[datapb.ImportTaskStateV2]int{
		"import": {datapb.ImportTaskStateV2_Completed: 1},
	})
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.DataNodeImportV3Tasks.WithLabelValues(node, "import", "InProgress")),
		"a series absent from the new stats must be reset")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.DataNodeImportV3Tasks.WithLabelValues(node, "import", "Completed")))
}
