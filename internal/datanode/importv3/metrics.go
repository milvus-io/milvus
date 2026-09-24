// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// The node-scoped import V3 worker observability surface. The labels constant
// for the node's lifetime (node_id) are curried once, so the scheduler and the
// task manager report semantic events instead of touching the global metric
// vectors, and the two share one label vocabulary. It mirrors the wal scanner's
// metricsutil.ScanMetrics.
const (
	taskPhaseQueue = "queue"
	taskPhaseRun   = "run"
	queueKindTasks = "tasks"
	queueKindSlots = "slots"

	reshardBytesLogical = "logical"
	reshardBytesWritten = "written"
	reshardBytesSpill   = "spill"

	reshardStageReadWait         = "read_wait"
	reshardStageRoute            = "route"
	reshardStageFlushBlock       = "flush_block"
	reshardStageFlushWall        = "flush_wall"
	reshardStagePrepareRead      = "prepare_read"
	reshardStagePrepareNormalize = "prepare_normalize"
	reshardStagePrepareFunctions = "prepare_functions"
	reshardStagePrepareSend      = "prepare_send"
	reshardStageSortRead         = "sort_read"
	reshardStageSort             = "sort"
	reshardStageSortWrite        = "sort_write"
)

type Metrics struct {
	nodeID       string
	taskLatency  prometheus.ObserverVec
	tasks        *prometheus.GaugeVec
	queue        *prometheus.GaugeVec
	reshardBytes *prometheus.CounterVec
	reshardRows  *prometheus.CounterVec
	reshardStage prometheus.ObserverVec
}

func NewMetrics(nodeID int64) *Metrics {
	constLabel := prometheus.Labels{metrics.NodeIDLabelName: strconv.FormatInt(nodeID, 10)}
	return &Metrics{
		nodeID:       constLabel[metrics.NodeIDLabelName],
		taskLatency:  metrics.DataNodeImportV3TaskLatency.MustCurryWith(constLabel),
		tasks:        metrics.DataNodeImportV3Tasks.MustCurryWith(constLabel),
		queue:        metrics.DataNodeImportV3SchedulerQueue.MustCurryWith(constLabel),
		reshardBytes: metrics.DataNodeImportV3ReshardBytes.MustCurryWith(constLabel),
		reshardRows:  metrics.DataNodeImportV3ReshardRows.MustCurryWith(constLabel),
		reshardStage: metrics.DataNodeImportV3ReshardStageLatency.MustCurryWith(constLabel),
	}
}

// importV3StateLabel is the metric-state label for a task state: the import
// task state enum value name without the proto prefix, so the
// datanode_import_v3_tasks gauge speaks the same vocabulary as the wire enum
// (InProgress, not Running).
func importV3StateLabel(state datapb.ImportTaskStateV2) string {
	return strings.TrimPrefix(state.String(), "ImportTaskStateV2_")
}

// ObserveQueueLatency observes the time a task spent waiting for a free slot.
func (m *Metrics) ObserveQueueLatency(kind string, d time.Duration) {
	m.taskLatency.WithLabelValues(kind, taskPhaseQueue).Observe(float64(d.Milliseconds()))
}

// ObserveRunLatency observes the task's Execute wall time.
func (m *Metrics) ObserveRunLatency(kind string, d time.Duration) {
	m.taskLatency.WithLabelValues(kind, taskPhaseRun).Observe(float64(d.Milliseconds()))
}

// SetQueue reports the scheduler backlog: tasks waiting for a free slot and the
// slots they reserve. Queued slots count toward used slots in QuerySlot.
func (m *Metrics) SetQueue(tasks int, slots int64) {
	m.queue.WithLabelValues(queueKindTasks).Set(float64(tasks))
	m.queue.WithLabelValues(queueKindSlots).Set(float64(slots))
}

// SetTaskStates publishes the current per-kind state distribution of started
// tasks. It first clears this node's series so a task that completed or was
// dropped does not leave a stale count behind, then applies the current stats.
func (m *Metrics) SetTaskStates(stats map[string]map[datapb.ImportTaskStateV2]int) {
	metrics.DataNodeImportV3Tasks.DeletePartialMatch(prometheus.Labels{metrics.NodeIDLabelName: m.nodeID})
	for kind, byState := range stats {
		for state, n := range byState {
			m.tasks.WithLabelValues(kind, importV3StateLabel(state)).Set(float64(n))
		}
	}
}

// ReshardRunObservation carries one finished reshard run's volume and phase
// durations. The phases overlap (the prepare stages run on their own goroutine,
// flush_wall sums concurrent writes), so they are distributions, not a
// wall-clock breakdown.
type ReshardRunObservation struct {
	Rows         int64
	LogicalBytes int64
	WrittenBytes int64
	SpillBytes   int64

	ReadWait   time.Duration
	RouteCost  time.Duration
	FlushBlock time.Duration
	FlushWall  time.Duration

	PrepareRead      time.Duration
	PrepareNormalize time.Duration
	PrepareFunctions time.Duration
	PrepareSend      time.Duration

	SortRead  time.Duration
	Sort      time.Duration
	SortWrite time.Duration
}

// ObserveReshardRun reports one finished reshard run's volume and phases.
func (m *Metrics) ObserveReshardRun(o ReshardRunObservation) {
	m.reshardRows.WithLabelValues().Add(float64(o.Rows))
	m.reshardBytes.WithLabelValues(reshardBytesLogical).Add(float64(o.LogicalBytes))
	m.reshardBytes.WithLabelValues(reshardBytesWritten).Add(float64(o.WrittenBytes))
	m.reshardBytes.WithLabelValues(reshardBytesSpill).Add(float64(o.SpillBytes))

	m.observeReshardStage(reshardStageReadWait, o.ReadWait)
	m.observeReshardStage(reshardStageRoute, o.RouteCost)
	m.observeReshardStage(reshardStageFlushBlock, o.FlushBlock)
	m.observeReshardStage(reshardStageFlushWall, o.FlushWall)
	m.observeReshardStage(reshardStagePrepareRead, o.PrepareRead)
	m.observeReshardStage(reshardStagePrepareNormalize, o.PrepareNormalize)
	m.observeReshardStage(reshardStagePrepareFunctions, o.PrepareFunctions)
	m.observeReshardStage(reshardStagePrepareSend, o.PrepareSend)
	m.observeReshardStage(reshardStageSortRead, o.SortRead)
	m.observeReshardStage(reshardStageSort, o.Sort)
	m.observeReshardStage(reshardStageSortWrite, o.SortWrite)
}

func (m *Metrics) observeReshardStage(stage string, d time.Duration) {
	m.reshardStage.WithLabelValues(stage).Observe(float64(d.Milliseconds()))
}

// Close drops every series this node reported.
func (m *Metrics) Close() {
	constLabel := prometheus.Labels{metrics.NodeIDLabelName: m.nodeID}
	metrics.DataNodeImportV3TaskLatency.DeletePartialMatch(constLabel)
	metrics.DataNodeImportV3Tasks.DeletePartialMatch(constLabel)
	metrics.DataNodeImportV3SchedulerQueue.DeletePartialMatch(constLabel)
	metrics.DataNodeImportV3ReshardBytes.DeletePartialMatch(constLabel)
	metrics.DataNodeImportV3ReshardRows.DeletePartialMatch(constLabel)
	metrics.DataNodeImportV3ReshardStageLatency.DeletePartialMatch(constLabel)
}
