package metrics

import "github.com/prometheus/client_golang/prometheus"

// Scheduler diagnostics deliberately use only bounded labels; handles are bound
// once per scheduler, never looked up for a task or merge candidate.
var (
	QueryNodeSchedulerDiagnosticEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_events_total",
		Help: "Local scheduler events, with explicit groups, requests or nq units; not client outcomes.",
	}, []string{nodeIDLabelName, "policy", "kind", "event", "unit"})
	QueryNodeSchedulerDiagnosticDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_duration_ms",
		Help: "Scheduler stage wall time, per input for add and per scheduling group otherwise.", Buckets: []float64{1, 10, 50, 200, 500, 1000},
	}, []string{nodeIDLabelName, "policy", "kind", "stage"})
	QueryNodeSchedulerDiagnosticSlack = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_slack_ms",
		Help: "Earliest original deadline minus observation time; negative means overdue, absent deadlines excluded.", Buckets: []float64{0, 10, 50, 200, 500},
	}, []string{nodeIDLabelName, "policy", "kind", "stage"})
	QueryNodeSchedulerDiagnosticGap = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_gap_ms",
		Help: "Absolute arrival/deadline gaps; candidate gaps include only checks reached by the original merge path.", Buckets: []float64{10, 25, 50, 100},
	}, []string{nodeIDLabelName, "policy", "kind", "stage"})
	QueryNodeSchedulerDiagnosticShape = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_shape",
		Help: "Search group requests/NQ by lifecycle stage, or receive batch/merge scan/requery selection streak sizes.", Buckets: []float64{1, 4, 8, 16},
	}, []string{nodeIDLabelName, "policy", "stage", "unit"})
	QueryNodeSchedulerDiagnosticMerge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_merge_total",
		Help: "Merge checks by first reached failure; candidate failures are not final input merge failures.",
	}, []string{nodeIDLabelName, "policy", "reason"})
	QueryNodeSchedulerDiagnosticCandidateDeadline = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_candidate_deadline_total",
		Help: "Candidate deadline checks: non-cumulative gap buckets or sum in nanoseconds; sum bucket counts for total checks.",
	}, []string{nodeIDLabelName, "policy", "stat"})
	QueryNodeSchedulerDiagnosticCost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_execute_total",
		Help: "Execute count or wall nanoseconds, by bounded request group size and returned outcome; not CPU time.",
	}, []string{nodeIDLabelName, "policy", "kind", "batch", "outcome", "unit"})
	QueryNodeSchedulerDiagnosticChildren = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_children_total",
		Help: "Merged child contexts at Done, separated by whether the parent group returned an error.",
	}, []string{nodeIDLabelName, "policy", "context", "group_failed"})
	QueryNodeSchedulerDiagnosticQueue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: milvusNamespace, Subsystem: "querynode", Name: "scheduler_diagnostic_groups",
		Help: "Policy queue current/peak, selected-not-started, running groups, or current/peak consecutive requery selections.",
	}, []string{nodeIDLabelName, "policy", "kind", "stat"})
)

func registerSchedulerDiagnostics(registry *prometheus.Registry) {
	registry.MustRegister(QueryNodeSchedulerDiagnosticEvents, QueryNodeSchedulerDiagnosticDuration,
		QueryNodeSchedulerDiagnosticSlack, QueryNodeSchedulerDiagnosticGap, QueryNodeSchedulerDiagnosticShape,
		QueryNodeSchedulerDiagnosticMerge, QueryNodeSchedulerDiagnosticCost,
		QueryNodeSchedulerDiagnosticChildren, QueryNodeSchedulerDiagnosticQueue)
	registry.MustRegister(QueryNodeSchedulerDiagnosticCandidateDeadline)
}
