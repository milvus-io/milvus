# Refined Look-Aside Balancer: Fixing Sub-Millisecond Herding (B + C)

Current state: Implemented

ISSUE: Reproduction linked to #48211 (SelectNode fast-path defect); user reports that under the default policy a single node receives about 33% more traffic than its peers.

Keywords: replica selection, look_aside, herding, workload score, EWMA, quantization

Released: 2.6.x / 3.0

## Summary

The `calculateScore` / `UpdateCostMetrics` logic of `LookAsideBalancer` suffers from two defects that, in sub-millisecond workloads, make the steering loop either never converge (permanent stickiness to one node) or mis-trigger on rounding noise. This design introduces `RefinedLookAsideBalancer`, a new replica-selection policy that applies two minimal fixes:

- **B: Service-time quantization floor.** When a sub-millisecond request's `ServiceTime` is truncated to 0, the workload term collapses to 0, the score loses its load correlation, and steering never converges. Floor the service time used by the workload term at 1ms to restore the score's response to `executingNQ`, giving the steering loop a termination condition.
- **C: EWMA smoothing of cost metrics.** The score currently uses the last single `ResponseTime`/`ServiceTime` sample; millisecond-rounding noise and outliers mis-trigger steering. Smooth the per-node service/response time with an exponential weighted moving average to reduce false triggers and oscillation.

`SelectNode`'s idx control logic (the #36181 steering semantics) is NOT changed, no proto change is introduced, and the original `LookAsideBalancer` is left untouched. All changes are confined to `internal/proxy/shardclient` plus one new proxy config item. Applies to 2.6 and 3.0.

## Motivation

### Symptom

With the default `replicaSelectionPolicy=look_aside`, sub-millisecond workloads show one node receiving about 33% more requests than its peers; switching to `round_robin` improves the balance.

### Root cause (two defects stacking)

Score formula (`look_aside_balancer.go`):

```go
executeSpeed = ResponseTime - ServiceTime
workload     = pow3(1 + TotalNQ + executingNQ) * ServiceTime
score        = executeSpeed + workload
```

1. **Quantization defect (fix B).** Metrics are reported in integer milliseconds (querynode side `tr.ElapseSpan().Milliseconds()`, see `internal/querynodev2/services.go` and `tasks/query_task.go`). When a single processing takes ~1ms, `ServiceTime` is frequently truncated to 0, so `workload` is always 0 and the score does not react to `executingNQ`. Since `executingNQ` is the only term that grows with load in the steering loop, the convergence condition `diff <= tolerance` is never met and steering stays stuck on one node — even though #36181 defines the intended behavior as "select the target node with smallest score in every assignment", the loop loses its termination condition in this quantized scenario.

2. **Noise defect (fix C).** The score uses the last completed request's single sample. `executeSpeed = ResponseTime - ServiceTime` is the difference of two independently millisecond-rounded values; under sub-millisecond loads the ±1ms rounding error exceeds the real signal, producing 50%-100% phantom score differences between nodes and entering steering while the nodes are actually balanced. A single outlier sample can dominate the score for a long time.

### Why not touch idx (contrast with rejected approach A)

`SelectNode` does not advance idx when it diverges (it keeps selecting the min-score node), which is #36181's deliberate design: when unbalanced, actively steer traffic to the idlest node until it recovers. This design does not change idx; it fixes the two defects that made the closed loop "never stop / mis-trigger", letting the intended loop self-recover.

## Public Interfaces

- New config `proxy.workloadMetricSmoothingFactor` (float, default `0.2`, `refreshable:"false"`), the EWMA smoothing coefficient for fix C.
- New replica-selection policy value `refined_look_aside` selectable via `proxy.replicaSelectionPolicy`. Default stays `look_aside`; no behavior change for existing deployments.
- No proto change, no external interface change; `CostAggregation` field semantics unchanged.

## Design Details

### Changed / added files

| File | Change |
|---|---|
| `internal/proxy/shardclient/refined_look_aside_balancer.go` | New `RefinedLookAsideBalancer`; `RefinedCostMetrics` with EWMA-smoothed fields; `UpdateCostMetrics` does EWMA; `calculateScore` uses smoothed values + the 1ms service-time floor |
| `internal/proxy/shardclient/refined_look_aside_balancer_test.go` | Score tests (floor, smoothing, sub-millisecond), steering-convergence tests, smoothing-factor fallback test |
| `internal/proxy/shardclient/lb_policy.go` | Register `refined_look_aside` policy |
| `pkg/util/paramtable/component_param.go` | New `WorkloadMetricSmoothingFactor` param |
| `pkg/util/paramtable/component_param_test.go` | Default-value assertion |

`RefinedLookAsideBalancer` embeds the existing `LookAsideBalancer` and reuses its health-check loop, round-robin index and node-reachability state, so the two balancers cannot drift apart on shared logic. Only the scoring-related behavior is overridden (`SelectNode`, `UpdateCostMetrics`, `CancelWorkload`, `calculateScore`).

### B: Service-time quantization floor

In `calculateScore`, the floor is applied to the service time used by the **workload term only**; `executeSpeed` keeps the real (smoothed) service time so rounding error is not amplified:

```go
const minServiceTimeMs int64 = 1

func (b *RefinedLookAsideBalancer) calculateScore(node int64, cost *internalpb.CostAggregation,
	executingNQ int64, serviceTime int64, responseTime int64) int64 {
	pow3 := func(n int64) int64 { return n * n * n }

	// no valid sample (including the only-TotalNQ case): keep the original behavior
	if cost == nil || responseTime == 0 {
		return pow3(executingNQ)
	}

	// executeSpeed keeps the real (smoothed) service time
	executeSpeed := responseTime - serviceTime
	if executingNQ < 0 { /* original guard */ return executeSpeed }
	if cost.GetTotalNQ() < 0 { /* original guard */ return executeSpeed }

	// B: quantization floor, applied to the workload term only
	if serviceTime < minServiceTimeMs {
		serviceTime = minServiceTimeMs
	}

	workload := pow3(1+cost.GetTotalNQ()+executingNQ) * serviceTime
	if workload < 0 {
		return math.MaxInt64
	}
	return executeSpeed + workload
}
```

Key points:

- The floor only affects the workload term; `executeSpeed` uses the real smoothed value.
- `responseTime == 0` still takes the original early return, preventing nodes with no data from producing phantom scores.
- The floor is a constant rather than a config: its semantics are tied to the millisecond quantization unit, and exposing it would add surface for no benefit.

**Termination argument.** After the floor, the node being persistently steered to accumulates `executingNQ`; `pow3(1+TotalNQ+executingNQ)*1` grows cubically, its score rises, the check branch switches to another node, the multi-node score spread falls back inside `tolerance`, and `idx.Inc()` exits steering back to round-robin. The loop goes from "never converges" to "converges in a bounded number of steps".

### C: EWMA smoothing of cost metrics

`RefinedCostMetrics` stores the raw cost plus two EWMA-smoothed values kept in **float64** (packed with `math.Float64bits` into an `atomic.Uint64`):

```go
type RefinedCostMetrics struct {
	cost               atomic.Pointer[internalpb.CostAggregation]
	serviceTimeSmooth  atomic.Uint64 // math.Float64bits of EWMA smoothed service time (ms)
	responseTimeSmooth atomic.Uint64 // math.Float64bits of EWMA smoothed response time (ms)
	executingNQ        atomic.Int64
	ts                 atomic.Int64
}
```

`UpdateCostMetrics` writes the raw sample and updates the smoothed values:

```go
func (b *RefinedLookAsideBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	if cost == nil {
		return
	}
	// mirror raw metrics to the embedded balancer so the health loop stays fresh
	b.LookAsideBalancer.UpdateCostMetrics(node, cost)
	metrics, ok := b.refinedMetricsMap.Get(node)
	if !ok {
		metrics = &RefinedCostMetrics{}
		b.refinedMetricsMap.Insert(node, metrics)
	}
	nowTs := time.Now().UnixMilli()
	// seed the smoothed value with the fresh sample when the previous one has
	// expired (or never existed): 0 cannot mark "unseeded" because it is
	// ServiceTime's normal value. Otherwise apply the EWMA.
	if nowTs-metrics.ts.Load() > b.metricExpireInterval {
		metrics.serviceTimeSmooth.Store(math.Float64bits(float64(cost.GetServiceTime())))
		metrics.responseTimeSmooth.Store(math.Float64bits(float64(cost.GetResponseTime())))
	} else {
		metrics.serviceTimeSmooth.Store(math.Float64bits(
			b.ewma(math.Float64frombits(metrics.serviceTimeSmooth.Load()), float64(cost.GetServiceTime()))))
		metrics.responseTimeSmooth.Store(math.Float64bits(
			b.ewma(math.Float64frombits(metrics.responseTimeSmooth.Load()), float64(cost.GetResponseTime()))))
	}
	metrics.cost.Store(cost)
	metrics.ts.Store(nowTs)
}

// ewma computes prev + (sample-prev) * alpha, kept in float64.
func (b *RefinedLookAsideBalancer) ewma(prev, sample float64) float64 {
	return prev + (sample-prev)*b.metricSmoothingFactor
}
```

Key points:

- **float64 precision.** An integer-domain EWMA truncates every step smaller than 5ms (with alpha 0.2) to 0, so a node reporting 1-4ms would stay at 0 forever and `calculateScore` would take the `responseTime == 0` early return — the floor would never apply to the exact ~1ms workload this fix targets. Keeping the smoothed value in float64 preserves sub-millisecond precision. At the scoring boundary the smoothed values are rounded to integer milliseconds (`int64(math.Round(...))`).
- **Seeding.** A fresh sample seeds the smoothed value directly when the previous sample has expired (`now - ts > metricExpireInterval`) or never existed (`ts == 0`). 0 cannot serve as the "unseeded" marker because it is ServiceTime's normal value: seeding on `prev == 0` would let a single 20ms outlier after a run of 0ms samples jump the smoothed value straight to 20 (undamped), and would re-trigger once a long run of 0s underflows the float back to 0. The ts-based rule also keeps a long-idle node's stale value out of its first fresh sample.
- **executingNQ is not smoothed** — it is a real-time in-flight gauge; smoothing it would slow the loop's response.
- The smoothing coefficient is read at construction (`refreshable:"false"`, like `checkWorkloadRequestNum` and `workloadToleranceFactor`), default `0.2`, and validated to `(0,1]`; an out-of-range value falls back to the default (`0.2`).

`SelectNode`'s check branch reads the smoothed values and passes them to `calculateScore`:

```go
executingNQ := metrics.executingNQ.Load()
if executingNQ != 0 || nowTs-metrics.ts.Load() <= b.metricExpireInterval {
	score = b.calculateScore(node, metrics.cost.Load(), executingNQ,
		b.ms(metrics.serviceTimeSmooth.Load()), b.ms(metrics.responseTimeSmooth.Load()))
}
```

The metric-expiry rule (`nowTs - ts <= metricExpireInterval`) is unchanged, preventing stale smoothed values from being used when there is no queued traffic.

**Availability.** Because the smoothed state lives in `refinedMetricsMap`, `SelectNode` also keeps an entry in the embedded balancer's `metricsMap` on every selection. Otherwise the inherited health loop could only mark a node unavailable if it had already returned cost metrics, and a node that never returned any (e.g. the proxy restarted while the query node is down) would never be skipped.

**Eviction of smoothed state.** The embedded health loop removes offline nodes from its own maps and from `knownNodeInfos`, but it cannot reach `refinedMetricsMap` (method dispatch on the embedded receiver). Without a removal path the map would grow with historical node IDs on QN restarts or scaling. The refined balancer therefore runs a periodic sweep (`refinedMetricsCleanupInterval`) that removes any refined entry whose node is no longer in `knownNodeInfos`. A node removed here is permanently gone (evicted after repeated health-check failures), so an in-flight request on it completes and its `CancelWorkload` becomes a no-op — the same behavior as the original balancer, whose metrics entry is removed on eviction too.

### Parameter definition (component_param.go, after `WorkloadToleranceFactor`)

```go
p.WorkloadMetricSmoothingFactor = ParamItem{
	Key:          "proxy.workloadMetricSmoothingFactor",
	Version:      "2.6.x",
	DefaultValue: "0.2",
	Doc:          "EWMA smoothing factor for query node cost metrics used by refined_look_aside replica selection. Lower value smooths more but reacts slower; range (0,1].",
}
p.WorkloadMetricSmoothingFactor.Init(base.mgr)
```

#### On choosing alpha

- The EWMA's effective memory window is roughly `1/alpha` samples. alpha=0.5 remembers only the last ~2 requests, too weak to suppress millisecond rounding jitter; load-balancer practice (Finagle Peak EWMA, Envoy outlier detection, gRPC least_request) usually uses 0.1-0.3.
- Default 0.2 (memory window ~5 requests): sub-millisecond noise is sufficiently flattened while slow real load drift (rising `ServiceTime`) is still tracked within a few requests.
- The trade-off is one-directional: smaller alpha smooths more but reacts slower. If burst overload reaction is too slow, increase alpha; if individual nodes still jitter-stick, decrease it. No code change needed.
- A future "smooth but burst-aware" option could use Finagle's Peak EWMA (keep the historical peak); out of scope here.

### Expected behavior

- Sub-millisecond balanced scenario: the score is computed from smoothed values with the workload term floored at 1ms; steering is not mis-triggered by rounding noise, and when it is triggered it converges and exits within a bounded number of steps.
- Real overload scenario: `executingNQ`/`ServiceTime` raise the score, and steering still moves traffic away from the overloaded node — the load-awareness of look-aside is preserved.

## Compatibility, Deprecation, and Migration Plan

- No compatibility issue: the parameter is new, the default matches current behavior (`ServiceTime >= 1` produces the same formula result; `ServiceTime = 0` changes from "score has no load correlation" to "score grows with load", which is the intended fix).
- The original `LookAsideBalancer` is byte-for-byte unchanged; existing unit-test expectations are untouched. The new policy is opt-in via `proxy.replicaSelectionPolicy = refined_look_aside`.
- 2.6 and 3.0 are both targeted; no migration steps.

## Test Plan

1. **Unit tests (calculateScore)**:
   - Floor: `ServiceTime=0, ResponseTime>0, executingNQ>0` → score grows cubically with `executingNQ` (before the fix it was a constant).
   - Original branches (no sample, negative NQ, overflow, huge TotalNQ) keep their expected values.
   - Sub-millisecond: a node reporting `ResponseTime=1, ServiceTime=0` must NOT hit the `responseTime == 0` early return, so the floor applies.
   - Smoothing: alternating low/high samples converge toward the middle; a single outlier (e.g. `ResponseTime=100`) does not make the score jump.
2. **Unit tests (steering convergence / herding)**: construct the discriminating sub-millisecond scenario — three nodes `ServiceTime=0` with different `ResponseTime` (1/2/2) and ~6 requests in flight on the lowest-RT node — call `SelectNode` repeatedly and assert no single node is persistently selected. A control test runs the identical scenario on the original `LookAsideBalancer` and asserts it herds all requests to the lowest-RT node (`300/0/0`), proving the test covers the fix. A second test asserts an overloaded node is shed while overloaded and receives traffic again once its in-flight requests drain.
3. **Parameter tests**: `component_param_test.go` asserts the default value; invalid values (`0`, `2`) fall back to the default.
4. **Health-loop availability test**: a node that never returned cost metrics (only the `SelectNode` entry exists) is marked unavailable after the configured number of failed health checks and is no longer selected.
5. **Eviction test**: the smoothed metrics of a node evicted by the health loop (removed from `knownNodeInfos`) are dropped by the sweep, while live nodes' entries are kept.
6. **System-level verification (optional)**: compare per-node request distribution and the `ProxyWorkLoadScore` metric under sub-millisecond/short-latency load; confirm the spread falls inside tolerance with no permanent stickiness.

## Rejected Alternatives

### A. Fix idx stickiness — advance idx unconditionally (rejected)

**Change**: make `SelectNode` always call `b.idx.Inc()`, returning to round-robin rhythm right after selecting the min-score node.

**Why rejected**:

1. **Breaks the look-aside semantics.** #36181 states that once the spread exceeds tolerance, the policy should pick the min-score node in *every assignment*. Keeping idx static is exactly how the steering loop is implemented. A would degrade it to "round-robin plus a min-score hint every 10 requests" and real overload could no longer be corrected.
2. **Does not touch the root cause.** A merely bypasses the loop; the quantized "score has no load correlation" defect (fix B) would remain.
3. **Misplaced coverage.** A changes the average behavior of all scenarios, while the problem is confined to the sub-millisecond/quantized pathological scenario. The right fix is to repair the pathological case's convergence, not to give up the reactive policy.

### B (original design variant). Directly modify `LookAsideBalancer` (rejected for this PR)

**Change**: put the floor and the EWMA straight into the existing `LookAsideBalancer`, with smoothing factor 1 preserving today's behavior.

**Why rejected for this PR**: the two fixes change scoring behavior at `ServiceTime = 0`; putting them in place behind the default policy changes production behavior for all `look_aside` users at once, with no rollback knob short of a version downgrade. A separate `refined_look_aside` policy keeps the default unchanged, ships the fix behind an explicit opt-in flag, and lets operators compare the two on the same cluster before switching. The concern behind B — the two balancers drifting apart — is addressed by embedding `LookAsideBalancer` and overriding only the scoring-related methods. Promoting `refined_look_aside` to the default can be considered as a follow-up once the refined policy is validated at system level.

### D. Microsecond time precision (deferred, long-term)

Add high-precision fields to `CostAggregation` or change its unit. Rejected as the primary approach:

- Cross-component, involves proto and mixed-version compatibility, high engineering cost;
- Pure unit scaling does not change relative score differences — herding remains; higher resolution exposes more real jitter and only works combined with B/C;
- For the sub-millisecond pathological scenario, B (floor) + C (smoothing) already cover the root cause.

### Alternative: document `round_robin` as the sub-millisecond recommendation (partially adopted)

Workable as a low-cost, zero-code-risk workaround, but it "avoids" rather than "fixes" and cannot address balancing under real overload. After B/C land, the `shardclient/README.md` should note the scenarios where `round_robin` is preferable.

## References

- Suresh et al., *Don't Mind the Queue: Load-Aware Load Balancing in datacenter networks*, NSDI'15 (score formula source). URL: `https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/suresh`
- Robert G. Brown, *Smoothing, Forecasting and Prediction of Discrete Time Series*, Prentice-Hall, 1963 (EWMA: `S_t = alpha*x_t + (1-alpha)*S_{t-1}`)
- S. W. Roberts, "Control Chart Tests Based on Geometric Moving Averages", Technometrics, 1959 (EWMA control charts for mean-shift detection)
- Marius Eriksen, *Your Server as a Function*, Twitter, 2013 (Finagle load balancer, Peak EWMA + Power of Two Choices)
- gRPC `least_request` LB (C-core: EWMA-smooth backend-reported load, pick lowest; same data flow as this design)
- Envoy outlier detection EWMA success-rate smoothing. URL: `https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier.html`
- Jacobson, 1988; RFC 6298 (TCP smoothed RTT: `SRTT = alpha*RTT + (1-alpha)*SRTT`). URL: `https://www.rfc-editor.org/rfc/rfc6298`
- PR #36181 Optimize workload based replica selection policy (checkRequestNum/toleranceFactor, defines steering semantics)
- PR #48212 / #48220 correct SelectNode fast-path loop and tolerance division by zero (baseline)
- PR #45018 extract shard client logic into dedicated package
