# Per-Resource-Group Streaming Node Count Gate for Rolling Upgrade

**Related Issues and PRs:**
- Issue: https://github.com/milvus-io/milvus/issues/55838 (this enhancement)
- Motivation: https://github.com/milvus-io/milvus/issues/46980 (cluster-total SN count gate for 2.5 -> 2.6.9)
- Implementation PR: TBD
- Released: TBD

---

## 1. Background and Motivation

### 1.1 Problem Statement

Milvus rolls out upgrades blue-green style: new nodes are added alongside old
ones, replicas are migrated onto the new nodes, the new replicas become ready,
and only then the old nodes are drained and removed ("add a replica to replace
the old replica, wait until ready, then bring the old one down").

During such an upgrade of the streaming layer, StreamingNodes (SNs) come up one
at a time. Two existing knobs are insufficient to keep WAL rebalancing from
migrating channels before a replica's SN quota is present:

- `streaming.walBalancer.expectedInitialStreamingNodeNum` waits for a
  **cluster-wide total** SN count, and only **once**, before the streaming
  service is first enabled (`IsStreamingEnabledOnce`). It is a startup gate for
  the 2.5 -> 2.6.9 path. On an already-running cluster that upgrades
  2.6.x -> 2.6.y, the streaming service is already enabled, so this config
  never blocks again.
- `streaming.walBalancer.balancePolicy.allowRebalance` is a **binary manual**
  switch. It does not encode "wait until each replica/RG has enough SNs".

Concretely: a cluster has 6 SNs and each replica is expected to be served by 2
SNs (RG isolation). An operator adds a new SN as part of a blue-green upgrade.
The total-count gate is satisfied (6 >= threshold), so rebalancing proceeds even
though no individual replica's new SN quota is complete. The first SN to come
up can absorb channels that were meant to be spread only after all new SNs are
present, overloading it and defeating the blue-green intent.

### 1.2 Goals

1. **Per-replica readiness expressed at the only layer that can gate WAL
   migration**: the streaming balancer. Because the streaming side has no
   replica concept — only PChannels and Resource Groups — the gate is expressed
   **per Resource Group**. Under RG isolation (`streaming.primaryResourceGroup`
   + `streaming.strictResourceGroupIsolation.enabled`, one RG per replica) this
   is exactly per-replica.
2. **Runtime, not one-time**: re-checks every balance round and resumes
   automatically once every relevant RG reaches its expected SN count. This
   closes the 2.6.x -> 2.6.y gap left by `expectedInitialStreamingNodeNum`.
3. **Opt-in and safe by default**: new config defaults to `0` (disabled). It
   must not affect deployments that do not opt in.
4. **Preserve availability machinery**: the gate stops only the automatic
   *optimization* rebalance; new WAL assignment, failure recovery, frozen-node
   evacuation and manual transfer stay enabled — identical to the semantics of
   `allowRebalance=false`.

### 1.3 Non-Goals

- Query-side ("delegator/segment") placement is out of scope. The "new replica
  serves only after it is ready" behavior on the query side is already provided
  by the `queryInvisible` / `SetReplicasQueryVisible` machinery
  (`internal/querycoordv2/load_config_visibility.go`,
  `shard_leader_readiness.go`) and is not changed here.
- Per-*replica* (as opposed to per-*RG*) semantics on the streaming side are not
  introduced. Replicas are QueryCoord metadata; a replica's shards span multiple
  PChannels, and a PChannel carries vchannels of many collections/replicas, so a
  PChannel-level migration cannot be attributed to one replica.
- CDC/replication behavior: this is a local scheduling policy (same class as
  `allowRebalance`); it changes no WAL message and no replicated state, so no
  replication story is required.

---

## 2. Design

### 2.1 Configuration

**File**: `pkg/util/paramtable/component_param.go`

```yaml
# New under streaming/walBalancer. Default 0 = disabled (opt-in); <= 0 means off.
streaming:
  walBalancer:
    expectedStreamingNodeNumPerRG: 0   # Automatic rebalance resumes only when every RG owning RW pchannels has >= this many available SNs
```

```go
WALBalancerExpectedStreamingNodeNumPerRG ParamItem `refreshable:"true"`
// Key:     "streaming.walBalancer.expectedStreamingNodeNumPerRG"
// Default: "0"
// Export:  false
// Doc:     "Expected available streaming node number per resource group, 0 by default means disabled.
//          When set to N (> 0), the WAL balancer suspends the automatic rebalance until every
//          resource group that owns RW pchannels has at least N available streaming nodes.
//          Intended for blue-green rolling upgrades: keep new SNs from absorbing channels until
//          each RG (one RG = one replica under strictResourceGroupIsolation) has its full SN quota.
//          Failure recovery, frozen-node evacuation and manual transfer are not affected."
```

- `refreshable:"true"` — read fresh each balance round, no restart required.
- Mirror the opt-in shape of `expectedInitialStreamingNodeNum` (default `0`,
  `Export: false`, `<= 0` means off).
- `Version`: next release of each branch (2.6.x / 3.0.x / master).

### 2.2 Runtime Gate in the Balance Loop

**File**: `internal/streamingcoord/server/balancer/balancer_impl.go`, `balance()`
(master:570; 2.6/3.0: same function body).

```
balance(ctx):
  rgName = streaming.primaryResourceGroup
  nodeStatus = fetchStreamingNodeStatus(ctx, rgName)          // already fetches RG-labeled nodes internally
  expected = streaming.walBalancer.expectedStreamingNodeNumPerRG
  if expected > 0:
      availableByRG = group(GetAvailableStreamingNodes(ctx), by ResourceGroup)   // excludes frozen nodes
      for every RG that owns >= 1 RW pchannel in currentLayout:
          if availableByRG[rg].count < expected:
              currentLayout.Config.AllowRebalance = false       // pause auto-rebalance for this round only
              log rated info: rg, available, expected
  expectedLayout, err = policy.Balance(currentLayout)           // unchanged
  ...
```

Notes:

- `GetAvailableStreamingNodes` already exists (`balancer.go` interface,
  excludes frozen nodes); node RG labels come from
  `types.StreamingNodeInfoWithResourceGroup`
  (`pkg/streaming/util/types/streaming_node.go`).
- "RG that owns >= 1 RW pchannel" is computed from `CurrentPChannelsView()`
  plus the access mode currently in effect (`types.AccessModeRW`). RO pchannels
  (e.g. CDC source) do not gate, mirroring `ConfirmPrimaryResourceGroupReady`.
- `currentLayout.Config` is rebuilt every round by
  `newCommonBalancePolicyConfig()` (`policy_registry.go:21`), so the
  `refreshable` parameter takes effect without restart. The per-round
  `AllowRebalance=false` override is purely in-memory and does not touch the
  persisted `allowRebalance` config (`request.go`/REST are untouched).

### 2.3 Semantics: What the Gate Actually Pauses

Forcing `CurrentLayout.Config.AllowRebalance = false` reuses the exact existing
policy semantics (`policy/vchannelfair/vchannel_fair_policy.go:74`,
`expected_layout.go:186`):

- Initial assignment of every channel to some node: **always runs** (step 1-2).
  This is what keeps UNAVAILABLE/failed-node channels serviceable.
- Auto-rebalance (unassign + reassign for balance, step 3-4): **paused** by
  `AllowRebalance=false`.

So while an RG is below its expected SN count:

| Action | Allowed |
|---|---|
| New WAL assignment (new collection / new pchannel) | yes |
| Failure recovery (UNAVAILABLE pchannel reassignment) | yes |
| Frozen-node evacuation / manual `TransferChannels` | yes |
| Automatic optimization rebalance onto newly joined SNs | **no** |

The moment every gating RG reaches its expected available SN count, the next
balance round resumes automatic rebalancing without operator action.

---

## 3. Alternatives Considered

### 3.1 Per-Replica Gate on the Streaming Side (rejected)

Requires feeding QueryCoord replica -> channel metadata into StreamingCoord to
know which SNs serve which replica. A replica's shards span many PChannels and a
PChannel hosts many replicas' vchannels, so a PChannel-level migration cannot be
attributed to one replica. Coupling is high, semantics are muddy; per-RG under
isolation is the honest, minimal expression.

### 3.2 Extend the Cluster-Total `expectedInitialStreamingNodeNum` (rejected)

It is a one-time startup gate gated by `IsStreamingEnabledOnce()`; making it
re-run after every node join changes its documented 2.5 -> 2.6.9 contract and
still cannot express per-RG/per-replica readiness.

### 3.3 Per-Channel Check Inside the Policy (deferred)

Threading RG into `types.StreamingNodeStatus` / `CurrentLayout.AllNodesInfo` and
checking per channel in `CurrentLayout.AllowRebalance(channelID)` would be more
precise (an RG that is short only stops channels owned by that RG). It touches a
shared type used by the query side as well, so it is deferred; the global
per-round pause is conservative but safe for the opt-in use case.

---

## 4. Compatibility and Rollback

- **Default off**: `<= 0` disables the gate entirely; existing behavior and
  every existing config are unchanged.
- **Rollback**: set the value back to `0` (refreshable) — the gate stops being
  enforced on the next balance round. No metadata or persisted state changes.
- **Operator error guard**: do not set N larger than the SN capacity of a real
  RG; a permanently short RG pauses optimization rebalance indefinitely
  (recovery still works). Documented in the config Doc string.
- **Interaction with `allowRebalance`**: when the operator already set
  `allowRebalance=false`, the new gate is redundant but harmless. When the gate
  pauses, the persisted `allowRebalance` value is untouched and the REST/`PUT`
  path (`request.go`) is unaffected.

## 5. Branch Coverage

The streaming balancer code touched here is line-for-line identical across
`master`, `origin/2.6` and `origin/3.0`:

| Branch | `execute()` gate | `balance()` | config location |
|---|---|---|---|
| master | `balancer_impl.go:333` | `balancer_impl.go:570` | `component_param.go` (StreamingCfg) |
| 2.6 | `balancer_impl.go:315` | same | `component_param.go:7631` (next to existing param) |
| 3.0 | `balancer_impl.go:346` | same | `component_param.go:9103` (next to existing param) |

A single change set applies to all three; only the `Version` field of the
ParamItem differs. `master`/`3.0` additionally carry the 2.6.5 / 3.0.0 version
gates (`WaitUntilWALbasedDDLReady` / `WaitUntilSchemaDropReady`), which are
unaffected. Query-side (QueryView on master, legacy balancer on 2.6/3.0) is not
touched.

## 6. Test Plan and Verification

### 6.1 Unit Tests (`internal/streamingcoord/server/balancer/balancer_test.go`)

| Test | Coverage |
|---|---|
| `TestBalancePerRGExpectedNodeNumDisabled` | config 0 / empty: gate never fires, layout unchanged |
| `TestBalancePerRGExpectedNodeNumBlocked` | config N; an RG owning RW pchannels has < N available SNs: this round produces no auto-rebalance (AllowRebalance=false path), initial/recovery assignment still applied |
| `TestBalancePerRGExpectedNodeNumRecovered` | after RG reaches N available SNs, auto-rebalance resumes in a later round |
| `TestBalancePerRGExpectedNodeNumRONotGating` | RO-only pchannels do not gate |
| `TestBalancePerRGExpectedNodeNumFrozenExcluded` | frozen SNs do not count toward N |

### 6.2 Integration Test (`tests/integration/rollingupgrade/manual_rolling_upgrade_test.go`)

Extend the `ManualRollingUpgradeSuite` with a case:

1. RG isolation cluster, one RG per replica, replica SN quota = 2.
2. Set `streaming.walBalancer.expectedStreamingNodeNumPerRG: 2`,
   `balancePolicy.allowRebalance: true`.
3. Add one new SN: assert no channel migrates to it until the second new SN is
   added; after the second SN is up, assert WAL balancing spreads channels to
   both new SNs.

### 6.3 Verification Gate (G1/G2)

- G1 (input audit): the gate consumes only `GetAvailableStreamingNodes` (node
  identity + RG label) and the access-mode of owned pchannels — both produced in
  `streamingcoord/server/balancer`; no upstream rewriting of the count exists.
- G2 (failure modes): trace the three real paths end-to-end — (a) RG short at
  startup of the balance loop, (b) RG short mid-loop while a node dies (recovery
  must still reassign UNAVAILABLE pchannels), (c) RG reaches quota mid-loop
  (rebalance resumes). Fault-inject each in the unit tests above.
- G3: claims in the PR body limited to behaviors verified by the tests above.

## 7. References

### Code Files

- `internal/streamingcoord/server/balancer/balancer_impl.go` — `balance()`,
  `execute()`, `blockUntilExpectedInitialStreamingNodeNumReached()`
- `internal/streamingcoord/server/balancer/balancer.go` — `Balancer` interface,
  `GetAvailableStreamingNodes`
- `internal/streamingcoord/server/balancer/policy_registry.go` —
  `newCommonBalancePolicyConfig`, `CurrentLayout.AllowRebalance`
- `internal/streamingcoord/server/balancer/policy/vchannelfair/vchannel_fair_policy.go`
- `internal/streamingcoord/server/balancer/policy/vchannelfair/expected_layout.go`
- `pkg/streaming/util/types/streaming_node.go` — `StreamingNodeInfoWithResourceGroup`,
  `StreamingNodeStatus`
- `pkg/util/paramtable/component_param.go` — `StreamingCfg`
- `docs/agent_guides/streaming-system/coordination/channel_management.md`

### Configuration

- `configs/milvus.yaml` — default config file
