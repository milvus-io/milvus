# Safe Configuration Projections and Management Mutations

## Document Information

- Date: 2026-08-26
- Status: Draft for review
- Author: @liliu-z
- Components: configuration manager, ParamTable, management HTTP API, streaming WAL
- Related issue: [#49846](https://github.com/milvus-io/milvus/issues/49846)
- Related authorization change: [#52580](https://github.com/milvus-io/milvus/pull/52580)

## 1. Summary

Milvus configuration currently mixes three audiences in one manager: runtime
consumers need exact values, diagnostic surfaces need a safe projection, and
the management API needs a constrained mutation interface. Reusing the raw
view at an external boundary can expose process-environment entries,
credentials, infrastructure topology, or the manager's deletion tombstone.

This design makes those audiences explicit:

- existing raw getters retain their compatibility contract;
- effective internal views omit deletion markers and inert overlays;
- external projections omit undeclared keys and redact sensitive values;
- one mutation-policy function resolves key identity once and applies all
  management restrictions in a stable order;
- request-derived configuration maps are logged by count, never by key or
  value.

## 2. Goals

1. Keep credentials and protected topology out of configuration dumps, HTTP
   responses, and logs.
2. Prevent process environment variables imported by `EnvSource` from becoming
   an accidental public configuration namespace.
3. Give separator variants such as `a.b`, `a/b`, and `A_B` the same
   registration, sensitivity, immutability, and etcd-write verdict.
4. Preserve existing in-process consumers of raw configuration.
5. Keep management mutation rules independent of HTTP request parsing.
6. Make omissions in sensitivity metadata detectable by tests.

## 3. Non-Goals

- This change does not add authentication or authorization to the management
  routes. That boundary is handled by #52580.
- It does not encrypt configuration at rest.
- It does not change configuration-source priority.
- It does not cache projections. These views are used by diagnostics and
  management operations, where avoiding invalidation races is more important
  than optimizing a small, infrequent full-table walk.
- It does not remove the ability to delete an open-ended sensitive
  `ParamGroup` member. Deletion is retained as cleanup because it removes the
  high-priority value and reveals only a lower-priority value already present
  in the process. Sensitive scalar deletes remain rejected.

## 4. Domain Model

### 4.1 Declared Configuration

A `ParamItem` declares one scalar key. A `ParamGroup` declares an open-ended
prefix. Source maps may contain other entries, especially because `EnvSource`
imports the entire process environment; those entries are source
implementation detail, not Milvus configuration.

### 4.2 Key Identity

One key travels as a `resolvedKey` containing:

- `lookup`: the separator-free identity used by sources and scalar lookups;
- `dotted`: the namespace-preserving identity used for prefix policy;
- `kind`: scalar, group member, or unknown;
- `segmented`: whether the namespace segmentation was endorsed by a
  declaration or source rather than invented by the caller.

Both identities remain together throughout classification. Re-deriving one
from arbitrary caller spelling would allow aliases to receive different
security verdicts. If two dotted spellings collapse to one lookup identity,
the manager records the collision and fails closed instead of choosing one by
map iteration order.

### 4.3 Sensitivity

Sensitive values include:

- credentials and private key material;
- values that govern access or impersonation;
- topology capable of redirecting credential-bearing traffic, including all
  parts of a connection target such as host/IP and port.

Explicit `Sensitive` and `NonSensitive` declarations take precedence. Dynamic
groups default to their prefix policy and may expose only reviewed leaf
suffixes. A name-pattern classifier is a final fail-closed defense for
undeclared or plugin-defined names; it is not the primary inventory.

## 5. Read Interfaces

| Interface | Values | Tombstones/inert overlays | Undeclared keys | Intended caller |
|---|---|---|---|---|
| `GetConfigs`, `GetBy` | raw | included for compatibility | included | legacy internal code |
| `GetEffectiveBy` | raw | omitted | included if selected | runtime aggregate consumers |
| `ProjectConfigs`, `ProjectBy` | sensitive values masked | omitted | omitted | diagnostics and external projection |
| `GetConfigsView` | sensitive values masked with source | omitted | omitted | source-annotated diagnostics |
| `GetRegisteredConfig` | non-sensitive raw value | omitted | rejected | management point lookup |

`ParamGroup.GetValue` uses `GetEffectiveBy`. A tombstone means that a runtime
override was deleted; it is never a literal configuration value.

The management GET handler maps `ErrKeySensitive` to the stable redaction value
`*****`. It continues to reject undeclared keys so a caller cannot enumerate
the process environment.

## 6. Mutation Policy

`EvaluateConfigMutation` is the single policy entry point for generic external
mutations. It resolves the canonical key once, then rejects in this order:

1. security-governing settings;
2. `mq.type`, which has a dedicated WAL transition protocol;
3. immutable settings;
4. unregistered sets;
5. sensitive sets and sensitive scalar deletes.

Allowed requests are deduplicated using `EtcdConfigKey`, the identity the
transaction actually writes. This prevents two separator variants from
targeting the same etcd entry in one request.

The policy module returns a decision rather than HTTP strings. The transport
owns status codes and response compatibility; the policy owns key identity and
security semantics.

## 7. Logging

Configuration values are passed through manager redaction before logging.
Maps carried by `AlterWAL` are different: both their keys and values are
caller-controlled request payload and the map is persisted into recovery
state. WAL broadcast, recovery, and callback logs therefore emit only a
configuration count.

Broadcast IDs use the well-known `FieldBroadcastID` constructor with their
native `uint64` type. RPC propagation has a distinct unsigned metadata tag so
values above `MaxInt64` round-trip without sign loss.

## 8. Compatibility

- Raw manager APIs remain raw and keep their historical tombstone behavior.
- `ParamGroup` changes only for deleted or otherwise inert runtime overlays;
  live values and source priority are unchanged.
- The management GET response keeps its ordered per-key shape. Sensitive keys
  now carry `value: "*****"`; undeclared and missing keys keep error entries.
- Kafka's printable configuration form intentionally masks credentials. It is
  diagnostic output, not a serialization interface.
- Adding `GetEffectiveBy` and the mutation decision types is additive.
- `FieldBroadcastID` now matches the existing protobuf/domain type `uint64`.
  No production caller currently propagates this field through RPC metadata;
  regular logging keeps the same `broadcastID` key.

## 9. Performance and Concurrency

Projection walks remain O(number of manager entries) and allocate the returned
map. They do not mutate the manager and use its concurrent maps and policy
registries. Management reads and diagnostic dumps are low-frequency relative
to query and insert paths. A cache would add invalidation obligations across
source refreshes, runtime overlays, key declarations, and policy registration;
without evidence that projection time is material, that tradeoff is not
justified.

Key resolution for caller input uses the uncached formatter so arbitrary HTTP
keys cannot grow the process-global formatting cache without bound.

## 10. Verification

Tests cover:

- all spellings of scalar and group keys;
- source-backed and environment-only group members;
- collision handling and suffix-exemption fail-closed behavior;
- tombstone removal from the effective `ParamGroup` view;
- positive inventories of sensitive scalars, groups, direct prefixes, and all
  parts of representative connection targets;
- management set/delete decisions and redacted GET responses;
- WAL recovery log fields containing neither payload keys nor values;
- full-range unsigned BroadcastID propagation.

The positive inventory is intentionally reviewed data. Heuristic tests remain
a second line of defense, but cannot prove that a new innocuous-looking
topology field was classified correctly.
