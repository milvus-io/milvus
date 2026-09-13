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
- It does not audit application-wide backend connection diagnostics or logs
  emitted inside external plugins. Logging changes cover configuration views,
  management payloads, and the configuration initialization/refresh boundaries
  described below.
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

Learning a spelling must not retroactively make an unsegmented value eligible
for a public suffix exemption. For each complete source generation, the manager
first learns all supplied spellings, then permanently marks identities with no
endorsed spelling ambiguous before publishing values. This history is independent
of the ownership index, which CREATE events update later. It survives value
removal so delayed events and later spellings cannot reclassify an earlier
generation. Initial pulls and runtime overlays record the same history. This
also covers a new lower-priority file entry or overlay that would otherwise
endorse a higher-priority value. Established spellings continue to apply to
ordinary environment and etcd overrides. Startup loads the initial local file
snapshot before importing environment overrides so file-backed group members
have their spellings established first. Source priority still gives environment
values precedence over files. A spelling first introduced by a later refresh
cannot endorse an existing unsegmented environment, persisted, or overlay value.

### 4.3 Sensitivity

Sensitive values include:

- credentials and private key material;
- values that govern access or impersonation;
- topology capable of redirecting credential-bearing traffic, including all
  parts of a connection target such as host/IP and port;
- transport and trust controls, including TLS enablement, certificate and CA
  paths, minimum TLS versions, authentication mechanisms, SNI, and object-store
  provider, routing, identity, and payload-signing options.

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

Read visibility and permission to change a setting are separate decisions.
Readable authorization settings, including `builtinRoles.enable` and
`builtinRoles.roles`, are fenced against generic writes and deletes, as are the
authentication plugin selector and its initialization-failure policy. Configured
role grants are applied when the coordinator initializes, so a setting does not
need to be immediately refreshable to require this protection.

## 7. Logging

Configuration values are passed through manager redaction before logging.
Maps carried by `AlterWAL` are different: both their keys and values are
caller-controlled request payload and the map is persisted into recovery
state. WAL broadcast, recovery, and callback logs therefore emit only a
configuration count.

The same rule applies to the etcd mutation transaction, its refresh events,
and configuration callbacks. Dynamic group names are omitted from manager
event logs. JSON decode errors can contain request snippets, and URL parse
errors can contain credentials, so configuration parser diagnostics must not
attach those raw errors. Access-log formatter validation describes the
required key shape without echoing the rejected name. File-backed scalar
event diagnostics use the declared key name and the value's sensitivity policy.

Callback and plugin initialization errors are opaque: even a public timing
setting can trigger a reload that reads a full credential-bearing configuration
map. Milvus omits their raw error text at initialization and reload log/panic
boundaries while preserving callback arguments, error propagation, and failure
behavior. Initial and periodic YAML parsing diagnostics likewise omit parser
input fragments before any sensitivity metadata has been applied.

Native cipher-plugin initialization follows the same rule. Milvus omits the
configured library path and plugin-provided name from startup logs. Loader
failures identify the failed stage without the path or `dlerror` text, including
the output that `ThrowInfo` emits before throwing. The plugin initialization C
boundary also replaces opaque exception text before invoking the shared
untyped-exception observer or returning a CStatus. Existing native codes,
out-of-memory handling, untyped-exception counting, and startup failure behavior
remain; verbatim native initialization error text does not. This keeps ordinary
Go C-status logging and downstream QueryNode/DataNode logs and StreamingNode
panic messages safe without changing other native operation diagnostics.

Remote configuration initialization also omits raw etcd client errors, which
can carry certificate/key/CA paths or rejected TLS settings. The immediate
shared etcd client constructors and config-source constructor log endpoint
counts and embedded-mode information; they omit connection targets, TLS
settings, and authentication settings. Client arguments and returned errors
remain unchanged. This constructor coverage does not extend to unrelated
operational etcd diagnostics throughout the application.
Immutable configuration persistence at startup masks the composite etcd key
because it contains the protected root prefix; the declared configuration key
and its value continue to use the existing diagnostic policy.

Broadcast IDs use the well-known `FieldBroadcastID` constructor with their
native `uint64` type. RPC propagation has a distinct unsigned metadata tag so
values above `MaxInt64` round-trip without sign loss.

## 8. Compatibility

- Raw manager APIs remain raw and keep their historical tombstone behavior.
- `ParamGroup` changes only for deleted or otherwise inert runtime overlays;
  live values and source priority are unchanged. Deleting or resetting through
  an alias removes every stored overlay spelling of that identity; raw delete
  views continue to carry tombstones.
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
map. Safe readers hold a manager snapshot read lock across classification and
value reads. Built-in file and etcd sources publish the complete generation's
spelling policy and values under the corresponding write lock, acquired before
the source lock. The policy includes the history of unsegmented publications,
including values whose CREATE events have not reached the ownership index.
Runtime overlay mutations use the same publication lock.
This prevents a reader from applying an old public classification to a newly
published sensitive value, including the interval before refresh events reach
the manager. Raw runtime getters keep their existing locking behavior.

Sources already refresh while configuration declarations initialize. Scalar
primary and fallback sensitivity metadata is therefore installed before any
of their declarations become visible. Dynamic groups and the directly declared
cluster TLS/authority prefixes likewise publish policy before their namespace.
The hook table installs its sensitive empty namespace before declaring the
`soPath` scalar that inherits that policy. Reviewed scalar overrides and group
suffix exemptions retain their final public-read behavior.

Source refreshes, cache eviction, and event callbacks run outside the snapshot
lock, since they can call safe getters. In particular, file projection triggers
its refresh before taking the read lock. Management reads and diagnostic dumps
are low-frequency relative to query and insert paths. A cache would add
invalidation obligations across
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
- file and etcd publication before event dispatch, point reads racing a
  refresh, and lower-priority spelling changes that must not expose an
  existing unsegmented value; both source-order directions and overlay spelling
  changes during delayed CREATE dispatch, including reset/fallback to the
  earlier value and retained ambiguity after removal;
- tombstone removal from the effective `ParamGroup` view;
- overlay deletion and reset across dotted, slash, underscore, and folded
  aliases;
- positive inventories of sensitive scalars, groups, direct prefixes, and all
  parts of representative connection targets;
- management set/delete decisions and redacted GET responses;
- WAL recovery log fields containing neither payload keys nor values;
- real etcd mutation/event logs and access-log initialization failure logs
  containing neither request-name nor request-value canaries;
- malformed management JSON, protected URL/JSON parsing, and configuration
  callback failure logs omitting payload fragments;
- actual remote configuration startup failures for missing TLS certificate,
  key, and CA files and invalid minimum TLS versions, with authentication on
  and off, preserving original in-process error details while keeping
  protected values out of the entire initialization log sequence;
- immutable configuration startup writes and create-if-absent races keeping
  the protected etcd root prefix out of logs while preserving the pinned value;
- actual scalar/fallback, group, hook, and cluster-prefix initialization paused
  during metadata registration, with real file refreshes keeping protected
  values out of logs and projections throughout the publication interval;
- missing native plugin libraries keeping their paths out of pre-throw stdout,
  CStatus messages, and Go startup logs; isolated native loader probes also
  cover missing factories, null factories, opaque factory/name exceptions,
  out-of-memory and unknown exceptions, and successful loading;
- full-range unsigned BroadcastID propagation.

The positive inventory is intentionally reviewed data. Heuristic tests remain
a second line of defense, but cannot prove that a new innocuous-looking
topology field was classified correctly.
