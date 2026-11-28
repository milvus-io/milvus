# Talon read-stage selection

## Configuration and scope

`common.storage.talon.mode` remains 0 (disabled), 1 (eligible reads), or 2
(eligible small reads). `common.storage.talon.enabledStages` selects operations:

- `all` is the backward-compatible default.
- `load_open` selects QueryNode format-reader open metadata, including footers
  and schema metadata. Subsequent data reads use the origin filesystem.
- `manifest` selects QueryNode segment-manifest reads during load. It does not
  select the data, index, or stats files referenced by a manifest.

`load_open` and `manifest` can be combined. YAML lists and comma-separated
values are accepted. Empty values, unknown names, and combining `all` with
another stage are rejected. Restart is required.

```yaml
common:
  storage:
    talon:
      mode: 1
      enabledStages: [load_open, manifest]
      coordinator: talon-coordinator:7000
```

LoadOpen is `1U << 0`; Manifest is `1U << 1`. Future operations receive new
independent bits; there is no OtherRead catchall. All is UINT32_MAX. Raw mask
zero from older senders/initializers retains legacy All behavior. The mask stays
within Milvus and never enters milvus-storage properties.

`enableForExternalTable` remains an additional gate, default false. Remote
external sources require it as well as a matching stage and enabled mode. Local
filesystems never route through Talon. Broker-backed external sources retain
origin routing. Explicit DataCoord prewarming remains separately controlled by
`talonPrewarm.enabled`.

## Transport and shared properties

Raw mode and mask travel together through paramtable, indexpb.StorageConfig,
indexcgopb/clusteringpb.StorageConfig, CStorageConfig, and native
StorageConfig/StorageV2FSCache::Key. All three protobuf schemas use uint32 field
25 for `talon_enabled_stages`. Converters preserve the request's values; they do
not consult a receiving worker's local policy.

Default filesystems are shared by arbitrary operations, so only All (or legacy
zero) permits their configured Talon mode. A restricted mask leaves unscoped
operations on origin storage. Each eligible entry point must explicitly select
its own stage instead of enabling unrelated users of the default filesystem.

`MakeInternalPropertiesFromStorageConfig` builds the ordinary base properties.
`LoonFFIPropertiesSingleton::Init` stores that immutable base and a raw
`TalonReadConfig` snapshot. `GetProperties(PropertiesOptions)` resolves the
requested stage into effective Talon mode, external mode, and open-only policy.
Equivalent effective settings share an immutable snapshot from a bounded
128-entry cache; unchanged settings reuse the base. Base-property updates
invalidate this cache while existing readers retain their snapshots. Callers
copy a snapshot before injecting collection-specific external aliases.

Go's `MakePropertiesFromStorageConfig` accepts optional stage bits and applies
the same stage selection before building FFI properties. The stage and manifest
metric origin are independent: the former selects routing; the latter selects
a fixed instrumentation population. Neither is inferred from a manifest path.

## Load reader and manifest entry points

LoadOpen is selected by sealed `LoadColumnGroups`, sealed
`PrepareLoadDiffForReopen`, and growing `LoadColumnsGroups`. Under a restricted
matching policy, the resulting properties set `fs.talon.open_only=true`.
External alias injection carries the effective mode and open-only policy into
that alias's filesystem properties, subject to the external and local gates.
Mode zero does not enable Talon.

Manifest is selected separately by:

- Native `SegmentLoadInfo::GetColumnGroups` and the growing segment's manifest
  read before creating its format reader.
- Go `NewStatsResolverFromLoadInfo`, for QueryNode manifest-backed stats lookup.
- QueryNode manifest-backed delta and compact-to overlay lookup, through
  `GetManifestHandleWithExtfs` and `GetDeltaLogPathsFromManifest`.

The manifest snapshot enables its configured Talon mode for the whole manifest
read and does not set the LoadOpen-only policy. Unscoped and DataNode
`StatsResolver` constructors do not acquire the QueryNode manifest stage.
Resolvers cache their result or error; repeated lookups do not issue another
FFI read. Metric origins such as create/preload/postsync/reopen survive the
same call chain without replacing the stage argument or duplicating observations.

Manifest properties are not reused for the returned stats, data, or index
files. Ordinary singleton users retain the base policy. Reader clones and lazy
materialization retain the properties selected for their load reader.

## Storage lifecycle and dependency

The pinned milvus-storage revision is
`95215819ea9094f7b96be0f9503346dfee0b62e5`. It supports the generic
`fs.talon.open_only` filesystem policy and `ControlTalonForFile` routing control;
it does not receive a Milvus stage bit or name. This supersedes the earlier
proposal to pass `reader.open.fs.*` overrides.

For an open-only Talon file, disabling Talon routes subsequent content reads
through its retained origin handle. Each read captures the routing flag when it
starts. For an unrestricted Talon file, the control operation leaves its
configured mode active. The filesystem cache key includes the open-only policy.
The current Parquet, Vortex, and delegated format-reader implementations control
routing at their metadata-to-data boundary. This does not prove acceleration
for every external format or every native FileIO path; those paths require their
own runtime validation.

Milvus's internal CStorageConfig and native configuration layouts change with
stage transport, so matching native libraries must be rebuilt. Older senders
omit the mask and retain legacy All behavior; older receivers cannot enforce
restricted stages. Upgrade participating components and restart before enabling
restricted stages. The observability wrappers preserve master's dependency pin,
routing rules, and error identity.

## Validation

Property tests cover disabled/full/small-read modes, legacy/load/manifest/
combined/unknown/All masks, immutable snapshot reuse and invalidation, local
storage, and external gates. Protocol round-trip tests verify the mask in all
three StorageConfig schemas. Resolver and entry-point tests verify stage
forwarding independently of metric origins, cached lookups, and property failures.
Manifest timing tests require one observation per phase with the final outcome.
These checks do not measure production latency improvement or replace native
runtime validation with the pinned storage dependency.
