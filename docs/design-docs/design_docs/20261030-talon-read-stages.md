# Talon read-stage selection

## Configuration and scope

`common.storage.talon.mode` remains 0 (disabled), 1 (eligible reads), or 2
(eligible small reads). `common.storage.talon.enabledStages` selects eligible
operations: `all` is the backward-compatible default; `load_open` enables only
QueryNode load-reader open metadata, including footers and schema metadata.
Data reads after open, index loads, ordinary manifests/stats, and background
reads use origin storage under `load_open`.

YAML lists and comma-separated values are accepted. Empty values, unknown names,
and combining `all` with another stage are rejected. Restart is required.

```yaml
common:
  storage:
    talon:
      mode: 1
      enabledStages: [load_open]
      coordinator: talon-coordinator:7000
```

Only LoadOpen (`1U << 0`) is a defined operation bit. Future operations receive
new independent bits; there is no OtherRead catchall. All is UINT32_MAX. Raw
mask zero from older senders/initializers retains legacy All behavior. The mask
stays within Milvus and never enters milvus-storage properties.

`enableForExternalTable` remains an additional gate, default false. Remote
external sources require it as well as a matching stage and enabled mode. Local
filesystems never route through Talon. Explicit DataCoord prewarming remains
separately controlled by `talonPrewarm.enabled`.

## Transport and default filesystems

Raw mode and mask travel together through paramtable, indexpb.StorageConfig,
indexcgopb/clusteringpb.StorageConfig, CStorageConfig, and native
StorageConfig/StorageV2FSCache::Key. All three protobuf schemas use uint32 field
25 for `talon_enabled_stages`. Converters preserve the request's values; they do
not consult a receiving worker's local policy.

Default filesystems are shared by arbitrary operations, so only All (or legacy
zero) permits their configured Talon mode. Any restricted mask, including an
unrecognized future bit, leaves the default FS disabled. A future operation's
entry point must explicitly select its own bit rather than enabling unrelated
users of the default FS.

`MakeInternalPropertiesFromStorageConfig` accepts only CStorageConfig and emits
ordinary FS properties. It does not accept a current-stage argument or emit any
open override. `LoonFFIPropertiesSingleton::Init` caches this ordinary base and a
small raw `TalonReadConfig` snapshot separately; it does not classify callers as
LoadOpen. The snapshot contains only mode, mask, and external-table enablement,
not duplicated provider clients or credentials.

## Load-local properties

Only the actual load-reader construction sites call `MakeLoadProperties`:

- Sealed external `LoadColumnGroups`, before external-spec alias injection.
- Sealed internal `PrepareLoadDiffForReopen`, for normal and lazy readers.
- Growing `LoadColumnsGroups`, after the ordinary manifest read.

The function copies the base, checks the LoadOpen bit, and adds
`reader.open.fs.talon.mode` only to that private copy when it differs from the
ordinary mode. The private external open-mode value is consumed during alias
injection to form `reader.open.extfs.<alias>.talon.mode`. External and local-FS
gates still apply. Mode zero adds no overrides. Base properties remain unchanged.

Reader clones, async chunk readers, lazy materialization, and reload retain the
load reader's private properties. Ordinary singleton users (manifest access,
JSON stats, TEXT LOB reads, writers, and default FS access) receive only the base.

## Storage open lifecycle

Storage understands generic `reader.open.fs.*` / `reader.open.extfs.*` overrides,
not Talon stages. Underlying clients remain in the existing shared FS cache;
reader-local wrappers select open/data sources independently. Successful open
switches retained handles to data routing before metadata or readers are
published. Providers are not destroyed by the routing transition.

Parquet and Vortex sync/async readers finish this phase. Metadata reuse retains a
finished scope; encrypted Parquet reconstruction creates a fresh open scope for
its decryptors. Iceberg/Paimon delegated Parquet/Vortex files share these paths.
Iceberg delete rows remain ordinary reads.

Vortex local-format loading bypasses the normal format-reader factory. Its
`VortexColumnGroup` explicitly uses the generic open wrapper for
`VortexFooterReader::Open` (footer, schema, and zonemap metadata), finishes it
afterward, and keeps the original source
FS for Cell fills. The pinned footer implementation only uses its source input
handle during Open.

Lance, Paimon's native Rust FileIO paths, and legacy packed-reader paths remain
on origin under a restricted mask. No new acceleration is claimed for them;
All preserves their existing behavior.

## Dependency and compatibility

This change requires milvus-storage support for generic reader-open filesystem
overrides and switching retained file handles after Open. The currently pinned
storage revision `3bc763b7` does not include that support; the dependency update
must be coordinated separately before this feature can be merged and released.
No compiler, toolchain, build-script, or dependency-pin changes are included here.

Milvus's internal CStorageConfig and native config layouts change, so matching
native libraries must be rebuilt. The storage Reader/Writer API does not receive
a Talon mask or stage name. Older Milvus senders omit the mask and retain legacy
All behavior; older receivers cannot enforce restricted stages. Upgrade the
participating components and restart before enabling load_open.

## Validation

The property tests cover modes 0/1/2, legacy/load/unknown/combined/All masks,
private-copy isolation, local storage, and the external-table gate. Protocol
round-trip tests verify that the mask survives all three StorageConfig schemas.
The Vortex source-counter regression checks open metadata versus later Cell
reads using independent filesystems. Native runtime verification with the
matching storage dependency remains required before merge.
