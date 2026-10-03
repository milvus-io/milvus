# MEP: DAX-Backed Backend for mmap-Enabled Data

- **Created:** 2026-09-28
- **Author(s):** @dongukim12
- **Status:** Draft
- **Component:** QueryNode | Storage
- **Related Issues:** #53874
- **Released:** N/A

## Summary

This proposal adds an optional DAX-backed backend for data that Milvus already selects as mmap-eligible.

The existing file-backed mmap backend remains the default and preserves current behavior. When DAX is explicitly configured, a QueryNode can place mmap-eligible vector, field, and index data in a configured DAX region instead of the existing file-backed mmap path.

This proposal does not move general Milvus memory allocation to DAX. Metadata, control-plane state, temporary buffers, and ordinary runtime allocations remain in Host DRAM.

## Motivation

Milvus already supports mmap-based placement for selected field, vector, and index data. The current mmap path uses regular file-backed mappings.

On hosts with DAX-capable memory hardware exposed through a DAX device, users cannot place this mmap-eligible data in a DAX region while retaining the existing Milvus mmap workflow. A DAX backend provides an additional placement option without replacing the existing mmap behavior or requiring a new client-facing API.

The goal is a generic upstream DAX backend. The design must not depend on a specific hardware vendor or device-specific API.

## Goals and Non-goals

### Goals

- Add an optional DAX backend for data already eligible for mmap.
- Keep file-backed mmap as the default backend.
- Keep DAX-specific logic behind a backend abstraction instead of spreading device checks through segment, vector, and index code.
- Manage DAX-region initialization, allocation, release, capacity, and concurrency explicitly.
- Preserve query correctness: the same loaded data must produce equivalent query and search results with either backend.
- Fail clearly when DAX is explicitly selected but the configured DAX region is unavailable or has insufficient capacity.

### Non-goals

- Replacing all Host DRAM allocations with DAX.
- Automatic migration or tiering between Host DRAM, DAX, and SSD.
- Promotion, eviction, or cache policies.
- Persistent DAX caching across QueryNode restarts.
- Hardware-vendor-specific configuration names, APIs, or code paths.
- Automatic discovery or provisioning of DAX-capable memory hardware.
- Hardware-vendor-specific management APIs in the public Milvus interface.
- Changes to the Milvus SDK, protobuf API, collection schema, or stored-data format.

## Public Interfaces

This proposal adds node-local configuration only. It introduces no SDK or protobuf interface changes.

The proposed QueryNode configuration is:

```yaml
queryNode:
  mmap:
    backend: file # supported values: file, dax
    dax:
      path: /dev/dax0.0
```

- `file` is the default and retains the existing file-backed mmap behavior.
- `dax` enables the DAX backend. The initial implementation supports a Linux device-DAX region specified by a valid DAX device path.
- Selecting `dax` on an unsupported host, with an unavailable path, or with an invalid region causes QueryNode initialization to fail with a clear error. Milvus must not silently fall back to file-backed mmap.

The implementation may expose operational metrics for DAX region capacity, allocated bytes, available bytes, allocation failures, and allocation latency.

### Hardware integration and extensibility

The DAX backend is separated from region provisioning through an internal DAX Region Provider interface. A provider is responsible for obtaining and validating a mappable DAX region and reporting the region capacity and alignment required by the allocator.

The initial provider opens and maps a Linux device-DAX path. If future DAX-capable memory hardware requires a separate management or provisioning API, a provider can encapsulate that integration without exposing hardware-vendor-specific APIs in the public Milvus configuration or in segment, vector, and index code.

## Design Details

### Current behavior

```text
mmap-enabled data
        |
        v
file-backed mmap
```

### Proposed behavior

```text
mmap-enabled data
        |
        v
backend selection
     /       \
    v         v
file mmap     DAX backend
existing      new
                 |
                 v
        DAX Region Manager
                 |
                 v
      configured DAX region
```

Backend selection occurs only after Milvus has determined that an object is mmap-eligible. Existing mmap eligibility rules are not changed by this proposal.

The file backend continues to use the current mmap path. The DAX backend maps the configured DAX region and provides aligned allocations from that region. Both backends return a memory view that existing segment, vector, and index code can consume without knowing which backend supplied it.

### DAX Region Manager

The DAX backend owns one DAX Region Manager per configured QueryNode process. The manager is responsible for:

- opening and mapping the configured DAX region;
- reporting total capacity and available capacity;
- allocating aligned ranges;
- associating each allocation with its owning loaded object;
- releasing ranges when the corresponding segment, field, vector, or index is released;
- protecting allocation and release operations from concurrent access;
- rejecting double-free and invalid handles; and
- rolling back allocations when a segment load fails.

The manager exposes generic DAX concepts only. Device-specific discovery and naming are outside the public Milvus interface.

### Lifecycle and failure handling

During QueryNode startup, the DAX backend validates the configured path and maps the region before serving loads.

During segment loading, mmap-eligible objects are allocated from the selected backend. If a DAX allocation or initialization step fails, the segment load fails and allocations created for that load are released.

During segment release or failed-load cleanup, the DAX Region Manager returns the corresponding range to the free-space allocator.

DAX allocations are runtime-only. After a QueryNode restart, data is loaded again through the normal Milvus loading path; no DAX-resident data is treated as persistent Milvus state.

## Compatibility, Deprecation, and Migration Plan

Existing deployments are unaffected because `queryNode.mmap.backend` defaults to `file`.

No collection schema, API, protobuf, metadata format, or persisted data format changes are introduced. A cluster can upgrade without data migration.

Enabling DAX is an explicit QueryNode configuration change and requires a QueryNode restart. Disabling DAX and returning to `file` is also a configuration change followed by restart; data is reloaded through the existing path.

No deprecation is proposed.

## Test Plan

- Unit tests for DAX Region Manager initialization, aligned allocation, release, capacity exhaustion, invalid handles, double-free prevention, and concurrent allocation.
- Configuration validation tests for `file`, `dax`, invalid backend values, and unavailable DAX paths.
- Segment load and release tests verifying that DAX allocations are released.
- Failure-injection tests verifying rollback after a partial load failure.
- Regression tests proving that the existing file-backed mmap path is unchanged.
- Integration tests comparing query and vector-search results between file mmap and DAX backends for the same collection and dataset.
- Hardware validation on a DAX-capable memory device.
- Performance measurements comparing file-backed mmap and DAX under the same workload, reported separately from correctness requirements.

## Rejected Alternatives

### Hardware-vendor-specific implementation

Using hardware-vendor-specific names, paths, or APIs in Milvus would prevent a general upstream solution and make the feature dependent on one platform. The upstream interface remains generic DAX.

### DAX selection based on mmap file-path parsing

Inferring DAX placement from `.mmap` file paths couples backend selection to file naming and storage layout. Backend selection should instead happen at the existing mmap allocation boundary.

### Automatic DRAM-DAX-SSD tiering

Automatic tiering requires policies for placement, eviction, promotion, persistence, and observability. It is intentionally excluded from the first contribution.

### Silent fallback from DAX to file-backed mmap

Silent fallback makes operational behavior and performance unpredictable. Explicit DAX configuration must either use DAX successfully or fail clearly.

## References

- Existing Milvus mmap implementation and configuration.
- DAX-capable hardware validation results.
