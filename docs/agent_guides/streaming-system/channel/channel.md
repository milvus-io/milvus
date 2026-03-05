# Channel Model

The WAL is partitioned into three channel types: **PChannel** (physical), **VChannel** (logical), and **CChannel** (control).

## PChannel (Physical Channel)

A PChannel is the **fundamental unit of WAL partitioning**. Each PChannel maps 1:1 to a topic/partition in the WAL backend. A PChannel is assigned to exactly one StreamingNode at a time by the Balancer, identified by a monotonically increasing **Term** that fences stale writes from previous assignments.

**Naming**: `<prefix>_<index>`, e.g. `by-dev-rootcoord-dml_0`. Count is fixed at startup, configured by `rootCoord.dmlChannelNum` (default: 16).

## VChannel (Virtual Channel)

A VChannel is a **logical channel** scoped to one shard of a collection. Multiple VChannels from different collections can share the same PChannel. VChannels are allocated to PChannels by the ChannelManager using a load-balanced strategy. VChannels define the scope for DML message routing, transactions, and segment assignment.

**Naming**: `<pchannel_name>_<collectionID>v<shardIndex>`, e.g. `by-dev-rootcoord-dml_0_12345v0` means PChannel `by-dev-rootcoord-dml_0`, collection `12345`, shard `0`. To extract the PChannel from a VChannel name, use `funcutil.ToPhysicalChannel()`.

## CChannel (Control Channel)

The CChannel is a **special VChannel** that serves as the singleton control channel for the entire cluster. It is permanently bound to a fixed PChannel (assigned at cluster initialization, never changes). It provides a single ordering point for cluster-wide broadcast operations (e.g., RBAC changes that affect all nodes regardless of collection).

**Naming**: `<pchannel_name>_vcchan`, e.g. `by-dev-rootcoord-dml_0_vcchan`.

## Key Packages

- `pkg/streaming/util/types/` — PChannel/VChannel type definitions
- `pkg/util/funcutil/` — Channel name generation, parsing, validation
- `internal/streamingcoord/server/balancer/` — `ChannelManager`, `PChannelMeta`, balance policy
