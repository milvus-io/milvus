# ChannelMgr Package

The `channelmgr` package resolves the DML channels (virtual and physical) of
collections. It decouples channel resolution from the collection metadata
cache: the resolver is injected at construction, so callers decide where
channel metadata comes from (production reads `metacache`; tests inject a fake).

## Overview

In Milvus, a collection is partitioned into shards represented by virtual
channels (`vChan`), which are mapped 1:1 to physical channels (`pChan`, the
actual message-stream topic/WAL). DML write tasks (insert/delete/upsert) and
read paths (search/query) need the channel list of a collection before they can
dispatch work. This package owns that lookup.

## Responsibilities

1. **Channel resolution**: resolve `(vchans, pchans)` for a collection id via an
   injected `GetChannelsFunc`, validating the vchan/pchan alignment on every
   resolver result.
2. **No channel cache of its own**: the package deliberately keeps no per-
   collection cache. The injected resolver owns caching (e.g. it reads the meta
   cache), so this package never serves stale channel metadata and never needs
   its own invalidation path.
3. **Message packing helpers**: `GenInsertMsgsByPartition` splits an insert
   payload into per-segment messages honoring the WAL-specific single-row limit;
   `GetActiveWALName` returns the active WAL implementation name.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      ChannelMgr                          │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │             channelsMgrImpl                      │   │
│  │  • getChannelsFunc  (injected resolver)         │   │
│  │  • vchan/pchan alignment check on resolve       │   │
│  └───────────────────────┬──────────────────────────┘   │
│                          │ GetChannels / GetVChannels   │
│                          ▼                              │
│               (collID → ChannelInfo{VChans,PChans})     │
└──────────────────────────────────────────────────────────┘
```

### Interface

```go
type ChannelsMgr interface {
    GetChannels(collectionID typeutil.UniqueID) ([]string, error)
    GetVChannels(collectionID typeutil.UniqueID) ([]string, error)
}

type GetChannelsFunc func(collectionID typeutil.UniqueID) (ChannelInfo, error)
```

### Construction

`NewChannelsMgr(getChannelsFunc)` builds a manager. The resolver is injected so
this package has **no dependency on `metacache`**:

```go
mgr := channelmgr.NewChannelsMgr(
    func(collectionID typeutil.UniqueID) (channelmgr.ChannelInfo, error) {
        info, err := metaCache.GetCollectionInfo(ctx, "", "", collectionID)
        if err != nil {
            return channelmgr.ChannelInfo{}, err
        }
        return channelmgr.ChannelInfo{VChans: info.VChannels, PChans: info.PChannels}, nil
    },
)
```

## Usage

- **DML tasks** (insert/delete/upsert) call `GetChannels(collID)` in
  `setChannels()` when enqueued, so the physical channels are known before the
  message is packed.
- **Search/query/flush/import** call `GetVChannels(collID)` to fan work out
  across the virtual channels.
- **Errors**: resolver errors (e.g. `metaCache.GetCollectionInfo` returning
  `ErrCollectionNotFound`) propagate to callers as-is, so Input-vs-System
  classification is decided at the data source, not rewritten here.

## Testing

The package is self-contained and testable without a coordinator: tests inject
a fake `GetChannelsFunc` and assert delegation and alignment-check behavior,
including that every call re-resolves (no internal cache).

**Mocks** (via mockery): `mock_channels_manager.go` mocks the `ChannelsMgr`
interface.

## Related Components

- **Proxy** (`internal/proxy/`): owns the `ChannelsMgr` instance; builds the
  resolver from `metacache` in `Proxy.Init`.
- **MetaCache** (`internal/proxy/metacache/`): the production channel data
  source; `CollectionInfo` carries `VChannels`/`PChannels`. Its own cache and
  invalidation machinery is what keeps channel lookups fast and fresh.
- **TaskScheduler** (`internal/proxy/task_scheduler.go`): consumes the pchans
  resolved by tasks for DML timestamp statistics.
