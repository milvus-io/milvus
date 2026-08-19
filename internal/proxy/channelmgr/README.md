# ChannelMgr Package

The `channelmgr` package resolves the DML channels (virtual and physical) of
collections and lazily caches the mapping on the proxy. It decouples channel
resolution from the collection metadata cache: the resolver is injected at
construction, so callers decide where channel metadata comes from (production
reads `metacache`; tests inject a fake).

## Overview

In Milvus, a collection is partitioned into shards represented by virtual
channels (`vChan`), which are mapped 1:1 to physical channels (`pChan`, the
actual message-stream topic/WAL). DML write tasks (insert/delete/upsert) and
read paths (search/query) need the channel list of a collection before they can
dispatch work. This package owns that lookup and its cache.

## Responsibilities

1. **Channel resolution**: resolve `(vchans, pchans)` for a collection id via an
   injected `GetChannelsFunc`.
2. **Lazy caching**: cache the resolved `ChannelInfo` per collection id with
   double-checked locking; repeated lookups hit the cache.
3. **Invalidation**: `RemoveStream` drops the cached entry for a collection
   (called when the collection is dropped) and updates pchan metrics.
4. **Repack**: the `RepackFunc` type and `DefaultInsertRepackFunc` bundle DML
   messages into `MsgPack`s by hash key for streaming insertion.
5. **Message packing helpers**: `GenInsertMsgsByPartition` splits an insert
   payload into per-segment messages honoring the WAL-specific single-row limit.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      ChannelMgr                          │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │             channelsMgrImpl                      │   │
│  │  • infos map[collID]streamInfos  (lazy cache)   │   │
│  │  • getChannelsFunc  (injected resolver)         │   │
│  │  • repackFunc       (message bundling)          │   │
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
    RemoveStream(collectionID typeutil.UniqueID)
}

type GetChannelsFunc func(collectionID typeutil.UniqueID) (ChannelInfo, error)
type RepackFunc func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error)
```

### Construction

`NewChannelsMgr(getChannelsFunc, repackFunc)` builds a manager. The resolver is
injected so this package has **no dependency on `metacache`**:

```go
mgr := channelmgr.NewChannelsMgr(
    func(collectionID typeutil.UniqueID) (channelmgr.ChannelInfo, error) {
        info, err := metaCache.GetCollectionInfo(ctx, "", "", collectionID)
        if err != nil {
            return channelmgr.ChannelInfo{}, err
        }
        return channelmgr.ChannelInfo{VChans: info.VChannels, PChans: info.PChannels}, nil
    },
    channelmgr.DefaultInsertRepackFunc,
)
```

## Usage

- **DML tasks** (insert/delete/upsert) call `GetChannels(collID)` in
  `setChannels()` when enqueued, so the physical channels are known before the
  message is packed.
- **Search/query/flush/import** call `GetVChannels(collID)` to fan work out
  across the virtual channels.
- **DropCollection** calls `RemoveStream(collID)` through the proxy's
  `InvalidateCollectionMetaCache` handler.

## Metrics

- `ProxyMsgStreamObjectsForPChan`: incremented on cache fill, decremented on
  `RemoveStream`, labeled by node id and pchan.

## Testing

The package is self-contained and testable without a coordinator: tests inject
a fake `GetChannelsFunc` and assert cache hit/refill behavior.

**Mocks** (via mockery): `mock_channels_manager.go` mocks the `ChannelsMgr`
interface.

## Related Components

- **Proxy** (`internal/proxy/`): owns the `ChannelsMgr` instance; builds the
  resolver from `metacache` in `Proxy.Init`.
- **MetaCache** (`internal/proxy/metacache/`): the production channel data
  source; `CollectionInfo` carries `VChannels`/`PChannels`.
- **TaskScheduler** (`internal/proxy/task_scheduler.go`): consumes the pchans
  resolved by tasks for DML timestamp statistics.
