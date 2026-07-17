# Proxy MetaCache: id-primary store with lazily-validated hints

- Status: Implemented (this PR)
- Date: 2026-07-16
- Scope: `internal/proxy/meta_cache.go`, `internal/proxy/version_cache.go`, `internal/rootcoord/ddl_callbacks_alter_alias.go`, `pkg/proto/messages.proto`
- Related: 10M-collections initiative; follow-up to the DescribeCollection-by-id / alias-invalidation fixes (#51313 / #51369)

## 1. Problem

An audit of the proxy meta cache found every id- or alias-driven invalidation doing a linear scan under the global write lock:

| Scan | Scope | Trigger |
|---|---|---|
| `removeCollectionByID` finds an id's name-keys | O(all cached collections) | **every** Load/Release/Drop/Rename/Alter broadcast |
| partition sweep (`StaleIf` prefix match) | O(all cached partition entries) | nested in every eviction |
| alias cleanup (`removeAliasesForCollectionLocked`) | O(db's aliases) | every eviction |
| partition-DDL name expansion | O(db's collections + aliases) | partition DDL |
| `RemoveDatabase` | O(db's entries) + O(all partition entries) | database DDL |

At a large cached working set (target: 1M collections) each broadcast stalls every query on the proxy for a full-cache sweep. The scans all existed for one reason: the primary store was keyed by *(db, name)* while invalidations arrive by *id*, *alias*, or *db* — so eviction had to search, and secondary structures (alias entries, partition entries under alias names) had to be found and torn down transactionally.

Additionally, cache fills (describe RPC + write-back) raced invalidation broadcasts on two unordered channels, guarded by a per-collection timestamp floor (`collectionCacheVersion`) with known gaps (alias write-backs unversioned, `RequestTime == 0` compat bypass, `RemoveDatabase` never raised it) and a tombstone-lifecycle problem.

## 2. Design

Two principles, each replacing a family of mechanisms:

**P1 — The primary store is keyed by the cluster-unique collection id and is the single source of truth.** Everything else — name resolution, alias resolution, partition entries — is a *hint* that resolves to an id and is validated against the primary on every read. A failed validation is a cache miss (one extra describe), never a stale read. Therefore invalidation only ever touches the primary store, in O(1), and hints are reclaimed lazily by the background GC instead of being kept transactionally in sync.

**P2 — Fills are ordered against invalidations pessimistically, not by timestamps.** A global `fillMu sync.RWMutex`: fills (RPC + write-back) hold the read side for their whole duration; invalidations take the write side, draining in-flight fills before evicting. A pre-DDL snapshot always lands *before* the eviction that cleans it; a fill issued after the eviction reads post-DDL state (rootcoord commits before broadcasting).

### 2.1 Data structures

```
collections  map[id]*collectionInfo          PRIMARY. live iff present AND entry.dbGen == dbGen[entry.dbName]
nameIdx      map[db]map[realName]id          hint; read validates: id live, entry.name == name, entry db == db
aliasInfo    map[db]map[alias]*aliasEntry    hint; alias -> target NAME ("" = negative, 30s TTL); chains
                                             through nameIdx for reads
dbGen        map[db]uint64                   database generation; bump = O(1) whole-db invalidation
dbInfo       map[db]*databaseInfo
partitionCache          "id-partition" -> partitionInfo    keyed by collection id
collLevelPartitionCache "id" -> partitionInfos
fillMu       RWMutex                          P2 (fills=R, invalidations=W); cache hits never touch it
mu           RWMutex                          map consistency; short critical sections, never across an RPC
```

Deleted: the (db,name)-keyed primary, the single-pointer by-id index and its I0 eager-eviction machinery, the alias reverse-index, the `collectionCacheVersion` floor and its `removeVersion` lifecycle, `VersionCache` group indexing, and every invalidation-time scan.

### 2.2 Operations

- **Read** (hit): by-id — `collections[id]` + dbGen check; by-name — `nameIdx` → id → primary, validated; by-alias — `aliasInfo` → name → `nameIdx` → primary. All O(1), `mu.RLock` only.
- **Fill** (miss): `fillMu.RLock` across describe + write-back. Writes primary + hints; overwrites nothing else — stale hints (old name after a rename, old id after drop+recreate) self-invalidate on read. Notably, an id-only describe whose response omits the db (older rootcoord) is now cacheable: the primary is id-keyed, it simply gets no name hint until a by-name lookup supplies the authoritative database.
- **Invalidate by id** (Load/Release/Drop/Rename/Alter, hot): `delete(collections, id)` — O(1). Partition entries deliberately survive: partition DDL has its own broadcast, and after a drop the recreated collection has a NEW id (ids are never reused), so old entries are unreachable and merely await GC.
- **Invalidate partition DDL**: resolve the id (broadcast, ensured describe, or hints) → delete primary entry (NumPartitions changed) + stale the exact `"id-partition"` / `"id"` keys.
- **Invalidate database** (Drop/AlterDatabase): `dbGen[db]++` + drop the db's hint buckets — O(1). Every primary entry cached under the db becomes invisible to reads immediately.
- **Alias DDL**: rootcoord resolves the alias's pre-alter target under its database lock and forwards BOTH target ids in the expiration (`AlterAliasMessageHeader.old_collection_id`, additive proto field) — the proxy evicts old and new targets by id, immune to the race where a concurrent describe re-points the proxy's single-valued alias resolution before the expiration arrives. DropAlias needs no id (nothing re-points an alias concurrently with its drop); older rootcoords (field 0) fall back to the proxy's alias-hint resolution.
- **GC** (`sweep()`, on the existing `backgroundGCLoop` tick): collect-then-delete of dead primary entries (dbGen moved on), hints that no longer validate, expired negative alias entries, and partition entries whose id left the primary (`PruneIf`). Correctness never depends on it.

### 2.3 Complexity

| Operation | Before | After |
|---|---|---|
| read hit (name / id / alias) | O(1) | O(1) (+1 map hop by name) |
| invalidate by id (hot path) | O(all cached) + O(all partitions) + O(db aliases) | **O(1)** |
| partition DDL invalidation | O(db collections + aliases) | **O(1)** |
| database DDL invalidation | O(db entries) + O(all partitions) | **O(1)** |
| write-lock hold, any invalidation | O(N) | **O(1)** (plus draining in-flight fills, RPC-bounded) |
| GC tick | — | O(cached), background, collect under RLock |

## 3. Concurrency

- Lock order: `fillMu` → `m.mu`. RPCs are never made under `m.mu`; fills hold only `fillMu.RLock` across their RPC.
- Cache hits touch neither `fillMu` nor any invalidation state; fills don't block each other; only rare invalidations take the exclusive side (bounded by the describe RPC timeout).
- Deadlock-freedom vs rootcoord: DDL ack callbacks release the meta lock before the expiration fan-out, so a fill's describe RPC is never blocked by the DDL whose invalidation is draining that fill. Proxy-side, `RemovePartition`'s ensure-describe runs *before* taking the write side.
- Trade-offs, accepted by design: an invalidation (and hence the broadcast ack) waits for the slowest in-flight fill; duplicate/late broadcasts over-invalidate (one extra re-describe) instead of being version-filtered.

## 4. Alternatives rejected

- **Multi-key id index / alias reverse index / partition group index** (earlier drafts): kept secondary structures transactionally in sync with evictions — every new structure added a teardown path and a way to get it wrong. Hints + read-validation make the sync problem vanish: a hint can only cost an extra describe.
- **Per-collection version floor** (previous implementation): timestamp comparison at the write-back is exact but leaves unversioned write-backs (alias resolution), a compat bypass (`RequestTime == 0`), and a tombstone lifecycle. The fill lock subsumes all three; its cost lands on rare invalidations instead of hot reads.
- **Copy-on-write / per-map sync.Map**: O(N) writes, or no cross-map consistency.

## 5. Remaining, out of scope

1. **Unbounded cache**: primary entries live until invalidated; needs an LRU/TTL bound for working sets beyond memory. The GC only reclaims dead entries.
2. **Lock-free reads via concurrent maps**: the hint-plus-validation design makes the read chain tolerant of torn multi-map views (entries are immutable once written, `dbGen` is monotonic so a torn read can only demote live→miss and never dead→hit, and a lost/early hint just costs one describe) — so `collections`/`nameIdx`/`aliasInfo` can each become a `typeutil.ConcurrentMap` (Go 1.24+ `sync.Map`), `dbGen` an atomic counter map, and the GC's re-verify a `CompareAndDelete`, removing `m.mu` from the hot read path entirely. This was impossible in the old transactionally-synced design (the doc's earlier "no cross-map consistency" objection) and is now a mechanical swap. Deliberately NOT done yet: `mu.RLock` on hits is tens of ns and invalidations are rare; adopt only when profiling shows reader contention (prefer this over lock sharding). `fillMu` is orthogonal and stays either way.
3. **WAL-subscription meta replication** (the endgame): proxies tail the control channel and apply meta changes from ONE ordered stream — fills and invalidations stop racing entirely, and `fillMu` and the expiration RPC fan-out (`LegacyProxyCollectionMetaCache` — the name anticipates this) both disappear. Belongs to the 10M-collections initiative.

## 6. Rollout

Proxy-side changes are in-memory and proxy-local. The one wire change is the additive `old_collection_id` field in `AlterAliasMessageHeader`: older rootcoords leave it 0 and the proxy falls back to its alias-hint resolution — the same "upgrade rootcoord before proxies; avoid alias DDL during mixed deployment" caveat already documented for the forwarded new-target id.

## 7. Testing

- Primary/hint coherence checker (`assertMetaCacheByIDConsistent`): primary keys equal entry ids; validating hints resolve to entries of their database. Dangling hints are legal by design.
- Fill/invalidation ordering: a describe parked in flight must be drained by a concurrent `RemoveCollectionsByID` / `RemoveDatabase`, and its pre-DDL write-back must not survive the eviction (gated-mock concurrency tests).
- Rename (old name misses immediately via hint validation), drop+recreate under a reused name (new id; old partition entries unreachable), alias re-point under concurrent describe (both targets evicted via forwarded ids), database generation invalidation, cross-db isolation, `-race` on the full proxy suite.
