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

**P1 — The primary store is keyed by the cluster-unique collection id and is the single source of truth.** Everything else — name resolution, alias resolution, partition entries — is a *hint* that resolves to an id and is validated against the primary on every read. A failed validation is a cache miss (one extra describe), never a stale read. Hints are written ONLY together with their entry (the name, the entry's declared aliases) and every eviction removes exactly that set through one unified routine — so every hint has a live owner, nothing dangles, and no background reclamation exists.

**P2 — Fills are ordered against invalidations pessimistically, not by timestamps.** A global `fillMu sync.RWMutex`: fills (RPC + write-back) hold the read side for their whole duration; invalidations take the write side, draining in-flight fills before evicting. A pre-DDL snapshot always lands *before* the eviction that cleans it; a fill issued after the eviction reads post-DDL state (rootcoord commits before broadcasting).

### 2.1 Data structures

```
collections  map[id]*collectionInfo          PRIMARY. liveness IS presence: every invalidation deletes explicitly
nameIdx      map[db]map[realName]id          hint; read validates: id live, entry.name == name, entry db == db
aliasInfo    map[db]map[alias]string         hint; alias -> target real name. Written ONLY from the target
                                             entry's declared aliases (update()), so every hint is
                                             discoverable from its entry and eviction always cleans it;
                                             chains through nameIdx for reads
dbInfo       map[db]*databaseInfo
partitionCache map["id"]partitionInfos       ONE cache, keyed by collection id: the full list (with a
                                             name->info map and the partition-key routing order); point
                                             lookups answer from name2Info, list consumers take it whole.
                                             The filler always fetched the full list anyway, so a separate
                                             per-partition cache only duplicated it. A PLAIN map guarded by
                                             mu and ordered against invalidations by fillMu, exactly like
                                             the primary store -- list values are immutable and handed out
                                             by pointer (GC-kept), so no refcount / stale-marker / version
                                             floor / background reclamation. A plain delete on invalidation
                                             is safe even while a request still routes over a list it read.
fillMu       RWMutex                          P2 (fills=R, invalidations=W); cache hits never touch it
mu           RWMutex                          map consistency; short critical sections, never across an RPC
```

Deleted: the (db,name)-keyed primary, the single-pointer by-id index and its I0 eager-eviction machinery, the alias reverse-index, the `collectionCacheVersion` floor and its `removeVersion` lifecycle, the whole `VersionCache` type (refcount + stale-marker + version floor) — the partition cache is now a plain `fillMu`-ordered map, so `backgroundGCLoop` is gone and there is NO background reclamation anywhere — and every invalidation-time scan.

### 2.2 Operations

- **Read** (hit): by-id — `collections[id]`; by-name — `nameIdx` → id → primary, validated (name and db must still match); by-alias — `aliasInfo` → name → `nameIdx` → primary. All O(1), `mu.RLock` only.
- **Fill** (miss): `fillMu.RLock` across describe + write-back. Writes primary + hints; overwrites nothing else — stale hints (old name after a rename, old id after drop+recreate) self-invalidate on read. An id-only describe whose response omits the db (older rootcoord during a rolling upgrade) is served UNCACHED: its database is unknown, so no database DDL could ever find and invalidate a cached copy. Repeat lookups re-describe until the rootcoords are upgraded — the entry class exists only in the upgrade window, and correctness beats a compat caching mechanism.
- **Invalidate by id** (Load/Release/Drop/Rename/Alter, hot): the unified eviction routine (`evictCollectionEntryLocked`) removes the primary entry TOGETHER with everything it owns — its name hint (only while still pointing at this id), its declared alias hints, its partition list — all O(1)-reachable from the entry in hand. EVERY deletion from the primary goes through this routine; a raw delete strands the entry's hints, and a later drop + same-name recreate resurrects a dead alias through them.
- **Invalidate partition DDL**: resolve the id from the broadcast or the LOCAL hints only — no RPC on any invalidation path. Complete by construction: a partition list exists only while its collection's primary entry lives, and a live entry always has its hints; unresolvable name ⇒ nothing cached ⇒ nothing to invalidate. Then delete the primary entry (NumPartitions changed) + stale the exact `"id"` key.
- **Drop database**: delete the db's primary entries explicitly — its `nameIdx` bucket reaches every one of them (completeness invariant: every live entry has a name hint, written together at fill time and only removed by the eviction that removes its entry) — then drop the hint buckets. O(the db's cached collections), on a rare operation — and the fill write lock is held only for the drain plus the O(1) bucket drop, NOT across the walk: the batched walk runs afterwards through the SAME unified eviction routine (a fill racing the walk may rebuild an entry with fresh hints; evicting it removes entry and hints together — one refetch, never a dangling hint; a tests-only hook covers this window deterministically), so a database drop blocks the proxy's fills no longer than any other invalidation's drain, and `mu` is held O(batch) at a time. This is the one deliberately non-O(1) invalidation left: paying a per-db walk on rare DropDatabase buys the removal of the whole generation concept (no per-entry generation stamps, no generation map, no generation-dead-but-physically-present state).
- **Alter database**: invalidate only the database-level `dbInfo` entry, under `fillMu` so a pre-alter `DescribeDatabase` flight cannot resurrect old properties after the expiration returns. Collection metadata, name/alias hints and partition lists do not contain database properties and remain valid, so clearing them would add an O(the db's cached collections) refill storm with no correctness benefit. If replica-number or resource-group properties change, RootCoord separately calls QueryCoord's `UpdateLoadConfig`; QueryCoord invalidates affected proxy shard-leader entries through its own collection-id path. AlterDatabase cache invalidation is therefore O(1).
- **Alias DDL**: rootcoord resolves the alias's pre-alter target under its database lock and forwards BOTH target ids in the expiration (`AlterAliasMessageHeader.old_collection_id`, additive proto field) — the proxy evicts old and new targets by id, immune to the race where a concurrent describe re-points the proxy's single-valued alias resolution before the expiration arrives. The ack callback routes the OLD-target eviction on `old_collection_id`, and the field's **zero value is deliberately the compat-safe scan path** because CreateAlias shares this message type and older rootcoords never set the field:
  - **`> 0`** — old target known: evict it by id, no scan.
  - **`== 0`** — old target UNKNOWN: fall back to the proxy's alias-hint resolution PLUS a holder scan (an invalidate with CollectionID == 0 + the alias name, gated `msgType != DropAlias`). Zero is reached two ways, both of which MUST scan: (a) a new AlterAlias whose broadcast could not resolve the pre-alter target (a meta inconsistency — near-impossible), and (b) a **replayed old-rootcoord AlterAlias** that predates the field. Making zero the scan path is what lets an upgrade-window replay still close its ghost instead of silently skipping it.
  - **`< 0`** (`aliasNoOldTarget` sentinel, set by CreateAlias) — provably no old target: no scan. This is what keeps a normal CreateAlias off the O(N) scan; it cannot be zero, because zero must mean "scan."
  - **DropAlias is EXCLUDED** from the scan entirely (it carries no id in every version; its single-target hint resolution is race-free — nothing re-points an alias concurrently with its drop — so scanning there would reintroduce a permanent O(N) stall on the normal path).
  The scan evicts every cached entry declaring the alias, closing the one window case with no healing event (a re-pointed hint would leave the old target's ghost alias stale indefinitely on a stable, name-addressed collection). It is O(cached collections) but runs only on the genuinely-unknown old-target case — a near-impossible meta inconsistency, or transient upgrade-window replay — and is dead code once rootcoords are upgraded and no old messages remain in the WAL.
- **No background GC.** Every eviction cleans everything the entry owns synchronously, all O(1)-reachable from the entry in hand: its name hint, its declared alias hints (also closing the cascade-dropped-alias resurrection: a lazily-kept alias hint would validate again after a same-name recreate), and its partition list (Load/Release/Alter pay one extra ShowPartitions on the next partition access -- the accepted price). A fill that observes a rename deletes the previous name hint likewise. `ResolveCollectionAlias`'s RPC fallback deliberately writes NO hint (it would be undeclared by its target and thus undiscoverable at eviction -- the stray class), so there is no residue at all: every hint's lifecycle is bound to its entry. There is **no background loop at all**: the partition cache is a plain `fillMu`-ordered map whose entries are deleted synchronously by the same eviction, and its list values are immutable and handed out by pointer, so an invalidation's plain delete is safe with no refcount/stale/prune bookkeeping (the former `VersionCache` and its `backgroundGCLoop` are removed entirely).
- **DescribeCollection projection parity.** Cached and remote providers use one public projection. Not-found responses preserve their precise typed error (`ErrDatabaseNotFound` versus `ErrCollectionNotFound`) and receive the same proxy-boundary InputError classification. Successful responses filter internal/dynamic/namespace fields consistently, restore struct field names on detached messages, and return the same schema properties/version regardless of `proxy.enableCachedServiceProvider`. MetaCache retains canonical TIMESTAMPTZ defaults as UTC microseconds; only affected cached fields are cloned before the shared API response hook rewrites them to strings and redacts `ExternalSpec`.

### 2.3 Complexity

| Operation | Before | After |
|---|---|---|
| read hit (name / id / alias) | O(1) | O(1) (+1 map hop by name) |
| invalidate by id (hot path) | O(all cached) + O(all partitions) + O(db aliases) | **O(1)** |
| partition DDL invalidation | O(db collections + aliases) | **O(1)** |
| DropDatabase invalidation | O(db entries) + O(all partitions) | O(db's cached collections), batched (rare op) |
| AlterDatabase invalidation | O(db entries) + O(all partitions) | **O(1)** (`dbInfo` only) |
| write-lock hold, any invalidation | O(N) | **O(1)** (plus draining in-flight fills, caller-context-governed) |
| background reclamation | — | none (evictions clean synchronously) |

## 3. Concurrency

- Lock order: `fillMu` → `m.mu`. RPCs are never made under `m.mu`; fills hold only `fillMu.RLock` across their RPC.
- Cache hits touch neither `fillMu` nor any invalidation state; fills don't block each other; only rare invalidations take the exclusive side.
- **Caller-owned fill deadline.** Collection, partition and database fills pass the API context through unchanged; MetaCache does not impose a shorter timeout than the caller selected. An invalidation therefore waits until the slowest in-flight fill completes or its caller/transport cancels it. Because Go's `RWMutex` blocks NEW readers once a writer is queued (`a blocked Lock call excludes new readers`), a deadline-less RPC wedged below the transport can pin `fillMu`, the RootCoord synchronous cache-expiration ack behind it, and subsequent fills. This is an explicit contract choice: request/RPC deadline policy belongs at the API or coordinator-client boundary, not inside a metadata cache.
- Deadlock-freedom vs rootcoord: DDL ack callbacks release the meta lock before the expiration fan-out, so a fill's describe RPC is never blocked by the DDL whose invalidation is draining that fill. Proxy-side, `RemovePartition`'s ensure-describe runs *before* taking the write side.
- Trade-offs, accepted by design: an invalidation (and hence the broadcast ack) waits for the slowest in-flight fill; duplicate/late broadcasts over-invalidate (one extra re-describe) instead of being version-filtered.

## 4. Alternatives rejected

- **Multi-key id index / alias reverse index / partition group index** (earlier drafts): kept secondary structures transactionally in sync with evictions — every new structure added a teardown path and a way to get it wrong. Hints + read-validation make the sync problem vanish: a hint can only cost an extra describe.
- **Per-collection version floor** (previous implementation): timestamp comparison at the write-back is exact but leaves unversioned write-backs (alias resolution), a compat bypass (`RequestTime == 0`), and a tombstone lifecycle. The fill lock subsumes all three; its cost lands on rare invalidations instead of hot reads.
- **Copy-on-write / per-map sync.Map**: O(N) writes, or no cross-map consistency.

## 5. Remaining, out of scope

1. **Unbounded cache**: primary entries live until invalidated; needs an LRU/TTL bound for working sets beyond memory.
1. **Shard `fillMu` per collection/database**: today one global fill gate means a fill for database A gates an invalidation for database B until that fill returns under its caller/transport context. Sharding the gate so a fill only gates invalidations for its own collection/database would remove the cross-tenant coupling entirely. Deliberately NOT done yet: invalidations are rare, and a sharded gate must still preserve the "drain all fills of this database before a database DDL" semantics; adopt if cross-tenant wait shows up in practice.
2. **Lock-free reads via concurrent maps**: the hint-plus-validation design makes the read chain tolerant of torn multi-map views (entries are immutable once written and ids are never reused, so a torn read can only demote live→miss and never dead→hit, and a lost/early hint just costs one describe) — so `collections`/`nameIdx`/`aliasInfo` can each become a `typeutil.ConcurrentMap` (Go 1.24+ `sync.Map`) with eviction using `CompareAndDelete` to unlink a hint only while it still points at the evicted entry, removing `m.mu` from the hot read path entirely. This was impossible in the old transactionally-synced design (the doc's earlier "no cross-map consistency" objection) and is now a mechanical swap. Deliberately NOT done yet: `mu.RLock` on hits is tens of ns and invalidations are rare; adopt only when profiling shows reader contention (prefer this over lock sharding). `fillMu` is orthogonal and stays either way.
3. **WAL-subscription meta replication** (the endgame): proxies tail the control channel and apply meta changes from ONE ordered stream — fills and invalidations stop racing entirely, and `fillMu` and the expiration RPC fan-out (`LegacyProxyCollectionMetaCache` — the name anticipates this) both disappear. Belongs to the 10M-collections initiative.

## 6. Accepted gaps — WONT-FIX

Deliberate, documented trade-offs. None affects data, routing or RBAC; none leaves state that survives its healing event. **We do not plan to address them.** Each gap's exact trigger conditions:

| # | Gap | Trigger (ALL conditions required) | Who observes what | Duration / heals when | Why won't fix |
|---|---|---|---|---|---|
| 1 | New alias missing from the target's `Aliases` list | (a) rolling-upgrade window: proxy new, rootcoord OLD (broadcast carries no target id); (b) the target collection was cached BEFORE the CreateAlias/AlterAlias; (c) the alias has not been used for data access yet | `DescribeCollection` of the target under-reports the new alias. Addressing BY the alias works correctly from the first request | Heals on the FIRST use of the alias (the fill overwrites the entry, declaring it) or on any eviction of the target | The alias↔target association exists only at rootcoord; no local predicate — not even a full scan — can select the target (its cached snapshot predates the alias). The only local alternative is evicting the whole database per window alias DDL: a refill storm to cure an under-reported display field |
| 2 | By-id lookups uncached | (a) upgrade window: rootcoord OLD; (b) the lookup is id-only (e.g. the 9091 admin API) — the describe response then omits DbName | Every by-id lookup issues a describe RPC (correct data, no caching) | Entire mixed window; full caching resumes the moment rootcoords are upgraded | Caching an entry whose database is unknown would create state no database event could ever reach — a permanent-staleness hole traded for a window-only cache miss |
| 3 | Junk-name resolutions RPC every time | The resolved name is neither a cached collection nor a declared alias — nonexistent names, typos, requests rejected before the data path ever caches the target | rootcoord sees one describe per such resolution (returns not-found) | Permanent for names that never become real; a real name stops triggering it the moment its collection is cached | A negative cache needs its own TTL/invalidation semantics for a cold/error path; mechanism cost > benefit |
| 4 | Partition list refetched after Load/Release/Alter | Any collection-level invalidation (they share the id-eviction path with Drop), then the next partition-addressed access | One extra ShowPartitions per (invalidation, collection) pair | One request; the refetch re-fills the list | The price of evictions cleaning everything they own synchronously — which is what allows having NO background GC at all |
| 5 | No LRU/TTL bound | A proxy that describes a very large working set (target scale: 1M collections) holds it all resident | Memory growth proportional to the described working set | Until restart or the designated follow-up lands | Designated follow-up, not this PR. A TTL bound would also hard-cap every residual display-staleness window above |

## 7. Rollout

Proxy-side changes are in-memory and proxy-local. The one wire change is the additive `old_collection_id` field in `AlterAliasMessageHeader`: older rootcoords leave it 0 and the proxy falls back to hint resolution plus the gated holder scan (see Alias DDL above), so even a proxy-first upgrade leaves at most lazily-healing Aliases display gaps. The "upgrade rootcoord before proxies; avoid alias DDL during mixed deployment" guidance remains the zero-gap path.

## 8. Testing

- Primary/hint coherence checker (`assertMetaCacheByIDConsistent`) asserts "every hint has a live owner" in all four directions: every primary entry has a name hint targeting a live entry and a hint for each declared alias; every name hint targets a live primary (a rename's old-name hint may point at the live renamed entry — read-time validation rejects it — but a hint to a DEAD id means an eviction bypassed the unified routine); every alias hint chains to a live entry that DECLARES it (the alias-resurrection bugs were exactly violations of this).
- Fill/invalidation ordering: a collection describe parked in flight must be drained by a concurrent `RemoveCollectionsByID` / `RemoveDatabase`, and its pre-DDL write-back must not survive the eviction. A database-info describe parked in flight must likewise be drained by `RemoveDatabaseInfo`, so AlterDatabase cannot be followed by resurrection of pre-alter properties (gated-mock concurrency tests).
- Rename (old name misses immediately via hint validation), drop+recreate under a reused name (new id; old partition entries unreachable), alias re-point under concurrent describe (both targets evicted via forwarded ids), database invalidation via the hint-bucket walk, cross-db isolation, `-race` on the full proxy suite.
