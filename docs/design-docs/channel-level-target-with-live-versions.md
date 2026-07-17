# Channel-level targets with live version retention

## Problem

QueryCoord keeps exactly two target snapshots per collection — `current` and `next` — and promotes
`current` only when **every** delegator of **every** channel is ready for `next`. Two consequences
follow, and both are live bugs today:

1. **One shard freezes the whole collection.** A single delegator that cannot become ready (a
   segment that never loads, a node under memory pressure) blocks the promote for all channels, so
   compaction results are never adopted, redundant segments are never reclaimed, and a collection
   load can never complete (#51309 has this shape; #48903's fix, #50686, added the all-or-nothing
   prepare gate that makes it strict).

2. **Coord throws away target versions that delegators are still serving.** A delegator is synced to
   `next`, then `next` is rebuilt (expiry refresh, or the out-of-band force-update on
   `ErrSegmentNotFound`) before the promote lands. The delegator now serves a version coord no longer
   holds a snapshot of — an *orphan*. Coord cannot reconstruct that readable set, so it cannot tell
   whether a redundant segment is still in use; it releases the segment, `loadedRatio` drops below
   1.0, and the shard goes unserviceable (#51260). The pin (#50686), its relaxation (#51307) and the
   release guard (#51261) are all patches around this one fact: **coord discards a version that is
   still being read.**

## Design

Two changes, one idea: *targets are per channel, and a version is not discarded while somebody is
still reading it.*

### 1. Per-channel targets

Each channel carries its own `current` and `next` version. Promote is decided per channel: when the
delegators of channel X are ready for X's next, X promotes — regardless of what channel Y is doing.

Consequence: a slow channel no longer stalls the others. The prepare gate and the next-target pin
(#50686 / #51307) exist only to prevent orphans, and orphans are addressed below, so both can go.

The persisted form already is per channel (`querypb.CollectionTarget.channel_targets`); only the
version is collection-scalar today. Add `ChannelTarget.version`, and on recovery fall back to the
collection version when it is absent (old data).

### 2. Live version retention (the part that removes orphans)

For each channel, coord keeps the **segment id set of every target version that some delegator's
readable view still points at**, in addition to `current` and `next`:

```
channel X:  live versions  { T0 (current) , T2 (a delegator is still on it) , T5 (next) }
```

A version is dropped once no delegator's view references it and it is neither `current` nor `next`
(min-reader GC, evaluated on the distribution heartbeat, which already carries every delegator's
view version).

This makes the release decision **total and sound without asking anybody**:

```
a sealed segment may be released  <=>  it is absent from every live version of its channel
```

There is no state in which coord "cannot know" — the snapshot a delegator reads is, by construction,
still held. Orphans do not exist; they become "a delegator reading an older live version", which is a
normal, well-defined state.

**Every version that leaves a slot must be retained, not only the one that is promoted.** A version
stops being reachable from `current`/`next` in two ways, and both must register it:

* it is **promoted** — `next` becomes `current`, and the outgoing `current` is retained;
* it is **discarded** — `next` is *replaced* by a freshly built target (an out-of-band
  `UpdateCollectionNextTarget`: a load task failing with `ErrSegmentNotFound`, a partition released,
  the survive-time timer firing). The outgoing `next` is the very version delegators may have just
  been synced to. This is exactly how the incident's straddle forms.

Retaining only the promoted version leaves the discarded one unresolvable, and the release decision
is back to "cannot know". Unit tests did not catch this — a real cluster did: with only the promoted
path retained, coord failed to resolve **4408** delegator reads of a discarded version (safety held —
they were kept, so nothing was wrongly released — but nothing was reclaimed either). With both paths
retained, the same scenario resolved every one of them.

## What this removes

| mechanism | why it exists today | after this change |
|---|---|---|
| prepare gate (#50686) | prevent a partially-synced next from becoming an orphan | unnecessary — orphan is not a state |
| next-target pin (#50686), its relaxation (#51307 / #51264) | keep `next` from being replaced while a delegator sits on it | unnecessary — replacing `next` no longer discards it |
| release guard (#51261 / #51327), delegator-reported serving set (#51336) | decide safely when coord cannot reconstruct a readable set | still valuable as a cross-check, but no longer load-bearing |

## Cost

* **State:** per channel, a map `version -> set(segmentID)`. Live versions in steady state: `current`,
  `next`, and occasionally one straggler per replica. ~80 KB per version for a 10k-segment channel —
  an order of magnitude smaller than the full `SegmentInfo` snapshots coord already holds for
  `current` and `next`.
* **Persistence:** none. Historical versions are runtime-only. After a coord restart, a delegator's
  view may reference a version coord no longer has; the release path then falls back to "cannot
  prove, do not release" until the delegator is re-synced — the same sound fallback used for old
  QueryNodes.
* **GC:** one comparison per channel on the distribution heartbeat (drop versions below the minimum
  view version that are neither current nor next).

## Semantics that change

Channels advance independently, so two shards of one collection can serve different target versions
for a few seconds. This does not affect correctness — compaction does not change the rows, only
their packing — and it is already the de-facto behaviour on 2.6, where the observer syncs the ready
delegators and leaves the others behind. What changes is that coord now *admits* it instead of
pretending the collection advances atomically.

## Verification

On a real 4-shard cluster (standalone, segment-load concurrency throttled to one task at a time, so
targets move while segments are still loading — the shape of the incident), 600s of client traffic
with churn, compaction and non-PK-expression deletes:

| | ordinary run | discarded-next forced |
|---|---|---|
| next targets discarded right after delegators were synced to them | 0 | 7 |
| channels promoted on their own | 23 | 21 |
| delegator reads of a version that is neither current nor next | 0 | **4805** |
| ↳ resolved by coord | — | **all of them** (0 unresolvable) |
| sealed segments reclaimed | 1530 | 1688 |
| segments released while still served / unserviceable shards / failed queries | 0 / 0 / 0 | **0 / 0 / 0** |

The second column is the point: the state that is an unresolvable *orphan* today occurred 4805 times
and was answered exactly, every time, while reclamation kept running. Coord never has to choose
between releasing a segment it cannot prove is unused and keeping everything.

Every `not serviceable` event in both runs is in the first seconds after boot, before the delegators
have been synced for the first time.
