// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package engine implements the per-vchannel pkindex storage engine.
//
// State is in two parts. Writes land in a generation: a container for one
// batch of writes, numbered in time order. Exactly one generation is active
// and takes writes; the rest are frozen and waiting to hand over. Today a
// generation is a pebble DB with its own WAL disabled (durability comes from
// the Milvus WAL) and automatic compaction disabled (merging is offloaded to
// datanode). Generations exist because pebble can delete by key range but not
// by write time: one container per batch makes handing a batch over a matter
// of deleting that container.
//
// The other part is the committed tables: read-only SSTs that are already
// uploaded and already listed by a committed manifest. They are not ingested
// into any pebble DB, and the whole set is swapped at once.
//
// A lookup walks the active generation, then the frozen ones newest-first,
// then the committed tables in the order they were installed. Deletes are
// stored as codec tombstone values, so a recent delete masks an older entry
// below it.
//
// The handover cycle, which the caller drives:
//
//	gen := RotateIncrement()               // freeze the active generation
//	flushed := FlushDraining(gen)          // write it out, staged by sst.ID
//	... upload, commit the manifest ...
//	InstallCommitted(tables, retire: gen)  // swap in, retire the frozen one
//
// Rotating first is what makes this safe without pausing writes: a write
// arriving mid-cycle lands in the new generation and can never reach the one
// being handed over. Coverage is only ever removed after it has been added
// somewhere else, which is why retiring is a parameter of InstallCommitted
// rather than a separate call.
package engine

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
)

// Generation identifies one container of writes. It increases monotonically
// and names that container's directory.
type Generation int64

// RW is the hot path: the deduplication decision and WAL replay.
type RW interface {
	// MultiGet resolves each key to its stored value, or nil when the key is
	// absent or deleted. Results align with keys.
	MultiGet(ctx context.Context, keys [][]byte) ([][]byte, error)

	// Write applies a batch of mutations to the active generation, in order
	// and atomically.
	//
	// Write is normally a memory operation, but it can block on disk: when the
	// background flush of the active generation falls behind, pebble stalls
	// writes once MemTableStopWritesThreshold memtables are queued (2 by
	// default). The caller decides how to live with that.
	Write(ctx context.Context, muts []Mutation) error
}

// Mutation is one write. Delete stores a codec tombstone and ignores Value;
// otherwise Value is stored as-is, and the tombstone encoding itself is
// reserved and rejected.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Lifecycle moves data between the layers and owns shutdown. Close and
// Destroy belong to whoever owns the engine, not to the callers that hold RW.
type Lifecycle interface {
	// RotateIncrement freezes the active generation and starts a new one. The
	// frozen one stays in the read path, so nothing becomes unreachable and
	// writes are never paused. It returns the frozen generation.
	RotateIncrement(ctx context.Context) (Generation, error)

	// FlushDraining writes a frozen generation out as SSTs and stages them
	// outside it, named by sst.ID, newest first. Because the generation is
	// frozen the result is stable, and calling it again for the same
	// generation returns the very same tables under the very same IDs, so an
	// upload that retries after a failure or a restart stays idempotent.
	FlushDraining(ctx context.Context, gen Generation) ([]FlushedTable, error)

	// DrainingGenerations lists the frozen generations, newest first.
	DrainingGenerations() []Generation

	// InstallCommitted swaps in the committed table set and, in the same step,
	// retires the frozen generations the new set covers. Tables must be
	// ordered newest-first: on overlapping key ranges the earlier table wins.
	// Passing no tables clears the committed set.
	//
	// Installing and retiring together is what keeps coverage from going
	// backwards. A generation named in retire that is not draining fails the
	// call before anything changes.
	//
	// A table already open under the same ID keeps its reader, so a set that
	// mostly repeats itself neither reopens files nor loses what their blocks
	// had cached. Only tables new to the set are opened, and when it returns
	// their index and filter are in the block cache: a later lookup does not
	// pay for them while a caller holds a lock. Nothing changes if opening one
	// of them fails.
	//
	// The engine checks each new table's size against its Info; it does not
	// re-read the bytes. Correctness of the content is pebble's per-block
	// checksum, not a whole-file hash.
	InstallCommitted(ctx context.Context, tables []CommittedTable, retire ...Generation) error

	// DropDraining retires a frozen generation without installing anything.
	// It is for a generation an already-committed manifest covers, which a
	// warm start finds on disk and must not upload again. On error the
	// generation is out of the read path regardless and must not be retried:
	// the next Open finds whatever is left and can drop it again.
	DropDraining(ctx context.Context, gen Generation) error

	// Stats snapshots the engine's shape, for triggering policies and metrics.
	Stats() Stats

	// Close releases all handles, keeping on-disk state for a warm restart.
	Close() error

	// Destroy closes the engine and deletes its whole data directory.
	Destroy() error
}

// TODO: the handover driver belongs to the snapshot component, not to this
// package. One handover of a vchannel is:
//
//  1. RotateIncrement freezes the active generation.
//  2. FlushDraining writes each frozen generation out and stages its tables,
//     newest generation first. This includes generations that an earlier
//     failed or interrupted handover left behind.
//  3. Upload the staged tables, then commit a manifest that lists the
//     complete committed set.
//  4. InstallCommitted installs that set and retires the frozen generations
//     that the set covers, in one call.
//
// If step 3 fails, the generations stay frozen and the next handover retries
// them. FlushDraining returns the same tables under the same IDs, so the
// upload is idempotent. The commit must be idempotent too: if step 3 succeeds
// and step 4 fails, the next handover commits the same tables again. Only one
// handover can run at a time for one engine, and no DropDraining can run
// during a handover. If a handover has nothing to publish, it retires the
// empty generations with DropDraining.

// CommittedTable is one table of the committed set: what it is, and where it
// is on this node. The file must stay in place until the set is replaced.
type CommittedTable struct {
	Info sst.Info
	Path string
}

// FlushedTable is one table a frozen generation produced. Path points into the
// engine's own staging area, named by ID and outside the generation, so it is
// free of pebble's per-DB file numbering.
//
// Staging is deleted when the generation retires, so the caller that uploads
// them and wants a local copy of a committed table must make its own link or
// copy and hand that back as the CommittedTable.Path.
type FlushedTable struct {
	Info sst.Info
	Path string
}

// Stats is a point-in-time snapshot of one engine's shape.
type Stats struct {
	// MemTableBytes is the active generation's current memtable size.
	MemTableBytes uint64
	// L0Tables is the number of L0 SSTs in the active generation.
	L0Tables int64
	// DrainingGenerations is the number of frozen generations.
	DrainingGenerations int
	// CommittedTables is the number of installed committed SSTs.
	CommittedTables int
}

// The conditions a caller branches on. They are sentinels rather than error
// categories because "the engine is closed" and "that generation is not being
// drained" call for different handling, which a shared merr code could not
// tell apart. Translation to merr happens at the boundary (see pkerr).
var (
	// ErrClosed is returned by every method once the engine is closed.
	ErrClosed = errors.New("pkindex engine closed")

	// ErrNotDraining reports a generation that is not currently being drained,
	// either because it was never rotated or because it was already retired.
	ErrNotDraining = errors.New("generation is not draining")
)
