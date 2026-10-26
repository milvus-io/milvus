// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the routing side's memory: the buckets that accumulate routed
// chunks and the fragment inputs a cut packs out of them. A bucket keeps its
// pieces in arrival order, with the in-memory ones always at the tail, so a
// spill is a plain tail move and a cut is a plain head drain -- the two parallel
// lists the old split had to re-merge are gone.

import (
	"sort"

	"github.com/milvus-io/milvus/internal/storage"
)

// piece is one routed chunk of a bucket, in memory (mem plus its accounted
// bytes) or on local disk (disk). A bucket's pieces are in arrival order and the
// in-memory ones are always its tail: a spill moves the tail to disk as one
// unsplittable piece, a cut drains them all.
type piece struct {
	mem  *storage.InsertData // set for an in-memory piece, nil when spilled
	disk *SpillRange         // set for a spilled piece, nil when in memory
	// accounted is the memory-accounted size of an in-memory piece (decoded
	// bytes plus the per-fragment structural overhead); zero for a spilled piece.
	accounted int64
	// logical is the piece's decoded size (InsertData.GetMemorySize, which walks
	// every string/JSON value), the metric that drives the fragment target, the
	// sort-group packing and the published descriptor.
	logical int64
	rows    int64
}

// bucketKey identifies one bucket by its linear (vchannel, partition) ordinals.
type bucketKey struct {
	vchannelOrdinal  int
	partitionOrdinal int
}

// bucket accumulates everything routed to one (vchannel, partition): its pieces
// in arrival order plus the totals that decide when it is cut. bytes is the
// accounted memory its in-memory tail still holds; logicalBytes and rows are the
// segment totals, kept across a spill (the fragment a bucket eventually flushes
// covers everything it ever received) and reset by a cut.
type bucket struct {
	key          bucketKey
	pieces       []piece
	bytes        int64 // accounted resident bytes (decoded + structural overhead)
	logicalBytes int64 // decoded bytes; the fragment-target trigger and descriptor metric
	rows         int64
}

// append joins one non-empty routed chunk to the bucket's in-memory tail. Its
// two sizes are measured once at routing time: mem drives resident accounting,
// logical drives the flush trigger, the sort-group packing and the descriptor.
func (b *bucket) append(data *storage.InsertData, mem, logical int64) {
	rows := int64(data.GetRowNum())
	b.pieces = append(b.pieces, piece{
		mem:       data,
		accounted: mem,
		logical:   logical,
		rows:      rows,
	})
	b.bytes += mem
	b.logicalBytes += logical
	b.rows += rows
}

// spill moves the bucket's in-memory tail to local disk as one unsplittable
// piece and returns the accounted bytes freed. The bucket keeps its logicalBytes
// and rows: the fragment it eventually flushes covers everything it ever
// received, in memory or on disk.
func (b *bucket) spill(spill *SpillManager, bucketOrdinal int64) (int64, error) {
	// The in-memory tail starts after the leading on-disk pieces.
	head := 0
	for head < len(b.pieces) && b.pieces[head].disk != nil {
		head++
	}
	items := make([]SpillBatch, 0, len(b.pieces)-head)
	for i := head; i < len(b.pieces); i++ {
		items = append(items, SpillBatch{Data: b.pieces[i].mem, Bytes: b.pieces[i].logical})
	}
	spillRange, err := spill.Append(bucketOrdinal, items)
	if err != nil {
		return 0, err
	}
	freed := b.bytes
	b.pieces = append(b.pieces[:head], piece{disk: &spillRange, logical: spillRange.Logical, rows: spillRange.Rows})
	b.bytes = 0
	return freed, nil
}

// fragmentInput is the bounded input of one storage.Sort call: a contiguous run
// of a bucket's pieces, in arrival order, with the totals a cut measured.
// Splitting a bucket into inputs is what turns "the bucket is on disk" into a
// real memory bound for the sort: storage.Sort materializes its whole input.
// Input sizes are the pieces' decoded logical bytes -- the metric Sort actually
// materializes once the pieces become compact arrow records.
type fragmentInput struct {
	pieces       []piece
	rows         int64
	logicalBytes int64
	// accounted is the memory-accounted size of the input's in-memory pieces
	// (decoded bytes plus the per-fragment structural overhead). Spilled pieces
	// contribute nothing: their bytes were released when they went to disk. A
	// detached write releases exactly this much once its fragment is written.
	accounted int64
}

// ranges returns the input's spilled pieces' ranges, in arrival order, for
// release by the detached write that replays them.
func (in fragmentInput) ranges() []SpillRange {
	ranges := make([]SpillRange, 0, len(in.pieces))
	for _, p := range in.pieces {
		if p.disk != nil {
			ranges = append(ranges, *p.disk)
		}
	}
	return ranges
}

// fragmentsWithKey pairs one bucket's identity with the fragment inputs a cut
// produced from it, so a run-end sweep can hand each input to the writer with
// the bucket's (vchannel, partition) ordinals without carrying the bucket.
type fragmentsWithKey struct {
	key    bucketKey
	inputs []fragmentInput
}

// packFragments packs a bucket's pieces, in arrival order, into contiguous
// fragment inputs that each fit inside one Sort's input budget. A single piece
// larger than the budget stays alone; the existing slot estimate makes that a
// single-record pathological case.
func packFragments(pieces []piece, limit int64) []fragmentInput {
	inputs := make([]fragmentInput, 0, 1)
	if limit <= 0 {
		for _, p := range pieces {
			inputs = append(inputs, fragmentInput{
				pieces:       []piece{p},
				rows:         p.rows,
				logicalBytes: p.logical,
				accounted:    p.accounted,
			})
		}
		return inputs
	}
	for _, p := range pieces {
		if len(inputs) == 0 || (inputs[len(inputs)-1].logicalBytes > 0 && inputs[len(inputs)-1].logicalBytes+p.logical > limit) {
			inputs = append(inputs, fragmentInput{})
		}
		g := &inputs[len(inputs)-1]
		g.pieces = append(g.pieces, p)
		g.rows += p.rows
		g.logicalBytes += p.logical
		g.accounted += p.accounted
	}
	return inputs
}

// bucketTable owns every bucket of one run and the accounted bytes they hold. It
// is the routing side's single mutable structure: add routes one chunk,
// cutIfCrossing and cutAll drain buckets into fragment inputs, and spillWhile
// reclaims memory under pressure.
type bucketTable struct {
	policy        *fragmentPolicy
	numPartitions int64
	buckets       map[bucketKey]*bucket
	bytes         int64 // accounted bytes held by the buckets' in-memory tails
}

func newBucketTable(policy *fragmentPolicy, numPartitions int64) *bucketTable {
	return &bucketTable{
		policy:        policy,
		numPartitions: numPartitions,
		buckets:       make(map[bucketKey]*bucket),
	}
}

// len reports how many buckets the table holds, for the run summary.
func (t *bucketTable) len() int { return len(t.buckets) }

// memBytes is the accounted bytes the buckets currently hold in memory.
func (t *bucketTable) memBytes() int64 { return t.bytes }

// add routes one non-empty chunk into its bucket. When the chunk would take the
// bucket to the fragment target it cuts the accumulated segment first and
// returns the fragments (in arrival order); the chunk then starts the next
// segment.
func (t *bucketTable) add(key bucketKey, data *storage.InsertData, mem, logical int64) []fragmentInput {
	b := t.buckets[key]
	if b == nil {
		b = &bucket{key: key}
		t.buckets[key] = b
	}
	cut := t.cutIfCrossing(b, logical)
	b.append(data, mem, logical)
	t.bytes += mem
	return cut
}

// cutIfCrossing cuts a bucket's accumulated segment when this chunk's logical
// bytes would take it to the fragment target, returning the fragments cut (nil
// when the bucket has not crossed). The check runs BEFORE the chunk joins the
// bucket: the flushed input is then strictly below the target, so packFragments
// packs it into a single group and no tiny overshoot fragment is cut for the
// chunk that crossed the threshold; that chunk starts the next segment instead.
func (t *bucketTable) cutIfCrossing(b *bucket, logical int64) []fragmentInput {
	if b.logicalBytes <= 0 || b.logicalBytes+logical < t.policy.target {
		return nil
	}
	return t.cut(b)
}

// cut packs a bucket's pieces into fragment inputs, in arrival order, and resets
// the segment: its pieces have left for the detached writes and its running
// totals start over.
func (t *bucketTable) cut(b *bucket) []fragmentInput {
	inputs := packFragments(b.pieces, t.policy.sortInput)
	t.bytes -= b.bytes
	b.pieces = nil
	b.bytes = 0
	b.logicalBytes = 0
	b.rows = 0
	return inputs
}

// cutAll cuts every non-empty bucket in bucket order, the same tail the serial
// pipeline produced.
func (t *bucketTable) cutAll() []fragmentsWithKey {
	keys := make([]bucketKey, 0, len(t.buckets))
	for key := range t.buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].vchannelOrdinal != keys[j].vchannelOrdinal {
			return keys[i].vchannelOrdinal < keys[j].vchannelOrdinal
		}
		return keys[i].partitionOrdinal < keys[j].partitionOrdinal
	})
	fragments := make([]fragmentsWithKey, 0, len(keys))
	for _, key := range keys {
		b := t.buckets[key]
		if len(b.pieces) == 0 {
			continue
		}
		fragments = append(fragments, fragmentsWithKey{key: key, inputs: t.cut(b)})
	}
	return fragments
}

// spillWhile drains the largest buckets' tails to local disk, one per call, until
// keepGoing reports false or every bucket is empty. It returns the accounted
// bytes freed and how many tails went to disk, the run summary's spill counters.
// It is the shared body of both spill policies: the whole-task resident ceiling
// and the dynamic free-memory checkpoint.
func (t *bucketTable) spillWhile(spill *SpillManager, cond func() bool) (int64, int, error) {
	var bytes int64
	var ranges int
	for cond() {
		freed, err := t.spillLargest(spill)
		if err != nil {
			return bytes, ranges, err
		}
		if freed == 0 {
			// Every bucket's tail is empty: whatever resident bytes remain
			// belong to in-flight fragment inputs, which the writes release on
			// their own.
			break
		}
		bytes += freed
		ranges++
	}
	return bytes, ranges, nil
}

// spillLargest is the dynamic-checkpoint spill: one bucket per call, the largest
// first, converging memory usage back under the floor.
func (t *bucketTable) spillLargest(spill *SpillManager) (int64, error) {
	var target *bucket
	for _, b := range t.buckets {
		if b.bytes == 0 {
			continue
		}
		if target == nil || b.bytes > target.bytes ||
			(b.bytes == target.bytes && (b.key.vchannelOrdinal < target.key.vchannelOrdinal ||
				(b.key.vchannelOrdinal == target.key.vchannelOrdinal && b.key.partitionOrdinal < target.key.partitionOrdinal))) {
			target = b
		}
	}
	if target == nil {
		return 0, nil
	}
	return t.spillBucket(spill, target)
}

// spillBucket moves one bucket's in-memory tail to its fixed shard and returns
// the accounted bytes freed.
func (t *bucketTable) spillBucket(spill *SpillManager, b *bucket) (int64, error) {
	bucketOrdinal := int64(b.key.vchannelOrdinal)*t.numPartitions + int64(b.key.partitionOrdinal)
	freed, err := b.spill(spill, bucketOrdinal)
	if err != nil {
		return 0, err
	}
	t.bytes -= freed
	return freed, nil
}
