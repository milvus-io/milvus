// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// bucketTestPolicy never cuts on its own and never splits a sort input: these
// tests drive the spill and the cut explicitly.
func bucketTestPolicy() *fragmentPolicy {
	return &fragmentPolicy{target: 1 << 40, sortInput: 1 << 40, overhead: 600}
}

// newBucketTestFixture builds a bucket table over one (vchannel, partition)
// bucket plus the spill area its drained tails go to.
func newBucketTestFixture(t *testing.T) (*bucketTable, *SpillManager, bucketKey) {
	t.Helper()
	spill, err := NewSpillManager(t.TempDir(), spillTestSchema(), 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spill.Close() })
	key := bucketKey{vchannelOrdinal: 0, partitionOrdinal: 0}
	return newBucketTable(bucketTestPolicy(), 1), spill, key
}

// routePieces routes count in-memory pieces of rows rows each into key's bucket
// through the table, so the bucket and the table both account them, and returns
// the accounted bytes charged.
func routePieces(t *testing.T, table *bucketTable, key bucketKey, pkStart int64, count, rows int) int64 {
	t.Helper()
	var accounted int64
	for i := 0; i < count; i++ {
		data, logical := spillTestBatch(pkStart+int64(i*rows), rows)
		mem := logical + table.policy.overhead
		require.Nil(t, table.add(key, data, mem, logical), "the fixture policy must not cut on its own")
		accounted += mem
	}
	return accounted
}

// requireZeroedPieceTail pins the bucket's piece invariant: every slot of the
// backing array past len is the zero piece. A drained slot that keeps its
// *storage.InsertData stays reachable for the GC, which scans the whole
// allocation and not the slice length, so the accounted bytes a spill reports
// as freed would never actually leave the heap.
func requireZeroedPieceTail(t *testing.T, b *bucket) {
	t.Helper()
	backing := b.pieces[:cap(b.pieces)]
	for i := len(b.pieces); i < len(backing); i++ {
		require.Nil(t, backing[i].mem, "slot %d past len %d still references a drained InsertData", i, len(b.pieces))
		require.Nil(t, backing[i].disk, "slot %d past len %d still references a spilled range", i, len(b.pieces))
	}
}

// TestBucketSpillDropsDrainedPieceReferences pins that a spill really releases
// the tail it drained: the bucket collapses into one unsplittable on-disk piece,
// the freed bytes it reports are the bytes it held, the segment totals survive
// (the flushed fragment covers everything the bucket ever received) and no slot
// of the backing array past the new len still references a drained InsertData.
// Without that last part the accounting reads zero while the heap keeps every
// routed chunk alive, and the free-memory checkpoint has nothing left to spill.
func TestBucketSpillDropsDrainedPieceReferences(t *testing.T) {
	table, spill, key := newBucketTestFixture(t)
	accounted := routePieces(t, table, key, 0, 3, 4)
	b := table.buckets[key]
	require.Len(t, b.pieces, 3)
	require.Equal(t, accounted, b.bytes)
	require.Equal(t, accounted, table.memBytes())
	logicalBytes, rows := b.logicalBytes, b.rows
	require.Positive(t, logicalBytes)

	freed, err := table.spillBucket(spill, b)
	require.NoError(t, err)

	require.Equal(t, accounted, freed, "a spill frees exactly the accounted bytes the tail held")
	require.Len(t, b.pieces, 1, "the drained tail collapses into one on-disk piece")
	require.NotNil(t, b.pieces[0].disk)
	require.Nil(t, b.pieces[0].mem)
	require.Zero(t, b.bytes)
	require.Zero(t, table.memBytes())
	require.Equal(t, logicalBytes, b.logicalBytes, "a spill keeps the segment totals")
	require.Equal(t, rows, b.rows)
	require.Equal(t, logicalBytes, b.pieces[0].logical)
	require.Equal(t, rows, b.pieces[0].rows)
	require.Greater(t, cap(b.pieces), len(b.pieces),
		"the drained slots must still sit inside the backing array for the assertions below to bite")
	requireZeroedPieceTail(t, b)

	// The drained rows still come back through the range the piece carries.
	_, gotRows, pks := readRange(t, spill, *b.pieces[0].disk)
	require.Equal(t, int(rows), gotRows)
	for i, pk := range pks {
		require.Equal(t, int64(i), pk)
	}
}

// TestBucketRespillKeepsLeadingDiskPieceAndDropsTail pins the second spill of
// one bucket, the path where a stale reference really hides: the pieces routed
// after the first spill land in the very slots it drained, so the tail the
// second spill drops is only partly overwritten by the append that installs the
// new on-disk piece. The leading disk piece must survive untouched and readable
// while every slot past the new len goes back to zero.
func TestBucketRespillKeepsLeadingDiskPieceAndDropsTail(t *testing.T) {
	table, spill, key := newBucketTestFixture(t)
	first := routePieces(t, table, key, 0, 3, 4)
	b := table.buckets[key]
	freed, err := table.spillBucket(spill, b)
	require.NoError(t, err)
	require.Equal(t, first, freed)
	lead := *b.pieces[0].disk

	// Route three more pieces into the already spilled bucket.
	second := routePieces(t, table, key, 100, 3, 4)
	require.Len(t, b.pieces, 4)
	require.Equal(t, second, b.bytes)
	require.Equal(t, lead, *b.pieces[0].disk, "routing must leave the leading on-disk piece alone")
	require.Equal(t, cap(b.pieces), len(b.pieces),
		"the new pieces must reuse the slots the first spill drained, not a fresh array")
	logicalBytes, rows := b.logicalBytes, b.rows

	freed, err = table.spillBucket(spill, b)
	require.NoError(t, err)

	require.Equal(t, second, freed, "the second spill frees only the bytes routed after the first")
	require.Len(t, b.pieces, 2, "the leading disk piece is kept and the new tail collapses into one more")
	require.Equal(t, lead, *b.pieces[0].disk, "the first spill's range must survive the second")
	require.NotNil(t, b.pieces[1].disk)
	require.Nil(t, b.pieces[1].mem)
	require.Zero(t, b.bytes)
	require.Zero(t, table.memBytes())
	require.Equal(t, logicalBytes, b.logicalBytes, "a spill keeps the segment totals")
	require.Equal(t, rows, b.rows)
	require.Greater(t, cap(b.pieces), len(b.pieces),
		"the drained slots must still sit inside the backing array for the assertions below to bite")
	requireZeroedPieceTail(t, b)

	// Both ranges read back: the leading one was not clobbered by the drop of
	// the tail that followed it.
	_, rows1, pks1 := readRange(t, spill, lead)
	require.Equal(t, 12, rows1)
	for i, pk := range pks1 {
		require.Equal(t, int64(i), pk)
	}
	_, rows2, pks2 := readRange(t, spill, *b.pieces[1].disk)
	require.Equal(t, 12, rows2)
	for i, pk := range pks2 {
		require.Equal(t, int64(100+i), pk)
	}
}

// TestBucketCutAfterSpillDrainsBucketAndBalancesTable pins that a cut of an
// already spilled bucket still behaves: the on-disk piece leaves for the
// detached write as one input carrying its range and no accounted bytes, the
// bucket drops its pieces entirely (so the backing array it shared goes out of
// scope), and the table's accounting stays balanced across route -> spill -> cut.
func TestBucketCutAfterSpillDrainsBucketAndBalancesTable(t *testing.T) {
	table, spill, key := newBucketTestFixture(t)
	accounted := routePieces(t, table, key, 0, 3, 4)
	b := table.buckets[key]
	logicalBytes, rows := b.logicalBytes, b.rows

	freed, err := table.spillBucket(spill, b)
	require.NoError(t, err)
	require.Equal(t, accounted, freed)
	spilled := *b.pieces[0].disk

	inputs := table.cut(b)

	require.Nil(t, b.pieces)
	require.Zero(t, b.bytes)
	require.Zero(t, b.logicalBytes)
	require.Zero(t, b.rows)
	require.Zero(t, table.memBytes(),
		"routing charged the table, the spill freed it and the cut must not double-count")
	require.Len(t, inputs, 1)
	require.Len(t, inputs[0].pieces, 1)
	require.Equal(t, spilled, *inputs[0].pieces[0].disk)
	require.Equal(t, logicalBytes, inputs[0].logicalBytes)
	require.Equal(t, rows, inputs[0].rows)
	require.Zero(t, inputs[0].accounted, "a spilled piece contributes no accounted bytes to the write")
	require.Equal(t, []SpillRange{spilled}, inputs[0].ranges())
	requireZeroedPieceTail(t, b)
}
