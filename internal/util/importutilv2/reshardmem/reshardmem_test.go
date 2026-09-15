// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package reshardmem

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	importparquet "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
)

// TestModel pins the shared formulas both DataCoord charging and DataNode
// enforcement derive from, at the default deployment sizes (R=16MiB,
// F=128MiB, P=64MiB parquet stream, W=32MiB packed writer).
func TestModel(t *testing.T) {
	const mib = int64(1024 * 1024)
	m := Model{ReadBuffer: 16 * mib, FragmentTarget: 128 * mib}

	require.Equal(t, importparquet.TotalReadBufferSize+32*mib, m.FixedOverhead())
	require.Equal(t, 6*16*mib, m.Pipeline())
	require.Equal(t, int64(15*FragmentFieldOverhead), FragmentOverhead(15))

	// StructOverhead: one R-sized source batch shreds across every bucket, so
	// the live fragment count scales with buckets and each fragment carries
	// the calibrated per-field structural overhead.
	require.Equal(t, int64(8*15*FragmentFieldOverhead), m.StructOverhead(128*mib, 1, 15))
	require.Equal(t, int64(128*2048*15*FragmentFieldOverhead), m.StructOverhead(2048*mib, 2048, 15))
	require.Equal(t, int64(0), m.StructOverhead(0, 2048, 15))
	require.Equal(t, int64(0), Model{FragmentTarget: 128 * mib}.StructOverhead(128*mib, 2048, 15), "degenerate read buffer charges nothing")

	// WorkingSet = fixed + pipeline + GCFactor x (resident + struct) + one
	// sort copy. Resident demand flattens beyond the cap; the structural term
	// keeps growing with buckets because the live set keeps shredding.
	single := 96*mib + 96*mib + int64(GCFactor*float64(128*mib+8*15*FragmentFieldOverhead)) + 128*mib
	require.Equal(t, single, m.WorkingSet(1, 16, 15))
	require.Equal(t, single, m.WorkingSet(0, 16, 15), "degenerate buckets clamp to one resident bucket")
	capped := 96*mib + 96*mib + int64(GCFactor*float64(16*128*mib+128*16*15*FragmentFieldOverhead)) + 128*mib
	require.Equal(t, capped, m.WorkingSet(16, 16, 15))
	beyond := 96*mib + 96*mib + int64(GCFactor*float64(16*128*mib+128*128*15*FragmentFieldOverhead)) + 128*mib
	require.Equal(t, beyond, m.WorkingSet(128, 16, 15))
	require.Greater(t, beyond, capped, "structural overhead grows with buckets beyond the cap")

	// BucketTailCap: unbounded while the whole in-flight set fits the cap
	// (low-bucket jobs never spill statically); beyond the cap the same total
	// resident budget spreads across every bucket.
	require.Equal(t, int64(math.MaxInt64), m.BucketTailCap(1, 16))
	require.Equal(t, int64(math.MaxInt64), m.BucketTailCap(16, 16))
	require.Equal(t, 16*128*mib/32, m.BucketTailCap(32, 16))
	require.Equal(t, mib, m.BucketTailCap(2048, 16))
	require.Equal(t, int64(1), Model{ReadBuffer: 16 * mib}.BucketTailCap(32, 16), "degenerate fragment target clamps to 1")

	// SortInput: the fragment target while the budget covers two copies plus
	// pipeline and fixed overhead; halved beyond that; clamped to 1 for
	// degenerate budgets so every spill item sorts alone instead of zero.
	require.Equal(t, 128*mib, m.SortInput(480*mib)) // (480-96-96)/2=144 >= 128
	require.Equal(t, 104*mib, m.SortInput(400*mib)) // (400-96-96)/2=104 < 128
	require.Equal(t, int64(1), m.SortInput(0))

	// FlushSpike = SortInput + pipeline + fixed; CheckpointFloor adds the
	// system-memory reserve.
	require.Equal(t, 128*mib+96*mib+96*mib, m.FlushSpike(480*mib))
	require.Equal(t, m.FlushSpike(480*mib)+int64(ReserveRatio*float64(1000*mib)),
		m.CheckpointFloor(480*mib, 1000*mib))
}
