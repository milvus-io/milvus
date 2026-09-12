// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package reshardmem

import (
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
	require.Equal(t, 4*16*mib, m.Pipeline())

	// WorkingSet = fixed + pipeline + (min(buckets,cap)+1) x F.
	require.Equal(t, 96*mib+64*mib+2*128*mib, m.WorkingSet(1, 16))
	require.Equal(t, 96*mib+64*mib+17*128*mib, m.WorkingSet(16, 16))
	require.Equal(t, m.WorkingSet(16, 16), m.WorkingSet(128, 16), "demand flattens beyond the cap")
	require.Equal(t, m.WorkingSet(1, 16), m.WorkingSet(0, 16), "degenerate buckets clamp to one")

	// SortInput: the fragment target while the budget covers two copies plus
	// pipeline and fixed overhead; halved beyond that; clamped to 1 for
	// degenerate budgets so every spill item sorts alone instead of zero.
	require.Equal(t, 128*mib, m.SortInput(480*mib)) // (480-64-96)/2=160 >= 128
	require.Equal(t, 120*mib, m.SortInput(400*mib)) // (400-64-96)/2=120 < 128
	require.Equal(t, int64(1), m.SortInput(0))

	// FlushSpike = SortInput + pipeline + fixed; CheckpointFloor adds the
	// system-memory reserve.
	require.Equal(t, 128*mib+64*mib+96*mib, m.FlushSpike(480*mib))
	require.Equal(t, m.FlushSpike(480*mib)+int64(ReserveRatio*float64(1000*mib)),
		m.CheckpointFloor(480*mib, 1000*mib))
}
