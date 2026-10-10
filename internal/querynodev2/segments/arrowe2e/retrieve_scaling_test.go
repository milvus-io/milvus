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

//go:build arrowbench

// Build-tagged out of the default suite. These are measurement harnesses: they
// assert nothing and only print a report, and at the sizes they need (8
// segments, up to 40000 rows, 120-400 paired samples per case) they cost many
// minutes. `make test-go` does not pass -short, so testing.Short() alone was
// not enough to keep them out of CI.
//
// Run with: go test -tags dynamic,test,arrowbench -run TestArrowTransportLatency ./...

// Profiling harness for the 4-segment retrieve inversion.
//
// run 6 of the latency harness showed segments.Retrieve getting FASTER with
// Arrow at one segment (965 -> 704 us) but SLOWER at four (774 -> 911), with the
// hit count held at 1000 in both. That points at a per-segment fixed cost in the
// Arrow path -- plausibly the cdata export/import pair, which runs once per
// segment per column -- rather than anything that scales with payload.
//
// This sweep isolates it: total rows and hit count are held constant and only
// the segmentation varies, so a cost that grows with N shows up as a delta that
// grows with N. Only segments.Retrieve is timed; the reduce and the wire are out
// of scope here.
package arrowe2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// timeRetrieveOnly runs just the per-segment retrieve and reports its duration
// plus the number of Arrow records handed back (0 on the protobuf path).
func (f *fixture) timeRetrieveOnly(t *testing.T, node *planpb.PlanNode) (time.Duration, int, int) {
	t.Helper()
	ctx := context.Background()
	plan := f.newPlan(t, node)
	defer plan.Delete()
	req := f.request(node)

	t0 := time.Now()
	results, pinned, err := segments.Retrieve(ctx, f.manager, plan, req)
	d := time.Since(t0)
	require.NoError(t, err)
	defer f.manager.Segment.Unpin(pinned)
	defer segments.ReleaseRecords(results)

	records, rows := 0, 0
	for _, r := range results {
		if r.Record != nil {
			records++
			rows += int(r.Record.NumRows())
		} else {
			rows += len(r.Result.GetOffset())
		}
	}
	return d, records, rows
}

func TestRetrieveScalingBySegmentCount(t *testing.T) {
	if testing.Short() {
		t.Skip("timing harness")
	}
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	defer setZeroCopy(t, false)

	const (
		totalRows = 20000
		hits      = 1000
		samples   = 200
	)

	var report string
	for _, numSegments := range []int{1, 2, 4, 8, 16} {
		f := newFixture(t, numSegments, totalRows/numSegments)
		node := requeryPlanNode(spreadPKs(hits, totalRows))

		for i := 0; i < 15; i++ {
			setZeroCopy(t, false)
			f.timeRetrieveOnly(t, node)
			setZeroCopy(t, true)
			f.timeRetrieveOnly(t, node)
		}

		offs := make([]float64, 0, samples)
		ons := make([]float64, 0, samples)
		var gotRecords, offRows, onRows int
		for i := 0; i < samples; i++ {
			if i%2 == 0 {
				setZeroCopy(t, false)
				d, _, r := f.timeRetrieveOnly(t, node)
				offs = append(offs, us(d))
				offRows = r
				setZeroCopy(t, true)
				d2, rec2, r2 := f.timeRetrieveOnly(t, node)
				ons = append(ons, us(d2))
				gotRecords, onRows = rec2, r2
			} else {
				setZeroCopy(t, true)
				d, rec, r := f.timeRetrieveOnly(t, node)
				ons = append(ons, us(d))
				gotRecords, onRows = rec, r
				setZeroCopy(t, false)
				d2, _, r2 := f.timeRetrieveOnly(t, node)
				offs = append(offs, us(d2))
				offRows = r2
			}
		}
		require.Equal(t, numSegments, gotRecords, "every segment must hand back a record")
		require.Equal(t, offRows, onRows, "both paths must retrieve the same rows")

		mOff, mOn := median(offs), median(ons)
		delta := mOn - mOff
		report += fmt.Sprintf(
			"%2d seg (%5d rows/seg, %4d rows retrieved)  off %8.1f  on %8.1f  "+
				"delta %+8.1f  per-seg %+7.1f  ratio %.3fx\n",
			numSegments, totalRows/numSegments, offRows, mOff, mOn, delta,
			delta/float64(numSegments), mOff/mOn)

		f.release()
	}
	fmt.Printf("\n===== retrieve only, %d total rows / %d hits held constant (us, medians) =====\n%s",
		totalRows, hits, report)
}

// TestRetrieveFixedCostPerCall decomposes the Arrow retrieve saving into a
// fixed per-call part and a marginal per-row part, on ONE segment so the
// segment count cannot confound it.
//
// This is the measurement that explains TestRetrieveScalingBySegmentCount: if
// the saving is a + b*rows with a negative (a fixed penalty per CGO call), then
// N segments pay that penalty N times and the advantage collapses as the same
// hits are spread over more segments -- which is exactly what that sweep shows.
func TestRetrieveFixedCostPerCall(t *testing.T) {
	if testing.Short() {
		t.Skip("timing harness")
	}
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	defer setZeroCopy(t, false)

	const rowsPerSeg = 20000
	f := newFixture(t, 1, rowsPerSeg)
	defer f.release()

	var report string
	var xs, ys []float64
	for _, hits := range []int{1, 10, 50, 100, 250, 500, 1000, 2000, 4000} {
		node := requeryPlanNode(spreadPKs(hits, rowsPerSeg))
		samples := 300
		if hits >= 1000 {
			samples = 150
		}

		for i := 0; i < 15; i++ {
			setZeroCopy(t, false)
			f.timeRetrieveOnly(t, node)
			setZeroCopy(t, true)
			f.timeRetrieveOnly(t, node)
		}

		offs := make([]float64, 0, samples)
		ons := make([]float64, 0, samples)
		for i := 0; i < samples; i++ {
			if i%2 == 0 {
				setZeroCopy(t, false)
				d, _, _ := f.timeRetrieveOnly(t, node)
				offs = append(offs, us(d))
				setZeroCopy(t, true)
				d2, _, _ := f.timeRetrieveOnly(t, node)
				ons = append(ons, us(d2))
			} else {
				setZeroCopy(t, true)
				d, _, _ := f.timeRetrieveOnly(t, node)
				ons = append(ons, us(d))
				setZeroCopy(t, false)
				d2, _, _ := f.timeRetrieveOnly(t, node)
				offs = append(offs, us(d2))
			}
		}
		mOff, mOn := median(offs), median(ons)
		saving := mOff - mOn
		xs = append(xs, float64(hits))
		ys = append(ys, saving)
		report += fmt.Sprintf("%5d hits  off %8.1f  on %8.1f  arrow saves %+8.1f us  ratio %.3fx\n",
			hits, mOff, mOn, saving, mOff/mOn)
	}

	// Least squares on saving = a + b*hits.
	var sx, sy, sxx, sxy float64
	n := float64(len(xs))
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * ys[i]
	}
	b := (n*sxy - sx*sy) / (n*sxx - sx*sx)
	a := (sy - b*sx) / n

	fmt.Printf("\n===== arrow retrieve saving vs hits, 1 segment (us, medians) =====\n%s"+
		"\nfit: saving = %+.1f us %+.4f us/row\n"+
		"  -> fixed per-call term : %+.1f us (negative means a per-segment penalty)\n"+
		"  -> marginal per-row    : %+.4f us/row\n"+
		"  -> break-even payload  : %.0f rows per segment\n",
		report, a, b, a, b, -a/b)
}
