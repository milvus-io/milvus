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

// Latency comparison: common.interface.zeroCopy off vs on.
//
// The measured sequence is the full worker -> delegator hop, because that is the
// scope the change covers:
//
//	retrieve   segments.Retrieve      per-segment CGO; builds FieldData per
//	                                  segment when off, hands back Arrow when on
//	reduce     RunQNQueryPipeline     cross-segment reduce; when on it records
//	                                  which rows won and copies nothing
//	materialize MaterializeArrowSelection the ONE payload pass when on; no-op off
//	marshal    proto.Marshal          wire encode -- IDENTICAL bytes in both arms
//	unmarshal  proto.Unmarshal        wire decode -- likewise
//
// marshal/unmarshal are kept in the total even though they now cancel exactly:
// the wire format is unchanged, which is the point of this scope, and seeing the
// two columns match is the check that it really is unchanged.
//
// Methodology: the two arms alternate single calls, the order flips per sample,
// and the verdict comes from a sign test on per-pair wins. Go's benchmark runner
// cannot be used -- it runs every sample of one sub-benchmark before any of the
// next, so drift lands entirely on one arm.
package arrowe2e_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type phases struct {
	retrieve, reduce, marshal, unmarshal, decode, total time.Duration
	wireBytes                                           int
}

// runTimed walks the same chain as runOnce, timing each stage.
func (f *fixture) runTimed(t *testing.T, node *planpb.PlanNode) phases {
	t.Helper()
	ctx := context.Background()
	plan := f.newPlan(t, node)
	defer plan.Delete()
	req := f.request(node)

	var p phases
	t0 := time.Now()
	results, pinned, err := segments.Retrieve(ctx, f.manager, plan, req)
	require.NoError(t, err)
	p.retrieve = time.Since(t0)
	defer f.manager.Segment.Unpin(pinned)
	defer segments.ReleaseRecords(results)

	reduceResults := make([]*segcorepb.RetrieveResults, 0, len(results))
	querySegments := make([]segments.Segment, 0, len(results))
	records := make([]arrow.Record, 0, len(results))
	hasArrow := false
	for _, r := range results {
		reduceResults = append(reduceResults, r.Result)
		querySegments = append(querySegments, r.Segment)
		records = append(records, r.Record)
		if r.Record != nil {
			hasArrow = true
		}
	}
	if !hasArrow {
		records = nil
	}

	t1 := time.Now()
	reduced, selection, err := segments.RunQNQueryPipeline(
		ctx, req, f.schema, node, reduceResults, records, querySegments, f.manager, plan)
	require.NoError(t, err)
	p.reduce = time.Since(t1)

	t4 := time.Now()
	require.NoError(t, queryutil.MaterializeArrowSelection(reduced, selection, f.schema))
	p.decode = time.Since(t4)

	out := &internalpb.RetrieveResults{
		Ids:              reduced.GetIds(),
		FieldsData:       reduced.GetFieldsData(),
		AllRetrieveCount: reduced.GetAllRetrieveCount(),
		HasMoreResult:    reduced.GetHasMoreResult(),
		ElementLevel:     reduced.GetElementLevel(),
	}

	t2 := time.Now()
	wire, err := proto.Marshal(out)
	require.NoError(t, err)
	p.marshal = time.Since(t2)
	p.wireBytes = len(wire)

	t3 := time.Now()
	var received internalpb.RetrieveResults
	require.NoError(t, proto.Unmarshal(wire, &received))
	p.unmarshal = time.Since(t3)

	p.total = p.retrieve + p.reduce + p.marshal + p.unmarshal + p.decode
	return p
}

type sample struct{ off, on phases }

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func us(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }

func TestArrowTransportLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("timing harness")
	}
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	defer setZeroCopy(t, false)

	cases := []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
		samples     int
	}{
		{"1seg x 100 hits", 1, 20000, 100, 400},
		{"1seg x 1000 hits", 1, 20000, 1000, 300},
		{"1seg x 10000 hits", 1, 40000, 10000, 120},
		{"4seg x 1000 hits", 4, 5000, 1000, 300},
		{"8seg x 2000 hits", 8, 5000, 2000, 150},
	}

	var report string
	for _, c := range cases {
		f := newFixture(t, c.numSegments, c.rowsPerSeg)
		node := requeryPlanNode(spreadPKs(c.hits, c.numSegments*c.rowsPerSeg))
		report += measurePair(t, f, node, c.name, c.samples)
		f.release()
	}
	fmt.Printf("\n===== zeroCopy off -> on, worker+delegator hop (us, medians) =====\n%s", report)
}

// TestArrowTransportLatencyAllTypes measures the shape the narrow harness above
// does not: every field of the schema, so three of the columns (JSON, array and
// the sparse vector) stay in fields_data.
//
// This is the case where the transport could plausibly lose. The fallback
// gathers into an intermediate Arrow array and then converts out of it, which
// is the two-pass cost the lazy selection exists to avoid -- so a payload
// dominated by those types gets none of the benefit while still paying the
// per-column setup.
func TestArrowTransportLatencyAllTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("timing harness")
	}
	initSegcore(t)
	defer setZeroCopy(t, false)

	var report string
	for _, c := range []struct {
		name        string
		numSegments int
		rowsPerSeg  int
		hits        int
		samples     int
	}{
		// Smaller than the narrow harness on purpose: this schema carries four
		// dense vectors plus JSON, array and a sparse vector, so a row is ~2kB
		// against ~536B there. At the narrow sizes the fixture alone exhausts
		// memory.
		{"all-types 1seg x 100", 1, 4000, 100, 200},
		{"all-types 1seg x 500", 1, 4000, 500, 150},
		{"all-types 4seg x 500", 4, 1000, 500, 150},
		{"all-types 8seg x 800", 8, 500, 800, 100},
	} {
		schema := mock_segcore.GenTestCollectionSchema("arrow-lat-all", schemapb.DataType_Int64, true)
		outputs := allFieldsOf(schema)
		f := newFixtureWith(t, schema, outputs, c.numSegments, c.rowsPerSeg)
		node := requeryPlanNodeFor(spreadPKs(c.hits, c.numSegments*c.rowsPerSeg), outputs)
		report += measurePair(t, f, node, c.name, c.samples)
		f.release()
	}
	fmt.Printf("\n===== ALL TYPES: zeroCopy off -> on (us, medians) =====\n%s", report)
}

// measurePair runs the two arms interleaved and returns one report block.
//
// Methodology, learned the hard way earlier in this work: Go's benchmark runner
// runs every sample of one sub-benchmark before any of the next, so drift lands
// on one arm and the same configuration swung 0.68x-1.46x between runs. Here the
// two arms alternate single calls, the order flips per sample, and the verdict
// comes from a sign test on per-pair wins -- which assumes nothing about the
// (skewed) latency distribution.
func measurePair(t *testing.T, f *fixture, node *planpb.PlanNode, name string, nSamples int) string {
	t.Helper()
	{
		// Warm both arms before timing: first-call costs (lazy proto init, Arrow
		// type registry, page faults) would otherwise land on whichever runs first.
		for i := 0; i < 15; i++ {
			setZeroCopy(t, false)
			f.runTimed(t, node)
			setZeroCopy(t, true)
			f.runTimed(t, node)
		}
	}

	samples := make([]sample, 0, nSamples)
	{
		for i := 0; i < nSamples; i++ {
			var s sample
			if i%2 == 0 {
				setZeroCopy(t, false)
				s.off = f.runTimed(t, node)
				setZeroCopy(t, true)
				s.on = f.runTimed(t, node)
			} else {
				setZeroCopy(t, true)
				s.on = f.runTimed(t, node)
				setZeroCopy(t, false)
				s.off = f.runTimed(t, node)
			}
			samples = append(samples, s)
		}

		pick := func(sel func(phases) time.Duration) (float64, float64) {
			offs := lo.Map(samples, func(s sample, _ int) float64 { return us(sel(s.off)) })
			ons := lo.Map(samples, func(s sample, _ int) float64 { return us(sel(s.on)) })
			return median(offs), median(ons)
		}

		wins := 0
		for _, s := range samples {
			if s.on.total < s.off.total {
				wins++
			}
		}
		n := len(samples)
		z := (float64(wins) - 0.5*float64(n)) / (0.5 * math.Sqrt(float64(n)))
		verdict := "INCONCLUSIVE"
		if math.Abs(z) > 3 {
			if wins*2 > n {
				verdict = "ARROW WINS"
			} else {
				verdict = "PROTO WINS"
			}
		}

		rOff, rOn := pick(func(p phases) time.Duration { return p.retrieve })
		dOff, dOn := pick(func(p phases) time.Duration { return p.reduce })
		mOff, mOn := pick(func(p phases) time.Duration { return p.marshal })
		uOff, uOn := pick(func(p phases) time.Duration { return p.unmarshal })
		cOff, cOn := pick(func(p phases) time.Duration { return p.decode })
		tOff, tOn := pick(func(p phases) time.Duration { return p.total })

		return fmt.Sprintf(
			"%-22s wire %7dB -> %7dB\n"+
				"%-22s   retrieve %8.1f -> %8.1f   reduce %8.1f -> %8.1f\n"+
				"%-22s   marshal  %8.1f -> %8.1f   unmarsh %8.1f -> %8.1f   materialize %7.1f -> %7.1f\n"+
				"%-22s   TOTAL    %8.1f -> %8.1f us   ratio %.3fx   on wins %d/%d (%.0f%%)  z=%+.1f  %s\n\n",
			name, samples[0].off.wireBytes, samples[0].on.wireBytes,
			"", rOff, rOn, dOff, dOn,
			"", mOff, mOn, uOff, uOn, cOff, cOn,
			"", tOff, tOn, tOff/tOn, wins, n, 100*float64(wins)/float64(n), z, verdict)
	}
}
