// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// TestReshardExecutorResidentIncludesInflightWrites pins the live-resident
// formula during a run: resident = bucket-table accounted bytes + the detached
// writer's in-flight bytes. After a cut the cut fragment's accounted bytes move
// from the bucket table into the in-flight writer, so the live set covers both
// until the write releases its bytes; peakResident records exactly that sum.
func TestReshardExecutorResidentIncludesInflightWrites(t *testing.T) {
	initReshardPipelineParams(t)
	fix := newReshardPipelineFixture()
	// A fragment target of 1 byte makes the second routed batch cross the
	// target and cut the first one as a detached fragment write.
	plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, 1)
	req := fix.request(plan, 3)
	ex, err := newReshardPlanExecutor(nil, req, plan, nil, NewMetrics(1), nil)
	require.NoError(t, err)
	ex.ctx = context.Background()
	// Disable the whole-task resident ceiling for this test: the point is the
	// live-set accounting, not the spill path, so the buckets must stay resident.
	ex.memory.residentBudget = math.MaxInt64

	spill, err := NewSpillManager(ex.write.spillRoot, ex.write.fragmentSchema, ex.write.spillStreams)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spill.Close() })
	ex.spillMgr = spill
	ex.writer = newFragmentWriter(ex.ctx, &ex.write, spill, 1)
	t.Cleanup(ex.writer.close)
	ex.buckets = newBucketTable(&ex.fragment, int64(len(plan.GetPartitionIds())))

	release := make(chan struct{})
	entered := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	writeMock := mockey.Mock(writeFragment).To(
		func(_ *writeSpec, _ *SpillManager, _, _ int, _ fragmentInput, seq int64) (*fragmentOutcome, error) {
			close(entered)
			<-release
			return &fragmentOutcome{descriptor: &importv3pb.FragmentDescriptor{Seq: seq}, timings: &storage.SortTimings{}}, nil
		}).Build()
	defer writeMock.UnPatch()

	// First batch: below the target, so it stays resident in its bucket with no
	// write in flight.
	require.NoError(t, ex.route(fix.batch(1, 2)))
	require.Zero(t, ex.writer.inflightBytes())
	require.Positive(t, ex.buckets.memBytes())
	require.Equal(t, ex.buckets.memBytes(), ex.resident(),
		"with no write in flight the live set is exactly the resident buckets")

	// Second batch crosses the target: the first fragment is cut and handed to
	// the (blocked) writer, so its bytes are now in flight while the second
	// batch stays resident.
	require.NoError(t, ex.route(fix.batch(3, 4)))
	<-entered
	require.Positive(t, ex.writer.inflightBytes(), "the cut fragment must be in flight")
	require.Positive(t, ex.buckets.memBytes(), "the second batch stays resident")
	require.Equal(t, ex.buckets.memBytes()+ex.writer.inflightBytes(), ex.resident())
	require.Equal(t, ex.resident(), ex.counters.peakResident,
		"route records the live set, including the in-flight write, as the peak")

	releaseFn()
	require.NoError(t, ex.writer.wait())
	require.Zero(t, ex.writer.inflightBytes(), "a completed write releases its accounted bytes")
}
