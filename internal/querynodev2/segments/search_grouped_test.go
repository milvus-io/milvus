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

package segments

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// groupedTestRequest builds one real SearchRequest. The fan-out reads
// SearchFieldID and GetNumOfQuery off it, which are cgo calls, so a stub will
// not do -- but no real segment is needed, which keeps this test independent
// of the heavier segment fixtures in this package.
func groupedTestRequest(t *testing.T) (*SearchRequest, *Manager) {
	t.Helper()
	paramtable.Init()

	const collectionID = 100
	schema := mock_segcore.GenTestCollectionSchema("grouped-search", schemapb.DataType_Int64, false)
	manager := NewManager()
	manager.Collection.PutOrRef(collectionID, schema,
		mock_segcore.GenTestIndexMeta(collectionID, schema),
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: collectionID,
			PartitionIDs: []int64{10},
		})
	collection := manager.Collection.Get(collectionID)
	require.NotNil(t, collection)
	t.Cleanup(func() { manager.Collection.Unref(collectionID, 1) })

	req, err := mock_segcore.GenSearchPlanAndRequests(
		collection.GetCCollection(), []int64{1}, mock_segcore.IndexFaissIDMap, 1)
	require.NoError(t, err)
	t.Cleanup(req.Delete)
	return req, manager
}

// resultTags records which (segment, branch) slot produced each result.
// Segments are searched concurrently, so writes are serialized.
type resultTags struct {
	mu   sync.Mutex
	tags map[*SearchResult][2]int
}

func newResultTags() *resultTags {
	return &resultTags{tags: make(map[*SearchResult][2]int)}
}

func (rt *resultTags) set(r *SearchResult, segIdx, branch int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tags[r] = [2]int{segIdx, branch}
}

func (rt *resultTags) get(r *SearchResult) [2]int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.tags[r]
}

// groupedMockSegment returns a segment whose SearchGrouped hands back one
// result per branch, tagging each with the (segment, branch) slot that made it
// so the transpose can be checked.
func groupedMockSegment(t *testing.T, segIdx int, tag *resultTags) Segment {
	t.Helper()
	m := NewMockSegment(t)
	m.EXPECT().DatabaseName().Return("default").Maybe()
	m.EXPECT().ResourceGroup().Return("rg").Maybe()
	m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
	m.EXPECT().SearchGrouped(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, reqs []*SearchRequest, _ *semaphore.Weighted) ([]*SearchResult, error) {
			out := make([]*SearchResult, len(reqs))
			for b := range reqs {
				r := new(SearchResult)
				out[b] = r
				tag.set(r, segIdx, b)
			}
			return out, nil
		}).Once()
	return m
}

// The per-branch reduce consumes one branch's results across all segments, so
// the fan-out must return results[branch][segment] -- not the branch-minor
// order the per-segment call produces.
func TestSearchSegmentsGroupedIsBranchMajor(t *testing.T) {
	const segCount, branches = 3, 2
	req, manager := groupedTestRequest(t)

	tag := newResultTags()
	segs := make([]Segment, 0, segCount)
	for i := 0; i < segCount; i++ {
		segs = append(segs, groupedMockSegment(t, i, tag))
	}
	reqs := make([]*SearchRequest, branches)
	for i := range reqs {
		reqs[i] = req
	}

	got, err := searchSegmentsGroupedAttempt(context.Background(), manager, segs, SegmentTypeSealed, reqs)
	require.NoError(t, err)
	require.Len(t, got, branches)
	for b := range got {
		require.Lenf(t, got[b], segCount, "branch %d", b)
		for s, r := range got[b] {
			require.NotNil(t, r)
			assert.Equalf(t, b, tag.get(r)[1], "branch slot %d holds branch %d", b, tag.get(r)[1])
			assert.Equalf(t, s, tag.get(r)[0], "segment slot %d holds segment %d", s, tag.get(r)[0])
		}
	}
}

// len(reqs) == 1 must reproduce the ungrouped fan-out; that is what makes the
// grouped path a superset rather than a second code path.
func TestSearchSegmentsGroupedSingleBranch(t *testing.T) {
	req, manager := groupedTestRequest(t)
	tag := newResultTags()
	segs := []Segment{groupedMockSegment(t, 0, tag), groupedMockSegment(t, 1, tag)}

	got, err := searchSegmentsGroupedAttempt(context.Background(), manager, segs,
		SegmentTypeSealed, []*SearchRequest{req})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Len(t, got[0], 2)
}

// A segment returning the wrong number of results would silently misalign
// every branch downstream, so it must be rejected rather than tolerated.
func TestSearchSegmentsGroupedRejectsBranchCountMismatch(t *testing.T) {
	req, manager := groupedTestRequest(t)

	m := NewMockSegment(t)
	m.EXPECT().DatabaseName().Return("default").Maybe()
	m.EXPECT().ResourceGroup().Return("rg").Maybe()
	m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
	m.EXPECT().SearchGrouped(mock.Anything, mock.Anything, mock.Anything).
		Return([]*SearchResult{new(SearchResult)}, nil).Once()

	_, err := searchSegmentsGroupedAttempt(context.Background(), manager, []Segment{m},
		SegmentTypeSealed, []*SearchRequest{req, req})
	assert.Error(t, err)
}

func TestSearchSegmentsGroupedPropagatesSegmentError(t *testing.T) {
	req, manager := groupedTestRequest(t)

	mkSeg := func(fail bool) Segment {
		m := NewMockSegment(t)
		m.EXPECT().DatabaseName().Return("default").Maybe()
		m.EXPECT().ResourceGroup().Return("rg").Maybe()
		m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
		m.EXPECT().SearchGrouped(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, reqs []*SearchRequest, _ *semaphore.Weighted) ([]*SearchResult, error) {
				if fail {
					return nil, errors.New("segment blew up")
				}
				out := make([]*SearchResult, len(reqs))
				for i := range out {
					out[i] = new(SearchResult)
				}
				return out, nil
			}).Maybe()
		return m
	}

	_, err := searchSegmentsGroupedAttempt(context.Background(), manager,
		[]Segment{mkSeg(false), mkSeg(true)}, SegmentTypeSealed, []*SearchRequest{req, req})
	assert.Error(t, err)
}

// parkedMockSegment returns a segment whose grouped search reports its arrival
// and then parks until the gate opens, so what is in flight at any moment is
// exactly what has arrived and not yet been released.
func parkedMockSegment(t *testing.T, arrived chan<- struct{}, gate <-chan struct{}) Segment {
	t.Helper()
	m := NewMockSegment(t)
	m.EXPECT().DatabaseName().Return("default").Maybe()
	m.EXPECT().ResourceGroup().Return("rg").Maybe()
	m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
	m.EXPECT().SearchGrouped(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, reqs []*SearchRequest, _ *semaphore.Weighted) ([]*SearchResult, error) {
			arrived <- struct{}{}
			<-gate
			out := make([]*SearchResult, len(reqs))
			for i := range out {
				out[i] = new(SearchResult)
			}
			return out, nil
		}).Once()
	return m
}

// The scheduler admits a grouped request as one task however many branches and
// segments it covers, so the bound on its branch searches has to be the task's
// too: one limiter for every segment. A per-segment bound would still be
// multiplied by the segment fan-out.
func TestSearchSegmentsGroupedSharesOneLimiter(t *testing.T) {
	req, manager := groupedTestRequest(t)

	var mu sync.Mutex
	var seen []*semaphore.Weighted
	mkSeg := func() Segment {
		m := NewMockSegment(t)
		m.EXPECT().DatabaseName().Return("default").Maybe()
		m.EXPECT().ResourceGroup().Return("rg").Maybe()
		m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
		m.EXPECT().SearchGrouped(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, reqs []*SearchRequest, limiter *semaphore.Weighted) ([]*SearchResult, error) {
				mu.Lock()
				seen = append(seen, limiter)
				mu.Unlock()
				out := make([]*SearchResult, len(reqs))
				for i := range out {
					out[i] = new(SearchResult)
				}
				return out, nil
			}).Once()
		return m
	}

	t.Run("every segment of a group gets the same limiter", func(t *testing.T) {
		seen = nil
		_, err := searchSegmentsGroupedAttempt(context.Background(), manager,
			[]Segment{mkSeg(), mkSeg(), mkSeg()}, SegmentTypeSealed, []*SearchRequest{req, req})
		require.NoError(t, err)
		require.Len(t, seen, 3)
		require.NotNil(t, seen[0])
		for _, limiter := range seen[1:] {
			assert.Same(t, seen[0], limiter)
		}
	})

	t.Run("an ungrouped request has no task scope to hand out", func(t *testing.T) {
		seen = nil
		_, err := searchSegmentsGroupedAttempt(context.Background(), manager,
			[]Segment{mkSeg(), mkSeg()}, SegmentTypeSealed, []*SearchRequest{req})
		require.NoError(t, err)
		require.Len(t, seen, 2)
		for _, limiter := range seen {
			assert.Nil(t, limiter)
		}
	})
}

// A segment between phase 1 and phase 2 is holding a filter bitset, so a
// grouped request may not park every segment in that window at once. An
// ungrouped one computes no shared bitset and keeps the unbounded fan-out it
// has always had.
func TestSearchSegmentsGroupedBoundsSegmentFanOut(t *testing.T) {
	const cpus = 4
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(cpus))
	require.Equal(t, cpus, hardware.GetCPUNum())

	req, manager := groupedTestRequest(t)

	fanOut := func(t *testing.T, segCount int, reqs []*SearchRequest) chan struct{} {
		gate := make(chan struct{})
		arrived := make(chan struct{}, segCount)
		segs := make([]Segment, 0, segCount)
		for i := 0; i < segCount; i++ {
			segs = append(segs, parkedMockSegment(t, arrived, gate))
		}
		done := make(chan error, 1)
		go func() {
			_, err := searchSegmentsGroupedAttempt(context.Background(), manager, segs, SegmentTypeSealed, reqs)
			done <- err
		}()
		t.Cleanup(func() {
			close(gate)
			require.NoError(t, <-done)
		})
		return arrived
	}

	expectArrivals := func(t *testing.T, arrived chan struct{}, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			select {
			case <-arrived:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d of %d segments started searching", i, n)
			}
		}
	}

	t.Run("grouped fan-out stops at the core count", func(t *testing.T) {
		arrived := fanOut(t, cpus+2, []*SearchRequest{req, req})
		expectArrivals(t, arrived, cpus)
		select {
		case <-arrived:
			t.Fatal("more segments hold a filter bitset than the core count allows")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("ungrouped fan-out is left unbounded", func(t *testing.T) {
		arrived := fanOut(t, cpus+2, []*SearchRequest{req})
		expectArrivals(t, arrived, cpus+2)
	})
}

// The limiter belongs to the task, so several segments running their branches
// through it share one budget rather than getting one each.
func TestRunBranchesBoundedSharesLimiterAcrossSegments(t *testing.T) {
	const segCount, branches, weight = 8, 4, 2

	limiter := semaphore.NewWeighted(weight)
	gate := make(chan struct{})
	arrived := make(chan struct{}, segCount*branches)

	var group errgroup.Group
	for i := 0; i < segCount; i++ {
		group.Go(func() error {
			return runBranchesBounded(context.Background(), branches, limiter,
				func(context.Context, int) error {
					arrived <- struct{}{}
					<-gate
					return nil
				})
		})
	}

	for i := 0; i < weight; i++ {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d branch searches started", i, weight)
		}
	}
	select {
	case <-arrived:
		t.Fatal("more branch searches in flight than the task limiter allows")
	case <-time.After(200 * time.Millisecond):
	}

	close(gate)
	require.NoError(t, group.Wait())
	assert.Len(t, arrived, segCount*branches-weight, "every branch must still run")
}

// A caller with no task to scope the bound to still gets one, and still runs
// every branch.
func TestRunBranchesBoundedNilLimiter(t *testing.T) {
	var ran atomic.Int64
	err := runBranchesBounded(context.Background(), 8, nil, func(context.Context, int) error {
		ran.Add(1)
		return nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, 8, ran.Load())
}

// Waiting for a unit must end when the request is canceled, not when the unit
// finally frees.
func TestRunBranchesBoundedCancellationUnblocksWaiters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runBranchesBounded(ctx, 4, semaphore.NewWeighted(1),
			func(branchCtx context.Context, _ int) error {
				started <- struct{}{}
				<-branchCtx.Done()
				return branchCtx.Err()
			})
	}()

	<-started
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not unblock the branch fan-out")
	}
}

// A cancellation that lands while the branches are being dispatched leaves
// some of them never run. The group itself reports nothing then -- the
// branches that did start all succeeded -- so the dispatch error has to
// survive on its own, or the caller gets a result slice with holes in it and
// no error to say so.
func TestRunBranchesBoundedReportsCancelledDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The test holds the only unit, so no branch can start and the group has
	// nothing to report when the dispatch gives up.
	limiter := semaphore.NewWeighted(1)
	require.NoError(t, limiter.Acquire(context.Background(), 1))
	defer limiter.Release(1)

	var started atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- runBranchesBounded(ctx, 4, limiter, func(context.Context, int) error {
			started.Add(1)
			return nil
		})
	}()

	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("the branch fan-out did not return after cancellation")
	}
	assert.Zero(t, started.Load(), "no branch may run once the dispatch was canceled")
}
