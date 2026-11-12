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

package proxy

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewMultiSearchTestProxy starts the real batch scheduler for the REST tests in proxy_test.
func NewMultiSearchTestProxy(t *testing.T, queueCapacity, workers int) *Proxy {
	t.Helper()
	s := newMultiSearchScheduler(t.Context(), queueCapacity)
	s.start(workers)
	t.Cleanup(s.stop)
	p := &Proxy{multiSearch: s}
	p.UpdateStateCode(commonpb.StateCode_Healthy)
	return p
}

func multiSearchTestPrepare(collection string) MultiSearchPrepare {
	return func(context.Context) (*milvuspb.SearchRequest, error) {
		return &milvuspb.SearchRequest{DbName: "default", CollectionName: collection}, nil
	}
}

func TestMultiSearchKeepsInputOrder(t *testing.T) {
	p := NewMultiSearchTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(metrics.WrapRestfulContext(t.Context(), 123), 5*time.Second)
	defer cancel()
	secondFinished := make(chan struct{})
	seen := make(chan *metrics.RPCStats, 2)
	prepare := []MultiSearchPrepare{
		multiSearchTestPrepare("slow"),
		multiSearchTestPrepare("denied"),
		func(context.Context) (*milvuspb.SearchRequest, error) {
			return nil, merr.WrapErrParameterMissingMsg("collection")
		},
	}
	results, err := p.RunMultiSearch(ctx, MultiSearchOptions{MaxConcurrency: 2}, prepare, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		seen <- metrics.GetStats(ctx)
		if req.GetCollectionName() == "denied" {
			close(secondFinished)
			return nil, status.Error(codes.PermissionDenied, "search permission denied")
		}
		select {
		case <-secondFinished:
			return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: "slow", Results: &schemapb.SearchResultData{
				Topks: []int64{0},
			}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, "slow", results[0].GetCollectionName())
	require.Equal(t, []int64{0}, results[0].GetResults().GetTopks())
	require.ErrorIs(t, merr.Error(results[1].GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, results[1].GetResults())
	require.ErrorIs(t, merr.Error(results[2].GetStatus()), merr.ErrParameterMissing)
	firstStats, secondStats := <-seen, <-seen
	require.NotSame(t, firstStats, secondStats)
	require.NotSame(t, metrics.GetStats(ctx), firstStats)
	require.NotSame(t, metrics.GetStats(ctx), secondStats)
}

func TestMultiSearchBoundsBatchAdmission(t *testing.T) {
	p := NewMultiSearchTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	execute := func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		if req.GetCollectionName() == "first" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	}
	done := make(chan error, 2)
	options := MultiSearchOptions{MaxConcurrency: 1}
	go func() {
		_, err := p.RunMultiSearch(ctx, options, []MultiSearchPrepare{multiSearchTestPrepare("first")}, execute)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first batch did not start")
	}
	go func() {
		_, err := p.RunMultiSearch(ctx, options, []MultiSearchPrepare{multiSearchTestPrepare("second")}, execute)
		done <- err
	}()
	require.Eventually(t, func() bool { return p.CheckMultiSearchAdmission() != nil }, time.Second, time.Millisecond)

	ran := make(chan struct{}, 1)
	prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
		ran <- struct{}{}
		return &milvuspb.SearchRequest{CollectionName: "third"}, nil
	}
	results, err := p.RunMultiSearch(ctx, options, []MultiSearchPrepare{prepare}, execute)
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Empty(t, results)
	close(release)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("accepted batch did not finish")
		}
	}
	require.Empty(t, ran)

	results, err = p.RunMultiSearch(ctx, options, []MultiSearchPrepare{prepare}, execute)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, merr.Ok(results[0].GetStatus()))
}

func TestMultiSearchKeepsRecordedResultsWhenLateSearchReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		release := make(chan struct{})
		releaseSearch := sync.OnceFunc(func() { close(release) })
		defer releaseSearch()
		cancelled := make(chan error, 1)
		items := []MultiSearchPrepare{
			multiSearchTestPrepare("finished"),
			multiSearchTestPrepare("late"),
			multiSearchTestPrepare("undispatched"),
		}
		deadline := time.Now().Add(time.Second)
		results, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{MaxConcurrency: 1, ExecutionDeadline: deadline}, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			gotDeadline, ok := ctx.Deadline()
			if !ok || !gotDeadline.Equal(deadline) {
				t.Errorf("Search deadline = %v, want %v", gotDeadline, deadline)
			}
			switch req.GetCollectionName() {
			case "finished":
				// Spend part of the common budget before the next Search starts.
				time.Sleep(500 * time.Millisecond)
			case "late":
				<-ctx.Done()
				cancelled <- ctx.Err()
				<-release
			default:
				t.Error("dispatched a Search after expiry")
			}
			return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: req.GetCollectionName()}, nil
		})
		require.NoError(t, err)
		require.Len(t, results, 3)
		require.Equal(t, "finished", results[0].GetCollectionName())
		require.Equal(t, merr.TimeoutCode, results[1].GetStatus().GetCode())
		require.Equal(t, merr.TimeoutCode, results[2].GetStatus().GetCode())
		require.ErrorIs(t, <-cancelled, context.DeadlineExceeded)

		releaseSearch()
		synctest.Wait()
		require.Equal(t, merr.TimeoutCode, results[1].GetStatus().GetCode())
		require.Empty(t, results[1].GetCollectionName())
	})
}

func TestMultiSearchExecutionBudgetReclaimsQueuedSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		release := make(chan struct{})
		releaseSearch := sync.OnceFunc(func() { close(release) })
		defer releaseSearch()
		started := make(chan struct{})
		execute := func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() == "running" {
				close(started)
				<-release
			}
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		}
		finished := make(chan error, 2)
		run := func(name string) {
			_, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{}, []MultiSearchPrepare{multiSearchTestPrepare(name)}, execute)
			finished <- err
		}
		go run("running")
		<-started

		type batchResult struct {
			items []*milvuspb.SearchResults
			err   error
		}
		expired := make(chan batchResult, 1)
		go func() {
			prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
				t.Error("prepared a batch after its deadline in the queue")
				return nil, merr.ErrParameterMissing
			}
			items, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{ExecutionDeadline: time.Now().Add(time.Second)}, []MultiSearchPrepare{prepare}, execute)
			expired <- batchResult{items, err}
		}()
		synctest.Wait()
		require.ErrorIs(t, p.CheckMultiSearchAdmission(), merr.ErrServiceTooManyRequests)
		result := <-expired
		require.NoError(t, result.err)
		require.Len(t, result.items, 1)
		require.Equal(t, merr.TimeoutCode, result.items[0].GetStatus().GetCode())
		require.NoError(t, p.CheckMultiSearchAdmission())

		go run("replacement")
		synctest.Wait()
		require.ErrorIs(t, p.CheckMultiSearchAdmission(), merr.ErrServiceTooManyRequests)
		releaseSearch()
		require.NoError(t, <-finished)
		require.NoError(t, <-finished)
	})
}

func TestMultiSearchParentCancellationEndsCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started := make(chan struct{})
		cancelled := make(chan error, 1)
		finished := make(chan error, 1)
		go func() {
			items, err := p.RunMultiSearch(ctx, MultiSearchOptions{ExecutionDeadline: time.Now().Add(time.Hour)}, []MultiSearchPrepare{multiSearchTestPrepare("running")}, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				close(started)
				<-ctx.Done()
				cancelled <- ctx.Err()
				return &milvuspb.SearchResults{Status: merr.Success()}, nil
			})
			if len(items) != 0 {
				t.Error("returned item results after parent cancellation")
			}
			finished <- err
		}()
		<-started
		cancel()
		require.ErrorIs(t, <-finished, context.Canceled)
		require.ErrorIs(t, <-cancelled, context.Canceled)
	})
}
