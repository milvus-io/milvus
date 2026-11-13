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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func multiSearchTestPrepare(collection string) MultiSearchPrepare {
	return func(context.Context) (*milvuspb.SearchRequest, error) {
		return &milvuspb.SearchRequest{DbName: "default", CollectionName: collection}, nil
	}
}

func TestMultiSearchMissingPreparerIsSystemError(t *testing.T) {
	p := NewMultiSearchTestProxy(t, 1, 1)
	results, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{}, []MultiSearchPrepare{nil}, nil)
	require.Empty(t, results)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
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

func TestMultiSearchLimitsActiveItems(t *testing.T) {
	const itemCount = multiSearchConcurrency + 2
	for _, tt := range []struct {
		name           string
		maxConcurrency uint32
		wantActive     int
	}{
		{name: "default", wantActive: multiSearchConcurrency},
		{name: "one", maxConcurrency: 1, wantActive: 1},
		{name: "above default", maxConcurrency: multiSearchConcurrency + 1, wantActive: multiSearchConcurrency + 1},
		{name: "limited by batch size", maxConcurrency: ^uint32(0), wantActive: itemCount},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewMultiSearchTestProxy(t, 1, 1)
				items := make([]MultiSearchPrepare, itemCount)
				for i := range items {
					items[i] = multiSearchTestPrepare("ready")
				}
				started := make(chan struct{}, itemCount)
				release := make(chan struct{})
				releaseAll := sync.OnceFunc(func() { close(release) })
				defer releaseAll()
				batchDone := make(chan error, 1)
				go func() {
					_, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{MaxConcurrency: tt.maxConcurrency}, items,
						func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
							started <- struct{}{}
							<-release
							return &milvuspb.SearchResults{Status: merr.Success()}, nil
						})
					batchDone <- err
				}()
				synctest.Wait()
				require.Len(t, started, tt.wantActive)
				if tt.wantActive < itemCount {
					release <- struct{}{}
					synctest.Wait()
					require.Len(t, started, tt.wantActive+1)
				}
				releaseAll()
				require.NoError(t, <-batchDone)
				require.Len(t, started, itemCount)
			})
		})
	}
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

func TestMultiSearchRetriesRateLimitedItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		start := time.Now()
		var limitedAttempts []time.Duration
		var readyAttempts []time.Duration
		prepare := []MultiSearchPrepare{multiSearchTestPrepare("limited"), multiSearchTestPrepare("ready")}
		results, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{MaxConcurrency: 1}, prepare, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() == "limited" {
				limitedAttempts = append(limitedAttempts, time.Since(start))
				// Search may rewrite fields as it prepares a task. Retry with the original request.
				req.CollectionName = "rewritten"
				if len(limitedAttempts) <= 2 {
					return nil, merr.WrapErrServiceRateLimit(10)
				}
			} else {
				readyAttempts = append(readyAttempts, time.Since(start))
			}
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		for _, result := range results {
			require.True(t, merr.Ok(result.GetStatus()))
		}
		require.Equal(t, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond}, limitedAttempts)
		require.Equal(t, []time.Duration{600 * time.Millisecond}, readyAttempts)
	})
}

func TestMultiSearchRetriesProxyDQLAdmissionOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		dql, err := scheduler.NewTaskScheduler(t.Context(), nil)
		require.NoError(t, err)
		t.Cleanup(dql.Close)
		dql.DqQueue.SetMaxTaskNum(0)
		p.sched = dql

		attempts := make(map[string]int)
		items := []MultiSearchPrepare{
			multiSearchTestPrepare("admission_recovers"),
			multiSearchTestPrepare("admission_stays_full"),
			multiSearchTestPrepare("querynode_rejection"),
		}
		start := time.Now()
		results, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{MaxConcurrency: 1}, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			name := req.GetCollectionName()
			attempts[name]++
			if name == "querynode_rejection" {
				return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrTooManyRequests(8, "query node queue"))}, nil
			}
			if name == "admission_recovers" && attempts[name] > 1 {
				return &milvuspb.SearchResults{Status: merr.Success()}, nil
			}
			return p.Search(ctx, req)
		})
		require.NoError(t, err)
		require.Len(t, results, 3)
		require.True(t, merr.Ok(results[0].GetStatus()))
		require.ErrorIs(t, merr.Error(results[1].GetStatus()), merr.ErrServiceTooManyRequests)
		require.Equal(t, merr.Status(merr.WrapErrTooManyRequests(0)), results[1].GetStatus())
		require.ErrorIs(t, merr.Error(results[2].GetStatus()), merr.ErrServiceTooManyRequests)
		require.Equal(t, map[string]int{"admission_recovers": 2, "admission_stays_full": 10, "querynode_rejection": 1}, attempts)
		require.Equal(t, 18*time.Second+200*time.Millisecond, time.Since(start))
	})
}

func TestMultiSearchStopsRateLimitRetriesAndKeepsLastStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   time.Duration
		attempts []time.Duration
	}{
		{"exhausted", 0, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond, 1400 * time.Millisecond,
			3 * time.Second, 6 * time.Second, 9 * time.Second, 12 * time.Second, 15 * time.Second, 18 * time.Second}},
		{"next backoff exceeds budget", time.Second, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewMultiSearchTestProxy(t, 1, 1)
				start := time.Now()
				options := MultiSearchOptions{}
				if tc.budget != 0 {
					options.ExecutionDeadline = start.Add(tc.budget)
				}
				var attempts []time.Duration
				var lastStatus *commonpb.Status
				items := []MultiSearchPrepare{multiSearchTestPrepare("limited")}
				results, err := p.RunMultiSearch(t.Context(), options, items, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(options.ExecutionDeadline) {
						t.Errorf("attempt deadline = %v, want %v", deadline, options.ExecutionDeadline)
					}
					attempts = append(attempts, time.Since(start))
					lastStatus = merr.Status(merr.WrapErrServiceRateLimit(float64(len(attempts))))
					lastStatus.ExtraInfo = map[string]string{"quota": "search"}
					return &milvuspb.SearchResults{Status: lastStatus}, nil
				})
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, tc.attempts, attempts)
				require.Equal(t, tc.attempts[len(tc.attempts)-1], time.Since(start))
				require.Same(t, lastStatus, results[0].GetStatus())
			})
		})
	}
}

func TestMultiSearchDoesNotRetryTerminalFailures(t *testing.T) {
	p := NewMultiSearchTestProxy(t, 1, 1)
	failures := map[string]error{
		"force_deny":       merr.WrapErrServiceQuotaExceeded("administrator disabled reads"),
		"permission":       status.Error(codes.PermissionDenied, "no search privilege"),
		"invalid":          merr.ErrParameterInvalid,
		"retryable_status": merr.ErrServiceUnavailable,
		"marked_input":     merr.WrapErrAsInputError(merr.ErrServiceRateLimit),
	}
	attempts := make(map[string]int)
	items := make([]MultiSearchPrepare, 0, len(failures))
	codes := make([]int32, 0, len(failures))
	for name, err := range failures {
		items = append(items, multiSearchTestPrepare(name))
		code := merr.Code(err)
		if name == "permission" {
			code = merr.Code(merr.ErrPrivilegeNotPermitted)
		}
		codes = append(codes, code)
	}
	results, err := p.RunMultiSearch(t.Context(), MultiSearchOptions{MaxConcurrency: 1}, items, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		attempts[req.GetCollectionName()]++
		return nil, failures[req.GetCollectionName()]
	})
	require.NoError(t, err)
	require.Len(t, results, len(failures))
	for i, result := range results {
		require.Equal(t, codes[i], result.GetStatus().GetCode())
	}
	for name := range failures {
		require.Equal(t, 1, attempts[name], name)
	}
}

func TestMultiSearchCancelsBackoffOnParentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		finished := make(chan error, 1)
		attempts := 0
		go func() {
			items := []MultiSearchPrepare{multiSearchTestPrepare("limited"), multiSearchTestPrepare("undispatched")}
			_, err := p.RunMultiSearch(ctx, MultiSearchOptions{MaxConcurrency: 1}, items, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				attempts++
				return nil, merr.ErrServiceRateLimit
			})
			finished <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-finished, context.Canceled)
		synctest.Wait()
		require.Equal(t, 1, attempts)
	})
}

func TestMultiSearchRecordsEachTerminalOnce(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)

	synctest.Test(t, func(t *testing.T) {
		p := NewMultiSearchTestProxy(t, 1, 1)
		unblock := make(chan struct{})
		releaseLateResult := sync.OnceFunc(func() { close(unblock) })
		defer releaseLateResult()
		nodeID := paramtable.GetStringNodeID()
		success := metrics.ProxyMultiSearchSubrequestsTotal.WithLabelValues(nodeID, "success")
		timeout := metrics.ProxyMultiSearchSubrequestsTotal.WithLabelValues(nodeID, "timeout")
		partial := metrics.ProxyMultiSearchBatchesTotal.WithLabelValues(nodeID, "partial_success")
		retry := metrics.ProxyMultiSearchRetriesTotal.WithLabelValues(nodeID, "rate_limit")
		attemptCounter := metrics.ProxyMultiSearchAttemptsTotal.WithLabelValues(nodeID)
		beforeSuccess, beforeTimeout := testutil.ToFloat64(success), testutil.ToFloat64(timeout)
		beforePartial, beforeRetry := testutil.ToFloat64(partial), testutil.ToFloat64(retry)
		beforeAttempts := testutil.ToFloat64(attemptCounter)
		attempts := 0
		items := []MultiSearchPrepare{
			multiSearchTestPrepare("recovers"),
			multiSearchTestPrepare("late"),
			multiSearchTestPrepare("undispatched"),
		}
		options := MultiSearchOptions{MaxConcurrency: 1, ExecutionDeadline: time.Now().Add(time.Second)}
		results, err := p.RunMultiSearch(t.Context(), options, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			switch req.GetCollectionName() {
			case "recovers":
				attempts++
				if attempts <= 2 {
					return nil, merr.ErrServiceRateLimit
				}
			case "late":
				<-ctx.Done()
				<-unblock
			default:
				t.Error("started search after its deadline")
			}
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		})
		require.NoError(t, err)
		require.Len(t, results, 3)
		require.Equal(t, beforeSuccess+1, testutil.ToFloat64(success))
		require.Equal(t, beforeTimeout+2, testutil.ToFloat64(timeout))
		require.Equal(t, beforePartial+1, testutil.ToFloat64(partial))
		require.Equal(t, beforeRetry+2, testutil.ToFloat64(retry))
		require.Equal(t, beforeAttempts+4, testutil.ToFloat64(attemptCounter))

		releaseLateResult()
		synctest.Wait()
		require.Equal(t, beforeSuccess+1, testutil.ToFloat64(success))
		require.Equal(t, beforeTimeout+2, testutil.ToFloat64(timeout))
		spans := recorder.Ended()
		require.Len(t, spans, 1)
		require.Len(t, spans[0].Events(), 3)
		for i, event := range spans[0].Events() {
			values := make(map[string]any)
			for _, attr := range event.Attributes {
				values[string(attr.Key)] = attr.Value.AsInterface()
			}
			require.Equal(t, int64(i), values["index"])
			if i == 0 {
				require.Equal(t, "success", values["result"])
				require.Equal(t, int64(3), values["attempts"])
				require.Equal(t, int64(2), values["rate_limited_attempts"])
				require.Equal(t, int64(2), values["deferrals"])
				require.Equal(t, int64(600), values["backoff_ms"])
			} else {
				require.Equal(t, "timeout", values["result"])
			}
		}
	})
}
