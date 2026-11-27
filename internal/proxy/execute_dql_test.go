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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func executeDQLTestPrepare(collection string) ExecuteDQLPrepare {
	return func(context.Context) (*milvuspb.SearchRequest, error) {
		return &milvuspb.SearchRequest{DbName: "default", CollectionName: collection}, nil
	}
}

func TestExecuteDQLOperatorCancelDuringPreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		p.requests = newTestRequests()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		finished := make(chan error, 1)
		preparationStopped := make(chan error, 1)
		prepare := func(ctx context.Context) (*milvuspb.SearchRequest, error) {
			<-ctx.Done()
			preparationStopped <- context.Cause(ctx)
			return nil, ctx.Err()
		}
		go func() {
			results, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{MaxConcurrency: 1},
				[]ExecuteDQLPrepare{prepare, executeDQLTestPrepare("not started")},
				func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					t.Error("started a Search after cancellation during preparation")
					return &milvuspb.SearchResults{Status: merr.Success()}, nil
				})
			if len(results) != 0 {
				t.Error("operator cancellation returned a result list")
			}
			finished <- err
		}()
		synctest.Wait()
		listed, err := p.ListLocalRunningRequests(t.Context(), &milvuspb.ListRunningRequestsRequest{})
		require.NoError(t, err)
		require.Len(t, listed.GetRequests(), 1)
		batch := listed.Requests[0]
		require.Equal(t, "ExecuteDQL", batch.GetType())
		require.Equal(t, reqregistry.StateRunning, batch.GetState())
		require.True(t, batch.GetCancellable())
		require.Len(t, batch.GetTaskIds(), 1)
		canceled, err := p.CancelLocalRequests(t.Context(), &milvuspb.CancelRequestsRequest{
			RequestIds: []string{batch.GetRequestId()}, Reason: "stop this batch",
		})
		require.NoError(t, err)
		require.Len(t, canceled.GetCanceled(), 1)
		require.Empty(t, canceled.GetNotFound())
		batchErr := <-finished
		require.ErrorIs(t, batchErr, merr.ErrRequestCanceled)
		require.Contains(t, batchErr.Error(), "stop this batch")
		require.False(t, merr.Status(batchErr).GetRetriable())
		require.ErrorIs(t, <-preparationStopped, merr.ErrRequestCanceled)
		require.NoError(t, ctx.Err(), "the client can still receive the cancellation response")
		synctest.Wait()
		listed, err = p.ListLocalRunningRequests(t.Context(), &milvuspb.ListRunningRequestsRequest{})
		require.NoError(t, err)
		require.Empty(t, listed.GetRequests())
	})
}

func TestExecuteDQLCancelAndCompletionAgree(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type outcome struct {
		results []*milvuspb.DQLResult
		err     error
	}
	for range 50 {
		started, release := make(chan struct{}), make(chan struct{})
		done := make(chan outcome, 1)
		go func() {
			results, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("last")},
				func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					close(started)
					select {
					case <-release:
						return &milvuspb.SearchResults{Status: merr.Success()}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				})
			done <- outcome{results, err}
		}()
		awaitAutoLoadSchedulerResult(t, started)
		listed, err := p.ListLocalRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		require.NoError(t, err)
		require.Len(t, listed.GetRequests(), 1)
		batchID := listed.Requests[0].GetRequestId()
		go close(release)
		canceled, err := p.CancelLocalRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []string{batchID}})
		require.NoError(t, err)
		got := awaitAutoLoadSchedulerResult(t, done)
		if len(canceled.GetCanceled()) == 1 {
			require.Empty(t, canceled.GetNotFound())
			require.ErrorIs(t, got.err, merr.ErrRequestCanceled)
			require.Empty(t, got.results)
		} else {
			require.Equal(t, []string{batchID}, canceled.GetNotFound())
			require.NoError(t, got.err)
			require.Len(t, got.results, 1)
			require.NoError(t, merr.Error(got.results[0].GetStatus()))
		}
		listed, err = p.ListLocalRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		require.NoError(t, err)
		require.Empty(t, listed.GetRequests())
	}
}

func TestExecuteDQLMissingPreparerIsSystemError(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
	results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{nil}, nil)
	require.Empty(t, results)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestExecuteDQLRecoversItemPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		items := []ExecuteDQLPrepare{
			func(context.Context) (*milvuspb.SearchRequest, error) { panic("preparation panicked") },
			executeDQLTestPrepare("panic"),
			executeDQLTestPrepare("ready"),
		}
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 2}, items,
			func(_ context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				if request.GetCollectionName() == "panic" {
					panic("Search panicked")
				}
				return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: "ready"}, nil
			})
		require.NoError(t, err)
		require.Len(t, results, 3)
		for _, result := range results[:2] {
			err := merr.Error(result.GetStatus())
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Nil(t, result.GetResult())
		}
		require.NoError(t, merr.Error(results[2].GetStatus()))
		require.NotNil(t, results[2].GetSearch().GetStatus())
		require.NoError(t, merr.Error(results[2].GetSearch().GetStatus()))
		require.Equal(t, "ready", results[2].GetSearch().GetCollectionName())
	})
}

func TestExecuteDQLBatchSizeLimit(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
	for _, count := range []int{256, 257} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			items := make([]ExecuteDQLPrepare, count)
			prepared, executed := 0, 0
			for i := range items {
				items[i] = func(context.Context) (*milvuspb.SearchRequest, error) {
					prepared++
					return &milvuspb.SearchRequest{}, nil
				}
			}
			results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1}, items,
				func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					executed++
					return &milvuspb.SearchResults{Status: merr.Success()}, nil
				})
			if count == 256 {
				require.NoError(t, err)
				require.Len(t, results, count)
				require.Equal(t, count, prepared)
				require.Equal(t, count, executed)
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterTooLarge)
			require.ErrorContains(t, err, "item count 257")
			require.ErrorContains(t, err, "maxBatchSize 256")
			require.Empty(t, results)
			require.Zero(t, prepared)
			require.Zero(t, executed)
		})
	}
}

func setExecuteDQLResultLimit(t *testing.T, limit int) {
	t.Helper()
	params := paramtable.Get()
	key := params.ProxyCfg.ExecuteDQLMaxResultBytes.Key
	require.NoError(t, params.Save(key, strconv.Itoa(limit)))
	t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
}

func executeDQLTestSearchResult(search *milvuspb.SearchResults) *milvuspb.DQLResult {
	return &milvuspb.DQLResult{Status: merr.Success(), Result: &milvuspb.DQLResult_Search{Search: search}}
}

func TestExecuteDQLResultByteLimit(t *testing.T) {
	ready := executeDQLTestSearchResult(&milvuspb.SearchResults{Status: merr.Success(), CollectionName: strings.Repeat("x", 128)})
	failed := &milvuspb.DQLResult{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}
	for _, tc := range []struct {
		name       string
		results    []*milvuspb.DQLResult
		under      int
		singleItem bool
	}{
		{name: "exact", results: []*milvuspb.DQLResult{ready, failed}},
		{name: "one byte over", results: []*milvuspb.DQLResult{ready, failed}, under: 1},
		{name: "one oversized item", results: []*milvuspb.DQLResult{ready}, under: 1, singleItem: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := &milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: tc.results}
			encoded, err := proto.Marshal(expected)
			require.NoError(t, err)
			limit := len(encoded) - tc.under
			setExecuteDQLResultLimit(t, limit)
			p := NewExecuteDQLTestProxy(t, 1, 1)
			items := make([]ExecuteDQLPrepare, len(tc.results))
			for i := range items {
				items[i] = executeDQLTestPrepare(strconv.Itoa(i))
			}
			results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1}, items,
				func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					i, _ := strconv.Atoi(req.GetCollectionName())
					if search := tc.results[i].GetSearch(); search != nil {
						return search, nil
					}
					return &milvuspb.SearchResults{Status: tc.results[i].GetStatus(), Results: &schemapb.SearchResultData{TopK: 1}}, nil
				})
			if tc.under == 0 {
				require.NoError(t, err)
				require.True(t, proto.Equal(expected, &milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: results}))
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterTooLarge)
			require.False(t, merr.Status(err).GetRetriable())
			require.ErrorContains(t, err, "at least "+strconv.Itoa(len(encoded))+" bytes")
			require.ErrorContains(t, err, "maxResultBytes ("+strconv.Itoa(limit)+" bytes)")
			require.Empty(t, results)
			if tc.singleItem {
				require.ErrorContains(t, err, "Reduce nq, topK, or outputFields")
			} else {
				require.NotContains(t, err.Error(), "One Search result")
			}
		})
	}
}

func TestExecuteDQLDefaultResultByteLimit(t *testing.T) {
	const maxResultBytes = 64 * 1024 * 1024
	payload := strings.Repeat("x", maxResultBytes)
	envelope := func(value string) *milvuspb.ExecuteDQLResponse {
		return &milvuspb.ExecuteDQLResponse{
			Status: merr.Success(),
			Results: []*milvuspb.DQLResult{executeDQLTestSearchResult(
				&milvuspb.SearchResults{Status: merr.Success(), CollectionName: value},
			)},
		}
	}
	// All length prefixes have the same width near 64 MiB.
	exactLength := 2*maxResultBytes - proto.Size(envelope(payload))
	p := NewExecuteDQLTestProxy(t, 1, 1)
	for _, extra := range []int{0, 1} {
		t.Run(strconv.Itoa(extra), func(t *testing.T) {
			expected := envelope(payload[:exactLength+extra])
			require.Equal(t, maxResultBytes+extra, proto.Size(expected))
			results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{},
				[]ExecuteDQLPrepare{executeDQLTestPrepare("large_result")},
				func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					return expected.GetResults()[0].GetSearch(), nil
				})
			if extra == 0 {
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, maxResultBytes, proto.Size(&milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: results}))
			} else {
				require.ErrorIs(t, err, merr.ErrParameterTooLarge)
				require.Empty(t, results)
			}
		})
	}
}

func TestExecuteDQLConcurrentByteLimitCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := &milvuspb.SearchResults{Status: merr.Success(), CollectionName: "ready"}
		setExecuteDQLResultLimit(t, proto.Size(&milvuspb.ExecuteDQLResponse{
			Status: merr.Success(), Results: []*milvuspb.DQLResult{executeDQLTestSearchResult(result)},
		}))
		p := NewExecuteDQLTestProxy(t, 1, 1)
		lateStarted, release := make(chan struct{}), make(chan struct{})
		releaseLate := sync.OnceFunc(func() { close(release) })
		defer releaseLate()
		canceled := make(chan error, 1)
		batches := metrics.ProxyExecuteDQLBatchesTotal
		nodeID := paramtable.GetStringNodeID()
		failed := batches.WithLabelValues(nodeID, "batch_failed")
		success := batches.WithLabelValues(nodeID, "all_success")
		beforeFailed, beforeSuccess := testutil.ToFloat64(failed), testutil.ToFloat64(success)

		items := []ExecuteDQLPrepare{
			executeDQLTestPrepare("first"),
			executeDQLTestPrepare("a"),
			executeDQLTestPrepare("b"),
			executeDQLTestPrepare("late"),
			executeDQLTestPrepare("undispatched"),
		}
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 3, ExecutionDeadline: time.Now().Add(time.Second)}, items,
			func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				switch req.GetCollectionName() {
				case "first":
				case "a", "b":
					<-lateStarted
				case "late":
					close(lateStarted)
					<-ctx.Done()
					canceled <- ctx.Err()
					<-release
				default:
					t.Error("dispatched Search after exceeding the byte limit")
				}
				return result, nil
			})
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		require.Empty(t, results)
		require.ErrorIs(t, <-canceled, context.Canceled)
		releaseLate()
		synctest.Wait()
		require.Equal(t, beforeFailed+1, testutil.ToFloat64(failed))
		require.Equal(t, beforeSuccess, testutil.ToFloat64(success))
	})
}

func TestExecuteDQLByteLimitIncludesUnfinishedResults(t *testing.T) {
	ready := &milvuspb.SearchResults{Status: merr.Success()}
	timeout := &milvuspb.DQLResult{Status: merr.Status(context.DeadlineExceeded)}
	responseSize := proto.Size(&milvuspb.ExecuteDQLResponse{
		Status: merr.Success(), Results: []*milvuspb.DQLResult{executeDQLTestSearchResult(ready), timeout, timeout},
	})
	for _, tc := range []struct {
		name  string
		limit int
	}{
		{name: "exact", limit: responseSize},
		{name: "one byte over", limit: responseSize - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				setExecuteDQLResultLimit(t, tc.limit)
				p := NewExecuteDQLTestProxy(t, 1, 1)
				items := []ExecuteDQLPrepare{executeDQLTestPrepare("ready"), executeDQLTestPrepare("late"), executeDQLTestPrepare("undispatched")}
				results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1, ExecutionDeadline: time.Now().Add(time.Second)}, items,
					func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
						switch req.GetCollectionName() {
						case "ready":
							return ready, nil
						case "late":
							<-ctx.Done()
						default:
							t.Error("dispatched Search after the execution deadline")
						}
						return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: strings.Repeat("x", responseSize)}, nil
					})
				if tc.limit == responseSize {
					require.NoError(t, err)
					require.Len(t, results, 3)
					require.Equal(t, merr.TimeoutCode, results[1].GetStatus().GetCode())
					require.Equal(t, merr.TimeoutCode, results[2].GetStatus().GetCode())
					require.Nil(t, results[1].GetResult())
					require.Nil(t, results[2].GetResult())
					require.Equal(t, responseSize, proto.Size(&milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: results}))
				} else {
					require.ErrorIs(t, err, merr.ErrParameterTooLarge)
					require.Empty(t, results)
				}
			})
		})
	}
}

func TestExecuteDQLKeepsInputOrder(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
	ctx, cancel := context.WithTimeout(metrics.WrapRestfulContext(t.Context(), 123), 5*time.Second)
	defer cancel()
	secondFinished := make(chan struct{})
	seen := make(chan *metrics.RPCStats, 2)
	prepare := []ExecuteDQLPrepare{
		executeDQLTestPrepare("slow"),
		executeDQLTestPrepare("denied"),
		func(context.Context) (*milvuspb.SearchRequest, error) {
			return nil, merr.WrapErrParameterMissingMsg("collection")
		},
	}
	results, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{MaxConcurrency: 2}, prepare, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
	require.Equal(t, "slow", results[0].GetSearch().GetCollectionName())
	require.Equal(t, []int64{0}, results[0].GetSearch().GetResults().GetTopks())
	require.ErrorIs(t, merr.Error(results[1].GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, results[1].GetResult())
	require.ErrorIs(t, merr.Error(results[2].GetStatus()), merr.ErrParameterMissing)
	require.Nil(t, results[2].GetResult())
	firstStats, secondStats := <-seen, <-seen
	require.NotSame(t, firstStats, secondStats)
	require.NotSame(t, metrics.GetStats(ctx), firstStats)
	require.NotSame(t, metrics.GetStats(ctx), secondStats)
}

func TestExecuteDQLLimitsActiveItems(t *testing.T) {
	const itemCount = 18
	for _, tt := range []struct {
		name           string
		configured     string
		maxConcurrency uint32
		wantActive     int
	}{
		{name: "default", wantActive: 4},
		{name: "one", maxConcurrency: 1, wantActive: 1},
		{name: "above default", maxConcurrency: 5, wantActive: 5},
		{name: "factory maximum", maxConcurrency: ^uint32(0), wantActive: 16},
		{name: "configured below default", configured: "2", wantActive: 2},
		{name: "configured below explicit request", configured: "3", maxConcurrency: ^uint32(0), wantActive: 3},
		{name: "limited by batch size", configured: "32", maxConcurrency: ^uint32(0), wantActive: itemCount},
		{name: "zero maximum still makes progress", configured: "0", maxConcurrency: 8, wantActive: 1},
		{name: "negative maximum still makes progress", configured: "-1", maxConcurrency: 8, wantActive: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.configured != "" {
				params := paramtable.Get()
				key := params.ProxyCfg.ExecuteDQLMaxConcurrency.Key
				require.NoError(t, params.Save(key, tt.configured))
				t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
			}
			synctest.Test(t, func(t *testing.T) {
				p := NewExecuteDQLTestProxy(t, 1, 1)
				items := make([]ExecuteDQLPrepare, itemCount)
				for i := range items {
					items[i] = executeDQLTestPrepare("ready")
				}
				started := make(chan struct{}, itemCount)
				release := make(chan struct{})
				releaseAll := sync.OnceFunc(func() { close(release) })
				defer releaseAll()
				batchDone := make(chan error, 1)
				go func() {
					_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: tt.maxConcurrency}, items,
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

func TestExecuteDQLBoundsBatchAdmission(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
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
	options := ExecuteDQLOptions{MaxConcurrency: 1}
	go func() {
		_, err := p.RunExecuteDQL(ctx, options, []ExecuteDQLPrepare{executeDQLTestPrepare("first")}, execute)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first batch did not start")
	}
	go func() {
		_, err := p.RunExecuteDQL(ctx, options, []ExecuteDQLPrepare{executeDQLTestPrepare("second")}, execute)
		done <- err
	}()
	require.Eventually(t, func() bool { return p.CheckExecuteDQLAdmission() != nil }, time.Second, time.Millisecond)

	ran := make(chan struct{}, 1)
	prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
		ran <- struct{}{}
		return &milvuspb.SearchRequest{CollectionName: "third"}, nil
	}
	results, err := p.RunExecuteDQL(ctx, options, []ExecuteDQLPrepare{prepare}, execute)
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

	results, err = p.RunExecuteDQL(ctx, options, []ExecuteDQLPrepare{prepare}, execute)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, merr.Ok(results[0].GetStatus()))
}

func TestExecuteDQLKeepsRecordedResultsWhenLateSearchReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		release := make(chan struct{})
		releaseSearch := sync.OnceFunc(func() { close(release) })
		defer releaseSearch()
		canceled := make(chan error, 1)
		items := []ExecuteDQLPrepare{
			executeDQLTestPrepare("finished"),
			executeDQLTestPrepare("late"),
			executeDQLTestPrepare("undispatched"),
		}
		deadline := time.Now().Add(time.Second)
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1, ExecutionDeadline: deadline}, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
				canceled <- ctx.Err()
				<-release
			default:
				t.Error("dispatched a Search after expiry")
			}
			return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: req.GetCollectionName()}, nil
		})
		require.NoError(t, err)
		require.Len(t, results, 3)
		require.Equal(t, "finished", results[0].GetSearch().GetCollectionName())
		require.Equal(t, merr.TimeoutCode, results[1].GetStatus().GetCode())
		require.Equal(t, merr.TimeoutCode, results[2].GetStatus().GetCode())
		require.ErrorIs(t, <-canceled, context.DeadlineExceeded)

		releaseSearch()
		synctest.Wait()
		require.Equal(t, merr.TimeoutCode, results[1].GetStatus().GetCode())
		require.Nil(t, results[1].GetResult())
	})
}

func TestExecuteDQLExecutionBudgetReclaimsQueuedSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
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
			_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare(name)}, execute)
			finished <- err
		}
		go run("running")
		<-started

		type batchResult struct {
			items []*milvuspb.DQLResult
			err   error
		}
		expired := make(chan batchResult, 1)
		go func() {
			prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
				t.Error("prepared a batch after its deadline in the queue")
				return nil, merr.ErrParameterMissing
			}
			items, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{ExecutionDeadline: time.Now().Add(time.Second)}, []ExecuteDQLPrepare{prepare}, execute)
			expired <- batchResult{items, err}
		}()
		synctest.Wait()
		require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
		result := <-expired
		require.NoError(t, result.err)
		require.Len(t, result.items, 1)
		require.Equal(t, merr.TimeoutCode, result.items[0].GetStatus().GetCode())
		require.NoError(t, p.CheckExecuteDQLAdmission())

		go run("replacement")
		synctest.Wait()
		require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
		releaseSearch()
		require.NoError(t, <-finished)
		require.NoError(t, <-finished)
	})
}

func TestExecuteDQLParentCancellationEndsCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started := make(chan struct{})
		canceled := make(chan error, 1)
		finished := make(chan error, 1)
		go func() {
			items, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{ExecutionDeadline: time.Now().Add(time.Hour)}, []ExecuteDQLPrepare{executeDQLTestPrepare("running")}, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				close(started)
				<-ctx.Done()
				canceled <- ctx.Err()
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
		require.ErrorIs(t, <-canceled, context.Canceled)
	})
}

func TestExecuteDQLRetriesRateLimitedItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		start := time.Now()
		var limitedAttempts []time.Duration
		var readyAttempts []time.Duration
		prepare := []ExecuteDQLPrepare{executeDQLTestPrepare("limited"), executeDQLTestPrepare("ready")}
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1}, prepare, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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

func TestExecuteDQLRetriesProxyDQLAdmissionOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		p.sched.DqQueue.SetMaxTaskNum(0)

		attempts := make(map[string]int)
		items := []ExecuteDQLPrepare{
			executeDQLTestPrepare("admission_recovers"),
			executeDQLTestPrepare("admission_stays_full"),
			executeDQLTestPrepare("querynode_rejection"),
		}
		start := time.Now()
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1}, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
		require.True(t, proto.Equal(merr.Status(merr.WrapErrTooManyRequests(0)), results[1].GetStatus()),
			"queue-full status changed: %v", results[1].GetStatus())
		require.ErrorIs(t, merr.Error(results[2].GetStatus()), merr.ErrServiceTooManyRequests)
		require.Equal(t, map[string]int{"admission_recovers": 2, "admission_stays_full": 10, "querynode_rejection": 1}, attempts)
		require.Equal(t, 18*time.Second+200*time.Millisecond, time.Since(start))
	})
}

func TestExecuteDQLRetriesSearchByPKWhenMainDQLQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		cache := NewMockCache(t)
		cache.EXPECT().GetCollectionInfo(mock.Anything, "default", "test_collection", int64(0)).
			Return(&collectionInfo{CollID: 100, Schema: mustNewSchemaInfo(searchByPKTestSchema(true))}, nil)
		p.setMetaCache(&executeDQLTestCache{Cache: cache})
		p.sched.DqQueue.SetMaxTaskNum(0)

		// Keep the real Proxy Query enqueue and wait. The stored vector is null,
		// so a successful lookup returns one empty Search result without fan-out.
		defer mockey.Mock((*queryTask).PreExecute).Return(nil).Build().UnPatch()
		defer mockey.Mock((*queryTask).Execute).Return(nil).Build().UnPatch()
		defer mockey.Mock((*queryTask).PostExecute).Return(nil).Build().UnPatch()
		defer mockey.Mock((*queryTask).Result).Return(searchByPKQueryResult([]int64{1}, nil, []bool{false})).Build().UnPatch()

		items := []ExecuteDQLPrepare{func(context.Context) (*milvuspb.SearchRequest, error) {
			return searchByPKRequest([]int64{1}), nil
		}}
		var attempts atomic.Int32
		done := make(chan struct{})
		var results []*milvuspb.DQLResult
		var err error
		go func() {
			defer close(done)
			results, err = p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, items,
				func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					attempts.Add(1)
					return p.Search(ctx, req)
				})
		}()
		synctest.Wait()
		require.EqualValues(t, 1, attempts.Load())
		require.Empty(t, done, "the Search-by-PK attempt should wait for an admission retry")
		p.sched.DqQueue.SetMaxTaskNum(1)
		<-done
		require.NoError(t, err)
		require.EqualValues(t, 2, attempts.Load())
		require.Len(t, results, 1)
		require.NoError(t, merr.Error(results[0].GetStatus()))
		require.Equal(t, []int64{0}, results[0].GetSearch().GetResults().GetTopks())
	})
}

func TestExecuteDQLStopsRateLimitRetriesAndKeepsLastStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   time.Duration
		attempts []time.Duration
	}{
		{"exhausted", 0, []time.Duration{
			0, 200 * time.Millisecond, 600 * time.Millisecond, 1400 * time.Millisecond,
			3 * time.Second, 6 * time.Second, 9 * time.Second, 12 * time.Second, 15 * time.Second, 18 * time.Second,
		}},
		{"next backoff exceeds budget", time.Second, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewExecuteDQLTestProxy(t, 1, 1)
				retries := metrics.ProxyExecuteDQLRetriesTotal.WithLabelValues(paramtable.GetStringNodeID(), "rate_limit")
				beforeRetries := testutil.ToFloat64(retries)
				start := time.Now()
				options := ExecuteDQLOptions{}
				if tc.budget != 0 {
					options.ExecutionDeadline = start.Add(tc.budget)
				}
				var attempts []time.Duration
				var lastStatus *commonpb.Status
				items := []ExecuteDQLPrepare{executeDQLTestPrepare("limited")}
				results, err := p.RunExecuteDQL(t.Context(), options, items, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
				require.Equal(t, beforeRetries+float64(len(attempts)-1), testutil.ToFloat64(retries), "the final rejection does not start a retry")
			})
		})
	}
}

func TestExecuteDQLDoesNotRetryTerminalFailures(t *testing.T) {
	p := NewExecuteDQLTestProxy(t, 1, 1)
	failures := map[string]error{
		"force_deny":       merr.WrapErrServiceQuotaExceeded("administrator disabled reads"),
		"permission":       status.Error(codes.PermissionDenied, "no search privilege"),
		"invalid":          merr.ErrParameterInvalid,
		"retryable_status": merr.ErrServiceUnavailable,
		"marked_input":     merr.WrapErrAsInputError(merr.ErrServiceRateLimit),
	}
	attempts := make(map[string]int)
	items := make([]ExecuteDQLPrepare, 0, len(failures))
	codes := make([]int32, 0, len(failures))
	for name, err := range failures {
		items = append(items, executeDQLTestPrepare(name))
		code := merr.Code(err)
		if name == "permission" {
			code = merr.Code(merr.ErrPrivilegeNotPermitted)
		}
		codes = append(codes, code)
	}
	results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{MaxConcurrency: 1}, items, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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

func TestExecuteDQLCancelsBackoffOnParentCancellation(t *testing.T) {
	for name, operator := range map[string]bool{"client": false, "operator": true} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewExecuteDQLTestProxy(t, 1, 1)
				retries := metrics.ProxyExecuteDQLRetriesTotal.WithLabelValues(paramtable.GetStringNodeID(), "rate_limit")
				beforeRetries := testutil.ToFloat64(retries)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				finished := make(chan error, 1)
				attempts := 0
				go func() {
					items := []ExecuteDQLPrepare{executeDQLTestPrepare("limited"), executeDQLTestPrepare("undispatched")}
					results, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{MaxConcurrency: 1}, items, func(_ context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
						if request.GetCollectionName() != "limited" {
							t.Error("started an item after cancellation during backoff")
						}
						attempts++
						return nil, merr.ErrServiceRateLimit
					})
					if len(results) != 0 {
						t.Error("canceled batch returned results")
					}
					finished <- err
				}()
				synctest.Wait()
				var batchID string
				if operator {
					listed, err := p.ListLocalRunningRequests(t.Context(), &milvuspb.ListRunningRequestsRequest{})
					require.NoError(t, err)
					require.Len(t, listed.GetRequests(), 1)
					batchID = listed.Requests[0].GetRequestId()
				}
				// Cross a retry boundary and cancel during the next backoff.
				time.Sleep(250 * time.Millisecond)
				synctest.Wait()
				require.Equal(t, 2, attempts)
				if operator {
					listed, err := p.ListLocalRunningRequests(t.Context(), &milvuspb.ListRunningRequestsRequest{})
					require.NoError(t, err)
					require.Len(t, listed.GetRequests(), 1)
					require.Equal(t, batchID, listed.Requests[0].GetRequestId(), "retry keeps the batch identity")
					canceled, err := p.CancelLocalRequests(t.Context(), &milvuspb.CancelRequestsRequest{RequestIds: []string{batchID}})
					require.NoError(t, err)
					require.Len(t, canceled.GetCanceled(), 1)
					require.ErrorIs(t, <-finished, merr.ErrRequestCanceled)
				} else {
					cancel()
					require.ErrorIs(t, <-finished, context.Canceled)
				}
				synctest.Wait()
				require.Equal(t, 2, attempts)
				require.Equal(t, beforeRetries+1, testutil.ToFloat64(retries), "cancellation during backoff does not start another retry")
			})
		})
	}
}

func TestExecuteDQLRecordsEachTerminalOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics.RegisterProxy(registry)
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)

	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		unblock := make(chan struct{})
		releaseLateResult := sync.OnceFunc(func() { close(unblock) })
		defer releaseLateResult()
		nodeID := paramtable.GetStringNodeID()
		success := metrics.ProxyExecuteDQLSubrequestsTotal.WithLabelValues(nodeID, "success")
		timeout := metrics.ProxyExecuteDQLSubrequestsTotal.WithLabelValues(nodeID, "timeout")
		partial := metrics.ProxyExecuteDQLBatchesTotal.WithLabelValues(nodeID, "partial_success")
		retry := metrics.ProxyExecuteDQLRetriesTotal.WithLabelValues(nodeID, "rate_limit")
		attemptCounter := metrics.ProxyExecuteDQLAttemptsTotal.WithLabelValues(nodeID)
		beforeSuccess, beforeTimeout := testutil.ToFloat64(success), testutil.ToFloat64(timeout)
		beforePartial, beforeRetry := testutil.ToFloat64(partial), testutil.ToFloat64(retry)
		beforeAttempts := testutil.ToFloat64(attemptCounter)
		attempts := 0
		items := []ExecuteDQLPrepare{
			executeDQLTestPrepare("recovers"),
			executeDQLTestPrepare("late"),
			executeDQLTestPrepare("undispatched"),
		}
		options := ExecuteDQLOptions{MaxConcurrency: 1, ExecutionDeadline: time.Now().Add(time.Second)}
		results, err := p.RunExecuteDQL(t.Context(), options, items, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
		for _, name := range []string{
			"milvus_proxy_execute_dql_batches_total",
			"milvus_proxy_execute_dql_subrequests_total",
			"milvus_proxy_execute_dql_attempts_total",
			"milvus_proxy_execute_dql_retries_total",
		} {
			count, err := testutil.GatherAndCount(registry, name)
			require.NoError(t, err)
			require.Positive(t, count, name)
		}

		releaseLateResult()
		synctest.Wait()
		require.Equal(t, beforeSuccess+1, testutil.ToFloat64(success))
		require.Equal(t, beforeTimeout+2, testutil.ToFloat64(timeout))
		var batchSpan trace.ReadOnlySpan
		for _, span := range recorder.Ended() {
			if span.Name() == "Proxy-ExecuteDQL" {
				batchSpan = span
			}
		}
		require.NotNil(t, batchSpan)
		require.Len(t, batchSpan.Events(), 3)
		seen := make(map[int64]bool)
		for _, event := range batchSpan.Events() {
			values := make(map[string]any)
			for _, attr := range event.Attributes {
				values[string(attr.Key)] = attr.Value.AsInterface()
			}
			index := values["index"].(int64)
			require.False(t, seen[index], "duplicate completion event for item %d", index)
			seen[index] = true
			require.False(t, event.Time.After(batchSpan.EndTime()))
			if index == 0 {
				require.Equal(t, "success", values["result"])
				require.Equal(t, int64(600), values["duration_ms"])
				require.Equal(t, 600*time.Millisecond, event.Time.Sub(batchSpan.StartTime()), "emit accepted completion before batch expiry")
			} else {
				require.Equal(t, "timeout", values["result"])
				require.Equal(t, int64(1000), values["duration_ms"])
			}
		}
		require.Equal(t, map[int64]bool{0: true, 1: true, 2: true}, seen)
	})
}

func TestExecuteDQLTimeoutKeepsWorkerUntilExecutionExits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		release := make(chan struct{})
		releaseSearch := sync.OnceFunc(func() { close(release) })
		defer releaseSearch()
		results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{ExecutionDeadline: time.Now().Add(time.Second)},
			[]ExecuteDQLPrepare{executeDQLTestPrepare("slow cancellation")},
			func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				<-ctx.Done()
				<-release
				return &milvuspb.SearchResults{Status: merr.Success()}, nil
			})
		require.NoError(t, err)
		require.Equal(t, merr.TimeoutCode, results[0].GetStatus().GetCode())

		started := make(chan struct{}, 1)
		execute := func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			started <- struct{}{}
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		}
		done := make(chan error, 1)
		go func() {
			_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("waiting")}, execute)
			done <- err
		}()
		synctest.Wait()
		require.Empty(t, started, "a timed-out batch still owns its worker until execution exits")
		require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
		_, err = p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("rejected")}, execute)
		require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
		releaseSearch()
		require.NoError(t, <-done)
		require.Len(t, started, 1)
		require.Equal(t, merr.TimeoutCode, results[0].GetStatus().GetCode())
	})
}

func TestExecuteDQLQueuedParentCancellationReclaimsCapacity(t *testing.T) {
	for name, operator := range map[string]bool{"client": false, "operator": true} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewExecuteDQLTestProxy(t, 1, 1)
				release := make(chan struct{})
				releaseSearch := sync.OnceFunc(func() { close(release) })
				defer releaseSearch()
				started := make(chan struct{})
				running := make(chan error, 1)
				go func() {
					_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("running")},
						func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
							close(started)
							<-release
							return &milvuspb.SearchResults{Status: merr.Success()}, nil
						})
					running <- err
				}()
				<-started
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				queued := make(chan error, 1)
				prepare := func(context.Context) (*milvuspb.SearchRequest, error) {
					t.Error("prepared canceled queued batch")
					return nil, merr.ErrServiceInternal
				}
				execute := func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					return &milvuspb.SearchResults{Status: merr.Success()}, nil
				}
				go func() {
					results, err := p.RunExecuteDQL(ctx, ExecuteDQLOptions{}, []ExecuteDQLPrepare{prepare}, execute)
					if len(results) != 0 {
						t.Error("returned results after parent cancellation")
					}
					queued <- err
				}()
				synctest.Wait()
				require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
				if operator {
					listed, err := p.ListLocalRunningRequests(t.Context(), &milvuspb.ListRunningRequestsRequest{})
					require.NoError(t, err)
					require.Len(t, listed.GetRequests(), 2)
					var queuedID string
					for _, request := range listed.GetRequests() {
						if request.GetState() == reqregistry.StateQueued {
							queuedID = request.GetRequestId()
						}
					}
					require.NotEmpty(t, queuedID)
					canceled, err := p.CancelLocalRequests(t.Context(), &milvuspb.CancelRequestsRequest{RequestIds: []string{queuedID}})
					require.NoError(t, err)
					require.Len(t, canceled.GetCanceled(), 1)
					require.ErrorIs(t, <-queued, merr.ErrRequestCanceled)
				} else {
					cancel()
					require.ErrorIs(t, <-queued, context.Canceled)
				}
				require.NoError(t, p.CheckExecuteDQLAdmission())
				go func() {
					_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("replacement")}, execute)
					queued <- err
				}()
				synctest.Wait()
				require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
				releaseSearch()
				require.NoError(t, <-running)
				require.NoError(t, <-queued)
			})
		})
	}
}

func TestExecuteDQLSchedulerCloseCancelsAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewExecuteDQLTestProxy(t, 1, 1)
		release := make(chan struct{})
		releaseSearch := sync.OnceFunc(func() { close(release) })
		defer releaseSearch()
		started := make(chan struct{})
		canceled := make(chan error, 1)
		finished := make(chan error, 2)
		execute := func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() != "running" {
				t.Error("dispatched a queued batch during shutdown")
				return nil, merr.ErrServiceInternal
			}
			close(started)
			<-ctx.Done()
			canceled <- ctx.Err()
			<-release
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		}
		run := func(name string) {
			_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare(name)}, execute)
			finished <- err
		}
		go run("running")
		<-started
		go run("queued")
		synctest.Wait()
		require.ErrorIs(t, p.CheckExecuteDQLAdmission(), merr.ErrServiceTooManyRequests)
		closed := make(chan struct{})
		go func() {
			p.sched.Close()
			close(closed)
		}()
		require.ErrorIs(t, <-canceled, context.Canceled)
		for range 2 {
			require.ErrorIs(t, <-finished, merr.ErrServiceUnavailable)
		}
		synctest.Wait()
		require.Empty(t, closed, "Close must wait for canceled execution to exit")
		require.NoError(t, p.CheckExecuteDQLAdmission(), "queued capacity is reclaimed during shutdown")
		_, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{}, []ExecuteDQLPrepare{executeDQLTestPrepare("after shutdown")}, execute)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		releaseSearch()
		<-closed
	})
}

func TestExecuteDQLTaskIDAllocation(t *testing.T) {
	for _, outcome := range []string{"success", "allocator unavailable", "budget expires", "scheduler closes"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// A nil TSO allocator proves the batch uses the skip-timestamp path.
				sched, err := scheduler.NewTaskScheduler(t.Context(), nil)
				require.NoError(t, err)
				require.NoError(t, sched.Start())
				t.Cleanup(sched.Close)
				cache := NewMockCache(t)
				cache.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
					switch outcome {
					case "allocator unavailable":
						return 0, merr.ErrServiceUnavailable
					case "scheduler closes":
						sched.Close()
						<-ctx.Done()
						return 0, ctx.Err()
					case "budget expires":
						<-ctx.Done()
						return 0, ctx.Err()
					default:
						return 42, nil
					}
				}).Once()
				p := &Proxy{sched: sched}
				p.setMetaCache(cache)
				p.UpdateStateCode(commonpb.StateCode_Healthy)
				prepared := false
				results, err := p.RunExecuteDQL(t.Context(), ExecuteDQLOptions{ExecutionDeadline: time.Now().Add(time.Second)},
					[]ExecuteDQLPrepare{func(context.Context) (*milvuspb.SearchRequest, error) {
						prepared = true
						return &milvuspb.SearchRequest{}, nil
					}}, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
						return &milvuspb.SearchResults{Status: merr.Success()}, nil
					})
				if outcome == "allocator unavailable" || outcome == "scheduler closes" {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					require.Empty(t, results)
				} else {
					require.NoError(t, err)
					require.Len(t, results, 1)
					if outcome == "budget expires" {
						require.Equal(t, merr.TimeoutCode, results[0].GetStatus().GetCode())
					} else {
						require.NoError(t, merr.Error(results[0].GetStatus()))
					}
				}
				require.Equal(t, outcome == "success", prepared)
				require.NoError(t, p.CheckExecuteDQLAdmission())
			})
		})
	}
}
