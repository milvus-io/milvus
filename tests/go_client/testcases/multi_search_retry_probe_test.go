package testcases

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Run alone on the controlled retry deployment (quota enabled and a short
// quota collection interval), with -multisearch-resource-probes.
func TestMultiSearchQuotaRecovery(t *testing.T) {
	if !*multiSearchResourceProbes {
		t.Skip("requires -multisearch-resource-probes and an isolated quota-enabled deployment")
	}
	for _, route := range []string{"grpc", "rest"} {
		t.Run(route, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			readyName := common.GenRandomString("multisearch_ready", 8)
			limitedName := common.GenRandomString("multisearch_limited", 8)
			createMultiSearchCollection(ctx, t, mc, readyName, 1)
			createMultiSearchCollection(ctx, t, mc, limitedName, 101)
			makeRequest := func(name string, nq int) *milvuspb.SearchRequest {
				vectors := make([]entity.Vector, nq)
				for i := range nq {
					vectors[i] = entity.FloatVector{1, 0}
				}
				request, err := client.NewSearchOption(name, 1, vectors).WithANNSField("vector").
					WithConsistencyLevel(entity.ClEventually).Request()
				require.NoError(t, err)
				return request
			}
			ready, limited := makeRequest(readyName, 1), makeRequest(limitedName, 4)
			service := mc.GetService()
			response, err := service.Search(ctx, ready)
			require.NoError(t, merr.CheckRPCCall(response, err))
			setRate := func(value string) {
				require.NoError(t, mc.AlterCollectionProperties(ctx,
					client.NewAlterCollectionPropertiesOption(limitedName).WithProperty("collection.searchRate.max.vps", value)))
			}
			awaitRefusal := func(wanted error) {
				require.Eventually(t, func() bool {
					response, callErr := service.Search(ctx, limited)
					return errors.Is(merr.CheckRPCCall(response, callErr), wanted)
				}, 15*time.Second, 20*time.Millisecond, "collection rate change must reach the Proxy")
			}
			call := func(includeReady bool, budget *int64) []int32 {
				requests := []*milvuspb.SearchRequest{limited}
				if includeReady {
					requests = append([]*milvuspb.SearchRequest{ready}, requests...)
				}
				if route == "grpc" {
					response, callErr := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
						Requests: requests, MaxConcurrency: 2, TimeoutMs: budget,
					})
					require.NoError(t, merr.CheckRPCCall(response, callErr))
					require.Len(t, response.GetResults(), len(requests))
					codes := make([]int32, len(requests))
					for i, item := range response.Results {
						codes[i] = merr.Code(merr.Error(item.GetStatus()))
					}
					return codes
				}
				items := make([]map[string]any, len(requests))
				for i, request := range requests {
					nq := 4
					if request.CollectionName == readyName {
						nq = 1
					}
					vectors := make([][]float32, nq)
					for j := range nq {
						vectors[j] = []float32{1, 0}
					}
					items[i] = map[string]any{"search": map[string]any{
						"collectionName": request.CollectionName, "data": vectors, "annsField": "vector",
						"limit": 1, "consistencyLevel": "Eventually",
					}}
				}
				payload := map[string]any{"requests": items, "maxConcurrency": 2}
				if budget != nil {
					payload["timeoutMs"] = *budget
				}
				body, marshalErr := json.Marshal(payload)
				require.NoError(t, marshalErr)
				endpoint, token := multiSearchRESTConfig()
				request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost,
					endpoint+"/v2/vectordb/entities/multi_search", bytes.NewReader(body))
				require.NoError(t, requestErr)
				request.Header.Set("Authorization", "Bearer "+token)
				request.Header.Set("Content-Type", "application/json")
				response, callErr := (&http.Client{Timeout: time.Minute}).Do(request)
				require.NoError(t, callErr)
				defer response.Body.Close()
				require.Equal(t, http.StatusOK, response.StatusCode)
				var result struct {
					Code int32 `json:"code"`
					Data []struct {
						Search struct {
							Code int32 `json:"code"`
						} `json:"search"`
					} `json:"data"`
				}
				require.NoError(t, json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result))
				require.Zero(t, result.Code)
				require.Len(t, result.Data, len(requests))
				codes := make([]int32, len(result.Data))
				for i, item := range result.Data {
					codes[i] = item.Search.Code
				}
				return codes
			}
			retries := func() int64 {
				return multiSearchMetric(t, ctx, "milvus_proxy_multisearch_retries_total", `reason="rate_limit"`)
			}
			readyCalls := func() int64 {
				return multiSearchMetric(t, ctx, "milvus_proxy_req_count", `function_name="Search"`,
					`status="total"`, `collection_name="`+readyName+`"`)
			}
			setRate("4")
			awaitRefusal(merr.ErrServiceRateLimit)
			attemptsBefore, retriesBefore, readyBefore := multiSearchAttemptCount(t, ctx), retries(), readyCalls()
			budget := int64(5000)
			started := time.Now()
			require.Equal(t, []int32{0, 0}, call(true, &budget))
			retryCount := retries() - retriesBefore
			require.Positive(t, retryCount)
			require.Equal(t, int64(2)+retryCount, multiSearchAttemptCount(t, ctx)-attemptsBefore)
			require.Equal(t, int64(1), readyCalls()-readyBefore, "the completed item must not run again")
			t.Logf("MULTISEARCH_QUOTA route=%s scenario=recovery retries=%d elapsed_ms=%d ready_attempts=1", route, retryCount, time.Since(started).Milliseconds())

			// Passing through zero lets the observed error distinguish the old
			// limiter from the subsequently applied, very small positive rate.
			setRate("0")
			awaitRefusal(merr.ErrServiceQuotaExceeded)
			setRate("0.001")
			awaitRefusal(merr.ErrServiceRateLimit)
			attemptsBefore, retriesBefore = multiSearchAttemptCount(t, ctx), retries()
			started = time.Now()
			require.Equal(t, []int32{merr.Code(merr.ErrServiceRateLimit)}, call(false, nil))
			require.Equal(t, int64(10), multiSearchAttemptCount(t, ctx)-attemptsBefore)
			require.Equal(t, int64(9), retries()-retriesBefore)
			t.Logf("MULTISEARCH_QUOTA route=%s scenario=attempt_limit attempts=10 retries=9 elapsed_ms=%d", route, time.Since(started).Milliseconds())

			attemptsBefore, retriesBefore = multiSearchAttemptCount(t, ctx), retries()
			budget = 450
			require.Equal(t, []int32{merr.Code(merr.ErrServiceRateLimit)}, call(false, &budget))
			require.Equal(t, int64(2), multiSearchAttemptCount(t, ctx)-attemptsBefore)
			require.Equal(t, int64(1), retries()-retriesBefore)
			t.Logf("MULTISEARCH_QUOTA route=%s scenario=remaining_budget budget_ms=450 attempts=2 retries=1", route)

			previous, configErr := hp.GetServerConfig("quotaAndLimits.limitReading.forceDeny")
			require.NoError(t, configErr)
			t.Cleanup(func() {
				_, restoreErr := hp.AlterServerConfig("quotaAndLimits.limitReading.forceDeny", previous)
				require.NoError(t, restoreErr)
			})
			_, configErr = hp.AlterServerConfig("quotaAndLimits.limitReading.forceDeny", "true")
			require.NoError(t, configErr)
			require.Eventually(t, func() bool {
				response, callErr := service.Search(ctx, ready)
				return errors.Is(merr.CheckRPCCall(response, callErr), merr.ErrServiceQuotaExceeded)
			}, 15*time.Second, 50*time.Millisecond)
			attemptsBefore, retriesBefore = multiSearchAttemptCount(t, ctx), retries()
			require.Equal(t, []int32{merr.Code(merr.ErrServiceQuotaExceeded), merr.Code(merr.ErrServiceQuotaExceeded)}, call(true, nil))
			require.Equal(t, int64(2), multiSearchAttemptCount(t, ctx)-attemptsBefore)
			require.Equal(t, retriesBefore, retries())
			t.Logf("MULTISEARCH_QUOTA route=%s scenario=force_deny attempts=2 retries=0", route)
			_, configErr = hp.AlterServerConfig("quotaAndLimits.limitReading.forceDeny", previous)
			require.NoError(t, configErr)
			require.Eventually(t, func() bool {
				response, callErr := service.Search(ctx, ready)
				return merr.CheckRPCCall(response, callErr) == nil
			}, 15*time.Second, 50*time.Millisecond)
		})
	}
}

func TestMultiSearchDQLQueueRecovery(t *testing.T) {
	if !*multiSearchResourceProbes {
		t.Skip("requires -multisearch-resource-probes and an isolated deployment with proxy.maxTaskNum=8")
	}
	maxTasks, err := hp.GetServerConfig("proxy.maxTaskNum")
	require.NoError(t, err)
	require.Equal(t, "8", maxTasks)
	for _, route := range []string{"grpc", "rest"} {
		t.Run(route, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			name := common.GenRandomString("multisearch_dql", 8)
			createMultiSearchCollection(ctx, t, mc, name, 1)
			request, requestErr := client.NewSearchOption(name, 1, []entity.Vector{entity.FloatVector{1, 0}}).
				WithANNSField("vector").WithConsistencyLevel(entity.ClEventually).Request()
			require.NoError(t, requestErr)
			blocked := multiSearchBlockingRequest(request)
			service := mc.GetService()
			blockedCtx, release := context.WithCancel(ctx)
			defer release()
			const blockers = 32
			blockerResults := make(chan error, blockers)
			for range blockers {
				go func() {
					response, callErr := service.Search(blockedCtx, proto.CloneOf(blocked))
					blockerResults <- merr.CheckRPCCall(response, callErr)
				}()
			}
			select {
			case refusal := <-blockerResults:
				require.ErrorIs(t, refusal, merr.ErrServiceTooManyRequests, "ordinary Search must fill the actual DQL queue")
			case <-time.After(5 * time.Second):
				t.Fatal("DQL queue did not fill")
			}
			retries := func() int64 {
				return multiSearchMetric(t, ctx, "milvus_proxy_multisearch_retries_total", `reason="queue_full"`)
			}
			before, attempts := retries(), multiSearchAttemptCount(t, ctx)
			type outcome struct {
				outer int32
				item  int32
				err   error
			}
			result := make(chan outcome, 1)
			if route == "grpc" {
				go func() {
					budget := int64(5000)
					response, callErr := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
						Requests: []*milvuspb.SearchRequest{request}, TimeoutMs: &budget,
					})
					got := outcome{outer: -1, item: -1, err: callErr}
					if response != nil {
						got.outer = merr.Code(merr.Error(response.GetStatus()))
						if len(response.Results) == 1 {
							got.item = merr.Code(merr.Error(response.Results[0].GetStatus()))
						}
					}
					result <- got
				}()
			} else {
				body, marshalErr := json.Marshal(map[string]any{
					"requests": []any{map[string]any{"search": map[string]any{
						"collectionName": name, "data": [][]float32{{1, 0}}, "annsField": "vector", "limit": 1,
					}}}, "timeoutMs": 5000,
				})
				require.NoError(t, marshalErr)
				endpoint, token := multiSearchRESTConfig()
				httpRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodPost,
					endpoint+"/v2/vectordb/entities/multi_search", bytes.NewReader(body))
				require.NoError(t, requestErr)
				httpRequest.Header.Set("Authorization", "Bearer "+token)
				httpRequest.Header.Set("Content-Type", "application/json")
				go func() {
					response, callErr := (&http.Client{Timeout: 10 * time.Second}).Do(httpRequest)
					got := outcome{outer: -1, item: -1, err: callErr}
					if callErr == nil {
						defer response.Body.Close()
						var decoded struct {
							Code int32 `json:"code"`
							Data []struct {
								Search struct {
									Code int32 `json:"code"`
								} `json:"search"`
							} `json:"data"`
						}
						got.err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&decoded)
						if response.StatusCode == http.StatusOK {
							got.outer = decoded.Code
							if len(decoded.Data) == 1 {
								got.item = decoded.Data[0].Search.Code
							}
						}
					}
					result <- got
				}()
			}
			require.Eventually(t, func() bool { return retries() > before }, 3*time.Second, 20*time.Millisecond,
				"an admitted batch must actually retry a DQL refusal")
			release()
			select {
			case received := <-result:
				require.NoError(t, received.err)
				require.Zero(t, received.outer)
				require.Zero(t, received.item)
			case <-ctx.Done():
				t.Fatal("batch did not recover when ordinary Search released the DQL queue")
			}
			for range blockers - 1 {
				select {
				case <-blockerResults:
				case <-ctx.Done():
					t.Fatal("DQL blockers did not terminate")
				}
			}
			retryCount := retries() - before
			require.Positive(t, retryCount)
			require.Equal(t, retryCount+1, multiSearchAttemptCount(t, ctx)-attempts)
			t.Logf("MULTISEARCH_DQL_RECOVERY route=%s retries=%d recovered=true", route, retryCount)
		})
	}
}
