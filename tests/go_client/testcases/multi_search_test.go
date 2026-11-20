package testcases

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestMultiSearchListAndCancel uses a WAL barrier for gRPC and a collection's
// rate limit for REST, so neither case depends on a search happening to be slow.
// A unique caller (gRPC) or trace ID (REST) isolates the batch from concurrent
// tests, including when REST authentication is disabled. DQL quotas must be enabled.
func TestMultiSearchListAndCancel(t *testing.T) {
	for _, transport := range []string{"grpc", "rest"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			root := hp.CreateDefaultMilvusClient(ctx, t)
			name := common.GenRandomString("multisearch_cancel", 8)
			createMultiSearchCollection(ctx, t, root, name, 1)
			user, password := common.GenRandomString("multisearch_caller", 8), common.GenRandomString("pwd", 12)
			require.NoError(t, root.CreateUser(ctx, client.NewCreateUserOption(user, password)))
			granted := false
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if granted {
					require.NoError(t, root.RevokeRole(cleanupCtx, client.NewRevokeRoleOption(user, "admin")))
				}
				require.NoError(t, root.DropUser(cleanupCtx, client.NewDropUserOption(user)))
			})
			require.NoError(t, root.GrantRole(ctx, client.NewGrantRoleOption(user, "admin")))
			granted = true
			caller := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{Username: user, Password: password})
			service := caller.GetService()
			ready, err := client.NewSearchOption(name, 1, []entity.Vector{entity.FloatVector{1, 0}}).
				WithANNSField("vector").WithConsistencyLevel(entity.ClEventually).Request()
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				response, callErr := service.Search(ctx, ready)
				return merr.CheckRPCCall(response, callErr) == nil
			}, 20*time.Second, 50*time.Millisecond, "the test caller must be ready")

			type outcome struct {
				httpStatus int
				code       int32
				reason     string
				items      int
				retriable  bool
				err        error
			}
			finished := make(chan outcome, 1)
			listRequest := &milvuspb.ListRunningRequestsRequest{User: user, MinElapsedMs: 100}
			batchTraceID := ""
			if transport == "grpc" {
				blocked := multiSearchBlockingRequest(ready)
				go func() {
					response, callErr := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
						Requests: []*milvuspb.SearchRequest{ready, blocked, proto.CloneOf(blocked), ready}, MaxConcurrency: 2,
					})
					finished <- outcome{
						code: response.GetStatus().GetCode(), reason: response.GetStatus().GetReason(),
						items: len(response.GetResults()), retriable: response.GetStatus().GetRetriable(), err: callErr,
					}
				}()
			} else {
				limitedName := common.GenRandomString("multisearch_cancel_limited", 8)
				createMultiSearchCollection(ctx, t, root, limitedName, 101)
				require.NoError(t, root.AlterCollectionProperties(ctx,
					client.NewAlterCollectionPropertiesOption(limitedName).WithProperty("collection.searchRate.max.vps", "1")))
				limited, requestErr := client.NewSearchOption(limitedName, 1, []entity.Vector{
					entity.FloatVector{1, 0}, entity.FloatVector{1, 0}, entity.FloatVector{1, 0}, entity.FloatVector{1, 0},
				}).WithANNSField("vector").Request()
				require.NoError(t, requestErr)
				require.Eventually(t, func() bool {
					response, callErr := service.Search(ctx, limited)
					return merr.Code(merr.CheckRPCCall(response, callErr)) == merr.Code(merr.ErrServiceRateLimit)
				}, 20*time.Second, 50*time.Millisecond, "the collection quota must reach the Proxy")
				payload, marshalErr := json.Marshal(map[string]any{
					"maxConcurrency": 1,
					"requests": []any{
						map[string]any{"search": map[string]any{"collectionName": name, "data": [][]float32{{1, 0}}, "limit": 1}},
						map[string]any{"search": map[string]any{"collectionName": limitedName, "data": [][]float32{{1, 0}, {1, 0}, {1, 0}, {1, 0}}, "limit": 1}},
						map[string]any{"search": map[string]any{"collectionName": name, "data": [][]float32{{1, 0}}, "limit": 1}},
					},
				})
				require.NoError(t, marshalErr)
				endpoint, _ := multiSearchRESTConfig()
				request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost,
					endpoint+"/v2/vectordb/entities/multi_search", bytes.NewReader(payload))
				require.NoError(t, requestErr)
				request.Header.Set("Authorization", "Bearer "+user+":"+password)
				request.Header.Set("Content-Type", "application/json")
				// With auth disabled, HTTP ignores the credential header and
				// registers an anonymous caller. The trace still identifies this
				// batch without selecting another test's anonymous requests.
				batchTraceID = strings.ReplaceAll(uuid.NewString(), "-", "")
				request.Header.Set("traceparent", "00-"+batchTraceID+"-0123456789abcdef-00")
				listRequest.User = ""
				go func() {
					response, callErr := http.DefaultClient.Do(request)
					got := outcome{err: callErr}
					if callErr == nil {
						defer response.Body.Close()
						got.httpStatus = response.StatusCode
						var body struct {
							Code    int32             `json:"code"`
							Message string            `json:"message"`
							Data    []json.RawMessage `json:"data"`
						}
						got.err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body)
						got.code, got.reason, got.items = body.Code, body.Message, len(body.Data)
					}
					finished <- got
				}()
			}

			selectBatch := func(list *milvuspb.ListRunningRequestsResponse) []*milvuspb.RunningRequestInfo {
				return slices.DeleteFunc(list.GetRequests(), func(info *milvuspb.RunningRequestInfo) bool {
					return batchTraceID != "" && info.GetTraceId() != batchTraceID
				})
			}
			var batch *milvuspb.RunningRequestInfo
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
				select {
				case got := <-finished:
					t.Fatalf("batch completed before cancellation: %+v", got)
				default:
				}
				list, callErr := service.ListRunningRequests(ctx, listRequest)
				require.NoError(t, merr.CheckRPCCall(list, callErr))
				for _, node := range list.GetNodeResults() {
					require.NoError(t, merr.Error(node.GetStatus()))
					require.False(t, node.GetUnimplemented())
				}
				requests := selectBatch(list)
				if len(requests) == 0 {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				require.Len(t, requests, 1, "a batch must not expose independent child Search requests")
				candidate := requests[0]
				require.Equal(t, "MultiSearch", candidate.GetType())
				if transport == "rest" {
					require.Contains(t, []string{user, ""}, candidate.GetUser())
				} else {
					require.Equal(t, user, candidate.GetUser())
				}
				require.True(t, candidate.GetCancellable())
				// The first Search completed before the controlled wait. Its
				// completion must not remove or finish the batch registration.
				if candidate.GetState() == "Running" && len(candidate.GetTaskIds()) >= 2 {
					batch = candidate
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			require.NotNil(t, batch, "the running batch must remain listed after its first Search completes")
			const reason = "cancel the complete MultiSearch batch"
			canceled, err := service.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{
				RequestIds: []string{batch.GetRequestId()}, Reason: reason,
			})
			require.NoError(t, merr.CheckRPCCall(canceled, err))
			require.Len(t, canceled.GetCanceled(), 1)
			require.Equal(t, batch.GetRequestId(), canceled.Canceled[0].GetRequestId())
			require.Empty(t, canceled.GetNotFound())
			require.Empty(t, canceled.GetUndetermined())
			select {
			case got := <-finished:
				require.NoError(t, got.err)
				if transport == "rest" {
					require.Equal(t, http.StatusOK, got.httpStatus)
				}
				require.Equal(t, merr.RequestCanceledCode, got.code)
				require.Contains(t, got.reason, reason)
				require.Zero(t, got.items)
				require.False(t, got.retriable)
			case <-time.After(5 * time.Second):
				t.Fatal("the canceled batch did not return")
			}
			require.Eventually(t, func() bool {
				list, callErr := service.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{User: listRequest.User})
				return merr.CheckRPCCall(list, callErr) == nil && len(selectBatch(list)) == 0
			}, 5*time.Second, 20*time.Millisecond)
			again, err := service.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []string{batch.GetRequestId()}})
			require.NoError(t, merr.CheckRPCCall(again, err))
			require.Equal(t, []string{batch.GetRequestId()}, again.GetNotFound())
			recovery, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{Requests: []*milvuspb.SearchRequest{ready}})
			require.NoError(t, merr.CheckRPCCall(recovery, err))
			require.Len(t, recovery.GetResults(), 1)
			require.NoError(t, merr.Error(recovery.Results[0].GetStatus()))
			t.Logf("MULTISEARCH_CANCEL transport=%s code=3002 task_count=%d recovered=true", transport, len(batch.GetTaskIds()))
		})
	}
}

func createMultiSearchCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, name string, pk int64) {
	t.Helper()
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("marker").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})

	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{pk, pk + 1}),
		column.NewColumnInt64("marker", []int64{1000 + pk, 2000 + pk}),
		column.NewColumnFloatVector("vector", 2, [][]float32{{1, 0}, {0, 1}}),
	))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	flat, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.IP)))
	require.NoError(t, err)
	require.NoError(t, flat.Await(ctx))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
}

func TestMultiSearchOrderedResults(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	db := common.GenRandomString("multisearch_db", 6)
	require.NoError(t, mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(db)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropDatabase(cleanupCtx, client.NewDropDatabaseOption(db)))
	})
	archive := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{DBName: db})
	name := common.GenRandomString("multisearch", 6)
	createMultiSearchCollection(ctx, t, mc, name, 1)
	createMultiSearchCollection(ctx, t, archive, name, 101)

	vectors := []entity.Vector{entity.FloatVector{1, 0}, entity.FloatVector{0, 1}}
	req, err := client.NewSearchOption(name, 3, vectors).
		WithANNSField("vector").
		WithOutputFields("marker").
		WithConsistencyLevel(entity.ClStrong).
		Request()
	require.NoError(t, err)

	explicit := proto.CloneOf(req)
	explicit.DbName = "default"
	empty := proto.CloneOf(req)
	empty.Dsl = "id < 0"
	missing := proto.CloneOf(req)
	missing.CollectionName += "_missing"

	service := archive.GetService()
	batch := &milvuspb.MultiSearchRequest{
		Requests:       []*milvuspb.SearchRequest{req, missing, explicit, empty},
		MaxConcurrency: 2,
	}
	response, err := service.MultiSearch(ctx, batch)
	require.NoError(t, merr.CheckRPCCall(response, err))
	require.Len(t, response.GetResults(), 4)

	for _, i := range []int{0, 2, 3} {
		ordinary, err := service.Search(ctx, batch.Requests[i])
		require.NoError(t, merr.CheckRPCCall(ordinary, err))
		item := response.Results[i]
		require.NotNil(t, item)
		require.NoError(t, merr.Error(item.GetStatus()))
		require.Equal(t, ordinary.GetCollectionName(), item.GetCollectionName())
		require.True(t, proto.Equal(ordinary.GetResults(), item.GetResults()), "item %d differs from Search", i)
		require.EqualValues(t, 2, item.GetResults().GetNumQueries())
	}
	require.Equal(t, []int64{2, 2}, response.Results[0].GetResults().GetTopks())
	require.Equal(t, int64(101), response.Results[0].GetResults().GetIds().GetIntId().GetData()[0])
	require.Equal(t, int64(1), response.Results[2].GetResults().GetIds().GetIntId().GetData()[0])
	failure := response.Results[1]
	require.NotNil(t, failure)
	require.ErrorIs(t, merr.Error(failure.GetStatus()), merr.ErrCollectionNotFound)
	require.Nil(t, failure.GetResults())
	require.Empty(t, response.Results[3].GetResults().GetIds().GetIntId().GetData())

	t.Run("high concurrency preserves every result", func(t *testing.T) {
		requests := slices.Repeat(batch.Requests, 5)
		resp, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
			Requests: requests, MaxConcurrency: ^uint32(0),
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), len(requests))
		for i, item := range resp.Results {
			want := response.Results[i%len(batch.Requests)]
			require.Equal(t, want.GetStatus().GetCode(), item.GetStatus().GetCode(), "item %d", i)
			require.True(t, proto.Equal(want.GetResults(), item.GetResults()), "item %d", i)
		}
	})

	t.Run("unused database metadata", func(t *testing.T) {
		missingDB := metadata.AppendToOutgoingContext(ctx, "dbname", db+"_missing")
		resp, err := service.MultiSearch(missingDB, &milvuspb.MultiSearchRequest{
			Requests: []*milvuspb.SearchRequest{explicit},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), 1)
		require.NoError(t, merr.Error(resp.Results[0].GetStatus()))
	})
	t.Run("empty search fails per item", func(t *testing.T) {
		resp, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
			Requests: []*milvuspb.SearchRequest{req, {}},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), 2)
		require.NoError(t, merr.Error(resp.Results[0].GetStatus()))
		require.ErrorIs(t, merr.Error(resp.Results[1].GetStatus()), merr.ErrParameterInvalid)
		require.Nil(t, resp.Results[1].GetResults())
	})
	t.Run("budget exhausted", func(t *testing.T) {
		zero := int64(0)
		resp, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
			Requests:       batch.GetRequests(),
			TimeoutMs:      &zero,
			MaxConcurrency: 1,
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), len(batch.GetRequests()))
		for _, item := range resp.GetResults() {
			require.NotNil(t, item)
			require.Equal(t, merr.TimeoutCode, item.GetStatus().GetCode())
			require.Nil(t, item.GetResults())
		}
	})
}
