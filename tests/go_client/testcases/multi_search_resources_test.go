package testcases

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// These probes need an otherwise idle, distributed deployment with one Proxy.
// Run explicitly with -run '^TestMultiSearch(ProxyRetainedHeap|ClusterQueueCapacity)$' and
// -args -multisearch-resource-probes. They are separate from shared E2E traffic.
var multiSearchResourceProbes = flag.Bool("multisearch-resource-probes", false, "run isolated MultiSearch resource probes")

func multiSearchRESTConfig() (string, string) {
	endpoint := strings.TrimRight(hp.GetURI(), "/")
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	token := hp.GetToken()
	if token == "" {
		token = hp.GetUser() + ":" + hp.GetPassword()
	}
	return endpoint, token
}

func multiSearchManagementRead(t *testing.T, ctx context.Context, path string) []byte {
	t.Helper()
	address := hp.GetURI()
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	u, err := url.Parse(address)
	require.NoError(t, err)
	endpoint := "http://" + net.JoinHostPort(u.Hostname(), "9091")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	require.NoError(t, err)
	user, password := hp.GetUser(), hp.GetPassword()
	if token := hp.GetToken(); token != "" {
		if tokenUser, tokenPassword, ok := strings.Cut(token, ":"); ok {
			user, password = tokenUser, tokenPassword
		}
	}
	req.SetBasicAuth(user, password)
	req.Header.Set("X-Milvus-Admin-Request", "true")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode, "management route %s", path)
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	require.NoError(t, err)
	return body
}

func multiSearchAttemptCount(t *testing.T, ctx context.Context) int64 {
	return multiSearchMetric(t, ctx, "milvus_proxy_multisearch_attempts_total")
}

func multiSearchMetric(t *testing.T, ctx context.Context, name string, labels ...string) int64 {
	t.Helper()
	var total int64
	scanner := bufio.NewScanner(strings.NewReader(string(multiSearchManagementRead(t, ctx, "/metrics"))))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		matches := true
		for _, label := range labels {
			matches = matches && strings.Contains(line, label)
		}
		if !matches {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 2)
		value, err := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, err)
		total += int64(value)
	}
	require.NoError(t, scanner.Err())
	return total
}

func multiSearchLiveHeap(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	// The server performs GC before writing this profile. Read only MemStats;
	// never emit the profile body, which may contain sensitive process data.
	body := multiSearchManagementRead(t, ctx, "/debug/pprof/heap?gc=1&debug=1")
	for line := range strings.SplitSeq(string(body), "\n") {
		if value, ok := strings.CutPrefix(line, "# HeapAlloc = "); ok {
			heap, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			require.NoError(t, err)
			return heap
		}
	}
	t.Fatal("heap profile did not contain HeapAlloc")
	return 0
}

func TestMultiSearchProxyRetainedHeap(t *testing.T) {
	if !*multiSearchResourceProbes {
		t.Skip("requires -multisearch-resource-probes and an idle distributed deployment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	const rows, dim, retainedItems = 1024, 256, 8
	name := common.GenRandomString("multisearch_heap", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema)))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	ids, vectors := make([]int64, rows), make([][]float32, rows)
	for i := range rows {
		ids[i] = int64(i)
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32((i+j)%17) / 17
		}
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", ids), column.NewColumnFloatVector("vector", dim, vectors)))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	build, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, build.Await(ctx))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	request, err := client.NewSearchOption(name, rows, []entity.Vector{entity.FloatVector(vectors[0])}).
		WithANNSField("vector").WithOutputFields("vector").WithConsistencyLevel(entity.ClEventually).Request()
	require.NoError(t, err)
	service := mc.GetService()
	warm, err := service.Search(ctx, request)
	require.NoError(t, merr.CheckRPCCall(warm, err))
	require.Len(t, warm.GetResults().GetFieldsData(), 1)
	require.Len(t, warm.GetResults().GetFieldsData()[0].GetVectors().GetFloatVector().GetData(), rows*dim)

	type outcome struct {
		response *milvuspb.MultiSearchResponse
		err      error
	}
	measure := func(completed int) (int64, int64) {
		blocked := proto.CloneOf(request)
		blocked.UseDefaultConsistency = false
		blocked.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
		blocked.GuaranteeTimestamp = tsoutil.ComposeTSByTime(time.Now().Add(10 * time.Minute))
		requests := make([]*milvuspb.SearchRequest, completed+1)
		for i := range completed {
			requests[i] = proto.CloneOf(request)
		}
		requests[completed] = blocked
		budget := int64(6000)
		before := multiSearchAttemptCount(t, ctx)
		result := make(chan outcome, 1)
		go func() {
			response, callErr := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
				Requests: requests, MaxConcurrency: 1, TimeoutMs: &budget,
			}, grpc.MaxCallRecvMsgSize(32<<20))
			result <- outcome{response, callErr}
		}()
		// At concurrency 1, starting the blocked final item proves that every
		// preceding native result has been accepted and retained by the batch.
		require.Eventually(t, func() bool {
			return multiSearchAttemptCount(t, ctx)-before == int64(completed+1)
		}, 3*time.Second, 20*time.Millisecond)
		select {
		case <-result:
			t.Fatal("future-timestamp Search did not keep the batch pending")
		default:
		}
		held := multiSearchLiveHeap(t, ctx)
		var received outcome
		select {
		case received = <-result:
		case <-ctx.Done():
			t.Fatal("batch did not finish within the probe deadline")
		}
		require.NoError(t, merr.CheckRPCCall(received.response, received.err))
		require.Len(t, received.response.GetResults(), completed+1)
		for i := range completed {
			item := received.response.Results[i]
			require.NoError(t, merr.Error(item.GetStatus()))
			require.Len(t, item.GetResults().GetFieldsData(), 1)
			require.Len(t, item.GetResults().GetFieldsData()[0].GetVectors().GetFloatVector().GetData(), rows*dim)
		}
		require.Equal(t, merr.TimeoutCode, received.response.Results[completed].GetStatus().GetCode())
		require.Equal(t, int64(completed+1), multiSearchAttemptCount(t, ctx)-before)
		return held, multiSearchLiveHeap(t, ctx)
	}

	retainedDeltas, releasedDeltas := make([]int64, 0, 3), make([]int64, 0, 3)
	for sample := range 3 {
		control, _ := measure(0)
		held, released := measure(retainedItems)
		retainedDeltas = append(retainedDeltas, held-control)
		releasedDeltas = append(releasedDeltas, released-control)
		t.Logf("MULTISEARCH_RETAINED_HEAP sample=%d native_vector_bytes=%d control_heap=%d held_heap=%d released_heap=%d retained_delta=%d released_delta=%d",
			sample, retainedItems*rows*dim*4, control, held, released, held-control, released-control)
	}
	slices.Sort(retainedDeltas)
	slices.Sort(releasedDeltas)
	// Check that the controlled payload is visible while pending and is
	// released afterwards, with room for unrelated small background objects.
	require.Greater(t, retainedDeltas[1], int64(retainedItems*rows*dim*4/2))
	require.Less(t, releasedDeltas[1], retainedDeltas[1]/2)
}

func TestMultiSearchClusterQueueCapacity(t *testing.T) {
	if !*multiSearchResourceProbes {
		t.Skip("requires -multisearch-resource-probes and an idle distributed deployment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := common.GenRandomString("multisearch_queue", 8)
	createMultiSearchCollection(ctx, t, mc, name, 1)
	search, err := client.NewSearchOption(name, 1, []entity.Vector{entity.FloatVector{1, 0}}).
		WithANNSField("vector").Request()
	require.NoError(t, err)
	search.UseDefaultConsistency = false
	search.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	search.GuaranteeTimestamp = tsoutil.ComposeTSByTime(time.Now().Add(10 * time.Minute))
	const workers, waiting, submitted = 8, 64, 80
	budget := int64(15000)
	batch := &milvuspb.MultiSearchRequest{Requests: []*milvuspb.SearchRequest{search}, TimeoutMs: &budget}
	type outcome struct {
		response *milvuspb.MultiSearchResponse
		err      error
	}
	results := make(chan outcome, submitted)
	blockedCtx, cancelBlocked := context.WithCancel(ctx)
	defer cancelBlocked()
	service := mc.GetService()
	before := multiSearchAttemptCount(t, ctx)
	submit := func() {
		go func() {
			response, callErr := service.MultiSearch(blockedCtx, proto.CloneOf(batch))
			results <- outcome{response, callErr}
		}()
	}
	for range workers {
		submit()
	}
	// Occupy the workers before filling the waiting queue. A simultaneous
	// burst can otherwise reach the queue before every worker has dequeued.
	require.Eventually(t, func() bool {
		return multiSearchAttemptCount(t, ctx)-before == workers
	}, 3*time.Second, 20*time.Millisecond)
	for range submitted - workers {
		submit()
	}
	refused := 0
	admissionDeadline := time.NewTimer(5 * time.Second)
	defer admissionDeadline.Stop()
	for refused < submitted-workers-waiting {
		select {
		case received := <-results:
			require.NoError(t, received.err)
			require.ErrorIs(t, merr.Error(received.response.GetStatus()), merr.ErrServiceTooManyRequests)
			require.Empty(t, received.response.GetResults())
			refused++
		case <-admissionDeadline.C:
			t.Fatalf("only %d queue refusals after submitting %d blocked batches", refused, submitted)
		}
	}
	require.Equal(t, int64(workers), multiSearchAttemptCount(t, ctx)-before,
		"only the worker-owned batches may start their Search")
	select {
	case unexpected := <-results:
		t.Fatalf("more than %d calls finished while the accepted batches should be blocked: %v", refused, unexpected.err)
	default:
	}
	endpoint, token := multiSearchRESTConfig()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(endpoint, "/")+"/v2/vectordb/entities/multi_search", bytes.NewBufferString("{"))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, response.StatusCode,
		"REST must reject the full batch queue before parsing an invalid body")
	cancelBlocked()
	for range submitted - refused {
		select {
		case received := <-results:
			require.Error(t, merr.CheckRPCCall(received.response, received.err), "canceled blocked batches must stop waiting")
		case <-ctx.Done():
			t.Fatal("accepted batches did not stop after parent cancellation")
		}
	}
	search.GuaranteeTimestamp = 0
	search.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
	require.Eventually(t, func() bool {
		recoveryCtx, recoveryCancel := context.WithTimeout(ctx, time.Second)
		defer recoveryCancel()
		response, callErr := service.MultiSearch(recoveryCtx, &milvuspb.MultiSearchRequest{
			Requests: []*milvuspb.SearchRequest{search},
		})
		return callErr == nil && merr.Ok(response.GetStatus()) && len(response.GetResults()) == 1 &&
			merr.Ok(response.Results[0].GetStatus())
	}, 5*time.Second, 50*time.Millisecond, "queue and workers must become reusable after cancellation")
	t.Logf("MULTISEARCH_QUEUE_CAPACITY workers=%d waiting=%d submitted=%d refused=%d rest_predecode_status=%d recovered=true",
		workers, waiting, submitted, refused, response.StatusCode)
}
