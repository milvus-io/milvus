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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// These queue and retry regressions need an otherwise idle deployment.
// Run explicitly with -run '^TestMultiSearch(ClusterQueueCapacity|QuotaRecovery|DQLQueueRecovery)$'
// and -args -multisearch-resource-probes, using each test's required configuration.
var multiSearchResourceProbes = flag.Bool("multisearch-resource-probes", false, "run isolated MultiSearch queue and retry regressions")

func multiSearchBlockingRequest(request *milvuspb.SearchRequest) *milvuspb.SearchRequest {
	blocked := proto.CloneOf(request)
	blocked.UseDefaultConsistency = false
	blocked.ConsistencyLevel = commonpb.ConsistencyLevel_Session
	blocked.GuaranteeTimestamp = tsoutil.ComposeTSByTime(time.Now().Add(10 * time.Minute))
	// QueryView samples ordinary Session reads as Strong. An iterator's fixed
	// snapshot instead reaches the cancellable WAL barrier for this timestamp.
	blocked.SearchParams = append(blocked.SearchParams, &commonpb.KeyValuePair{Key: "iterator", Value: "true"})
	return blocked
}

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
	blocked := multiSearchBlockingRequest(search)
	const workers, waiting, submitted = 8, 64, 80
	budget := int64(15000)
	batch := &milvuspb.MultiSearchRequest{Requests: []*milvuspb.SearchRequest{blocked}, TimeoutMs: &budget}
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
