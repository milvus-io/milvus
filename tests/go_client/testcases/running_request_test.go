package testcases

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestCancelRequestsNotFound checks the answers that do not depend on timing:
// an id nobody holds is reported as not found,
// and a filter that matches nothing lists nothing while every proxy answers.
func TestCancelRequestsNotFound(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// an id of the right shape that no proxy issued
	const unknownID = "999999-to03zu-3a2"
	res, err := mc.CancelRequests(ctx, client.NewCancelRequestsOption(unknownID).WithReason("go sdk test"))
	common.CheckErr(t, err, true)
	require.Empty(t, res.Canceled)
	require.Equal(t, []string{unknownID}, res.NotFound)
	require.Empty(t, res.Undetermined)
	require.NotEmpty(t, res.NodeResults)
	for _, nr := range res.NodeResults {
		require.NoError(t, nr.Err, "proxy %d", nr.NodeID)
		require.False(t, nr.Unimplemented, "proxy %d", nr.NodeID)
	}

	list, err := mc.ListRunningRequests(ctx, client.NewListRunningRequestsOption().WithCollectionName("no_such_collection_"+common.GenRandomString("c", 6)))
	common.CheckErr(t, err, true)
	require.Empty(t, list.Requests)
	require.NotEmpty(t, list.NodeResults)
	for _, nr := range list.NodeResults {
		require.NoError(t, nr.Err, "proxy %d", nr.NodeID)
	}
}

// TestListAndCancelRunningSearch runs a wide search in the background, finds
// it with ListRunningRequests, cancels it by id and checks that the search
// fails with the cancel reason.
func TestListAndCancelRunningSearch(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100000))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewFlatIndex(entity.L2)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// A FLAT search over 100,000 rows with the maximum query count gives
	// List and Cancel time to observe the request.
	const nq = 16384
	const limit = 10
	const reason = "go sdk cancel test"
	vectors := hp.GenSearchVectors(nq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchCtx, cancelSearch := context.WithCancel(ctx)
	defer cancelSearch()
	searchErr := make(chan error, 1)
	go func() {
		_, err := mc.Search(searchCtx, client.NewSearchOption(schema.CollectionName, limit, vectors).
			WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
			WithConsistencyLevel(entity.ClStrong))
		searchErr <- err
	}()

	var running *entity.RunningRequest
	deadline := time.Now().Add(30 * time.Second)
	for running == nil && time.Now().Before(deadline) {
		select {
		case err := <-searchErr:
			t.Fatalf("Search finished before List captured it: %v", err)
		default:
		}
		list, err := mc.ListRunningRequests(ctx, client.NewListRunningRequestsOption().WithCollectionName(schema.CollectionName))
		common.CheckErr(t, err, true)
		if len(list.Requests) > 0 {
			running = list.Requests[0]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NotNil(t, running, "the search never showed up in ListRunningRequests")
	require.Equal(t, "Search", running.Type)
	require.Equal(t, schema.CollectionName, running.CollectionName)
	require.EqualValues(t, nq, running.NQ)
	require.EqualValues(t, limit, running.TopK)
	require.Contains(t, []string{"Queued", "Running"}, running.State)
	require.True(t, running.Cancellable)
	require.Regexp(t, `^\d+-[0-9a-z]+-[0-9a-z]+$`, running.ID)

	res, err := mc.CancelRequests(ctx, client.NewCancelRequestsOption(running.ID).WithReason(reason))
	common.CheckErr(t, err, true)
	if slices.Contains(res.NotFound, running.ID) {
		// Distinguish a missed cancellation window from a live request that
		// was incorrectly reported as missing. Both fail this lifecycle test.
		select {
		case err := <-searchErr:
			t.Fatalf("Search finished before Cancel: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("CancelRequests reported the id as not found, but the search is still running")
		}
	}
	require.Len(t, res.Canceled, 1)
	require.Equal(t, running.ID, res.Canceled[0].ID)
	require.Empty(t, res.NotFound)
	require.Empty(t, res.Undetermined)

	select {
	case err := <-searchErr:
		require.Error(t, err, "the canceled search returned a result")
		var coded interface{ Code() int32 }
		require.True(t, errors.As(err, &coded), "Search error has no server code: %v", err)
		require.EqualValues(t, 3002, coded.Code())
		require.True(t, strings.Contains(err.Error(), reason), "search error does not carry the cancel reason: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the search did not return within 60s of being canceled")
	}

	// Match the Python lifecycle: check List once immediately after error 3002.
	listed, err := mc.ListRunningRequests(ctx, client.NewListRunningRequestsOption().WithCollectionName(schema.CollectionName))
	require.NoError(t, err)
	require.NotEmpty(t, listed.NodeResults)
	for _, node := range listed.NodeResults {
		require.NoError(t, node.Err)
		require.False(t, node.Unimplemented)
	}
	for _, request := range listed.Requests {
		require.NotEqual(t, running.ID, request.ID, "Canceled Search remains registered")
	}
	control, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, limit, vectors[:1]).
		WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Len(t, control, 1)
	require.Equal(t, limit, control[0].ResultCount)

	// Canceling the same id again: it is gone, so it is reported as not found.
	again, err := mc.CancelRequests(ctx, client.NewCancelRequestsOption(running.ID))
	common.CheckErr(t, err, true)
	require.Empty(t, again.Canceled)
	require.Equal(t, []string{running.ID}, again.NotFound)
}

// TestCancelRequestsEmptyIDs rejects empty/nil IDs before any management RPC.
func TestCancelRequestsEmptyIDs(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	var calls atomic.Int32
	cfg := hp.GetDefaultClientConfig()
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/CancelRequests") {
				calls.Add(1)
			}
			return invoke(ctx, method, req, reply, cc, opts...)
		}))
	mc := hp.CreateMilvusClient(ctx, t, cfg)
	for _, ids := range [][]string{{}, nil} {
		_, err := mc.CancelRequests(ctx, client.NewCancelRequestsOption(ids...))
		require.Error(t, err)
		var coded interface{ Code() int32 }
		require.True(t, errors.As(err, &coded))
		require.EqualValues(t, 1101, coded.Code())
	}
	require.Zero(t, calls.Load(), "Invalid IDs issued a CancelRequests RPC")
}
