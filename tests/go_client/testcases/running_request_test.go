package testcases

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestCancelRequestsNotFound checks the answers that do not depend on timing:
// an id nobody holds is reported as not found, an empty id list is rejected,
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

	_, err = mc.CancelRequests(ctx, client.NewCancelRequestsOption())
	common.CheckErr(t, err, false, "request ids are required")

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
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*10))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Wide enough that the search runs for a while: many queries, a large
	// limit and a filter.
	const nq = 2000
	const limit = 1000
	const reason = "go sdk cancel test"
	vectors := hp.GenSearchVectors(nq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchErr := make(chan error, 1)
	go func() {
		_, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, limit, vectors).
			WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
			WithConsistencyLevel(entity.ClStrong))
		searchErr <- err
	}()

	var running *entity.RunningRequest
	deadline := time.Now().Add(30 * time.Second)
	for running == nil && time.Now().Before(deadline) {
		select {
		case err := <-searchErr:
			t.Skipf("the search finished before it could be listed (err=%v); nothing to cancel on this deployment", err)
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
		// The search may have finished between the list and the cancel. Skip
		// only if it really did: not found for a live request is a bug.
		select {
		case err := <-searchErr:
			t.Skipf("the search finished before it could be canceled (err=%v); nothing to cancel on this deployment", err)
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
		require.True(t, strings.Contains(err.Error(), reason), "search error does not carry the cancel reason: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the search did not return within 60s of being canceled")
	}

	// Canceling the same id again: it is gone, so it is reported as not found.
	again, err := mc.CancelRequests(ctx, client.NewCancelRequestsOption(running.ID))
	common.CheckErr(t, err, true)
	require.Empty(t, again.Canceled)
	require.Equal(t, []string{running.ID}, again.NotFound)
}
