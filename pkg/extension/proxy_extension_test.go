package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

func TestNoopProxyExtensionIsInert(t *testing.T) {
	n := NoopProxyExtension{}

	assert.Nil(t, n.InterceptDML(context.Background(), "Insert", &milvuspb.InsertRequest{}),
		"the native default must not reject a write, or a stock binary would refuse its own DML")

	assert.NoError(t, n.OnConnect(1, &commonpb.ClientInfo{}),
		"the native default must not refuse a connection, or a stock binary could not be connected to")

	// Start must return rather than block, and must not panic on a nil
	// registry: a caller is entitled to hand it one, and the inert default
	// never touches it.
	n.Start(context.Background(), nil)
}

// TestNoopRewriteRequestParamsReturnsItsArgumentsUntouched pins the one inert
// answer that is not the zero value. Every caller installs both returns, so a
// native default that returned nil or a fresh empty slice would silently erase
// the search parameters of any implementation that embedded it without
// overriding this method, and one that derived a context would allocate on
// every DQL request a stock binary serves.
//
// Equality is not enough for either: the assertions are on identity, the very
// same context and the very same backing array.
func TestNoopRewriteRequestParamsReturnsItsArgumentsUntouched(t *testing.T) {
	params := []*commonpb.KeyValuePair{
		{Key: "metric_type", Value: "L2"},
		{Key: "x-form-reserved", Value: "in07-a"},
	}
	ctx := context.Background()

	gotCtx, cleaned := NoopProxyExtension{}.RewriteRequestParams(ctx, params)

	assert.True(t, ctx == gotCtx, "the native default must hand back the caller's own context, not a derived one")
	require.Len(t, cleaned, 2)
	assert.True(t, &params[0] == &cleaned[0],
		"the native default must hand back the caller's own slice, reserved-looking entry included: it has no protocol of its own to strip")
}

// TestNoopProxyExtensionFallsThroughOnLoadSemantics pins the inert answer for
// the load-semantics group. nil is "fall through", and it is the only answer a
// stock binary can give: a success status is what an implementation of these
// returns, and a native default that answered one would turn every load,
// release and progress query in a community build into a no-op that reported
// success - the collection never loaded, the client never told.
func TestNoopProxyExtensionFallsThroughOnLoadSemantics(t *testing.T) {
	n := NoopProxyExtension{}
	ctx := context.Background()

	assert.Nil(t, n.InterceptLoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: "coll"}),
		"a stock binary must load the collection its client asked for")
	assert.Nil(t, n.InterceptLoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: "coll", Refresh: true}),
		"a stock binary must let a refresh reach querycoord")
	assert.Nil(t, n.InterceptReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: "coll"}),
		"a stock binary must release the collection its client asked for")
	assert.Nil(t, n.InterceptLoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{CollectionName: "coll"}),
		"a stock binary must load the partitions its client asked for")
	assert.Nil(t, n.InterceptLoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{CollectionName: "coll", Refresh: true}),
		"a stock binary must let a refresh reach querycoord")
	assert.Nil(t, n.InterceptReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{CollectionName: "coll"}),
		"a stock binary must release the partitions its client asked for")
	assert.Nil(t, n.InterceptGetLoadState(ctx, &milvuspb.GetLoadStateRequest{CollectionName: "coll"}),
		"a stock binary must report the load state it actually has")
	assert.Nil(t, n.InterceptGetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{CollectionName: "coll"}),
		"a stock binary must report the loading progress it actually has")
}

// TestNoopProxyExtensionEmbedsWithoutOverride checks the promise the doc
// comment makes to implementations: embedding the noop is enough to satisfy
// the interface, and an embedder that overrides nothing still gets the inert
// answer rather than a nil method value.
func TestNoopProxyExtensionEmbedsWithoutOverride(t *testing.T) {
	type embedder struct{ NoopProxyExtension }

	var e ProxyExtension = embedder{}
	assert.Nil(t, e.InterceptDML(context.Background(), "Insert", &milvuspb.InsertRequest{}),
		"an implementation that overrides nothing must inherit the inert answer")

	params := []*commonpb.KeyValuePair{{Key: "metric_type", Value: "L2"}}
	ctx := context.Background()
	gotCtx, cleaned := e.RewriteRequestParams(ctx, params)
	assert.True(t, ctx == gotCtx,
		"an embedder that overrides nothing must inherit the pass-through, not a nil method value")
	assert.Equal(t, params, cleaned)
	assert.NoError(t, e.OnConnect(1, nil))
	e.Start(context.Background(), nil)

	assert.Nil(t, e.InterceptLoadCollection(ctx, &milvuspb.LoadCollectionRequest{}),
		"an embedder that overrides nothing must inherit the fall-through, not take over the load")
	assert.Nil(t, e.InterceptReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{}))
	assert.Nil(t, e.InterceptLoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{}))
	assert.Nil(t, e.InterceptReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{}))
	assert.Nil(t, e.InterceptGetLoadState(ctx, &milvuspb.GetLoadStateRequest{}))
	assert.Nil(t, e.InterceptGetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{}))
}
