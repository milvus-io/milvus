package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestQueryPlacementReleaseRunsFinishExactlyOnce is the pin-leak guard in its
// smallest form. milvus installs Release in a defer, and a defer can be reached
// twice when a caller also releases early on a rejection path; a Release that
// simply forwarded would then release the underlying pin twice, which on a
// reference-counted pin frees a resource group that another query is still
// using.
func TestQueryPlacementReleaseRunsFinishExactlyOnce(t *testing.T) {
	released := 0
	placement := QueryPlacement{Finish: func() { released++ }}

	placement.Release()
	assert.Equal(t, 1, released, "the first Release must run the finish callback")

	placement.Release()
	placement.Release()
	assert.Equal(t, 1, released, "releasing again must not run the finish callback a second time")
}

// TestQueryPlacementReleaseToleratesNothingToRelease covers the two shapes a
// caller legitimately holds: the zero placement a stock binary gets from the
// inert default, and the zero placement an implementation returns when its
// readiness check took nothing. Both reach the same deferred Release, so a
// Release that assumed a non-nil Finish would panic on every search in a stock
// binary.
func TestQueryPlacementReleaseToleratesNothingToRelease(t *testing.T) {
	tookNothing := QueryPlacement{}
	assert.NotPanics(t, tookNothing.Release,
		"a placement that took nothing must still be releasable")

	var absent *QueryPlacement
	assert.NotPanics(t, func() { absent.Release() },
		"a nil placement must be releasable, so a caller need not branch before its defer")
}

// TestQueryPlacementReleaseIsReachableThroughADefer pins the shape the seam
// depends on: `defer placement.Release()` on a local value must observe the
// Finish the value carries, not a copy taken at defer time.
func TestQueryPlacementReleaseIsReachableThroughADefer(t *testing.T) {
	released := false
	func() {
		placement := QueryPlacement{Finish: func() { released = true }}
		defer placement.Release()
	}()
	assert.True(t, released, "a deferred Release must run the finish callback the placement carries")
}

// TestNoopEnsureQueryReadyAdmitsAndScopesNothing pins both halves of the inert
// answer. An error here would refuse every search in a stock binary; a resource
// group here would silently restrict routing milvus is meant to leave alone.
func TestNoopEnsureQueryReadyAdmitsAndScopesNothing(t *testing.T) {
	placement, err := NoopProxyExtension{}.EnsureQueryReady(context.Background(), "db", "coll")

	assert.NoError(t, err, "the native default must admit the query, or a stock binary could not search")
	assert.Equal(t, "", placement.ResourceGroup,
		"the native default must not scope routing: a stock binary routes across every replica")
	assert.Nil(t, placement.Finish, "the native default took nothing, so it has nothing to release")
}

// TestNoopEnsureQueryReadyIsInheritedByEmbedders holds the promise the interface
// doc makes to implementations: embedding the default is enough, and an
// embedder that overrides nothing gets the admitting answer rather than a nil
// method value.
func TestNoopEnsureQueryReadyIsInheritedByEmbedders(t *testing.T) {
	type embedder struct{ NoopProxyExtension }

	var e ProxyExtension = embedder{}
	placement, err := e.EnsureQueryReady(context.Background(), "db", "coll")
	assert.NoError(t, err)
	assert.Equal(t, "", placement.ResourceGroup)
}

func TestQueryResourceGroupSurvivesOnTheContext(t *testing.T) {
	ctx := WithQueryResourceGroup(context.Background(), "rg-a")
	assert.Equal(t, "rg-a", QueryResourceGroupFromContext(ctx),
		"the routing scope must be readable by every stage that runs under the request's context")

	assert.Equal(t, "", QueryResourceGroupFromContext(context.Background()),
		"a context nothing scoped must report no scope, which is what every request in a stock binary looks like")
}

// TestQueryResourceGroupKeyIsUnreachableFromOutside proves nothing but this
// package can plant a routing scope. An extension carries its own values on the
// same context - that is what RewriteRequestParams is for - and one of them
// guessing the string "rg" or "resource_group" must not become the scope every
// shard-leader lookup honors. The scope is milvus's decision, made in exactly
// one place, and this is what keeps it that way.
func TestQueryResourceGroupKeyIsUnreachableFromOutside(t *testing.T) {
	ctx := context.Background()
	for _, guess := range []string{"rg", "resource_group", "queryResourceGroupKey"} {
		//nolint:staticcheck // planting a string key is exactly what is under test
		ctx = context.WithValue(ctx, guess, "forged-"+guess)
	}

	assert.Equal(t, "", QueryResourceGroupFromContext(ctx),
		"only WithQueryResourceGroup may set the scope routing reads back")
}

// TestQueryResourceGroupRebindShadowsTheOuterValue pins the precedence a nested
// bind has: a re-scoped context answers with the inner value while the context
// it derived from keeps answering with its own. A sub-query run under an outer
// request's context - the search-by-primary-key requery does exactly this -
// must not be able to re-route the request it runs inside.
func TestQueryResourceGroupRebindShadowsTheOuterValue(t *testing.T) {
	outer := WithQueryResourceGroup(context.Background(), "rg-outer")
	inner := WithQueryResourceGroup(outer, "rg-inner")

	assert.Equal(t, "rg-inner", QueryResourceGroupFromContext(inner))
	assert.Equal(t, "rg-outer", QueryResourceGroupFromContext(outer),
		"scoping a derived context must not rewrite the context it derived from")
}
