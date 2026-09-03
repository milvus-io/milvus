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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// readyExtension is a proxy extension whose EnsureQueryReady behaves the way an
// on-demand form's does: it hands back a resource group to route to, and it
// hands back a release that must be run whether or not it also reported a
// failure.
type readyExtension struct {
	extension.NoopProxyExtension

	mu sync.Mutex
	// fail, when set, is returned as the readiness error.
	fail error
	// resourceGroup is what a successful readiness reports as the placement.
	resourceGroup string
	// takesAPin decides whether the returned placement carries a Finish. An
	// implementation that failed after taking a pin returns one; one that
	// failed before taking anything does not.
	takesAPin bool

	calls          int
	releases       int
	dbsFor         []string
	collectionsFor []string
}

func (r *readyExtension) EnsureQueryReady(_ context.Context, _ extension.Coordinator, dbName, collectionName string) (extension.QueryPlacement, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.dbsFor = append(r.dbsFor, dbName)
	r.collectionsFor = append(r.collectionsFor, collectionName)

	placement := extension.QueryPlacement{}
	if r.takesAPin {
		placement.Finish = func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.releases++
		}
	}
	if r.fail != nil {
		return placement, r.fail
	}
	placement.ResourceGroup = r.resourceGroup
	return placement, nil
}

func (r *readyExtension) snapshot() (calls, releases int, dbs, collections []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.releases, append([]string(nil), r.dbsFor...), append([]string(nil), r.collectionsFor...)
}

func installReadyExtension(t *testing.T, ext *readyExtension) *readyExtension {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{ProxyExt: ext},
	}))
	return ext
}

func healthyProxy() *Proxy {
	node := &Proxy{sched: &taskScheduler{dqQueue: &dqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node
}

// ---------------------------------------------------------------------------
// Inertness
// ---------------------------------------------------------------------------

// TestEnsureQueryReadyIsInertWithNoProvider is the inertness proof for the gate.
// Equality of the context is not enough: a seam that rebuilt an equal context
// would allocate on every search in a stock binary, so this asserts the very
// same context comes back.
func TestEnsureQueryReadyIsInertWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	node := &Proxy{}
	// Abnormal on purpose: with no provider the gate must not even reach the
	// health check, so an unhealthy stock proxy is admitted here and rejected
	// later by milvus's own handler, exactly as it was before this seam.
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	ctx := context.Background()
	got, placement, err := ensureQueryReady(ctx, node, "db", "coll")

	assert.NoError(t, err, "a stock binary must not reject its own query")
	assert.True(t, got == ctx, "with no provider the request must run under the caller's own context")
	assert.Nil(t, placement.Finish, "with no provider there is nothing to release")
	assert.Equal(t, "", placement.ResourceGroup, "with no provider routing must not be scoped")
	assert.Equal(t, "", extension.QueryResourceGroupFromContext(got),
		"a stock binary must route across every replica, so no scope may reach the shard-leader lookup")
}

// TestSearchAdmitsWithNoProvider is the entry-point half of the inertness proof:
// the added gate must not turn a stock search into a rejection.
func TestSearchAdmitsWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	defer mockey.Mock((*baseTaskQueue).Enqueue).Return(nil).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := healthyProxy()
	resp, err := node.Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode(),
		"a stock binary must serve a search the gate never had an opinion about")
}

// ---------------------------------------------------------------------------
// The gate itself
// ---------------------------------------------------------------------------

// TestEnsureQueryReadyAsksAboutTheQueryAndNothingElse pins what milvus hands
// the gate: the database and the collection the query is against, which is all
// milvus knows about it. Anything a form itself needs is on the context, bound
// by its own RewriteRequestParams and read back under its own key - milvus does
// not carry it, and there is no parameter here for it to be carried in.
func TestEnsureQueryReadyAsksAboutTheQueryAndNothingElse(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-a"})

	_, _, err := ensureQueryReady(context.Background(), healthyProxy(), "db", "coll")
	require.NoError(t, err)

	calls, _, dbs, collections := ext.snapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, []string{"db"}, dbs, "the gate must be told which database the query is against")
	assert.Equal(t, []string{"coll"}, collections,
		"the gate must be told which collection the query is against")
}

// TestEnsureQueryReadyRunsTheGateOnTheRequestsOwnContext pins the coupling
// between the two seams: whatever RewriteRequestParams bound is only readable
// here if the gate is called with the context that came back from it. A seam
// that dropped the rewritten context would leave every form unable to tell what
// its own request declared.
func TestEnsureQueryReadyRunsTheGateOnTheRequestsOwnContext(t *testing.T) {
	type probeKey struct{}
	seen := make(chan any, 1)
	ext := &readyExtension{resourceGroup: "rg-a"}
	installReadyExtension(t, ext)
	defer mockey.Mock((*readyExtension).EnsureQueryReady).To(
		func(_ *readyExtension, ctx context.Context, _ extension.Coordinator, _, _ string) (extension.QueryPlacement, error) {
			seen <- ctx.Value(probeKey{})
			return extension.QueryPlacement{ResourceGroup: "rg-a"}, nil
		}).Build().UnPatch()

	//nolint:staticcheck // a struct key on a test-local type is the point
	ctx := context.WithValue(context.Background(), probeKey{}, "bound-by-the-rewrite")
	_, _, err := ensureQueryReady(ctx, healthyProxy(), "db", "coll")
	require.NoError(t, err)

	assert.Equal(t, "bound-by-the-rewrite", <-seen,
		"the gate must run on the context the request carries, or nothing an extension bound earlier is readable")
}

// TestEnsureQueryReadyIsConsultedForARequestThatDeclaredNothing is the seam's
// half of a policy decision that is deliberately not milvus's. milvus consults
// the gate for every DQL request, with no opinion on whether the form has
// anything to go on, because only an implementation knows whether a request that
// told it nothing is a control-plane client to wave through or a data-plane
// client to refuse. A seam that short-circuited would take the decision away
// from every deployment form at once.
func TestEnsureQueryReadyIsConsultedForARequestThatDeclaredNothing(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-default"})

	_, _, err := ensureQueryReady(context.Background(), healthyProxy(), "db", "coll")
	require.NoError(t, err)

	calls, _, _, _ := ext.snapshot()
	assert.Equal(t, 1, calls, "the gate must still be consulted for a request that declared nothing")
}

// TestSearchIsRefusedWhenTheFormSaysSo is the same policy seen from the entry
// point, with an extension that refuses - which is what an on-demand form does
// for a request it has no compute to run on. The refusal must reach the client
// as the extension worded it, including the name of a parameter milvus itself
// has never heard of.
func TestSearchIsRefusedWhenTheFormSaysSo(t *testing.T) {
	installReadyExtension(t, &readyExtension{
		fail: errors.New("this query declares nothing to run it on: pass " + testReservedParamKey),
	})

	// WaitToFinish is stubbed alongside Enqueue even though a refused query
	// reaches neither: it keeps this test failing on its assertions rather than
	// crashing in the search path if the gate ever stops refusing.
	enqueued := 0
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		enqueued++
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	resp, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
	assert.Contains(t, resp.GetStatus().GetReason(), testReservedParamKey,
		"a client that omitted the parameter must be told which one it omitted, in the form's own words")
	assert.Equal(t, 0, enqueued, "a query the form refused must not run")
}

// TestSearchIsServedWithNoProvider is the other side of the same decision: the
// refusal is one form's policy, not milvus's, so a stock binary must serve a
// search that declares nothing exactly as it always has.
func TestSearchIsServedWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	enqueued := 0
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		enqueued++
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	resp, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode(),
		"a stock binary must serve a search that declares nothing; the requirement belongs to one form, not to milvus")
	assert.Equal(t, 1, enqueued)
}

// TestEnsureQueryReadyBindsTheResourceGroupItWasGiven is the routing half of the
// coupling: whatever resource group readiness reported must be the scope the
// rest of the request runs under.
func TestEnsureQueryReadyBindsTheResourceGroupItWasGiven(t *testing.T) {
	installReadyExtension(t, &readyExtension{resourceGroup: "rg-a"})

	got, _, err := ensureQueryReady(context.Background(), healthyProxy(), "db", "coll")
	require.NoError(t, err)
	assert.Equal(t, "rg-a", extension.QueryResourceGroupFromContext(got),
		"the query must be routed to the resource group it was just made ready on")
}

// TestEnsureQueryReadyBindsNoScopeWhenReadinessNamesNone keeps the unscoped
// answer meaning "every replica" rather than "a resource group called the empty
// string", which would match none and fail every query.
func TestEnsureQueryReadyBindsNoScopeWhenReadinessNamesNone(t *testing.T) {
	installReadyExtension(t, &readyExtension{resourceGroup: ""})

	ctx := context.Background()
	got, _, err := ensureQueryReady(ctx, healthyProxy(), "db", "coll")
	require.NoError(t, err)
	assert.True(t, got == ctx, "an unscoped readiness must not wrap the context at all")
	assert.Equal(t, "", extension.QueryResourceGroupFromContext(got))
}

// TestEnsureQueryReadyDoesNotWakeAClusterForAnUnhealthyProxy is why the health
// check lives in the seam. Waking a cluster's query nodes costs money and takes
// seconds; doing it for a query the proxy is about to refuse anyway is a bill
// for nothing.
func TestEnsureQueryReadyDoesNotWakeAClusterForAnUnhealthyProxy(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-a"})

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	_, placement, err := ensureQueryReady(context.Background(), node, "db", "coll")
	assert.Error(t, err, "an unhealthy proxy must refuse the query")
	assert.Nil(t, placement.Finish)

	calls, _, _, _ := ext.snapshot()
	assert.Equal(t, 0, calls, "an unhealthy proxy must not reach the extension at all")
}

// ---------------------------------------------------------------------------
// Release on every exit path
// ---------------------------------------------------------------------------

// TestSearchReleasesThePlacementWhenReadinessFails is the leak guard, driven
// through the real entry point. An implementation that got part-way - started
// the cluster, took the pin that stops the idle sweep reclaiming it, then timed
// out waiting for the collection to load - hands the pin back alongside its
// error. If Search skipped the release because there was an error, that
// resource group would look busy forever and its query nodes would never scale
// down again.
func TestSearchReleasesThePlacementWhenReadinessFails(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{
		fail:      errors.New("timed out waiting for the collection to load"),
		takesAPin: true,
	})

	enqueued := 0
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		enqueued++
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	resp, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode(),
		"a failed readiness must reject the query, not let it run against a cluster that is not up")
	assert.Contains(t, resp.GetStatus().GetReason(), "timed out waiting for the collection to load",
		"the client must be told why its query was refused")
	assert.Equal(t, 0, enqueued, "a rejected query must never reach the task queue")

	_, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, releases,
		"the pin taken by a readiness that then failed must still be released, or the resource group never scales down again")
}

func TestQueryReleasesThePlacementWhenReadinessFails(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{fail: errors.New("cluster is starting"), takesAPin: true})

	// A refused query never reaches the queue. The stub is here so that a gate
	// which stopped refusing makes this test fail on its assertions instead of
	// crashing further down the query path, where nothing else is wired.
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		return errors.New("the query must not have reached the queue")
	}).Build().UnPatch()

	node := healthyProxy()
	require.NoError(t, node.initRateCollector())

	resp, err := node.Query(context.Background(), &milvuspb.QueryRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())

	_, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, releases, "Query must release the pin a failed readiness handed back")
}

func TestHybridSearchReleasesThePlacementWhenReadinessFails(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{fail: errors.New("cluster is starting"), takesAPin: true})

	// See the note in TestQueryReleasesThePlacementWhenReadinessFails: the stub
	// keeps a gate that stopped refusing visible as a failed assertion rather
	// than as a crash further down the search path.
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		return errors.New("the hybrid search must not have reached the queue")
	}).Build().UnPatch()

	resp, err := healthyProxy().HybridSearch(context.Background(), &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())

	_, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, releases, "HybridSearch must release the pin a failed readiness handed back")
}

// TestSearchReleasesThePlacementWhenTheSearchSucceeds is the other half: the
// pin must be held for the whole request and let go once, at the end. A release
// that never ran is the same leak seen from the success side.
func TestSearchReleasesThePlacementWhenTheSearchSucceeds(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-a", takesAPin: true})

	heldDuringSearch := -1
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		_, releases, _, _ := ext.snapshot()
		heldDuringSearch = releases
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	resp, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetStatus().GetCode())

	assert.Equal(t, 0, heldDuringSearch,
		"the pin must still be held while the search runs, or the idle sweep could reclaim the query nodes mid-query")
	_, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, releases, "the pin must be released exactly once when the request finishes")
}

// TestSearchReleasesThePlacementWhenTheSearchPanics covers the exit path a
// caller cannot see coming. The release is deferred rather than run at the end
// of the handler precisely so that a panic deeper in the request still gives
// the resource group back.
func TestSearchReleasesThePlacementWhenTheSearchPanics(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-a", takesAPin: true})

	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(*baseTaskQueue, task) error {
		panic("query node client exploded")
	}).Build().UnPatch()

	assert.Panics(t, func() {
		_, _ = healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	})

	_, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, releases, "a panic on the request path must still give the resource group back")
}

// TestSearchAsksReadinessOnceForTheWholeRetryLoop pins where the gate sits.
// node.search runs more than once for a single client search (the result-limit
// retry, the recall evaluation), and each of those attempts must run inside the
// one readiness answer rather than waking the cluster again and taking a second
// pin the handler would only release once.
func TestSearchAsksReadinessOnceForTheWholeRetryLoop(t *testing.T) {
	ext := installReadyExtension(t, &readyExtension{resourceGroup: "rg-a", takesAPin: true})

	searches := 0
	defer mockey.Mock((*Proxy).search).To(func(*Proxy, context.Context, *milvuspb.SearchRequest, bool, bool) (*milvuspb.SearchResults, bool, bool, bool, error) {
		searches++
		return &milvuspb.SearchResults{Status: &commonpb.Status{}}, false, false, false, nil
	}).Build().UnPatch()

	_, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	require.Positive(t, searches)

	calls, releases, _, _ := ext.snapshot()
	assert.Equal(t, 1, calls, "one client search must produce one readiness check, however many attempts it makes")
	assert.Equal(t, 1, releases, "one readiness check must produce one release")
}

// ---------------------------------------------------------------------------
// The coupling: made ready on a resource group, routed to that resource group
// ---------------------------------------------------------------------------

// TestSearchRunsTheTaskScopedToTheResourceGroup is the whole point of the
// slice. The shard-leader lookup reads its scope off the context the task runs
// under, so if the scope does not reach the task, the query is made ready on
// one resource group and served by whichever replica the collection-wide
// answer happened to list.
func TestSearchRunsTheTaskScopedToTheResourceGroup(t *testing.T) {
	installReadyExtension(t, &readyExtension{resourceGroup: "rg-a"})

	var scope string
	seen := false
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(_ *baseTaskQueue, t task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		seen = true
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	_, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)

	require.True(t, seen, "the search must have reached the task queue")
	assert.Equal(t, "rg-a", scope,
		"the task must run scoped to the resource group readiness named, or routing ignores it")
}

func TestQueryRunsTheTaskScopedToTheResourceGroup(t *testing.T) {
	installReadyExtension(t, &readyExtension{resourceGroup: "rg-b"})

	var scope string
	seen := false
	// The task is refused after its context has been read, so the handler
	// unwinds instead of running a query against half-built state; what is
	// under test is the context the task was built with, not the query.
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(_ *baseTaskQueue, t task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		seen = true
		return errors.New("refused after the scope was observed")
	}).Build().UnPatch()

	node := healthyProxy()
	require.NoError(t, node.initRateCollector())
	_, err := node.Query(context.Background(), &milvuspb.QueryRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)

	require.True(t, seen, "the query must have reached the task queue")
	assert.Equal(t, "rg-b", scope, "the query task must run scoped to the resource group readiness named")
}

// TestSearchTaskCarriesNoScopeWithNoProvider is the routing half of the
// inertness proof, taken at the same place: in a stock binary the task must run
// with no scope at all, so the shard-leader lookup asks the collection-wide
// question milvus has always asked.
func TestSearchTaskCarriesNoScopeWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	scope := "unset"
	defer mockey.Mock((*baseTaskQueue).Enqueue).To(func(_ *baseTaskQueue, t task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	_, err := healthyProxy().Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"})
	require.NoError(t, err)
	assert.Equal(t, "", scope, "a stock binary must route across every replica of the collection")
}
