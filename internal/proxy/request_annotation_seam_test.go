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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// testReservedParamKey is the DQL parameter the form below reserves for itself.
// It is deliberately a word milvus has no notion of: the seam must strip
// whatever an extension says it strips, and the tests must fail if milvus ever
// starts recognizing a key of its own.
const testReservedParamKey = "x-form-reserved"

// formAnnotationKey is the fake form's private context key. It stands for the
// key a real extension uses to carry what it lifted off a request: milvus
// cannot name it, cannot read it, and must not need to.
type formAnnotationKey struct{}

// formExtension is a proxy extension that annotates requests the way a real
// deployment form does - it takes its own parameter off the request, carries
// what it found under its own context key, reads it back when milvus asks
// whether the query may run, and answers with a routing decision of its own -
// so the seams are exercised against behavior rather than against a recorder
// that answers constants.
//
// Its decision is deliberately not its input: a request that declared "in07-a"
// is routed to the resource group "rg-in07-a". Nothing milvus does may confuse
// the two.
type formExtension struct {
	extension.NoopProxyExtension

	mu       sync.Mutex
	bindings map[int64]string
	// refuse, when set, is returned from OnConnect for every connection.
	refuse error

	connectCalls    int
	rewriteCalls    int
	readyCalls      int
	startCalls      int
	connsSeen       extension.ProxyConnections
	startCtxWasDone chan struct{}

	// rewrittenCtx and cleanedParams are what RewriteRequestParams last
	// returned, so a test can assert the seam installed THOSE rather than
	// something that merely compares equal to them.
	rewrittenCtx  context.Context
	cleanedParams []*commonpb.KeyValuePair
}

func newFormExtension() *formExtension {
	return &formExtension{
		bindings:        map[int64]string{},
		startCtxWasDone: make(chan struct{}),
	}
}

func (c *formExtension) OnConnect(_ context.Context, identifier int64, info *commonpb.ClientInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectCalls++
	if c.refuse != nil {
		return c.refuse
	}
	if id := info.GetReserved()[testReservedParamKey]; id != "" {
		c.bindings[identifier] = id
	}
	return nil
}

func (c *formExtension) RewriteRequestParams(ctx context.Context, params []*commonpb.KeyValuePair) (context.Context, []*commonpb.KeyValuePair) {
	cleaned := make([]*commonpb.KeyValuePair, 0, len(params))
	annotation := ""
	for _, p := range params {
		if p.GetKey() == testReservedParamKey {
			if v := strings.TrimSpace(p.GetValue()); v != "" {
				annotation = v
			}
			continue
		}
		cleaned = append(cleaned, p)
	}
	if annotation != "" {
		ctx = context.WithValue(ctx, formAnnotationKey{}, annotation)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.rewriteCalls++
	c.rewrittenCtx = ctx
	c.cleanedParams = cleaned
	return ctx, cleaned
}

// EnsureQueryReady reads back what the form itself bound, then falls back to
// what the connection declared - the two routes a real form has, neither of
// which milvus takes part in.
func (c *formExtension) EnsureQueryReady(ctx context.Context, _ extension.Coordinator, _, _ string) (extension.QueryPlacement, error) {
	c.mu.Lock()
	c.readyCalls++
	conns := c.connsSeen
	c.mu.Unlock()

	annotation, _ := ctx.Value(formAnnotationKey{}).(string)
	if annotation == "" && conns != nil {
		if identifier, ok := conns.IdentifierFromContext(ctx); ok {
			c.mu.Lock()
			annotation = c.bindings[identifier]
			c.mu.Unlock()
		}
	}
	if annotation == "" {
		// This form waves an unannotated request through unscoped; whether that
		// is right is its own business, and milvus must not have an opinion.
		return extension.QueryPlacement{}, nil
	}
	return extension.QueryPlacement{ResourceGroup: resourceGroupFor(annotation)}, nil
}

// resourceGroupFor is the form's mapping from what a client declared to where
// the form decided the query runs. It exists so that the declaration and the
// decision are never the same string in these tests.
func resourceGroupFor(annotation string) string { return "rg-" + annotation }

func (c *formExtension) Start(ctx context.Context, conns extension.ProxyConnections) {
	c.mu.Lock()
	c.startCalls++
	c.connsSeen = conns
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		close(c.startCtxWasDone)
	}()
}

func (c *formExtension) counts() (connect, rewrite, ready, start int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectCalls, c.rewriteCalls, c.readyCalls, c.startCalls
}

func (c *formExtension) lastRewrite() (context.Context, []*commonpb.KeyValuePair) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rewrittenCtx, c.cleanedParams
}

func installFormExtension(t *testing.T) *formExtension {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	ext := newFormExtension()
	require.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{ProxyExt: ext},
	}))
	return ext
}

func noProviderInstalled(t *testing.T) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
}

func kv(pairs ...string) []*commonpb.KeyValuePair {
	out := make([]*commonpb.KeyValuePair, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonpb.KeyValuePair{Key: pairs[i], Value: pairs[i+1]})
	}
	return out
}

func keysOf(params []*commonpb.KeyValuePair) []string {
	keys := make([]string, 0, len(params))
	for _, p := range params {
		keys = append(keys, p.GetKey())
	}
	return keys
}

// ---------------------------------------------------------------------------
// Inertness: with no provider installed every seam must be a nil comparison.
// ---------------------------------------------------------------------------

// TestRewriteRequestParamsIsIdentityWithNoProvider is the inertness proof for
// the DQL entry seam. Equality is not enough here: a seam that rebuilt an equal
// slice would still allocate on every search in a stock binary, so this asserts
// the very same slice header comes back, and the very same context.
func TestRewriteRequestParamsIsIdentityWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	ctx := context.Background()
	params := kv("metric_type", "L2", testReservedParamKey, "in07-a")

	gotCtx, gotParams := rewriteRequestParams(ctx, params)

	assert.True(t, ctx == gotCtx, "the seam must not derive a context when no provider is installed")
	require.Len(t, gotParams, 2)
	assert.Same(t, params[0], gotParams[0])
	assert.Same(t, params[1], gotParams[1],
		"with no provider installed even the reserved-looking entry must be left alone: only an extension knows the key is reserved")
	assert.True(t, &params[0] == &gotParams[0],
		"the returned slice must be the caller's own backing array, not a copy of it")
}

func TestOnConnectIsInertWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	assert.NoError(t, onConnect(context.Background(), 42, &commonpb.ClientInfo{
		Reserved: map[string]string{testReservedParamKey: "in07-a"},
	}), "with no provider installed the handshake must not be able to fail here")
}

func TestStartProxyExtensionIsInertWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	// Nothing to observe but the absence of a panic: with no provider there is
	// no goroutine, no ticker and no registry adapter constructed.
	startProxyExtension(context.Background())
}

// TestObserveResourceGroupSQLatencyEmitsNoSeriesWithNoProvider is the inertness
// proof for the attribution site. A metric family that gained a series in a
// stock binary would be a behavior change even though no request behaves
// differently, so the assertion is on the family's series count, not on a
// value.
func TestObserveResourceGroupSQLatencyEmitsNoSeriesWithNoProvider(t *testing.T) {
	noProviderInstalled(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	observeResourceGroupSQLatency(context.Background(), metrics.SearchLabel, "db", "coll", 12)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupSQLatency),
		"a stock binary must expose no per-resource-group series at all")
}

// TestObserveResourceGroupSQLatencyEmitsNoSeriesForAnUnscopedRequest covers the
// other half: a provider is installed, but nothing scoped this request. An
// unscoped request must not land in a series labeled with an empty resource
// group, which would silently pool every unrouted call into one bucket.
func TestObserveResourceGroupSQLatencyEmitsNoSeriesForAnUnscopedRequest(t *testing.T) {
	installFormExtension(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	observeResourceGroupSQLatency(context.Background(), metrics.SearchLabel, "db", "coll", 12)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupSQLatency),
		"installing a provider must not by itself attribute a request to a resource group")
}

// TestObserveResourceGroupSQLatencyAttributesToTheScopeRoutingUsed pins which
// of the two candidate values the label carries: the scope the query was
// actually routed with, which is the only one milvus itself decided on and
// honored.
func TestObserveResourceGroupSQLatencyAttributesToTheScopeRoutingUsed(t *testing.T) {
	installFormExtension(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")
	observeResourceGroupSQLatency(ctx, metrics.SearchLabel, "db", "coll", 17)

	h, err := metrics.ProxyResourceGroupSQLatency.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel, "db", "coll", "rg-a")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), histogramCount(t, h),
		"the completed search must land in the series of the resource group that served it")
	assert.Equal(t, 1, collectSeries(t, metrics.ProxyResourceGroupSQLatency),
		"exactly one series: a second one would mean the scope was read twice differently")
}

func collectSeries(t *testing.T, c prometheus.Collector) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	n := 0
	for range ch {
		n++
	}
	return n
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	m, ok := o.(prometheus.Metric)
	require.True(t, ok, "a histogram child must also be a Metric")
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetHistogram().GetSampleCount()
}

// ---------------------------------------------------------------------------
// The DQL entry seam.
// ---------------------------------------------------------------------------

// TestRewriteRequestParamsInstallsWhatTheExtensionReturned is the test the whole
// seam hangs on. A seam whose returns are the caller's own arguments looks
// correct in isolation and still ships the reserved key to segcore, and one that
// dropped the context would lose the only record of what the request said. So
// both assertions are on identity: the very context and the very slice the
// extension handed back, which is what the call sites install on the request.
func TestRewriteRequestParamsInstallsWhatTheExtensionReturned(t *testing.T) {
	ext := installFormExtension(t)

	params := kv("metric_type", "L2", testReservedParamKey, "in07-a", "nprobe", "16")
	in := context.Background()
	gotCtx, gotParams := rewriteRequestParams(in, params)

	wantCtx, wantParams := ext.lastRewrite()
	assert.True(t, wantCtx == gotCtx,
		"the seam must hand on the extension's own context, not the one it was called with")
	assert.False(t, in == gotCtx, "test precondition: this extension did derive a context")
	require.Len(t, gotParams, 2,
		"the reserved entry must be gone from what the seam handed back, or the caller's own slice was returned instead")
	assert.True(t, &wantParams[0] == &gotParams[0],
		"the seam must hand on the extension's own cleaned slice, not a copy and not the caller's")

	assert.Equal(t, []string{"metric_type", "nprobe"}, keysOf(gotParams),
		"the reserved key must be gone from what replaces the request's parameters")
	assert.Equal(t, []string{"metric_type", testReservedParamKey, "nprobe"}, keysOf(params),
		"the caller's own slice must be left intact: milvus may still log or retry the request as it arrived")
}

// TestRewriteRequestParamsInstallsTheCleanedParamsEvenWithNothingBound pins the
// case a "only install when the context changed" shortcut gets wrong: an entry
// whose value is blank leaves the extension nothing to bind, and the cleaned
// slice must still replace the request's own or the reserved key reaches
// segcore anyway.
func TestRewriteRequestParamsInstallsTheCleanedParamsEvenWithNothingBound(t *testing.T) {
	installFormExtension(t)

	params := kv("metric_type", "L2", testReservedParamKey, "   ")
	in := context.Background()
	gotCtx, cleaned := rewriteRequestParams(in, params)

	assert.Equal(t, []string{"metric_type"}, keysOf(cleaned),
		"a blank reserved entry is still a reserved entry and must be stripped")
	assert.True(t, in == gotCtx,
		"with nothing bound the extension returned the caller's context, and the seam must not wrap it")
}

// ---------------------------------------------------------------------------
// The Connect seam.
// ---------------------------------------------------------------------------

func TestOnConnectReachesTheInstalledExtension(t *testing.T) {
	ext := installFormExtension(t)

	require.NoError(t, onConnect(context.Background(), 7, &commonpb.ClientInfo{
		Reserved: map[string]string{testReservedParamKey: "in07-a"},
	}))

	connects, _, _, _ := ext.counts()
	assert.Equal(t, 1, connects)
	ext.mu.Lock()
	defer ext.mu.Unlock()
	assert.Equal(t, "in07-a", ext.bindings[7],
		"the identifier the handshake is about to hand the client must be the one bound")
}

func TestOnConnectSurfacesARefusal(t *testing.T) {
	ext := installFormExtension(t)
	sentinel := errors.New("client declared something unusable")
	ext.refuse = sentinel

	err := onConnect(context.Background(), 7, &commonpb.ClientInfo{})
	assert.Same(t, sentinel, err, "the refusal must reach the handshake unchanged")
}

// TestConnectRefusalLeavesNoRegisteredConnection pins the ordering the call
// site depends on: OnConnect runs BEFORE the connection is registered, so a
// refused client leaves nothing behind in the connection manager for the
// disconnect sweep to trip over.
// NOTE: Connect registers the connection in the process-global
// connection.GetManager(), which exposes no per-entry removal - registrations
// made here outlive the test and are collected only by the manager's own
// inactivity sweep. Keep identifiers unique per test so leftovers cannot
// collide.
func TestConnectRefusalLeavesNoRegisteredConnection(t *testing.T) {
	ext := installFormExtension(t)
	ext.refuse = errors.New("refused")

	const identifier = int64(918273645)
	require.False(t, connection.GetManager().Has(identifier), "test precondition")

	assert.Error(t, onConnect(context.Background(), identifier, &commonpb.ClientInfo{}))
	assert.False(t, connection.GetManager().Has(identifier),
		"a refused connection must never be registered")
}

// newConnectTestProxy builds the smallest proxy whose Connect handshake runs to
// completion: a coordinator that lists the default database and an allocator
// that hands out the identifier the client is about to be given.
func newConnectTestProxy(t *testing.T, identifier int64) *Proxy {
	t.Helper()
	mockCoord := mocks.NewMockMixCoordClient(t)
	mockCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
		Status:  merr.Success(),
		DbNames: []string{util.DefaultDBName},
	}, nil)

	mockey.Mock((*timestampAllocator).AllocOne).Return(Timestamp(identifier), nil).Build()
	t.Cleanup(mockey.UnPatchAll)

	node := &Proxy{mixCoord: mockCoord, tsoAllocator: &timestampAllocator{}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node
}

// TestConnectBindsTheIdentifierItHandsTheClient pins the Connect call site: the
// identifier the extension is told about must be the very one the response
// carries, or every later request would look up a binding that was filed under
// a different key.
func TestConnectBindsTheIdentifierItHandsTheClient(t *testing.T) {
	ext := installFormExtension(t)

	const identifier = int64(20260812)
	node := newConnectTestProxy(t, identifier)

	resp, err := node.Connect(context.Background(), &milvuspb.ConnectRequest{
		ClientInfo: &commonpb.ClientInfo{Reserved: map[string]string{testReservedParamKey: "in07-a"}},
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetStatus().GetCode(), "the handshake must succeed")
	require.Equal(t, identifier, resp.GetIdentifier())

	ext.mu.Lock()
	defer ext.mu.Unlock()
	assert.Equal(t, "in07-a", ext.bindings[resp.GetIdentifier()],
		"Connect must bind the identifier it returns to the client")
}

// TestConnectRefusedByTheExtensionRegistersNothing pins the ordering: a refusal
// fails the handshake AND leaves no registered connection behind, which is only
// true while the seam sits ahead of connection.GetManager().Register.
func TestConnectRefusedByTheExtensionRegistersNothing(t *testing.T) {
	ext := installFormExtension(t)
	ext.refuse = errors.New("client declared something unusable")

	const identifier = int64(20260813)
	node := newConnectTestProxy(t, identifier)

	resp, err := node.Connect(context.Background(), &milvuspb.ConnectRequest{
		ClientInfo: &commonpb.ClientInfo{},
	})
	require.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode(), "a refused connection must not report success")
	assert.False(t, connection.GetManager().Has(identifier),
		"a connection the extension refused must never be registered")
}

// ---------------------------------------------------------------------------
// Start and the connection registry handed to it.
// ---------------------------------------------------------------------------

func TestStartProxyExtensionHandsOverAWorkingRegistry(t *testing.T) {
	ext := installFormExtension(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startProxyExtension(ctx)

	_, _, _, starts := ext.counts()
	assert.Equal(t, 1, starts)

	ext.mu.Lock()
	conns := ext.connsSeen
	ext.mu.Unlock()
	require.NotNil(t, conns, "the extension must be handed a registry, not nil")

	const identifier = int64(556677)
	assert.False(t, conns.Connected(identifier), "an identifier that was never registered is not connected")

	connection.GetManager().Register(context.Background(), identifier, &commonpb.ClientInfo{})
	t.Cleanup(func() { connection.GetManager().Update(identifier) })
	assert.True(t, conns.Connected(identifier),
		"the registry must report the live connection, or the sweep would drop bindings of connected clients")
}

func TestProxyConnectionsReadsTheIdentifierOffTheRequest(t *testing.T) {
	conns := proxyConnections{}

	_, ok := conns.IdentifierFromContext(context.Background())
	assert.False(t, ok, "a request that never connected carries no identifier")

	ctx := metadataContextWithIdentifier(t, 4242)
	identifier, ok := conns.IdentifierFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, int64(4242), identifier,
		"the identifier must be the one the client sends back, or the connection binding is looked up under the wrong key")
}

func TestStartProxyExtensionCancelsWithTheProxyContext(t *testing.T) {
	ext := installFormExtension(t)

	ctx, cancel := context.WithCancel(context.Background())
	startProxyExtension(ctx)
	cancel()

	select {
	case <-ext.startCtxWasDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the context handed to Start must be canceled when the proxy stops, or its background work outlives the proxy")
	}
}

// ---------------------------------------------------------------------------
// The production entry points.
// ---------------------------------------------------------------------------

// TestSearchInstallsTheRewrittenRequestAndAttributesIt is the end-to-end proof
// for both halves of the mechanism, driven through Proxy.Search rather than
// through the seam helper.
//
// It fails if the entry point stops installing the cleaned parameters on the
// request (the reserved key would reach the query node), if it stops carrying
// the rewritten context forward (the extension could no longer read back what
// the request declared, and the query would be routed nowhere), or if the
// attribution site in node.search is removed (the series disappears).
func TestSearchInstallsTheRewrittenRequestAndAttributesIt(t *testing.T) {
	installFormExtension(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	defer mockey.Mock((*Proxy).handleIfSearchByPK).Return(nil, nil).Build().UnPatch()
	defer mockey.Mock((*baseTaskQueue).Enqueue).Return(nil).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &taskScheduler{dqQueue: &dqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	request := &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "coll",
		SearchParams:   kv("metric_type", "L2", testReservedParamKey, "in07-a", "nprobe", "16"),
	}
	_, err := node.Search(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, []string{"metric_type", "nprobe"}, keysOf(request.GetSearchParams()),
		"the entry point must install the cleaned parameters on the request; discarding them ships the reserved key to segcore")

	assert.Equal(t, 1, collectSeries(t, metrics.ProxyResourceGroupSQLatency),
		"the completed search must be attributed to exactly one resource group")
	h, err := metrics.ProxyResourceGroupSQLatency.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel, "db", "coll", resourceGroupFor("in07-a"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), histogramCount(t, h),
		"the search must be attributed to the resource group the form decided on, which is what routing used")
}

// TestSearchIsNotAttributedToWhatTheClientDeclared is the other half of the same
// assertion, stated so that it cannot pass by accident. The label must carry the
// form's decision, not the client's declaration: on a form where the two differ,
// labeling by the declaration would report a query against a resource group it
// never touched.
func TestSearchIsNotAttributedToWhatTheClientDeclared(t *testing.T) {
	installFormExtension(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	defer mockey.Mock((*Proxy).handleIfSearchByPK).Return(nil, nil).Build().UnPatch()
	defer mockey.Mock((*baseTaskQueue).Enqueue).Return(nil).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &taskScheduler{dqQueue: &dqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	_, err := node.Search(context.Background(), &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "coll",
		SearchParams:   kv(testReservedParamKey, "in07-a"),
	})
	require.NoError(t, err)

	h, err := metrics.ProxyResourceGroupSQLatency.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel, "db", "coll", "in07-a")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), histogramCount(t, h),
		"no series may carry what the client declared: milvus labels queries with the scope it routed them under")
}

// TestSearchIsAttributedWhenTheFormDecidesWithoutAParameter covers the route a
// request takes when it declared nothing of its own: an SDK client that named
// its form once at Connect sends no reserved parameter at all, and the search
// must still be routed and attributed. It fails if OnConnect stops recording the
// binding, if Start stops handing over the registry the lookup goes through, or
// if milvus ever starts requiring the request itself to declare something.
func TestSearchIsAttributedWhenTheFormDecidesWithoutAParameter(t *testing.T) {
	ext := installFormExtension(t)
	metrics.ProxyResourceGroupSQLatency.Reset()

	startProxyExtension(context.Background())
	require.NoError(t, onConnect(context.Background(), 31337, &commonpb.ClientInfo{
		Reserved: map[string]string{testReservedParamKey: "in07-connect"},
	}))

	defer mockey.Mock((*Proxy).handleIfSearchByPK).Return(nil, nil).Build().UnPatch()
	defer mockey.Mock((*baseTaskQueue).Enqueue).Return(nil).Build().UnPatch()
	defer mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &taskScheduler{dqQueue: &dqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	ctx := metadataContextWithIdentifier(t, 31337)
	_, err := node.Search(ctx, &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "coll",
		SearchParams:   kv("metric_type", "L2"),
	})
	require.NoError(t, err)

	h, err := metrics.ProxyResourceGroupSQLatency.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel, "db", "coll", resourceGroupFor("in07-connect"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), histogramCount(t, h),
		"a request that declared nothing must still be routed and attributed by whatever the form decided")

	_, _, ready, _ := ext.counts()
	assert.Positive(t, ready)
}

// TestHybridSearchStripsTheReservedRankParam and TestQueryStripsTheReservedParam
// drive the two remaining entry points on the cheap rejection path: an
// unhealthy proxy answers before any task is built, which is exactly what makes
// them a clean assertion that the STRIPPING happens at the entry, ahead of
// everything else the handler does.
func TestHybridSearchStripsTheReservedRankParam(t *testing.T) {
	installFormExtension(t)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	request := &milvuspb.HybridSearchRequest{
		DbName:         "db",
		CollectionName: "coll",
		RankParams:     kv("strategy", "rrf", testReservedParamKey, "in07-a"),
	}
	resp, err := node.HybridSearch(context.Background(), request)
	require.NoError(t, err)
	require.NotEqual(t, int32(0), resp.GetStatus().GetCode(), "an unhealthy proxy must still reject the request")

	assert.Equal(t, []string{"strategy"}, keysOf(request.GetRankParams()),
		"HybridSearch must install the cleaned rank params on the request")
}

func TestQueryStripsTheReservedQueryParam(t *testing.T) {
	installFormExtension(t)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	// Query counts its own rate before it looks at the state code, and the
	// collector is a package global the full proxy start-up installs.
	require.NoError(t, node.initRateCollector())

	request := &milvuspb.QueryRequest{
		DbName:         "db",
		CollectionName: "coll",
		QueryParams:    kv("limit", "10", testReservedParamKey, "in07-a"),
	}
	resp, err := node.Query(context.Background(), request)
	require.NoError(t, err)
	require.NotEqual(t, int32(0), resp.GetStatus().GetCode(), "an unhealthy proxy must still reject the request")

	assert.Equal(t, []string{"limit"}, keysOf(request.GetQueryParams()),
		"Query must install the cleaned query params on the request")
}

// TestSearchLeavesTheRequestAloneWithNoProvider is the entry-point half of the
// inertness proof: a stock binary must hand node.search the very parameters the
// client sent, reserved-looking key included.
func TestSearchLeavesTheRequestAloneWithNoProvider(t *testing.T) {
	noProviderInstalled(t)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	params := kv("metric_type", "L2", testReservedParamKey, "in07-a")
	request := &milvuspb.SearchRequest{SearchParams: params}
	_, err := node.Search(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, []string{"metric_type", testReservedParamKey}, keysOf(request.GetSearchParams()))
	assert.True(t, &params[0] == &request.SearchParams[0],
		"with no provider installed the request must still carry the caller's own slice")
}

func metadataContextWithIdentifier(t *testing.T, identifier int64) context.Context {
	t.Helper()
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(util.IdentifierKey, strconv.FormatInt(identifier, 10)))
}
