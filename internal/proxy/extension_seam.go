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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// This file is the proxy-side seam. It declares WHERE the proxy consults the
// installed extension; the implementations live outside this tree. With no
// provider installed every seam resolves to the inert default, so a stock
// binary behaves exactly as the community build.

// proxyExtension returns the installed proxy extension, or the inert default.
func proxyExtension() extension.ProxyExtension {
	if e := extension.Caps().ProxyExt; e != nil {
		return e
	}
	return extension.NoopProxyExtension{}
}

// interceptDML consults the extension at the entry of a write path. A non-nil
// status rejects the write; short-circuiting is permitted here. Nil returns
// directly rather than through the Noop fallback, matching every other
// hot-path seam in this file: with no provider this is one pointer load and
// one nil comparison, no interface dispatch.
func interceptDML(ctx context.Context, op string, req proto.Message) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptDML(ctx, op, req)
}

// interceptAdminRPC consults the extension at the entry of the administrative
// RPCs a deployment form may withhold from tenants (credentials, RBAC,
// privilege groups, replication, flush/replica introspection). It runs before
// anything else in the handler - a withheld RPC has no health or argument
// semantics worth computing - and a non-nil status is the whole answer. The
// handler is shared by every listener; an implementation that must let its
// control plane through reads the internal-domain mark off ctx.
func interceptAdminRPC(ctx context.Context, op string) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptAdminRPC(ctx, op)
}

// The load-semantics seams below sit at the entry of the six RPCs that decide
// whether a collection is serviceable, each after its handler's own health
// check and before it builds anything. A non-nil return from one of them is the
// whole answer to the RPC and the handler returns it as it stands: it is not
// merged, rewrapped or inspected, so an extension that replaces an outcome
// replaces all of it.
//
// Each resolves extension.Caps().ProxyExt itself and returns nil on nil rather
// than falling back to the inert default. With no provider installed a seam is
// therefore one atomic load and one nil comparison - no method call, no request
// touched, no allocation - and the handler continues into the native path it
// would have run without the seam at all.

// interceptLoadCollection consults the extension before a collection is loaded.
func interceptLoadCollection(ctx context.Context, req *milvuspb.LoadCollectionRequest) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptLoadCollection(ctx, req)
}

// interceptReleaseCollection consults the extension before a collection is
// released.
func interceptReleaseCollection(ctx context.Context, req *milvuspb.ReleaseCollectionRequest) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptReleaseCollection(ctx, req)
}

// interceptLoadPartitions consults the extension before partitions are loaded.
func interceptLoadPartitions(ctx context.Context, req *milvuspb.LoadPartitionsRequest) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptLoadPartitions(ctx, req)
}

// interceptReleasePartitions consults the extension before partitions are
// released.
func interceptReleasePartitions(ctx context.Context, req *milvuspb.ReleasePartitionsRequest) *commonpb.Status {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptReleasePartitions(ctx, req)
}

// interceptGetLoadState consults the extension before the load state of a
// collection is read.
func interceptGetLoadState(ctx context.Context, req *milvuspb.GetLoadStateRequest) *milvuspb.GetLoadStateResponse {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptGetLoadState(ctx, req)
}

// interceptGetLoadingProgress consults the extension before the loading
// progress of a collection is read.
func interceptGetLoadingProgress(ctx context.Context, req *milvuspb.GetLoadingProgressRequest) *milvuspb.GetLoadingProgressResponse {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.InterceptGetLoadingProgress(ctx, req)
}

// The request-path seams below resolve extension.Caps().ProxyExt themselves and
// return early on nil rather than going through proxyExtension() above. The
// difference matters here: falling back to the inert default would still call
// a method, on every Connect and every DQL request, to be told nothing. These
// seams sit on the request path, so with no provider installed they must cost a
// nil comparison and nothing else.

// proxyConnections is the read-only view of the proxy's connection registry
// handed to a proxy extension's background work. It holds no state of its own:
// the connection manager is a process-wide singleton, so the adapter is a
// zero-sized value that forwards.
type proxyConnections struct{}

var _ extension.ProxyConnections = proxyConnections{}

func (proxyConnections) IdentifierFromContext(ctx context.Context) (int64, bool) {
	identifier, err := connection.GetIdentifierFromContext(ctx)
	if err != nil {
		// A request with no identifier is ordinary - it is what a client that
		// never called Connect sends - so the error is the answer, not a
		// failure to report.
		return 0, false
	}
	return identifier, true
}

func (proxyConnections) Connected(identifier int64) bool {
	return connection.GetManager().Has(identifier)
}

// onConnect binds a connection to whatever the client declared about itself,
// before the connection is registered. A non-nil error fails the handshake;
// short-circuiting is permitted here.
func onConnect(identifier int64, info *commonpb.ClientInfo) error {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return nil
	}
	return ext.OnConnect(identifier, info)
}

// startProxyExtension starts the extension's proxy-side background work. It is
// called once from Proxy.Start; the work stops when ctx is canceled, which
// Proxy.Stop does.
func startProxyExtension(ctx context.Context) {
	if ext := extension.Caps().ProxyExt; ext != nil {
		ext.Start(ctx, proxyConnections{})
	}
}

// rewriteRequestParams runs at the entry of a DQL handler, on the parameter
// slice that handler carries (search, query or rank params).
//
// It returns the context the rest of the request must run under and the
// parameters that must replace the ones on the request. Both returns are
// installed unconditionally, and both are load-bearing: the cleaned slice is
// what keeps a reserved parameter from reaching the query node and segcore,
// which read these as search knobs, and the context is where the extension put
// whatever it lifted off, since the parameter no longer carries it.
//
// milvus does not read what moved and cannot: it is bound under the extension's
// own key. This seam exists because the parameters are milvus's to hold, not
// because milvus has an interest in them.
//
// With no provider installed it returns its arguments untouched - the same
// context and the very same slice, not a copy - so the request is byte for byte
// what a stock milvus would have handled.
func rewriteRequestParams(ctx context.Context, params []*commonpb.KeyValuePair) (context.Context, []*commonpb.KeyValuePair) {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return ctx, params
	}
	return ext.RewriteRequestParams(ctx, params)
}

// ensureQueryReady runs at the entry of a DQL handler, after the parameters
// have been rewritten and before the request is turned into a task. The
// ordering is what lets an extension read, on this call, whatever it bound on
// the context in RewriteRequestParams.
//
// It returns the context the rest of the request must run under - carrying the
// resource group the query is to be routed to - together with the placement
// whose Release the caller must defer, and the error that rejects the query.
//
// The caller must install that defer BEFORE it examines the error:
//
//	ctx, placement, err := ensureQueryReady(...)
//	defer placement.Release()
//	if err != nil { reject }
//
// A release that only runs on the success path is the failure this ordering
// exists to prevent. An extension that took a pin and then failed later in its
// own readiness sequence hands the pin back through the placement, and a
// caller that skipped Release on the error path would leave the resource group
// looking busy forever - invisible until its query nodes stop scaling down.
//
// The health check is here rather than at the call sites because it only
// applies to the extension path. milvus's own handlers check their state code
// further in, on their own terms; what this adds is that an unhealthy proxy
// must not reach out and wake a cluster's query nodes for a query it is about
// to refuse anyway.
//
// With no provider installed it returns the caller's own context, a zero
// placement whose Release does nothing, and no error, having done one nil
// comparison.
func ensureQueryReady(ctx context.Context, node *Proxy, dbName, collectionName string) (context.Context, extension.QueryPlacement, error) {
	ext := extension.Caps().ProxyExt
	if ext == nil {
		return ctx, extension.QueryPlacement{}, nil
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return ctx, extension.QueryPlacement{}, err
	}
	placement, err := ext.EnsureQueryReady(ctx, dbName, collectionName)
	if err != nil {
		// The placement is returned even so: it may still carry a Finish the
		// caller has to release, and deciding here that a failed readiness
		// took nothing is exactly the assumption that leaks pins.
		return ctx, placement, err
	}
	if placement.ResourceGroup == "" {
		return ctx, placement, nil
	}
	return extension.WithQueryResourceGroup(ctx, placement.ResourceGroup), placement, nil
}

// observeResourceGroupSQLatency attributes one completed search or query to the
// resource group that served it. Attribution only: it is reached after the
// request is done and cannot change its outcome.
//
// It labels the query with the scope ensureQueryReady decided on, not with
// anything the client declared. The two are the same value on the form that has
// both, but only the decision is milvus's own concept and only the decision is
// what routing actually used - a query labeled with a request it did not honor
// would be a metric that agrees with the client and disagrees with the cluster.
//
// It reads the context rather than asking the extension: the scope is already
// there, bound by ensureQueryReady, and asking again would let the label and the
// routing drift apart. Nothing binds a scope in a stock binary, so this is inert
// with no provider installed without having to check for one, and
// ProxyResourceGroupSQLatency stays without a single series there.
func observeResourceGroupSQLatency(ctx context.Context, queryType, dbName, collectionName string, latencyMs int64) {
	// The provider gate saves the ctx.Value chain walk on every completed
	// search and query of a stock binary; the context read below stays the
	// authority on the label when a provider is installed.
	if extension.Caps().ProxyExt == nil {
		return
	}
	resourceGroup := extension.QueryResourceGroupFromContext(ctx)
	if resourceGroup == "" {
		return
	}
	metrics.ProxyResourceGroupSQLatency.WithLabelValues(
		paramtable.GetStringNodeID(),
		queryType,
		dbName,
		collectionName,
		resourceGroup,
	).Observe(float64(latencyMs))
}

// apiKeyVerifier returns the installed API key verifier, or nil when none is
// installed and the native token path applies.
func apiKeyVerifier() extension.APIKeyVerifier {
	return extension.Caps().APIKey
}

// ExternalListenerRequiresAPIKey reports whether the external listener must
// refuse username and password authentication. False with no verifier
// installed, which is milvus's own behavior.
func ExternalListenerRequiresAPIKey() bool {
	if v := apiKeyVerifier(); v != nil {
		return v.RequireAPIKeyOnExternalListener()
	}
	return false
}

// mixCoordAdmissionClient adapts types.MixCoordClient to extension.CoordClient.
//
// The two are not structurally interchangeable even though MixCoordClient
// embeds rootcoordpb.RootCoordClient, which declares both methods: the
// generated gRPC client stub signatures carry a trailing variadic
// ...grpc.CallOption parameter that extension.CoordClient's plain two-argument
// signature does not, so Go's structural interface check rejects passing a
// types.MixCoordClient as an extension.CoordClient directly. This adapter
// only narrows the call shape; every call is forwarded unchanged.
type mixCoordAdmissionClient struct {
	types.MixCoordClient
}

func (c mixCoordAdmissionClient) ListDatabases(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return c.MixCoordClient.ListDatabases(ctx, req)
}

func (c mixCoordAdmissionClient) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return c.MixCoordClient.ShowCollections(ctx, req)
}

// admissionChecker returns the installed admission checker, or nil when the
// native path applies and no admission work should be done at all.
func admissionChecker() extension.AdmissionChecker {
	return extension.Caps().Admission
}

// checkCreateCollectionAdmission consults the installed admission checker
// before a collection is created. With no provider installed it returns nil
// and never touches coord, so a stock binary pays no cost for the seam.
//
// Callers must run this only after establishing that the target collection
// does not already exist: re-creating an existing collection must surface
// rootcoord's own "already exists" or schema-mismatch error, not a quota
// rejection, or a retry against an instance already at its cap would see
// ResourceExhausted instead of the real (harmless) answer.
//
// This is the single production entry point for collection-creation
// admission (task.go's createCollectionTask.PreExecute calls it directly);
// keep it that way rather than resolving admissionChecker() a second time at
// the call site.
func checkCreateCollectionAdmission(ctx context.Context, coord types.MixCoordClient) error {
	c := admissionChecker()
	if c == nil {
		return nil
	}
	return c.CheckCreateCollection(ctx, mixCoordAdmissionClient{coord})
}

// The database-creation path has no equivalent checkCreateDatabaseAdmission
// wrapper. createDatabaseTask.PreExecute (task_database.go) must interleave
// its existence check between "admission is installed" and "call the
// checker" -- CheckDatabase only needs to run in the former case, and the
// wrapper shape above has no room for a caller-supplied step in the middle
// without taking a callback nobody else would need. So task_database.go
// resolves admissionChecker() itself and calls CheckCreateDatabase directly;
// see the comment there for why that is not a second consultation of
// extension.Caps() but the only one for that path.
