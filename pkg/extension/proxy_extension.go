package extension

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// ProxyConnections is the slice of the proxy's connection registry a
// ProxyExtension may consult. The proxy hands it to Start and it stays valid
// until the context Start was given is canceled.
//
// It is an interface rather than the connection manager itself for the reason
// every other capability parameter is: pkg/v3 is a separate Go module and must
// not import internal/, and an extension that could reach the whole manager
// could also register, purge and rewrite connections milvus owns. What it can
// do here is read: turn a request into the connection it arrived on, and ask
// whether a connection it remembers is still there.
type ProxyConnections interface {
	// IdentifierFromContext returns the identifier of the connection the
	// request on ctx was sent over - the same value the Connect handshake
	// returned to that client and passed to OnConnect - and false when the
	// request carries none. A request with no identifier is ordinary: it is
	// what a client that never called Connect sends.
	//
	// TRUST: the identifier is read off a client-controlled header and is not
	// authenticated - any client can send any value. It is a ROUTING key (a
	// wrong value routes the request to the wrong binding, which is the
	// sender's own problem), never an authorization boundary: nothing that
	// grants access may be keyed on it alone.
	IdentifierFromContext(ctx context.Context) (int64, bool)

	// Connected reports whether a connection identifier is still registered.
	// It goes false once the proxy drops the connection, including when the
	// client simply stopped talking and the inactivity sweep collected it, so
	// an extension holding per-connection state can let go of it.
	Connected(identifier int64) bool
}

// ProxyExtension is the proxy-side capability. Every method is consulted by a
// seam in the proxy package; NoopProxyExtension is the native default.
//
// Short-circuit contract: a method may replace the native outcome ONLY where
// this doc comment says so. A method with no such note must not be used to
// alter control flow.
//
// The interface carries exactly what the proxy wires today, and a method is
// added here in the same change as the seam that consults it - never earlier.
// A capability method with no call site is worse than an absent one: an
// implementation can fill it, compile, pass its own tests and ship, believing
// a behavior is in effect that nothing ever asks for. An absent method fails
// to compile instead, which is the honest answer.
//
// # Request annotation
//
// Three of the methods below are one mechanism, not three independent hooks. A
// deployment form may need to know something about a request that milvus has no
// concept of - which of the form's own clusters it is for, say - and clients
// say it in whichever place suits them: once at Connect for an SDK session
// (OnConnect), per RPC for a gateway multiplexing many clients over one
// connection, or inside the DQL parameters for a request that arrived over
// REST. Only the last of those needs milvus's help, because a DQL parameter
// cannot be seen before the request reaches its handler and must not be left in
// place once it has: RewriteRequestParams is where a form takes it off. Start is
// what stops the per-connection half from growing without bound.
//
// milvus never learns what any of it means. Whatever a form takes off a request
// it carries on the context under its own key and reads back itself, so the
// vocabulary stays where it belongs.
//
// # Load semantics
//
// Six of the methods below are the load-semantics group: LoadCollection,
// ReleaseCollection, LoadPartitions, ReleasePartitions, GetLoadState and
// GetLoadingProgress, each consulted at the entry of the RPC it is named after.
// They are six methods rather than one hook because the contract differs per
// RPC - a refresh must reach querycoord, a release must not be confused with a
// load - and because two of them answer with a response message rather than a
// status. A single hook keyed by an operation name would have to say all of
// that in prose and hand back an untyped message the caller asserts on.
//
// Every one of them may replace the native outcome, which is the whole point of
// the group: on a form that decides for itself when a collection is
// serviceable, an explicit load is not work to do. What each may replace, and
// the one condition under which it must not, is on the method.
type ProxyExtension interface {
	// InterceptDML may short-circuit: a non-nil status rejects the write before
	// it reaches the write path. op is the operation name - one of "Insert",
	// "Delete", "Upsert", "Flush", "FlushAll" and "Import" (which ImportV2
	// funnels through; those are the six write paths wired today). ctx carries
	// the caller's deadline and any request-scoped values. req is the DML
	// request being considered.
	//
	// A short-circuited request returns before the handler's own metrics,
	// rate-limit accounting and trace span - the same place milvus's own
	// checkExternalCollectionBlockedForWrite rejects from - so refused writes
	// appear in the gRPC-layer access log and stats, not in ProxyFunctionCall
	// or the NQ counters.
	InterceptDML(ctx context.Context, op string, req proto.Message) *commonpb.Status

	// InterceptAdminRPC may short-circuit the administrative RPCs a deployment
	// form withholds from its tenants. op is the RPC name; a non-nil status is
	// the whole answer to the RPC. The wired table, kept in step with the
	// impl.go call sites: GetReplicas, GetFlushState, GetFlushAllState;
	// Create/Update/DeleteCredential, ListCredUsers; Create/Drop/AlterRole,
	// OperateUserRole, SelectRole, SelectUser, OperatePrivilege(V2),
	// SelectGrant, Backup/RestoreRBAC; Create/Drop/List/OperatePrivilegeGroup;
	// ReplicateMessage, Update/GetReplicateConfiguration, GetReplicateInfo,
	// CreateReplicateStream.
	//
	// The seam runs in the handler, which every listener shares, so an
	// implementation that withholds an RPC from tenants while its control
	// plane still manages accounts distinguishes the callers by provenance:
	// ctx carries FromInternalDomain for requests that arrived on an
	// internal-domain listener, and those are the control plane's.
	InterceptAdminRPC(ctx context.Context, op string) *commonpb.Status

	// InterceptLoadCollection is consulted at the entry of LoadCollection,
	// after the proxy's health check and before the load task is built.
	//
	// MAY REPLACE: a non-nil status is the whole answer to the RPC. No task is
	// built, querycoord never hears of the request, and the collection is left
	// exactly as it was. A form that decides for itself when a collection
	// becomes serviceable - one that loads it on the first query that needs it -
	// has nothing for an explicit load to do, and letting the native load run as
	// well would place replicas it did not ask for and does not track.
	//
	// MUST NOT REPLACE A REFRESH: a request with Refresh set is not a load.
	// querycoord answers it from a branch of its own that re-pulls the target of
	// a collection which must ALREADY be loaded, returning CollectionNotLoaded
	// when it is not. That is meaningful whatever a form does with ordinary
	// loads, because the data behind a collection can change under it, and it is
	// the only way a client can ask for the re-read. Replacing it reports
	// success for work nothing did. Return nil for it.
	//
	// A nil status falls through to the native load, unchanged.
	InterceptLoadCollection(ctx context.Context, req *milvuspb.LoadCollectionRequest) *commonpb.Status

	// InterceptReleaseCollection is consulted at the entry of ReleaseCollection,
	// after the proxy's health check and before the release task is built.
	//
	// MAY REPLACE: a non-nil status is the whole answer and nothing is released.
	// A form that reclaims replicas on a schedule of its own - an idle timeout,
	// or the retirement of whatever it loaded them for - would otherwise have an
	// explicit release take away replicas its own bookkeeping still believes in,
	// and on such a form the client is not the owner of that decision.
	//
	// A nil status falls through to the native release, unchanged.
	InterceptReleaseCollection(ctx context.Context, req *milvuspb.ReleaseCollectionRequest) *commonpb.Status

	// InterceptLoadPartitions is consulted at the entry of LoadPartitions, after
	// the proxy's health check and before the load task is built.
	//
	// MAY REPLACE: as InterceptLoadCollection, at partition granularity.
	//
	// MUST NOT REPLACE A REFRESH: as InterceptLoadCollection. The two RPCs carry
	// the same refresh mode and querycoord answers both from the same re-pull,
	// so a form that lets one through and swallows the other has no contract at
	// all - it has whichever of the two its clients happened to call.
	//
	// A nil status falls through to the native load, unchanged.
	InterceptLoadPartitions(ctx context.Context, req *milvuspb.LoadPartitionsRequest) *commonpb.Status

	// InterceptReleasePartitions is consulted at the entry of ReleasePartitions,
	// after the proxy's health check and before the release task is built.
	//
	// MAY REPLACE: as InterceptReleaseCollection, at partition granularity.
	//
	// A nil status falls through to the native release, unchanged.
	InterceptReleasePartitions(ctx context.Context, req *milvuspb.ReleasePartitionsRequest) *commonpb.Status

	// InterceptGetLoadState is consulted at the entry of GetLoadState, after the
	// proxy's health check and before the collection is looked up.
	//
	// MAY REPLACE: a non-nil response is returned to the client as the whole
	// answer, including its status, and milvus reads nothing out of it. A form
	// that admits a query by making its collection serviceable on the way in has
	// no half-loaded state a client could act on: by the time a query can
	// observe the collection it is loaded, and the native answer would describe
	// replicas that form manages on its own schedule.
	//
	// An implementation that replaces this owns the whole response. A status it
	// leaves unset is the zero status, which is success - so a form that means
	// to report a failure must say so in the status it returns.
	//
	// A nil response falls through to the native lookup, unchanged.
	InterceptGetLoadState(ctx context.Context, req *milvuspb.GetLoadStateRequest) *milvuspb.GetLoadStateResponse

	// InterceptGetLoadingProgress is consulted at the entry of
	// GetLoadingProgress, after the proxy's health check and before the
	// collection is looked up.
	//
	// MAY REPLACE: as InterceptGetLoadState, and with the same ownership of the
	// whole response.
	//
	// The response carries two numbers, and a form that replaces it answers for
	// both: RefreshProgress reports how far along the re-pull a Refresh asked
	// for has got, and a canned response reporting only Progress leaves it at
	// zero - which a client waiting on a refresh reads as "not started".
	//
	// A nil response falls through to the native lookup, unchanged.
	InterceptGetLoadingProgress(ctx context.Context, req *milvuspb.GetLoadingProgressRequest) *milvuspb.GetLoadingProgressResponse

	// OnConnect runs during the Connect handshake, before the connection is
	// registered, and binds it to whatever the client declared about itself.
	// identifier is the value Connect is about to return to the client and
	// that later requests carry back; info is the client info as sent, and may
	// be nil.
	//
	// MAY REJECT: a non-nil error fails the handshake and the connection is
	// never registered, so a client that declared something unusable is told
	// so at Connect rather than at its first query. A client that declared
	// nothing is not unusual - it is what every control-plane-only client
	// looks like - so returning an error for a missing declaration would
	// refuse connections milvus itself has no problem with.
	//
	// ORDERING: this runs BEFORE the connection is registered (a rejected
	// handshake must not leave a registered connection nothing will ever
	// collect), so there is a window in which the binding exists while
	// Connected(identifier) still answers false. A sweeper that collects
	// bindings on Connected()==false must therefore grant a fresh binding a
	// grace period longer than a Connect round trip, or it will collect the
	// binding this very handshake just created.
	OnConnect(identifier int64, info *commonpb.ClientInfo) error

	// RewriteRequestParams runs at the entry of every search, hybrid search and
	// query, on the parameter slice that handler carries (search, query or rank
	// params). It returns the context the rest of the request must run under and
	// the parameters that must replace the ones on the request.
	//
	// MAY REPLACE BOTH: milvus installs both returns unconditionally, not only
	// when the implementation found something to take. That is not a caller
	// convention that can be forgotten - it is the point of the method. A
	// reserved parameter is a private protocol between a distribution and its
	// own clients; every other component down the line, query node and segcore
	// included, receives these parameters as search knobs and has no idea what
	// to do with one. A cleaned slice the caller discarded would leave it on the
	// request.
	//
	// An implementation must not mutate the slice it is given: milvus may still
	// log or retry the request the caller sent. Returning the caller's own
	// context and the caller's own slice is the correct answer for an
	// implementation with nothing to take, and the only one that costs a stock
	// request nothing.
	//
	// milvus does not look at what moved, and there is nowhere for it to look:
	// whatever the implementation lifted off the parameters it binds onto the
	// returned context under a key of its own, and it is the one that reads it
	// back - see EnsureQueryReady. The value belongs to the form's vocabulary,
	// not to milvus's, and a round trip through milvus would put the word into
	// milvus without milvus ever using it.
	//
	// It is called on the request path, so it must be cheap and must not do I/O.
	RewriteRequestParams(ctx context.Context, params []*commonpb.KeyValuePair) (context.Context, []*commonpb.KeyValuePair)

	// EnsureQueryReady is consulted at the entry of every search, hybrid
	// search and query, before the request is turned into a task, so that a
	// form which brings its compute up on demand can do so - and can refuse
	// the query if it cannot.
	//
	// MAY REJECT: a non-nil error rejects the query and nothing downstream
	// runs. That is the point of the method: on a form where a cluster's query
	// nodes are started only when a query arrives, letting the query through
	// unready does not degrade it, it fails it - against no nodes, or against
	// nodes holding no data. milvus does not interpret the error; it is
	// returned to the client as the reason its query was refused, so an
	// implementation that wants the client to retry must say so in the error
	// it returns.
	//
	// MAY REPLACE ROUTING: the returned QueryPlacement.ResourceGroup restricts
	// which replicas may serve the query. See QueryPlacement.
	//
	// milvus passes what it knows about the query - the database and the
	// collection - and nothing else. Anything the form itself needs to decide
	// on, it put on ctx in RewriteRequestParams or recorded at OnConnect, and
	// reads back here under its own key. Whether a request that told the form
	// nothing may run is the form's decision, not milvus's: only an
	// implementation knows whether that is a control-plane client to wave
	// through or a data-plane client to refuse.
	//
	// The returned QueryPlacement.Finish is released by milvus exactly once,
	// on every exit path of the request including panics, and including the
	// path where this method itself returned an error. An implementation that
	// releases its own state before returning an error must therefore return a
	// zero QueryPlacement rather than one still carrying Finish, or the
	// release runs twice.
	//
	// It is called on the request path and it may block - waking a cluster is
	// not instant - so it must respect the deadline on ctx.
	EnsureQueryReady(ctx context.Context, dbName, collectionName string) (QueryPlacement, error)

	// Start runs the extension's proxy-side background work. It is called once
	// while the proxy starts, must return promptly rather than blocking, and
	// whatever it started must stop when ctx is canceled. Proxy shutdown
	// cancels ctx but does NOT wait for that work to finish - there is no
	// join - so the work must be safe to abandon mid-step: nothing that
	// corrupts state when the process exits while it runs.
	//
	// OBSERVE ONLY: it cannot fail the proxy's start-up and cannot change what
	// any request does.
	//
	// conns is how the background work learns that connections went away:
	// per-connection state recorded by OnConnect is otherwise kept for
	// identifiers that will never be seen again, since a dropped connection
	// produces no event of its own.
	Start(ctx context.Context, conns ProxyConnections)
}

// NoopProxyExtension is the native default: every method is inert, so a binary
// with no provider behaves exactly as the community build. Implementations
// should embed it so that a method added to the interface does not break them.
type NoopProxyExtension struct{}

var _ ProxyExtension = NoopProxyExtension{}

func (NoopProxyExtension) InterceptDML(context.Context, string, proto.Message) *commonpb.Status {
	return nil
}

func (NoopProxyExtension) InterceptAdminRPC(context.Context, string) *commonpb.Status {
	return nil
}

// The load-semantics defaults all answer nil, which is "fall through": a stock
// binary loads, releases and reports on collections exactly as the community
// build does. nil is the inert answer here precisely because the alternative is
// so easy to write - a success status looks harmless and would turn every load
// in a stock binary into a no-op that reported success.

func (NoopProxyExtension) InterceptLoadCollection(context.Context, *milvuspb.LoadCollectionRequest) *commonpb.Status {
	return nil
}

func (NoopProxyExtension) InterceptReleaseCollection(context.Context, *milvuspb.ReleaseCollectionRequest) *commonpb.Status {
	return nil
}

func (NoopProxyExtension) InterceptLoadPartitions(context.Context, *milvuspb.LoadPartitionsRequest) *commonpb.Status {
	return nil
}

func (NoopProxyExtension) InterceptReleasePartitions(context.Context, *milvuspb.ReleasePartitionsRequest) *commonpb.Status {
	return nil
}

func (NoopProxyExtension) InterceptGetLoadState(context.Context, *milvuspb.GetLoadStateRequest) *milvuspb.GetLoadStateResponse {
	return nil
}

func (NoopProxyExtension) InterceptGetLoadingProgress(context.Context, *milvuspb.GetLoadingProgressRequest) *milvuspb.GetLoadingProgressResponse {
	return nil
}

func (NoopProxyExtension) OnConnect(int64, *commonpb.ClientInfo) error { return nil }

// RewriteRequestParams returns its arguments untouched: the caller's own
// context and the caller's own slice. Both halves are the inert answer to a
// question that is easy to answer wrongly - the caller installs what comes back,
// so a fresh empty slice would drop the request's search parameters on the floor
// and a derived context would allocate on every DQL in a stock binary.
func (NoopProxyExtension) RewriteRequestParams(ctx context.Context, params []*commonpb.KeyValuePair) (context.Context, []*commonpb.KeyValuePair) {
	return ctx, params
}

// EnsureQueryReady admits the query and scopes it to nothing. Both halves are
// load-bearing: an inert default that returned an error would refuse every
// search in a stock binary, and one that named a resource group would restrict
// routing milvus is meant to leave alone.
func (NoopProxyExtension) EnsureQueryReady(context.Context, string, string) (QueryPlacement, error) {
	return QueryPlacement{}, nil
}

func (NoopProxyExtension) Start(context.Context, ProxyConnections) {}
