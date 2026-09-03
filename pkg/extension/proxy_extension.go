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

package extension

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
//
// Both methods are safe for concurrent use from any goroutine, including the
// background work Start launches, which necessarily runs alongside the
// request handlers. milvus implements this interface; it may gain methods and
// never loses one (see the package evolution policy).
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
	// It goes false once the proxy drops the connection. OnDisconnect is the
	// primary way an extension learns of that; Connected is for
	// reconciliation - a sweep that double-checks bindings against the
	// registry - and for the window described on OnConnect.
	Connected(identifier int64) bool
}

// ProxyExtension is the proxy-side capability. NoopProxyExtension is the
// native default and the Noop base under the package evolution policy:
// implementations embed it, and a method is added to this interface only
// together with its inert default there.
//
// # Fall-through and short-circuit
//
// Every Intercept* method answers "fall through" with the zero value of its
// results - a nil error, or (false, nil), or (nil, nil) - and milvus then
// runs the native path exactly as if no provider were installed. Anything
// else is a short-circuit, and the shapes are chosen so that a short-circuit
// cannot be produced by accident: none of them returns a *commonpb.Status,
// because merr.Status(nil) is a non-nil success status and the natural
// `return merr.Status(check())` would have short-circuited every request the
// check passed.
//
// # Errors
//
// An error an implementation returns is reported to the client through
// merr.Status, so it must be - or wrap, with merr.Wrap/Wrapf - a sentinel
// from pkg/util/merr. Any other error collapses to UnexpectedError (code 1),
// which no SDK retries and no caller can classify. A condition the request
// itself caused (a tenant that may not call this RPC, a declaration that is
// unusable) is one of merr's input-class sentinels, or a wrap of one; a
// condition of the deployment (a cluster not up yet) is
// merr.ErrServiceUnavailable or another retriable sentinel, so the client
// retries instead of failing.
//
// # Mutation
//
// A request handed to any method here, and everything reachable from it, is
// READ-ONLY. milvus runs the native path on the same object after a
// fall-through and may log or retry it, so an implementation that changed a
// field - cleared Refresh on a load, say - would have the native path act on
// a request the client never sent, and would bypass a rule below (MUST NOT
// REPLACE A REFRESH) that is checked on the request as received. Where a
// method may hand milvus something to use instead, it says so and returns
// it; nothing is changed in place.
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
// place once it has: RewriteRequestParams is where a form takes it off.
// OnDisconnect is what stops the per-connection half from growing without
// bound.
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
// verdict. A single hook keyed by an operation name would have to say all of
// that in prose and hand back an untyped message the caller asserts on.
//
// Every one of them may replace the native outcome, which is the whole point of
// the group: on a form that decides for itself when a collection is
// serviceable, an explicit load is not work to do. What each may replace, and
// the one condition under which it must not, is on the method.
type ProxyExtension interface {
	// OnConnect runs during the Connect handshake, before the connection is
	// registered, and binds it to whatever the client declared about itself.
	// ctx is the Connect RPC's context - it carries the authenticated user,
	// the database, the peer address and the request metadata, which is what
	// a form binding the connection to one of its own tenants reads.
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
	// Connected(identifier) still answers false. A reconciliation sweep that
	// consults Connected must therefore grant a fresh binding a grace period
	// longer than a Connect round trip, or it will collect the binding this
	// very handshake just created. OnDisconnect never fires for a connection
	// OnConnect rejected.
	OnConnect(ctx context.Context, identifier int64, info *commonpb.ClientInfo) error

	// OnDisconnect runs once when the proxy drops a registered connection -
	// whether the client disconnected, the inactivity sweep collected it, or
	// the registry purged it for exceeding its size - so an implementation
	// can let go of whatever OnConnect bound. It is the only event a dropped
	// connection produces; Connected(identifier) answers false from this
	// point on.
	//
	// OBSERVE ONLY. It is called from the proxy's own goroutine - the sweep's
	// or the handler's - so it must return promptly and must not block on the
	// registry.
	OnDisconnect(identifier int64)

	// RewriteRequestParams runs at the entry of search, hybrid search and
	// query, once per parameter slice the request carries - a search's
	// SearchParams, a query's QueryParams, and for a hybrid search its
	// RankParams and each sub-request's SearchParams, since every one of
	// those reaches the query nodes. It returns the context the rest of the
	// request must run under and the parameters that must replace the ones
	// on the request. For a hybrid search the context returned for the last
	// slice is the one the request runs under; an implementation must
	// therefore bind the same value from whichever slice carries it, or bind
	// cumulatively.
	//
	// It runs BEFORE EnsureQueryReady, on the same request, so what it binds
	// onto the context is what EnsureQueryReady reads back.
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
	// An implementation must not mutate the slice or its elements: milvus may
	// still log or retry the request the caller sent. Returning the caller's
	// own context and the caller's own slice is the correct answer for an
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
	// search and query, after RewriteRequestParams and before the request is
	// turned into a task, so that a form which brings its compute up on
	// demand can do so - and can refuse the query if it cannot.
	//
	// MAY REJECT: a non-nil error rejects the query and nothing downstream
	// runs. That is the point of the method: on a form where a cluster's query
	// nodes are started only when a query arrives, letting the query through
	// unready does not degrade it, it fails it - against no nodes, or against
	// nodes holding no data. The error reaches the client through merr.Status
	// (see # Errors): a form that wants the client to wait and retry returns
	// merr.ErrServiceUnavailable or another retriable sentinel, and one that
	// is refusing the request itself returns an input-class one.
	//
	// MAY REPLACE ROUTING: the returned QueryPlacement.ResourceGroup restricts
	// which replicas may serve the query. See QueryPlacement.
	//
	// milvus passes what it knows about the query - the database and the
	// collection - plus the coordinator, and nothing else. Anything the form
	// itself needs to decide on, it put on ctx in RewriteRequestParams or
	// recorded at OnConnect, and reads back here under its own key. Whether a
	// request that told the form nothing may run is the form's decision, not
	// milvus's: only an implementation knows whether that is a control-plane
	// client to wave through or a data-plane client to refuse.
	//
	// coord is the proxy's own coordinator client, handed over rather than
	// rediscovered: milvus's service discovery is under internal/ and a form
	// outside this repository cannot reach it, so a form that had to find the
	// coordinator itself would be reimplementing it. Making a resource group
	// serviceable is a coordinator conversation - load the collection onto it,
	// then watch until its shard leaders answer - and this is what a form
	// holds that conversation with. See Coordinator.
	//
	// The returned QueryPlacement.Finish is released by milvus exactly once,
	// through QueryPlacement.Release, on every exit path of the request
	// including panics, and including the path where this method itself
	// returned an error. An implementation that releases its own state before
	// returning an error must therefore return a zero QueryPlacement rather
	// than one still carrying Finish, or the release runs twice.
	//
	// It is called on the request path and it may block - waking a cluster is
	// not instant - so it must respect the deadline on ctx.
	EnsureQueryReady(ctx context.Context, coord Coordinator, dbName, collectionName string) (QueryPlacement, error)

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
	// conns is the registry view the background work may consult; it is
	// valid until ctx is canceled and safe to use from any goroutine.
	Start(ctx context.Context, conns ProxyConnections)
}

// NoopProxyExtension is the native default: every method is inert, so a binary
// with no provider behaves exactly as the community build. Implementations
// embed it so that a method added to the interface - which under the package
// evolution policy arrives together with its inert default here - does not
// break them.
type NoopProxyExtension struct{}

var _ ProxyExtension = NoopProxyExtension{}

// The load-semantics defaults all answer "fall through": a stock binary loads,
// releases and reports on collections exactly as the community build does.

func (NoopProxyExtension) OnConnect(context.Context, int64, *commonpb.ClientInfo) error { return nil }

func (NoopProxyExtension) OnDisconnect(int64) {}

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
func (NoopProxyExtension) EnsureQueryReady(context.Context, Coordinator, string, string) (QueryPlacement, error) {
	return QueryPlacement{}, nil
}

func (NoopProxyExtension) Start(context.Context, ProxyConnections) {}
