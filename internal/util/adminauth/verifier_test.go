// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adminauth

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// fakeMixCoord implements the two methods this package calls and embeds the
// interface for the rest, which panics if anything else is ever reached.
//
// Hand-written rather than generated, because internal/mocks pulls in a
// dependency graph that needs cgo -- and that is what kept this file, covering
// the most concurrency-dense code in the change, from running on a plain
// checkout at all.
type fakeMixCoord struct {
	types.MixCoordClient
	getCredential func(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error)
	closes        atomic.Int32
}

func (f *fakeMixCoord) GetCredential(
	ctx context.Context, req *rootcoordpb.GetCredentialRequest, _ ...grpc.CallOption,
) (*rootcoordpb.GetCredentialResponse, error) {
	return f.getCredential(ctx, req)
}

func (f *fakeMixCoord) Close() error {
	f.closes.Add(1)
	return nil
}

func fakeClient(
	fn func(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error),
) *fakeMixCoord {
	return &fakeMixCoord{getCredential: fn}
}

// credentialFor is the OK answer a healthy mix coord gives.
func credentialFor(req *rootcoordpb.GetCredentialRequest, passwordHash string) *rootcoordpb.GetCredentialResponse {
	return &rootcoordpb.GetCredentialResponse{
		Status:   merr.Success(),
		Username: req.GetUsername(),
		Password: passwordHash,
	}
}

// ctxHolder records the lifetime context handed to newClient. The constructor
// runs on the singleflight goroutine, so the test may not read it barefoot.
type ctxHolder struct {
	mu  sync.Mutex
	ctx context.Context
}

func (h *ctxHolder) set(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctx = ctx
}

func (h *ctxHolder) get() context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ctx
}

const testPassword = "correct-horse-battery-staple"

// isMismatch and isNotPermitted name the two verdicts a verifier is allowed to
// reach on its own; everything else means "could not check" and renders 503.
func isMismatch(err error) bool { return errors.Is(err, merr.ErrPrivilegeNotAuthenticated) }

func isNotPermitted(err error) bool { return errors.Is(err, merr.ErrPrivilegeNotPermitted) }

func hashed(t *testing.T, password string) string {
	t.Helper()
	// MinCost: these tests are about the cache and the limiter, not about how
	// slow bcrypt is. DefaultCost put well over a second into every comparison,
	// which both dominated the suite and turned the Eventually budgets below
	// into a race against CI machine speed.
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	assert.NoError(t, err)
	return string(h)
}

// clientReturning answers every GetCredential with the given bcrypt hash.
func clientReturning(t *testing.T, passwordHash string) types.MixCoordClient {
	t.Helper()
	return fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		return credentialFor(req, passwordHash), nil
	})
}

// fakeClock lets a test step over the cache windows without sleeping. Several
// tests below verify a failure and then a success; failureTTL means the retry
// only happens once the clock has moved past it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Now()} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestVerifierWithClock builds a verifier whose cache windows a test drives
// explicitly.
func newTestVerifierWithClock(
	t *testing.T,
	newClient func(context.Context) (types.MixCoordClient, error),
) (*RootCredentialVerifier, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	verifier := newTestVerifier(t, context.Background(), newClient)
	verifier.now = clock.now
	return verifier, clock
}

func newTestVerifier(
	t *testing.T,
	lifetimeCtx context.Context,
	newClient func(context.Context) (types.MixCoordClient, error),
) *RootCredentialVerifier {
	t.Helper()
	verifier := NewRootCredentialVerifier(lifetimeCtx, newClient)
	t.Cleanup(func() {
		assert.NoError(t, verifier.Close())
	})
	return verifier
}

func TestVerifier_AcceptsCorrectRootPassword(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, hashed(t, testPassword)), nil
	}).Verify
	assert.NoError(t, verify(context.Background(), "root", testPassword))
}

func TestVerifier_RejectsWrongPassword(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, hashed(t, testPassword)), nil
	}).Verify
	err := verify(context.Background(), "root", "not-the-password")
	assert.True(t, isMismatch(err),
		"a genuine mismatch must be reported as an authentication failure (401), not as unavailable")
}

func TestVerifier_RejectsMalformedStoredHashAsUnavailable(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, "malformed-bcrypt-hash"), nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"a corrupt stored hash must render 503, not 401")
}

func TestVerifier_RejectsNonRootWithoutDialing(t *testing.T) {
	// A non-root user must be refused before any RPC: the management plane is
	// root-only, so dialing the coord to check another user's password would be
	// wasted work and would let an unauthenticated caller drive load onto coord.
	var dials int32
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return clientReturning(t, hashed(t, testPassword)), nil
	}).Verify

	err := verify(context.Background(), "alice", testPassword)
	// 403, not 401: no password would admit this user, so the reply must not
	// invite a retry. Matches http.CheckRootAuth's verdict for the same case.
	assert.True(t, isNotPermitted(err))
	assert.False(t, isMismatch(err))
	assert.Zero(t, atomic.LoadInt32(&dials), "must not dial mix coord for a non-root user")
}

func TestVerifier_DialsLazilyAndReusesClient(t *testing.T) {
	// Nothing should be dialed at construction time — that is what keeps this
	// off every node's boot path and makes it free while adminAuthEnabled is
	// false. Once dialed, the client is reused.
	var dials int32
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return clientReturning(t, hashed(t, testPassword)), nil
	}).Verify
	assert.Zero(t, atomic.LoadInt32(&dials), "constructing the verifier must not dial")

	assert.NoError(t, verify(context.Background(), "root", testPassword))
	assert.NoError(t, verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(1), atomic.LoadInt32(&dials), "client should be dialed once and reused")
}

func TestVerifier_FailedDialIsNotCached(t *testing.T) {
	// A coord that is unreachable while a node boots must not permanently
	// disable management access on that node, so a failed dial is retried.
	var dials int32
	verifier, clock := newTestVerifierWithClock(t, func(context.Context) (types.MixCoordClient, error) {
		if atomic.AddInt32(&dials, 1) == 1 {
			return nil, errors.New("coord unreachable")
		}
		return clientReturning(t, hashed(t, testPassword)), nil
	})
	verify := verifier.Verify

	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"an unreachable coord must not be reported as a bad password")

	// Immediately retrying is answered from the negative cache without
	// dialing again: that is what keeps a sick coord from being used as an
	// amplifier by an unauthenticated caller.
	assert.Error(t, verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(1), atomic.LoadInt32(&dials), "retry must be suppressed within failureTTL")

	clock.advance(failureTTL + time.Second)
	assert.NoError(t, verify(context.Background(), "root", testPassword),
		"a later attempt must succeed once the coord is reachable")
	assert.Equal(t, int32(2), atomic.LoadInt32(&dials))
}

func TestVerifier_NilClientIsNotCached(t *testing.T) {
	var dials int32
	verifier, clock := newTestVerifierWithClock(t, func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return nil, nil
	})
	verify := verifier.Verify

	assert.Error(t, verify(context.Background(), "root", testPassword))
	clock.advance(failureTTL + time.Second)
	assert.Error(t, verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), atomic.LoadInt32(&dials),
		"a nil client must be treated as a failed construction and retried")
}

func TestVerifier_RejectsOnRPCError(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return fakeClient(func(context.Context, *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
			return nil, errors.New("rpc failed")
		}), nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"an RPC failure must render 503, not 401")
}

func TestVerifier_RejectsNilResponse(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return fakeClient(func(context.Context, *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
			return nil, nil
		}), nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"an empty RPC response is a credential-store failure, not a bad password")
}

func TestVerifier_RejectsOnErrorStatus(t *testing.T) {
	// A non-OK Status must fail closed. Without checking it, a response whose
	// Password field is empty would be compared against an empty hash.
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return fakeClient(func(context.Context, *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
			return &rootcoordpb.GetCredentialResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "boom"},
			}, nil
		}), nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"a non-OK status means the credential could not be checked -> 503")
}

func TestVerifier_RejectsEmptyStoredHash(t *testing.T) {
	// Defense in depth against an OK response carrying no credential: bcrypt
	// must never be asked to treat "" as a valid hash for "".
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, ""), nil
	}).Verify
	assert.Error(t, verify(context.Background(), "root", ""))
	assert.Error(t, verify(context.Background(), "root", testPassword))
}

func TestVerifier_RPCFailureDoesNotCancelClientContext(t *testing.T) {
	// mix.NewClient does not dial immediately. The first actual RPC may fail
	// while MixCoord is unavailable, but the cached client must retain a live
	// service-discovery context so its next RPC can reconnect after recovery.
	var creates int32
	var calls int32
	var clientCtx ctxHolder
	passwordHash := hashed(t, testPassword)

	cli := fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		if err := clientCtx.get().Err(); err != nil {
			return nil, err
		}
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, errors.New("mix coord unavailable")
		}
		return credentialFor(req, passwordHash), nil
	})

	verifier, clock := newTestVerifierWithClock(t, func(ctx context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&creates, 1)
		clientCtx.set(ctx)
		return cli, nil
	})

	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err))
	assert.NoError(t, clientCtx.get().Err(), "finishing one HTTP request must not cancel the cached client")
	clock.advance(failureTTL + time.Second)
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword),
		"the cached client must recover once MixCoord is reachable")
	assert.Equal(t, int32(1), atomic.LoadInt32(&creates))
}

func TestVerifier_CanceledRequestDoesNotPoisonClient(t *testing.T) {
	var (
		calls             atomic.Int32
		fetchCtxCancelled atomic.Bool
		firstFetchStarted = make(chan struct{})
		firstFetchDone    = make(chan struct{})
		clientCtx         ctxHolder
	)
	passwordHash := hashed(t, testPassword)

	cli := fakeClient(func(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		if calls.Add(1) == 1 {
			close(firstFetchStarted)
			fetchCtxCancelled.Store(ctx.Err() != nil)
			close(firstFetchDone)
			return nil, context.Canceled
		}
		return credentialFor(req, passwordHash), nil
	})

	verifier, clock := newTestVerifierWithClock(t, func(ctx context.Context) (types.MixCoordClient, error) {
		clientCtx.set(ctx)
		return cli, nil
	})

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	assert.Error(t, verifier.Verify(requestCtx, "root", testPassword))

	// Verify may have returned the moment the caller's context was done, while
	// the shared lookup was still running. Join it before reading what it saw,
	// or this asserts on a zero value and passes whatever the code does.
	<-firstFetchDone

	// The lookup is shared: whoever arrives first performs it for everyone, so
	// it must not inherit the first caller's cancellation. Otherwise one client
	// hanging up aborts a lookup other callers are waiting on and writes a
	// context.Canceled into the negative cache that is then served to them.
	assert.False(t, fetchCtxCancelled.Load(),
		"the shared lookup must not observe the caller's cancellation")
	assert.NoError(t, clientCtx.get().Err(), "request cancellation must not cancel the cached client")

	// Step past the negative cache and check that an uncancelled caller gets a
	// real answer from the same client. The clock is advanced inside the loop
	// on purpose: storeFailure runs on the shared lookup's goroutine and stamps
	// failureExpiry from whatever the fake clock reads then, so advancing once,
	// beforehand, can be overwritten by a stamp that never expires.
	assert.Eventually(t, func() bool {
		clock.advance(failureTTL + time.Second)
		return verifier.Verify(context.Background(), "root", testPassword) == nil
	}, 5*time.Second, 5*time.Millisecond,
		"the cached client must serve the next caller once the negative cache expires")
	assert.Zero(t, cli.closes.Load(), "a canceled request must not close the cached client")
}

// A caller whose own deadline expires must not be pinned to a shared lookup for
// the whole of fetchTimeout: on a node that has lost its coordinator that is
// every gated request, and an HTTP client that hung up should stop occupying a
// goroutine here.
func TestVerifier_CallerEscapesASlowSharedLookup(t *testing.T) {
	releaseFetch := make(chan struct{})
	defer close(releaseFetch)
	fetchStarted := make(chan struct{})
	var startOnce sync.Once

	verifier := NewCachedRootVerifier(func(ctx context.Context) (string, error) {
		// Once: a caller that gave up on the shared lookup can leave the flight
		// and a later one can start a second, and closing an already-closed
		// channel would take the whole test binary down rather than fail a test.
		startOnce.Do(func() { close(fetchStarted) })
		<-releaseFetch
		return "", errors.New("coordinator is gone")
	})

	go func() {
		_ = verifier.Verify(context.Background(), "root", testPassword)
	}()
	<-fetchStarted

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- verifier.Verify(requestCtx, "root", testPassword) }()

	select {
	case err := <-done:
		assert.Error(t, err)
		assert.False(t, isMismatch(err),
			"giving up on a slow lookup is not a wrong password")
	case <-time.After(5 * time.Second):
		t.Fatal("a caller whose context is done stayed pinned to the shared lookup")
	}
}

// singleflight collapses backend work but keeps one result channel per caller.
// Bound the callers as well, or an unavailable coordinator lets anonymous
// traffic retain an unbounded number of handler goroutines and channels for the
// whole lookup timeout.
func TestCachedRootVerifier_ShedsSlowLookupCallers(t *testing.T) {
	var (
		lookups     atomic.Int32
		startOnce   sync.Once
		releaseOnce sync.Once
	)
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	release := func() { releaseOnce.Do(func() { close(releaseFetch) }) }
	t.Cleanup(release)

	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		lookups.Add(1)
		startOnce.Do(func() { close(fetchStarted) })
		<-releaseFetch
		return "", errors.New("credential store unreachable")
	})
	verifier.maxRefreshCallers = 2

	joined := make(chan error, verifier.maxRefreshCallers)
	go func() { joined <- verifier.Verify(context.Background(), "root", testPassword) }()
	<-fetchStarted
	go func() { joined <- verifier.Verify(context.Background(), "root", testPassword) }()
	require.Eventually(t, func() bool {
		return verifier.refreshCallers.Load() == verifier.maxRefreshCallers
	}, time.Second, time.Millisecond, "callers never filled the credential lookup bound")

	// The next caller must get a fast 503 rather than attach another result
	// channel to the blocked flight.
	shed := make(chan error, 1)
	go func() { shed <- verifier.Verify(context.Background(), "root", testPassword) }()
	select {
	case err := <-shed:
		require.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable))
		assert.Equal(t, errCredentialLookupSaturated().Error(), err.Error())
	case <-time.After(time.Second):
		t.Fatal("a caller past the credential lookup bound was queued")
	}

	release()
	for i := int32(0); i < verifier.maxRefreshCallers; i++ {
		assert.Error(t, <-joined)
	}
	assert.Eventually(t, func() bool {
		return verifier.refreshCallers.Load() == 0
	}, time.Second, time.Millisecond, "credential lookup caller slots were not released")
	assert.Equal(t, int32(1), lookups.Load(), "the burst must still share one backend lookup")
}

// DoChan retains a canceled caller's result channel until the shared lookup
// completes. Its caller slot must live equally long; releasing it when the HTTP
// request leaves would let a stream of short-lived connections bypass the
// bound while their channels remain retained inside singleflight.
func TestCachedRootVerifier_CanceledCallerKeepsSlotUntilLookupFinishes(t *testing.T) {
	var releaseOnce sync.Once
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	release := func() { releaseOnce.Do(func() { close(releaseFetch) }) }
	t.Cleanup(release)

	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		close(fetchStarted)
		<-releaseFetch
		return "", errors.New("credential store unreachable")
	})
	verifier.maxRefreshCallers = 1

	requestCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- verifier.Verify(requestCtx, "root", testPassword) }()
	<-fetchStarted
	cancel()
	require.Error(t, <-done)
	assert.Equal(t, int32(1), verifier.refreshCallers.Load(),
		"the canceled caller's result channel is still retained by singleflight")

	shed := make(chan error, 1)
	go func() { shed <- verifier.Verify(context.Background(), "root", testPassword) }()
	select {
	case err := <-shed:
		require.Error(t, err)
		assert.Equal(t, errCredentialLookupSaturated().Error(), err.Error())
	case <-time.After(time.Second):
		t.Fatal("a canceled caller released its slot before the shared lookup finished")
	}

	release()
	assert.Eventually(t, func() bool {
		return verifier.refreshCallers.Load() == 0
	}, time.Second, time.Millisecond, "the canceled caller's slot was not released after lookup")
}

// mix.NewClient reaches sessionutil, which panics rather than erroring when the
// process-wide etcd client cannot be built. It runs on its own goroutine, where
// net/http's per-connection recover cannot see it, so createClient must contain
// the panic itself or one bad request takes the process down.
func TestVerifier_ClientConstructorPanicBecomesAnError(t *testing.T) {
	var attempts atomic.Int32
	verifier, clock := newTestVerifierWithClock(t, func(context.Context) (types.MixCoordClient, error) {
		attempts.Add(1)
		panic("etcd client unavailable")
	})

	err := verifier.Verify(context.Background(), "root", testPassword)
	require.Error(t, err)
	assert.False(t, isMismatch(err),
		"a constructor failure is not a wrong password")

	// And it is not sticky: the next attempt past the negative cache retries.
	clock.advance(failureTTL + time.Second)
	assert.Error(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), attempts.Load())
}

func TestVerifier_CachesRootHashWithinTTL(t *testing.T) {
	// The gated endpoints answer unauthenticated callers too, so without a cache
	// every probe against a worker becomes a GetCredential RPC to the mix coord.
	var calls int32
	passwordHash := hashed(t, testPassword)
	cli := fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		atomic.AddInt32(&calls, 1)
		return credentialFor(req, passwordHash), nil
	})

	clock := newFakeClock()
	verifier := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return cli, nil
	})
	verifier.now = clock.now

	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	// A wrong password is still rejected locally, and still without an RPC.
	assert.True(t, isMismatch(
		verifier.Verify(context.Background(), "root", "wrong")))
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "cached hash must serve repeat checks")

	clock.advance(hashTTL + time.Second)
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "an expired hash must be refetched")
}

// A worker that cannot reach the coordinator must still be stoppable: losing
// the coordinator is a routine step of a rolling upgrade, and answering 503 to
// /management/stop there turns a graceful drain into a SIGKILL.
func TestVerifier_UsesStaleHashWhenCoordUnreachable(t *testing.T) {
	var calls int32
	passwordHash := hashed(t, testPassword)
	cli := fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return credentialFor(req, passwordHash), nil
		}
		return nil, errors.New("mix coord is down")
	})

	clock := newFakeClock()
	verifier := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return cli, nil
	})
	verifier.now = clock.now

	// Warm the cache while coord is healthy.
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))

	// Past the freshness window, coord is gone. The refresh fails, and the
	// stale hash keeps both verdicts intact — right password in, wrong
	// password out — rather than collapsing everything into 503.
	clock.advance(hashTTL + time.Second)
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.True(t, isMismatch(
		verifier.Verify(context.Background(), "root", "wrong")),
		"a stale hash must still reject a wrong password")

	// Past the staleness bound the node fails closed again, so a rotated root
	// password cannot be honored indefinitely by a partitioned worker.
	clock.advance(staleHashTTL)
	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"an unreachable store is 503, not a wrong password")
}

// A worker that has never reached the coordinator has nothing to fall back to
// and must say "cannot check", not "wrong password".
func TestVerifier_NoStaleHashWithoutASuccessfulFetch(t *testing.T) {
	cli := fakeClient(func(context.Context, *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		return nil, errors.New("mix coord is down")
	})

	verifier := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return cli, nil
	})

	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err))
}

// The management endpoints answer unauthenticated callers, so a caller who
// guesses the username "root" must not be able to turn each request into a
// credential lookup. On the coordinator that lookup is a metastore read, which
// is why this cache is shared rather than living only in the worker verifier.
func TestCachedRootVerifier_OneLookupServesABurst(t *testing.T) {
	var lookups int32
	passwordHash := hashed(t, testPassword)
	clock := newFakeClock()
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		atomic.AddInt32(&lookups, 1)
		return passwordHash, nil
	})
	verifier.now = clock.now

	// Wrong passwords: exactly what an unauthenticated caller sends. Each is
	// rejected — either as a mismatch, or shed by the comparison limiter under
	// the concurrency, which is why this asserts the lookup count rather than
	// the status of any individual caller.
	const attackers = 32
	var wg sync.WaitGroup
	for i := 0; i < attackers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Error(t, verifier.Verify(context.Background(), "root", "guess"))
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), atomic.LoadInt32(&lookups),
		"a burst must collapse to one credential lookup")

	clock.advance(hashTTL + time.Second)
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), atomic.LoadInt32(&lookups), "an expired hash must be refetched")
}

// Same property for the failing case, which is the one that matters most: a
// coordinator that is already struggling must not be handed one lookup per
// request by anyone who can reach the port.
func TestCachedRootVerifier_FailingLookupsAreRateLimited(t *testing.T) {
	var lookups int32
	clock := newFakeClock()
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		atomic.AddInt32(&lookups, 1)
		return "", errors.New("credential store unreachable")
	})
	verifier.now = clock.now

	for i := 0; i < 10; i++ {
		err := verifier.Verify(context.Background(), "root", "guess")
		assert.Error(t, err)
		assert.False(t, isMismatch(err))
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&lookups))

	clock.advance(failureTTL + time.Second)
	assert.Error(t, verifier.Verify(context.Background(), "root", "guess"))
	assert.Equal(t, int32(2), atomic.LoadInt32(&lookups))
}

func TestCachedRootVerifier_RejectsOverlongPasswordBeforeLookup(t *testing.T) {
	var lookups atomic.Int32
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		lookups.Add(1)
		return hashed(t, testPassword), nil
	})

	err := verifier.Verify(context.Background(), "root",
		strings.Repeat("x", bcryptMaxPasswordBytes+1))
	assert.True(t, isMismatch(err))
	assert.Zero(t, lookups.Load(),
		"a password bcrypt cannot represent must not drive a credential lookup")
}

// bcrypt is ~60ms of CPU, and the web console makes a dozen gated calls per
// refresh. Repeating an already-verified password must not repeat that cost.
func TestCachedRootVerifier_RepeatedCorrectPasswordSkipsBcrypt(t *testing.T) {
	clock := newFakeClock()
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		return hashed(t, testPassword), nil
	})
	verifier.now = clock.now

	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))

	// Swap in a valid hash of a different password, without disturbing the
	// short-circuit: if the second check still ran bcrypt it would now reject.
	verifier.mu.Lock()
	verifier.hash = hashed(t, "a-completely-different-password")
	verifier.mu.Unlock()
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword),
		"a repeat of a verified password must be answered from the short-circuit")

	// A different password must not benefit from it.
	err := verifier.Verify(context.Background(), "root", "some-other-password")
	assert.Error(t, err)
}

// A rotation must invalidate the short-circuit, or the old password would keep
// working past the cache windows that are supposed to bound it.
func TestCachedRootVerifier_RotationClearsTheShortCircuit(t *testing.T) {
	const newPassword = "rotated-password"
	current := hashed(t, testPassword)
	clock := newFakeClock()
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		return current, nil
	})
	verifier.now = clock.now

	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))

	current = hashed(t, newPassword)
	clock.advance(hashTTL + time.Second)

	assert.True(t, isMismatch(
		verifier.Verify(context.Background(), "root", testPassword)),
		"the old password must stop working once the new hash is fetched")
	assert.NoError(t, verifier.Verify(context.Background(), "root", newPassword))
}

// A saturated comparison queue must shed rather than grow. An unbounded queue
// would be served FIFO, putting the operator's own /management/stop behind an
// attacker's entire backlog — the availability staleHashTTL exists to protect,
// lost on the other side of the same code path.
func TestComparisonLimiter_ShedsInsteadOfQueueing(t *testing.T) {
	// waitTimeout is deliberately long: the waiters have to stay queued for the
	// whole test so the queue is full at the moment the shed is measured. They
	// leave through their context instead of by timing out, which is what makes
	// this deterministic rather than a race against a timer.
	limiter := &comparisonLimiter{
		slots:       syncutil.NewSemaphore(1),
		maxWaiting:  2,
		waitTimeout: time.Minute,
	}

	queuedCtx, releaseQueued := context.WithCancel(context.Background())
	release := make(chan struct{})
	occupied := make(chan struct{})
	// Unblock in the reverse order of acquisition — defers run LIFO, so the
	// waiters are released first and the slot only frees once none of them can
	// still take it. Otherwise a waiter could run its function after the test
	// has returned, and anything it reported would panic the test binary.
	defer close(release)
	defer releaseQueued()

	go func() {
		_ = limiter.do(context.Background(), func() error {
			close(occupied)
			<-release
			return nil
		})
	}()
	<-occupied

	for i := int32(0); i < limiter.maxWaiting; i++ {
		go func() { _ = limiter.do(queuedCtx, func() error { return nil }) }()
	}
	assert.Eventually(t, func() bool {
		return limiter.waiting.Load() == limiter.maxWaiting
	}, time.Second, time.Millisecond, "waiters never filled the queue")

	// Slot held, queue full: this must be refused at once rather than joining
	// the backlog. That is the whole point — an operator gets a fast 503 they
	// can retry instead of a wait they cannot outlast.
	ran := false
	start := time.Now()
	err := limiter.do(context.Background(), func() error {
		ran = true
		return nil
	})
	elapsed := time.Since(start)

	assert.False(t, ran, "a shed caller must not run the comparison")
	assert.ErrorIs(t, err, errComparisonSaturated())
	assert.Equal(t, errComparisonSaturated().Error(), err.Error(),
		"shedding must say it is saturation, not some other unavailable dependency")
	assert.Less(t, elapsed, limiter.waitTimeout/10, "a full queue must shed, not enqueue")
	assert.False(t, isMismatch(err),
		"shedding must not be reported as a bad password")
}

// The production limiter is sized from GOMAXPROCS at first use. What matters is
// the shape of the bound, not the arithmetic: traffic up to the concurrency
// limit runs, and traffic past limit+queue sheds. Recomputing the formula here
// would pin nothing -- changing /4 to /16 would keep such a test green.
func TestComparisonLimiterBoundsConcurrencyAndShedsPastTheQueue(t *testing.T) {
	limiter := &comparisonLimiter{
		slots:       syncutil.NewSemaphore(2),
		maxWaiting:  2,
		waitTimeout: time.Minute,
	}

	// Fill both slots and hold them.
	release := make(chan struct{})
	defer close(release)
	holding := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_ = limiter.do(context.Background(), func() error {
				holding <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	for i := 0; i < 2; i++ {
		<-holding
	}

	// Fill the queue. These leave through their context, not through the
	// timeout, so the shed below is measured against a queue that is provably
	// full rather than against a timer.
	queuedCtx, releaseQueued := context.WithCancel(context.Background())
	defer releaseQueued()
	for i := int32(0); i < limiter.maxWaiting; i++ {
		go func() { _ = limiter.do(queuedCtx, func() error { return nil }) }()
	}
	assert.Eventually(t, func() bool {
		return limiter.waiting.Load() == limiter.maxWaiting
	}, time.Second, time.Millisecond, "waiters never filled the queue")

	ran := false
	start := time.Now()
	err := limiter.do(context.Background(), func() error {
		ran = true
		return nil
	})
	elapsed := time.Since(start)

	assert.False(t, ran, "a shed caller must not run the comparison")
	assert.ErrorIs(t, err, errComparisonSaturated())
	assert.Equal(t, errComparisonSaturated().Error(), err.Error(),
		"shedding must say it is saturation, not some other unavailable dependency")
	assert.Less(t, elapsed, limiter.waitTimeout/10, "a full queue must shed, not enqueue")
	assert.False(t, isMismatch(err), "shedding must not be reported as a bad password")
}

// Traffic within the production limiter's concurrency bound must not shed --
// that is what keeps the console's opening burst from looking like a broken
// gate. Sized from the limiter itself rather than from a fixed number: on a
// two-CPU pod the bound is 2 running plus 8 queued, so a burst larger than that
// genuinely can shed, and asserting otherwise would be asserting something
// false. Concurrent, because a serial loop can never fail a TryAcquire and so
// says nothing about the bound.
func TestProductionComparisonLimiterAdmitsTrafficWithinItsBound(t *testing.T) {
	limiter := passwordComparisons()
	slots := int(limiter.maxWaiting / 4) // maxWaiting is 4*slots by construction
	require.GreaterOrEqual(t, slots, 2, "the floor keeps a small pod usable")

	var wg sync.WaitGroup
	var shed atomic.Int32
	for i := 0; i < slots; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := limiter.do(context.Background(), func() error { return nil }); err != nil {
				shed.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Zero(t, shed.Load(), "traffic within the concurrency bound must not shed")
}

func TestCachedRootVerifier_ForgetDropsTheHash(t *testing.T) {
	var lookups int32
	passwordHash := hashed(t, testPassword)
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		atomic.AddInt32(&lookups, 1)
		return passwordHash, nil
	})

	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	verifier.Forget()
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), atomic.LoadInt32(&lookups),
		"a stopped component must not keep answering from a hash it dropped")
}

func TestVerifier_CloseReleasesClientAndStopsNewChecks(t *testing.T) {
	var creates int32
	var clientCtx ctxHolder
	passwordHash := hashed(t, testPassword)
	cli := fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
		return credentialFor(req, passwordHash), nil
	})

	verifier := newTestVerifier(t, context.Background(), func(ctx context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&creates, 1)
		clientCtx.set(ctx)
		return cli, nil
	})
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.NoError(t, verifier.Close())
	assert.ErrorIs(t, clientCtx.get().Err(), context.Canceled)
	assert.Equal(t, int32(1), cli.closes.Load(), "Close must release the cached client exactly once")

	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&creates), "a closed verifier must not recreate its client")
	assert.NoError(t, verifier.Close(), "Close should be idempotent")
}

// The short-circuit is what keeps the console affordable, so the guard that
// binds it to one specific stored hash is a security property, not a detail:
// without it a password verified against the pre-rotation hash would keep
// opening the gate against the post-rotation one. storeHash makes the
// production state unreachable, so exercise the guard directly.
func TestCachedRootVerifier_ShortCircuitIsBoundToTheStoredHash(t *testing.T) {
	before, after := hashed(t, testPassword), hashed(t, "rotated")
	sha := crypto.SHA256(testPassword, util.UserRoot)

	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		return before, nil
	})
	verifier.storeHash(verifier.currentGeneration(), before)
	verifier.rememberVerified(before, sha)

	assert.True(t, verifier.matchesVerified(before, sha))
	assert.False(t, verifier.matchesVerified(after, sha),
		"a password verified against one hash must not be honored against another")

	verifier.storeHash(verifier.currentGeneration(), after)
	assert.False(t, verifier.matchesVerified(after, sha),
		"a refresh that changes the hash must drop the short-circuit")

	// And a comparison racing that refresh must not re-arm it against the hash
	// it no longer holds.
	verifier.rememberVerified(before, sha)
	assert.False(t, verifier.matchesVerified(before, sha))
}

// These three windows are the contract the config documentation states: how
// long a rotated root password takes to take effect, how long a node that has
// lost its coordinator keeps answering, and how often a failing node retries.
// The tests above drive the fake clock by these symbols, so without this they
// would all stay green if the values changed.
func TestCacheWindowsMatchTheDocumentedContract(t *testing.T) {
	assert.Equal(t, 10*time.Second, hashTTL)
	assert.Equal(t, 10*time.Minute, staleHashTTL)
	assert.Equal(t, 2*time.Second, failureTTL)
	assert.Equal(t, 5*time.Second, fetchTimeout)
}

// Shedding has to reach the caller as 503, not as a bad password: an operator
// told their password is wrong while the node is merely busy goes looking for a
// credential problem that does not exist.
func TestSaturationRendersAsServiceUnavailable(t *testing.T) {
	assert.False(t, isMismatch(errComparisonSaturated()))
	assert.False(t, isNotPermitted(errComparisonSaturated()))
	assert.True(t, errors.Is(errComparisonSaturated(), merr.ErrServiceUnavailable),
		"shedding must reach the boundary as an unavailable dependency, which renders 503")
}

// The comparison is the one place a wrong password and a corrupt credential
// store look alike, and telling them apart is what keeps an operator holding
// the right password from being sent to look for a wrong one.
func TestVerifyStoredPassword(t *testing.T) {
	hash := hashed(t, testPassword)
	maxLengthPassword := strings.Repeat("x", bcryptMaxPasswordBytes)

	assert.NoError(t, VerifyStoredPassword(hash, testPassword))
	assert.NoError(t, VerifyStoredPassword(hashed(t, maxLengthPassword), maxLengthPassword))
	assert.True(t, isMismatch(VerifyStoredPassword(hash, "wrong-password")))
	assert.True(t, isMismatch(VerifyStoredPassword(hash,
		strings.Repeat("x", bcryptMaxPasswordBytes+1))))

	err := VerifyStoredPassword("malformed-bcrypt-hash", testPassword)
	assert.Error(t, err)
	assert.False(t, isMismatch(err),
		"a corrupt stored hash is a credential-store failure, not a bad password")
	assert.False(t, isNotPermitted(err))
}
