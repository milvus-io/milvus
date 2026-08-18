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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	// fetchTimeout bounds a credential lookup, whose own retry policy would
	// otherwise run for ~53s on everyone's behalf.
	fetchTimeout = 5 * time.Second

	// hashTTL bounds how long a fetched root hash is reused, and with it how
	// long a rotated root password takes to take effect here.
	hashTTL = 10 * time.Second

	// staleHashTTL bounds how long a hash that can no longer be refreshed is
	// still accepted, so that losing the coordinator does not also lose the
	// ability to drain a node.
	staleHashTTL = 10 * time.Minute

	// failureTTL bounds retries when a lookup is failing with nothing cached to
	// fall back on, so an anonymous caller cannot use a sick coordinator as an
	// amplifier.
	failureTTL = 2 * time.Second
)

// comparisonLimiter bounds the cost of password comparison. Three bounds, not
// one: a bare semaphore has an unbounded queue, so an attacker who keeps it
// full puts the operator's own /management/stop behind their whole backlog.
type comparisonLimiter struct {
	slots       *syncutil.Semaphore
	waiting     atomic.Int32
	maxWaiting  int32
	waitTimeout time.Duration
}

// Sized lazily: GOMAXPROCS is set from the cgroup by hardware.InitMaxprocs,
// which runs in main, so a package-level var would size itself from the host's
// core count.
//
// The floor is the uncomfortable part, and it is a real cost of enabling the
// gate: on a two-CPU pod two concurrent comparisons are *all* of the CPU, so an
// unauthenticated caller sending garbage passwords at /management/stop -- a
// route every node publishes -- can take a large share of that node away from
// the data path. Two is nonetheless the floor, because one would make the
// console unusable on small nodes. This bounds CPU, it does not rate-limit
// attempts; keep the port off untrusted networks regardless.
var passwordComparisons = sync.OnceValue(func() *comparisonLimiter {
	slots := max(2, runtime.GOMAXPROCS(0)/4)
	return &comparisonLimiter{
		slots:       syncutil.NewSemaphore(slots),
		maxWaiting:  int32(4 * slots),
		waitTimeout: 500 * time.Millisecond,
	}
})

// errComparisonSaturated says the node shed the request rather than checking
// the credential, so an on-call engineer is not sent to check etcd while the
// actual cause is load.
//
// Built at the return site rather than parked in a package variable: errors.Is
// on a merr error compares codes, not identity, so a shared sentinel would
// match every other ErrServiceUnavailable -- and a package-level one would
// freeze a stack trace pointing at init instead of at the shed request.
func errComparisonSaturated() error {
	return merr.WrapErrServiceUnavailable(
		"credential verification is saturated on this node; retry")
}

// do runs fn while holding a comparison slot, or returns
// errComparisonSaturated without running it.
func (l *comparisonLimiter) do(ctx context.Context, fn func() error) error {
	if l.slots.TryAcquire() {
		defer l.slots.Release()
		return fn()
	}

	if l.waiting.Add(1) > l.maxWaiting {
		l.waiting.Add(-1)
		return errComparisonSaturated()
	}

	waitCtx, cancel := context.WithTimeout(ctx, l.waitTimeout)
	err := l.slots.Acquire(waitCtx)
	cancel()
	// Decremented as soon as the wait is over, not when fn returns: counting a
	// goroutine that already holds a slot as a waiter would shrink the queue by
	// however long bcrypt takes.
	l.waiting.Add(-1)
	if err != nil {
		return errComparisonSaturated()
	}
	defer l.slots.Release()
	return fn()
}

func compareBounded(ctx context.Context, storedHash, password string) error {
	return passwordComparisons().do(ctx, func() error {
		return VerifyStoredPassword(storedHash, password)
	})
}

// VerifyStoredPassword compares a plaintext password with a stored bcrypt hash.
// Only ErrMismatchedHashAndPassword means the password is wrong; a malformed
// hash is a credential-store failure and must not be reported as a bad
// password, or an operator holding the right one goes looking for the wrong
// problem.
func VerifyStoredPassword(storedHash, password string) error {
	err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
		return merr.WrapErrPrivilegeNotAuthenticated("invalid root password")
	default:
		return merr.WrapErrServiceInternalErr(err, "stored root credential hash is invalid")
	}
}

// CachedRootVerifier answers root credential checks from a cached bcrypt hash,
// refreshing it through fetch. Proxy, coordinator and worker all use it.
type CachedRootVerifier struct {
	fetch func(ctx context.Context) (string, error)
	now   func() time.Time

	// fetches collapses a burst of concurrent misses into one lookup.
	//
	// x/sync directly rather than conc.Singleflight: conc's DoChan starts a
	// goroutine per caller, so on a node that has lost its coordinator every
	// anonymous request would park one for the whole of fetchTimeout. This one
	// runs a goroutine only for the leader.
	fetches singleflight.Group

	mu   sync.Mutex
	hash string
	// verifiedSha is the salted SHA of the password that last matched hash, so
	// a repeated correct password costs a lookup instead of another bcrypt.
	// Dropped whenever hash changes, so a rotation cannot leave a stale
	// password accepted. Same trade privilege.privilegeCache already makes.
	verifiedSha     string
	hashExpiry      time.Time
	hashStaleExpiry time.Time
	lastFailure     error
	failureExpiry   time.Time
	// generation increments on Forget so a lookup already in flight cannot
	// repopulate the cache of a component that has stopped.
	generation uint64
}

// NewCachedRootVerifier wraps fetch, which must return root's stored bcrypt
// hash or an error meaning "could not look it up".
func NewCachedRootVerifier(fetch func(ctx context.Context) (string, error)) *CachedRootVerifier {
	return &CachedRootVerifier{fetch: fetch, now: time.Now}
}

// Verify checks a root credential. It returns nil on a match,
// merr.ErrPrivilegeNotAuthenticated for a mismatch,
// merr.ErrPrivilegeNotPermitted for a non-root user, and any other error when
// the credential could not be checked -- the distinction the management-plane
// boundary renders as 401, 403 and 503.
func (c *CachedRootVerifier) Verify(ctx context.Context, username, password string) error {
	if username != util.UserRoot {
		// 403, not 401: no password admits this caller, so the reply must not
		// invite a retry, and rejecting before any lookup keeps an anonymous
		// caller from driving lookups with invented usernames.
		return merr.WrapErrPrivilegeNotPermitted("only root user can access this endpoint")
	}

	if hash := c.freshHash(); hash != "" {
		return c.compare(ctx, hash, password)
	}

	hash, err := c.refresh(ctx)
	if err != nil {
		// Losing the credential owner must not also lose the ability to stop
		// this node; see staleHashTTL.
		if stale := c.staleHash(); stale != "" {
			// Rated: during a coordinator restart this is every gated request
			// on every node, for the whole staleHashTTL window.
			mlog.RatedWarn(ctx, 1.0, "verifying root credential against a stale cached hash",
				mlog.String("reason", "credential lookup failed"), mlog.Err(err))
			return c.compare(ctx, stale, password)
		}
		return err
	}
	return c.compare(ctx, hash, password)
}

// compare checks password against storedHash, skipping bcrypt when this exact
// password has already been shown to match this exact hash.
func (c *CachedRootVerifier) compare(ctx context.Context, storedHash, password string) error {
	sha := crypto.SHA256(password, util.UserRoot)
	if c.matchesVerified(storedHash, sha) {
		return nil
	}
	if err := compareBounded(ctx, storedHash, password); err != nil {
		return err
	}
	c.rememberVerified(storedHash, sha)
	return nil
}

func (c *CachedRootVerifier) matchesVerified(storedHash, sha string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.verifiedSha != "" && c.hash == storedHash && c.verifiedSha == sha
}

func (c *CachedRootVerifier) rememberVerified(storedHash, sha string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Only if the hash is still the one that was matched: a refresh may have
	// replaced it while bcrypt was running.
	if c.hash == storedHash {
		c.verifiedSha = sha
	}
}

// refresh returns a fresh hash, doing at most one lookup across concurrent
// callers and at most one lookup per failureTTL while lookups keep failing.
func (c *CachedRootVerifier) refresh(ctx context.Context) (string, error) {
	// Check the negative cache before queueing: a caller that is going to be
	// answered from it has no reason to wait behind someone else's lookup.
	if err := c.cachedFailure(); err != nil {
		return "", err
	}

	// DoChan rather than Do: the shared lookup runs to completion for whoever
	// needs it, but a caller whose own deadline expires -- or whose client hung
	// up -- must not be pinned to it for the whole of fetchTimeout.
	result := c.fetches.DoChan("root", func() (any, error) {
		// Another caller may have refreshed while we queued.
		if hash := c.freshHash(); hash != "" {
			return hash, nil
		}
		if err := c.cachedFailure(); err != nil {
			return "", err
		}

		gen := c.currentGeneration()
		// Detached from the caller's cancellation, but with a deadline: the
		// lookup is shared, so letting one client hang up abort it -- and
		// record a context.Canceled served to everyone else for failureTTL --
		// would let an attacker keep a node answering 503 to /management/stop.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		hash, err := c.fetch(fetchCtx)
		if err != nil {
			c.storeFailure(gen, err)
			return "", err
		}
		c.storeHash(gen, hash)
		return hash, nil
	})
	select {
	case r := <-result:
		if r.Err != nil {
			return "", r.Err
		}
		hash, _ := r.Val.(string)
		return hash, nil
	case <-ctx.Done():
		return "", merr.WrapErrServiceUnavailable(
			"credential lookup did not complete before the request deadline")
	}
}

func (c *CachedRootVerifier) currentGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *CachedRootVerifier) freshHash() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hash == "" || c.now().After(c.hashExpiry) {
		return ""
	}
	return c.hash
}

// staleHash returns the last known root hash past its freshness window but
// within staleHashTTL. It is used only after a refresh has already failed.
func (c *CachedRootVerifier) staleHash() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hash == "" || c.now().After(c.hashStaleExpiry) {
		return ""
	}
	return c.hash
}

func (c *CachedRootVerifier) cachedFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastFailure == nil || c.now().After(c.failureExpiry) {
		return nil
	}
	return c.lastFailure
}

func (c *CachedRootVerifier) storeHash(gen uint64, hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != gen {
		return
	}
	now := c.now()
	if c.hash != hash {
		// A rotation invalidates the short-circuit; the new password must go
		// through bcrypt once before it is trusted again.
		c.verifiedSha = ""
	}
	c.hash = hash
	c.hashExpiry = now.Add(hashTTL)
	c.hashStaleExpiry = now.Add(staleHashTTL)
	c.lastFailure = nil
	c.failureExpiry = time.Time{}
}

func (c *CachedRootVerifier) storeFailure(gen uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != gen {
		return
	}
	c.lastFailure = err
	c.failureExpiry = c.now().Add(failureTTL)
}

// Forget drops everything cached. Call it on shutdown so a stopped component
// stops holding a credential hash it no longer has any business holding.
func (c *CachedRootVerifier) Forget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	c.hash = ""
	c.verifiedSha = ""
	c.hashExpiry = time.Time{}
	c.hashStaleExpiry = time.Time{}
	c.lastFailure = nil
	c.failureExpiry = time.Time{}
}
