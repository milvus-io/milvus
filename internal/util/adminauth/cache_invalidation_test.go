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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCachedRootVerifier_ForgetRevokesSharedRefreshResult(t *testing.T) {
	oldHash, newHash := hashed(t, testPassword), hashed(t, "rotated")
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var lookups atomic.Int32
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		if lookups.Add(1) == 1 {
			close(started)
			<-release
			return oldHash, nil
		}
		return newHash, nil
	})
	leader, follower := make(chan error, 1), make(chan error, 1)
	go func() { leader <- verifier.Verify(context.Background(), util.UserRoot, testPassword) }()
	<-started
	verifier.Forget()
	// This request begins after the notification, but joins a lookup that
	// already read the previous credential. Rejecting only cache writeback
	// would still authenticate this request with the obsolete result.
	go func() { follower <- verifier.Verify(context.Background(), util.UserRoot, testPassword) }()
	require.Eventually(t, func() bool { return verifier.refreshCallers.Load() == 2 }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, lookups.Load(), "revocation must not spawn overlapping backend work")
	unblock()
	require.ErrorIs(t, <-leader, merr.ErrServiceUnavailable)
	// If the follower is scheduled after the first lookup finishes, it can
	// read the new hash and return a mismatch instead of unavailability.
	err := <-follower
	require.Error(t, err)
	require.True(t, isMismatch(err) || errors.Is(err, merr.ErrServiceUnavailable))
	require.NoError(t, verifier.Verify(context.Background(), util.UserRoot, "rotated"))
	require.ErrorIs(t, verifier.Verify(context.Background(), util.UserRoot, testPassword), merr.ErrPrivilegeNotAuthenticated)
	require.EqualValues(t, 2, lookups.Load())
}

func TestCachedRootVerifier_ForgetRevokesStaleAndVerifiedPassword(t *testing.T) {
	hash := hashed(t, testPassword)
	var fail atomic.Bool
	var lookups atomic.Int32
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		lookups.Add(1)
		if fail.Load() {
			return "", merr.WrapErrServiceUnavailable("credential owner unavailable")
		}
		return hash, nil
	})
	now := time.Now()
	verifier.now = func() time.Time { return now }
	require.NoError(t, verifier.Verify(context.Background(), util.UserRoot, testPassword))
	fail.Store(true)
	now = now.Add(hashTTL + time.Second)
	require.NoError(t, verifier.Verify(context.Background(), util.UserRoot, testPassword), "outage fallback before notification")
	verifier.Forget()
	require.ErrorIs(t, verifier.Verify(context.Background(), util.UserRoot, testPassword), merr.ErrServiceUnavailable)
	require.EqualValues(t, 3, lookups.Load(), "notification also clears the previous negative cache")
}

func TestCachedRootVerifier_ForgetDuringPasswordComparison(t *testing.T) {
	oldHash, newHash := hashed(t, testPassword), hashed(t, "rotated")
	var rotated atomic.Bool
	verifier := NewCachedRootVerifier(func(context.Context) (string, error) {
		if rotated.Load() {
			return newHash, nil
		}
		return oldHash, nil
	})
	// Hold comparison capacity so Forget deterministically runs after the
	// credential is fetched but before bcrypt completes.
	limiter := passwordComparisons()
	held := 0
	for limiter.slots.TryAcquire() {
		held++
	}
	release := func() {
		for held > 0 {
			limiter.slots.Release()
			held--
		}
	}
	defer release()
	result := make(chan error, 1)
	go func() { result <- verifier.Verify(context.Background(), util.UserRoot, testPassword) }()
	require.Eventually(t, func() bool { return limiter.waiting.Load() == 1 }, time.Second, time.Millisecond)
	rotated.Store(true)
	verifier.Forget()
	release()
	require.ErrorIs(t, <-result, merr.ErrServiceUnavailable)
	require.NoError(t, verifier.Verify(context.Background(), util.UserRoot, "rotated"))
	require.ErrorIs(t, verifier.Verify(context.Background(), util.UserRoot, testPassword), merr.ErrPrivilegeNotAuthenticated)
}
