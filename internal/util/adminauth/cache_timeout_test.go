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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MixCoord's GetCredential can block in address resolution using the client's
// lifetime context. It is not sufficient to put a deadline on the RPC context:
// even callers with no deadline must leave, and stale credentials must work.
func TestCachedRootVerifier_UncooperativeCredentialLookup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		staleAge time.Duration
		password string
		want     error
	}{
		{name: "cold cache", password: testPassword, want: merr.ErrServiceUnavailable},
		{name: "expired stale cache", staleAge: staleHashTTL + time.Second, password: testPassword, want: merr.ErrServiceUnavailable},
		{name: "stale correct password", staleAge: hashTTL + time.Second, password: testPassword},
		{name: "stale wrong password", staleAge: hashTTL + time.Second, password: "wrong", want: merr.ErrPrivilegeNotAuthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			started := make(chan struct{})
			var calls atomic.Int32
			nextPassword := "rotated-password"
			nextHash := hashed(t, nextPassword)
			verifier := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
				return fakeClient(func(_ context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
					if calls.Add(1) == 1 {
						close(started)
					}
					<-release // Deliberately ignore the credential-fetch deadline.
					return credentialFor(req, nextHash), nil
				}), nil
			})
			// Release the fake RPC before Close, including on assertion failure.
			t.Cleanup(unblock)
			verifier.maxRefreshCallers = 1
			clock := newFakeClock()
			verifier.now = clock.now
			if tc.staleAge != 0 {
				verifier.storeHash(verifier.currentGeneration(), hashed(t, testPassword))
				clock.advance(tc.staleAge)
			}

			result := make(chan error, 1)
			go func() { result <- verifier.Verify(context.Background(), "root", tc.password) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("credential lookup did not start")
			}
			select {
			case err := <-result:
				if tc.want == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.want)
				}
			case <-time.After(fetchTimeout + time.Second):
				t.Fatal("credential lookup pinned the request beyond fetchTimeout")
			}

			require.EqualValues(t, 1, verifier.refreshCallers.Load(),
				"timing out must retain the singleflight result-channel accounting slot")
			for i := 0; i < 100; i++ {
				err := verifier.Verify(context.Background(), "root", tc.password)
				if tc.want == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.want)
				}
			}
			require.EqualValues(t, 1, calls.Load(), "timeouts cannot start more backend workers")
			require.EqualValues(t, 1, verifier.refreshCallers.Load())

			unblock()
			require.Eventually(t, func() bool { return verifier.refreshCallers.Load() == 0 }, time.Second, time.Millisecond)
			require.NoError(t, verifier.Verify(context.Background(), "root", nextPassword),
				"the shared result must be available after the backend recovers")
			assert.ErrorIs(t, verifier.Verify(context.Background(), "root", testPassword), merr.ErrPrivilegeNotAuthenticated,
				"a late refresh must invalidate acceptance of the stale password")
		})
	}
}
