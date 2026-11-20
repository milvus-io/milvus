//go:build test

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sourcecontext

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/milvus-io/milvus/pkg/v3/sourceauth"
	"github.com/stretchr/testify/require"
)

const testRoot = "abfss://data@milvustest.dfs.core.windows.net/table"

var testRegistry sourceauth.Registry
var installTestRuntime sync.Once
var installTestError error

type nativeSession struct {
	fetch  func(context.Context, sourceauth.FetchRequest) sourceauth.Credential
	closed atomic.Int32
}

func (*nativeSession) AccountName() string       { return "milvustest" }
func (*nativeSession) AllowedRootURIs() []string { return []string{testRoot} }
func (s *nativeSession) Fetch(ctx context.Context, req sourceauth.FetchRequest) sourceauth.Credential {
	return s.fetch(ctx, req)
}
func (s *nativeSession) Close() { s.closed.Add(1) }

func nativeBinding(t *testing.T, session *nativeSession) string {
	t.Helper()
	installTestRuntime.Do(func() {
		installTestError = sourceauth.Install(&testRegistry, func(ctx context.Context, _ any, _ string) (context.Context, error) { return ctx, nil })
	})
	require.NoError(t, installTestError)
	name := "test-" + uuid.NewString()
	require.NoError(t, testRegistry.Register(name, func(context.Context, sourceauth.Binding) (sourceauth.Session, error) { return session, nil }))
	ctx := sourceauth.WithAuthorizer(context.Background(), func(context.Context, sourceauth.Operation) (sourceauth.Reference, error) {
		return sourceauth.Reference{Provider: name, Value: "owner"}, nil
	})
	spec, err := sourceauth.Prepare(ctx, sourceauth.Operation{Source: testRoot, Spec: `{"format":"lance-table","extfs":{"cloud_provider":"azure","access_key_id":"milvustest"}}`}, true)
	require.NoError(t, err)
	return spec
}
func validNativeCredential() sourceauth.Credential {
	return nativeCredentialUntil(time.Now().UTC().Add(time.Minute).Truncate(time.Second))
}

func nativeCredentialUntil(expiry time.Time) sourceauth.Credential {
	sas := url.Values{"sv": {"2023-11-03"}, "spr": {"https"}, "sp": {"rl"}, "sr": {"d"}, "sdd": {"1"}, "se": {expiry.Format(time.RFC3339)}, "sig": {"test-only"}}
	return sourceauth.Credential{Status: sourceauth.CredentialOK, AccountName: "milvustest", ScopeRootURI: testRoot, SASToken: sas.Encode(), ExpiresAt: expiry}
}

func TestNativeExecutionDeadlineDoesNotCapSignedLease(t *testing.T) {
	expiry := time.Now().UTC().Add(12 * time.Hour).Truncate(time.Second)
	session := &nativeSession{fetch: func(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
		return nativeCredentialUntil(expiry)
	}}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()
	native, err := Open(ctx, testRoot, nativeBinding(t, session))
	require.NoError(t, err)
	defer native.Close()
	require.Equal(t, sourceauth.CredentialOK, nativeTestFetch(native.Handle(), testRoot+"/data.bin"))
	cancel()
	// Verify cancellation in native state, independently of a later Go callback.
	require.Eventually(t, func() bool {
		return nativeTestCheckState(native.Handle()) == sourceauth.CredentialCancelled
	}, 5*time.Second, time.Millisecond)
}

func TestNativeExplicitAuthorityDeadlineStillBoundsSignedLease(t *testing.T) {
	session := &nativeSession{fetch: func(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
		return nativeCredentialUntil(time.Now().UTC().Add(12 * time.Hour).Truncate(time.Second))
	}}
	native, err := openNative(context.Background(), testRoot, nativeBinding(t, session), time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	defer nativeTestRelease(uintptr(native))
	require.Equal(t, sourceauth.CredentialInvalidResponse, nativeTestFetch(uintptr(native), testRoot+"/data.bin"))
}

func TestNativeRetainsGoProviderAndSeparatesOwners(t *testing.T) {
	var calls atomic.Int32
	alice := &nativeSession{fetch: func(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
		calls.Add(1)
		return validNativeCredential()
	}}
	bob := &nativeSession{fetch: func(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
		return sourceauth.Credential{Status: sourceauth.CredentialDenied}
	}}
	first, err := openNative(context.Background(), testRoot, nativeBinding(t, alice), 0)
	require.NoError(t, err)
	retained, ok := nativeTestRetain(uintptr(first))
	require.True(t, ok)
	nativeTestRelease(uintptr(first))
	require.Zero(t, alice.closed.Load())
	require.Equal(t, sourceauth.CredentialOK, nativeTestFetch(retained, testRoot+"/data.bin"))
	second, err := openNative(context.Background(), testRoot, nativeBinding(t, bob), 0)
	require.NoError(t, err)
	require.Equal(t, sourceauth.CredentialDenied, nativeTestFetch(uintptr(second), testRoot+"/data.bin"))
	require.Equal(t, sourceauth.CredentialOK, nativeTestFetch(retained, testRoot+"/data.bin"))
	require.EqualValues(t, 2, calls.Load())
	nativeTestRelease(retained)
	nativeTestRelease(uintptr(second))
	require.EqualValues(t, 1, alice.closed.Load())
	require.EqualValues(t, 1, bob.closed.Load())
}

func TestNativeCancellationDrainsInflightCallback(t *testing.T) {
	started := make(chan struct{})
	session := &nativeSession{fetch: func(ctx context.Context, _ sourceauth.FetchRequest) sourceauth.Credential {
		close(started)
		<-ctx.Done()
		return sourceauth.Credential{Status: sourceauth.CredentialCancelled}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	native, err := Open(ctx, testRoot, nativeBinding(t, session))
	require.NoError(t, err)
	done := make(chan sourceauth.CredentialStatus, 1)
	go func() { done <- nativeTestFetch(native.Handle(), testRoot+"/data.bin") }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not start")
	}
	cancel()
	select {
	case status := <-done:
		require.Equal(t, sourceauth.CredentialCancelled, status)
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not reach callback")
	}
	native.Close()
	require.EqualValues(t, 1, session.closed.Load())
}

func TestNativeProviderPanicAndTransientRecovery(t *testing.T) {
	var attempt atomic.Int32
	session := &nativeSession{fetch: func(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
		switch attempt.Add(1) {
		case 1:
			panic("secret must not escape")
		case 2:
			return sourceauth.Credential{Status: sourceauth.CredentialTransient}
		default:
			return validNativeCredential()
		}
	}}
	native, err := Open(context.Background(), testRoot, nativeBinding(t, session))
	require.NoError(t, err)
	defer native.Close()
	require.Equal(t, sourceauth.CredentialInvalidResponse, nativeTestFetch(native.Handle(), testRoot+"/data.bin"))
	require.Equal(t, sourceauth.CredentialTransient, nativeTestFetch(native.Handle(), testRoot+"/data.bin"))
	require.Equal(t, sourceauth.CredentialOK, nativeTestFetch(native.Handle(), testRoot+"/data.bin"))
}
