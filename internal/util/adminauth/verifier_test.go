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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const testPassword = "correct-horse-battery-staple"

func hashed(t *testing.T, password string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	assert.NoError(t, err)
	return string(h)
}

// clientReturning builds a mock whose GetCredential answers with the given
// bcrypt hash and an OK status.
func clientReturning(t *testing.T, passwordHash string) types.MixCoordClient {
	t.Helper()
	cli := mocks.NewMockMixCoordClient(t)
	cli.EXPECT().Close().Return(nil).Maybe()
	cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *rootcoordpb.GetCredentialRequest, _ ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
			return &rootcoordpb.GetCredentialResponse{
				Status:   merr.Success(),
				Username: req.GetUsername(),
				Password: passwordHash,
			}, nil
		}).Maybe()
	return cli
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
	assert.True(t, internalhttp.IsAuthenticationError(err),
		"a genuine mismatch must be reported as an authentication failure (401), not as unavailable")
}

func TestVerifier_RejectsMalformedStoredHashAsUnavailable(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, "malformed-bcrypt-hash"), nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
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

	assert.True(t, internalhttp.IsAuthenticationError(verify(context.Background(), "alice", testPassword)))
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
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		if atomic.AddInt32(&dials, 1) == 1 {
			return nil, errors.New("coord unreachable")
		}
		return clientReturning(t, hashed(t, testPassword)), nil
	}).Verify

	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"an unreachable coord must not be reported as a bad password")
	assert.NoError(t, verify(context.Background(), "root", testPassword),
		"a later attempt must succeed once the coord is reachable")
	assert.Equal(t, int32(2), atomic.LoadInt32(&dials))
}

func TestVerifier_NilClientIsNotCached(t *testing.T) {
	var dials int32
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return nil, nil
	}).Verify

	assert.Error(t, verify(context.Background(), "root", testPassword))
	assert.Error(t, verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(2), atomic.LoadInt32(&dials),
		"a nil client must be treated as a failed construction and retried")
}

func TestVerifier_RejectsOnRPCError(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		cli := mocks.NewMockMixCoordClient(t)
		cli.EXPECT().Close().Return(nil).Maybe()
		cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
			Return(nil, errors.New("rpc failed")).Maybe()
		return cli, nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"an RPC failure must render 503, not 401")
}

func TestVerifier_RejectsNilResponse(t *testing.T) {
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		cli := mocks.NewMockMixCoordClient(t)
		cli.EXPECT().Close().Return(nil).Maybe()
		cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
			Return(nil, nil).Maybe()
		return cli, nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"an empty RPC response is a credential-store failure, not a bad password")
}

func TestVerifier_RejectsOnErrorStatus(t *testing.T) {
	// A non-OK Status must fail closed. Without checking it, a response whose
	// Password field is empty would be compared against an empty hash.
	verify := newTestVerifier(t, context.Background(), func(context.Context) (types.MixCoordClient, error) {
		cli := mocks.NewMockMixCoordClient(t)
		cli.EXPECT().Close().Return(nil).Maybe()
		cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
			Return(&rootcoordpb.GetCredentialResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "boom"},
			}, nil).Maybe()
		return cli, nil
	}).Verify
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
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
	var clientCtx context.Context
	passwordHash := hashed(t, testPassword)

	cli := mocks.NewMockMixCoordClient(t)
	cli.EXPECT().Close().Return(nil).Maybe()
	cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *rootcoordpb.GetCredentialRequest, _ ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
			if err := clientCtx.Err(); err != nil {
				return nil, err
			}
			if atomic.AddInt32(&calls, 1) == 1 {
				return nil, errors.New("mix coord unavailable")
			}
			return &rootcoordpb.GetCredentialResponse{
				Status:   merr.Success(),
				Username: req.GetUsername(),
				Password: passwordHash,
			}, nil
		}).Twice()

	verifier := newTestVerifier(t, context.Background(), func(ctx context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&creates, 1)
		clientCtx = ctx
		return cli, nil
	})

	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err))
	assert.NoError(t, clientCtx.Err(), "finishing one HTTP request must not cancel the cached client")
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword),
		"the cached client must recover once MixCoord is reachable")
	assert.Equal(t, int32(1), atomic.LoadInt32(&creates))
}

func TestVerifier_CanceledRequestDoesNotPoisonClient(t *testing.T) {
	var calls int32
	var clientCtx context.Context
	passwordHash := hashed(t, testPassword)

	cli := mocks.NewMockMixCoordClient(t)
	cli.EXPECT().Close().Return(nil).Maybe()
	cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, _ ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return nil, ctx.Err()
			}
			return &rootcoordpb.GetCredentialResponse{
				Status:   merr.Success(),
				Username: req.GetUsername(),
				Password: passwordHash,
			}, nil
		}).Twice()

	verifier := newTestVerifier(t, context.Background(), func(ctx context.Context) (types.MixCoordClient, error) {
		clientCtx = ctx
		return cli, nil
	})

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	err := verifier.Verify(requestCtx, "root", testPassword)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NoError(t, clientCtx.Err(), "request cancellation must not cancel the cached client")
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
}

func TestVerifier_CloseReleasesClientAndStopsNewChecks(t *testing.T) {
	var creates int32
	var clientCtx context.Context
	cli := mocks.NewMockMixCoordClient(t)
	cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: "root",
			Password: hashed(t, testPassword),
		}, nil).Once()
	cli.EXPECT().Close().Return(nil).Once()

	verifier := newTestVerifier(t, context.Background(), func(ctx context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&creates, 1)
		clientCtx = ctx
		return cli, nil
	})
	assert.NoError(t, verifier.Verify(context.Background(), "root", testPassword))
	assert.NoError(t, verifier.Close())
	assert.ErrorIs(t, clientCtx.Err(), context.Canceled)

	err := verifier.Verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&creates), "a closed verifier must not recreate its client")
	assert.NoError(t, verifier.Close(), "Close should be idempotent")
}
