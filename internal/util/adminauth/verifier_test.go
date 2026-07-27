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

func TestVerifier_AcceptsCorrectRootPassword(t *testing.T) {
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, hashed(t, testPassword)), nil
	})
	assert.NoError(t, verify(context.Background(), "root", testPassword))
}

func TestVerifier_RejectsWrongPassword(t *testing.T) {
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, hashed(t, testPassword)), nil
	})
	err := verify(context.Background(), "root", "not-the-password")
	assert.ErrorIs(t, err, internalhttp.ErrInvalidCredential)
	assert.True(t, internalhttp.IsAuthenticationError(err),
		"a genuine mismatch must be reported as an authentication failure (401), not as unavailable")
}

func TestVerifier_RejectsNonRootWithoutDialing(t *testing.T) {
	// A non-root user must be refused before any RPC: the management plane is
	// root-only, so dialing the coord to check another user's password would be
	// wasted work and would let an unauthenticated caller drive load onto coord.
	var dials int32
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return clientReturning(t, hashed(t, testPassword)), nil
	})

	assert.ErrorIs(t, verify(context.Background(), "alice", testPassword), internalhttp.ErrInvalidCredential)
	assert.Zero(t, atomic.LoadInt32(&dials), "must not dial mix coord for a non-root user")
}

func TestVerifier_DialsLazilyAndReusesClient(t *testing.T) {
	// Nothing should be dialed at construction time — that is what keeps this
	// off every node's boot path and makes it free while adminAuthEnabled is
	// false. Once dialed, the client is reused.
	var dials int32
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		atomic.AddInt32(&dials, 1)
		return clientReturning(t, hashed(t, testPassword)), nil
	})
	assert.Zero(t, atomic.LoadInt32(&dials), "constructing the verifier must not dial")

	assert.NoError(t, verify(context.Background(), "root", testPassword))
	assert.NoError(t, verify(context.Background(), "root", testPassword))
	assert.Equal(t, int32(1), atomic.LoadInt32(&dials), "client should be dialed once and reused")
}

func TestVerifier_FailedDialIsNotCached(t *testing.T) {
	// A coord that is unreachable while a node boots must not permanently
	// disable management access on that node, so a failed dial is retried.
	var dials int32
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		if atomic.AddInt32(&dials, 1) == 1 {
			return nil, errors.New("coord unreachable")
		}
		return clientReturning(t, hashed(t, testPassword)), nil
	})

	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"an unreachable coord must not be reported as a bad password")
	assert.NoError(t, verify(context.Background(), "root", testPassword),
		"a later attempt must succeed once the coord is reachable")
	assert.Equal(t, int32(2), atomic.LoadInt32(&dials))
}

func TestVerifier_RejectsOnRPCError(t *testing.T) {
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		cli := mocks.NewMockMixCoordClient(t)
		cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
			Return(nil, errors.New("rpc failed")).Maybe()
		return cli, nil
	})
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"an RPC failure must render 503, not 401")
}

func TestVerifier_RejectsOnErrorStatus(t *testing.T) {
	// A non-OK Status must fail closed. Without checking it, a response whose
	// Password field is empty would be compared against an empty hash.
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		cli := mocks.NewMockMixCoordClient(t)
		cli.EXPECT().GetCredential(mock.Anything, mock.Anything).
			Return(&rootcoordpb.GetCredentialResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "boom"},
			}, nil).Maybe()
		return cli, nil
	})
	err := verify(context.Background(), "root", testPassword)
	assert.Error(t, err)
	assert.False(t, internalhttp.IsAuthenticationError(err),
		"a non-OK status means the credential could not be checked -> 503")
}

func TestVerifier_RejectsEmptyStoredHash(t *testing.T) {
	// Defense in depth against an OK response carrying no credential: bcrypt
	// must never be asked to treat "" as a valid hash for "".
	verify := NewRootCredentialVerifier(func(context.Context) (types.MixCoordClient, error) {
		return clientReturning(t, ""), nil
	})
	assert.Error(t, verify(context.Background(), "root", ""))
	assert.Error(t, verify(context.Background(), "root", testPassword))
}
