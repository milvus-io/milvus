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

// Package adminauth provides the credential verifier that lets nodes without
// local credential metadata authenticate management-plane requests.
package adminauth

import (
	"context"
	"sync"
	"time"

	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// verifyTimeout bounds each credential RPC. The caller is an HTTP request on
// the metrics port, frequently a human debugging a sick cluster, so an
// unreachable coord must produce a prompt failure rather than a hung request.
const verifyTimeout = 5 * time.Second

// RootCredentialVerifier verifies management-plane credentials on nodes that
// do not own credential metadata. The MixCoord client is created lazily and is
// cached for the component lifetime, while each credential lookup and any
// address discovery it triggers are bounded by the HTTP request context.
type RootCredentialVerifier struct {
	ctx       context.Context
	cancel    context.CancelFunc
	newClient func(ctx context.Context) (types.MixCoordClient, error)

	mu     sync.Mutex
	client types.MixCoordClient
	closed bool
}

// NewRootCredentialVerifier returns a credential verifier for nodes that do
// not own credential metadata — querynode, datanode and streamingnode. It
// resolves the root user's stored bcrypt hash through the mix coord and
// compares it locally.
//
// newClient is called lazily the first time a credential actually needs
// verifying. Creating the client lazily rather than at startup keeps this off
// the boot path of every node: with
// common.security.adminAuthEnabled at its default of false the client is never
// created at all. A failed constructor call is not cached. Once constructed,
// the MixCoord client remains cached across HTTP requests so it can reconnect
// after MixCoord becomes available or moves. Reconnect address discovery uses
// the current RPC context, so a blocked etcd lookup is still bounded by the
// request's five-second timeout without shortening the cached client's life.
//
// Register the result's Verify method with
// http.RegisterFallbackPasswordVerifyFunc. It occupies the fallback slot, so
// on nodes that do have local credential metadata (proxy, mix coord) the
// cheaper in-process verifier always wins and no RPC is made.
//
// Only the root user is accepted. Management endpoints act on process lifecycle
// and cluster runtime state, so they stay root-only even for otherwise valid
// users; http.CheckRootAuth enforces the same rule ahead of this call, making
// this defense in depth.
//
// Note the operational trade-off this creates, which belongs in release notes:
// once adminAuthEnabled is true, management access to a worker node depends on
// the mix coord being reachable to verify the password. A worker stays
// profileable while coord is healthy; if coord is down, /debug/pprof on that
// worker answers 503 rather than serving a profile. Turning the flag off
// restores unauthenticated access. Close the verifier when the role exits.
func NewRootCredentialVerifier(
	lifetimeCtx context.Context,
	newClient func(ctx context.Context) (types.MixCoordClient, error),
) *RootCredentialVerifier {
	ctx, cancel := context.WithCancel(lifetimeCtx)
	return &RootCredentialVerifier{
		ctx:       ctx,
		cancel:    cancel,
		newClient: newClient,
	}
}

func (v *RootCredentialVerifier) getClient() (types.MixCoordClient, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, merr.WrapErrServiceUnavailable("root credential verifier is closed")
	}
	if v.client != nil {
		return v.client, nil
	}
	created, err := v.newClient(v.ctx)
	if err != nil {
		return nil, err
	}
	if created == nil {
		return nil, merr.WrapErrServiceUnavailable("mix coord client is unavailable")
	}
	v.client = created
	return v.client, nil
}

// Verify checks a root credential. Request cancellation and verifyTimeout
// bound only this lookup; they never cancel the cached client's reconnect and
// service-discovery context.
func (v *RootCredentialVerifier) Verify(ctx context.Context, username, password string) error {
	if username != util.UserRoot {
		return internalhttp.NewAuthenticationError("invalid root password")
	}

	ctx, cancel := context.WithTimeout(ctx, verifyTimeout)
	defer cancel()

	// Every failure below except the bcrypt mismatch means "could not
	// check", not "wrong password", and is returned as a non-authentication
	// error so the caller renders 503 rather than telling the operator their
	// correct password is invalid while the coord is down.
	cli, err := v.getClient()
	if err != nil {
		return merr.Wrap(err, "mix coord client unavailable")
	}

	resp, err := cli.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{
		Username: username,
	})
	if err != nil {
		return merr.Wrap(err, "GetCredential failed")
	}
	if resp == nil {
		return merr.WrapErrServiceInternal("GetCredential returned an empty response")
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return merr.Wrap(err, "GetCredential returned error")
	}
	if resp.GetPassword() == "" {
		// Not a wrong password: the store answered OK but carried no hash,
		// which is an internal inconsistency. Reporting it as unverifiable
		// (503) also stops bcrypt from being handed an empty hash.
		return merr.WrapErrServiceInternal("credential store returned an empty hash for root")
	}

	return internalhttp.VerifyStoredRootPassword(resp.GetPassword(), password)
}

// Close releases the cached MixCoord client and prevents new credential
// checks from creating one during role shutdown.
func (v *RootCredentialVerifier) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	v.cancel()
	client := v.client
	v.client = nil
	v.mu.Unlock()

	if client != nil {
		return client.Close()
	}
	return nil
}
