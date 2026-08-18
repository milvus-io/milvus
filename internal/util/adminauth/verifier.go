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

// Package adminauth provides the credential verifiers that back the
// management-plane authentication gate on nodes that do and do not own
// credential metadata.
package adminauth

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RootCredentialVerifier verifies management-plane credentials on nodes that do
// not own credential metadata — querynode, datanode and streamingnode. It
// resolves root's stored bcrypt hash through the mix coord and compares it
// locally, caching the hash (see CachedRootVerifier).
type RootCredentialVerifier struct {
	*CachedRootVerifier

	ctx       context.Context
	cancel    context.CancelFunc
	newClient func(ctx context.Context) (types.MixCoordClient, error)

	// creations collapses concurrent first requests into one constructor call
	// and runs it off the request goroutine; see getClient. x/sync directly, so
	// a wedged constructor holds one goroutine rather than one per caller.
	creations singleflight.Group

	mu     sync.Mutex
	client types.MixCoordClient
	closed bool
}

// NewRootCredentialVerifier returns the worker-node credential verifier.
// newClient is called lazily on the first credential check, so with the gate at
// its default of false the client is never created. Register Verify on
// VerifierSlotWorker and Close it when the role exits.
//
// A worker whose coordinator is unreachable answers 503 to /debug/pprof and
// /management/stop once its last known hash falls out of staleHashTTL; until
// then it keeps serving from that hash, because failing closed there would turn
// a graceful drain into a SIGKILL.
func NewRootCredentialVerifier(
	lifetimeCtx context.Context,
	newClient func(ctx context.Context) (types.MixCoordClient, error),
) *RootCredentialVerifier {
	ctx, cancel := context.WithCancel(lifetimeCtx)
	v := &RootCredentialVerifier{
		ctx:       ctx,
		cancel:    cancel,
		newClient: newClient,
	}
	v.CachedRootVerifier = NewCachedRootVerifier(v.fetchRootHash)
	return v
}

// fetchRootHash reads root's stored bcrypt hash from the mix coord. Every
// failure means "could not check", not "wrong password", so it is returned as a
// non-authentication error and renders 503. ctx carries fetchTimeout; it bounds
// the RPC, and bounds how long this waits on a client construction that may
// itself never finish (see getClient).
func (v *RootCredentialVerifier) fetchRootHash(ctx context.Context) (string, error) {
	cli, err := v.getClient(ctx)
	if err != nil {
		return "", merr.Wrap(err, "mix coord client unavailable")
	}

	resp, err := cli.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{
		Username: util.UserRoot,
	})
	if err != nil {
		return "", merr.Wrap(err, "GetCredential failed")
	}
	return RootHashFromResponse(resp)
}

// RootHashFromResponse validates a GetCredentialResponse and extracts the
// stored hash. Exported so the coordinator-side verifier, which reads the same
// RPC in-process, classifies the same malformed answers the same way.
func RootHashFromResponse(resp *rootcoordpb.GetCredentialResponse) (string, error) {
	if resp == nil {
		return "", merr.WrapErrServiceInternal("GetCredential returned an empty response")
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return "", merr.Wrap(err, "GetCredential returned error")
	}
	if resp.GetPassword() == "" {
		// Not a wrong password: the store answered OK but carried no hash,
		// which is an internal inconsistency. Reporting it as unverifiable
		// (503) also stops bcrypt from being handed an empty hash.
		return "", merr.WrapErrServiceInternal("credential store returned an empty hash for root")
	}
	return resp.GetPassword(), nil
}

// getClient returns the cached MixCoord client, constructing it on first use.
//
// newClient is not context-aware: it reaches sessionutil, ignores the context
// it is given, and panics rather than erroring when the process-wide etcd
// client cannot be built. It therefore runs on its own goroutine, so a stuck
// constructor bounds out at ctx instead of holding every gated request on this
// node, and publishes the client for the next request.
func (v *RootCredentialVerifier) getClient(ctx context.Context) (types.MixCoordClient, error) {
	if client, err := v.loadClient(); client != nil || err != nil {
		return client, err
	}
	select {
	case result := <-v.creations.DoChan("client", v.createClient):
		if result.Err != nil {
			return nil, result.Err
		}
		client, _ := result.Val.(types.MixCoordClient)
		return client, nil
	case <-ctx.Done():
		return nil, merr.WrapErrServiceUnavailable("mix coord client is not ready yet")
	}
}

// createClient runs on the singleflight goroutine, so a panic here would take
// the process down rather than being recovered per connection by net/http.
func (v *RootCredentialVerifier) createClient() (client any, err error) {
	defer func() {
		if r := recover(); r != nil {
			mlog.Warn(v.ctx, "panic while creating mix coord client", mlog.Any("panic", r))
			client, err = nil, merr.WrapErrServiceUnavailable(
				fmt.Sprintf("mix coord client could not be created: %v", r))
		}
	}()

	if cached, err := v.loadClient(); cached != nil || err != nil {
		return cached, err
	}

	created, err := v.newClient(v.ctx)
	if err != nil {
		return nil, err
	}
	if created == nil {
		return nil, merr.WrapErrServiceUnavailable("mix coord client is unavailable")
	}

	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		// Close ran while we were dialing and will never see this client, so
		// releasing it here is the only thing that keeps the connection from
		// outliving the role.
		_ = created.Close()
		return nil, merr.WrapErrServiceUnavailable("root credential verifier is closed")
	}
	v.client = created
	v.mu.Unlock()
	return created, nil
}

// loadClient returns the cached client, or an error if the verifier is closed.
// A nil client with a nil error means "not built yet".
func (v *RootCredentialVerifier) loadClient() (types.MixCoordClient, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, merr.WrapErrServiceUnavailable("root credential verifier is closed")
	}
	return v.client, nil
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

	v.Forget()

	if client != nil {
		return client.Close()
	}
	return nil
}
