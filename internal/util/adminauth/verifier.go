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

	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/bcrypt"

	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// verifyTimeout bounds the credential lookup, including lazily dialing the mix
// coord. The caller is an HTTP request on the metrics port, frequently a human
// debugging a sick cluster, so an unreachable coord must produce a prompt
// failure rather than a hung connection.
const verifyTimeout = 5 * time.Second

// NewRootCredentialVerifier returns a credential verifier for nodes that do not
// own credential metadata — querynode, datanode and streamingnode. It resolves
// the root user's stored bcrypt hash through the mix coord and compares it
// locally.
//
// newClient is called at most once per successful dial, the first time a
// credential actually needs verifying. Dialing lazily rather than at startup
// keeps this off the boot path of every node: with
// common.security.adminAuthEnabled at its default of false the client is never
// created at all. A failed dial is not cached, so a coord that is down while a
// node boots does not permanently disable management access on that node.
//
// Register the result with http.RegisterFallbackPasswordVerifyFunc. It occupies
// the fallback slot, so on nodes that do have local credential metadata (proxy,
// mix coord) the cheaper in-process verifier always wins and no RPC is made.
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
// restores unauthenticated access.
func NewRootCredentialVerifier(newClient func(ctx context.Context) (types.MixCoordClient, error)) internalhttp.FallbackVerifier {
	var (
		mu     sync.Mutex
		client types.MixCoordClient
	)

	getClient := func(ctx context.Context) (types.MixCoordClient, error) {
		mu.Lock()
		defer mu.Unlock()
		if client != nil {
			return client, nil
		}
		created, err := newClient(ctx)
		if err != nil {
			return nil, err
		}
		client = created
		return client, nil
	}

	return func(ctx context.Context, username, password string) error {
		if username != util.UserRoot {
			return internalhttp.ErrInvalidCredential
		}

		ctx, cancel := context.WithTimeout(ctx, verifyTimeout)
		defer cancel()

		// Every failure below except the bcrypt mismatch means "could not
		// check", not "wrong password", and is returned as a plain error so the
		// caller renders 503 rather than telling the operator their correct
		// password is invalid while the coord is down.
		cli, err := getClient(ctx)
		if err != nil {
			return errors.Wrap(err, "mix coord client unavailable")
		}

		resp, err := cli.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{
			Username: username,
		})
		if err != nil {
			return errors.Wrap(err, "GetCredential failed")
		}
		if err := merr.Error(resp.GetStatus()); err != nil {
			return errors.Wrap(err, "GetCredential returned error")
		}
		if resp.GetPassword() == "" {
			// Not a wrong password: the store answered OK but carried no hash,
			// which is an internal inconsistency. Reporting it as unverifiable
			// (503) also stops bcrypt from being handed an empty hash.
			return merr.WrapErrServiceInternal("credential store returned an empty hash for root")
		}

		if bcrypt.CompareHashAndPassword([]byte(resp.GetPassword()), []byte(password)) != nil {
			// Don't log here; the caller logs the mismatch together with the
			// request path, which is more useful for triage.
			return internalhttp.ErrInvalidCredential
		}
		return nil
	}
}
