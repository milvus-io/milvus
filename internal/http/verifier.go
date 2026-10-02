// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var (
	// passwordVerifyFunc is a callback function to verify user password.
	// This is set by the proxy package to avoid circular dependency.
	passwordVerifyFunc func(ctx context.Context, username, password string) bool

	// managementVerifiers holds one credential verifier per role that can own
	// one, indexed by ManagementVerifierSlot and consulted in slot order.
	// Standalone runs every role in one process, so a shared slot would make
	// the winner depend on goroutine scheduling.
	managementVerifiers [numManagementVerifierSlots]CredentialVerifier

	passwordVerifyMu sync.RWMutex
)

// CredentialVerifier checks a credential and returns nil only on a match.
//
// An error rather than a bool, so that "wrong credential" and "could not check"
// stay distinguishable -- collapsing them tells an operator whose cluster is
// half-down that their correct password is invalid. Return
// merr.ErrPrivilegeNotAuthenticated for a mismatch,
// merr.ErrPrivilegeNotPermitted for a caller no password would admit, anything
// else for "could not check".
type CredentialVerifier func(ctx context.Context, username, password string) error

// ManagementVerifierSlot names the role that owns a management-plane
// credential verifier. Lower slots win: a standalone process registers Proxy,
// MixCoord and Worker verifiers, and Proxy is the one that answers.
type ManagementVerifierSlot int

const (
	// VerifierSlotProxy is the Proxy's verifier.
	VerifierSlotProxy ManagementVerifierSlot = iota
	// VerifierSlotCoordinator is MixCoord's in-process verifier.
	VerifierSlotCoordinator
	// VerifierSlotWorker is the verifier used by nodes that hold no credential
	// metadata (querynode, datanode, streamingnode) and must ask MixCoord.
	VerifierSlotWorker
	numManagementVerifierSlots
)

// RegisterPasswordVerifyFunc registers the proxy-owned data-plane password
// check used by HTTP RBAC. This is called by the proxy package to avoid a
// circular dependency.
func RegisterPasswordVerifyFunc(fn func(ctx context.Context, username, password string) bool) {
	passwordVerifyMu.Lock()
	defer passwordVerifyMu.Unlock()
	passwordVerifyFunc = fn
}

// RegisterManagementVerifier installs (or, with a nil fn, removes) this role's
// management-plane credential verifier.
func RegisterManagementVerifier(slot ManagementVerifierSlot, fn CredentialVerifier) {
	passwordVerifyMu.Lock()
	defer passwordVerifyMu.Unlock()
	managementVerifiers[slot] = fn
}

// resolveManagementVerifier returns the verifier the management plane uses, or
// nil when this node has none. Slot order decides where a refresh goes and what
// it costs, not whether an unauthenticated caller can drive one per request:
// every management verifier caches root's hash. The data-plane bool verifier is
// deliberately not consulted because non-root HTTP RBAC users must not be
// checked against this root-only cache.
func resolveManagementVerifier() CredentialVerifier {
	passwordVerifyMu.RLock()
	defer passwordVerifyMu.RUnlock()
	for _, verifier := range managementVerifiers {
		if verifier != nil {
			return verifier
		}
	}
	return nil
}

// verifyManagementPassword checks the credential with whichever management
// verifier this node has.
func verifyManagementPassword(ctx context.Context, username, password, endpoint string) error {
	verifier := resolveManagementVerifier()
	if verifier == nil {
		return &ErrServiceUnavailable{msg: "password verification not available on this node"}
	}

	// Only two verdicts are the verifier's to make; everything else means it
	// could not check, whatever the reason. A verifier's own root-only check is
	// defense in depth behind CheckRootAuth's, so its 403 has to survive rather
	// than being swallowed into 503 -- otherwise the backstop reports an outage
	// instead of a refusal.
	err := verifier(ctx, username, password)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, merr.ErrPrivilegeNotAuthenticated), IsAuthenticationError(err):
		return &ErrAuthentication{msg: "invalid root password"}
	case errors.Is(err, merr.ErrPrivilegeNotPermitted), IsPermissionDeniedError(err):
		return &ErrPermissionDenied{msg: "only root user can access this endpoint"}
	default:
		// Store unreachable, hash malformed, comparison shed under load: all
		// "could not check", and the body says only that. Why goes to the log
		// and the metric, never to a caller who has not authenticated -- merr
		// renders wrapped errors with a stack trace carrying absolute build
		// paths.
		mlog.RatedWarn(ctx, 1.0, "cannot verify credential on this node",
			mlog.String("endpoint", truncateForLog(endpoint)), mlog.Err(err))
		return &ErrServiceUnavailable{
			msg: "cannot verify credentials on this node; check the node's logs",
		}
	}
}

// verifyRBACPassword verifies credentials for HTTP RBAC using only the
// proxy-owned bool verifier: the management verifier is a root-only hook, and
// letting it stand in here would make a MixCoord registration reject valid
// non-root users in a standalone process.
func verifyRBACPassword(ctx context.Context, username, password string) error {
	passwordVerifyMu.RLock()
	verifier := passwordVerifyFunc
	passwordVerifyMu.RUnlock()
	if verifier == nil {
		return &ErrServiceUnavailable{msg: "password verification not available"}
	}
	if !verifier(ctx, username, password) {
		return &ErrAuthentication{msg: "invalid credentials"}
	}
	return nil
}
