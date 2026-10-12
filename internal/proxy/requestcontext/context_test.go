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

package requestcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
)

func TestFunctionScopeUsesVerifiedPrincipalAndResolvedTarget(t *testing.T) {
	raw := metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("spoofed:password"), util.HeaderDBName, "spoofed_db"))
	require.Equal(t, types.RequestContext{Database: util.DefaultDBName, Collection: "canonical"}, FunctionScope(raw, "", "canonical"))
	verified := WithAuthenticatedUser(raw, "alice")
	scoped := WithFunctionScope(verified, "resolved_db", "canonical")
	require.Equal(t, types.RequestContext{Database: "resolved_db", Collection: "canonical", User: "alice"}, types.GetRequestContext(scoped))
	require.Empty(t, AuthenticatedUser(raw))
	copy := types.GetRequestContext(scoped)
	copy.User = "tampered"
	require.Equal(t, "alice", types.GetRequestContext(scoped).User)
}

func TestFunctionScopeDoesNotShareIdentityOrLoseCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	alice := WithFunctionScope(WithAuthenticatedUser(parent, "alice"), "tenant_a", "a")
	bob := WithFunctionScope(WithAuthenticatedUser(parent, "bob"), "tenant_b", "b")
	require.Equal(t, "alice", types.GetRequestContext(alice).User)
	require.Equal(t, "bob", types.GetRequestContext(bob).User)
	require.Empty(t, types.GetRequestContext(parent).User)
	cancel()
	require.ErrorIs(t, alice.Err(), context.Canceled)
	require.ErrorIs(t, bob.Err(), context.Canceled)
}
