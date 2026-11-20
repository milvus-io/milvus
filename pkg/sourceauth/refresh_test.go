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

package sourceauth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func incomingRPC(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestOmittedAuthorizedRefreshRejectsChangedCoordinatorSource(t *testing.T) {
	stored := fixtureOperation()
	ctx := callerContext("alice")
	source, spec, err := PrepareRefresh(ctx, stored, "", "")
	require.NoError(t, err)
	rpc := incomingRPC(RefreshRPCContext(ctx, stored.Source, stored.Spec, true))
	require.NoError(t, ValidateRefreshAdmission(rpc, stored.Source, stored.Spec, source, spec))
	err = ValidateRefreshAdmission(rpc, stored.Source+"-new", stored.Spec, source, spec)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	status := merr.Status(err)
	require.True(t, status.Retriable)
}

func TestCoordinatorRejectsMissingFreshBinding(t *testing.T) {
	stored := fixtureOperation()
	bound, err := Prepare(callerContext("alice"), stored, true)
	require.NoError(t, err)
	for _, replacement := range [][2]string{{"", ""}, {stored.Source, stored.Spec}} {
		err := ValidateRefreshAdmission(context.Background(), stored.Source, bound, replacement[0], replacement[1])
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	}
}

func TestInternalPreconditionCannotBeAppendedOrReplayedFromCaller(t *testing.T) {
	ctx := metadata.NewOutgoingContext(callerContext("alice"), metadata.Pairs(refreshPreconditionKey, "injected", refreshPreconditionKey, "duplicate"))
	stored := fixtureOperation()
	rpc := incomingRPC(RefreshRPCContext(ctx, stored.Source, stored.Spec, true))
	require.NoError(t, ValidateRefreshAdmission(rpc, stored.Source, stored.Spec, "", ""))
	explicit := incomingRPC(RefreshRPCContext(ctx, stored.Source, stored.Spec, false))
	md, _ := metadata.FromIncomingContext(explicit)
	require.Empty(t, md.Get(refreshPreconditionKey))
}
