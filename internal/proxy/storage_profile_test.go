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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/storageprofile"
)

func TestBeginProxyStorageProfilePropagatesNestedRequestState(t *testing.T) {
	enabled := Params.StorageProfileCfg.Enabled.GetValue()
	allowExplicit := Params.StorageProfileCfg.RequestAllowExplicit.GetValue()
	defer func() {
		require.NoError(t, Params.Save(Params.StorageProfileCfg.Enabled.Key, enabled))
		require.NoError(t, Params.Save(Params.StorageProfileCfg.RequestAllowExplicit.Key, allowExplicit))
	}()
	require.NoError(t, Params.Save(Params.StorageProfileCfg.Enabled.Key, "true"))
	require.NoError(t, Params.Save(Params.StorageProfileCfg.RequestAllowExplicit.Key, "true"))

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		storageprofile.ExplicitRequestHeader, "summary",
	))
	bound, scope, level, scopeID := beginProxyStorageProfile(ctx, storageprofile.WorkloadKindSearch)
	defer scope.Finish()

	assert.Equal(t, storageprofile.StorageProfileSummary, level)
	assert.Equal(t, storageprofile.StorageProfileSummary, storageprofile.ProfileLevelFromContext(bound))
	assert.NotEmpty(t, scopeID)
	assert.Equal(t, scopeID, storageprofile.AttributionFromContext(bound).RequestID)
}
