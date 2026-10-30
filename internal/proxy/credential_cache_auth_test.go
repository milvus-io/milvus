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

package proxy

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only GetCredential is used by these notification/verification paths.
type notificationCredentialClient struct {
	types.MixCoordClient
	getCredential func(*rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error)
}

func (c *notificationCredentialClient) GetCredential(_ context.Context, request *rootcoordpb.GetCredentialRequest, _ ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	return c.getCredential(request)
}

func TestCredentialNotificationsRevokeManagementPassword(t *testing.T) {
	oldHash, err := bcrypt.GenerateFromPassword([]byte("previous"), bcrypt.MinCost)
	require.NoError(t, err)
	newHash, err := bcrypt.GenerateFromPassword([]byte("rotated"), bcrypt.MinCost)
	require.NoError(t, err)
	for _, update := range []bool{true, false} {
		for _, withBusinessCache := range []bool{true, false} {
			t.Run(fmt.Sprintf("update=%t/business-cache=%t", update, withBusinessCache), func(t *testing.T) {
				ctx := context.Background()
				var rotated, unavailable atomic.Bool
				var lookups atomic.Int32
				mixCoord := &notificationCredentialClient{
					getCredential: func(request *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
						lookups.Add(1)
						if unavailable.Load() {
							return nil, merr.WrapErrServiceUnavailable("coordinator unavailable")
						}
						hash := oldHash
						if rotated.Load() {
							hash = newHash
						}
						return &rootcoordpb.GetCredentialResponse{Status: merr.Success(), Username: request.Username, Password: string(hash)}, nil
					},
				}
				businessCache := privilege.NewPrivilegeCache(mixCoord)
				if !withBusinessCache {
					businessCache = nil
				}
				// Isolate the process-global business cache while exercising
				// the real notification handler, cache and management fetcher.
				patch := mockey.Mock(privilege.GetPrivilegeCache).Return(businessCache).Build()
				t.Cleanup(func() { patch.UnPatch() })
				node := &Proxy{managementRootVerifier: newManagementRootVerifier(mixCoord)}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				verify := func(password string) error { return node.managementRootVerifier.Verify(ctx, util.UserRoot, password) }
				notify := func(username string) {
					var status *commonpb.Status
					var err error
					if update {
						status, err = node.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{
							Username: username, Password: crypto.SHA256("rotated", username),
						})
					} else {
						status, err = node.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{Username: username})
					}
					require.NoError(t, err)
					require.NoError(t, merr.Error(status))
				}
				require.NoError(t, verify("previous"))
				rotated.Store(true)
				notify("alice")
				require.NoError(t, verify("previous"), "another user's notification must not evict root")
				require.EqualValues(t, 1, lookups.Load())
				notify(util.UserRoot)
				require.NoError(t, verify("rotated"), "update notification carries SHA256, so fetch the authoritative bcrypt hash")
				require.ErrorIs(t, verify("previous"), merr.ErrPrivilegeNotAuthenticated)
				require.EqualValues(t, 2, lookups.Load())
				if update && businessCache != nil {
					credential, err := businessCache.GetCredentialInfo(ctx, util.UserRoot)
					require.NoError(t, err)
					require.Equal(t, crypto.SHA256("rotated", util.UserRoot), credential.Sha256Password)
				}
				unavailable.Store(true)
				notify(util.UserRoot)
				require.ErrorIs(t, verify("rotated"), merr.ErrServiceUnavailable, "an acknowledged notification revokes outage fallback too")
			})
		}
	}
}
