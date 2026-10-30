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

package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only the coordinator boundary is replaced; authorization runs in the real
// Proxy handlers. A denied call must never read telemetry or mutate commands.
type telemetryAuthCoordinator struct {
	types.MixCoordClient
	calls int
}

func (c *telemetryAuthCoordinator) GetClientTelemetry(context.Context, *milvuspb.GetClientTelemetryRequest, ...grpc.CallOption) (*milvuspb.GetClientTelemetryResponse, error) {
	c.calls++
	return &milvuspb.GetClientTelemetryResponse{Status: merr.Success()}, nil
}

func (c *telemetryAuthCoordinator) PushClientCommand(context.Context, *milvuspb.PushClientCommandRequest, ...grpc.CallOption) (*milvuspb.PushClientCommandResponse, error) {
	c.calls++
	return &milvuspb.PushClientCommandResponse{Status: merr.Success()}, nil
}

func (c *telemetryAuthCoordinator) DeleteClientCommand(context.Context, *milvuspb.DeleteClientCommandRequest, ...grpc.CallOption) (*milvuspb.DeleteClientCommandResponse, error) {
	c.calls++
	return &milvuspb.DeleteClientCommandResponse{Status: merr.Success()}, nil
}

func (c *telemetryAuthCoordinator) ClientHeartbeat(context.Context, *milvuspb.ClientHeartbeatRequest, ...grpc.CallOption) (*milvuspb.ClientHeartbeatResponse, error) {
	c.calls++
	return &milvuspb.ClientHeartbeatResponse{Status: merr.Success()}, nil
}

func TestTelemetryRPCRequiresRoot(t *testing.T) {
	oldAuth := Params.CommonCfg.AuthorizationEnabled.SwapTempValue("true")
	t.Cleanup(func() { Params.CommonCfg.AuthorizationEnabled.SwapTempValue(oldAuth) })
	for _, tc := range []struct {
		name string
		ctx  context.Context
		code int32
	}{
		{"ordinary_user", NewContextWithMetadata(context.Background(), "alice", "default"), merr.Code(merr.ErrPrivilegeNotPermitted)},
		{"missing_identity", context.Background(), merr.Code(merr.ErrPrivilegeNotAuthenticated)},
		{"malformed_identity", metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, "!!!")), merr.Code(merr.ErrPrivilegeNotAuthenticated)},
		{"root", NewContextWithMetadata(context.Background(), util.UserRoot, "default"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord := &telemetryAuthCoordinator{}
			node := &Proxy{mixCoord: coord}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			read, err := node.GetClientTelemetry(tc.ctx, &milvuspb.GetClientTelemetryRequest{})
			require.NoError(t, err)
			assert.Equal(t, tc.code, read.GetStatus().GetCode())
			push, err := node.PushClientCommand(tc.ctx, &milvuspb.PushClientCommandRequest{})
			require.NoError(t, err)
			assert.Equal(t, tc.code, push.GetStatus().GetCode())
			del, err := node.DeleteClientCommand(tc.ctx, &milvuspb.DeleteClientCommandRequest{})
			require.NoError(t, err)
			assert.Equal(t, tc.code, del.GetStatus().GetCode())
			if tc.code != 0 {
				assert.Zero(t, coord.calls)
			} else {
				assert.Equal(t, 3, coord.calls)
			}
		})
	}
}

func TestTelemetryHeartbeatRemainsAvailable(t *testing.T) {
	oldAuth := Params.CommonCfg.AuthorizationEnabled.SwapTempValue("true")
	t.Cleanup(func() { Params.CommonCfg.AuthorizationEnabled.SwapTempValue(oldAuth) })
	coord := &telemetryAuthCoordinator{}
	node := &Proxy{mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx := NewContextWithMetadata(context.Background(), "alice", "default")
	resp, err := node.ClientHeartbeat(ctx, &milvuspb.ClientHeartbeatRequest{})
	require.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))
	assert.Equal(t, 1, coord.calls)
}

func TestTelemetryRPCWithoutAuthorization(t *testing.T) {
	oldAuth := Params.CommonCfg.AuthorizationEnabled.SwapTempValue("false")
	t.Cleanup(func() { Params.CommonCfg.AuthorizationEnabled.SwapTempValue(oldAuth) })
	coord := &telemetryAuthCoordinator{}
	node := &Proxy{mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	read, err := node.GetClientTelemetry(context.Background(), &milvuspb.GetClientTelemetryRequest{})
	require.NoError(t, err)
	assert.True(t, merr.Ok(read.GetStatus()))
	push, err := node.PushClientCommand(context.Background(), &milvuspb.PushClientCommandRequest{})
	require.NoError(t, err)
	assert.True(t, merr.Ok(push.GetStatus()))
	del, err := node.DeleteClientCommand(context.Background(), &milvuspb.DeleteClientCommandRequest{})
	require.NoError(t, err)
	assert.True(t, merr.Ok(del.GetStatus()))
	assert.Equal(t, 3, coord.calls)
}

func TestTelemetryHTTPForwardsVerifiedRoot(t *testing.T) {
	oldAuth := Params.CommonCfg.AuthorizationEnabled.SwapTempValue("true")
	oldAdmin := Params.CommonCfg.AdminAuthEnabled.SwapTempValue("false")
	t.Cleanup(func() {
		Params.CommonCfg.AuthorizationEnabled.SwapTempValue(oldAuth)
		Params.CommonCfg.AdminAuthEnabled.SwapTempValue(oldAdmin)
		internalhttp.RegisterManagementVerifier(internalhttp.VerifierSlotProxy, nil)
	})
	// Credential verification is covered separately. Exercise the legacy HTTP
	// adapter after it has verified a root or ordinary user's credentials.
	verify := mockey.Mock(passwordVerify).Return(true).Build()
	t.Cleanup(func() { verify.UnPatch() })
	internalhttp.RegisterManagementVerifier(internalhttp.VerifierSlotProxy, func(context.Context, string, string) error { return nil })
	for _, gate := range []string{"false", "true"} {
		for _, username := range []string{util.UserRoot, "alice"} {
			t.Run("management_gate="+gate+"/"+username, func(t *testing.T) {
				Params.CommonCfg.AdminAuthEnabled.SwapTempValue(gate)
				coord := &telemetryAuthCoordinator{}
				node := &Proxy{mixCoord: coord}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				router := gin.New()
				router.GET("/", TelemetryAuthMiddleware(), getTelemetryClients(node))
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.SetBasicAuth(username, "test-password")
				req.Header.Set(internalhttp.AdminRequestHeader, "true")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if username == util.UserRoot {
					assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
					assert.Equal(t, 1, coord.calls)
				} else {
					assert.NotEqual(t, http.StatusOK, w.Code)
					assert.Zero(t, coord.calls)
				}
			})
		}
	}
}
