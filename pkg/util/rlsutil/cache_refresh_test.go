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

package rlsutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

func TestEncodeRefresh(t *testing.T) {
	tests := []struct {
		name        string
		req         *messagespb.RefreshRLSCacheRequest
		wantOpType  int32
		wantErr     bool
		errContains string
	}{
		{
			name: "valid CreatePolicy request",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:         messagespb.RLSCacheOpType_CreatePolicy,
				DbName:         "testdb",
				CollectionName: "testcol",
				PolicyName:     "pol1",
				UserName:       "user1",
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_CreatePolicy),
		},
		{
			name: "valid DropPolicy request",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:     messagespb.RLSCacheOpType_DropPolicy,
				DbName:     "db",
				PolicyName: "p",
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_DropPolicy),
		},
		{
			name: "valid UpdateUserTags request",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:   messagespb.RLSCacheOpType_UpdateUserTags,
				UserName: "alice",
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_UpdateUserTags),
		},
		{
			name: "valid DeleteUserTag request",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:   messagespb.RLSCacheOpType_DeleteUserTag,
				UserName: "bob",
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_DeleteUserTag),
		},
		{
			name: "valid UpdateCollectionConfig request",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:         messagespb.RLSCacheOpType_UpdateCollectionConfig,
				DbName:         "db2",
				CollectionName: "col2",
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_UpdateCollectionConfig),
		},
		{
			name:        "nil request",
			req:         nil,
			wantErr:     true,
			errContains: "nil refresh request",
		},
		{
			name: "empty fields are ok",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType: messagespb.RLSCacheOpType_CreatePolicy,
			},
			wantOpType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_CreatePolicy),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opType, opKey, err := EncodeRefresh(tt.req)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantOpType, opType)

			// opKey should be valid JSON
			var payload refreshPayload
			err = json.Unmarshal([]byte(opKey), &payload)
			assert.NoError(t, err)
			assert.Equal(t, int32(tt.req.GetOpType()), payload.OpType)
			assert.Equal(t, tt.req.GetDbName(), payload.DBName)
			assert.Equal(t, tt.req.GetCollectionName(), payload.CollectionName)
			assert.Equal(t, tt.req.GetPolicyName(), payload.PolicyName)
			assert.Equal(t, tt.req.GetUserName(), payload.UserName)
		})
	}
}

func TestDecodeRefresh(t *testing.T) {
	tests := []struct {
		name       string
		opType     int32
		opKey      string
		wantIsRLS  bool
		wantErr    bool
		wantOpType messagespb.RLSCacheOpType
		wantDB     string
		wantCol    string
		wantPolicy string
		wantUser   string
	}{
		{
			name:       "non-RLS opType returns false",
			opType:     0,
			opKey:      "",
			wantIsRLS:  false,
			wantErr:    false,
		},
		{
			name:       "non-RLS opType 999 returns false",
			opType:     999,
			opKey:      "",
			wantIsRLS:  false,
			wantErr:    false,
		},
		{
			name:       "negative opType returns false",
			opType:     -1,
			opKey:      "",
			wantIsRLS:  false,
			wantErr:    false,
		},
		{
			name:       "RLS opType with empty opKey",
			opType:     RLSProxyRefreshOpBase,
			opKey:      "",
			wantIsRLS:  true,
			wantErr:    false,
			wantOpType: messagespb.RLSCacheOpType_CreatePolicy,
		},
		{
			name:   "RLS opType with valid JSON",
			opType: RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_DropPolicy),
			opKey: func() string {
				data, _ := json.Marshal(refreshPayload{
					OpType:         int32(messagespb.RLSCacheOpType_DropPolicy),
					DBName:         "mydb",
					CollectionName: "mycol",
					PolicyName:     "mypol",
					UserName:       "myuser",
				})
				return string(data)
			}(),
			wantIsRLS:  true,
			wantErr:    false,
			wantOpType: messagespb.RLSCacheOpType_DropPolicy,
			wantDB:     "mydb",
			wantCol:    "mycol",
			wantPolicy: "mypol",
			wantUser:   "myuser",
		},
		{
			name:      "RLS opType with malformed JSON",
			opType:    RLSProxyRefreshOpBase,
			opKey:     "{invalid json",
			wantIsRLS: true,
			wantErr:   true,
		},
		{
			name:       "RLS opType with UpdateUserTags",
			opType:     RLSProxyRefreshOpBase + int32(messagespb.RLSCacheOpType_UpdateUserTags),
			opKey:      `{"op_type":2,"user_name":"alice"}`,
			wantIsRLS:  true,
			wantErr:    false,
			wantOpType: messagespb.RLSCacheOpType_UpdateUserTags,
			wantUser:   "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, isRLS, err := DecodeRefresh(tt.opType, tt.opKey)
			assert.Equal(t, tt.wantIsRLS, isRLS)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			if !tt.wantIsRLS {
				assert.Nil(t, req)
				return
			}

			assert.NotNil(t, req)
			assert.Equal(t, tt.wantOpType, req.GetOpType())
			assert.Equal(t, tt.wantDB, req.GetDbName())
			assert.Equal(t, tt.wantCol, req.GetCollectionName())
			assert.Equal(t, tt.wantPolicy, req.GetPolicyName())
			assert.Equal(t, tt.wantUser, req.GetUserName())
		})
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *messagespb.RefreshRLSCacheRequest
	}{
		{
			name: "CreatePolicy full fields",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:         messagespb.RLSCacheOpType_CreatePolicy,
				DbName:         "production",
				CollectionName: "users",
				PolicyName:     "tenant_isolation",
				UserName:       "admin",
			},
		},
		{
			name: "DropPolicy",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:         messagespb.RLSCacheOpType_DropPolicy,
				DbName:         "staging",
				CollectionName: "items",
				PolicyName:     "region_filter",
			},
		},
		{
			name: "UpdateUserTags minimal",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:   messagespb.RLSCacheOpType_UpdateUserTags,
				UserName: "viewer",
			},
		},
		{
			name: "DeleteUserTag",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:   messagespb.RLSCacheOpType_DeleteUserTag,
				UserName: "olduser",
			},
		},
		{
			name: "UpdateCollectionConfig",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType:         messagespb.RLSCacheOpType_UpdateCollectionConfig,
				DbName:         "analytics",
				CollectionName: "events",
			},
		},
		{
			name: "empty optional fields",
			req: &messagespb.RefreshRLSCacheRequest{
				OpType: messagespb.RLSCacheOpType_CreatePolicy,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opType, opKey, err := EncodeRefresh(tt.req)
			assert.NoError(t, err)

			decoded, isRLS, err := DecodeRefresh(opType, opKey)
			assert.NoError(t, err)
			assert.True(t, isRLS)
			assert.NotNil(t, decoded)

			assert.Equal(t, tt.req.GetOpType(), decoded.GetOpType())
			assert.Equal(t, tt.req.GetDbName(), decoded.GetDbName())
			assert.Equal(t, tt.req.GetCollectionName(), decoded.GetCollectionName())
			assert.Equal(t, tt.req.GetPolicyName(), decoded.GetPolicyName())
			assert.Equal(t, tt.req.GetUserName(), decoded.GetUserName())
		})
	}
}

func TestAllRLSCacheOpTypeValues(t *testing.T) {
	allOpTypes := []struct {
		name   string
		opType messagespb.RLSCacheOpType
	}{
		{"CreatePolicy", messagespb.RLSCacheOpType_CreatePolicy},
		{"DropPolicy", messagespb.RLSCacheOpType_DropPolicy},
		{"UpdateUserTags", messagespb.RLSCacheOpType_UpdateUserTags},
		{"DeleteUserTag", messagespb.RLSCacheOpType_DeleteUserTag},
		{"UpdateCollectionConfig", messagespb.RLSCacheOpType_UpdateCollectionConfig},
	}

	for _, tt := range allOpTypes {
		t.Run(tt.name, func(t *testing.T) {
			req := &messagespb.RefreshRLSCacheRequest{
				OpType:         tt.opType,
				DbName:         "db",
				CollectionName: "col",
				PolicyName:     "pol",
				UserName:       "usr",
			}

			opType, opKey, err := EncodeRefresh(req)
			assert.NoError(t, err)

			// Verify opType is offset correctly
			assert.Equal(t, RLSProxyRefreshOpBase+int32(tt.opType), opType)
			assert.GreaterOrEqual(t, opType, RLSProxyRefreshOpBase)

			// Verify round-trip
			decoded, isRLS, err := DecodeRefresh(opType, opKey)
			assert.NoError(t, err)
			assert.True(t, isRLS)
			assert.Equal(t, tt.opType, decoded.GetOpType())
		})
	}
}
