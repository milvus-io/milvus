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

package contextutil

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"google.golang.org/grpc/metadata"
)

type ctxTenantKey struct{}

// WithTenantID creates a new context that has tenantID injected.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxTenantKey{}, tenantID)
}

// TenantID tries to retrieve tenantID from the given context.
// If it doesn't exist, an empty string is returned.
func TenantID(ctx context.Context) string {
	if requestID, ok := ctx.Value(ctxTenantKey{}).(string); ok {
		return requestID
	}

	return ""
}

// Passthrough "user" field in grpc from incoming to outgoing.
func PassthroughUserInGrpcMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	user := md[strings.ToLower(util.HeaderUser)]
	if len(user) == 0 || len(user[0]) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, util.HeaderUser, user[0])
}

// Set "user" field in grpc outgoing metadata
func WithUserInGrpcMetadata(ctx context.Context, user string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, util.HeaderUser, crypto.Base64Encode(user))
}

// Get "user" field from grpc metadata, empty string will returned if not set.
func GetUserFromGrpcMetadata(ctx context.Context) string {
	if user := getUserFromGrpcMetadataAux(ctx, metadata.FromIncomingContext); user != "" {
		return user
	}
	return getUserFromGrpcMetadataAux(ctx, metadata.FromOutgoingContext)
}

// Aux function for `GetUserFromGrpc`
func getUserFromGrpcMetadataAux(ctx context.Context, mdGetter func(ctx context.Context) (metadata.MD, bool)) string {
	md, ok := mdGetter(ctx)
	if !ok {
		return ""
	}
	user := md[strings.ToLower(util.HeaderUser)]
	if len(user) == 0 || len(user[0]) == 0 {
		return ""
	}
	// It may be duplicated in meta, but should be always same.
	rawuser, err := crypto.Base64Decode(user[0])
	if err != nil {
		return ""
	}
	return rawuser
}
