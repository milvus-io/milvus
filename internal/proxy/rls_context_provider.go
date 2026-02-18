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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// AuthContextProvider extracts user context from gRPC metadata for production use.
// It implements the ContextProvider interface defined in rls_query_interceptor.go.
type AuthContextProvider struct{}

// NewAuthContextProvider creates a new AuthContextProvider
func NewAuthContextProvider() *AuthContextProvider {
	return &AuthContextProvider{}
}

// GetUserContext extracts user information from the gRPC context metadata.
// Returns nil, nil if no user is found in the context (e.g., unauthenticated requests
// or missing gRPC metadata). This is a normal condition, not an error.
func (p *AuthContextProvider) GetUserContext(ctx context.Context) (*RLSQueryContext, error) {
	username, err := GetCurUserFromContext(ctx)
	if err != nil || username == "" {
		// No user in context - this is normal for unauthenticated or internal requests
		return nil, nil
	}

	roles, err := GetRole(username)
	if err != nil {
		log.Warn("failed to get roles for RLS context, using empty role set",
			zap.String("username", username),
			zap.Error(err))
		roles = []string{}
	}

	return &RLSQueryContext{
		UserName:  username,
		UserRoles: roles,
	}, nil
}
