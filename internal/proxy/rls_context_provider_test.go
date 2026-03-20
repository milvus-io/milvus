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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAuthContextProvider(t *testing.T) {
	provider := NewAuthContextProvider()
	assert.NotNil(t, provider)
}

func TestAuthContextProviderGetUserContextNoUser(t *testing.T) {
	provider := NewAuthContextProvider()

	// Empty context - no user
	ctx := context.Background()
	rlsCtx, err := provider.GetUserContext(ctx)

	assert.NoError(t, err)
	assert.Nil(t, rlsCtx)
}

func TestSimpleContextProviderBasic(t *testing.T) {
	provider := NewSimpleContextProvider("alice", []string{"admin", "reader"})

	assert.NotNil(t, provider)
	assert.Equal(t, "alice", provider.userName)
	assert.Equal(t, []string{"admin", "reader"}, provider.userRoles)
}

func TestSimpleContextProviderGetRLSContext(t *testing.T) {
	provider := NewSimpleContextProvider("bob", []string{"writer"})

	rlsCtx, err := provider.GetRLSContext(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, rlsCtx)
	assert.Equal(t, "bob", rlsCtx.CurrentUserName)
	assert.Equal(t, []string{"writer"}, rlsCtx.CurrentRoles)
}

func TestSimpleContextProviderEmptyUser(t *testing.T) {
	provider := NewSimpleContextProvider("", []string{})

	rlsCtx, err := provider.GetRLSContext(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, rlsCtx)
	assert.Equal(t, "", rlsCtx.CurrentUserName)
	assert.Empty(t, rlsCtx.CurrentRoles)
}

func TestSimpleContextProviderWithTags(t *testing.T) {
	provider := NewSimpleContextProvider("alice", []string{"admin"})
	tags := map[string]string{
		"department": "engineering",
		"location":   "us-west",
	}
	provider.userTags = tags

	rlsCtx, err := provider.GetRLSContext(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, rlsCtx)
	assert.Equal(t, "alice", rlsCtx.CurrentUserName)
	assert.Equal(t, tags, rlsCtx.CurrentUserTags)
}

func TestRLSQueryContext(t *testing.T) {
	ctx := &RLSQueryContext{
		UserName:  "testuser",
		UserRoles: []string{"role1", "role2"},
	}

	assert.Equal(t, "testuser", ctx.UserName)
	assert.Equal(t, []string{"role1", "role2"}, ctx.UserRoles)
}

func TestRLSContext(t *testing.T) {
	ctx := &RLSContext{
		CurrentUserName: "testuser",
		CurrentRoles:    []string{"admin"},
		CurrentUserTags: map[string]string{"key": "value"},
	}

	assert.Equal(t, "testuser", ctx.CurrentUserName)
	assert.Equal(t, []string{"admin"}, ctx.CurrentRoles)
	assert.Equal(t, map[string]string{"key": "value"}, ctx.CurrentUserTags)
}

func TestContextProviderInterface(t *testing.T) {
	// Verify SimpleContextProvider implements ContextProvider
	var _ ContextProvider = (*SimpleContextProvider)(nil)

	provider := NewSimpleContextProvider("user", []string{"role"})
	rlsCtx, err := provider.GetRLSContext(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, rlsCtx)
}

func TestSimpleContextProviderConcurrency(t *testing.T) {
	provider := NewSimpleContextProvider("user", []string{"admin"})

	done := make(chan bool)

	// Multiple concurrent calls
	for i := 0; i < 20; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				ctx, err := provider.GetRLSContext(context.Background())
				if err != nil || ctx == nil {
					t.Error("unexpected error or nil context")
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}
