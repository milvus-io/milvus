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

	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestInitRLSManager(t *testing.T) {
	originalManager := globalRLSManager
	defer func() {
		globalRLSManager = originalManager
	}()

	mgr := InitRLSManager()
	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.GetCache())
	assert.Same(t, mgr, GetRLSManager())
}

func TestRLSManagerSetContextProvider(t *testing.T) {
	mgr := InitRLSManager()
	mgr.SetContextProvider(NewSimpleContextProvider("alice", []string{"PUBLIC"}))

	assert.NotNil(t, mgr.GetQueryInterceptor())
	assert.NotNil(t, mgr.GetSearchInterceptor())
	assert.NotNil(t, mgr.GetInsertInterceptor())
	assert.NotNil(t, mgr.GetDeleteInterceptor())
	assert.NotNil(t, mgr.GetUpsertInterceptor())
}

func TestRLSManagerSetPolicyLoader(t *testing.T) {
	mgr := InitRLSManager()
	loader := newMockRLSPolicyLoader()

	mgr.SetPolicyLoader(loader)
	assert.NotNil(t, mgr.GetCacheWithLoader())

	mgr.SetPolicyLoader(nil)
	assert.Nil(t, mgr.GetCacheWithLoader())
}

func TestRLSManagerEnsurePoliciesLoaded(t *testing.T) {
	mgr := InitRLSManager()
	loader := newMockRLSPolicyLoader()
	loader.SetPolicies("db1", "coll1", []*model.RLSPolicy{
		model.NewRLSPolicy(
			"p1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"}, "tenant_id == 't1'", "", "test",
		),
	})
	mgr.SetPolicyLoader(loader)

	// ensure interceptors are initialized to make manager fully usable
	mgr.SetContextProvider(NewSimpleContextProvider("alice", []string{"PUBLIC"}))

	mgr.EnsurePoliciesLoaded(context.Background(), 1, 100, "db1", "coll1")
	policies := mgr.GetCache().GetPoliciesForCollection(1, 100)
	assert.NotEmpty(t, policies)
}

func TestRLSManagerEnsureUserTagsLoaded(t *testing.T) {
	mgr := InitRLSManager()
	loader := newMockRLSPolicyLoader()
	loader.SetUserTags("alice", map[string]string{"department": "eng"})
	mgr.SetPolicyLoader(loader)

	mgr.EnsureUserTagsLoaded(context.Background(), "alice")
	tags := mgr.GetCache().GetUserTags("alice")
	assert.Equal(t, map[string]string{"department": "eng"}, tags)
}
