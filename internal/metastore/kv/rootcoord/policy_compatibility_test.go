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

package rootcoord

import (
	"context"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// These cases also run against master's per-grantee implementation to verify
// that batching preserves its results and validation/error ordering.
func TestListPolicyReadCompatibility(t *testing.T) {
	ctx := context.Background()
	tenant := util.DefaultTenant
	granteePrefix := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	firstKey := granteePrefix + "role1/Collection/default.coll1"
	secondKey := granteePrefix + "role2/Collection/default.coll2"
	thirdKey := granteePrefix + "role3/Collection/default.coll3"
	firstID := crypto.GranteeID(firstKey)
	sharedID := "0123456789abcdef"
	readErr := merr.WrapErrIoFailedReason("privilege scan failed")

	tests := []struct {
		name             string
		keys             []string
		values           []string
		granteeErr       error
		privilegeErr     error
		wantErr          error
		wantMessage      string
		wantNoPrivileges bool
	}{
		{
			name: "grantee read failure", granteeErr: readErr,
			wantErr: readErr, wantNoPrivileges: true,
		},
		{
			name: "empty grantees", privilegeErr: readErr, wantNoPrivileges: true,
		},
		{
			name: "empty non-nil grantees", keys: []string{}, values: []string{},
			privilegeErr: readErr, wantNoPrivileges: true,
		},
		{
			name: "empty grantees skip shape validation", values: []string{firstID},
			privilegeErr: readErr, wantNoPrivileges: true,
		},
		{
			name:         "malformed grantees skip shape validation",
			keys:         []string{granteePrefix + "role1/Collection/", granteePrefix + "malformed"},
			privilegeErr: readErr, wantNoPrivileges: true,
		},
		{
			name: "missing value is rejected before privilege read",
			keys: []string{firstKey, secondKey}, values: []string{firstID},
			privilegeErr: readErr, wantErr: merr.ErrServiceInternal,
			wantMessage: "loaded grantee keys and values", wantNoPrivileges: true,
		},
		{
			name: "extra value is rejected before privilege read",
			keys: []string{firstKey}, values: []string{firstID, sharedID},
			privilegeErr: readErr, wantErr: merr.ErrServiceInternal,
			wantMessage: "loaded grantee keys and values", wantNoPrivileges: true,
		},
		{
			name: "first legacy collision precedes privilege failure",
			keys: []string{firstKey, secondKey}, values: []string{sharedID, sharedID},
			privilegeErr: readErr, wantErr: merr.ErrIoFailed,
			wantMessage: "shared legacy grantee id", wantNoPrivileges: true,
		},
		{
			name: "malformed legacy owner precedes privilege failure",
			keys: []string{firstKey, granteePrefix + "malformed"}, values: []string{sharedID, sharedID},
			privilegeErr: readErr, wantErr: merr.ErrIoFailed,
			wantMessage: "shared legacy grantee id", wantNoPrivileges: true,
		},
		{
			name: "earlier privilege failure precedes later legacy collision",
			keys: []string{firstKey, secondKey, thirdKey}, values: []string{firstID, sharedID, sharedID},
			privilegeErr: readErr, wantErr: readErr, wantMessage: "privilege scan failed",
		},
		{
			name: "later legacy collision after successful read",
			keys: []string{firstKey, secondKey, thirdKey}, values: []string{firstID, sharedID, sharedID},
			wantErr: merr.ErrIoFailed, wantMessage: "shared legacy grantee id",
		},
		{
			name: "missing privileges are empty",
			keys: []string{firstKey, secondKey}, values: []string{firstID, strings.Repeat("a", 32)},
			privilegeErr: merr.Wrap(merr.ErrIoKeyNotFound, "missing prefix"),
		},
		{
			name: "legacy collision still checked after missing privileges",
			keys: []string{firstKey, secondKey, thirdKey}, values: []string{firstID, sharedID, sharedID},
			privilegeErr: merr.ErrIoKeyNotFound, wantErr: merr.ErrIoFailed,
			wantMessage: "shared legacy grantee id",
		},
		{
			name: "ordinary privilege failure is preserved",
			keys: []string{firstKey}, values: []string{firstID},
			privilegeErr: readErr, wantErr: readErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metaKV := memkv.NewMemoryKV()
			catalog := NewCatalog(metaKV)
			var prefixes []string
			patched := mockey.Mock((*memkv.MemoryKV).LoadWithPrefix).To(
				func(_ *memkv.MemoryKV, _ context.Context, prefix string) ([]string, []string, error) {
					prefixes = append(prefixes, prefix)
					if prefix == granteePrefix {
						return test.keys, test.values, test.granteeErr
					}
					return nil, nil, test.privilegeErr
				}).Build()
			defer patched.UnPatch()

			policies, err := catalog.ListPolicy(ctx, tenant)
			if test.wantErr == nil {
				require.NoError(t, err)
				assert.Nil(t, policies)
			} else {
				require.ErrorIs(t, err, test.wantErr)
				assert.Equal(t, []*milvuspb.GrantEntity{}, policies)
			}
			if test.wantMessage != "" {
				assert.Contains(t, err.Error(), test.wantMessage)
			}
			if test.wantNoPrivileges {
				assert.Equal(t, []string{granteePrefix}, prefixes)
			} else {
				assert.Greater(t, len(prefixes), 1)
			}
		})
	}
}

func TestListPolicyResultCompatibility(t *testing.T) {
	for _, tenant := range []string{util.DefaultTenant, "tenant-a"} {
		t.Run(tenant, func(t *testing.T) {
			ctx := context.Background()
			metaKV := memkv.NewMemoryKV()
			catalog := NewCatalog(metaKV)
			granteePrefix := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
			privilegePrefix := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant)
			firstKey := "role1/Collection/default.coll1"
			secondKey := "role2/Collection/default.coll2"
			thirdKey := "role3/Collection/default.coll3"
			firstID := crypto.GranteeID(GranteePrefix + "/" + firstKey)
			secondID := crypto.GranteeID(GranteePrefix + "/" + secondKey)
			thirdID := crypto.GranteeID(GranteePrefix + "/" + thirdKey)
			storedID := strings.Repeat("a", 32)
			missingID := strings.Repeat("b", 32)
			malformedID := strings.Repeat("c", 32)
			require.NotEqual(t, storedID, firstID)
			require.NotEqual(t, missingID, secondID)
			require.NotEqual(t, malformedID, thirdID)
			require.NoError(t, metaKV.MultiSave(ctx, map[string]string{
				granteePrefix + firstKey:  storedID,
				granteePrefix + secondKey: missingID,
				granteePrefix + thirdKey:  malformedID,
				// Stored privileges win over computed-ID privileges.
				privilegePrefix + storedID + "/PrivilegeRelease": "root",
				privilegePrefix + firstID + "/PrivilegeLoad":     "root",
				// A missing stored prefix falls back to the computed ID.
				privilegePrefix + secondID + "/PrivilegeLoad":    "root",
				privilegePrefix + secondID + "/PrivilegeRelease": "root",
				// Existing but malformed privileges still suppress fallback.
				privilegePrefix + malformedID + "/invalid/nested": "root",
				privilegePrefix + malformedID + "/":               "root",
				privilegePrefix + thirdID + "/PrivilegeLoad":      "root",
				// Unreferenced and malformed metadata never becomes a policy.
				privilegePrefix + "orphan/PrivilegeLoad": "root",
				privilegePrefix + "malformed":            "root",
				granteePrefix + "invalid/Collection/":    "invalid-id",
			}))

			policies, err := catalog.ListPolicy(ctx, tenant)
			require.NoError(t, err)
			require.Len(t, policies, 3)
			for i, expected := range []struct{ role, object, privilege string }{
				{"role1", "coll1", "Release"},
				{"role2", "coll2", "Load"},
				{"role2", "coll2", "Release"},
			} {
				assert.Equal(t, expected.role, policies[i].GetRole().GetName())
				assert.Equal(t, "Collection", policies[i].GetObject().GetName())
				assert.Equal(t, expected.object, policies[i].GetObjectName())
				assert.Equal(t, util.DefaultDBName, policies[i].GetDbName())
				assert.Equal(t, expected.privilege, policies[i].GetGrantor().GetPrivilege().GetName())
			}
		})
	}
}
