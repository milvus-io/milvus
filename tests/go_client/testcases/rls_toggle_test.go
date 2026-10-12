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

package testcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestRLSToggleSchemaDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name := prepareRLSCollectionWithEnforcement(ctx, t, mc, "", false, false)
	createRLSPolicy(ctx, t, mc, name, "owner", "owner == $current_principal", "", milvuspb.RowPolicyAction_Query)
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
	require.ErrorContains(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(name, "owner")), "RLS")
	require.NoError(t, mc.AlterCollectionFieldProperty(ctx, client.NewAlterCollectionFieldPropertiesOption(name, "owner").WithProperty("field.description", "Owner identity")))
	require.Error(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "owner", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query).WithUsingExpr("missing == 'alice'")))
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Equal(t, "owner == $current_principal", policies[0].GetUsingExpr())
	require.NoError(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "owner")))
	require.NoError(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(name, "owner")))
}
