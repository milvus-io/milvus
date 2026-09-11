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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCore_GetFeatureUsage(t *testing.T) {
	ctx := context.Background()
	req := &internalpb.GetFeatureUsageRequest{}

	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		resp, err := c.GetFeatureUsage(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Empty(t, resp.GetEntries())
	})

	t.Run("metadata error is a failed status, never a partial report", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		c.meta = meta
		meta.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return(nil, errors.New("mock err"))

		resp, err := c.GetFeatureUsage(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Empty(t, resp.GetEntries())
	})

	t.Run("normal", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		c.meta = meta

		meta.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{
			{Name: util.DefaultDBName},
			{Name: "db1", Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "2"}}},
		}, nil)
		meta.EXPECT().ListCollections(mock.Anything, util.DefaultDBName, typeutil.MaxTimestamp, true).Return([]*model.Collection{{
			CollectionID: 1, ShardsNum: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Partitions: []*model.Partition{{PartitionID: 1}},
			Fields: []*model.Field{
				{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
				{FieldID: 101, DataType: schemapb.DataType_VarChar, IsPartitionKey: true},
				{FieldID: 102, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
			},
			Properties: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
		}}, nil)
		meta.EXPECT().ListCollections(mock.Anything, "db1", typeutil.MaxTimestamp, true).Return(nil, nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, int64(1)).Return([]string{"alias_a", "alias_b"})
		meta.EXPECT().SelectRole(mock.Anything, util.DefaultTenant, mock.Anything, false).Return([]*milvuspb.RoleResult{
			{Role: &milvuspb.RoleEntity{Name: util.RoleAdmin}},
			{Role: &milvuspb.RoleEntity{Name: util.RolePublic}},
			{Role: &milvuspb.RoleEntity{Name: "ops"}},
		}, nil)
		meta.EXPECT().ListPolicy(mock.Anything, util.DefaultTenant).Return([]*milvuspb.GrantEntity{{}, {}}, nil)
		meta.EXPECT().ListPrivilegeGroups(mock.Anything).Return([]*milvuspb.PrivilegeGroupInfo{{}}, nil)

		resp, err := c.GetFeatureUsage(ctx, req)
		assert.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, typeutil.RootCoordRole, resp.GetRole())
		assert.NotZero(t, resp.GetCollectedAt())

		find := func(group, name string) *internalpb.FeatureEntry {
			for _, e := range resp.GetEntries() {
				if e.Group == group && e.Name == name {
					return e
				}
			}
			return nil
		}
		require.NotNil(t, find(featureusage.GroupObjects, featureusage.ObjectDatabases))
		assert.EqualValues(t, 1, find(featureusage.GroupObjects, featureusage.ObjectDatabases).Value, "default db not counted")
		assert.EqualValues(t, 2, find(featureusage.GroupObjects, featureusage.ObjectAliases).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupObjects, featureusage.ObjectCustomRoles).Value, "admin and public excluded")
		assert.EqualValues(t, 2, find(featureusage.GroupObjects, featureusage.ObjectGrants).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupObjects, featureusage.ObjectPrivilegeGroups).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupFieldTypes, "Int64").Value)
		assert.EqualValues(t, 1, find(featureusage.GroupDeclared, featureusage.DeclaredPartitionKey).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupDeclared, featureusage.DeclaredAutoID).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupProperties, common.MmapEnabledKey+"=true").Value)
		assert.EqualValues(t, 1, find(featureusage.GroupDBProperties, common.DatabaseReplicaNumber).Value)

		// No user-controlled string in the response.
		assert.NotContains(t, resp.String(), "alias_a")
		assert.NotContains(t, resp.String(), "ops")
		assert.NotContains(t, resp.String(), "db1")
	})
}
