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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

	t.Run("normal", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		c.meta = meta

		// One snapshot call: the report reads the MetaTable cache once and
		// never touches the catalog, so no RBAC listing is expected either.
		meta.EXPECT().FeatureUsageSnapshot(mock.Anything).Return(featureusage.CollectionInput{
			Databases: []*model.Database{
				{Name: util.DefaultDBName},
				{Name: "db1", Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "2"}}},
			},
			Collections: []*model.Collection{{
				CollectionID: 1, ShardsNum: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
				Partitions: []*model.Partition{{PartitionID: 1}},
				Fields: []*model.Field{
					// The server adds these to every collection; they are not the
					// user's Int64 fields and must not be counted as such.
					{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true, AutoID: true},
					{FieldID: 101, DataType: schemapb.DataType_VarChar, IsPartitionKey: true},
					{FieldID: 102, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
				},
				Properties: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
			}},
			AliasCount: 2,
		})

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
		assert.EqualValues(t, 0, find(featureusage.GroupFieldTypes, "Int64").Value,
			"RowID and Timestamp are server fields, not a user's Int64 field")
		assert.EqualValues(t, 1, find(featureusage.GroupFieldTypes, "VarChar").Value)
		for _, e := range resp.GetEntries() {
			assert.NotContains(t, []string{"custom_roles", "grants", "privilege_groups"}, e.GetName(),
				"RBAC objects are not counted: listing them reads the catalog")
		}
		assert.EqualValues(t, 1, find(featureusage.GroupDeclared, featureusage.DeclaredPartitionKey).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupDeclared, featureusage.DeclaredAutoID).Value)
		assert.EqualValues(t, 1, find(featureusage.GroupProperties, common.MmapEnabledKey+"=true").Value)
		assert.EqualValues(t, 1, find(featureusage.GroupDBProperties, common.DatabaseReplicaNumber).Value)

		// No user-controlled string in the response. Alias names are not read
		// at all -- the snapshot carries a total -- so only the database name
		// is left to check.
		assert.NotContains(t, resp.String(), "db1")
	})
}
