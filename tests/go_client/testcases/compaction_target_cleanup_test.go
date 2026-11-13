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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestCompactionTargetCleanupQuery(t *testing.T) {
	const rows, dim = 1024, 4
	for _, tc := range []struct {
		family          string
		withExpiryField bool
	}{
		{family: "DELETE_CLEANUP"},
		{family: "TTL", withExpiryField: true},
	} {
		t.Run(tc.family, func(t *testing.T) {
			ctx := hp.CreateContext(t, 5*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			options := client.NewListCompactionTargetsOption().WithIncludeSystem(true)
			listed, err := mc.ListCompactionTargets(ctx, options)
			if client.ErrorCode(err) == 3000 && strings.Contains(err.Error(), "target-based compaction is disabled") {
				t.Skip("requires target-based compaction")
			}
			require.NoError(t, err)
			find := func(list []*milvuspb.CompactionTargetInfo) *milvuspb.CompactionTargetInfo {
				for _, target := range list {
					if target.GetOther().GetCondition() == tc.family {
						return target
					}
				}
				return nil
			}
			target := find(listed)
			if target == nil {
				require.Eventually(t, func() bool {
					current, err := mc.ListCompactionTargets(ctx, options)
					if err != nil {
						return false
					}
					target = find(current)
					return target != nil
				}, time.Minute, time.Second)
			}
			require.True(t, target.GetOther().GetClusterWide())
			enabled := false
			for _, p := range target.GetOther().GetParameters() {
				if p.GetKey() == "enabled" {
					enabled = p.GetValue() == "true"
				}
			}
			if !enabled {
				t.Skip("requires automatic compaction")
			}

			name := common.GenRandomString("tbc_cleanup", 8)
			schema := entity.NewSchema().WithName(name).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim))
			if tc.withExpiryField {
				schema.WithField(entity.NewField().WithName("expires_at").WithDataType(entity.FieldTypeTimestamptz))
			}
			create := client.NewCreateCollectionOption(name, schema).WithShardNum(1).
				WithProperty("collection.autocompaction.enabled", false).
				WithIndexOptions(client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
			if tc.withExpiryField {
				create.WithProperty("ttl_field", "expires_at")
			}
			require.NoError(t, mc.CreateCollection(ctx, create))
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
			})

			ids := make([]int64, rows)
			vectors := make([][]float32, rows)
			expiry := make([]time.Time, rows)
			nearExpiry := time.Now().Add(90 * time.Second)
			for i := range ids {
				ids[i] = int64(i)
				vectors[i] = []float32{float32(i), 1, 0, 1}
				expiry[i] = nearExpiry.Add(time.Hour)
				if i < rows/2 {
					expiry[i] = nearExpiry
				}
			}
			insert := client.NewColumnBasedInsertOption(name).WithInt64Column("id", ids).
				WithFloatVectorColumn("vector", dim, vectors)
			if tc.withExpiryField {
				insert.WithColumns(column.NewColumnTimestamptz("expires_at", expiry))
			}
			inserted, err := mc.Insert(ctx, insert)
			require.NoError(t, err)
			require.EqualValues(t, rows, inserted.InsertCount)
			flush := func() {
				t.Helper()
				operation, err := mc.Flush(ctx, client.NewFlushOption(name))
				require.NoError(t, err)
				require.NoError(t, operation.Await(ctx))
			}
			flush()

			persisted := func() ([]*milvuspb.PersistentSegmentInfo, bool) {
				resp, err := mc.GetService().GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{CollectionName: name})
				return resp.GetInfos(), err == nil && resp.GetStatus() != nil && resp.GetStatus().GetCode() == 0
			}
			var inputID int64
			require.Eventually(t, func() bool {
				segments, ok := persisted()
				if !ok {
					return false
				}
				for _, segment := range segments {
					if segment.GetIsSorted() && segment.GetNumRows() == rows {
						inputID = segment.GetSegmentID()
						return true
					}
				}
				return false
			}, time.Minute, time.Second)

			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			if !tc.withExpiryField {
				deleted, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("id < 512"))
				require.NoError(t, err)
				require.EqualValues(t, rows/2, deleted.DeleteCount)
				flush()
			}

			require.NoError(t, mc.AlterCollectionProperties(ctx,
				client.NewAlterCollectionPropertiesOption(name).WithProperty("collection.autocompaction.enabled", true)))
			var outputID int64
			require.Eventually(t, func() bool {
				segments, ok := persisted()
				if !ok {
					return false
				}
				for _, segment := range segments {
					if slices.Contains(segment.GetCompactionFrom(), inputID) && segment.GetNumRows() == rows/2 {
						outputID = segment.GetSegmentID()
						return true
					}
				}
				return false
			}, 2*time.Minute, time.Second, "expected a compacted replacement with half of the original rows")

			// Wait for the actual replacement to serve queries before
			// checking that the deleted/expired primary keys are absent.
			require.Eventually(t, func() bool {
				response, err := mc.GetService().GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
				return err == nil && response.GetStatus() != nil && response.GetStatus().GetCode() == 0 &&
					len(response.GetInfos()) == 1 && response.GetInfos()[0].GetSegmentID() == outputID
			}, time.Minute, time.Second)
			result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").
				WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			returned, ok := result.GetColumn("id").(*column.ColumnInt64)
			require.True(t, ok)
			require.ElementsMatch(t, ids[rows/2:], returned.Data())

			after, err := mc.DescribeCompactionTarget(ctx, target.GetTargetId())
			require.NoError(t, err)
			require.Equal(t, target.GetTargetId(), after.GetTargetId())
			require.Equal(t, milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_ACTIVE, after.GetState())
		})
	}
}
