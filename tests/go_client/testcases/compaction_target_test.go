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
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestCompactionTargetRewrite(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	_, err := mc.ListCompactionTargets(ctx, client.NewListCompactionTargetsOption())
	if client.ErrorCode(err) == 3000 && strings.Contains(err.Error(), "target-based compaction is disabled") {
		t.Skip("requires target-based compaction")
	}
	require.NoError(t, err)

	name := common.GenRandomString("tbc_rewrite", 8)
	const rows = 1_024
	const dim = 4
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithShardNum(1).
		WithIndexOptions(client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})

	ids := make([]int64, rows)
	vectors := make([][]float32, rows)
	for i := range ids {
		ids[i] = int64(i)
		vectors[i] = []float32{float32(i), 1, 0, 1}
	}
	inserted, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name).WithInt64Column("id", ids).
		WithFloatVectorColumn("vector", dim, vectors))
	require.NoError(t, err)
	require.EqualValues(t, rows, inserted.InsertCount)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))

	var inputID int64
	require.Eventually(t, func() bool {
		response, err := mc.GetService().GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{CollectionName: name})
		if err != nil || response.GetStatus() == nil || response.GetStatus().GetCode() != 0 {
			return false
		}
		for _, segment := range response.GetInfos() {
			if segment.GetIsSorted() && segment.GetNumRows() == rows {
				inputID = segment.GetSegmentID()
				return true
			}
		}
		return false
	}, time.Minute, time.Second)

	created, err := mc.CreateCompactionTarget(ctx, client.NewRewriteCompactionTargetOption(name, inputID))
	require.NoError(t, err)
	require.Positive(t, created.GetTargetId())
	require.EqualValues(t, rows, created.GetTotalRows())

	target, err := mc.WaitForCompactionTarget(ctx, created.GetTargetId(), time.Second)
	require.NoError(t, err)
	require.Equal(t, "satisfied", target.GetStopReason())
	require.EqualValues(t, rows, target.GetTotalRows())
	require.NotNil(t, target.RemainingRows)
	require.Zero(t, target.GetRemainingRows())

	segments, err := mc.GetService().GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{CollectionName: name})
	require.NoError(t, err)
	require.NotNil(t, segments.GetStatus())
	require.Zero(t, segments.GetStatus().GetCode())
	var rewrittenRows int64
	for _, segment := range segments.GetInfos() {
		if slices.Contains(segment.GetCompactionFrom(), inputID) {
			rewrittenRows += segment.GetNumRows()
		}
	}
	require.EqualValues(t, rows, rewrittenRows)

	targets, err := mc.ListCompactionTargets(ctx, client.NewListCompactionTargetsOption().
		WithCollectionID(target.GetCollectionId()).
		WithStates(milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_INACTIVE))
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, target.GetTargetId(), targets[0].GetTargetId())
	require.Nil(t, targets[0].RemainingRows)

	require.NoError(t, mc.DropCompactionTarget(ctx, target.GetTargetId()))
	stopped, err := mc.DescribeCompactionTarget(ctx, target.GetTargetId())
	require.NoError(t, err)
	require.Equal(t, target.GetStopReason(), stopped.GetStopReason())
	require.Equal(t, target.GetStoppedAt(), stopped.GetStoppedAt())
}

func TestCompactionTargetClosesWhenCollectionIsDropped(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	_, err := mc.ListCompactionTargets(ctx, client.NewListCompactionTargetsOption())
	if client.ErrorCode(err) == 3000 && strings.Contains(err.Error(), "target-based compaction is disabled") {
		t.Skip("requires target-based compaction")
	}
	require.NoError(t, err)

	name := common.GenRandomString("tbc_drop_scope", 8)
	const dim = 4
	require.NoError(t, mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(name, dim)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})
	collection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
	require.NoError(t, err)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(name).
		WithFloatVectorColumn("vector", dim, [][]float32{{1, 0, 0, 0}}))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))

	protection, err := mc.GetService().CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{
		Name: "protect_during_drop", CollectionName: name, CompactionProtectionSeconds: 60,
	})
	require.NoError(t, err)
	require.NotNil(t, protection)
	require.Zero(t, protection.GetCode(), protection.GetReason())

	created, err := mc.CreateCompactionTarget(ctx, client.NewRewriteCompactionTargetOption(name))
	require.NoError(t, err)
	require.EqualValues(t, 1, created.GetTotalRows())
	before, err := mc.DescribeCompactionTarget(ctx, created.GetTargetId())
	require.NoError(t, err)
	require.Equal(t, milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_ACTIVE, before.GetState())

	require.NoError(t, mc.DropCollection(ctx, client.NewDropCollectionOption(name)))
	after, err := mc.DescribeCompactionTarget(ctx, created.GetTargetId())
	require.NoError(t, err)
	require.Equal(t, collection.ID, after.GetCollectionId())
	require.Empty(t, after.GetCollectionName())
	require.Equal(t, milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_INACTIVE, after.GetState())
	require.Equal(t, "scope_dropped", after.GetStopReason())
	require.NotZero(t, after.GetStoppedAt())
	require.Nil(t, after.RemainingRows)

	listed, err := mc.ListCompactionTargets(ctx, client.NewListCompactionTargetsOption().WithCollectionID(collection.ID))
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, after.GetTargetId(), listed[0].GetTargetId())
	require.NoError(t, mc.DropCompactionTarget(ctx, after.GetTargetId()))
	retained, err := mc.DescribeCompactionTarget(ctx, after.GetTargetId())
	require.NoError(t, err)
	require.Equal(t, after.GetStoppedAt(), retained.GetStoppedAt())
	require.Equal(t, "scope_dropped", retained.GetStopReason())
}
