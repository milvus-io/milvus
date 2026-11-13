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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionTargetCollectionFollowsStableIdentity(t *testing.T) {
	ctx := t.Context()
	core, err := NewCore(ctx, nil)
	require.NoError(t, err)
	defer core.cancel()

	table := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"default": model.NewDefaultDatabase(nil),
			"other":   model.NewDatabase(2, "other", pb.DatabaseState_DatabaseCreated, nil),
		},
		collID2Meta: map[int64]*model.Collection{
			41: {CollectionID: 41, DBID: 1, DBName: "default", Name: "records", State: pb.CollectionState_CollectionCreated, UpdateTimestamp: 10},
		},
	}
	core.meta = table
	_, _, err = core.CompactionTargetCollection(ctx, 41)
	require.Error(t, err)
	core.UpdateStateCode(commonpb.StateCode_Healthy)

	resource, external, err := core.CompactionTargetCollection(ctx, 41)
	require.NoError(t, err)
	require.False(t, external)
	require.Equal(t, "records", resource.GetCollectionName())
	require.Equal(t, "default", resource.GetDbName())
	require.EqualValues(t, 10, resource.GetRevision())

	moved := table.collID2Meta[41].Clone()
	moved.Name, moved.DBName, moved.DBID, moved.UpdateTimestamp = "renamed", "other", 2, 11
	table.collID2Meta[41] = moved
	table.collID2Meta[42] = &model.Collection{
		CollectionID: 42, DBID: 1, DBName: "default", Name: "records",
		State: pb.CollectionState_CollectionCreated, UpdateTimestamp: 12,
	}
	resource, _, err = core.CompactionTargetCollection(ctx, 41)
	require.NoError(t, err)
	require.EqualValues(t, 41, resource.GetCollectionId())
	require.Equal(t, "renamed", resource.GetCollectionName())
	require.Equal(t, "other", resource.GetDbName())
	require.EqualValues(t, 11, resource.GetRevision())

	moved.State = pb.CollectionState_CollectionDropping
	resource, _, err = core.CompactionTargetCollection(ctx, 41)
	require.NoError(t, err)
	require.True(t, resource.GetDeleted())
	require.Empty(t, resource.GetCollectionName())
	require.Equal(t, "other", resource.GetDbName())

	delete(table.collID2Meta, 41)
	resource, _, err = core.CompactionTargetCollection(ctx, 41)
	require.NoError(t, err)
	require.True(t, resource.GetDeleted())
	require.Empty(t, resource.GetDbName())

	table.collID2Meta[42].State = pb.CollectionState_CollectionCreating
	_, _, err = core.CompactionTargetCollection(ctx, 42)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
