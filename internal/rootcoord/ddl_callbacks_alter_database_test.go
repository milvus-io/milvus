// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksAlterDatabase(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)

	// Cannot alter collection with empty properties and delete keys.
	resp, err := core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Cannot alter collection properties with delete keys at same time.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "1"}},
		DeleteKeys: []string{common.DatabaseReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// hook related properties are not allowed to be altered.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: hookutil.EncryptionEnabledKey, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Alter a database that does not exist should return error.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "1"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 1)

	// alter a property of a database.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.DatabaseReplicaNumber, Value: "2"},
			{Key: common.DatabaseResourceGroups, Value: "rg1,rg2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 2)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{"rg1", "rg2"})

	// alter a property of a database should be idempotent.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.DatabaseReplicaNumber, Value: "2"},
			{Key: common.DatabaseResourceGroups, Value: "rg1,rg2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 2)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{"rg1", "rg2"})

	// delete a property of a database.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber, common.DatabaseResourceGroups},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 0)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{})

	// delete a property of a collection should be idempotent.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber, common.DatabaseResourceGroups},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 0)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{})
}

func assertDatabaseReplicaNumber(t *testing.T, ctx context.Context, core *Core, dbName string, replicaNumber int64) {
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	replicaNum, err := common.DatabaseLevelReplicaNumber(db.Properties)
	if replicaNumber == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, replicaNumber, replicaNum)
}

func assertDatabaseResourceGroups(t *testing.T, ctx context.Context, core *Core, dbName string, resourceGroups []string) {
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	rgs, err := common.DatabaseLevelResourceGroups(db.Properties)
	if len(resourceGroups) == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.ElementsMatch(t, resourceGroups, rgs)
}
